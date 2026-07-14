//! Real node-to-node transport: JSON-over-HTTP/1.1 between raft listeners.
//!
//! Outbound ([`HttpTransport::send`]): each RPC is a `POST /raft` to the
//! peer's raft address, carrying an [`Envelope`] (sender id + request). The
//! HTTP client is hand-rolled over `tokio::net::TcpStream` — no HTTP-client
//! crate is on the dependency whitelist. Connections are persistent
//! HTTP/1.1, pooled per *address* (phase 16): checkout is exclusive (a
//! connection serves one RPC at a time), checkin happens only at the
//! successful tail — a fully-consumed, Content-Length-framed 200 — so a
//! stream dropped by the RPC timeout can never re-enter the pool. A failed
//! attempt on a *pooled* connection is retried once on a fresh one (the
//! stale-idle race); see [`HttpTransport::post_json`] for the safety
//! argument.
//!
//! Inbound: [`HttpTransport::new`] returns an axum `Router` to mount on the
//! node's raft listener; it feeds decoded RPCs into the same
//! `mpsc::Receiver<Inbound>` interface the simulator uses, so the Raft core
//! cannot tell the transports apart.
//!
//! Failure mapping mirrors a real network: connect/IO/parse errors and slow
//! peers all surface as [`TransportError::Timeout`] (indistinguishable by
//! design); only an id missing from the peer map is `Unreachable`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::raft::rpc::{RpcRequest, RpcResponse};
use crate::raft::transport::{Inbound, Transport, TransportError};
use crate::raft::types::NodeId;

#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    from: NodeId,
    request: RpcRequest,
}

/// Idle connections kept per address; parallelism beyond this churns
/// (extra connections are opened per RPC and dropped instead of pooled).
const MAX_IDLE_PER_PEER: usize = 4;
/// Backstop against a peer streaming garbage instead of a header terminator.
const MAX_RESPONSE_HEAD: usize = 16 * 1024;

#[derive(Clone)]
pub struct HttpTransport {
    id: NodeId,
    /// Peer id → raft address (`host:port`). Bootstrapped from config;
    /// replaced at runtime when membership changes (phase 15) — main.rs
    /// follows the Raft core's membership watch and calls [`Self::set_peers`].
    peers: Arc<RwLock<HashMap<NodeId, String>>>,
    /// Idle connections, keyed by ADDRESS rather than node id: a phase-15
    /// address change is a new key, so stale sockets to a re-addressed
    /// member are never handed out for its new address. Shared across
    /// clones; a std `Mutex` because it is never held across an await.
    pool: Arc<Mutex<HashMap<String, Vec<TcpStream>>>>,
    rpc_timeout: Duration,
}

impl HttpTransport {
    /// Builds the outbound transport, the router to serve on this node's
    /// raft listener, and the inbound channel for the Raft core.
    pub fn new(
        id: NodeId,
        peers: HashMap<NodeId, String>,
        rpc_timeout: Duration,
    ) -> (Self, Router, mpsc::UnboundedReceiver<Inbound>) {
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let router = Router::new()
            .route("/raft", post(handle_raft_rpc))
            .with_state(inbound_tx);
        let transport = Self {
            id,
            peers: Arc::new(RwLock::new(peers)),
            pool: Arc::new(Mutex::new(HashMap::new())),
            rpc_timeout,
        };
        (transport, router, inbound_rx)
    }

    /// Replaces the peer address book (phase 15: membership changed). All
    /// clones share it — in-flight RPCs already resolved their address and
    /// finish against the old one, exactly like a real network change.
    /// Idle connections to addresses that left the book are pruned in the
    /// same critical section — otherwise sockets to removed or re-addressed
    /// members would sit in the pool until process exit.
    pub fn set_peers(&self, peers: HashMap<NodeId, String>) {
        tracing::info!(node = self.id, peers = ?peers, "raft peer addresses updated");
        let mut book = self.peers.write().expect("peer map lock poisoned");
        *book = peers;
        let mut pool = self.pool.lock().expect("pool lock poisoned");
        pool.retain(|addr, _| book.values().any(|current| current == addr));
    }

    /// Pops an idle connection to `addr`, if any. Checkout is exclusive:
    /// parallel RPCs to one peer each get their own stream, so responses
    /// can never interleave.
    fn checkout(&self, addr: &str) -> Option<TcpStream> {
        self.pool
            .lock()
            .expect("pool lock poisoned")
            .get_mut(addr)
            .and_then(Vec::pop)
    }

    /// Returns a connection to the idle pool. Called ONLY at the successful
    /// tail of an RPC — a fully-consumed, Content-Length-framed 200. That
    /// single call site is what makes cancellation safe: when the outer
    /// rpc_timeout drops the in-flight future, the stream it owned is
    /// dropped with it and its half-read response can never leak into a
    /// later RPC.
    fn checkin(&self, addr: &str, stream: TcpStream) {
        let mut pool = self.pool.lock().expect("pool lock poisoned");
        let idle = pool.entry(addr.to_string()).or_default();
        if idle.len() < MAX_IDLE_PER_PEER {
            idle.push(stream);
        }
    }

    /// POSTs `body` to the peer, over a pooled connection when one is idle.
    ///
    /// Retry policy: exactly one retry, on a FRESH connection, and only
    /// when the failed attempt used a POOLED connection — the stale-idle
    /// race, where the peer closed the socket while it sat in the pool. A
    /// fresh-connection failure is a real network answer and is never
    /// retried. The retry may duplicate an RPC the peer already processed
    /// (the pooled attempt can fail after the request was written); that
    /// is safe because Raft RPCs are duplicate-tolerant — phase 10's
    /// duplication fault is the standing proof, and InstallSnapshot
    /// carries its own idempotence guard (phase 14). Callers wrap this in
    /// rpc_timeout, so the retry spends the same budget, not extra.
    async fn post_json(&self, addr: &str, path: &str, body: &[u8]) -> std::io::Result<Vec<u8>> {
        if let Some(mut stream) = self.checkout(addr) {
            match request_response(&mut stream, addr, path, body).await {
                Ok((raw, reusable)) => {
                    if reusable {
                        self.checkin(addr, stream);
                    }
                    return Ok(raw);
                }
                Err(error) => {
                    tracing::trace!(
                        node = self.id,
                        addr,
                        %error,
                        "pooled connection failed; retrying on a fresh one"
                    );
                }
            }
        }
        let mut stream = TcpStream::connect(addr).await?;
        let (raw, reusable) = request_response(&mut stream, addr, path, body).await?;
        if reusable {
            self.checkin(addr, stream);
        }
        Ok(raw)
    }
}

impl Transport for HttpTransport {
    async fn send(&self, to: NodeId, req: RpcRequest) -> Result<RpcResponse, TransportError> {
        // Resolve the address up front; the lock is never held across await.
        let addr = self
            .peers
            .read()
            .expect("peer map lock poisoned")
            .get(&to)
            .cloned();
        let Some(addr) = addr else {
            return Err(TransportError::Unreachable(to));
        };
        let body = serde_json::to_vec(&Envelope {
            from: self.id,
            request: req,
        })
        .expect("rpc serialization cannot fail");

        let raw = tokio::time::timeout(self.rpc_timeout, self.post_json(&addr, "/raft", &body))
            .await
            .map_err(|_elapsed| TransportError::Timeout)?
            .map_err(|error| {
                tracing::trace!(node = self.id, to, %error, "raft rpc transport error");
                TransportError::Timeout
            })?;
        serde_json::from_slice(&raw).map_err(|error| {
            tracing::warn!(node = self.id, to, %error, "undecodable raft rpc response");
            TransportError::Timeout
        })
    }
}

async fn handle_raft_rpc(
    State(inbound_tx): State<mpsc::UnboundedSender<Inbound>>,
    body: Bytes,
) -> Response {
    let envelope: Envelope = match serde_json::from_slice(&body) {
        Ok(envelope) => envelope,
        Err(error) => {
            tracing::warn!(%error, "undecodable raft rpc request");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    let (reply_tx, reply_rx) = oneshot::channel();
    let delivered = inbound_tx.send(Inbound {
        from: envelope.from,
        request: envelope.request,
        reply: reply_tx,
    });
    if delivered.is_err() {
        // The raft node has stopped.
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match reply_rx.await {
        Ok(response) => (
            StatusCode::OK,
            serde_json::to_vec(&response).expect("rpc serialization cannot fail"),
        )
            .into_response(),
        Err(_node_gone) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

/// Writes one HTTP/1.1 POST (persistent — no `Connection: close`) and reads
/// one response. Returns the body and whether the connection is clean for
/// reuse: exactly the declared Content-Length was consumed and the server
/// did not opt out of keep-alive. Chunked responses are rejected (axum
/// sends Content-Length for our fixed-size JSON bodies); a 200 without
/// Content-Length is read to close and reported non-reusable.
async fn request_response(
    stream: &mut TcpStream,
    addr: &str,
    path: &str,
    body: &[u8],
) -> std::io::Result<(Vec<u8>, bool)> {
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;

    // Incremental read: scan for the end of headers (body bytes may arrive
    // in the same read), then take exactly Content-Length body bytes —
    // EOF framing is gone with `Connection: close`.
    let mut buf = Vec::with_capacity(1024);
    let header_end = loop {
        if let Some(end) = find_header_end(&buf) {
            break end;
        }
        if buf.len() > MAX_RESPONSE_HEAD {
            return Err(invalid_data("response head too large".into()));
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before response head",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = parse_response_head(&buf[..header_end])?;
    let mut response_body = buf.split_off(header_end + 4);

    match head.content_length {
        Some(length) => {
            let buffered = response_body.len();
            if buffered < length {
                response_body.resize(length, 0);
                stream.read_exact(&mut response_body[buffered..]).await?;
                Ok((response_body, head.keep_alive))
            } else {
                // Bytes beyond the declared body leave the connection in an
                // unknowable state: return the body, never repool.
                let exact = buffered == length;
                response_body.truncate(length);
                Ok((response_body, head.keep_alive && exact))
            }
        }
        None => {
            // No Content-Length: close-delimited body; consuming it uses
            // the connection up by definition.
            stream.read_to_end(&mut response_body).await?;
            Ok((response_body, false))
        }
    }
}

fn invalid_data(msg: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

struct ResponseHead {
    content_length: Option<usize>,
    /// False when the server sent `Connection: close`.
    keep_alive: bool,
}

/// Parses a response head (status line + headers, without the trailing
/// `\r\n\r\n`). Non-200 and chunked responses are errors — the caller
/// treats them like any transport failure, and they are never repooled.
fn parse_response_head(raw: &[u8]) -> std::io::Result<ResponseHead> {
    let head =
        std::str::from_utf8(raw).map_err(|_| invalid_data("non-utf8 response head".into()))?;

    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| invalid_data(format!("malformed status line: {head:.80}")))?;
    if status != 200 {
        return Err(invalid_data(format!("raft rpc returned status {status}")));
    }

    let lower = head.to_ascii_lowercase();
    if lower.contains("transfer-encoding: chunked") {
        return Err(invalid_data("chunked responses are not supported".into()));
    }

    let mut content_length = None;
    for line in lower.lines().skip(1) {
        if let Some(value) = line.strip_prefix("content-length:") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| invalid_data(format!("bad content-length: {line:.80}")))?,
            );
        }
    }
    Ok(ResponseHead {
        content_length,
        keep_alive: !lower.contains("connection: close"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_response_head_extracts_length_and_rejects_errors() {
        let ok = parse_response_head(b"HTTP/1.1 200 OK\r\ncontent-length: 5").unwrap();
        assert_eq!(ok.content_length, Some(5));
        assert!(ok.keep_alive);

        let no_length = parse_response_head(b"HTTP/1.1 200 OK\r\nx: y").unwrap();
        assert_eq!(no_length.content_length, None);

        let close = parse_response_head(b"HTTP/1.1 200 OK\r\nConnection: close").unwrap();
        assert!(!close.keep_alive, "server keep-alive opt-out honored");

        assert!(parse_response_head(b"HTTP/1.1 503 Service Unavailable").is_err());
        assert!(parse_response_head(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked").is_err());
        assert!(parse_response_head(b"HTTP/1.1 200 OK\r\nContent-Length: nope").is_err());
        assert!(parse_response_head(b"garbage").is_err());
    }

    #[test]
    fn find_header_end_scans_incremental_buffers() {
        assert_eq!(find_header_end(b"HTTP/1.1 200 OK\r\ncont"), None);
        assert_eq!(find_header_end(b"HTTP/1.1 200 OK\r\n\r\n"), Some(15));
        // Body bytes already buffered past the terminator are fine.
        assert_eq!(find_header_end(b"HTTP/1.1 200 OK\r\n\r\nhello"), Some(15));
    }
}
