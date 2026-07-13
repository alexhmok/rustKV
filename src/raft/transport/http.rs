//! Real node-to-node transport: JSON-over-HTTP/1.1 between raft listeners.
//!
//! Outbound ([`HttpTransport::send`]): each RPC is a `POST /raft` to the
//! peer's raft address, carrying an [`Envelope`] (sender id + request). The
//! HTTP client is hand-rolled over `tokio::net::TcpStream` — no HTTP-client
//! crate is on the dependency whitelist. One connection per RPC with
//! `Connection: close` (framing by EOF or Content-Length); at heartbeat
//! rates over a LAN this is fine. TODO(perf): connection reuse/pooling if
//! RPC volume ever makes churn measurable.
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
use std::sync::Arc;
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

#[derive(Clone)]
pub struct HttpTransport {
    id: NodeId,
    /// Peer id → raft address (`host:port`). Fixed cluster membership.
    peers: Arc<HashMap<NodeId, String>>,
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
            peers: Arc::new(peers),
            rpc_timeout,
        };
        (transport, router, inbound_rx)
    }
}

impl Transport for HttpTransport {
    async fn send(&self, to: NodeId, req: RpcRequest) -> Result<RpcResponse, TransportError> {
        let Some(addr) = self.peers.get(&to) else {
            return Err(TransportError::Unreachable(to));
        };
        let body = serde_json::to_vec(&Envelope {
            from: self.id,
            request: req,
        })
        .expect("rpc serialization cannot fail");

        let raw = tokio::time::timeout(self.rpc_timeout, post_json(addr, "/raft", &body))
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

/// Minimal HTTP/1.1 POST: one connection, `Connection: close`, returns the
/// response body of a 200. Chunked responses are rejected (axum sends
/// Content-Length for our fixed-size JSON bodies).
async fn post_json(addr: &str, path: &str, body: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await?;
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;

    let mut raw = Vec::with_capacity(1024);
    stream.read_to_end(&mut raw).await?;
    parse_response(&raw)
}

fn parse_response(raw: &[u8]) -> std::io::Result<Vec<u8>> {
    let bad = |msg: String| std::io::Error::new(std::io::ErrorKind::InvalidData, msg);
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| bad("no header terminator in response".into()))?;
    let head = std::str::from_utf8(&raw[..header_end])
        .map_err(|_| bad("non-utf8 response head".into()))?;

    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| bad(format!("malformed status line: {head:.80}")))?;
    if status != 200 {
        return Err(bad(format!("raft rpc returned status {status}")));
    }
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return Err(bad("chunked responses are not supported".into()));
    }

    let body = &raw[header_end + 4..];
    // Trust Content-Length when present; otherwise the close-delimited rest.
    for line in head.lines().skip(1) {
        if let Some(value) = line
            .to_ascii_lowercase()
            .strip_prefix("content-length:")
            .map(str::trim)
            .and_then(|v| v.parse::<usize>().ok())
        {
            if body.len() < value {
                return Err(bad("truncated response body".into()));
            }
            return Ok(body[..value].to_vec());
        }
    }
    Ok(body.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_response_handles_content_length_and_close_delimited() {
        let ok = b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhelloEXTRA";
        assert_eq!(parse_response(ok).unwrap(), b"hello");

        let no_length = b"HTTP/1.1 200 OK\r\nx: y\r\n\r\nrest-of-stream";
        assert_eq!(parse_response(no_length).unwrap(), b"rest-of-stream");

        let error = b"HTTP/1.1 503 Service Unavailable\r\n\r\n";
        assert!(parse_response(error).is_err());

        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
        assert!(parse_response(chunked).is_err());

        assert!(parse_response(b"garbage").is_err());
    }
}
