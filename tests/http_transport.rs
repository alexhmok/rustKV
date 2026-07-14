//! Tests of the real HTTP node-to-node transport: the transport in
//! isolation, then a full in-process 3-node cluster where BOTH the client
//! API and raft RPCs run over real sockets.
//!
//! Covered: RPC roundtrip over HTTP, unreachable-vs-timeout semantics
//! (unknown id, dead peer, black-holed listener), connection pooling
//! (phase 16: reuse across sequential RPCs, stale-conn retry after a
//! server restart, exclusive checkout under parallel RPCs, no repool
//! after a timeout), and a real-transport cluster electing a leader,
//! committing writes everywhere, and surviving a leader crash.
//! NOT covered: transport behavior under OS-level packet loss (the sim
//! transport owns fault injection; real-network partitions are exercised
//! via Docker, see README).
//! Real-time tests (real sockets): poll-based waits, serialized like
//! tests/cluster_http.rs.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{node_config, put};
use rustkv::raft::Storage;
use rustkv::raft::node::{RaftHandle, RaftNode, RoleKind, StateMachine};
use rustkv::raft::rpc::{RequestVoteArgs, RequestVoteReply, RpcRequest, RpcResponse};
use rustkv::raft::transport::Transport;
use rustkv::raft::transport::http::HttpTransport;
use rustkv::raft::types::NodeId;
use rustkv::store::KvStore;
use tempfile::TempDir;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const RPC_TIMEOUT: Duration = Duration::from_millis(500);

/// Binds a raft listener for `id`, returning its transport (aware of
/// `peers`), its bound address, and the inbound channel.
async fn bind_transport(
    id: NodeId,
    peers: HashMap<NodeId, String>,
) -> (
    HttpTransport,
    String,
    tokio::sync::mpsc::UnboundedReceiver<rustkv::raft::transport::Inbound>,
) {
    let (transport, router, inbound) = HttpTransport::new(id, peers, RPC_TIMEOUT);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (transport, addr, inbound)
}

#[tokio::test]
async fn rpc_roundtrip_over_real_http() {
    let _serial = SERIAL.lock().await;
    // Node 2 first (to learn its address), then node 1 pointed at it.
    let (_t2, addr2, mut rx2) = bind_transport(2, HashMap::new()).await;
    let (t1, _addr1, _rx1) = bind_transport(1, HashMap::from([(2, addr2)])).await;

    // Node 2 answers its inbound RPCs.
    tokio::spawn(async move {
        while let Some(inbound) = rx2.recv().await {
            assert_eq!(inbound.from, 1);
            let RpcRequest::RequestVote(args) = &inbound.request else {
                panic!("unexpected rpc");
            };
            let reply = RequestVoteReply {
                term: args.term,
                vote_granted: true,
            };
            let _ = inbound.reply.send(RpcResponse::RequestVote(reply));
        }
    });

    let request = RpcRequest::RequestVote(RequestVoteArgs {
        term: 7,
        candidate_id: 1,
        last_log_index: 0,
        last_log_term: 0,
    });
    let response = t1.send(2, request).await.unwrap();
    assert_eq!(
        response,
        RpcResponse::RequestVote(RequestVoteReply {
            term: 7,
            vote_granted: true
        })
    );
}

/// Regression pin for the catch-up liveness bug found in the post-project
/// testing review: axum's 2 MiB default body limit on the raft port made
/// any AppendEntries batch or InstallSnapshot payload beyond it permanently
/// undeliverable (the limit layer resets the upload mid-write; the sender
/// sees a timeout and retries the identical oversized RPC forever, so a
/// follower more than ~2 MiB behind could never rejoin). The raft router
/// now disables the limit; this pins a >2 MiB RPC roundtripping.
#[tokio::test]
async fn rpcs_larger_than_two_mebibytes_roundtrip() {
    let _serial = SERIAL.lock().await;
    let (_t2, addr2, mut rx2) = bind_transport(2, HashMap::new()).await;
    let (t1, _addr1, _rx1) = bind_transport(1, HashMap::from([(2, addr2)])).await;

    tokio::spawn(async move {
        while let Some(inbound) = rx2.recv().await {
            let RpcRequest::AppendEntries(args) = &inbound.request else {
                panic!("unexpected rpc");
            };
            let reply = rustkv::raft::rpc::AppendEntriesReply {
                term: args.term,
                success: true,
            };
            let _ = inbound.reply.send(RpcResponse::AppendEntries(reply));
        }
    });

    // One 3 MiB entry — the size class of a real catch-up batch or
    // snapshot payload.
    let request = RpcRequest::AppendEntries(rustkv::raft::rpc::AppendEntriesArgs {
        term: 7,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![rustkv::raft::types::LogEntry {
            term: 7,
            index: 1,
            command: rustkv::raft::types::Command::Put {
                key: "big".to_string(),
                value: serde_json::Value::String("x".repeat(3 * 1024 * 1024)),
                session: None,
            },
        }],
        leader_commit: 0,
    });
    let response = t1.send(2, request).await.unwrap();
    assert_eq!(
        response,
        RpcResponse::AppendEntries(rustkv::raft::rpc::AppendEntriesReply {
            term: 7,
            success: true
        })
    );
}

#[tokio::test]
async fn unknown_peer_is_unreachable_dead_peer_times_out() {
    let _serial = SERIAL.lock().await;
    // Reserve a port, then close it, so 9 points at a dead address.
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap().to_string();
    drop(dead);

    let (t1, _addr1, _rx1) = bind_transport(1, HashMap::from([(9, dead_addr)])).await;
    let request = RpcRequest::RequestVote(RequestVoteArgs {
        term: 1,
        candidate_id: 1,
        last_log_index: 0,
        last_log_term: 0,
    });

    use rustkv::raft::transport::TransportError;
    assert_eq!(
        t1.send(42, request.clone()).await,
        Err(TransportError::Unreachable(42)),
        "id missing from the peer map"
    );
    assert_eq!(
        t1.send(9, request.clone()).await,
        Err(TransportError::Timeout),
        "configured but dead peer"
    );

    // A listener whose raft node never answers (inbound receiver dropped)
    // must also surface as a timeout.
    let (_t3, addr3, rx3) = bind_transport(3, HashMap::new()).await;
    drop(rx3);
    let (t1b, _addr, _rx) = bind_transport(1, HashMap::from([(3, addr3)])).await;
    assert_eq!(t1b.send(3, request).await, Err(TransportError::Timeout));
}

// ---- connection pooling (phase 16) ----
//
// These need accept counts, which axum::serve doesn't expose, so they run
// against a hand-rolled counting server: an accept loop bumping a counter,
// each connection served in a task that reads framed requests in a loop
// and answers RequestVote with the request's own term (so every response
// is attributable to exactly one request).

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustkv::raft::transport::TransportError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn vote_request(term: u64) -> RpcRequest {
    RpcRequest::RequestVote(RequestVoteArgs {
        term,
        candidate_id: 1,
        last_log_index: 0,
        last_log_term: 0,
    })
}

fn vote_reply(term: u64) -> RpcResponse {
    RpcResponse::RequestVote(RequestVoteReply {
        term,
        vote_granted: true,
    })
}

/// Transport for node 1 with a single peer (id 2) at `peer_addr`. The
/// router/inbound side is unused — these tests exercise outbound only.
fn client_transport(peer_addr: String, rpc_timeout: Duration) -> HttpTransport {
    let (transport, _router, _inbound) =
        HttpTransport::new(1, HashMap::from([(2, peer_addr)]), rpc_timeout);
    transport
}

struct CountingServer {
    addr: String,
    accepted: Arc<AtomicUsize>,
    accept_task: tokio::task::JoinHandle<()>,
    conn_tasks: Arc<StdMutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl CountingServer {
    /// Aborts the accept loop and every connection task — client-visible
    /// as closed sockets, like a process crash. Frees the port.
    fn kill(&self) {
        self.accept_task.abort();
        for task in self.conn_tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

/// `delay`: (request ordinal counted across all connections, pause) —
/// postpones that one response so a client can time out while it is
/// pending.
async fn spawn_counting_server(bind: &str, delay: Option<(usize, Duration)>) -> CountingServer {
    // Retry the bind briefly: the restart-on-same-port test rebinds right
    // after kill() and the old socket may still be tearing down.
    let listener = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match TcpListener::bind(bind).await {
                Ok(listener) => break listener,
                Err(error) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "could not bind {bind}: {error}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    };
    let addr = listener.local_addr().unwrap().to_string();
    let accepted = Arc::new(AtomicUsize::new(0));
    let conn_tasks = Arc::new(StdMutex::new(Vec::new()));
    let served = Arc::new(AtomicUsize::new(0));

    let accept_task = tokio::spawn({
        let accepted = accepted.clone();
        let conn_tasks = conn_tasks.clone();
        async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                accepted.fetch_add(1, Ordering::SeqCst);
                let served = served.clone();
                let conn_task = tokio::spawn(async move {
                    while let Some(body) = read_framed_request(&mut stream).await {
                        let ordinal = served.fetch_add(1, Ordering::SeqCst);
                        if let Some((delayed_ordinal, pause)) = delay
                            && ordinal == delayed_ordinal
                        {
                            tokio::time::sleep(pause).await;
                        }
                        // The envelope is {"from":..,"request":{"RequestVote":{..}}}.
                        let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let term = envelope["request"]["RequestVote"]["term"].as_u64().unwrap();
                        let json = serde_json::to_vec(&vote_reply(term)).unwrap();
                        let head =
                            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", json.len());
                        if stream.write_all(head.as_bytes()).await.is_err()
                            || stream.write_all(&json).await.is_err()
                        {
                            break;
                        }
                    }
                });
                conn_tasks.lock().unwrap().push(conn_task);
            }
        }
    });
    CountingServer {
        addr,
        accepted,
        accept_task,
        conn_tasks,
    }
}

/// Reads one Content-Length-framed HTTP request body; None on EOF/error.
async fn read_framed_request(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    let header_end = loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
    let length: usize = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map(str::trim)?
        .parse()
        .ok()?;
    let mut body = buf.split_off(header_end + 4);
    while body.len() < length {
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    body.truncate(length);
    Some(body)
}

#[tokio::test]
async fn sequential_rpcs_reuse_a_pooled_connection() {
    let _serial = SERIAL.lock().await;
    let server = spawn_counting_server("127.0.0.1:0", None).await;
    let transport = client_transport(server.addr.clone(), RPC_TIMEOUT);

    for term in 1..=50u64 {
        let response = transport.send(2, vote_request(term)).await.unwrap();
        assert_eq!(response, vote_reply(term));
    }

    let accepted = server.accepted.load(Ordering::SeqCst);
    assert!(
        accepted <= 2,
        "50 sequential RPCs should reuse one pooled connection, saw {accepted} accepts"
    );
    server.kill();
}

#[tokio::test]
async fn stale_pooled_connection_recovers_on_a_fresh_retry() {
    let _serial = SERIAL.lock().await;
    let server = spawn_counting_server("127.0.0.1:0", None).await;
    let addr = server.addr.clone();
    let transport = client_transport(addr.clone(), RPC_TIMEOUT);

    // First RPC pools its connection; killing the server closes it while
    // it sits idle — the stale-idle race the retry exists for.
    assert_eq!(
        transport.send(2, vote_request(1)).await.unwrap(),
        vote_reply(1)
    );
    server.kill();
    let restarted = spawn_counting_server(&addr, None).await;

    let response = transport.send(2, vote_request(2)).await.unwrap();
    assert_eq!(response, vote_reply(2));
    assert_eq!(
        restarted.accepted.load(Ordering::SeqCst),
        1,
        "retry must arrive on a fresh connection to the restarted server"
    );
    restarted.kill();
}

#[tokio::test]
async fn parallel_rpcs_get_exclusive_connections_and_distinct_responses() {
    let _serial = SERIAL.lock().await;
    let server = spawn_counting_server("127.0.0.1:0", None).await;
    let transport = client_transport(server.addr.clone(), RPC_TIMEOUT);

    let mut joins = Vec::new();
    for term in 1..=8u64 {
        let transport = transport.clone();
        joins.push(tokio::spawn(async move {
            (term, transport.send(2, vote_request(term)).await)
        }));
    }
    for join in joins {
        let (term, response) = join.await.unwrap();
        assert_eq!(
            response.unwrap(),
            vote_reply(term),
            "parallel RPCs must not interleave responses"
        );
    }
    server.kill();
}

#[tokio::test]
async fn timed_out_connection_is_never_repooled() {
    let _serial = SERIAL.lock().await;
    // The SECOND request (ordinal 1) stalls past the client timeout, so it
    // times out on a POOLED connection with the response still pending.
    let stall = Duration::from_millis(600);
    let server = spawn_counting_server("127.0.0.1:0", Some((1, stall))).await;
    let transport = client_transport(server.addr.clone(), Duration::from_millis(200));

    // Pool a connection, then time out on it.
    assert_eq!(
        transport.send(2, vote_request(7)).await.unwrap(),
        vote_reply(7)
    );
    assert_eq!(
        transport.send(2, vote_request(8)).await,
        Err(TransportError::Timeout)
    );

    // Let the stalled term-8 response get written (to a dropped socket).
    // If the timed-out stream had been repooled, the next RPC would read
    // that stale response instead of its own.
    tokio::time::sleep(stall).await;
    let response = transport.send(2, vote_request(9)).await.unwrap();
    assert_eq!(response, vote_reply(9));
    assert_eq!(
        server.accepted.load(Ordering::SeqCst),
        2,
        "the RPC after the timeout must open a fresh connection"
    );
    server.kill();
}

/// Phase-15/16 interaction, previously executed but unasserted: `set_peers`
/// must prune idle pooled connections to addresses that left the book —
/// otherwise sockets to removed or re-addressed members would linger until
/// process exit. The drop is observed server-side (EOF ends the connection
/// task) and client-side (the next RPC after re-adding the address opens a
/// fresh connection instead of resurrecting the pruned one).
#[tokio::test]
async fn set_peers_prunes_pooled_connections_to_departed_addresses() {
    let _serial = SERIAL.lock().await;
    let server = spawn_counting_server("127.0.0.1:0", None).await;
    let transport = client_transport(server.addr.clone(), RPC_TIMEOUT);

    assert_eq!(
        transport.send(2, vote_request(1)).await.unwrap(),
        vote_reply(1)
    );
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);

    // Membership change: node 2 leaves. Its pooled socket must be closed.
    transport.set_peers(HashMap::new());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !server
        .conn_tasks
        .lock()
        .unwrap()
        .iter()
        .all(tokio::task::JoinHandle::is_finished)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "pruned pooled socket was never closed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        transport.send(2, vote_request(2)).await,
        Err(TransportError::Unreachable(2)),
        "departed peer is unreachable"
    );

    // Node 2 rejoins at the same address: the pool entry is gone, so the
    // next RPC must open a fresh connection.
    transport.set_peers(HashMap::from([(2, server.addr.clone())]));
    assert_eq!(
        transport.send(2, vote_request(3)).await.unwrap(),
        vote_reply(3)
    );
    assert_eq!(
        server.accepted.load(Ordering::SeqCst),
        2,
        "the RPC after the prune must use a fresh connection"
    );
    server.kill();
}

/// MAX_IDLE_PER_PEER pinned end-to-end (previously a code comment only): a
/// parallel burst opens one connection per RPC, but only 4 survive checkin;
/// the excess are dropped, observable as server-side EOFs on exactly
/// `accepted - 4` connections.
#[tokio::test]
async fn idle_pool_cap_drops_excess_connections_after_a_burst() {
    let _serial = SERIAL.lock().await;
    let server = spawn_counting_server("127.0.0.1:0", None).await;
    let transport = client_transport(server.addr.clone(), RPC_TIMEOUT);

    let mut joins = Vec::new();
    for term in 1..=8u64 {
        let transport = transport.clone();
        joins.push(tokio::spawn(async move {
            transport.send(2, vote_request(term)).await
        }));
    }
    for join in joins {
        join.await.unwrap().unwrap();
    }
    let accepted = server.accepted.load(Ordering::SeqCst);
    assert!(
        accepted > 4,
        "burst opened only {accepted} connections — too few to exercise the cap"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let finished = server
            .conn_tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|t| t.is_finished())
            .count();
        if finished == accepted - 4 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {} connections dropped by the idle cap, saw {finished}",
            accepted - 4
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    server.kill();
}

/// A raw server answering every framed request on every connection with the
/// same canned bytes — for probing how the pool treats responses with
/// nonstandard framing (excess bytes, `Connection: close`).
async fn spawn_canned_server(raw_response: Vec<u8>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accepted = Arc::new(AtomicUsize::new(0));
    tokio::spawn({
        let accepted = accepted.clone();
        async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                accepted.fetch_add(1, Ordering::SeqCst);
                let response = raw_response.clone();
                tokio::spawn(async move {
                    while read_framed_request(&mut stream).await.is_some() {
                        if stream.write_all(&response).await.is_err() {
                            break;
                        }
                    }
                });
            }
        }
    });
    (addr, accepted)
}

/// Phase-16 gap closure: bytes beyond the declared Content-Length leave a
/// connection in an unknowable state. The RPC itself succeeds (the body is
/// truncated to the declared length) but the connection is poisoned — the
/// next RPC must not read leftover junk, and completes on a fresh
/// connection (directly if the poisoned one was never repooled, via the
/// one fresh retry if the junk raced past the excess-bytes check).
#[tokio::test]
async fn excess_body_bytes_poison_the_connection_for_reuse() {
    let _serial = SERIAL.lock().await;
    let body = serde_json::to_vec(&vote_reply(1)).unwrap();
    let mut response =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
    response.extend_from_slice(&body);
    response.extend_from_slice(b"JUNKJUNK");
    let (addr, accepted) = spawn_canned_server(response).await;
    let transport = client_transport(addr, RPC_TIMEOUT);

    assert_eq!(
        transport.send(2, vote_request(1)).await.unwrap(),
        vote_reply(1),
        "body must be truncated to the declared length"
    );
    assert_eq!(
        transport.send(2, vote_request(1)).await.unwrap(),
        vote_reply(1),
        "the junk must never surface in a later RPC's response"
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        2,
        "the second RPC must complete on a fresh connection"
    );
}

/// The server's keep-alive opt-out is honored end-to-end (previously pinned
/// only at the parse level): a correctly framed 200 carrying
/// `Connection: close` is consumed but never repooled.
#[tokio::test]
async fn server_connection_close_opt_out_is_never_repooled() {
    let _serial = SERIAL.lock().await;
    let body = serde_json::to_vec(&vote_reply(1)).unwrap();
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(&body);
    let (addr, accepted) = spawn_canned_server(response).await;
    let transport = client_transport(addr, RPC_TIMEOUT);

    assert_eq!(
        transport.send(2, vote_request(1)).await.unwrap(),
        vote_reply(1)
    );
    assert_eq!(
        transport.send(2, vote_request(1)).await.unwrap(),
        vote_reply(1)
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        2,
        "a Connection: close response must not be repooled"
    );
}

// ---- a real 3-node cluster over the HTTP transport, in-process ----

struct RealNode {
    id: NodeId,
    raft: RaftHandle,
    store: Arc<KvStore>,
    _dir: TempDir,
}

async fn spawn_real_cluster(n: u64) -> Vec<RealNode> {
    // Bind all raft listeners first so every transport knows every address.
    let mut listeners = Vec::new();
    let mut addrs: HashMap<NodeId, String> = HashMap::new();
    for id in 1..=n {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.insert(id, listener.local_addr().unwrap().to_string());
        listeners.push((id, listener));
    }

    let mut nodes = Vec::new();
    for (id, listener) in listeners {
        let peers: HashMap<NodeId, String> = addrs
            .iter()
            .filter(|(pid, _)| **pid != id)
            .map(|(pid, addr)| (*pid, addr.clone()))
            .collect();
        let (transport, router, inbound) = HttpTransport::new(id, peers, RPC_TIMEOUT);
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(KvStore::new());
        let mut config = node_config(id, n, 71);
        config.election_timeout_min = Duration::from_millis(200);
        config.election_timeout_max = Duration::from_millis(400);
        config.heartbeat_interval = Duration::from_millis(50);
        let raft = RaftNode::spawn(
            config,
            Storage::open(dir.path()).unwrap(),
            transport,
            inbound,
            store.clone() as Arc<dyn StateMachine>,
        );
        nodes.push(RealNode {
            id,
            raft,
            store,
            _dir: dir,
        });
    }
    nodes
}

async fn wait_for_leader(nodes: &[RealNode], among: &[NodeId]) -> NodeId {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leaders: Vec<NodeId> = nodes
            .iter()
            .filter(|n| among.contains(&n.id) && n.raft.status().role == RoleKind::Leader)
            .map(|n| n.id)
            .collect();
        if let [leader] = leaders[..] {
            return leader;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader over the real transport within 15s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn cluster_over_real_http_transport_elects_replicates_and_survives_leader_crash() {
    let _serial = SERIAL.lock().await;
    let nodes = spawn_real_cluster(3).await;
    let all: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();

    // Elect and commit a write.
    let leader = wait_for_leader(&nodes, &all).await;
    let handle = &nodes.iter().find(|n| n.id == leader).unwrap().raft;
    let proposal = handle.propose(put("k1", 1)).await.unwrap();
    assert_eq!(proposal.committed.await, Ok(true));

    // Applied on every node's state machine, over real sockets.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if nodes.iter().all(|n| n.store.get("k1").is_some()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "write did not propagate to all nodes"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Crash the leader; the survivors elect a successor and keep committing.
    nodes.iter().find(|n| n.id == leader).unwrap().raft.crash();
    let survivors: Vec<NodeId> = all.iter().copied().filter(|&id| id != leader).collect();
    let new_leader = wait_for_leader(&nodes, &survivors).await;
    let handle = &nodes.iter().find(|n| n.id == new_leader).unwrap().raft;
    let proposal = handle.propose(put("k2", 2)).await.unwrap();
    assert_eq!(proposal.committed.await, Ok(true));

    for node in nodes.iter().filter(|n| survivors.contains(&n.id)) {
        assert_eq!(node.store.get("k1"), Some(serde_json::json!(1)));
    }
    for (_, handle) in nodes.iter().map(|n| (n.id, &n.raft)) {
        handle.shutdown();
    }
}

// ---- phase 20b: the size-aware RPC timeout ----

/// A raft peer that answers every RPC correctly but only after `pause` —
/// the deterministic stand-in for a slow link: the transport maps slow
/// peers and slow networks to the same thing by design, and real loopback
/// speed would make any transfer-time-based leg machine-dependent.
async fn spawn_slow_server(pause: Duration) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accept_task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                while let Some(body) = read_framed_request(&mut stream).await {
                    tokio::time::sleep(pause).await;
                    let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let response =
                        if let Some(term) = envelope["request"]["AppendEntries"]["term"].as_u64() {
                            serde_json::to_vec(&RpcResponse::AppendEntries(
                                rustkv::raft::rpc::AppendEntriesReply {
                                    term,
                                    success: true,
                                },
                            ))
                            .unwrap()
                        } else {
                            let term = envelope["request"]["RequestVote"]["term"].as_u64().unwrap();
                            serde_json::to_vec(&vote_reply(term)).unwrap()
                        };
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                        response.len()
                    );
                    if stream.write_all(head.as_bytes()).await.is_err()
                        || stream.write_all(&response).await.is_err()
                    {
                        break;
                    }
                }
            });
        }
    });
    (addr, accept_task)
}

/// Phase 20b inversion, on real sockets against a slow peer (300ms to
/// answer anything). Disease first: a catch-up-sized payload under the
/// flat (pre-phase-20, constructor-default) timeout at a heartbeat-scale
/// 50ms budget times out — and every retry would be the identical doomed
/// RPC (the FAILURE_MODES.md "bandwidth × timeout" gap). Inverted: the
/// same payload against the same base budget goes through once the
/// transport grows the budget by the body's transfer time at the assumed
/// bandwidth (1 MiB body at 1 MiB/s = ~+1s > the peer's 300ms). The
/// small-RPC leg then proves the growth keys on BODY SIZE, not on
/// configuration: the same size-aware transport gives a small RPC exactly
/// the tight base budget, timing out long before the slow peer answers.
#[tokio::test]
async fn size_aware_timeout_lets_big_payloads_through_and_keeps_small_ones_tight() {
    let _serial = SERIAL.lock().await;
    let base = Duration::from_millis(50);
    let pause = Duration::from_millis(300);
    let bandwidth = 1024 * 1024;
    let (addr, accept_task) = spawn_slow_server(pause).await;

    let big_request = || {
        RpcRequest::AppendEntries(rustkv::raft::rpc::AppendEntriesArgs {
            term: 7,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![rustkv::raft::types::LogEntry {
                term: 7,
                index: 1,
                command: rustkv::raft::types::Command::Put {
                    key: "big".to_string(),
                    value: serde_json::Value::String("x".repeat(1024 * 1024)),
                    session: None,
                },
            }],
            leader_commit: 0,
        })
    };

    // The disease: flat timeout (the constructor default) — the payload
    // can never be delivered within the heartbeat-scale budget.
    let flat = client_transport(addr.clone(), base);
    assert_eq!(
        flat.send(2, big_request()).await,
        Err(TransportError::Timeout),
        "a slow-peer payload must not fit the flat base budget"
    );

    // Inverted: same base budget, size-aware — the 1 MiB body earns ~1s.
    let aware = client_transport(addr, base).with_assumed_bandwidth(Some(bandwidth));
    assert_eq!(
        aware.send(2, big_request()).await,
        Ok(RpcResponse::AppendEntries(
            rustkv::raft::rpc::AppendEntriesReply {
                term: 7,
                success: true
            }
        )),
        "the same payload against the same base budget must fit once the \
         timeout is size-aware"
    );

    // Small RPCs keep tight failure detection on the SAME transport: a
    // vote times out at the base budget, well before the peer's 300ms
    // answer — the budget grew for the body above, not for the config.
    let start = std::time::Instant::now();
    assert_eq!(
        aware.send(2, vote_request(1)).await,
        Err(TransportError::Timeout),
        "a small RPC against the slow peer must still time out at base"
    );
    assert!(
        start.elapsed() < pause,
        "a small RPC's budget must stay at the flat base ({:?} elapsed)",
        start.elapsed()
    );
    accept_task.abort();
}
