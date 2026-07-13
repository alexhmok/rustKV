//! Tests of the real HTTP node-to-node transport: the transport in
//! isolation, then a full in-process 3-node cluster where BOTH the client
//! API and raft RPCs run over real sockets.
//!
//! Covered: RPC roundtrip over HTTP, unreachable-vs-timeout semantics
//! (unknown id, dead peer, black-holed listener), and a real-transport
//! cluster electing a leader, committing writes everywhere, and surviving
//! a leader crash.
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
