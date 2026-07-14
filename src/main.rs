//! Thin binary shell: env config, tracing setup, two axum servers (client
//! API + raft RPC). All logic lives in the `rustkv` library.
//!
//! Runs one cluster member. With no RUSTKV_PEERS it is a single-node
//! cluster; with peers configured it forms a real multi-node cluster over
//! the HTTP transport; with RUSTKV_JOIN=1 it starts as a joiner waiting to
//! be added via the admin API (phase 15). See src/config.rs for all
//! variables, and scripts/run-cluster.sh for a ready-made local 3-node
//! setup.
//!
//! Dynamic membership wiring (phase 15): the Raft core publishes the
//! in-effect membership (with addresses) on a watch channel; a task here
//! folds every change into the HTTP transport's peer address book and the
//! API layer's redirect URLs. The core itself never touches the network.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rustkv::api::{self, ApiContext};
use rustkv::config::NodeConfig;
use rustkv::kv::KvNode;
use rustkv::raft::node::{RaftConfig, RaftNode, StateMachine};
use rustkv::raft::storage::Storage;
use rustkv::raft::transport::http::HttpTransport;
use rustkv::raft::types::{MemberAddr, NodeId};
use rustkv::store::KvStore;
use tracing_subscriber::EnvFilter;

const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = NodeConfig::from_env().unwrap_or_else(|error| panic!("bad config: {error}"));
    let store = Arc::new(KvStore::new());
    let storage = Storage::open(&config.data_dir)
        .unwrap_or_else(|error| panic!("cannot open data dir {}: {error}", config.data_dir));

    let peer_ids: Vec<_> = {
        let mut ids: Vec<_> = config.peers.keys().copied().collect();
        ids.sort_unstable();
        ids
    };
    let (transport, raft_router, inbound) = HttpTransport::new(
        config.id,
        config.peers.clone(),
        Duration::from_millis(config.rpc_timeout_ms),
    );
    let transport = transport.with_assumed_bandwidth(config.assumed_bandwidth);
    let mut raft_config = RaftConfig::new(config.id, peer_ids);
    raft_config.snapshot_threshold = config.snapshot_threshold;
    raft_config.snapshot_trailing = config.snapshot_trailing;
    raft_config.max_append_bytes = config.max_append_bytes;
    raft_config.snapshot_chunk_bytes = config.snapshot_chunk_bytes;
    raft_config.join = config.join;
    // Address book for the bootstrap membership. Self uses its ADVERTISE
    // addresses (default: the listen addresses): whatever goes in here is
    // what a future ConfigChange embeds in the log and hands the whole
    // cluster — a 0.0.0.0 bind must never leak into that (phase-15
    // amendment; RUSTKV_ADVERTISE_* in config.rs).
    let mut bootstrap_addrs: BTreeMap<NodeId, MemberAddr> = config
        .peers
        .iter()
        .map(|(&id, raft_addr)| {
            (
                id,
                MemberAddr {
                    raft: raft_addr.clone(),
                    client: config
                        .peer_client_urls
                        .get(&id)
                        .cloned()
                        .unwrap_or_default(),
                },
            )
        })
        .collect();
    bootstrap_addrs.insert(
        config.id,
        MemberAddr {
            raft: config.advertise_raft_addr.clone(),
            client: config.advertise_client_url.clone(),
        },
    );
    raft_config.bootstrap_addrs = bootstrap_addrs;
    let raft = RaftNode::spawn(
        raft_config,
        storage,
        transport.clone(),
        inbound,
        store.clone() as Arc<dyn StateMachine>,
    );
    let mut membership_rx = raft.membership_watch();
    let kv = KvNode::new(store, raft, WRITE_TIMEOUT);
    let peer_urls = Arc::new(RwLock::new(config.peer_client_urls.clone()));
    let ctx = Arc::new(ApiContext {
        kv,
        peer_urls: peer_urls.clone(),
    });

    // Fold membership changes into the transport's address book and the
    // API's redirect map (phase 15). Runs once immediately (idempotent for
    // the bootstrap config), then on every change. The raft address book
    // keeps DEPARTING peers too (phase 19b): a removed peer is owed one
    // last replication of its own removal entry, and dropping its address
    // before that ack would make the parting sends unreachable — the core
    // withdraws it from the view once the peer has acked (or on
    // step-down). Client redirect URLs stay members-only: a removed peer
    // is not a place to send clients.
    let self_id = config.id;
    tokio::spawn(async move {
        loop {
            let view = membership_rx.borrow_and_update().clone();
            let raft_addrs: HashMap<NodeId, String> = view
                .members
                .iter()
                .chain(view.departing.iter())
                .filter(|(id, addr)| **id != self_id && !addr.raft.is_empty())
                .map(|(id, addr)| (*id, addr.raft.clone()))
                .collect();
            transport.set_peers(raft_addrs);
            let client_urls: HashMap<NodeId, String> = view
                .members
                .iter()
                .filter(|(id, addr)| **id != self_id && !addr.client.is_empty())
                .map(|(id, addr)| (*id, addr.client.clone()))
                .collect();
            *peer_urls.write().expect("peer_urls lock poisoned") = client_urls;
            if membership_rx.changed().await.is_err() {
                break; // raft node stopped
            }
        }
    });

    let raft_listener = tokio::net::TcpListener::bind(&config.raft_listen)
        .await
        .unwrap_or_else(|error| panic!("failed to bind raft {}: {error}", config.raft_listen));
    let client_listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .unwrap_or_else(|error| panic!("failed to bind {}: {error}", config.listen));
    tracing::info!(
        node = config.id,
        client_addr = %config.listen,
        raft_addr = %config.raft_listen,
        data_dir = %config.data_dir,
        peers = config.peers.len(),
        "rustkv listening"
    );

    tokio::spawn(async move {
        axum::serve(raft_listener, raft_router)
            .await
            .expect("raft server error");
    });
    axum::serve(client_listener, api::router(ctx))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install ctrl-c handler");
    tracing::info!("shutdown signal received");
}
