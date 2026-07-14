//! Thin binary shell: env config, tracing setup, two axum servers (client
//! API + raft RPC). All logic lives in the `rustkv` library.
//!
//! Runs one cluster member. With no RUSTKV_PEERS it is a single-node
//! cluster; with peers configured it forms a real multi-node cluster over
//! the HTTP transport. See src/config.rs for all variables, and
//! scripts/run-cluster.sh for a ready-made local 3-node setup.

use std::sync::Arc;
use std::time::Duration;

use rustkv::api::{self, ApiContext};
use rustkv::config::NodeConfig;
use rustkv::kv::KvNode;
use rustkv::raft::node::{RaftConfig, RaftNode, StateMachine};
use rustkv::raft::storage::Storage;
use rustkv::raft::transport::http::HttpTransport;
use rustkv::store::KvStore;
use tracing_subscriber::EnvFilter;

const RPC_TIMEOUT: Duration = Duration::from_millis(150);
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
    let (transport, raft_router, inbound) =
        HttpTransport::new(config.id, config.peers.clone(), RPC_TIMEOUT);
    let mut raft_config = RaftConfig::new(config.id, peer_ids);
    raft_config.snapshot_threshold = config.snapshot_threshold;
    raft_config.snapshot_trailing = config.snapshot_trailing;
    let raft = RaftNode::spawn(
        raft_config,
        storage,
        transport,
        inbound,
        store.clone() as Arc<dyn StateMachine>,
    );
    let kv = KvNode::new(store, raft, WRITE_TIMEOUT);
    let ctx = Arc::new(ApiContext {
        kv,
        peer_urls: config.peer_client_urls.clone(),
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
