//! Thin binary shell: config from the environment, tracing setup, axum server.
//! All logic lives in the `rustkv` library so it can be tested in-process.
//!
//! Currently runs a single-node Raft cluster: every write goes through the
//! persisted log (fsync before ack) and the KV state is rebuilt from the log
//! on startup. Phase 7 adds the HTTP node-to-node transport and multi-node
//! cluster config; until then the transport is an inert simulator handle.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rustkv::api::{self, ApiContext};
use rustkv::kv::KvNode;
use rustkv::raft::node::{RaftConfig, RaftNode};
use rustkv::raft::storage::Storage;
use rustkv::raft::transport::sim::{FaultConfig, SimNetwork};
use rustkv::store::KvStore;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // No CLI-parsing crate is on the dependency whitelist, so config is a
    // positional arg or env vars: `rustkv [listen-addr]`, RUSTKV_LISTEN,
    // RUSTKV_DATA_DIR.
    let listen = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("RUSTKV_LISTEN").ok())
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let data_dir = std::env::var("RUSTKV_DATA_DIR").unwrap_or_else(|_| "./rustkv-data".to_string());

    let store = Arc::new(KvStore::new());
    let storage = Storage::open(&data_dir)
        .unwrap_or_else(|error| panic!("cannot open data dir {data_dir}: {error}"));

    // Single-node cluster: the simulated transport is never used (no peers)
    // but satisfies the wiring until the HTTP transport lands in phase 7.
    let net = SimNetwork::new(0, FaultConfig::default());
    let (transport, inbound) = net.register(1);
    let raft = RaftNode::spawn(
        RaftConfig::new(1, Vec::new()),
        storage,
        transport,
        inbound,
        {
            let sm: Arc<dyn rustkv::raft::node::StateMachine> = store.clone();
            sm
        },
    );
    let kv = KvNode::new(store, raft, Duration::from_secs(5));
    let ctx = Arc::new(ApiContext {
        kv,
        peer_urls: HashMap::new(),
    });

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|error| panic!("failed to bind {listen}: {error}"));
    tracing::info!(addr = %listen, data_dir, "rustkv listening (single-node cluster)");

    axum::serve(listener, api::router(ctx))
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
