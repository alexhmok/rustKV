//! Thin binary shell: config from the environment, tracing setup, axum server.
//! All logic lives in the `rustkv` library so it can be tested in-process.

use std::sync::Arc;

use rustkv::api;
use rustkv::store::KvStore;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // No CLI-parsing crate is on the dependency whitelist, so config is a
    // positional arg or an env var: `rustkv [listen-addr]` / `RUSTKV_LISTEN`.
    let listen = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("RUSTKV_LISTEN").ok())
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let store = Arc::new(KvStore::new());
    let app = api::router(store);

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .unwrap_or_else(|error| panic!("failed to bind {listen}: {error}"));
    tracing::info!(addr = %listen, "rustkv listening");

    axum::serve(listener, app)
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
