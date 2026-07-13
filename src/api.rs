//! Client-facing HTTP API.
//!
//! Contract:
//! - `GET /{key}`    -> 200 with the stored JSON, 404 if absent
//! - `PUT /{key}`    -> 201 on write (create or overwrite); 400 if the body is not valid JSON
//! - `DELETE /{key}` -> 204 (idempotent: also 204 if the key was absent)
//!
//! From phase 5 on, PUT/DELETE go through the Raft log instead of mutating the
//! store directly, and non-leaders forward/redirect to the leader.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use serde_json::Value;

use crate::store::KvStore;

/// Builds the client API router over a shared store.
pub fn router(store: Arc<KvStore>) -> Router {
    Router::new()
        .route("/{key}", get(get_key).put(put_key).delete(delete_key))
        .with_state(store)
}

async fn get_key(State(store): State<Arc<KvStore>>, Path(key): Path<String>) -> Response {
    match store.get(&key) {
        Some(value) => {
            tracing::debug!(key, "get: hit");
            (StatusCode::OK, Json(value)).into_response()
        }
        None => {
            tracing::debug!(key, "get: miss");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

// The body is parsed manually (rather than via `axum::Json`) so that requests
// without a `Content-Type: application/json` header are still accepted; the
// contract only requires the body to be valid JSON.
async fn put_key(
    State(store): State<Arc<KvStore>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Response {
    match serde_json::from_slice::<Value>(&body) {
        Ok(value) => {
            tracing::info!(key, "put");
            store.put(key, value);
            StatusCode::CREATED.into_response()
        }
        Err(error) => {
            tracing::warn!(key, %error, "put rejected: body is not valid JSON");
            (StatusCode::BAD_REQUEST, "body must be valid JSON\n").into_response()
        }
    }
}

async fn delete_key(State(store): State<Arc<KvStore>>, Path(key): Path<String>) -> StatusCode {
    let existed = store.delete(&key);
    tracing::info!(key, existed, "delete");
    StatusCode::NO_CONTENT
}
