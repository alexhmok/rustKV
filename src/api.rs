//! Client-facing HTTP API, backed by the replicated log.
//!
//! Contract:
//! - `GET /{key}`    -> 200 with the stored JSON, 404 if absent. Served from
//!   the local state machine — may be stale on followers (documented).
//! - `PUT /{key}`    -> 201 once the write is committed by a majority;
//!   400 if the body is not valid JSON.
//! - `DELETE /{key}` -> 204 once the delete is committed (idempotent).
//!
//! Non-leaders answer writes with `307 Temporary Redirect` to the leader's
//! client URL when it is known (the brief allows forward-or-redirect;
//! redirect keeps this layer stateless), else `503`. A write that cannot
//! reach majority commit within the node's timeout returns `504` — the
//! outcome is unknown and the client must verify before assuming either way.
//! `503` responses are always safe to retry.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use serde_json::Value;

use crate::kv::{KvNode, WriteError};
use crate::raft::types::{Command, NodeId};

/// Everything a handler needs: the local KV service and, for redirects, the
/// client-facing base URL of each peer (empty for a single-node deployment;
/// populated from cluster config in phase 7).
pub struct ApiContext {
    pub kv: Arc<KvNode>,
    pub peer_urls: HashMap<NodeId, String>,
}

pub fn router(ctx: Arc<ApiContext>) -> Router {
    Router::new()
        .route("/{key}", get(get_key).put(put_key).delete(delete_key))
        .with_state(ctx)
}

async fn get_key(State(ctx): State<Arc<ApiContext>>, Path(key): Path<String>) -> Response {
    match ctx.kv.get(&key) {
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
    State(ctx): State<Arc<ApiContext>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Response {
    let value = match serde_json::from_slice::<Value>(&body) {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(key, %error, "put rejected: body is not valid JSON");
            return (StatusCode::BAD_REQUEST, "body must be valid JSON\n").into_response();
        }
    };
    match ctx
        .kv
        .write(Command::Put {
            key: key.clone(),
            value,
        })
        .await
    {
        Ok(()) => {
            tracing::info!(key, "put committed");
            StatusCode::CREATED.into_response()
        }
        Err(error) => write_error_response(&ctx, &key, error),
    }
}

async fn delete_key(State(ctx): State<Arc<ApiContext>>, Path(key): Path<String>) -> Response {
    match ctx.kv.write(Command::Delete { key: key.clone() }).await {
        Ok(()) => {
            tracing::info!(key, "delete committed");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => write_error_response(&ctx, &key, error),
    }
}

fn write_error_response(ctx: &ApiContext, key: &str, error: WriteError) -> Response {
    tracing::debug!(key, %error, "write not served locally");
    match error {
        WriteError::NotLeader {
            leader_hint: Some(leader),
        } if ctx.peer_urls.contains_key(&leader) => {
            // NOTE: the key is embedded as-is; exotic characters that need
            // re-encoding are out of scope for now.
            let location = format!("{}/{key}", ctx.peer_urls[&leader]);
            (
                StatusCode::TEMPORARY_REDIRECT,
                [(header::LOCATION, location)],
            )
                .into_response()
        }
        WriteError::NotLeader { .. } => (
            StatusCode::SERVICE_UNAVAILABLE,
            "no leader known; retry shortly\n",
        )
            .into_response(),
        WriteError::Timeout => (
            StatusCode::GATEWAY_TIMEOUT,
            "write not confirmed: this node cannot reach a majority; \
             the write may or may not commit later\n",
        )
            .into_response(),
        WriteError::Superseded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "leadership changed before the write committed; safe to retry\n",
        )
            .into_response(),
        WriteError::Shutdown => {
            (StatusCode::SERVICE_UNAVAILABLE, "node shutting down\n").into_response()
        }
    }
}
