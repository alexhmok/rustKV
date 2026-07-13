//! Client-facing HTTP API, backed by the replicated log.
//!
//! Contract:
//! - `GET /{key}`    -> 200 with the stored JSON, 404 if absent.
//!   Linearizable by default (ReadIndex, phase 9): served by the leader
//!   after confirming leadership against a majority, so never stale.
//!   `GET /{key}?stale=true` skips the confirmation and reads the local
//!   state machine — fast, but may be stale on followers (the pre-phase-9
//!   behavior, kept as an explicit opt-in).
//! - `PUT /{key}`    -> 201 once the write is committed by a majority;
//!   400 if the body is not valid JSON.
//! - `DELETE /{key}` -> 204 once the delete is committed (idempotent).
//! - `GET /cluster/status` -> 200 with this node's raft status (id, term,
//!   role, leader, commit index). Under `/cluster/` so no single-segment
//!   key is shadowed.
//!
//! Non-leaders answer writes and linearizable reads with `307 Temporary
//! Redirect` to the leader's client URL when it is known (the brief allows
//! forward-or-redirect; redirect keeps this layer stateless), else `503`.
//! An operation that cannot confirm majority contact within the node's
//! timeout returns `504` — for writes the outcome is unknown and the client
//! must verify before assuming either way; a timed-out read simply didn't
//! happen. `503` responses are always safe to retry.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kv::{KvNode, ReadError, WriteError};
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
        .route("/cluster/status", get(get_status))
        .with_state(ctx)
}

#[derive(Deserialize)]
struct GetParams {
    #[serde(default)]
    stale: bool,
}

async fn get_key(
    State(ctx): State<Arc<ApiContext>>,
    Path(key): Path<String>,
    Query(params): Query<GetParams>,
) -> Response {
    if params.stale {
        return match ctx.kv.get(&key) {
            Some(value) => {
                tracing::debug!(key, "stale get: hit");
                (StatusCode::OK, Json(value)).into_response()
            }
            None => {
                tracing::debug!(key, "stale get: miss");
                StatusCode::NOT_FOUND.into_response()
            }
        };
    }
    match ctx.kv.get_linearizable(&key).await {
        Ok(Some(value)) => {
            tracing::debug!(key, "get: hit");
            (StatusCode::OK, Json(value)).into_response()
        }
        Ok(None) => {
            tracing::debug!(key, "get: miss");
            StatusCode::NOT_FOUND.into_response()
        }
        Err(error) => read_error_response(&ctx, &key, error),
    }
}

/// This node's raft status, for tests, scripts, and operators.
#[derive(Serialize)]
struct StatusBody {
    id: NodeId,
    term: u64,
    role: String,
    leader_id: Option<NodeId>,
    commit_index: u64,
    last_log_index: u64,
}

async fn get_status(State(ctx): State<Arc<ApiContext>>) -> Response {
    let status = ctx.kv.status();
    Json(StatusBody {
        id: status.id,
        term: status.term,
        role: format!("{:?}", status.role),
        leader_id: status.leader_id,
        commit_index: status.commit_index,
        last_log_index: status.last_log_index,
    })
    .into_response()
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

/// 307 to the leader's client URL, if we know who and where it is.
fn redirect_to_leader(
    ctx: &ApiContext,
    key: &str,
    leader_hint: Option<NodeId>,
) -> Option<Response> {
    let leader = leader_hint?;
    let base = ctx.peer_urls.get(&leader)?;
    // NOTE: the key is embedded as-is; exotic characters that need
    // re-encoding are out of scope for now.
    let location = format!("{base}/{key}");
    Some(
        (
            StatusCode::TEMPORARY_REDIRECT,
            [(header::LOCATION, location)],
        )
            .into_response(),
    )
}

fn write_error_response(ctx: &ApiContext, key: &str, error: WriteError) -> Response {
    tracing::debug!(key, %error, "write not served locally");
    match error {
        WriteError::NotLeader { leader_hint } => match redirect_to_leader(ctx, key, leader_hint) {
            Some(redirect) => redirect,
            None => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no leader known; retry shortly\n",
            )
                .into_response(),
        },
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

fn read_error_response(ctx: &ApiContext, key: &str, error: ReadError) -> Response {
    tracing::debug!(key, %error, "read not served locally");
    match error {
        ReadError::NotLeader { leader_hint } => match redirect_to_leader(ctx, key, leader_hint) {
            Some(redirect) => redirect,
            None => (
                StatusCode::SERVICE_UNAVAILABLE,
                "no leader known; retry shortly\n",
            )
                .into_response(),
        },
        ReadError::Timeout => (
            StatusCode::GATEWAY_TIMEOUT,
            "read not confirmed: this node cannot reach a majority; \
             retry (or use ?stale=true for a possibly-stale local read)\n",
        )
            .into_response(),
        ReadError::Retry => (
            StatusCode::SERVICE_UNAVAILABLE,
            "leadership changed during the read; safe to retry\n",
        )
            .into_response(),
        ReadError::Shutdown => {
            (StatusCode::SERVICE_UNAVAILABLE, "node shutting down\n").into_response()
        }
    }
}
