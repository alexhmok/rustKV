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
//! Dedup tokens (phase 13): writes may carry `X-Client-Id` and
//! `X-Client-Seq` headers (both u64). Both-or-neither — anything else is
//! `400`. With a token, retrying the byte-same request after a `504` is
//! safe: the retry may commit a second log entry, but the state machine
//! applies each (client, seq) at most once. Without them, writes keep the
//! at-least-once semantics below, byte-identical to before. Seqs must be
//! strictly increasing per client, with at most
//! [`crate::store::SESSION_WINDOW`] ops outstanding at a time (ops may be
//! pipelined; dedup matches exact seqs over that sliding window).
//!
//! Cluster admin (phase 15, dynamic membership):
//! - `GET /cluster/members`         -> 200 with the in-effect membership
//!   (this node's view; served locally, like `/cluster/status`).
//! - `PUT /cluster/members/{id}`    -> 201 once the ConfigChange adding the
//!   member is committed; body `{"raft": "host:port", "client": "http://…"}`.
//! - `DELETE /cluster/members/{id}` -> 204 once the removal is committed;
//!   404 if the id is not a member.
//!   Invalid changes (not a single-server delta, another change in flight,
//!   the leader's no-op not yet committed) -> `409` with the reason.
//!   Non-leaders redirect both, like writes.
//!
//! Non-leaders answer writes and linearizable reads with `307 Temporary
//! Redirect` to the leader's client URL when it is known (the brief allows
//! forward-or-redirect; redirect keeps this layer stateless), else `503`.
//! An operation that cannot confirm majority contact within the node's
//! timeout returns `504` — for writes the outcome is unknown and the client
//! must verify before assuming either way; a timed-out read simply didn't
//! happen. `503` responses are always safe to retry.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kv::{KvNode, ReadError, WriteError};
use crate::raft::types::{Command, MemberAddr, NodeId, Session};

/// Everything a handler needs: the local KV service and, for redirects, the
/// client-facing base URL of each peer (empty for a single-node deployment;
/// bootstrapped from cluster config in phase 7). Behind a lock since
/// phase 15: membership changes update it at runtime (main.rs follows the
/// Raft core's membership watch). Never held across an `.await`.
pub struct ApiContext {
    pub kv: Arc<KvNode>,
    pub peer_urls: Arc<RwLock<HashMap<NodeId, String>>>,
}

pub fn router(ctx: Arc<ApiContext>) -> Router {
    Router::new()
        .route("/{key}", get(get_key).put(put_key).delete(delete_key))
        .route("/cluster/status", get(get_status))
        .route("/cluster/members", get(list_members))
        .route(
            "/cluster/members/{id}",
            axum::routing::put(put_member).delete(delete_member),
        )
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

const CLIENT_ID_HEADER: &str = "x-client-id";
const CLIENT_SEQ_HEADER: &str = "x-client-seq";

/// Parses the optional dedup token headers: both-or-neither, each a u64.
/// The error is the 400 body text; callers wrap it into a response.
fn session_from_headers(key: &str, headers: &HeaderMap) -> Result<Option<Session>, &'static str> {
    let parse = |name: &str| {
        headers
            .get(name)
            .map(|v| v.to_str().ok().and_then(|s| s.parse::<u64>().ok()))
    };
    match (parse(CLIENT_ID_HEADER), parse(CLIENT_SEQ_HEADER)) {
        (None, None) => Ok(None),
        (Some(Some(client)), Some(Some(seq))) => Ok(Some(Session { client, seq })),
        (Some(None), _) | (_, Some(None)) => {
            tracing::warn!(key, "write rejected: unparseable dedup token header");
            Err("X-Client-Id and X-Client-Seq must be unsigned integers\n")
        }
        _ => {
            tracing::warn!(key, "write rejected: only one dedup token header present");
            Err("X-Client-Id and X-Client-Seq must be provided together\n")
        }
    }
}

// The body is parsed manually (rather than via `axum::Json`) so that requests
// without a `Content-Type: application/json` header are still accepted; the
// contract only requires the body to be valid JSON.
async fn put_key(
    State(ctx): State<Arc<ApiContext>>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let session = match session_from_headers(&key, &headers) {
        Ok(session) => session,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
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
            session,
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

async fn delete_key(
    State(ctx): State<Arc<ApiContext>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Response {
    let session = match session_from_headers(&key, &headers) {
        Ok(session) => session,
        Err(reason) => return (StatusCode::BAD_REQUEST, reason).into_response(),
    };
    match ctx
        .kv
        .write(Command::Delete {
            key: key.clone(),
            session,
        })
        .await
    {
        Ok(()) => {
            tracing::info!(key, "delete committed");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => write_error_response(&ctx, &key, error),
    }
}

/// 307 to the leader's client URL, if we know who and where it is. `path`
/// is the request path without the leading slash (a key, or an admin path
/// like `cluster/members/4`).
fn redirect_to_leader(
    ctx: &ApiContext,
    path: &str,
    leader_hint: Option<NodeId>,
) -> Option<Response> {
    let leader = leader_hint?;
    let base = ctx
        .peer_urls
        .read()
        .expect("peer_urls lock poisoned")
        .get(&leader)
        .cloned()?;
    // NOTE: the path is embedded as-is; exotic characters that need
    // re-encoding are out of scope for now.
    let location = format!("{base}/{path}");
    Some(
        (
            StatusCode::TEMPORARY_REDIRECT,
            [(header::LOCATION, location)],
        )
            .into_response(),
    )
}

fn write_error_response(ctx: &ApiContext, path: &str, error: WriteError) -> Response {
    tracing::debug!(path, %error, "write not served locally");
    match error {
        WriteError::NotLeader { leader_hint } => match redirect_to_leader(ctx, path, leader_hint) {
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
        WriteError::InvalidConfig { reason } => {
            (StatusCode::CONFLICT, format!("{reason}\n")).into_response()
        }
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

// ---- cluster admin (phase 15) ----

/// This node's view of the in-effect membership. Local, like `/cluster/
/// status`: any node answers, no leadership confirmation — operators asking
/// "who do YOU think is in the cluster" is the point.
async fn list_members(State(ctx): State<Arc<ApiContext>>) -> Response {
    Json(ctx.kv.membership()).into_response()
}

/// Adds (or re-adds) member `id` with the given addresses: proposes a
/// ConfigChange carrying the complete new configuration and answers 201
/// once it is committed. The new node should already be running in join
/// mode (`RUSTKV_JOIN=1`) so the leader can catch it up.
async fn put_member(
    State(ctx): State<Arc<ApiContext>>,
    Path(id): Path<NodeId>,
    body: Bytes,
) -> Response {
    let addr = match serde_json::from_slice::<MemberAddr>(&body) {
        Ok(addr) => addr,
        Err(error) => {
            tracing::warn!(id, %error, "add-member rejected: bad body");
            return (
                StatusCode::BAD_REQUEST,
                "body must be {\"raft\": \"host:port\", \"client\": \"http://host:port\"}\n",
            )
                .into_response();
        }
    };
    let mut members = ctx.kv.membership();
    if members.contains_key(&id) {
        return (
            StatusCode::CONFLICT,
            "already a member (address changes are not supported)\n",
        )
            .into_response();
    }
    members.insert(id, addr);
    let path = format!("cluster/members/{id}");
    match ctx.kv.write(Command::ConfigChange { members }).await {
        Ok(()) => {
            tracing::info!(id, "member added");
            StatusCode::CREATED.into_response()
        }
        Err(error) => write_error_response(&ctx, &path, error),
    }
}

/// Removes member `id` (the current leader included — it steps down once
/// the removal commits). 404 if `id` is not in this node's view.
async fn delete_member(State(ctx): State<Arc<ApiContext>>, Path(id): Path<NodeId>) -> Response {
    let mut members = ctx.kv.membership();
    if members.remove(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = format!("cluster/members/{id}");
    match ctx.kv.write(Command::ConfigChange { members }).await {
        Ok(()) => {
            tracing::info!(id, "member removed");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => write_error_response(&ctx, &path, error),
    }
}
