//! The KV service: client operations mapped onto the Raft core.
//!
//! Writes go through [`RaftHandle::propose`] and only succeed once the entry
//! is committed by a majority and applied locally — a node cut off from the
//! majority times out instead of acknowledging (the CP guarantee). Reads
//! come in two flavors: [`KvNode::get_linearizable`] (the default) confirms
//! leadership via ReadIndex before reading, so it is never stale and, like
//! writes, times out rather than answering from a minority; [`KvNode::get`]
//! reads the local state machine directly and may be stale on followers or
//! a just-deposed leader (the documented fast path).

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::raft::node::{ProposeError, RaftHandle, Status};
use crate::raft::types::{Command, NodeId};
use crate::store::KvStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteError {
    /// This node is not the leader; retry against `leader_hint` if present.
    NotLeader { leader_hint: Option<NodeId> },
    /// Not confirmed within the timeout. The write MAY still commit later —
    /// the outcome is unknown (typical cause: this leader lost its majority).
    Timeout,
    /// Definitely not applied: a leadership change replaced the entry.
    /// Safe to retry.
    Superseded,
    /// A configuration change was rejected before being appended (phase 15):
    /// not a single-server delta, another change in flight, or the leader's
    /// no-op not yet committed. Definitely did not happen.
    InvalidConfig { reason: &'static str },
    /// The node has shut down.
    Shutdown,
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::NotLeader {
                leader_hint: Some(id),
            } => write!(f, "not the leader; try node {id}"),
            WriteError::NotLeader { leader_hint: None } => {
                write!(f, "not the leader, and no leader is known")
            }
            WriteError::Timeout => write!(f, "write not confirmed in time; outcome unknown"),
            WriteError::Superseded => write!(f, "write superseded by a leadership change"),
            WriteError::InvalidConfig { reason } => {
                write!(f, "invalid configuration change: {reason}")
            }
            WriteError::Shutdown => write!(f, "node has shut down"),
        }
    }
}

impl std::error::Error for WriteError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// This node is not the leader; retry against `leader_hint` if present.
    NotLeader { leader_hint: Option<NodeId> },
    /// Leadership not confirmed within the timeout (typical cause: this
    /// leader lost its majority). Unlike a write timeout there is nothing
    /// ambiguous in flight — safe to retry elsewhere.
    Timeout,
    /// Leadership was lost while the read was pending. Safe to retry.
    Retry,
    /// The node has shut down.
    Shutdown,
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::NotLeader {
                leader_hint: Some(id),
            } => write!(f, "not the leader; try node {id}"),
            ReadError::NotLeader { leader_hint: None } => {
                write!(f, "not the leader, and no leader is known")
            }
            ReadError::Timeout => write!(f, "read not confirmed in time; retry"),
            ReadError::Retry => write!(f, "leadership changed during the read; retry"),
            ReadError::Shutdown => write!(f, "node has shut down"),
        }
    }
}

impl std::error::Error for ReadError {}

/// One node's KV service: the local state machine plus the Raft handle.
pub struct KvNode {
    store: Arc<KvStore>,
    raft: RaftHandle,
    write_timeout: Duration,
}

impl KvNode {
    pub fn new(store: Arc<KvStore>, raft: RaftHandle, write_timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            store,
            raft,
            write_timeout,
        })
    }

    /// Local read. May be stale on followers (see module docs).
    pub fn get(&self, key: &str) -> Option<Value> {
        self.store.get(key)
    }

    /// Linearizable read (§6.4 ReadIndex): confirms this node is still the
    /// leader against a majority, waits until the state machine reflects
    /// everything committed before the read began, then reads locally.
    /// Leader-only — non-leaders return [`ReadError::NotLeader`].
    pub async fn get_linearizable(&self, key: &str) -> Result<Option<Value>, ReadError> {
        let ticket = match self.raft.read().await {
            Ok(ticket) => ticket,
            Err(ProposeError::NotLeader { leader_hint }) => {
                return Err(ReadError::NotLeader { leader_hint });
            }
            // Reads carry no configuration; unreachable, mapped for totality.
            Err(ProposeError::InvalidConfigChange { .. }) => return Err(ReadError::Retry),
            Err(ProposeError::Shutdown) => return Err(ReadError::Shutdown),
        };
        match tokio::time::timeout(self.write_timeout, ticket.granted).await {
            Err(_elapsed) => Err(ReadError::Timeout),
            Ok(Err(_step_down)) => Err(ReadError::Retry),
            Ok(Ok(())) => Ok(self.store.get(key)),
        }
    }

    pub fn status(&self) -> Status {
        self.raft.status()
    }

    pub fn raft(&self) -> &RaftHandle {
        &self.raft
    }

    /// The in-effect cluster membership (phase 15), for the admin API.
    pub fn membership(&self) -> crate::raft::types::Membership {
        self.raft.membership()
    }

    /// Proposes a write and waits for majority commit + local apply.
    pub async fn write(&self, command: Command) -> Result<(), WriteError> {
        let proposal = match self.raft.propose(command).await {
            Ok(proposal) => proposal,
            Err(ProposeError::NotLeader { leader_hint }) => {
                return Err(WriteError::NotLeader { leader_hint });
            }
            Err(ProposeError::InvalidConfigChange { reason }) => {
                return Err(WriteError::InvalidConfig { reason });
            }
            Err(ProposeError::Shutdown) => return Err(WriteError::Shutdown),
        };
        match tokio::time::timeout(self.write_timeout, proposal.committed).await {
            Err(_elapsed) => Err(WriteError::Timeout),
            Ok(Err(_node_gone)) => Err(WriteError::Shutdown),
            Ok(Ok(true)) => Ok(()),
            Ok(Ok(false)) => Err(WriteError::Superseded),
        }
    }
}
