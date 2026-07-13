//! The KV service: client operations mapped onto the Raft core.
//!
//! Writes go through [`RaftHandle::propose`] and only succeed once the entry
//! is committed by a majority and applied locally — a node cut off from the
//! majority times out instead of acknowledging (the CP guarantee). Reads are
//! served from the local state machine and may be stale on followers or a
//! just-deposed leader. TODO: linearizable reads (ReadIndex or leader
//! leases) are deliberately out of scope.

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
            WriteError::Shutdown => write!(f, "node has shut down"),
        }
    }
}

impl std::error::Error for WriteError {}

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

    pub fn status(&self) -> Status {
        self.raft.status()
    }

    pub fn raft(&self) -> &RaftHandle {
        &self.raft
    }

    /// Proposes a write and waits for majority commit + local apply.
    pub async fn write(&self, command: Command) -> Result<(), WriteError> {
        let proposal = match self.raft.propose(command).await {
            Ok(proposal) => proposal,
            Err(ProposeError::NotLeader { leader_hint }) => {
                return Err(WriteError::NotLeader { leader_hint });
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
