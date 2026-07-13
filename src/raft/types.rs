//! Core Raft data types, shared by the consensus core, persistence, and (in
//! later phases) the transport RPCs.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Identifies a cluster member. Fixed membership comes from config (phase 7).
pub type NodeId = u64;

/// Raft term. Starts at 0 on a fresh node; term 0 never has a leader.
pub type Term = u64;

/// 1-based position in the Raft log. 0 is the sentinel meaning "no entry"
/// (e.g. `prev_log_index` before the first entry).
pub type LogIndex = u64;

/// A state-machine command carried by a log entry. Put/Delete mirror the
/// client API's write operations; Noop is appended by a fresh leader (§8) to
/// commit prior-term entries promptly and is skipped by the state machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Put { key: String, value: Value },
    Delete { key: String },
    Noop,
}

/// One entry in the replicated log.
///
/// `index` is stored redundantly (it is implied by the position in the log)
/// so that persisted lines and RPC payloads are self-describing and
/// contiguity can be validated on replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Command,
}

/// State that must survive crashes and be flushed to disk *before* answering
/// any RPC (Raft §5.1, Figure 2 "persistent state"). The log itself is
/// persisted separately (append-only) by [`crate::raft::Storage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}
