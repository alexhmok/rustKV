//! The two Raft RPCs (Figure 2 of the paper), as plain serializable data.
//!
//! Phase 2 defines only the message shapes so the transport layer has a
//! payload; the RPC *semantics* (vote rules, consistency checks) arrive with
//! leader election (phase 3) and log replication (phase 4).

use serde::{Deserialize, Serialize};

use super::types::{LogEntry, LogIndex, NodeId, Term};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    // TODO(phase 4): conflict-backtracking hints (conflict_index/conflict_term)
    // if plain next_index decrement proves too slow in fault tests.
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RpcRequest {
    RequestVote(RequestVoteArgs),
    AppendEntries(AppendEntriesArgs),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RpcResponse {
    RequestVote(RequestVoteReply),
    AppendEntries(AppendEntriesReply),
}
