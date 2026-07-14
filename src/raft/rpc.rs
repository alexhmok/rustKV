//! The Raft RPCs (Figure 2 of the paper, plus PreVote from §9.6 / thesis
//! §4.2.3), as plain serializable data.
//!
//! Phase 2 defines only the message shapes so the transport layer has a
//! payload; the RPC *semantics* (vote rules, consistency checks) arrive with
//! leader election (phase 3) and log replication (phase 4). PreVote
//! (phase 11) reuses the RequestVote payloads under distinct variants, so a
//! pre-vote probe can never be conflated with a real, binding vote.

use serde::{Deserialize, Serialize};

use super::types::{LogEntry, LogIndex, NodeId, Snapshot, Term};

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

/// InstallSnapshot (§7, phase 14): sent instead of AppendEntries when a
/// follower's next_index falls at or below the leader's snapshot boundary —
/// the entries it needs no longer exist as entries. Single-shot: the whole
/// snapshot rides in one RPC (no §7 offset/chunking — payloads are small by
/// scope; a documented gap, like the unbounded in-memory payload it implies).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    pub term: Term,
    pub leader_id: NodeId,
    pub snapshot: Snapshot,
}

/// Per Figure 13 the reply carries only the follower's term: a higher term
/// deposes the leader as usual; otherwise the leader may assume the follower
/// now holds everything through the snapshot's boundary (a duplicate the
/// follower no-op'd included — the boundary was then already committed there).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub term: Term,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RpcRequest {
    RequestVote(RequestVoteArgs),
    AppendEntries(AppendEntriesArgs),
    /// A pre-vote probe (§9.6): "would you vote for me for `term`?". Same
    /// payload as RequestVote — `term` is the *prospective* term, one above
    /// the asker's current term — but granting one records nothing and
    /// binds nobody.
    PreVote(RequestVoteArgs),
    /// Phase 14. Serde's external tagging keys variants by name, so adding
    /// this changes nothing about the other variants' encoding (pinned by
    /// tests below — mixed-version clusters and old peers depend on it).
    InstallSnapshot(InstallSnapshotArgs),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RpcResponse {
    RequestVote(RequestVoteReply),
    AppendEntries(AppendEntriesReply),
    PreVote(RequestVoteReply),
    InstallSnapshot(InstallSnapshotReply),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::Command;
    use serde_json::json;

    /// The pre-phase-14 variants must serialize byte-identical to phase-13
    /// output (this is the HTTP transport's wire format; three_process.rs
    /// exercises it against the real binary). Strings pinned verbatim.
    #[test]
    fn existing_variants_serialize_byte_identical_to_phase_13() {
        let ae = RpcRequest::AppendEntries(AppendEntriesArgs {
            term: 3,
            leader_id: 1,
            prev_log_index: 4,
            prev_log_term: 2,
            entries: vec![LogEntry {
                term: 3,
                index: 5,
                command: Command::Put {
                    key: "k".to_string(),
                    value: json!(1),
                    session: None,
                },
            }],
            leader_commit: 4,
        });
        assert_eq!(
            serde_json::to_string(&ae).unwrap(),
            r#"{"AppendEntries":{"term":3,"leader_id":1,"prev_log_index":4,"prev_log_term":2,"entries":[{"term":3,"index":5,"command":{"Put":{"key":"k","value":1}}}],"leader_commit":4}}"#
        );
        let vote = RpcRequest::RequestVote(RequestVoteArgs {
            term: 2,
            candidate_id: 3,
            last_log_index: 7,
            last_log_term: 1,
        });
        assert_eq!(
            serde_json::to_string(&vote).unwrap(),
            r#"{"RequestVote":{"term":2,"candidate_id":3,"last_log_index":7,"last_log_term":1}}"#
        );
        let reply = RpcResponse::AppendEntries(AppendEntriesReply {
            term: 3,
            success: true,
        });
        assert_eq!(
            serde_json::to_string(&reply).unwrap(),
            r#"{"AppendEntries":{"term":3,"success":true}}"#
        );
    }

    #[test]
    fn install_snapshot_roundtrips() {
        let req = RpcRequest::InstallSnapshot(InstallSnapshotArgs {
            term: 5,
            leader_id: 2,
            snapshot: Snapshot {
                last_included_index: 9,
                last_included_term: 4,
                membership: None,
                state: json!({"map": {"k": 1}, "sessions": {}}),
            },
        });
        let encoded = serde_json::to_string(&req).unwrap();
        assert_eq!(serde_json::from_str::<RpcRequest>(&encoded).unwrap(), req);
        let reply = RpcResponse::InstallSnapshot(InstallSnapshotReply { term: 5 });
        let encoded = serde_json::to_string(&reply).unwrap();
        assert_eq!(
            serde_json::from_str::<RpcResponse>(&encoded).unwrap(),
            reply
        );
    }
}
