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
/// the entries it needs no longer exist as entries.
///
/// Chunking (§7's offset/done, phase 20c): with the leader's
/// `snapshot_chunk_bytes` set, the serialized `state` is streamed as
/// `data` slices at byte `offset`s (each chunk carrying the boundary
/// metadata in `snapshot`, its `state` left `null`), and the follower
/// persists + restores only at `done`. The serde defaults are chosen so a
/// phase-14..19 single-shot message — no `offset`/`data`/`done` fields at
/// all — reads as an offset-0, done, data-inline chunk, and a phase-20
/// single-shot sender (chunking off, the default) skips all three fields
/// and emits byte-identical phase-14 output (both pinned below). Chunked
/// messages themselves are NOT readable by pre-phase-20 binaries (their
/// `state` is `null`): don't enable chunking mid-rolling-upgrade.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    pub term: Term,
    pub leader_id: NodeId,
    /// The boundary metadata; carries the whole state inline when (and
    /// only when) `data` is `None` — the single-shot form.
    pub snapshot: Snapshot,
    /// Byte offset of `data` within the serialized state (§7). Absent on
    /// the wire when 0.
    #[serde(default, skip_serializing_if = "offset_is_zero")]
    pub offset: u64,
    /// One chunk of the serialized state, split on UTF-8 boundaries.
    /// `None` = single-shot: the state rides in `snapshot.state` instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    /// True on the final chunk (and, via the default, on every single-shot
    /// message). Absent on the wire when true.
    #[serde(default = "done_default", skip_serializing_if = "done_is_true")]
    pub done: bool,
}

fn offset_is_zero(offset: &u64) -> bool {
    *offset == 0
}

fn done_default() -> bool {
    true
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde requires the &bool shape
fn done_is_true(done: &bool) -> bool {
    *done
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

    /// A phase-14..19 single-shot InstallSnapshot on today's wire, both
    /// directions (phase 20c): a single-shot sender (chunking off, the
    /// default) must emit the pre-phase-20 encoding VERBATIM — no
    /// offset/data/done fields — and that old encoding must deserialize
    /// as an offset-0, done, data-inline message. Mixed-version clusters
    /// (with chunking off) depend on both.
    #[test]
    fn single_shot_install_snapshot_wire_format_is_pinned_to_phase_14() {
        let phase_14_wire = r#"{"InstallSnapshot":{"term":5,"leader_id":2,"snapshot":{"last_included_index":9,"last_included_term":4,"membership":null,"state":{"map":{"k":1},"sessions":{}}}}}"#;
        let single_shot = RpcRequest::InstallSnapshot(InstallSnapshotArgs {
            term: 5,
            leader_id: 2,
            snapshot: Snapshot {
                last_included_index: 9,
                last_included_term: 4,
                membership: None,
                state: json!({"map": {"k": 1}, "sessions": {}}),
            },
            offset: 0,
            data: None,
            done: true,
        });
        assert_eq!(
            serde_json::to_string(&single_shot).unwrap(),
            phase_14_wire,
            "single-shot messages must serialize exactly as phase 14 did"
        );
        assert_eq!(
            serde_json::from_str::<RpcRequest>(phase_14_wire).unwrap(),
            single_shot,
            "a pre-phase-20 message must read as an offset-0/done single chunk"
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
            offset: 0,
            data: None,
            done: true,
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

    /// The chunked form (phase 20c) has a pinned encoding of its own: the
    /// boundary metadata rides every chunk with a `null` state, and the
    /// offset/data/done trio appears exactly when it is informative
    /// (offset nonzero, data present, done false).
    #[test]
    fn chunked_install_snapshot_serialization_is_pinned_and_roundtrips() {
        let chunk = RpcRequest::InstallSnapshot(InstallSnapshotArgs {
            term: 5,
            leader_id: 2,
            snapshot: Snapshot {
                last_included_index: 9,
                last_included_term: 4,
                membership: None,
                state: serde_json::Value::Null,
            },
            offset: 16,
            data: Some("k\":1},\"sessions".to_string()),
            done: false,
        });
        let encoded = serde_json::to_string(&chunk).unwrap();
        assert_eq!(
            encoded,
            r#"{"InstallSnapshot":{"term":5,"leader_id":2,"snapshot":{"last_included_index":9,"last_included_term":4,"membership":null,"state":null},"offset":16,"data":"k\":1},\"sessions","done":false}}"#
        );
        assert_eq!(serde_json::from_str::<RpcRequest>(&encoded).unwrap(), chunk);

        // A FINAL chunk (done=true, offset 0 on a one-chunk transfer)
        // omits the defaulted fields but keeps `data` — still a chunk, not
        // a single-shot.
        let final_chunk = RpcRequest::InstallSnapshot(InstallSnapshotArgs {
            term: 5,
            leader_id: 2,
            snapshot: Snapshot {
                last_included_index: 9,
                last_included_term: 4,
                membership: None,
                state: serde_json::Value::Null,
            },
            offset: 0,
            data: Some("{}".to_string()),
            done: true,
        });
        let encoded = serde_json::to_string(&final_chunk).unwrap();
        assert_eq!(
            encoded,
            r#"{"InstallSnapshot":{"term":5,"leader_id":2,"snapshot":{"last_included_index":9,"last_included_term":4,"membership":null,"state":null},"data":"{}"}}"#
        );
        assert_eq!(
            serde_json::from_str::<RpcRequest>(&encoded).unwrap(),
            final_chunk
        );
    }
}
