//! Core Raft data types, shared by the consensus core, persistence, and (in
//! later phases) the transport RPCs.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Identifies a cluster member. Bootstrap membership comes from config
/// (phase 7); from phase 15 on it is log-derived (`Command::ConfigChange`).
pub type NodeId = u64;

/// Raft term. Starts at 0 on a fresh node; term 0 never has a leader.
pub type Term = u64;

/// 1-based position in the Raft log. 0 is the sentinel meaning "no entry"
/// (e.g. `prev_log_index` before the first entry).
pub type LogIndex = u64;

/// A client dedup token (phase 13): `seq` must be strictly increasing per
/// `client`. The state machine applies a tokened command only if its `seq`
/// is above the client's highest applied one, so a retried ambiguous write
/// commits again but mutates nothing — exactly-once application on top of
/// at-least-once delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub client: u64,
    pub seq: u64,
}

/// How to reach one cluster member (phase 15): its raft RPC address
/// (`host:port`) and its client-facing base URL (for write/read redirects).
/// Opaque to the consensus core — it only carries them; the transport and
/// API layers consume them. May be empty where addresses are meaningless
/// (the in-memory simulator routes by id).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberAddr {
    pub raft: String,
    pub client: String,
}

/// A complete cluster configuration (phase 15): every member with its
/// addresses. `BTreeMap` so iteration order — and therefore serialization
/// and peer fan-out order — is deterministic.
pub type Membership = BTreeMap<NodeId, MemberAddr>;

/// A state-machine command carried by a log entry. Put/Delete mirror the
/// client API's write operations; Noop is appended by a fresh leader (§8) to
/// commit prior-term entries promptly and is skipped by the state machine.
///
/// `session` is the optional dedup token. It is `#[serde(default)]` so
/// pre-phase-13 log files stay readable, and skipped when absent so
/// token-less commands serialize byte-identical to pre-phase-13 output
/// (protecting both old data dirs and the RPC wire format) — pinned by
/// unit tests below.
///
/// `ConfigChange` (phase 15) is Raft-internal: it carries the COMPLETE new
/// membership (single-server delta vs the active config, validated at
/// proposal time) and is ignored by the KV state machine — membership is
/// consensus bookkeeping, not user data. Serde's external tagging keys
/// variants by name, so adding it changes nothing about the other variants'
/// encoding (pinned below).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Put {
        key: String,
        value: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<Session>,
    },
    Delete {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<Session>,
    },
    Noop,
    ConfigChange {
        members: Membership,
    },
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

/// A snapshot of the state machine at a log prefix (phase 14): everything
/// the entries up to and including `last_included_index` produced. One shape
/// serves both `snapshot.json` on disk and the InstallSnapshot RPC payload —
/// a follower persists exactly what the leader sent.
///
/// `state` is opaque to Raft (the state machine's `snapshot()`/`restore()`
/// pair owns its meaning — for the KV store, a [`KvSnapshot`] of map +
/// dedup sessions). `membership` (phase 15) is the configuration in effect
/// AT the boundary: the latest `ConfigChange` at or below
/// `last_included_index`, or `None` if membership was still
/// bootstrap-derived there — `None` serializes as `null`, byte-identical to
/// phase-14 output, and phase-14 snapshot files deserialize unchanged
/// (both pinned below).
///
/// [`KvSnapshot`]: crate::store::KvSnapshot
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub membership: Option<Membership>,
    pub state: Value,
}

/// State that must survive crashes and be flushed to disk *before* answering
/// any RPC (Raft §5.1, Figure 2 "persistent state"). The log itself is
/// persisted separately (append-only) by [`crate::raft::Storage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Token-less commands must serialize byte-identical to the pre-phase-13
    /// format: this is the on-disk `log.jsonl` line format AND the HTTP RPC
    /// wire format, so a mixed-version cluster and old data dirs both depend
    /// on it. The strings are pinned verbatim from phase-12 output.
    #[test]
    fn tokenless_commands_serialize_byte_identical_to_phase_12() {
        let entry = LogEntry {
            term: 3,
            index: 7,
            command: Command::Put {
                key: "k".to_string(),
                value: json!({"a": 1}),
                session: None,
            },
        };
        assert_eq!(
            serde_json::to_string(&entry).unwrap(),
            r#"{"term":3,"index":7,"command":{"Put":{"key":"k","value":{"a":1}}}}"#
        );
        let delete = Command::Delete {
            key: "gone".to_string(),
            session: None,
        };
        assert_eq!(
            serde_json::to_string(&delete).unwrap(),
            r#"{"Delete":{"key":"gone"}}"#
        );
    }

    /// Pre-phase-13 log lines (no `session` field) must deserialize, with
    /// the token absent — old data dirs stay readable.
    #[test]
    fn phase_12_log_lines_deserialize_without_a_session() {
        let entry: LogEntry = serde_json::from_str(
            r#"{"term":3,"index":7,"command":{"Put":{"key":"k","value":{"a":1}}}}"#,
        )
        .unwrap();
        assert_eq!(
            entry.command,
            Command::Put {
                key: "k".to_string(),
                value: json!({"a": 1}),
                session: None,
            }
        );
        let delete: Command = serde_json::from_str(r#"{"Delete":{"key":"gone"}}"#).unwrap();
        assert_eq!(
            delete,
            Command::Delete {
                key: "gone".to_string(),
                session: None,
            }
        );
    }

    #[test]
    fn tokened_commands_roundtrip() {
        let command = Command::Put {
            key: "k".to_string(),
            value: json!(1),
            session: Some(Session { client: 4, seq: 9 }),
        };
        let encoded = serde_json::to_string(&command).unwrap();
        assert_eq!(
            encoded,
            r#"{"Put":{"key":"k","value":1,"session":{"client":4,"seq":9}}}"#
        );
        assert_eq!(serde_json::from_str::<Command>(&encoded).unwrap(), command);

        let delete = Command::Delete {
            key: "k".to_string(),
            session: Some(Session { client: 4, seq: 10 }),
        };
        let encoded = serde_json::to_string(&delete).unwrap();
        assert_eq!(
            encoded,
            r#"{"Delete":{"key":"k","session":{"client":4,"seq":10}}}"#
        );
        assert_eq!(serde_json::from_str::<Command>(&encoded).unwrap(), delete);
    }

    fn two_member_map() -> Membership {
        BTreeMap::from([
            (
                1,
                MemberAddr {
                    raft: "n1:9080".to_string(),
                    client: "http://n1:8080".to_string(),
                },
            ),
            (
                2,
                MemberAddr {
                    raft: "n2:9080".to_string(),
                    client: "http://n2:8080".to_string(),
                },
            ),
        ])
    }

    /// ConfigChange entries (phase 15) have a pinned encoding of their own —
    /// this is what lands in log.jsonl and inside AppendEntries — and the
    /// BTreeMap keeps member order deterministic.
    #[test]
    fn config_change_serialization_is_pinned_and_roundtrips() {
        let entry = LogEntry {
            term: 4,
            index: 9,
            command: Command::ConfigChange {
                members: two_member_map(),
            },
        };
        let encoded = serde_json::to_string(&entry).unwrap();
        assert_eq!(
            encoded,
            r#"{"term":4,"index":9,"command":{"ConfigChange":{"members":{"1":{"raft":"n1:9080","client":"http://n1:8080"},"2":{"raft":"n2:9080","client":"http://n2:8080"}}}}}"#
        );
        assert_eq!(serde_json::from_str::<LogEntry>(&encoded).unwrap(), entry);
    }

    /// The 14→15 thread, pinned both ways: a phase-14 `snapshot.json` (its
    /// `membership` always `null`) must deserialize to `None` under the now-
    /// typed field, and a membership-less snapshot must serialize
    /// byte-identical to phase-14 output — old data dirs and mixed-version
    /// InstallSnapshot payloads both depend on it.
    #[test]
    fn phase_14_snapshots_stay_readable_and_byte_identical() {
        let phase_14 = r#"{"last_included_index":8,"last_included_term":3,"membership":null,"state":{"map":{"k":1},"sessions":{}}}"#;
        let snapshot: Snapshot = serde_json::from_str(phase_14).unwrap();
        assert_eq!(snapshot.membership, None);
        assert_eq!(snapshot.last_included_index, 8);
        assert_eq!(
            serde_json::to_string(&snapshot).unwrap(),
            phase_14,
            "membership-less snapshots must serialize exactly as phase 14 did"
        );
    }

    #[test]
    fn snapshot_with_membership_roundtrips() {
        let snapshot = Snapshot {
            last_included_index: 12,
            last_included_term: 5,
            membership: Some(two_member_map()),
            state: json!({"map": {}, "sessions": {}}),
        };
        let encoded = serde_json::to_string(&snapshot).unwrap();
        assert_eq!(
            serde_json::from_str::<Snapshot>(&encoded).unwrap(),
            snapshot
        );
    }
}
