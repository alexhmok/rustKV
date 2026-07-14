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

/// A state-machine command carried by a log entry. Put/Delete mirror the
/// client API's write operations; Noop is appended by a fresh leader (§8) to
/// commit prior-term entries promptly and is skipped by the state machine.
///
/// `session` is the optional dedup token. It is `#[serde(default)]` so
/// pre-phase-13 log files stay readable, and skipped when absent so
/// token-less commands serialize byte-identical to pre-phase-13 output
/// (protecting both old data dirs and the RPC wire format) — pinned by
/// unit tests below.
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
/// dedup sessions). `membership` is reserved for dynamic membership
/// (phase 15) and is always `None` today.
///
/// [`KvSnapshot`]: crate::store::KvSnapshot
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    /// TODO(membership): populated by phase 15's dynamic membership.
    pub membership: Option<Value>,
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
}
