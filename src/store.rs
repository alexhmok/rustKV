//! In-memory key-value state machine.
//!
//! All mutations arrive by applying committed Raft log entries (the
//! [`StateMachine`] impl); the HTTP layer only reads it directly.
//!
//! Dedup (phase 13): commands carrying a [`Session`] token are applied
//! exactly once — a `sessions` table tracks, per client, which seqs have
//! applied, and a command whose exact seq already applied skips the
//! mutation (the duplicate log entry still committed; only its
//! application is a no-op). Matching is EXACT over a sliding window of
//! the most recent [`SESSION_WINDOW`] seqs, not `seq <= max`: a client
//! may pipeline up to that many independent ops, and an op arriving
//! after a higher-seq one still applies (concurrent ops from one client
//! may linearize in either order — what must never happen is acking a
//! write that was silently skipped). Below the window a seq is treated
//! as a duplicate, which is only sound under the documented contract:
//! at most [`SESSION_WINDOW`] outstanding ops per client, seqs strictly
//! increasing. The table lives IN the state machine so it is rebuilt by
//! log replay after a restart and will ride along in phase 14's snapshot
//! ([`KvSnapshot`]). There is deliberately NO expiry: a per-node TTL
//! would diverge replicas (apply must stay a pure fold of the log). The
//! table therefore grows unboundedly with the number of distinct clients
//! — a documented gap. No results are cached (Put/Delete return unit).

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::raft::node::StateMachine;
use crate::raft::types::{Command, LogEntry, Session};

/// How many recent seqs per client the dedup window covers (the size of
/// [`SessionState::recent`]): a client may have at most this many ops
/// outstanding at once.
pub const SESSION_WINDOW: u64 = 64;

/// One client's dedup state: which of its last [`SESSION_WINDOW`] seqs
/// have applied. Bit `i` of `recent` set means seq `max_seq - i` applied
/// (bit 0 is `max_seq` itself); anything below the window is assumed
/// already applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionState {
    pub max_seq: u64,
    pub recent: u64,
}

impl SessionState {
    fn new(seq: u64) -> Self {
        Self {
            max_seq: seq,
            recent: 1,
        }
    }

    fn contains(&self, seq: u64) -> bool {
        if seq > self.max_seq {
            return false;
        }
        let offset = self.max_seq - seq;
        // Below the window: assumed a duplicate (sound only under the
        // <= SESSION_WINDOW outstanding-ops contract).
        offset >= SESSION_WINDOW || self.recent & (1 << offset) != 0
    }

    fn record(&mut self, seq: u64) {
        if seq > self.max_seq {
            let shift = seq - self.max_seq;
            self.recent = self
                .recent
                .checked_shl(u32::try_from(shift).unwrap_or(u32::MAX))
                .unwrap_or(0)
                | 1;
            self.max_seq = seq;
        } else if self.max_seq - seq < SESSION_WINDOW {
            self.recent |= 1 << (self.max_seq - seq);
        }
    }
}

/// The full state a snapshot must capture (phase 14's payload, shaped now):
/// the KV map AND the dedup sessions — forgetting the latter would resurrect
/// skipped duplicates on a snapshot-restored node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvSnapshot {
    pub map: HashMap<String, Value>,
    pub sessions: HashMap<u64, SessionState>,
}

/// Thread-safe map of string keys to arbitrary JSON values, plus the dedup
/// sessions table (client → applied-seq window).
///
/// Uses std (not tokio) `RwLock`s: guards are never held across an `.await`,
/// and the two locks are never held at the same time (apply touches them
/// one after the other; export/import go map first, then sessions).
#[derive(Debug, Default)]
pub struct KvStore {
    map: RwLock<HashMap<String, Value>>,
    sessions: RwLock<HashMap<u64, SessionState>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a clone of the stored value, or `None` if the key is absent.
    pub fn get(&self, key: &str) -> Option<Value> {
        self.map.read().expect("kv lock poisoned").get(key).cloned()
    }

    /// Inserts or overwrites the value for `key`.
    pub fn put(&self, key: String, value: Value) {
        self.map
            .write()
            .expect("kv lock poisoned")
            .insert(key, value);
    }

    /// Removes `key`. Returns `true` if it existed.
    pub fn delete(&self, key: &str) -> bool {
        self.map
            .write()
            .expect("kv lock poisoned")
            .remove(key)
            .is_some()
    }

    /// A copy of the full map — for tests and debugging, not the hot path.
    pub fn snapshot(&self) -> HashMap<String, Value> {
        self.map.read().expect("kv lock poisoned").clone()
    }

    /// The complete snapshottable state: map + sessions (phase 14's payload).
    pub fn export(&self) -> KvSnapshot {
        KvSnapshot {
            map: self.map.read().expect("kv lock poisoned").clone(),
            sessions: self
                .sessions
                .read()
                .expect("sessions lock poisoned")
                .clone(),
        }
    }

    /// Replaces the entire state with a snapshot (phase 14's restore path).
    pub fn import(&self, snapshot: KvSnapshot) {
        *self.map.write().expect("kv lock poisoned") = snapshot.map;
        *self.sessions.write().expect("sessions lock poisoned") = snapshot.sessions;
    }

    /// True if a tokened command was already applied: its exact seq is in
    /// the client's applied window (or below it — see [`SessionState`]).
    /// Token-less commands are never duplicates.
    fn already_applied(&self, session: Option<Session>) -> bool {
        let Some(Session { client, seq }) = session else {
            return false;
        };
        self.sessions
            .read()
            .expect("sessions lock poisoned")
            .get(&client)
            .is_some_and(|state| state.contains(seq))
    }

    /// Records a tokened command's seq in the client's applied window.
    fn record_applied(&self, session: Option<Session>) {
        if let Some(Session { client, seq }) = session {
            self.sessions
                .write()
                .expect("sessions lock poisoned")
                .entry(client)
                .and_modify(|state| state.record(seq))
                .or_insert_with(|| SessionState::new(seq));
        }
    }
}

impl StateMachine for KvStore {
    // Must remain a pure fold of the log — no clocks, no randomness. That
    // purity is what makes the sessions table restart-safe (rebuilt by
    // replay) and snapshottable (phase 14).
    fn apply(&self, entry: &LogEntry) {
        match &entry.command {
            Command::Put {
                key,
                value,
                session,
            } => {
                if self.already_applied(*session) {
                    tracing::debug!(index = entry.index, key, "duplicate put skipped at apply");
                    return;
                }
                self.put(key.clone(), value.clone());
                self.record_applied(*session);
            }
            Command::Delete { key, session } => {
                if self.already_applied(*session) {
                    tracing::debug!(
                        index = entry.index,
                        key,
                        "duplicate delete skipped at apply"
                    );
                    return;
                }
                self.delete(key);
                self.record_applied(*session);
            }
            Command::Noop => {}
        }
    }

    // NOTE: this trait method coexists with the inherent map-only
    // `KvStore::snapshot()`; concrete calls resolve to the inherent one, so
    // existing callers are untouched (pinned by a test below).
    fn snapshot(&self) -> Value {
        serde_json::to_value(self.export()).expect("KvSnapshot serialization cannot fail")
    }

    fn restore(&self, state: &Value) {
        let snapshot: KvSnapshot = serde_json::from_value(state.clone())
            .expect("malformed KV snapshot payload; fail-stop");
        self.import(snapshot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn put_get_overwrite_delete() {
        let store = KvStore::new();
        assert_eq!(store.get("k"), None);

        store.put("k".to_string(), json!({"a": 1}));
        assert_eq!(store.get("k"), Some(json!({"a": 1})));

        store.put("k".to_string(), json!({"b": 2}));
        assert_eq!(store.get("k"), Some(json!({"b": 2})));

        assert!(store.delete("k"));
        assert!(!store.delete("k"));
        assert_eq!(store.get("k"), None);
    }

    fn apply(store: &KvStore, index: u64, command: Command) {
        store.apply(&LogEntry {
            term: 1,
            index,
            command,
        });
    }

    fn tokened_put(key: &str, value: u64, client: u64, seq: u64) -> Command {
        Command::Put {
            key: key.to_string(),
            value: json!(value),
            session: Some(Session { client, seq }),
        }
    }

    fn state(max_seq: u64, recent: u64) -> SessionState {
        SessionState { max_seq, recent }
    }

    #[test]
    fn duplicate_seq_skips_the_mutation() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 1));
        // Interleaved conflicting write, then the duplicate: the duplicate
        // must NOT clobber it (an LWW map makes k=1-over-k=1 invisible).
        apply(&store, 2, tokened_put("k", 2, 8, 1));
        apply(&store, 3, tokened_put("k", 1, 7, 1));
        assert_eq!(store.get("k"), Some(json!(2)));
        assert_eq!(
            store.export().sessions,
            HashMap::from([(7, state(1, 0b1)), (8, state(1, 0b1))])
        );
    }

    /// The inversion of the original `lower_seq_skips_the_mutation`: a
    /// lower seq that never applied is a pipelined op that lost the race
    /// to the log, not a duplicate — skipping-yet-acking it was the false
    /// ack bug. It applies; only its exact retry is then skipped.
    #[test]
    fn out_of_order_pipelined_op_applies() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("b", 3, 7, 3));
        apply(&store, 2, tokened_put("a", 1, 7, 1));
        assert_eq!(store.get("a"), Some(json!(1)));
        assert_eq!(
            store.export().sessions,
            HashMap::from([(7, state(3, 0b101))])
        );
        // Retries of both seqs are duplicates now; the gap (seq 2) is not.
        apply(&store, 3, tokened_put("a", 99, 7, 1));
        apply(&store, 4, tokened_put("b", 99, 7, 3));
        assert_eq!(store.get("a"), Some(json!(1)));
        assert_eq!(store.get("b"), Some(json!(3)));
        apply(&store, 5, tokened_put("c", 2, 7, 2));
        assert_eq!(store.get("c"), Some(json!(2)));
        assert_eq!(
            store.export().sessions,
            HashMap::from([(7, state(3, 0b111))])
        );
    }

    #[test]
    fn higher_seq_applies_and_advances_the_session() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 1));
        apply(&store, 2, tokened_put("k", 2, 7, 2));
        assert_eq!(store.get("k"), Some(json!(2)));
        assert_eq!(
            store.export().sessions,
            HashMap::from([(7, state(2, 0b11))])
        );
    }

    #[test]
    fn clients_are_independent() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 5));
        // Same seq, different client: applies.
        apply(&store, 2, tokened_put("k", 2, 8, 5));
        assert_eq!(store.get("k"), Some(json!(2)));
        assert_eq!(
            store.export().sessions,
            HashMap::from([(7, state(5, 0b1)), (8, state(5, 0b1))])
        );
    }

    /// The window slides: seqs that fall more than SESSION_WINDOW behind
    /// the max are assumed duplicates (the <= 64-outstanding contract), so
    /// dedup keeps working for arbitrarily old retries.
    #[test]
    fn below_window_seqs_are_assumed_duplicates() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 1));
        // Jump far ahead: the window slides past seq 1 entirely.
        apply(&store, 2, tokened_put("k", 2, 7, 1 + SESSION_WINDOW));
        assert_eq!(
            store.export().sessions,
            HashMap::from([(7, state(1 + SESSION_WINDOW, 0b1))])
        );
        // A retry of seq 1 (offset 64, below the window) is still skipped...
        apply(&store, 3, tokened_put("k", 99, 7, 1));
        assert_eq!(store.get("k"), Some(json!(2)));
        // ...while the oldest seq INSIDE the window (offset 63), never
        // applied, still applies.
        apply(&store, 4, tokened_put("k", 3, 7, 2));
        assert_eq!(store.get("k"), Some(json!(3)));
    }

    #[test]
    fn duplicate_delete_is_skipped() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 1));
        apply(
            &store,
            2,
            Command::Delete {
                key: "k".to_string(),
                session: Some(Session { client: 7, seq: 2 }),
            },
        );
        // Someone else re-creates the key; the replayed duplicate delete
        // must not destroy it.
        apply(&store, 3, tokened_put("k", 9, 8, 1));
        apply(
            &store,
            4,
            Command::Delete {
                key: "k".to_string(),
                session: Some(Session { client: 7, seq: 2 }),
            },
        );
        assert_eq!(store.get("k"), Some(json!(9)));
    }

    #[test]
    fn tokenless_commands_never_touch_the_sessions_table() {
        let store = KvStore::new();
        apply(
            &store,
            1,
            Command::Put {
                key: "k".to_string(),
                value: json!(1),
                session: None,
            },
        );
        // Token-less commands keep at-least-once semantics: re-applying is
        // a second mutation, not a skip.
        apply(
            &store,
            2,
            Command::Put {
                key: "k".to_string(),
                value: json!(2),
                session: None,
            },
        );
        apply(
            &store,
            3,
            Command::Delete {
                key: "k".to_string(),
                session: None,
            },
        );
        assert_eq!(store.get("k"), None);
        assert_eq!(store.export().sessions, HashMap::new());
    }

    /// The method-resolution landmine, pinned: `KvStore::snapshot()` (the
    /// inherent, map-only test helper) and `StateMachine::snapshot()` (the
    /// phase-14 payload) coexist. Concrete calls resolve to the inherent
    /// method; the trait method is reached through `dyn StateMachine` (or
    /// UFCS) and captures map + sessions.
    #[test]
    fn inherent_and_trait_snapshot_methods_coexist() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 1));

        // Inherent: just the map.
        let map: HashMap<String, Value> = store.snapshot();
        assert_eq!(map, HashMap::from([("k".to_string(), json!(1))]));

        // Trait: the full opaque payload, restorable elsewhere.
        let payload: Value = StateMachine::snapshot(&store);
        assert_eq!(
            payload,
            serde_json::to_value(store.export()).unwrap(),
            "trait snapshot is the serialized KvSnapshot"
        );
        let restored = KvStore::new();
        StateMachine::restore(&restored, &payload);
        assert_eq!(restored.export(), store.export());
        // Sessions rode along: the retry still dedups on the restored store.
        apply(&restored, 2, tokened_put("k", 99, 7, 1));
        assert_eq!(restored.get("k"), Some(json!(1)));
    }

    #[test]
    fn export_import_roundtrips_map_and_sessions() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("a", 1, 7, 1));
        store.put("b".to_string(), json!({"x": true}));
        let exported = store.export();

        let restored = KvStore::new();
        restored.import(exported.clone());
        assert_eq!(restored.export(), exported);
        // The restored table keeps deduplicating: client 7's seq 1 replays
        // as a no-op.
        apply(&restored, 2, tokened_put("a", 99, 7, 1));
        assert_eq!(restored.get("a"), Some(json!(1)));
    }
}
