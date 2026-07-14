//! In-memory key-value state machine.
//!
//! All mutations arrive by applying committed Raft log entries (the
//! [`StateMachine`] impl); the HTTP layer only reads it directly.
//!
//! Dedup (phase 13): commands carrying a [`Session`] token are applied
//! exactly once — a `sessions` table maps each client to its highest
//! applied `seq`, and a command at or below that seq skips the mutation
//! (the duplicate log entry still committed; only its application is a
//! no-op). The table lives IN the state machine so it is rebuilt by log
//! replay after a restart and will ride along in phase 14's snapshot
//! ([`KvSnapshot`]). There is deliberately NO expiry: a per-node TTL would
//! diverge replicas (apply must stay a pure fold of the log). The table
//! therefore grows unboundedly with the number of distinct clients — a
//! documented gap, under the stated assumption of one outstanding op per
//! client. No results are cached (Put/Delete return unit).

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::raft::node::StateMachine;
use crate::raft::types::{Command, LogEntry, Session};

/// The full state a snapshot must capture (phase 14's payload, shaped now):
/// the KV map AND the dedup sessions — forgetting the latter would resurrect
/// skipped duplicates on a snapshot-restored node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvSnapshot {
    pub map: HashMap<String, Value>,
    pub sessions: HashMap<u64, u64>,
}

/// Thread-safe map of string keys to arbitrary JSON values, plus the dedup
/// sessions table (client → highest applied seq).
///
/// Uses std (not tokio) `RwLock`s: guards are never held across an `.await`,
/// and the two locks are never held at the same time (apply touches them
/// one after the other; export/import go map first, then sessions).
#[derive(Debug, Default)]
pub struct KvStore {
    map: RwLock<HashMap<String, Value>>,
    sessions: RwLock<HashMap<u64, u64>>,
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

    /// True if a tokened command was already applied: its seq is at or
    /// below the client's highest applied one. Token-less commands are
    /// never duplicates.
    fn already_applied(&self, session: Option<Session>) -> bool {
        let Some(Session { client, seq }) = session else {
            return false;
        };
        self.sessions
            .read()
            .expect("sessions lock poisoned")
            .get(&client)
            .is_some_and(|&applied| seq <= applied)
    }

    /// Records a tokened command as applied. Only called after
    /// [`Self::already_applied`] returned false, so a plain insert is a
    /// max-update.
    fn record_applied(&self, session: Option<Session>) {
        if let Some(Session { client, seq }) = session {
            self.sessions
                .write()
                .expect("sessions lock poisoned")
                .insert(client, seq);
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

    #[test]
    fn duplicate_seq_skips_the_mutation() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 1));
        // Interleaved conflicting write, then the duplicate: the duplicate
        // must NOT clobber it (an LWW map makes k=1-over-k=1 invisible).
        apply(&store, 2, tokened_put("k", 2, 8, 1));
        apply(&store, 3, tokened_put("k", 1, 7, 1));
        assert_eq!(store.get("k"), Some(json!(2)));
        assert_eq!(store.export().sessions, HashMap::from([(7, 1), (8, 1)]));
    }

    #[test]
    fn lower_seq_skips_the_mutation() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 3, 7, 3));
        apply(&store, 2, tokened_put("k", 1, 7, 1));
        assert_eq!(store.get("k"), Some(json!(3)));
        assert_eq!(store.export().sessions, HashMap::from([(7, 3)]));
    }

    #[test]
    fn higher_seq_applies_and_advances_the_session() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 1));
        apply(&store, 2, tokened_put("k", 2, 7, 2));
        assert_eq!(store.get("k"), Some(json!(2)));
        assert_eq!(store.export().sessions, HashMap::from([(7, 2)]));
    }

    #[test]
    fn clients_are_independent() {
        let store = KvStore::new();
        apply(&store, 1, tokened_put("k", 1, 7, 5));
        // Same seq, different client: applies.
        apply(&store, 2, tokened_put("k", 2, 8, 5));
        assert_eq!(store.get("k"), Some(json!(2)));
        assert_eq!(store.export().sessions, HashMap::from([(7, 5), (8, 5)]));
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
