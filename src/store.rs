//! In-memory key-value state machine.
//!
//! In phase 0 the HTTP handlers mutate this directly. From phase 5 on, all
//! mutations arrive by applying committed Raft log entries, and this type
//! becomes the state machine behind the log.

use std::collections::HashMap;
use std::sync::RwLock;

use serde_json::Value;

/// Thread-safe map of string keys to arbitrary JSON values.
///
/// Uses a std (not tokio) `RwLock`: guards are never held across an `.await`.
#[derive(Debug, Default)]
pub struct KvStore {
    map: RwLock<HashMap<String, Value>>,
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
}
