//! Raft consensus core.
//!
//! Phase 1: log entry / hard state types and crash-safe persistence.
//! Later phases add leader election (3), log replication (4), and the
//! state-machine wiring (5). The core never talks to the network directly —
//! only through the transport trait (phase 2).

pub mod storage;
pub mod types;

pub use storage::{Storage, StorageError};
pub use types::{Command, HardState, LogEntry, LogIndex, NodeId, Term};
