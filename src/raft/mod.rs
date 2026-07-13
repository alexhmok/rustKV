//! Raft consensus core.
//!
//! Phases so far: log/hard-state types and crash-safe persistence (1), RPC
//! message shapes and the transport abstraction with a deterministic
//! simulator (2). Later phases add leader election (3), log replication (4),
//! and the state-machine wiring (5). The core never talks to the network
//! directly — only through the transport trait.

pub mod node;
pub mod rpc;
pub mod storage;
pub mod transport;
pub mod types;

pub use node::{RaftConfig, RaftHandle, RaftNode, RoleKind, Status};
pub use storage::{Storage, StorageError};
pub use transport::{Inbound, Transport, TransportError};
pub use types::{Command, HardState, LogEntry, LogIndex, NodeId, Term};
