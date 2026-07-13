//! rustkv — a distributed key-value store.
//!
//! The library holds everything testable in-process: the KV state machine, the
//! client-facing HTTP API, and (in later phases) the Raft core, persistence,
//! and the transport trait with its real and simulated implementations. The
//! binary (`src/main.rs`) is a thin shell that wires up config, tracing, and
//! the network.
//!
//! Current status: phase 5 — client writes go through the replicated log
//! (majority commit before the HTTP response), committed entries apply to
//! the KV state machine on every node, and non-leaders redirect writes to
//! the leader. The node-to-node HTTP transport is phase 7; until then
//! multi-node clusters exist only on the simulated transport. See PLAN.md
//! for the roadmap.

pub mod api;
pub mod kv;
pub mod raft;
pub mod rng;
pub mod store;
