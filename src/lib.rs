//! rustkv — a distributed key-value store.
//!
//! The library holds everything testable in-process: the KV state machine, the
//! client-facing HTTP API, and (in later phases) the Raft core, persistence,
//! and the transport trait with its real and simulated implementations. The
//! binary (`src/main.rs`) is a thin shell that wires up config, tracing, and
//! the network.
//!
//! Current status: phase 2 — single-node server, Raft log persistence, and
//! the node-to-node transport abstraction with a deterministic fault-
//! injecting simulator (nothing wired to the server yet; that is phase 5).
//! See PLAN.md for the roadmap.

pub mod api;
pub mod raft;
pub mod rng;
pub mod store;
