# rustkv — durable project rules

PLAN.md is the source of truth for phase progress. Update it as work completes.

## Hard dependency constraint

`[dependencies]` may ONLY contain: tokio, axum, serde, serde_json, tracing,
tracing-subscriber. No consensus/raft crates, no RPC frameworks (tonic etc.), no rand,
no clap — Raft, node-to-node transport, PRNGs, and arg parsing are implemented by hand.
Check this list before touching Cargo.toml. `[dev-dependencies]` (test-only) are
allowed but kept minimal (currently: reqwest).

## Architecture

- Lib + thin bin. All logic (Raft core, KV state machine, API) lives in the library,
  testable in-process; `main.rs` only wires config, tracing, and the network.
- The Raft core NEVER talks to the network directly — only through the transport
  trait. Two impls: real HTTP (tokio/axum, phase 7) and an in-memory simulator with
  seeded delay/drop/reorder for deterministic tests (phase 2).
- Fixed cluster membership from config. No snapshotting/compaction, no dynamic
  membership — leave `TODO` markers where they'd go.
- Durability: persist term/votedFor/log to disk (fsync) before responding to RPCs;
  rebuild commit index and KV state by replaying the log on startup.

## Engineering standards

- `tracing` with structured fields (node id, term, role) — never `println!`.
- `make lint` (fmt --check + clippy --all-targets -D warnings) must pass at every
  checkpoint; `rust-toolchain.toml` pins the toolchain; Cargo.lock stays committed.
- Test before claiming: never state a behavior works without a test demonstrating it;
  explicitly call out anything untested or happy-path-only.
- Phase discipline: finish the current phase, verify, commit, update PLAN.md, then
  STOP for a checkpoint with the user. Do not start the next phase unasked.
  Phase 8 (Jepsen) must not be started without explicit approval.
