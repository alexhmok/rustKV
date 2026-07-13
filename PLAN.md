# rustkv — phase plan and progress

Source of truth for what is done, tested, and outstanding. Checkpoint with the user
after each phase. Rules that apply to all phases live in CLAUDE.md.

## Phase 0 — scaffold + single-node store ✅ (this checkpoint)

Done:
- Repo scaffold: lib+bin split, whitelist-only Cargo.toml, committed Cargo.lock,
  rust-toolchain.toml (1.89.0 + clippy/rustfmt), .gitignore, Makefile
  (build/test/fmt/lint/run), README.md, CLAUDE.md, git history.
- Single-node store: `KvStore` (in-memory `RwLock<HashMap<String, Value>>`) behind
  the exact HTTP contract (GET 200/404, PUT 201/400, DELETE 204) with `tracing`
  structured logging and graceful shutdown.

Tested:
- Unit: store put/get/overwrite/delete.
- Integration (real server on ephemeral port, reqwest): 404 miss, put/get roundtrip,
  overwrite, delete + delete-idempotency, invalid-JSON → 400, missing Content-Type
  accepted.

Untested / known gaps:
- Concurrency beyond RwLock's guarantees; large bodies; unusual key encodings.
- Decisions taken where the contract was silent: PUT returns 201 on overwrite too;
  DELETE is idempotent (204 for absent keys); any JSON value (not just objects) is
  accepted; keys are single path segments.

## Phase 1 — log + persistence types ✅

Done (`src/raft/types.rs`, `src/raft/storage.rs`):
- Types: `Command { Put, Delete }`, `LogEntry { term, index, command }`,
  `HardState { current_term, voted_for }`; `NodeId`/`Term`/`LogIndex` aliases
  (indexes 1-based, 0 = sentinel).
- `Storage::open(dir)`: creates/loads `hard_state.json` + `log.jsonl`, replays and
  validates the log (contiguous indexes), keeps an in-memory mirror.
- Durability: hard state written atomically (temp → fsync → rename → fsync dir); log
  appends are newline-delimited JSON fsynced before returning; `truncate_from` is an
  atomic whole-file rewrite (`TODO(compaction)` marks where snapshots would change this).
- Torn-write repair: an unparsable or newline-less FINAL line is un-acked by
  construction → dropped and the file truncated; anywhere else fails as `Corrupt`.
- Sync `std::fs` I/O by design; the Raft core will own storage from its own task.
- tempfile added as dev-dependency (test temp dirs).

Tested (10 unit tests): fresh-dir defaults; hard state and log survive reopen;
non-contiguous appends rejected (incl. gap inside a batch, nothing written); truncate
durably drops suffix and indexes are reusable; truncate past end is a no-op,
truncate(0) rejected; torn final line dropped + file repaired (partial line, and a
complete-but-garbage line); corrupt middle line and on-disk index gap fail loudly.

Untested / known gaps:
- No real power-loss testing — crash tolerance is simulated by hand-corrupting files.
- fsync guarantees are whatever `File::sync_all` provides per platform.
- Concurrent access (single-owner by design), very large logs (no compaction).
- Not wired into the server yet — that is phase 5.

## Phase 2 — transport trait + deterministic simulator ⬜

Planned approach:
- Trait owned by the Raft core, roughly
  `async fn send(&self, to: NodeId, req: RpcRequest) -> Result<RpcResponse, TransportError>`;
  exact shape may adjust when election lands.
- `SimTransport` in the library proper (not #[cfg(test)]): in-process registry of node
  handlers; seeded per-message delay/drop/reorder decisions.
- PRNG hand-rolled (~20-line SplitMix64/xorshift) — `rand` is not on the runtime
  whitelist and the simulator ships in the lib.
- Determinism: `#[tokio::test(start_paused = true)]` (current-thread runtime, virtual
  time) so a seed fully reproduces a schedule.

## Phase 3 — leader election ⬜
Terms, randomized timeouts, RequestVote, majority, election restriction (§5.4.1).
Tested on the simulated transport.

## Phase 4 — log replication ⬜
AppendEntries, log-matching/backtracking, commit on majority, current-term commit rule
(§5.4.2). Tested on the simulated transport.

## Phase 5 — KV on top of Raft ⬜
Apply committed entries to the KV map; client writes go through the log and commit to
a majority before responding; leader forwarding/redirect for non-leaders. End-to-end
tests.

## Phase 6 — deterministic fault tests ⬜
Partitions, leader crash/restart mid-write, heal-and-re-partition across seeds.
Safety invariants asserted: at most one leader per term; no committed write lost.

## Phase 7 — real HTTP transport + local/Docker cluster ⬜
HTTP transport between nodes; 3-node local run (three processes); Dockerfile + Compose
cluster for partition testing (Docker is deferred until here; user installs the daemon).

## Phase 8 — Jepsen harness (optional) ⬜
Do NOT start without explicit user approval.

## Out of scope (deliberate)
Snapshotting/log compaction, dynamic membership changes — leave clean TODOs.
