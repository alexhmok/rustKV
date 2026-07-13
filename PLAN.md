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

## Phase 2 — transport trait + deterministic simulator ✅

Done (`src/raft/rpc.rs`, `src/raft/transport/{mod,sim}.rs`, `src/rng.rs`):
- RPC message shapes (RequestVote/AppendEntries args+replies; semantics come in 3/4).
- `Transport` trait (outbound: `send(to, req) -> Result<RpcResponse, TransportError>`)
  plus the `Inbound{from, request, reply: oneshot}` type both transports deliver on an
  mpsc channel — the Raft core's event loop will `select!` on it, owning no network code.
- `SimNetwork`/`SimTransport` (in the lib proper): seeded per-leg delay + drop, RPC
  timeout, runtime-swappable `FaultConfig`, directed link blocking (phase 6 partition
  building block), crash-as-black-hole (drop the inbox receiver), node re-registration
  for restarts. All randomness drawn up front per send (fixed draw count → stable traces).
- Reordering is emergent from independent per-message delays (asserted by test).
- `SplitMix64` PRNG hand-rolled in the lib (`rand` not whitelisted); verified against
  Vigna's reference vectors. Tokio `test-util` (dev-only) enables virtual time.

Tested (13 new): PRNG reference vectors/determinism/bounds; exact virtual-time
roundtrip (both legs); drop-all → timeout at exactly rpc_timeout; unreachable vs
timeout semantics; crashed node black-holes; block/unblock recovery; runtime config
swap; same seed → byte-identical 20-message lossy trace, different seeds diverge;
concurrent sends provably reorder and reproducibly so per seed.

Untested / known gaps:
- Determinism holds on a current-thread paused-time runtime (the phase 3-6 test
  harness); not guaranteed on a multi-threaded runtime.
- No bandwidth/duplicate-message modeling (Raft must tolerate duplicates anyway —
  phase 4's idempotent AppendEntries handling covers it).
- Trait shape may grow (e.g. broadcast helpers) when election lands.

## Phase 3 — leader election ✅

Done (`src/raft/node.rs`):
- `RaftNode`: one event-loop task owning all consensus state (storage, role,
  timers) — no locks. Inbound RPCs via the transport channel; outbound RPCs from
  short-lived tasks reporting term-tagged replies back on an internal channel
  (stale replies discarded); observers read a `watch` channel of `Status`.
- Elections per §5.2: randomized timeouts (seeded jitter RNG), RequestVote with
  majority counting, §5.4.1 election restriction (last-term/last-index compare),
  idempotent vote grants, term adoption + step-down on any higher term.
- Heartbeats: empty AppendEntries with real (trivial while logs are empty)
  prev_log consistency check; candidates step down on AE from a legit leader.
- Persistence before replying: term/vote fsynced via phase 1 Storage; storage
  failures are fail-stop (panic). Determinism: `select! { biased; .. }` because
  tokio's randomized branch polling would break seed-reproducibility.
- `RaftHandle`: status/watch, graceful shutdown, `crash()` (abort + inbox drop).

Tested (10 integration tests, virtual time, all seeded): exactly-one-leader +
full convergence across 10 seeds; same seed ⇒ same (leader, term); 10 virtual
seconds heartbeat stability; leader crash ⇒ re-election at higher term; leader
partition ⇒ deposed, heals as follower, one leader after reconvergence; isolated
follower churns terms but never wins, majority undisturbed, cluster reconverges
after heal (known basic-Raft rejoin churn — no PreVote, by scope); at most one
leader per term across 5 seeds under 25% message loss (10ms sampling); RPC-level
vote rules (idempotent re-grant, competing candidate refused, stale term told
current term) + votedFor/term survive restart; RPC-level §5.4.1 matrix.

Untested / known gaps:
- One-leader-per-term is asserted by 10ms status sampling, not event-level
  observation — phase 6 tightens this.
- No PreVote/CheckQuorum (out of scope): a rejoining node's inflated term causes
  one round of re-election churn (tested as expected behavior).
- AppendEntries carries no entries yet; commit_index never advances (phase 4).

## Phase 4 — log replication ✅

Done (`src/raft/node.rs`, `src/raft/storage.rs::entries_from`):
- Follower §5.3: prev_log consistency check; duplicate-tolerant entry walk (skip
  already-held, truncate suffix at first conflict — fail-stop assert that committed
  entries are never truncated — append the rest); commit advance capped at
  min(leader_commit, prefix verified by this RPC).
- Leader: `Role::Leader { next_index, match_index }` (re-initialized per election);
  every AppendEntries reply is tagged with (term, prev, len) sent, so reordered/stale
  replies fold in safely (match via max, backtracking via min); rejection ⇒ next_index
  steps below the failing prev and resends immediately; success with a still-lagging
  peer resends immediately (catch-up isn't heartbeat-paced).
- Commit: majority match (leader's own log counted), §5.4.2 current-term rule —
  prior-term entries commit only transitively.
- `RaftHandle::propose(command) -> (term, index)` = durably appended on the leader,
  NOT yet committed; commitment observable via Status (commit_index/last_log_index
  added). Non-leaders reject with a leader hint. Phase 5 wires clients through this.
- Deliberate basic-Raft gaps (documented in node.rs): no no-op entry on election win,
  no conflict-hint fast backtracking (linear next_index decrement), no AE batching cap.
- Test helpers extracted to tests/common/ (shared by election/replication/phase-6).

Tested (8 integration + 1 unit, seeded virtual time): propose→commit→identical disk
logs on all nodes; NotLeader + hint; lagging-follower catch-up through partition/heal;
Figure-7-style divergence — minority leader's uncommitted entries truncated and
replaced after heal (exercises full backtracking chain); minority leader accepts but
never commits across 3 virtual seconds, doomed entry absent everywhere after heal (CP);
same seed ⇒ identical (leader, term, byte-identical logs); 10 confirmed writes survive
sustained 15% loss across 3 seeds with unknown-outcome retry semantics; RPC-level AE
conformance (idempotent duplicates, commit capped at verified prefix, gap rejection,
stale term, conflict overwrite verified on disk).

Untested / known gaps:
- Message duplication by the transport itself (AE handler is duplicate-tolerant and
  that code path is tested, but the simulator never duplicates in flight).
- Crash/restart mid-replication and event-level invariant checks — phase 6.
- Client-visible semantics of "accepted but never committed" — phase 5 (writes will
  block on commit, so minority-side clients time out instead of seeing success).

## Phase 5 — KV on top of Raft ✅

Done (`src/kv.rs`, `src/api.rs`, `src/main.rs`, node/store changes):
- `StateMachine` trait; committed entries applied in log order on every node (KvStore
  impl; Noop skipped). Apply + commit-notification live in the Raft event loop.
- `RaftHandle::propose` now returns a `Proposal` whose `committed` oneshot resolves
  true (committed + applied locally) or false (truncated by another leader — definitely
  not applied); unresolved = caller times out (that IS the CP behavior).
- §8 leadership no-op (`Command::Noop`): each election win appends one, so prior-term
  entries — and the KV state after restart — commit without client traffic. This closed
  the phase-4 "prior-term entries commit late" gap and shifted log indexes in tests.
- `KvNode.write`: propose → await commit with timeout → WriteError::{NotLeader(hint),
  Timeout(outcome unknown), Superseded(safe retry), Shutdown}.
- HTTP: PUT 201 / DELETE 204 only after commit; non-leaders send 307 + Location to the
  leader's client URL (redirect chosen over proxy-forwarding; peer URL map filled from
  config in phase 7), 503 no-leader/superseded (retryable), 504 unconfirmed (ambiguous).
  GETs stay local: may be stale on followers; linearizable reads out of scope (TODO).
- Binary = persistent single-node cluster (RUSTKV_DATA_DIR, default ./rustkv-data);
  state rebuilt from the log on startup via the no-op re-commit.

Tested (55 total; 12 new/e2e): all phase-4 scenarios re-verified with state-machine
assertions (identical snapshots on all nodes; orphan/doomed keys never applied;
commit-notification true/false semantics); RPC-level apply timing (nothing applied
before commit; truncated entry never applied); single-node HTTP contract (7 tests,
now raft-backed) + KV state surviving restart over the same data dir; 3-node clusters
with real HTTP client APIs over the simulated transport: leader writes visible
everywhere, raw 307 + Location and auto-followed redirects for PUT and DELETE,
minority-partitioned leader → 504 with the doomed key never appearing anywhere, and
majority-side recovery after heal. Manual binary smoke test incl. restart.

Untested / known gaps:
- Reads are not linearizable and this is documented, not fixed (no ReadIndex/leases).
- Client-visible retry ambiguity on 504 (write may commit later) is inherent; no
  client-side dedup tokens (would be a later phase / Jepsen finding).
- HTTP cluster tests are real-time and serialized (a shared mutex) — poll-based, not
  seed-deterministic; the deterministic coverage lives in the sim-transport suites.
- Location header embeds keys as-is; exotic key encodings unhandled (noted in code).

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
