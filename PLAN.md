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

## Phase 6 — deterministic fault tests ✅

Done (`tests/faults.rs`; harness: `TestCluster::{crash, restart}` in tests/common/):
- restart(id) = reopen the node's data dir, fresh empty state machine (rebuilt by
  re-applying the log once commit is re-learned), new inbox on the sim network, fresh
  timeout jitter per incarnation.
- Invariants asserted per run: at most one leader per term (sampled continuously);
  no confirmed write lost (exact (term, index, command) present in every final log);
  full convergence (identical logs on disk + identical state-machine snapshots);
  atomicity of unknown-outcome writes (applied everywhere or nowhere); no key
  committed twice (each value proposed at most once by construction).

Scenarios (all virtual-time, deterministic per seed):
- Leader crash mid-write + restart, 5 seeds: confirmed writes survive; the in-flight
  write's fate is atomic across nodes.
- 5× heal-and-re-partition cycles rotating the victim (leader included), 3 seeds,
  2 confirmed writes per cycle; victim reintegrates each time.
- Randomized fault schedule, 8 seeds: 40 steps mixing writes, node isolation/heal,
  crash/restart (≤1 down at a time) under 10% message loss; recovery phase then full
  invariant check. Same-seed run reproduces the identical action/outcome trace and
  final log (determinism test).
- Majority loss: writes stall (no commit for 3 virtual seconds, nothing applied);
  restarting one follower restores the majority and the stalled proposal commits.

Test-the-tests: two hand-run mutations verified the suite has teeth — breaking quorum
(majority()=1) tripped the leader-per-term invariant, the no-commit-without-majority
assert, and the in-node SAFETY VIOLATION fail-stop; disabling the AppendEntries
consistency check was caught by the randomized schedules. Both reverted.

Untested / known gaps:
- Leader-per-term is sampled between driver steps, not event-intercepted; a
  sub-sample flicker could theoretically escape (would need transport-level
  observation hooks — noted for a possible Jepsen phase).
- Client-level retry duplication is out of scope (each value proposed once).
- Fault schedules don't yet vary FaultConfig mid-run (drop-rate spikes) — easy to add
  to the driver if wanted.
- cluster_http real-time tests hardened against CPU starvation (200–400ms election
  timeouts + agreement-based waits) after a cross-binary flake surfaced this phase.

## Phase 7 — real HTTP transport + local/Docker cluster ✅

Done:
- `src/raft/transport/http.rs`: JSON over HTTP/1.1. Outbound is a hand-rolled client
  over TcpStream (no HTTP-client crate on the whitelist): one connection per RPC,
  `Connection: close`, Content-Length/EOF framing, chunked rejected
  (`TODO(perf)` for pooling). Inbound: `POST /raft` axum router feeding the same
  `Inbound` channel as the simulator — the Raft core can't tell transports apart.
  IO/parse/slow failures all map to Timeout; only an unknown id is Unreachable.
- `src/config.rs`: env-based NodeConfig (RUSTKV_NODE_ID / LISTEN / RAFT_LISTEN /
  DATA_DIR / PEERS / PEER_CLIENT_URLS), unit-tested incl. rejection cases.
- Binary runs one member with two listeners (client API + raft RPC); no peers =
  single-node. `scripts/run-cluster.sh` + `make cluster` = local 3-process cluster.
- Dockerfile (multi-stage, builder pinned to rust:1.89.0 matching the toolchain file)
  + compose.yaml with TWO networks: `client` (published ports) and `raft` (peer
  traffic via network-scoped `*-raft` aliases). That split was learned the hard way:
  with one network, `docker network disconnect` also severs the published port, so
  the isolated node's 504 CP behavior can't be observed. `make compose-up/down`.

Tested:
- Unit: HTTP response parsing (content-length, close-delimited, non-200, chunked,
  garbage); NodeConfig parsing.
- tests/http_transport.rs: RPC roundtrip over real sockets; unreachable-vs-timeout
  (unknown id, dead addr, black-holed listener); in-process 3-node cluster over the
  real transport electing, replicating to every state machine, surviving leader crash.
- tests/three_process.rs: three OS processes of the actual binary (CARGO_BIN_EXE),
  driven purely via the client API — formation, redirected writes visible everywhere,
  kill -9 of the leader process, survivor writes, killed node rejoining from its data
  dir with old + new values.
- Manual `make cluster` smoke (PUT/GET/DELETE across processes with redirects).
- Docker (daemon started locally, image built and run): compose cluster formation,
  replication; leader partitioned via `docker network disconnect` → client-visible
  504, doomed key never committed anywhere, majority kept serving, heal converged
  (doomed truncated, partition-era write everywhere); `docker restart` persistence.

Untested / known gaps:
- Docker verification was manual (this machine, daemon 24.0.5) — not automated in
  `make test`; a scripted compose partition test would need the daemon in CI.
- Compose healing requires re-adding the `--alias nodeN-raft` (documented in README);
  omitting it leaves the node unresolvable by peers.
- No TLS/auth on the raft port and no connection pooling (out of scope; TODOs).
- Follower reads remain eventually consistent (documented since phase 5).

## Phase 8 — Jepsen-style consistency harness ✅ (approved by user)

Built natively in Rust on the deterministic simulator instead of the Clojure
framework: real Jepsen would add a JVM/SSH/VM stack, and our simulator gives
something Jepsen cannot — every history is a pure function of its seed, so any
violation replays exactly. Trade-off noted below.

Done (`tests/common/lin.rs`, `tests/jepsen.rs`):
- Wing & Gong linearizability checker for a per-key last-write-wins register
  (Put/Delete/Get), compositional per key, memoized DFS over (mask, state).
  Jepsen-equivalent outcome semantics: ok = must linearize; fail = definitely
  didn't happen (excluded); unknown (client timeout) = takes effect any time
  after invocation or never (return = ∞, optional to linearize).
- Workload driver: 4 concurrent client processes × 12 randomized ops over 3 keys
  (reads from random nodes, writes via the visible leader with ok/fail/unknown
  recording + (term,index) tags) while a nemesis partitions/heals random nodes;
  Jepsen-style final reads after heal+convergence pin down unknown writes.
- Checked claims across seeds:
  * checker validation: hand-crafted valid histories accepted, invalid ones
    (stale read, read-through-delete, phantom value, failed-write visible,
    non-monotonic reads) rejected — the checker has teeth;
  * write linearizability via the log witness: every confirmed write present at
    its assigned (term, index) with the right command, identical logs, and log
    order consistent with real-time order (the log IS the linearization);
  * same seed ⇒ byte-identical history and logs;
  * full histories with local reads: the checker finds real, replayable
    stale-read violations under partitions (e.g. seed 0: a client's committed
    Delete at t=517ms followed 5ms later by its own read returning the deleted
    value from a lagging node). This is the documented non-linearizable-read
    design made precise — and the fix (ReadIndex/leases) is now specified by a
    failing-check-away if ever wanted.

Untested / known gaps:
- This is not the Clojure Jepsen: no Elle transactional anomalies checker, no
  real-VM/SSH nemeses (real-network partitions are covered manually via Docker,
  phase 7), no wall-clock-skew faults (the sim has one clock by construction).
- Nemesis here is partition-only (crash/restart schedules live in tests/faults.rs;
  combining both under the linearizability checker would be a natural extension).
- The checker caps at 63 ops per key (u64 mask) — sized to the workload.

## Phase 9 — linearizable reads via ReadIndex ✅

Done (`src/raft/node.rs`, `src/kv.rs`, `src/api.rs`):
- ReadIndex (§6.4) with zero wire changes: every outbound AppendEntries is
  tagged with a local monotonic `heartbeat_seq`; a read registered at seq `s`
  (which bumps the seq and broadcasts an AE round immediately) is
  leadership-confirmed once a majority — self included — has answered an AE
  sent at seq >= `s`. Any reply at the leader's term counts, including a
  log-mismatch rejection (it still acknowledges authority).
- §6.4 no-op gate: `Role::Leader` records `term_start_index` (the election
  no-op's index); a read's index is `max(commit_index, term_start_index)`,
  captured once at registration, and the ticket resolves only when
  `last_applied` reaches it — a fresh leader can't serve state it doesn't yet
  know is committed.
- Step-down safety: pending reads live INSIDE `Role::Leader` (unlike `pending`
  proposals, which deliberately survive step-down), so `become_follower`
  drops their oneshot senders — waiters get a retryable error promptly,
  never a hang or a stale value. `RaftHandle::read() -> ReadTicket`.
- `KvNode::get_linearizable` (`ReadError::{NotLeader, Timeout, Retry,
  Shutdown}`); reuses the write timeout. `KvNode::get` stays as the local path.
- HTTP: `GET /{key}` is now linearizable by default — non-leaders 307 to the
  leader (shared redirect helper with writes), unconfirmable reads 504,
  step-down 503; `GET /{key}?stale=true` keeps the old local read as an
  explicit opt-in. New `GET /cluster/status` (id/term/role/leader/commit) —
  under `/cluster/` so no single-segment key is shadowed.

Tested (80 total; 7 new):
- tests/read_index.rs (sim, seeded, virtual time): single-node immediate
  grant; grants reflect committed writes; follower NotLeader + hint; the
  §6.4 gate observable under slow links (read registered while the no-op is
  uncommitted stays pending, grants after commit); the money test — a
  minority-partitioned leader holding a provably stale value accepts a read
  but never confirms it (3 virtual seconds), and healing resolves the hung
  ticket as an error via step-down, with a retry on the new leader seeing
  the new value.
- tests/jepsen.rs: `run_workload` parametrized by ReadMode. Stale mode keeps
  the pre-phase-9 behavior byte-identical (the stale-violation test still
  proves the checker catches real staleness). NEW
  `linearizable_reads_pass_the_checker`: same seeds/nemesis/client mix with
  reads through ReadIndex — the WGL checker finds ZERO violations across all
  seeds (with a guard against vacuous success). This is the phase's headline:
  the fix is validated by the exact harness that demonstrated the bug.
- tests/cluster_http.rs: follower GET 307 + follow-redirect, `?stale=true`
  local reads (the per-node replication waits now use it on purpose),
  partitioned leader answers 504 to linearizable GET while stale GET still
  serves, `/cluster/status` smoke. Manual binary smoke (status/put/both
  GET modes/404).

Untested / known gaps:
- The `term_start_index` gate's exotic branch — a read confirmed purely by
  log-mismatch rejection acks before the no-op commits — is not specifically
  exercised (needs a diverged-follower + timing setup); the common path is.
- Reads carry no dedup/session tokens (irrelevant: reads are side-effect-free).
- Real-time cluster_http tests remain subject to the documented cross-binary
  CPU-starvation flake class (one occurrence seen during a full parallel run
  this phase; passes in isolation and on re-run).

## Phase 10 — harness hardening ✅

Done:
- `TestCluster` interior mutability (tests/common/mod.rs): nodes/stores
  behind `Mutex` holding `Arc` clones, `incarnation: AtomicU64`,
  `restart(&self, id)` — so concurrent workload/nemesis tasks can share the
  cluster via `Arc` and crash/restart nodes mid-run. New `crashed` tracker +
  `alive_ids()`: a crashed node's status watch freezes at its last value, so
  workloads must exclude it from leader sampling. Mechanical call-site
  updates across election/replication/faults/jepsen/read_index tests.
- Jepsen nemesis (tests/jepsen.rs) now rolls each round between
  partition/heal and crash-then-restart (at most one node down, restarted
  before the round ends, so the final heal always finds everyone running).
  `run_workload` reports the crash-round count and the linearizable-mode
  tests assert it's nonzero across their seed set — crash coverage can't
  silently vanish in a future seed re-pin.
- Sim message duplication (src/raft/transport/sim.rs):
  `FaultConfig.duplicate_probability` (default 0.0). Exactly two extra
  unconditional RNG draws per send (duplicated? + duplicate delay) inside
  the existing critical section, preserving the fixed-draw-count determinism
  contract. The duplicate is a fire-and-forget second `Inbound` with a
  throwaway reply oneshot on its own delay — it shares nothing with the
  primary exchange's timeout, so it can arrive after the sender gave up;
  it is delivered even if the primary leg is dropped, but never through a
  blocked link.
- Event-level safety observer (closes the phase-3/6 sampling gap): the
  sim's send critical section inspects every AppendEntries crossing the
  network for THREE order-independent content invariants — Election Safety
  (§5.2: one leadership claimant per term), Log Matching (§5.3: one command
  per (term, index), ever, across all leaders/retransmits/duplicates), and
  message well-formedness (entries contiguous from prev_log_index, terms
  non-decreasing and never above the sender's). Violations are recorded,
  not panicked (sends run in spawned tasks); `SimNetwork::
  safety_violations()` is asserted empty by `TestCluster::shutdown()`, so
  every sim-cluster test checks all three at teardown. The sampled
  `observe_leaders` checks in faults.rs are gone (election.rs keeps its
  sampling test as a redundant cluster-level check). No Raft-core hook: the
  core still never knows about the network. Deliberately NOT checked:
  sequencing invariants like leader_commit monotonicity — send-observation
  order is task-scheduling order, not core-emission order, so they cannot
  be asserted soundly here.

Tested (88 total; 8 new):
- Sim unit: duplicate_probability=1.0 delivers exactly two copies and the
  primary exchange still succeeds; per-seed reproducibility of outcomes AND
  receiver arrival counts under drop+duplication; checker-has-teeth — two
  forged AppendEntries (same term, different leader_id) through bare
  registered senders record exactly one violation, idempotent/other-term/
  RequestVote claims record none, and a claim through a blocked link still
  records (a send is a claim regardless of delivery); log-matching teeth —
  a forged different command at a seen (term, index) records, while exact
  retransmits and a conflict-overwrite under a NEW term (legal) do not;
  malformed-AE teeth — gap after prev, entry term above the sender's, and
  terms decreasing along a batch each record.
- Teardown wiring has teeth end-to-end: a `#[should_panic]` test forges
  conflicting leadership claims into a LIVE cluster's network via a bare
  registered transport and `TestCluster::shutdown()` must refuse to pass.
- Duplication soaks at 0.1: the randomized fault schedules (8 seeds) and
  the full jepsen linearizable workload (6 seeds, log witness + WGL checker,
  zero violations) both pass with 10% of requests delivered twice.
- Crash/restart nemesis: `linearizable_reads_pass_the_checker` (and the
  soak) now run partitions+crashes; 5 of 6 seeds roll ≥1 crash round.
  Jepsen determinism re-pinned on seed 3 (1 crash round, dup 0.1); faults
  determinism on seed 5 now at dup 0.1.
- Test-the-tests mutation re-run: `majority()=1` trips the NEW event-level
  assert at teardown ("nodes 2 and 3 both sent AppendEntries as leader of
  term 2") in a suite whose sampled check was removed. Reverted. A second
  hand-run mutation (`entries_from(next + 1)`) was instructive: it produced
  EMPTY suffixes, i.e. a pure liveness bug shipping no malformed content —
  tests hang on commit-awaits instead of reaching the teardown assert. The
  observer checks message contents, not progress; liveness failures still
  surface as the suite's existing wait/commit timeouts.

Seed churn (expected — two extra draws per send shift every schedule):
every seeded suite was re-run and passed WITHOUT re-pinning; the stale-mode
jepsen check now finds violations on all 6 seeds (previously a subset), and
all other seed-pinned expectations (faults.rs counts, election/replication
outcomes) held as written.

Untested / known gaps:
- Reply legs are never duplicated (requests only); the Raft core tolerates
  duplicate replies by design (stale-reply folding) but the sim doesn't
  exercise that path.
- The duplicate copy skips the drop draw: a duplicated request always lands
  once the link allows it (drop applies to the primary only) — documented
  semantics, not a bug, but a different model than two fully independent
  copies.
- The safety observer inspects AppendEntries only; other RPC variants —
  including phase 11's PreVote — fall through its match and are ignored by
  construction (no sim change needed when PreVote lands).
- The observer cannot catch under-sending/liveness bugs (see the
  `entries_from(next + 1)` mutation note above) or sequencing violations
  (leader_commit monotonicity) — the former is covered by commit/convergence
  timeouts, the latter has no sound send-side observation point.

## Project complete (phases 0-10)
Remaining ideas beyond the original scope, in planned order: PreVote,
client dedup tokens for 504 retries, snapshotting/compaction +
InstallSnapshot, dynamic membership, connection pooling, scripted Docker
partition test. (TLS on the raft port: dropped — blocked on the dependency
whitelist.)

## Out of scope (deliberate)
Snapshotting/log compaction, dynamic membership changes — leave clean TODOs.
