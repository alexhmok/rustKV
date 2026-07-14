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

## Phase 11 — PreVote ✅

Done (`src/raft/rpc.rs`, `src/raft/node.rs`):
- New RPC variants `RpcRequest::PreVote(RequestVoteArgs)` /
  `RpcResponse::PreVote(RequestVoteReply)` — the RequestVote payloads
  reused under distinct variants, so a probe is structurally impossible to
  conflate with a binding vote. Serde gives wire compat for free (the HTTP
  transport needed zero changes; three_process proves interop).
- `Role::PreCandidate { votes }` + `RoleKind::PreCandidate` (surfaces in
  `/cluster/status` via the existing Debug rendering). Election timeout now
  starts a *pre-campaign*: NO term bump, NO persistence, probes carry the
  prospective term `current_term + 1` while the node's own term stays
  untouched. A pre-vote majority triggers the old election body (now
  `start_election`): durable term+1 + self-vote, become Candidate.
- Grant rule (`handle_pre_vote`): prospective term must exceed ours AND the
  §5.4.1 log tuple compare (same as a real vote) AND leader stickiness —
  denied while this node IS the leader or heard a valid AppendEntries
  within `election_timeout_min` (`last_leader_contact`, set in
  `handle_append_entries` even on log-mismatch rejections). The
  leader-denies half is load-bearing: without it, in a 3-node cluster the
  leader itself would hand a healed up-to-date node its pre-vote majority.
  Granting records nothing, adopts nothing, and — unlike a real vote —
  never resets the grantor's election timer.
- `Event::PreVoteReply { sent_term (prospective), from, result }`. Guards
  before counting: still PreCandidate AND `sent_term == current_term + 1`;
  a denial carrying a higher term → `become_follower` (how a term-lagged
  node catches up and becomes grantable next timeout). The REAL RequestVote
  handler is deliberately unchanged (not sticky, still adopts terms); its
  timer-reset liveness note now points at PreVote as the mitigation.

Tested (93 total; 5 new, 1 inverted):
- The headline INVERTS phase 3's isolated-follower test: 5 virtual seconds
  of isolation (20+ timeouts) and the follower's term NEVER advances (it
  sits in PreCandidate); on heal the leader keeps leading and the cluster
  term is byte-identical — the churn the old test asserted as expected
  behavior is gone.
- RPC-level grant matrix (prepared storage): stale/short log denied,
  prospective term not beyond current denied, up-to-date granted — to
  multiple askers (no one-grant-per-term rule) — with the term provably
  unmoved throughout, and the real term-3 vote still grantable after.
- Stickiness: heartbeat a passive node, then an up-to-date pre-vote is
  denied while a REAL RequestVote for the same term still succeeds.
- Timer independence: a node fed a continuous stream of grantable probes
  still starts its own pre-campaign within election_timeout_max.
- Cold start: 3 seeds elect through pre-vote from a never-led cluster
  (stickiness can't deadlock the first election).
- Sim unit test pins that PreVote traffic is invisible to the phase-10
  safety observer (conflicting-looking probes record nothing; a real
  conflicting AE afterwards still records) — the observer ignores
  non-AppendEntries by construction, as phase 10 predicted.
- Full regression (faults + jepsen + duplication soaks + linearizable
  checker + three OS-process cluster) passed WITHOUT re-pinning any seed:
  stale mode still finds violations on all 6 seeds, both duplication soaks
  and the zero-violation linearizable claim hold, and the crash-round
  guards (nemesis-RNG-driven, schedule-independent) were unaffected.

Untested / known gaps:
- No CheckQuorum: an isolated leader still believes it leads its old term
  (partitioned_leader test's inline comment remains true; harmless — its
  writes can't commit and it steps down on first contact).
- `last_leader_contact` is volatile: a freshly restarted node may grant a
  pre-vote inside what would have been the stickiness window. Harmless —
  a probe majority still needs real votes to matter.
- Stale grant replies from an earlier pre-campaign round count toward a
  later round with the same prospective term (grants are non-binding, so
  this affects nothing safety-relevant; noted for precision).

## Phase 12 — CheckQuorum ✅

PreVote's matched pair: phase 11's stickiness suppressed the disruptive
term churn that used to rescue basic Raft from some asymmetric partitions;
CheckQuorum restores that liveness without re-admitting the disruption.
This closes the phase-11 "no CheckQuorum" gap (left in phase 11's list
above as the historical record).

Done (`src/raft/node.rs` only):
- `Role::Leader` gains `last_contact: HashMap<NodeId, Instant>`, updated
  at exactly the site where `acked_seq` updates (any AppendEntries reply
  at our term — success or log-mismatch rejection — is contact; never
  derived from match_index, since a rejecting peer is still reachable),
  and initialized to leadership start so a fresh leader can't be deposed
  before its first acks could possibly arrive. The quorum signal IS phase
  9's ReadIndex ack stream, so the check cannot diverge from commit
  ability.
- At each heartbeat tick, BEFORE sending: count self + peers heard within
  `election_timeout_max`; below `majority()` (the same function used by
  vote counting and commit advancement) → `become_follower(current_term,
  None)` and return without sending. No term bump (bumping would loop; the
  equal-term step-down also skips the hard-state fsync). Piggybacked on
  the existing tick: no new timer, no new RNG draws — seed churn comes
  from behavior changes only. Step-down semantics were settled in phase 9:
  pending reads resolve as retryable errors, pending proposals survive.
- A single-node cluster counts itself as its own majority and never steps
  down (the binary's default mode).

Tested (96 total; 3 new, 4 reworked/inverted):
- The headline, test-first as planned: both asymmetric-partition stalls
  were written as documented-behavior tests against phase-11 code and
  demonstrated (3+ virtual seconds: nothing commits, no election starts,
  no term ever moves — stalled forever), then implemented against and
  inverted. Variant (a): both followers' reply legs to the leader severed
  (directional `set_link_blocked` — heartbeats keep flowing out, every ack
  dies) → the deaf leader now steps down within ~election_timeout_max, its
  silence lets a follower campaign and win, a new write commits; and since
  the stalled entry WAS replicated (only its acks were lost), it commits
  everywhere after heal via the surviving pending proposal — no data loss.
  Variant (b), the phase-11 regression: one follower deaf to the leader,
  the other's acks lost — the deaf node parks in PreCandidate (stickiness
  + leader-denial) and phase-11 code stalls forever; now the leader steps
  down, the ack-severed follower (holding the longer log) elects, writes
  resume, and the old leader parks non-disruptively at its OLD term until
  heal.
- Single-node guard: 5 virtual seconds with zero peer contact — still
  leader, term unmoved, still commits (explicit sim test; http_api's
  single-node suite exercises it implicitly).
- Documented-behavior inversions/reworks: election.rs
  `partitioned_leader_is_deposed_and_rejoins_as_follower` — the isolated
  leader now steps down WITHOUT waiting for heal, at its own term (the
  second inversion of that test's inline comment, after phase 11's);
  read_index.rs money test — the pending linearizable read on the
  partitioned leader now resolves as a retryable error via the leader's
  OWN step-down while the partition is still up (still never a stale
  value); replication.rs `minority_leader_accepts_but_never_commits` — the
  minority leader deposes itself mid-window, post-step-down proposals get
  NotLeader, and the doomed entry still never commits anywhere (safety
  claim unchanged); faults.rs majority-loss — the stalled survivor steps
  down, and after one follower restarts, its longer log wins pre-vote +
  election and the stalled proposal STILL commits: the payoff test for
  "pending proposals survive step-down" (phase 5 design). cluster_http's
  partitioned-leader test: the linearizable GET on the deposed leader now
  answers 503 (retryable) promptly instead of hanging into a 504.
- No spurious step-downs: `heartbeats_prevent_spurious_reelections` (10
  virtual seconds, term never moves) passes unchanged, as does the
  no-fault convergence suite — no tight step-down/re-elect loop.
- Full regression green with ZERO seed re-pins (phase 12 changes behavior
  schedules only, no RNG draw counts): faults + jepsen + both duplication
  soaks + the linearizable checker. The jepsen nemesis's partition rounds
  (150–400ms against a 300ms check window) now depose partitioned leaders
  mid-workload and the WGL checker still finds zero violations across all
  seeds.

Churn / window-tuning datum: under 25% uniform message loss (40ms rpc
timeout, 50ms heartbeats), the observed leader-term sets across seeds 0–4
are byte-identical with and without CheckQuorum — zero loss-induced
step-downs. The window (election_timeout_max ≈ 6 heartbeats per peer) is
comfortably conservative: random loss never trips it; only real
connectivity loss (nemesis partitions, crashes) does. The loss suites
assert safety, not leadership stability, so legitimate step-downs pass.

Untested / known gaps (documented, not fixed):
- Residual liveness gap: a follower that can't hear a HEALTHY leader parks
  in PreCandidate indefinitely while the cluster commits without it —
  CheckQuorum correctly never fires (the leader still hears a majority).
  Variant (b) covers the flip side only.
- Per the liveness literature, even PreVote+CheckQuorum does not close
  every partial-partition schedule; we claim only the schedules the sim
  constructs.
- Real votes stay non-sticky (deliberate — no etcd-style lease/wedge or
  leadership-transfer machinery); re-evaluated in the dynamic-membership
  phase for removed servers.

## Phase 13 — Client dedup tokens (exactly-once writes) ✅

The anomaly this closes (lost update by resurrection): a write whose
outcome was ambiguous (504 — leader lost its majority before confirming)
is retried after a leadership change; both copies commit, and the LATE
duplicate's application clobbers a conflicting write another client had
confirmed in between. A naive retry-same-value schedule cannot show this
in an LWW map — the interleaved conflicting write is what makes the
duplicate application observable.

Done:
- `src/raft/types.rs`: `Session { client, seq }`; `Command::Put/Delete`
  gain `session: Option<Session>` with `#[serde(default,
  skip_serializing_if = "Option::is_none")]` — old log.jsonl lines stay
  readable AND token-less commands serialize byte-identical to phase-12
  output (both pinned by unit tests with verbatim JSON strings; the
  three_process interop test confirms the wire format end-to-end).
- `src/store.rs`: dedup table IN the state machine —
  `sessions: RwLock<HashMap<u64, u64>>` (client → highest applied seq;
  superseded by the windowed amendment below). `apply()` skips the
  mutation of an already-applied tokened command. Dedup is at APPLY,
  never at propose: the first copy may be committed-but-not-yet-applied,
  so a propose-time check against the table would race it; the duplicate
  entry still commits and occupies a log index. apply() stays a pure fold
  of the log (no clocks/randomness) — which is exactly what makes the
  table restart-safe (rebuilt by replay) and snapshottable.
- Phase-14 hook landed now: `KvSnapshot { map, sessions }` +
  `KvStore::export()/import()` (roundtrip-tested) — the snapshot payload
  shape is settled; the pre-existing map-only `snapshot()` is untouched.
- `src/api.rs`: optional `X-Client-Id`/`X-Client-Seq` headers (u64),
  both-or-neither, unparseable values → 400; attached to the Command at
  construction. Absent → at-least-once semantics, byte-identical.
- `tests/common/mod.rs`: `put()` stays token-less; `put_with_token()`
  added. No new dependencies; kv.rs unchanged (no retry helper — nothing
  earned it: sim tests drive propose directly, HTTP tests drive reqwest).

Tested (112 total; 16 new, plus faults/jepsen reworks):
- Headline, test-first (`tests/dedup.rs`): the schedule was written
  against phase-12 code and demonstrated the anomaly (final state k=1 —
  B's confirmed k=2 silently destroyed), then implemented against and
  inverted. Propose k=1 on L, sever both followers' reply legs (phase-12
  trick: entry replicates, never commits on L, outcome Unknown);
  CheckQuorum deposes L; a follower's no-op commits the entry
  transitively; B confirms k=2; A retries same token — the retry COMMITS
  (two same-token entries in the log) but mutates nothing: k=2 on every
  node, sessions = {1→1} everywhere, both preserved across a
  crash/restart by log replay alone. The untokened variant stays in the
  suite as the documented at-least-once behavior.
- Store unit tests: duplicate seq skipped (with interleaved conflict),
  lower seq skipped, higher seq applies, clients independent, duplicate
  delete skipped past a re-put, token-less commands never touch the
  table, export/import roundtrip (including post-import dedup).
- Serde pinning: token-less byte-identity, old-line readability, tokened
  roundtrip — all against verbatim strings.
- HTTP (`http_api.rs`): retried PUT and retried DELETE with the same
  token → success status both times, applied once (interleaved
  conflicting write proves the skip); next seq applies; five malformed
  header combinations → 400 for PUT and DELETE, nothing stored.
- `tests/faults.rs`: `write_until_confirmed` now retries ONE value with
  ONE token until a definite Ok (Unknown no longer burns values); the
  "no key committed twice" invariant relaxed to "duplicate keys in the
  log are legal iff every occurrence shares one token", with the logical
  effect asserted via final state (every confirmed key present exactly
  with its value). All seeds green.
- `tests/jepsen.rs`: write modes parametrized. Linearizable-mode writers
  attach client=process/seq=op#, wait a deliberately tight 150ms, and
  retry ambiguous outcomes (bounded, same command) — one Recorded op per
  logical write (invoked at first attempt, returned at final ack), sound
  only because of dedup. WGL checker: ZERO violations on all seeds, and
  now zero permanently-Unknown ops (asserted). Vacuity guard: at least
  one op across the seed set acked only AFTER an ambiguous attempt
  (seed 5 rolls two) — the exact schedule where a duplicate copy also
  commits. Stale mode kept `FireOnce` writes so its workload is
  byte-identical: the ≥1-stale-violation regression passed with NO
  re-pinning.
- Seed churn as calibrated: zero — the sim passes values in memory and
  this phase adds no RNG draws, so election/replication/read_index/
  cluster_http/http_transport/three_process all passed without any edit;
  churn appeared only where driver logic changed (faults/jepsen
  workloads), as predicted.

Untested / known gaps (documented, not fixed):
- The sessions table never expires entries: unbounded growth with the
  number of distinct clients (a per-node TTL would diverge replicas —
  rejected by design).
- No result caching: Put/Delete return unit, so a deduped retry can't
  report the original's result — fine today, a real gap if commands ever
  return values.
- Dedup is exactly-once APPLICATION, not exactly-once log occupancy:
  duplicates still consume log indexes (and, in phase 14, snapshot
  work).

### Phase 13 amendment — windowed dedup (post-checkpoint)

The original table (client → highest applied seq, skip on `seq <= max`)
made "one outstanding op per client" a silent-failure trap: a client
pipelining two independent ops (the natural reading of the headers as
per-op idempotency keys) could get the lower seq skipped as a
"duplicate" yet still acked when the higher seq won the race to the log
— a 201 for a write that never happened and never will, i.e. a
linearizability violation reachable through the public API. Amended
before phase 14 freezes `KvSnapshot`, so the sessions representation
never needs an on-disk migration.

Done: sessions became `client → SessionState { max_seq, recent: u64 }`
— exact-match dedup over a sliding 64-seq window (`SESSION_WINDOW`).
An op arriving after a higher seq still applies (concurrent same-client
ops may linearize in either order); only a seq that exactly applied
before — or fell below the window, sound under the ≤64-outstanding
contract — is skipped. Still a pure fold of the log; wire and log
formats untouched (only the in-memory/snapshot representation changed).

Tested (114 total; red→green): tests/dedup.rs
`pipelined_ops_from_one_client_both_apply_regardless_of_order` was
written first and demonstrated the false ack (op acked Ok, key never
appears) against the original table, then inverted: both pipelined
writes apply on every node, genuine retries of either seq still skip.
Store unit tests: `lower_seq_skips_the_mutation` INVERTED to
`out_of_order_pipelined_op_applies` (including the seq-gap case);
window-slide test (below-window retry still deduped, oldest in-window
seq still applies). Full regression green with faults/jepsen unchanged
— their retries reuse exact seqs monotonically, so the semantics change
is invisible to them.

Remaining contract (documented, enforced by construction not by
rejection): seqs strictly increasing per client, at most 64 outstanding;
a client exceeding the window can have a below-window op wrongly
skipped-and-acked — the same failure class, now requiring >64 pipelined
ops instead of 2.

## Phase 14 — Snapshotting / log compaction + InstallSnapshot ✅

The highest-risk phase of the roadmap: storage's implicit
first-index-is-1 assumption died. The index-0 sentinel generalized to a
snapshot boundary; 0/0 (no snapshot.json) reproduces the old behavior
exactly, which is what kept every pre-phase-14 test and data dir intact.

Done:
- `src/raft/types.rs`: `Snapshot { last_included_index,
  last_included_term, membership: Option<Value>, state: Value }` — ONE
  shape for `snapshot.json` on disk and the InstallSnapshot RPC payload
  (a follower persists exactly what the leader sent). `state` is opaque
  to Raft; `membership` is reserved for phase 15 and always `None`.
- `src/raft/storage.rs` (the bulk; both `TODO(compaction)` markers
  resolved): `snapshot: Option<Snapshot>` held in memory whole (small by
  scope — it doubles as the leader's payload cache, invalidated by each
  compaction). All index arithmetic centralized in one private
  `pos(index)`; every former `log[i-1]` site audited: `entry`/
  `entries_from`/`term` (== boundary → `Some(snapshot_term)`; < boundary
  → `None` = "compacted, only a snapshot can answer"), `last_index` =
  boundary + retained len, `last_term` falls back to the boundary term
  (load-bearing for elections after a full compaction), `truncate_from`
  ERRORS at or below the boundary (compacted = committed = never
  rewound). `compact_to(last_applied, state)` captures the boundary
  entry's term BEFORE dropping it, then: write snapshot atomically →
  rewrite log without the prefix → reopen the append handle.
  `install_snapshot(&Snapshot)` persists a received snapshot, retaining
  the log suffix iff our entry AT the boundary matches its term, else
  clearing the log. Crash window between the two writes: replay skips
  entries at or below the boundary (still validating line contiguity),
  then requires the retained log to continue at boundary+1 — reopening
  completes a half-done compaction idempotently.
- Trigger (deterministic by construction — applied-entry count, no
  size/timer): after each apply batch (and after `resolve_pending`, so
  nothing pending ever sits below the new boundary),
  `last_applied - snapshot_index >= threshold` →
  `compact_to(last_applied, state_machine.snapshot())`. Always at
  `last_applied`, never `commit_index` (committed-but-unapplied entries
  aren't in the state yet). `RaftConfig.snapshot_threshold: Option<u64>`
  — `None` = off = the default everywhere (the seed-churn firewall);
  env `RUSTKV_SNAPSHOT_THRESHOLD` (>= 1 enforced) wired through
  config.rs/main.rs.
- `StateMachine` trait grew `snapshot() -> Value` / `restore(&Value)`;
  KvStore implements them via phase 13's `export()/import()` +
  serde_json. The landmine — KvStore's inherent map-only `snapshot()`
  used by nearly every cluster test — is real but benign: Rust resolves
  concrete calls to the inherent method, `dyn StateMachine` gets the
  trait; pinned by a store.rs test calling both. Restore-at-boot lives
  in `RaftNode::spawn` (the single chokepoint — main.rs and
  `TestCluster::restart` work unchanged): restore the state machine,
  init `commit_index`/`last_applied` to the boundary; the retained log
  then replays through normal commit advancement.
- InstallSnapshot (§7, single-shot — no chunking): leader sends it from
  `send_append` when `next_index[peer] <= snapshot_index`, checked
  BEFORE the prev_log_term lookup (which cannot answer below the
  boundary). Follower: usual term checks + step-down; idempotence guard
  `last_included_index <= commit_index` → success no-op (phase 10's
  duplication fault is the standing proof); else persist (fsync before
  replying) → restore → bump commit/last_applied. Reply carries only the
  follower's term (Figure 13); a higher term deposes the leader. On
  reply the leader folds `match_index`/`next_index` to the boundary
  (max, so stale replies never rewind) and resumes AE for the tail.
  Local proposals at or below an installed boundary become unverifiable
  (their terms are gone): their senders are DROPPED (waiters get the
  retryable/unknown error) rather than resolved `false` — a false
  "definitely didn't commit" for an entry that IS in the snapshot would
  be a lie the lin checker could catch.
- Two AppendEntries generalizations on the follower (both vacuous at
  boundary 0): a `prev_log_index` BELOW our boundary passes the
  consistency check (compacted ⇒ committed ⇒ matches, by Leader
  Completeness), and entries at or below the boundary are skipped in the
  walk — without these, a follower that compacted ahead of the leader's
  bookkeeping (its commit acks lost) would reject backtracking probes
  forever while a never-compacting leader has no snapshot to send.
- CheckQuorum interplay (decided + documented in node.rs): an
  InstallSnapshot reply counts as `last_contact` (it IS contact at our
  term) but never as `acked_seq` — ReadIndex confirmation stays
  AE-seq-tagged only, so a snapshot-fed peer can't confirm a read.
- Harness: `TestCluster` threads `snapshot_threshold` through spawn AND
  `restart()` (a reborn node reverting to `None` would silently diverge
  from the scenario); `disk_snapshot(id)` next to `disk_log(id)`;
  `spawn_cluster_with_threshold`. RPC serde: existing variants' wire
  encoding pinned byte-identical by verbatim-string tests (external
  tagging keys by name, so the new variant changes nothing);
  three_process.rs passed unchanged.

Tested (137 total; 23 new):
- Storage unit (11 new): compact→reopen boundary arithmetic (term/entry/
  entries_from/last_index/last_term at, below, above the boundary);
  compacting the whole log (empty retained log, boundary answers,
  appends continue at boundary+1); truncate at/below boundary errors;
  compact outside the retained range errors; the crash-window
  idempotence test hand-builds the exact between-the-two-writes dir
  state (plus the whole-log-covered variant); a forged boundary/log gap
  fails loudly; install_snapshot retains a matching suffix / clears a
  divergent log / refuses boundary regression / survives reopen; old
  data dirs WITHOUT snapshot.json open byte-identically (log bytes
  compared) and no snapshot file is conjured; torn-final-line repair
  still works past a boundary.
- tests/snapshot.rs (6 new, sim, paused time): threshold trigger
  compacts every node (disk: snapshot + retained log continuing at each
  node's own boundary; state machines identical); single node restarts
  from its own snapshot and keeps working; the money test ×3 seeds —
  crash a follower, commit+compact PAST its whole log on the survivors
  (asserted, so the scenario can't lose its teeth), restart it → it
  converges and its final log contains NO entry at or below its
  crash-time last index (AE backfill was impossible; the snapshot path
  provably ran); the same scenario under 100% request duplication
  (duplicated InstallSnapshots hit the no-op guard; sim safety observer
  clean); the phase-13 cross-check BOTH ways — a tokened write whose
  application lives in the compacted prefix still dedups its retry on
  (a) a node restarted from its own snapshot.json and (b) a node that
  caught up via InstallSnapshot, with the interleaved conflicting value
  winning the final state and the original entry provably absent from
  the node's log (sessions rode the snapshot, nothing else could know).
- Jepsen low-threshold variant (threshold 16, same 6 seeds, same
  nemesis): the WGL checker still finds ZERO violations; final-STATE
  equality (map + sessions, asserted inside run_workload after
  convergence — with snapshots on it replaces raw-log equality, since
  per-node compaction points legally differ; with them off it's a free
  extra claim). Vacuity guard: no node's retained log still reaches
  index 1. Crash rounds still nonzero across the seed set.
- Faults: `randomized_fault_schedule` parametrized; a new 4-seed run
  with threshold 8 under the full mix (10% loss, partitions,
  crash/restart) using snapshot-aware final assertions — identical
  exports, every confirmed write in the final state, each node's
  retained log continuing from its own boundary, confirmed writes at
  their exact (term, index) wherever still retained, ≥1 node compacted.
  The token-less-threshold tests are untouched.
- Sim unit: InstallSnapshot traffic is invisible to the phase-10 safety
  observer (falls through the AppendEntries-only match by construction),
  with a real conflicting AE still recording.
- Serde pins: AppendEntries/RequestVote request+reply JSON pinned
  verbatim against phase-13 output; InstallSnapshot roundtrips.
- Seed-churn firewall held exactly as designed: with the feature off
  this phase adds no RNG draws and no messages, and every pre-existing
  suite (election, replication, read_index, faults, jepsen, dedup,
  cluster_http, http_*, three_process) passed with ZERO behavioral
  edits and ZERO re-pins. (Mechanical edits only: two driver functions
  gained a `snapshot_threshold` param passed as `None`.)
- Manual binary smoke: single node with RUSTKV_SNAPSHOT_THRESHOLD=4 —
  10 writes → snapshot.json at boundary 8 + 3-line retained log on
  disk; kill -9 → restart → restores from snapshot, serves all keys,
  accepts new writes.

Untested / known gaps (documented, not fixed):
- No §7 chunking: the snapshot rides one RPC, held in memory whole on
  both ends — payload size is unbounded in memory and on the wire
  (fine at this project's scale; the real fix is offset-chunked
  InstallSnapshot).
- No snapshot-rate limiting: a lagging peer is re-sent the snapshot at
  heartbeat pace until its first reply folds next_index forward
  (duplicates are follower no-ops, so this wastes bandwidth, not
  correctness).
- Dedup duplicates still consume log indexes and now also snapshot
  work (phase 13's gap, unchanged).
- A leader compacts independently of peer progress: compacting past a
  live-but-slow peer's match_index forces a snapshot where entries
  would have done (no match_index floor on compact_to).
- The sessions table inside snapshots inherits phase 13's unbounded
  growth.

## Project complete (phases 0-14)
Remaining ideas beyond the original scope, in planned order: dynamic
membership (incl. re-evaluating vote stickiness for removed servers;
the snapshot file's `membership` field and InstallSnapshot joiner
catch-up are already in place), connection pooling, scripted Docker
partition test. (TLS on the raft port: dropped — blocked on the
dependency whitelist.)

## Out of scope (deliberate)
Dynamic membership changes — clean TODOs remain (`TODO(membership)` on
the snapshot's reserved field). Snapshotting graduated from this list in
phase 14.
