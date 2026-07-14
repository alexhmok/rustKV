# rustkv — failure modes

The consolidated catalog of every known way this system can fail, degrade,
or surprise an operator or client. Sources: the per-phase "untested / known
gaps" records in PLAN.md and the post-completion testing review of
2026-07-14 (which also produced the fixes marked below). Each entry states
the trigger, what a client or operator observes, and its status:

- **by design** — correct behavior of a CP system or an explicit scope
  decision; will not change.
- **mitigated** — a real limitation with a shipped guard, knob, or
  documented operational rule.
- **open** — a known gap with the fix path named but not built.

Safety claims are backed by the deterministic sim/jepsen suites (185 tests,
plus opt-in wide soaks: 512 randomized fault schedules across 256 seeds
with 10% message duplication and aggressive compaction — zero safety or
linearizability violations) and the scripted real-network Docker partition
test. Everything below is about *availability, liveness, and operational
edges* — no known scenario loses a confirmed write or serves a
non-linearizable default read.

## Consistency and client-visible semantics

### Ambiguous write outcomes (HTTP 504) — by design
A write that cannot confirm majority replication within the node's write
timeout answers `504`: the write **may still commit later** (e.g. after a
partition heals). This is the CP trade — the alternative is lying. Clients
that need exactly-once semantics must retry the *identical* request with
`X-Client-Id`/`X-Client-Seq` dedup tokens; the state machine applies each
(client, seq) at most once, so the retry is safe even if the original also
commits. Untokened retries are at-least-once by contract. Proven end to
end: the jepsen linearizable suites drive tokened retries through
partitions, crashes, duplication and snapshots with zero WGL violations.

### Stale reads are opt-in only — by design
`GET /{key}` is linearizable (ReadIndex) and never stale. `?stale=true`
reads the local state machine and can be arbitrarily stale on a follower or
a deposed leader — that is its documented contract (the jepsen stale-mode
suite demonstrates real, replayable staleness violations to prove the
checker catches them). Use it for latency/availability, never for
read-your-writes.

### Dedup window contract: ≤ 64 outstanding ops per client — by design (documented trap)
Dedup matches exact seqs over a sliding 64-seq window. A client that
pipelines **more** than 64 concurrent ops can have a below-window op
silently skipped as an assumed duplicate — acked but never applied (a real
linearizability violation, reachable only by violating the documented
contract). Pinned as behavior by
`store::tests::beyond_window_pipelining_wrongly_skips_a_never_applied_op`.
Widening the window means changing `SESSION_WINDOW`'s bitmask
representation.

### Sessions table grows forever — open
One entry per distinct client id, never expired (a per-node TTL would
diverge replicas; expiry must ride the log to stay deterministic, and no
such command exists). Long-lived deployments with high client-id churn grow
state and snapshot size without bound. Practical guidance: reuse client
ids; treat a client id as a long-lived session, not a per-request value.

### No result caching on deduped retries — open (latent)
Put/Delete return unit, so a skipped duplicate can't return "the original's
result" because there is nothing to return. Becomes a real gap only if
commands ever return values (e.g. compare-and-swap).

## Catch-up, snapshots, and large payloads

### FIXED in the testing review: >2 MiB catch-up was permanently undeliverable
Until 2026-07-14, axum's default 2 MiB body limit sat on the raft port. Any
AppendEntries catch-up batch or InstallSnapshot payload beyond it was
rejected by the receiving axum layer with a mid-upload connection reset;
the sender's transport surfaced a generic timeout and the leader retried
the *identical* oversized RPC at heartbeat pace, forever. Observable as: a
restarted or re-added follower stuck at `commit_index=0`, pre-campaigning
endlessly, while the leader logs (at trace level only) broken-pipe errors —
**with snapshotting off (the default) this bit any follower more than
~2 MiB of log behind**. Demonstrated on a real 3-process cluster (stuck
>45s at 3.8 MiB behind; identical run at 1.5 MiB recovered in 2s), fixed by
disabling the body limit on the raft route (`DefaultBodyLimit::disable()`,
cluster-internal port, payloads already held in memory whole by design),
and pinned by `rpcs_larger_than_two_mebibytes_roundtrip`. After the fix the
same 3.8 MiB scenarios recover in 2-3s, and a 64 MiB snapshot transfers in
~5s on loopback at the default timeout.

### Single-shot snapshot / batch vs the RPC timeout — mitigated (knob), fix path named
InstallSnapshot rides ONE HTTP RPC (no §7 chunking), and AppendEntries has
no batch-size cap, so a whole catch-up must fit through one RPC within
`RUSTKV_RPC_TIMEOUT_MS` (default 150). If it can't — slow network or disk,
huge state — the transfer times out and retries indefinitely: same symptom
as the fixed bug above, but bandwidth-bound instead of hard-capped.
Loopback measurements put the default budget at ≥ 64 MiB, so this binds
only on real networks (e.g. 150ms ≈ 1.8 MiB on a gigabit link). Mitigation:
raise `RUSTKV_RPC_TIMEOUT_MS` (raising it also delays per-RPC failure
detection, though election timeouts are independent). The real fix —
size-aware timeout, then streaming/chunked InstallSnapshot and an AE batch
cap — is documented in PLAN.md as out of scope. Also note both ends hold
the payload in memory whole, and the leader's staged trailing-window
capture holds another full copy.

### Snapshot re-send waste — by design (bandwidth, not correctness)
A lagging peer is re-sent the full snapshot at heartbeat pace until its
first reply folds `next_index` forward; duplicates are follower no-ops
(idempotence guard). With pooling, a stale-connection retry can transmit
the payload twice inside one RPC window. Wasteful, never incorrect.

### Leader compacts independently of slow peers — mitigated (off by default)
With `snapshot_trailing` at its default 0, a leader may compact past a
live-but-slow peer's log position, forcing a snapshot transfer where log
entries would have sufficed. Set `RUSTKV_SNAPSHOT_TRAILING` (etcd's
SnapshotCatchUpEntries equivalent) to keep a window of entries behind the
boundary; the payoff test proves an isolated follower then heals via plain
AppendEntries.

### Compaction destroys per-entry evidence — by design, upgrade path named
A node that installs a snapshot drops proposals whose entries fell below
the boundary as *ambiguous* (channel closed), never as a false "definitely
didn't commit" — the terms needed to verify them are gone. The upgrade path
(an RLE `term_runs` table in the snapshot restoring definite answers) is
documented in PLAN.md phase 14 and deliberately not built.

## Elections, partitions, and liveness

### Minority partitions reject writes — by design (the CP contract)
A leader cut off from the majority: writes time out (504), linearizable
reads fail closed (503 after CheckQuorum deposes it, 504 before), stale
reads still serve. The doomed writes never commit anywhere (proven in sim,
jepsen, and the real-network Docker test). Which of 503/504 a client sees
during the window is a race — treat both as retryable-elsewhere.

### PreVote parking — by design, one residual gap open
A follower that cannot reach a majority parks in PreCandidate without
churning terms (phase 11), and CheckQuorum deposes a deaf leader (phase
12), closing the classic asymmetric-partition stalls both ways. **Residual,
open:** a follower that can't hear a *healthy* leader (one-way link loss
toward that follower only) parks forever while the cluster commits without
it — CheckQuorum correctly never fires. Per the liveness literature, even
PreVote+CheckQuorum doesn't close every partial-partition schedule; only
the schedules the sim constructs are claimed. Operator signal: a node
endlessly logging pre-campaigns at a frozen term while the cluster is
otherwise healthy has a one-way connectivity problem (or, pre-fix, the
catch-up size bug above).

### Rejoin churn without PreVote — closed since phase 11
Historical: a healed node's inflated term used to force a re-election
round. PreVote stickiness eliminated it (the phase-3 test that asserted the
churn as expected is inverted).

### CheckQuorum window — by design
A leader steps down after missing majority contact for
`election_timeout_max`. Under 25% uniform message loss the window provably
never trips (measured across seeds); only real connectivity loss does.
Shrinking the window would trade loss-tolerance for detection speed.

## Membership changes

### Dynamic 1→2 growth is impossible — by design (guard)
Adding a second member to a single-node cluster is refused (409): the new
majority (2 of 2) includes a member that has never been heard, and an
unguarded accept would brick the cluster permanently (nothing can ever
commit again; CheckQuorum then deposes the only node that could fix it).
The guard makes the unrecoverable case unreachable. Bootstrap the target
topology statically instead; the real fix (learner members) is out of
scope.

### No learners: every add reduces fault tolerance until catch-up — open
A new member counts toward quorum the moment its ConfigChange is appended.
For 3→4, all three originals are load-bearing until the joiner catches up —
and catch-up speed is governed by the payload/timeout entry above. Keep
adds to windows of full health (the availability guard enforces
reachability of the new majority at propose time, but not catch-up speed).

### Reconfig guard blind spot in a fresh term — closed in phase 19 (was: open)
The guard's reachability signal (`last_contact`) initializes to leadership
start, so a change proposed within the first `election_timeout_max` of a
new term used to pass while a member was actually down. Closed: a member
now counts as reachable only if it has also ACKED an AppendEntries at the
leader's term (the `acked_seq` key set — populated at exactly one site,
empty at each leadership start — is the acked-this-term flag; self always
counts). Latency cost is ~one heartbeat RTT and in practice zero: the
no-op gate already forces waiting for a majority of acks. Demonstrated
both ways in `membership::add_in_a_fresh_term_is_refused_until_members_ack`
(pre-fix, the identical schedule accepts an add inside the blind window
while a member is down — recorded in the PLAN.md phase-19 entry). Guard
rejections remain side-effect-free; retry after the members ack.

### Removed servers are now told (parting sends) — mitigated in phase 19 (was: by design)
Leaders used to stop replicating to a removed peer the moment the removal
took effect, so the peer never learned and probed with pre-votes forever.
Now the leader keeps sending AppendEntries (or InstallSnapshot, if the
removal entry was compacted) to the removed peer until
`match_index[peer]` covers the removal entry, then goes quiet; the peer
adopts the configuration that excludes it on APPEND (§4.1) and the
campaign gate parks it silently — Follower, frozen term, frozen log. The
membership watch carries departing peers' addresses until the parting ack
so the real binary's transport can still reach them
(`three_process::removed_process_learns_of_its_removal_and_goes_quiet`;
sim leg in `membership::removed_follower_cannot_disrupt_the_members`).
Residual, best-effort by design: the departing bookkeeping dies with the
leadership — a peer removed under a leader that crashed or stepped down
before the parting ack still parks probing forever (the old behavior; its
pre-votes remain structurally deniable, liveness noise on its own box
only). A parked peer may also hold the removal entry uncommitted-locally
(the parting protocol guarantees delivery of the ENTRY, not the commit
index) — harmless, since adoption is append-time. Operators should still
stop the process and clean the data dir eventually. Address changes for
an existing member are still refused (409); remove-then-re-add is the
workaround.

### Admin ops have no dedup tokens — by design (verify on 409)
A retried add/remove after an ambiguous 504 may answer 409 ("already a
member" / zero delta). Treat 409 as "possibly already done — GET
/cluster/members and verify".

### Mixed-version constraint — open (operational rule)
A pre-phase-15 binary cannot deserialize a `ConfigChange` log entry
(AppendEntries rejected on the wire; `Corrupt` on replay). Snapshots are
forward-safe. Rule: never propose a membership change while a rolling
upgrade is in progress.

### Ongaro concurrent-change schedule — closed in phase 18 (was: test debt)
The disjoint-majority disease the phase-15 gates prevent is now constructed
in the sim, via the harness-only `RaftConfig.test_disable_reconfig_gates`
flag (never reachable from env config): with the gates off, a
minority-partitioned 5-node leader STACKS two removals (members 5 → 4 → 3,
each effective on append) until its partition of two is a quorum, and two
disjoint majorities — {leader, follower} and the far three, still on the
5-member config — commit different entries at the same log index:
split-brain, demonstrated down to both sides serving granted linearizable
reads of state the other never heard of. With the gates on, the identical
driver is refused at exactly the stacking step (`InvalidConfigChange`,
one-in-flight). Two findings from the construction, recorded in the PLAN.md
phase-18 record: (a) the classic TWO-LEADER form of the disease is not
constructible in this implementation — for a single in-flight single-server
change, every new-config commit majority intersects every old-config vote
majority, and the intersecting node either denies the rival's vote (§5.4.1,
its log is longer) or rejects the old leader's AppendEntries (term check);
the gates' unique work is preventing STACKING. (b) The event-level safety
observer is silent through the unsafe run — its checks are per-term and the
split-brain leaders hold different terms (see the harness-limitations note
below).

## Transport and networking

### Half-open pooled connections burn one RPC — by design (accepted cost)
A pooled idle connection severed without an RST (partition, host power
loss, NAT/conntrack eviction) produces no error on use: the write buffers,
the read hangs, and the outer `RUSTKV_RPC_TIMEOUT_MS` kills the call — the
fresh-connection retry never fires because nothing *failed*. Cost: one lost
heartbeat or delayed vote per stale socket; self-healing (the stream is
dropped). This failure mode did not exist pre-pooling and is the first
place to look if a deployment shows periodic single-heartbeat losses.
Observed live in the Docker partition test (docker disconnect sends no
RST). Ex-leaders are disproportionately exposed: pools are only warm on
leaders, so election-time RPCs hit the stale sockets.

### Raft port has no TLS or auth — open (scope-blocked)
Anyone who can reach the raft listener can send RPCs (and, post-fix,
arbitrarily large bodies — the body limit is deliberately disabled there).
TLS is blocked on the dependency whitelist; the operational stance is
network isolation (the Docker compose splits client and raft networks;
production deployments must firewall the raft port).

### Non-rustkv servers behind the raft port hang to timeout — by design
The pooled client no longer sends `Connection: close`, so a nonconforming
unframed 200 (no Content-Length, no server close) hangs to timeout where
the phase-7 client would have succeeded. Only matters if the raft port ever
fronts something that isn't rustkv.

### Docker heal requires the network alias — by design (documented)
`docker network connect` without `--alias nodeN-raft` leaves the healed
node unresolvable by peers (README documents the exact command; the
partition-test script encodes it).

## HTTP API edges

### Client API request bodies cap at 2 MiB — by design (pinned)
The *client* port keeps axum's default limit. An oversized PUT is rejected
and stores nothing — but the client may see a **connection reset mid-upload
instead of a readable 413** (axum responds and closes while the body is
still arriving; the standard early-response race). Pinned by
`large_values_roundtrip_and_oversized_bodies_are_rejected`.

### Keys are single decoded path segments — by design (pinned)
Percent-encodings decode before storage (`/caf%C3%A9` stores key `café`);
`%2F` decodes to a key containing a literal slash, addressable only in
encoded form. Redirect `Location` headers embed the *decoded* key as-is, so
keys needing re-encoding can misroute a redirect — exotic-key writes should
go to the leader directly. Root path is not a key. Pinned by
`exotic_key_encodings_are_decoded_and_roundtrip`.

### `null` is a storable value — by design (pinned)
`GET` of a stored `null` answers 200 with body `null`, distinguishable from
404. LWW-register semantics over arbitrary JSON values.

## Test-harness limitations (not product failures)

- **cluster_http timing flake — mitigated 2026-07-14**: real-time tests
  sharing one process could see leadership move between sampling and
  asserting (~1/8 of full-binary debug runs, three affected sites). The
  flaky sites now re-sample leadership in bounded retry loops
  (correctness claims unchanged; a wrong 307 target or lost write still
  fails). Post-fix: 0 failures in 32 consecutive debug full-binary runs —
  the class is CPU-starvation-driven, so "rarer than 1/32" is the honest
  claim, not "gone". A failure that reproduces on a re-run is real; one
  that doesn't is this.
- **Determinism is sim-only**: seeded reproducibility holds on the
  current-thread paused-time runtime; real-socket suites are poll-based.
- **No real power-loss testing**: crash durability is argued from
  fsync-before-reply plus hand-corrupted-file recovery tests; fsync
  guarantees are whatever `File::sync_all` provides per platform.
- **The sim duplicates requests, never replies**; the core tolerates
  duplicate replies by design (stale-reply folding) but that path has no
  fault-injection coverage.
- **The event-level safety observer** checks AppendEntries content
  invariants only — liveness bugs surface as test timeouts, not observer
  hits; PreVote/InstallSnapshot traffic is deliberately invisible to it.
  Its election-safety and log-matching checks are both PER-TERM, so
  cross-term disjoint-majority commits (the phase-18 Ongaro schedule) are
  invisible to it too — divergence/WGL assertions are the detector for
  that class.
- **WGL checker caps at 63 ops per key** (u64 bitmask), sized to the
  workloads.
- **Docker/partition coverage is one scenario** (leader partitioned, heal,
  converge) and needs a local daemon; it is deliberately outside
  `cargo test`.
