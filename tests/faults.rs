//! Phase 6: deterministic fault tests on the simulated transport with
//! virtual time — every scenario is a pure function of its seed.
//!
//! Safety invariants asserted:
//! - at most one leader per term — event-level since phase 10: the sim
//!   transport inspects every AppendEntries crossing the network and
//!   records conflicting leadership claims; `TestCluster::shutdown`
//!   (called by `assert_final_consistency`) asserts none were seen, so no
//!   sub-sample flicker can escape;
//! - no confirmed write lost: once a proposal's commit is positively
//!   observed, its (term, index, command) must be in every node's log at
//!   the end;
//! - convergence: after healing/restarting everything, all logs are
//!   byte-identical and all state machines hold identical contents;
//! - atomicity of unconfirmed writes: a write whose outcome was unknown is
//!   either applied on every node or on none.
//!
//! Scenarios: leader crash/restart mid-write, repeated partition/heal
//! cycles (including partitioning the leader), a seeded randomized schedule
//! mixing writes, partitions, heals, crashes and restarts under 10% message
//! loss (run both with and without message duplication), write stall
//! without a majority — where CheckQuorum (phase 12) now deposes the
//! stalled leader, whose surviving pending proposal still commits after a
//! restart restores the majority — and a same-seed determinism check over
//! the randomized schedule.
//!
//! Client-level retry duplication (phase 13): `write_until_confirmed`
//! retries ONE value with ONE dedup token until some attempt definitely
//! commits, so ambiguous outcomes no longer burn values. The log may then
//! legally hold the same key several times — but only under one shared
//! token, and only one application happens (asserted via final state).
//! The single-shot writes of the randomized schedule stay token-less.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::*;
use rustkv::raft::node::{RoleKind, Status};
use rustkv::raft::types::{Command, LogEntry, LogIndex, NodeId, Session, Term};
use rustkv::rng::SplitMix64;
use serde_json::json;

/// The one logical client behind every `write_until_confirmed` retry loop;
/// its seqs are the (strictly increasing) values themselves.
const RETRY_CLIENT: u64 = 0;

/// A write whose commit was positively observed.
#[derive(Debug)]
struct Confirmed {
    term: Term,
    index: LogIndex,
    value: u64,
    session: Option<Session>,
}

impl Confirmed {
    /// The exact command the log must hold at (term, index).
    fn command(&self) -> Command {
        Command::Put {
            key: format!("v{}", self.value),
            value: json!(self.value),
            session: self.session,
        }
    }
}

/// The single visible leader among `ids`, if there is exactly one.
fn unique_leader(cluster: &TestCluster, ids: &[NodeId]) -> Option<Status> {
    let leaders: Vec<Status> = cluster
        .statuses_among(ids)
        .into_iter()
        .filter(|s| s.role == RoleKind::Leader)
        .collect();
    match leaders[..] {
        [leader] => Some(leader),
        _ => None,
    }
}

/// One attempt: find a unique leader among `ids`, propose `command`, and
/// wait up to 2 virtual seconds for the commit. `None` = outcome unknown or
/// no leader — the entry may still commit later.
async fn try_confirmed_command(
    cluster: &TestCluster,
    ids: &[NodeId],
    command: Command,
) -> Option<(Term, LogIndex)> {
    let leader = unique_leader(cluster, ids)?;
    let proposal = cluster.handle(leader.id).propose(command).await.ok()?;
    match tokio::time::timeout(ms(2000), proposal.committed).await {
        Ok(Ok(true)) => Some((proposal.term, proposal.index)),
        _ => None,
    }
}

/// A single token-less attempt at `v{value}={value}`; on `None` the caller
/// must NOT reuse the value (it may still commit later).
async fn try_confirmed_write(
    cluster: &TestCluster,
    ids: &[NodeId],
    value: u64,
) -> Option<(Term, LogIndex)> {
    try_confirmed_command(cluster, ids, put(&format!("v{value}"), value)).await
}

/// Retries ONE value with ONE dedup token until some attempt definitely
/// commits (phase 13: an Unknown outcome no longer burns the value — the
/// retry carries the same Session, so even if an earlier ambiguous attempt
/// lands too, the mutation applies once). Panics after 100 attempts (a
/// liveness failure worth failing loudly on).
async fn write_until_confirmed(
    cluster: &TestCluster,
    ids: &[NodeId],
    next_value: &mut u64,
    confirmed: &mut Vec<Confirmed>,
) {
    *next_value += 1;
    let value = *next_value;
    let session = Session {
        client: RETRY_CLIENT,
        seq: value,
    };
    let command = put_with_token(&format!("v{value}"), value, session.client, session.seq);
    for _ in 0..100 {
        if let Some((term, index)) = try_confirmed_command(cluster, ids, command.clone()).await {
            confirmed.push(Confirmed {
                term,
                index,
                value,
                session: Some(session),
            });
            return;
        }
        tokio::time::sleep(ms(100)).await;
    }
    panic!("no write confirmed within 100 attempts");
}

/// Waits until all nodes report the same last index, fully committed.
/// Same length + all committed ⇒ identical logs (log matching).
async fn converge(cluster: &TestCluster) {
    wait_until(
        "cluster converges (all logs equal and fully committed)",
        || {
            let statuses = cluster.statuses_among(&cluster.all_ids());
            let max_last = statuses.iter().map(|s| s.last_log_index).max().unwrap();
            statuses
                .iter()
                .all(|s| s.last_log_index == max_last && s.commit_index == max_last)
        },
    )
    .await;
}

/// Snapshots must match while nodes are alive; logs are compared from disk
/// after shutdown; every confirmed write must be present at its exact
/// (term, index) and in the final state. A Put key may appear in the log
/// more than once ONLY if every occurrence carries the same dedup token
/// (phase 13: a retried ambiguous write plus its confirmed retry) — the
/// exactly-once claim is on the logical effect, asserted via final state,
/// not on log occupancy.
async fn assert_final_consistency(cluster: &TestCluster, confirmed: &[Confirmed], context: &str) {
    assert!(
        !confirmed.is_empty(),
        "{context}: scenario confirmed no writes — nothing was actually tested"
    );
    let reference_snapshot = cluster.store(1).snapshot();
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).snapshot(),
            reference_snapshot,
            "{context}: node {id} state machine diverges"
        );
    }
    for c in confirmed {
        assert_eq!(
            reference_snapshot.get(&format!("v{}", c.value)),
            Some(&json!(c.value)),
            "{context}: confirmed write v{} missing from the final state",
            c.value
        );
    }

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let reference_log = cluster.disk_log(1);
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.disk_log(id),
            reference_log,
            "{context}: node {id} log diverges"
        );
    }
    for c in confirmed {
        let entry = &reference_log[usize::try_from(c.index - 1).unwrap()];
        assert_eq!(
            (entry.term, &entry.command),
            (c.term, &c.command()),
            "{context}: confirmed write v{} (term {}, index {}) was lost",
            c.value,
            c.term,
            c.index
        );
    }
    let mut keys: HashMap<String, Option<Session>> = HashMap::new();
    for entry in &reference_log {
        if let Command::Put { key, session, .. } = &entry.command
            && let Some(previous) = keys.insert(key.clone(), *session)
        {
            assert!(
                session.is_some() && previous == *session,
                "{context}: key {key} committed twice without a shared dedup token"
            );
        }
    }
}

// ---- leader crash/restart mid-write ----

#[tokio::test(start_paused = true)]
async fn leader_crash_mid_write_preserves_confirmed_writes() {
    for seed in [41, 42, 43, 44, 45] {
        let context = format!("seed {seed}");
        let cluster = spawn_cluster(3, seed, low_loss_faults());
        let mut confirmed = Vec::new();

        let leader = cluster.wait_for_leader().await;
        let p1 = cluster
            .handle(leader.id)
            .propose(put("v1", 1))
            .await
            .unwrap();
        assert_eq!(
            (p1.term, p1.committed.await),
            (leader.term, Ok(true)),
            "{context}"
        );
        confirmed.push(Confirmed {
            term: p1.term,
            index: p1.index,
            value: 1,
            session: None,
        });

        // In-flight write, then crash the leader before its commit is known.
        // The entry is on the crashed leader's disk and may or may not have
        // reached a majority — both outcomes are legal, but must be atomic.
        let p2 = cluster
            .handle(leader.id)
            .propose(put("v2", 2))
            .await
            .unwrap();
        cluster.crash(leader.id);
        drop(p2);

        let survivors: Vec<NodeId> = cluster
            .all_ids()
            .into_iter()
            .filter(|&id| id != leader.id)
            .collect();
        let new = cluster.wait_for_leader_among(&survivors).await;
        assert!(new.term > leader.term, "{context}");
        let p3 = cluster.handle(new.id).propose(put("v3", 3)).await.unwrap();
        assert_eq!(p3.committed.await, Ok(true), "{context}");
        confirmed.push(Confirmed {
            term: p3.term,
            index: p3.index,
            value: 3,
            session: None,
        });

        cluster.restart(leader.id).await;
        converge(&cluster).await;

        // v2's fate must be identical on every node.
        let v2_present: Vec<bool> = cluster
            .all_ids()
            .iter()
            .map(|&id| cluster.store(id).get("v2").is_some())
            .collect();
        assert!(
            v2_present.iter().all(|&p| p == v2_present[0]),
            "{context}: unconfirmed write applied on some nodes but not others"
        );

        assert_final_consistency(&cluster, &confirmed, &context).await;
    }
}

// ---- heal-and-re-partition cycles ----

#[tokio::test(start_paused = true)]
async fn partition_heal_cycles_preserve_confirmed_writes() {
    for seed in [7, 8, 9] {
        let context = format!("seed {seed}");
        let cluster = spawn_cluster(3, seed, low_loss_faults());
        let all = cluster.all_ids();
        let mut confirmed = Vec::new();
        let mut next_value = 0u64;

        cluster.wait_for_leader().await;
        for cycle in 0..5u64 {
            // Rotate the victim so the leader gets partitioned too.
            let victim = (cycle % 3) + 1;
            let majority: Vec<NodeId> = all.iter().copied().filter(|&id| id != victim).collect();
            for &id in &majority {
                cluster.net.set_pair_blocked(victim, id, true);
            }

            // The majority side must keep committing writes.
            for _ in 0..2 {
                write_until_confirmed(&cluster, &majority, &mut next_value, &mut confirmed).await;
            }

            for &id in &majority {
                cluster.net.set_pair_blocked(victim, id, false);
            }
            tokio::time::sleep(ms(500)).await; // let the victim reintegrate
        }

        converge(&cluster).await;
        assert_eq!(confirmed.len(), 10, "{context}: two writes per cycle");
        assert_final_consistency(&cluster, &confirmed, &context).await;
    }
}

// ---- randomized fault schedules ----

/// Runs a seeded random schedule of writes, partitions, heals, crashes and
/// restarts under 10% message loss (plus `duplicate_probability` message
/// duplication), asserting invariants throughout.
/// Returns the action trace and the final log for determinism checks.
///
/// With `snapshot_threshold` set (phase 14) the final check swaps
/// `assert_final_consistency`'s disk-log-equality and
/// confirmed-write-at-(term, index) claims — snapshot-incompatible, since
/// per-node compaction points legally differ and compacted entries are gone
/// from disk — for snapshot-aware ones (identical final states, every
/// confirmed write's effect present, retained logs consistent with each
/// node's own boundary). The token-less-threshold callers pass `None` and
/// are untouched.
async fn randomized_fault_schedule(
    seed: u64,
    duplicate_probability: f64,
    snapshot_threshold: Option<u64>,
) -> (
    Vec<String>,
    Vec<LogEntry>,
    usize,
    rustkv::raft::transport::sim::FaultStats,
) {
    let context = format!("seed {seed}");
    let faults = rustkv::raft::transport::sim::FaultConfig {
        min_delay: ms(1),
        max_delay: ms(15),
        drop_probability: 0.10,
        duplicate_probability,
        rpc_timeout: ms(40),
    };
    let cluster = spawn_cluster_with_threshold(3, seed, faults.clone(), snapshot_threshold);
    let all = cluster.all_ids();
    let mut rng = SplitMix64::new(seed.wrapping_mul(0x5851_F42D_4C95_7F2D).wrapping_add(99));
    let mut trace = Vec::new();
    let mut confirmed: Vec<Confirmed> = Vec::new();
    let mut next_value = 0u64;
    let mut crashed: Option<NodeId> = None; // at most one down at a time
    let mut isolated: Vec<NodeId> = Vec::new(); // kept sorted for determinism

    for step in 0..40 {
        let alive: Vec<NodeId> = all
            .iter()
            .copied()
            .filter(|id| Some(*id) != crashed)
            .collect();

        match rng.next_range(0..=9) {
            // Writes are the most common action.
            0..=4 => {
                next_value += 1;
                match try_confirmed_write(&cluster, &alive, next_value).await {
                    Some((term, index)) => {
                        confirmed.push(Confirmed {
                            term,
                            index,
                            value: next_value,
                            session: None,
                        });
                        trace.push(format!(
                            "step {step}: confirmed v{next_value} at t{term} i{index}"
                        ));
                    }
                    None => trace.push(format!("step {step}: v{next_value} outcome unknown")),
                }
            }
            5 | 6 => {
                let id = rng.next_range(1..=3);
                if let Some(pos) = isolated.iter().position(|&i| i == id) {
                    isolated.remove(pos);
                    for &other in all.iter().filter(|&&o| o != id) {
                        cluster.net.set_pair_blocked(id, other, false);
                    }
                    trace.push(format!("step {step}: heal {id}"));
                } else {
                    isolated.push(id);
                    isolated.sort_unstable();
                    for &other in all.iter().filter(|&&o| o != id) {
                        cluster.net.set_pair_blocked(id, other, true);
                    }
                    trace.push(format!("step {step}: isolate {id}"));
                }
            }
            7 => {
                for &id in &isolated {
                    for &other in all.iter().filter(|&&o| o != id) {
                        cluster.net.set_pair_blocked(id, other, false);
                    }
                }
                isolated.clear();
                trace.push(format!("step {step}: heal all"));
            }
            8 => {
                if crashed.is_none() {
                    let id = rng.next_range(1..=3);
                    cluster.crash(id);
                    crashed = Some(id);
                    trace.push(format!("step {step}: crash {id}"));
                }
            }
            _ => {
                if let Some(id) = crashed.take() {
                    cluster.restart(id).await;
                    trace.push(format!("step {step}: restart {id}"));
                }
            }
        }
        tokio::time::sleep(ms(rng.next_range(50..=300))).await;
    }

    // Recovery: heal and restart everything, then prove the cluster still
    // commits, then converge and check every invariant.
    for &id in &isolated {
        for &other in all.iter().filter(|&&o| o != id) {
            cluster.net.set_pair_blocked(id, other, false);
        }
    }
    if let Some(id) = crashed.take() {
        cluster.restart(id).await;
    }
    trace.push("recovery: healed and restarted everything".to_string());
    for _ in 0..2 {
        write_until_confirmed(&cluster, &all, &mut next_value, &mut confirmed).await;
    }
    converge(&cluster).await;
    trace.push(format!("done: {} confirmed writes", confirmed.len()));
    // Vacuity (T2): the losses/duplications this schedule promised must
    // have actually occurred, or the run proved nothing about them.
    let stats = assert_scheduled_faults_fired(&cluster, &faults, &context);
    let compacted_nodes = if snapshot_threshold.is_none() {
        assert_final_consistency(&cluster, &confirmed, &context).await;
        0
    } else {
        assert_final_consistency_with_snapshots(&cluster, &confirmed, &context).await
    };
    (trace, cluster.disk_log(1), compacted_nodes, stats)
}

/// The snapshot-aware final check (see `randomized_fault_schedule` docs):
/// identical final states with every confirmed write's effect present, and
/// per-node disk consistency (retained log continues from that node's own
/// boundary; a confirmed write is at its exact (term, index) whenever that
/// index is still retained).
async fn assert_final_consistency_with_snapshots(
    cluster: &TestCluster,
    confirmed: &[Confirmed],
    context: &str,
) -> usize {
    assert!(
        !confirmed.is_empty(),
        "{context}: scenario confirmed no writes — nothing was actually tested"
    );
    let reference_state = cluster.store(1).export();
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).export(),
            reference_state,
            "{context}: node {id} state machine diverges"
        );
    }
    for c in confirmed {
        assert_eq!(
            reference_state.map.get(&format!("v{}", c.value)),
            Some(&json!(c.value)),
            "{context}: confirmed write v{} missing from the final state",
            c.value
        );
    }

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let mut compacted_nodes = 0;
    for id in cluster.all_ids() {
        let boundary = cluster
            .disk_snapshot(id)
            .map_or(0, |s| s.last_included_index);
        if boundary > 0 {
            compacted_nodes += 1;
        }
        let log = cluster.disk_log(id);
        if let Some(first) = log.first() {
            assert_eq!(
                first.index,
                boundary + 1,
                "{context}: node {id} retained log must continue from its boundary"
            );
        }
        for c in confirmed {
            if c.index > boundary {
                let entry = &log[usize::try_from(c.index - boundary - 1).unwrap()];
                assert_eq!(
                    (entry.term, &entry.command),
                    (c.term, &c.command()),
                    "{context}: node {id}: confirmed write v{} (term {}, index {}) was lost",
                    c.value,
                    c.term,
                    c.index
                );
            }
        }
    }
    // Vacuity is judged by the caller: the pinned seeds require compaction
    // per run; the wide soak requires it across the seed set (a rare quiet
    // schedule — e.g. seed 151, most of its run without a majority — can
    // legally finish under the threshold).
    compacted_nodes
}

#[tokio::test(start_paused = true)]
async fn randomized_fault_schedules_preserve_safety_across_seeds() {
    let mut total_reorders = 0u64;
    let mut total_blocked = 0u64;
    for seed in 0..8 {
        let (_, _, _, stats) = randomized_fault_schedule(seed, 0.0, None).await;
        total_reorders += stats.reorders;
        total_blocked += stats.legs_blocked;
    }
    // Cross-set vacuity (T2): a single quiet seed is legal, but across the
    // set the schedules must have exercised emergent reordering and real
    // partition suppression, or the "survives partitions and reordering"
    // claim was never actually stressed.
    assert!(total_reorders > 0, "no seed ever reordered a message");
    assert!(total_blocked > 0, "no partition ever suppressed a message");
}

/// The duplication soak: the same randomized schedules with 10% of all
/// requests delivered twice, exercising the duplicate-tolerant
/// AppendEntries walk (and vote idempotence) end-to-end under partitions,
/// crashes and loss at once.
#[tokio::test(start_paused = true)]
async fn randomized_fault_schedules_survive_message_duplication() {
    let mut total_reorders = 0u64;
    for seed in 0..8 {
        let (_, _, _, stats) = randomized_fault_schedule(seed, 0.10, None).await;
        total_reorders += stats.reorders;
    }
    assert!(total_reorders > 0, "no seed ever reordered a message");
}

/// Phase 14: the same randomized fault mix with an aggressively low
/// snapshot threshold — nodes compact mid-schedule, restarts restore from
/// snapshots, and lagging nodes cross compaction boundaries via
/// InstallSnapshot, all under 10% loss.
#[tokio::test(start_paused = true)]
async fn randomized_fault_schedules_survive_with_snapshots_on() {
    for seed in 0..4 {
        let (_, _, compacted, _) = randomized_fault_schedule(seed, 0.0, Some(8)).await;
        assert!(
            compacted > 0,
            "seed {seed}: no node compacted — raise the write mix or lower the threshold"
        );
    }
}

/// Extended soak, excluded from the default run (`cargo test --test faults
/// -- --ignored`): message duplication AND snapshots together — no pinned
/// schedule combines those two faults — across a much wider seed range than
/// the pinned suites. Fresh seeds through the full invariant check are
/// exactly where an untested fault interaction would surface.
#[tokio::test(start_paused = true)]
#[ignore = "extended soak; run explicitly with --ignored"]
async fn extended_soak_duplication_and_snapshots_across_seeds() {
    let seeds: u64 = std::env::var("RUSTKV_SOAK_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    let mut seeds_with_compaction = 0u64;
    for seed in 0..seeds {
        let (_, _, compacted, _) = randomized_fault_schedule(seed, 0.10, Some(8)).await;
        if compacted > 0 {
            seeds_with_compaction += 1;
        }
    }
    // Cross-set vacuity guard: single quiet seeds are legal, but the soak
    // as a whole must overwhelmingly exercise compaction.
    assert!(
        seeds_with_compaction * 2 > seeds,
        "only {seeds_with_compaction}/{seeds} seeds compacted — the soak lost its teeth"
    );
}

#[tokio::test(start_paused = true)]
async fn same_seed_reproduces_the_same_fault_run() {
    let (trace_a, log_a, _, stats_a) = randomized_fault_schedule(5, 0.10, None).await;
    let (trace_b, log_b, _, stats_b) = randomized_fault_schedule(5, 0.10, None).await;
    assert_eq!(trace_a, trace_b, "action/outcome traces must be identical");
    assert_eq!(log_a, log_b, "final logs must be identical");
    assert_eq!(
        stats_a, stats_b,
        "fault-event counts must be identical too — a diverging count means \
         nondeterminism the trace comparison happened not to see"
    );
}

/// T2 determinism audit: the phase-14 configuration (snapshots compacting
/// mid-schedule) had no repeated-run determinism check — compaction adds
/// disk truncation, snapshot capture, and InstallSnapshot to the replayed
/// surface. Same seed must reproduce the identical trace, retained log,
/// compaction count, and fault-event counts.
#[tokio::test(start_paused = true)]
async fn same_seed_reproduces_the_same_fault_run_with_snapshots_on() {
    for seed in 0..2 {
        let (trace_a, log_a, compacted_a, stats_a) =
            randomized_fault_schedule(seed, 0.10, Some(8)).await;
        let (trace_b, log_b, compacted_b, stats_b) =
            randomized_fault_schedule(seed, 0.10, Some(8)).await;
        assert_eq!(trace_a, trace_b, "seed {seed}: traces diverge");
        assert_eq!(log_a, log_b, "seed {seed}: retained logs diverge");
        assert_eq!(
            (compacted_a, stats_a),
            (compacted_b, stats_b),
            "seed {seed}: compaction or fault counts diverge"
        );
    }
}

/// Extended determinism soak, excluded from the default run (wired into
/// `make soak` via the extended_soak name filter): every seed in the range
/// is run TWICE under the strongest fault mix (10% duplication + snapshots)
/// and must reproduce byte-identically. This is the wide-N version of the
/// audit above — HashMap iteration order, real time leaking into paused
/// time, or unseeded randomness anywhere in the stack shows up here as a
/// trace/log/stats divergence on some seed.
#[tokio::test(start_paused = true)]
#[ignore = "extended soak; run explicitly with --ignored"]
async fn extended_soak_same_seed_determinism_across_seeds() {
    let seeds: u64 = std::env::var("RUSTKV_SOAK_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    for seed in 0..seeds {
        let (trace_a, log_a, compacted_a, stats_a) =
            randomized_fault_schedule(seed, 0.10, Some(8)).await;
        let (trace_b, log_b, compacted_b, stats_b) =
            randomized_fault_schedule(seed, 0.10, Some(8)).await;
        assert_eq!(trace_a, trace_b, "seed {seed}: traces diverge");
        assert_eq!(log_a, log_b, "seed {seed}: retained logs diverge");
        assert_eq!(
            (compacted_a, stats_a),
            (compacted_b, stats_b),
            "seed {seed}: compaction or fault counts diverge"
        );
    }
}

// ---- T2 checker sensitivity: the final-consistency checker itself must
// reject the violations it claims to guard against. Each probe runs a real
// healthy cluster, then presents the checker with evidence of a specific
// violation — a lost acknowledged write, a replaced acknowledged write, an
// at-least-once double commit, a double-applied (diverged) replica — and
// the run must FAIL. These are negative tests of the checker, not of
// rustkv: a checker that never fires is indistinguishable from one that
// works. ----

/// A quiet 3-node cluster with one genuinely confirmed write, the fixture
/// every sensitivity probe below tampers with.
async fn healthy_cluster_with_one_confirmed_write(seed: u64) -> (TestCluster, Vec<Confirmed>) {
    let cluster = spawn_cluster(3, seed, low_loss_faults());
    cluster.wait_for_leader().await;
    let mut confirmed = Vec::new();
    let mut next_value = 0;
    write_until_confirmed(
        &cluster,
        &cluster.all_ids(),
        &mut next_value,
        &mut confirmed,
    )
    .await;
    converge(&cluster).await;
    (cluster, confirmed)
}

/// A lost acknowledged write: the client was told v999 committed, but no
/// node ever applied it. The final-state check must refuse.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "missing from the final state")]
async fn the_checker_rejects_a_lost_acknowledged_write() {
    let (cluster, mut confirmed) = healthy_cluster_with_one_confirmed_write(62).await;
    confirmed.push(Confirmed {
        term: confirmed[0].term,
        index: confirmed[0].index,
        value: 999,
        session: None,
    });
    assert_final_consistency(&cluster, &confirmed, "lost-ack probe").await;
}

/// A replaced acknowledged write: the log no longer holds the confirmed
/// command at its assigned (term, index) — what a rolled-back-but-acked
/// entry looks like. The disk-log check must refuse.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "was lost")]
async fn the_checker_rejects_a_replaced_confirmed_write() {
    let (cluster, mut confirmed) = healthy_cluster_with_one_confirmed_write(63).await;
    confirmed[0].term += 1;
    assert_final_consistency(&cluster, &confirmed, "replaced-write probe").await;
}

/// An at-least-once double commit: one key committed twice without a shared
/// dedup token is the log signature of a duplicated untokened apply (the
/// fault workloads write unique per-value keys, so it can't arise
/// legitimately there). The log-scan rule must refuse.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "without a shared dedup token")]
async fn the_checker_rejects_an_untokened_double_commit() {
    let (cluster, confirmed) = healthy_cluster_with_one_confirmed_write(64).await;
    for value in [1, 2] {
        while try_confirmed_command(&cluster, &cluster.all_ids(), put("dup", value))
            .await
            .is_none()
        {
            tokio::time::sleep(ms(50)).await;
        }
    }
    // Let every replica apply both commits, so the only violation left for
    // the checker to find is the untokened double occupancy in the log.
    converge(&cluster).await;
    wait_until("all replicas apply the second commit", || {
        cluster
            .all_ids()
            .iter()
            .all(|&id| cluster.store(id).get("dup") == Some(json!(2)))
    })
    .await;
    assert_final_consistency(&cluster, &confirmed, "double-commit probe").await;
}

/// A double-applied write, simulated by poking one replica's state machine
/// directly: replica 2 now holds state no correct log replay produced. The
/// cross-node divergence check must refuse.
#[tokio::test(start_paused = true)]
#[should_panic(expected = "state machine diverges")]
async fn the_checker_rejects_a_diverged_state_machine() {
    let (cluster, confirmed) = healthy_cluster_with_one_confirmed_write(65).await;
    cluster.store(2).put("v_forged".to_string(), json!(1));
    assert_final_consistency(&cluster, &confirmed, "divergence probe").await;
}

// ---- the teardown safety assert itself has teeth ----

/// Forges conflicting leadership claims through a bare transport registered
/// on a live cluster's network: `TestCluster::shutdown` must refuse to pass.
/// (The recording logic is unit-tested in sim.rs; this pins the teardown
/// assert wiring end-to-end.)
#[tokio::test(start_paused = true)]
#[should_panic(expected = "sim-observed safety violations")]
async fn forged_leadership_conflict_fails_the_run_at_teardown() {
    use rustkv::raft::rpc::{AppendEntriesArgs, RpcRequest};

    let cluster = spawn_cluster(3, 61, low_loss_faults());
    cluster.wait_for_leader().await;

    let (forger, _rx) = cluster.net.register(99);
    for forged_leader in [97, 98] {
        let _ = rustkv::raft::transport::Transport::send(
            &forger,
            1,
            RpcRequest::AppendEntries(AppendEntriesArgs {
                term: 999,
                leader_id: forged_leader,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            }),
        )
        .await;
    }
    cluster.shutdown();
}

// ---- majority loss: writes stall, CheckQuorum deposes the survivor, and a
// restart restores liveness (with the stalled proposal still committing) ----

#[tokio::test(start_paused = true)]
async fn writes_stall_without_majority_and_resume_after_restart() {
    let cluster = spawn_cluster(3, 51, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    let p1 = cluster
        .handle(leader.id)
        .propose(put("v1", 1))
        .await
        .unwrap();
    assert_eq!(p1.committed.await, Ok(true));

    let followers: Vec<NodeId> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != leader.id)
        .collect();
    cluster.crash(followers[0]);
    cluster.crash(followers[1]);
    tokio::time::sleep(ms(50)).await;

    // The lone survivor — still leader, the silence being younger than the
    // check-quorum window — accepts the proposal but must not confirm it.
    let mut p2 = cluster
        .handle(leader.id)
        .propose(put("v2", 2))
        .await
        .unwrap();
    let stalled = tokio::time::timeout(Duration::from_secs(3), &mut p2.committed).await;
    assert!(stalled.is_err(), "no commit without a majority");
    assert_eq!(
        cluster.store(leader.id).get("v2"),
        None,
        "never applied either"
    );
    // CheckQuorum (phase 12): inside that window the survivor noticed it
    // can't hear a majority and stepped down — at its own term, with the
    // pending proposal deliberately kept alive across the step-down.
    let deposed = cluster.handle(leader.id).status();
    assert_ne!(
        deposed.role,
        RoleKind::Leader,
        "check-quorum deposes the lone survivor"
    );
    assert_eq!(deposed.term, leader.term, "step-down never bumps the term");

    // One follower back = a majority of 2/3. The old leader's LONGER log —
    // it still holds v2 — denies the restarted follower's pre-votes and
    // wins the election itself, and the write that stalled under its old
    // leadership finally commits: the payoff of pending proposals
    // surviving step-down (the phase 5 design decision).
    cluster.restart(followers[0]).await;
    let new = cluster
        .wait_for_leader_among(&[leader.id, followers[0]])
        .await;
    assert_eq!(new.id, leader.id, "the longer log wins the re-election");
    assert!(new.term > leader.term);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), &mut p2.committed).await,
        Ok(Ok(true)),
        "the stalled proposal commits once a majority exists"
    );
    wait_until("stalled write applies on the restored majority", || {
        cluster.store(leader.id).get("v2").is_some()
            && cluster.store(followers[0]).get("v2").is_some()
    })
    .await;

    // Full recovery and the usual final consistency checks.
    cluster.restart(followers[1]).await;
    converge(&cluster).await;
    let confirmed = vec![Confirmed {
        term: p1.term,
        index: p1.index,
        value: 1,
        session: None,
    }];
    assert_final_consistency(&cluster, &confirmed, "majority-loss scenario").await;
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).get("v2"),
            Some(serde_json::json!(2)),
            "node {id}: v2 committed after recovery"
        );
    }
}
