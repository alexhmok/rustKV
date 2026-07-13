//! Phase 6: deterministic fault tests on the simulated transport with
//! virtual time — every scenario is a pure function of its seed.
//!
//! Safety invariants asserted:
//! - at most one leader per term (sampled continuously through each run);
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
//! loss, write stall without a majority + resumption after restart, and a
//! same-seed determinism check over the randomized schedule.
//!
//! Honest limits: leader-per-term is checked by sampling (between driver
//! steps), not event-level interception; each value is proposed at most
//! once, so client-level retry duplication is out of scope (see PLAN.md).

mod common;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use common::*;
use rustkv::raft::node::{RoleKind, Status};
use rustkv::raft::types::{Command, LogEntry, LogIndex, NodeId, Term};
use rustkv::rng::SplitMix64;

/// A write whose commit was positively observed.
type Confirmed = (Term, LogIndex, u64);

fn observe_leaders(
    cluster: &TestCluster,
    alive: &[NodeId],
    seen: &mut HashMap<Term, NodeId>,
    context: &str,
) {
    for status in cluster.statuses_among(alive) {
        if status.role == RoleKind::Leader {
            let prev = seen.entry(status.term).or_insert(status.id);
            assert_eq!(
                *prev, status.id,
                "{context}: two leaders observed in term {}",
                status.term
            );
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

/// One attempt: find a unique leader among `ids`, propose `value`, and wait
/// up to 2 virtual seconds for the commit. `None` = outcome unknown or no
/// leader; the caller must NOT reuse the value (it may still commit later).
async fn try_confirmed_write(
    cluster: &TestCluster,
    ids: &[NodeId],
    value: u64,
) -> Option<(Term, LogIndex)> {
    let leader = unique_leader(cluster, ids)?;
    let proposal = cluster
        .handle(leader.id)
        .propose(put(&format!("v{value}"), value))
        .await
        .ok()?;
    match tokio::time::timeout(ms(2000), proposal.committed).await {
        Ok(Ok(true)) => Some((proposal.term, proposal.index)),
        _ => None,
    }
}

/// Burns values until one write is confirmed among `ids`; panics after 100
/// attempts (a liveness failure worth failing loudly on).
async fn write_until_confirmed(
    cluster: &TestCluster,
    ids: &[NodeId],
    next_value: &mut u64,
    confirmed: &mut Vec<Confirmed>,
) {
    for _ in 0..100 {
        *next_value += 1;
        if let Some((term, index)) = try_confirmed_write(cluster, ids, *next_value).await {
            confirmed.push((term, index, *next_value));
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
/// (term, index); no Put key may appear twice (each value proposed once).
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
    for &(term, index, value) in confirmed {
        let entry = &reference_log[usize::try_from(index - 1).unwrap()];
        assert_eq!(
            (entry.term, &entry.command),
            (term, &put(&format!("v{value}"), value)),
            "{context}: confirmed write v{value} (term {term}, index {index}) was lost"
        );
    }
    let mut keys = HashSet::new();
    for entry in &reference_log {
        if let Command::Put { key, .. } = &entry.command {
            assert!(
                keys.insert(key.clone()),
                "{context}: key {key} committed twice"
            );
        }
    }
}

// ---- leader crash/restart mid-write ----

#[tokio::test(start_paused = true)]
async fn leader_crash_mid_write_preserves_confirmed_writes() {
    for seed in [41, 42, 43, 44, 45] {
        let context = format!("seed {seed}");
        let mut cluster = spawn_cluster(3, seed, low_loss_faults());
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
        confirmed.push((p1.term, p1.index, 1));

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
        confirmed.push((p3.term, p3.index, 3));

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
        let mut leaders_by_term = HashMap::new();
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
            observe_leaders(&cluster, &all, &mut leaders_by_term, &context);

            for &id in &majority {
                cluster.net.set_pair_blocked(victim, id, false);
            }
            tokio::time::sleep(ms(500)).await; // let the victim reintegrate
            observe_leaders(&cluster, &all, &mut leaders_by_term, &context);
        }

        converge(&cluster).await;
        assert_eq!(confirmed.len(), 10, "{context}: two writes per cycle");
        assert_final_consistency(&cluster, &confirmed, &context).await;
    }
}

// ---- randomized fault schedules ----

/// Runs a seeded random schedule of writes, partitions, heals, crashes and
/// restarts under 10% message loss, asserting invariants throughout.
/// Returns the action trace and the final log for determinism checks.
async fn randomized_fault_schedule(seed: u64) -> (Vec<String>, Vec<LogEntry>) {
    let context = format!("seed {seed}");
    let faults = rustkv::raft::transport::sim::FaultConfig {
        min_delay: ms(1),
        max_delay: ms(15),
        drop_probability: 0.10,
        rpc_timeout: ms(40),
    };
    let mut cluster = spawn_cluster(3, seed, faults);
    let all = cluster.all_ids();
    let mut rng = SplitMix64::new(seed.wrapping_mul(0x5851_F42D_4C95_7F2D).wrapping_add(99));
    let mut trace = Vec::new();
    let mut confirmed: Vec<Confirmed> = Vec::new();
    let mut leaders_by_term = HashMap::new();
    let mut next_value = 0u64;
    let mut crashed: Option<NodeId> = None; // at most one down at a time
    let mut isolated: Vec<NodeId> = Vec::new(); // kept sorted for determinism

    for step in 0..40 {
        let alive: Vec<NodeId> = all
            .iter()
            .copied()
            .filter(|id| Some(*id) != crashed)
            .collect();
        observe_leaders(&cluster, &alive, &mut leaders_by_term, &context);

        match rng.next_range(0..=9) {
            // Writes are the most common action.
            0..=4 => {
                next_value += 1;
                match try_confirmed_write(&cluster, &alive, next_value).await {
                    Some((term, index)) => {
                        confirmed.push((term, index, next_value));
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
    observe_leaders(&cluster, &all, &mut leaders_by_term, &context);
    converge(&cluster).await;
    trace.push(format!("done: {} confirmed writes", confirmed.len()));
    assert_final_consistency(&cluster, &confirmed, &context).await;
    (trace, cluster.disk_log(1))
}

#[tokio::test(start_paused = true)]
async fn randomized_fault_schedules_preserve_safety_across_seeds() {
    for seed in 0..8 {
        randomized_fault_schedule(seed).await;
    }
}

#[tokio::test(start_paused = true)]
async fn same_seed_reproduces_the_same_fault_run() {
    let (trace_a, log_a) = randomized_fault_schedule(5).await;
    let (trace_b, log_b) = randomized_fault_schedule(5).await;
    assert_eq!(trace_a, trace_b, "action/outcome traces must be identical");
    assert_eq!(log_a, log_b, "final logs must be identical");
}

// ---- majority loss stalls writes; restart restores liveness ----

#[tokio::test(start_paused = true)]
async fn writes_stall_without_majority_and_resume_after_restart() {
    let mut cluster = spawn_cluster(3, 51, low_loss_faults());
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

    // The lone survivor accepts the proposal but must not confirm it.
    let p2 = cluster
        .handle(leader.id)
        .propose(put("v2", 2))
        .await
        .unwrap();
    let stalled = tokio::time::timeout(Duration::from_secs(3), p2.committed).await;
    assert!(stalled.is_err(), "no commit without a majority");
    assert_eq!(
        cluster.store(leader.id).get("v2"),
        None,
        "never applied either"
    );

    // One follower back = majority of 2/3: the stalled write must land.
    cluster.restart(followers[0]).await;
    wait_until("stalled write commits once a majority exists", || {
        cluster.store(leader.id).get("v2").is_some()
            && cluster.store(followers[0]).get("v2").is_some()
    })
    .await;

    // Full recovery and the usual final consistency checks.
    cluster.restart(followers[1]).await;
    converge(&cluster).await;
    let confirmed = vec![(p1.term, p1.index, 1)];
    assert_final_consistency(&cluster, &confirmed, "majority-loss scenario").await;
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).get("v2"),
            Some(serde_json::json!(2)),
            "node {id}: v2 committed after recovery"
        );
    }
}
