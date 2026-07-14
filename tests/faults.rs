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
async fn randomized_fault_schedule(
    seed: u64,
    duplicate_probability: f64,
) -> (Vec<String>, Vec<LogEntry>) {
    let context = format!("seed {seed}");
    let faults = rustkv::raft::transport::sim::FaultConfig {
        min_delay: ms(1),
        max_delay: ms(15),
        drop_probability: 0.10,
        duplicate_probability,
        rpc_timeout: ms(40),
    };
    let cluster = spawn_cluster(3, seed, faults);
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
    assert_final_consistency(&cluster, &confirmed, &context).await;
    (trace, cluster.disk_log(1))
}

#[tokio::test(start_paused = true)]
async fn randomized_fault_schedules_preserve_safety_across_seeds() {
    for seed in 0..8 {
        randomized_fault_schedule(seed, 0.0).await;
    }
}

/// The duplication soak: the same randomized schedules with 10% of all
/// requests delivered twice, exercising the duplicate-tolerant
/// AppendEntries walk (and vote idempotence) end-to-end under partitions,
/// crashes and loss at once.
#[tokio::test(start_paused = true)]
async fn randomized_fault_schedules_survive_message_duplication() {
    for seed in 0..8 {
        randomized_fault_schedule(seed, 0.10).await;
    }
}

#[tokio::test(start_paused = true)]
async fn same_seed_reproduces_the_same_fault_run() {
    let (trace_a, log_a) = randomized_fault_schedule(5, 0.10).await;
    let (trace_b, log_b) = randomized_fault_schedule(5, 0.10).await;
    assert_eq!(trace_a, trace_b, "action/outcome traces must be identical");
    assert_eq!(log_a, log_b, "final logs must be identical");
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
