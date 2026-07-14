//! Phase 15: dynamic membership (single-server changes) on the
//! deterministic simulator (seeded, virtual time).
//!
//! Scenarios:
//! - grow 3→4 and shrink 4→3 UNDER LIVE WRITES, with the WGL
//!   linearizability checker over the whole history — the membership change
//!   is the nemesis;
//! - removing the LEADER: the entry commits, the leader steps down, the
//!   survivors elect (the phase-10 event-level election-safety observer is
//!   asserted at teardown, which is exactly what it exists for);
//! - joiner catch-up BOTH ways: purely via InstallSnapshot when the
//!   survivors compacted (the phase-14 payoff — the joiner is spawned with
//!   snapshotting OFF, so the snapshot on its disk can only have arrived
//!   over the wire), and via plain AppendEntries backfill when nothing was
//!   ever compacted (byte-identical logs from index 1);
//! - the phantom-member trap: a leader crashes out of an uncommitted
//!   ConfigChange, the truncation forces a membership rescan, and the
//!   once-phantomed node proves it by WINNING an election and committing
//!   under the correct (smaller) quorum;
//! - proposal-time validation: single-server delta only, one change in
//!   flight at a time, nothing before this term's no-op commits, never
//!   down to an empty configuration;
//! - a joiner never campaigns before a configuration includes it;
//! - the named removed-server disruption scenario (thesis §4.2.3, deferred
//!   here from phases 11/12): a removed follower left RUNNING keeps
//!   probing forever but never disturbs the members — and even during the
//!   stickiness-lapsed window of a real election (leader crash) it cannot
//!   win, because the committed removal entry makes every member's log
//!   strictly longer than its own. See PLAN.md for the decision record.

mod common;

use std::sync::Arc;

use common::lin::{OpKind, Recorded, WriteKind, WriteOutcome, check_linearizable};
use common::*;
use rustkv::raft::node::{ProposeError, RoleKind};
use rustkv::raft::transport::sim::FaultConfig;
use rustkv::raft::types::{Command, LogIndex, Membership, NodeId, Session};
use rustkv::rng::SplitMix64;
use serde_json::json;
use tokio::time::Instant;

/// Proposes `key = value` on the current leader among `ids` and waits for
/// the commit confirmation (low-loss scenarios: this must simply succeed).
async fn confirm_put(cluster: &TestCluster, ids: &[NodeId], key: &str, value: u64) -> LogIndex {
    let leader = cluster.wait_for_leader_among(ids).await;
    let proposal = cluster
        .handle(leader.id)
        .propose(put(key, value))
        .await
        .expect("leader accepts the proposal");
    assert_eq!(
        proposal.committed.await,
        Ok(true),
        "write {key}={value} must commit"
    );
    proposal.index
}

/// Waits until every node in `ids` reports the same fully-committed last
/// index (same length + all committed ⇒ identical logs, per log matching).
async fn converge_among(cluster: &TestCluster, ids: &[NodeId]) {
    wait_until("nodes converge (equal logs, fully committed)", || {
        let statuses = cluster.statuses_among(ids);
        let max_last = statuses.iter().map(|s| s.last_log_index).max().unwrap();
        statuses
            .iter()
            .all(|s| s.last_log_index == max_last && s.commit_index == max_last)
    })
    .await;
}

/// Proposes a ConfigChange on the current leader among `ids` — the current
/// membership with `mutate` applied — and waits for the commit.
async fn commit_config(
    cluster: &TestCluster,
    ids: &[NodeId],
    mutate: impl FnOnce(&mut Membership),
) -> LogIndex {
    let leader = cluster.wait_for_leader_among(ids).await;
    let mut members = cluster.handle(leader.id).membership();
    mutate(&mut members);
    let proposal = cluster
        .handle(leader.id)
        .propose(Command::ConfigChange { members })
        .await
        .expect("leader accepts the configuration change");
    assert_eq!(
        proposal.committed.await,
        Ok(true),
        "the configuration change must commit"
    );
    proposal.index
}

// ---- grow/shrink under live writes, checked for linearizability ----

const KEYS: [&str; 2] = ["a", "b"];

fn unique_leader(cluster: &TestCluster) -> Option<NodeId> {
    let leaders: Vec<NodeId> = cluster
        .statuses_among(&cluster.alive_ids())
        .into_iter()
        .filter(|s| s.role == RoleKind::Leader)
        .map(|s| s.id)
        .collect();
    match leaders[..] {
        [leader] => Some(leader),
        _ => None,
    }
}

/// One client process: randomized linearizable reads (any node; refused/
/// unconfirmed reads observed nothing and are skipped) and tokened writes
/// retried through ambiguity (phase 13) — the same recording rules as the
/// jepsen workload, sized down for a membership-change scenario.
async fn client_workload(
    cluster: Arc<TestCluster>,
    process: u64,
    seed: u64,
    ops: u64,
    start: Instant,
) -> Vec<Recorded> {
    let mut rng = SplitMix64::new(seed.wrapping_mul(97).wrapping_add(process + 11));
    let mut history = Vec::new();
    let mut seq = 0u64;
    for op_no in 1..=ops {
        tokio::time::sleep(ms(rng.next_range(10..=90))).await;
        let key = KEYS[rng.next_range(0..=1) as usize];
        if rng.next_range(0..=9) < 5 {
            // Linearizable read from a random live node.
            let alive = cluster.alive_ids();
            let node = alive[rng.next_range(0..=(alive.len() as u64 - 1)) as usize];
            let invoked_us = start.elapsed().as_micros() as u64;
            let Ok(ticket) = cluster.handle(node).read().await else {
                continue;
            };
            let Ok(Ok(())) = tokio::time::timeout(ms(1500), ticket.granted).await else {
                continue;
            };
            let result = cluster.store(node).get(key).and_then(|v| v.as_u64());
            history.push(Recorded {
                process,
                key: key.to_string(),
                op: OpKind::Read { result },
                invoked_us,
                returned_us: start.elapsed().as_micros() as u64 + 1,
            });
        } else {
            seq += 1;
            let value = (process + 1) * 1_000_000 + seq;
            let command = Command::Put {
                key: key.to_string(),
                value: json!(value),
                session: Some(Session {
                    client: process,
                    seq: op_no,
                }),
            };
            let invoked_us = start.elapsed().as_micros() as u64;
            let mut outcome = WriteOutcome::Fail;
            let mut ambiguous = false;
            for _attempt in 0..4 {
                let Some(leader) = unique_leader(&cluster) else {
                    tokio::time::sleep(ms(30)).await;
                    continue;
                };
                match cluster.handle(leader).propose(command.clone()).await {
                    Err(_) => {}
                    Ok(p) => match tokio::time::timeout(ms(500), p.committed).await {
                        Ok(Ok(true)) => {
                            outcome = WriteOutcome::Ok;
                            break;
                        }
                        Ok(Ok(false)) => {}
                        _ => ambiguous = true,
                    },
                }
                tokio::time::sleep(ms(30)).await;
            }
            if outcome != WriteOutcome::Ok && ambiguous {
                outcome = WriteOutcome::Unknown;
            }
            let returned_us = if outcome == WriteOutcome::Unknown {
                u64::MAX
            } else {
                start.elapsed().as_micros() as u64
            };
            history.push(Recorded {
                process,
                key: key.to_string(),
                op: OpKind::Write {
                    kind: WriteKind::Put(value),
                    outcome,
                    log_pos: None,
                },
                invoked_us,
                returned_us,
            });
        }
    }
    history
}

/// Jepsen-style final reads pinning down unknown writes, appended to the
/// history after full convergence.
async fn final_reads(cluster: &TestCluster, history: &mut Vec<Recorded>, start: Instant) {
    for key in KEYS {
        let invoked_us = start.elapsed().as_micros() as u64;
        let result = loop {
            let Some(leader) = unique_leader(cluster) else {
                tokio::time::sleep(ms(10)).await;
                continue;
            };
            let Ok(ticket) = cluster.handle(leader).read().await else {
                continue;
            };
            if let Ok(Ok(())) = tokio::time::timeout(ms(1500), ticket.granted).await {
                break cluster.store(leader).get(key).and_then(|v| v.as_u64());
            }
        };
        history.push(Recorded {
            process: 99,
            key: key.to_string(),
            op: OpKind::Read { result },
            invoked_us,
            returned_us: start.elapsed().as_micros() as u64 + 1,
        });
    }
}

fn assert_states_identical(cluster: &TestCluster, ids: &[NodeId]) {
    let reference = cluster.store(ids[0]).export();
    for &id in ids {
        assert_eq!(
            cluster.store(id).export(),
            reference,
            "node {id}: state machine diverges"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn grow_3_to_4_under_live_writes_stays_linearizable() {
    let seed = 151;
    let cluster = Arc::new(spawn_cluster(3, seed, low_loss_faults()));
    let start = Instant::now();
    cluster.wait_for_leader().await;

    let clients: Vec<_> = (0..2u64)
        .map(|p| tokio::spawn(client_workload(Arc::clone(&cluster), p, seed, 10, start)))
        .collect();

    // The membership change IS the nemesis: grow to 4 mid-workload.
    tokio::time::sleep(ms(150)).await;
    cluster.add_node(4);
    commit_config(&cluster, &[1, 2, 3], |m| {
        m.insert(4, member_addr(4));
    })
    .await;

    let mut history = Vec::new();
    for client in clients {
        history.extend(client.await.expect("client task"));
    }
    let all = cluster.all_ids();
    assert_eq!(all.len(), 4);
    converge_among(&cluster, &all).await;
    final_reads(&cluster, &mut history, start).await;
    assert_states_identical(&cluster, &all);
    // Vacuity guard: the workload really overlapped the change.
    let writes = history
        .iter()
        .filter(|r| {
            matches!(
                r.op,
                OpKind::Write {
                    outcome: WriteOutcome::Ok,
                    ..
                }
            )
        })
        .count();
    assert!(
        writes >= 5,
        "too few confirmed writes ({writes}) to mean much"
    );
    if let Err(reason) = check_linearizable(&history) {
        panic!("grow 3→4 produced a linearizability violation:\n{reason}");
    }
    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn shrink_4_to_3_under_live_writes_stays_linearizable() {
    let seed = 152;
    let cluster = Arc::new(spawn_cluster(4, seed, low_loss_faults()));
    let start = Instant::now();
    let leader = cluster.wait_for_leader().await;
    let victim = *cluster
        .all_ids()
        .iter()
        .find(|&&id| id != leader.id)
        .unwrap();

    let clients: Vec<_> = (0..2u64)
        .map(|p| tokio::spawn(client_workload(Arc::clone(&cluster), p, seed, 10, start)))
        .collect();

    tokio::time::sleep(ms(150)).await;
    commit_config(&cluster, &cluster.all_ids(), |m| {
        m.remove(&victim);
    })
    .await;

    let mut history = Vec::new();
    for client in clients {
        history.extend(client.await.expect("client task"));
    }
    // Only now drop the (silent, no-longer-member) victim from the harness:
    // clients sample live nodes, so it must stay registered while they run.
    cluster.remove_node(victim);
    let members: Vec<NodeId> = cluster.all_ids();
    assert_eq!(members.len(), 3);
    converge_among(&cluster, &members).await;
    final_reads(&cluster, &mut history, start).await;
    assert_states_identical(&cluster, &members);
    if let Err(reason) = check_linearizable(&history) {
        panic!("shrink 4→3 produced a linearizability violation:\n{reason}");
    }
    cluster.shutdown();
}

// ---- the membership soak (amendment): churn + partitions + checker ----

/// Drives one configuration change to completion through whoever currently
/// leads, tolerating rejections (guard, in-flight, NotLeader), leadership
/// changes, and ambiguity. `mutate` must be idempotent against the current
/// view: if applying it changes nothing, an earlier ambiguous copy already
/// took effect and the change counts as done (effect-on-append makes the
/// leader's view the authority). Returns false if the change never landed
/// within the attempt budget — the soak just moves on.
async fn try_commit_config(cluster: &TestCluster, mutate: impl Fn(&mut Membership)) -> bool {
    for _ in 0..8 {
        let Some(leader) = unique_leader(cluster) else {
            tokio::time::sleep(ms(40)).await;
            continue;
        };
        let handle = cluster.handle(leader);
        let current = handle.membership();
        let mut target = current.clone();
        mutate(&mut target);
        if target == current {
            return true;
        }
        // A rejection means re-sample and retry; so does a truncated or
        // ambiguous outcome — the next attempt re-reads the view, and the
        // zero-delta check above absorbs a late-committing copy.
        if let Ok(proposal) = handle
            .propose(Command::ConfigChange { members: target })
            .await
            && let Ok(Ok(true)) = tokio::time::timeout(ms(800), proposal.committed).await
        {
            return true;
        }
        tokio::time::sleep(ms(40)).await;
    }
    false
}

/// The jepsen-style workload with membership churn AS the nemesis, plus
/// partition rounds on top: random grow-to-4 / shrink-to-3 rounds (the
/// shrink victim may be the leader — §4.2.2 in the wild) interleaved with
/// node isolation, under loss and tight client timeouts. Every committed
/// read/write lands in one history checked by the WGL linearizability
/// checker; the event-level election-safety observer is asserted at
/// teardown as always. Returns (grows, shrinks, confirmed writes).
async fn membership_soak(seed: u64) -> (u64, u64, usize) {
    let faults = FaultConfig {
        min_delay: ms(1),
        max_delay: ms(15),
        drop_probability: 0.05,
        duplicate_probability: 0.0,
        rpc_timeout: ms(40),
    };
    let cluster = Arc::new(spawn_cluster(3, seed, faults));
    let start = Instant::now();
    cluster.wait_for_leader().await;

    let clients: Vec<_> = (0..2u64)
        .map(|p| tokio::spawn(client_workload(Arc::clone(&cluster), p, seed, 12, start)))
        .collect();

    let mut rng = SplitMix64::new(seed ^ 0x00C0_FFEE);
    let mut next_id = 4u64;
    let (mut grows, mut shrinks) = (0u64, 0u64);
    for _round in 0..6 {
        tokio::time::sleep(ms(rng.next_range(60..=220))).await;
        if rng.next_range(0..=9) < 3 {
            // Partition a random node for a while, then heal it.
            let ids = cluster.alive_ids();
            let victim = ids[rng.next_range(0..=(ids.len() as u64 - 1)) as usize];
            for other in cluster.all_ids() {
                if other != victim {
                    cluster.net.set_pair_blocked(victim, other, true);
                }
            }
            tokio::time::sleep(ms(rng.next_range(100..=300))).await;
            for other in cluster.all_ids() {
                if other != victim {
                    cluster.net.set_pair_blocked(victim, other, false);
                }
            }
        } else {
            let Some(leader) = unique_leader(&cluster) else {
                continue;
            };
            let members = cluster.handle(leader).membership();
            if members.len() <= 3 {
                let id = next_id;
                next_id += 1;
                cluster.add_node(id);
                if try_commit_config(&cluster, |m| {
                    m.insert(id, member_addr(id));
                })
                .await
                {
                    grows += 1;
                }
            } else {
                // Any member may be the victim — the leader included.
                let candidates: Vec<NodeId> = members.keys().copied().collect();
                let victim = candidates[rng.next_range(0..=(candidates.len() as u64 - 1)) as usize];
                if try_commit_config(&cluster, |m| {
                    m.remove(&victim);
                })
                .await
                {
                    shrinks += 1;
                }
            }
        }
    }

    let mut history = Vec::new();
    for client in clients {
        history.extend(client.await.expect("client task"));
    }

    // Heal everything and settle on the authoritative final membership.
    for a in cluster.all_ids() {
        for b in cluster.all_ids() {
            if a != b {
                cluster.net.set_pair_blocked(a, b, false);
            }
        }
    }
    let final_members: Vec<NodeId> =
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(leader) = unique_leader(&cluster) {
                    break cluster
                        .handle(leader)
                        .membership()
                        .keys()
                        .copied()
                        .collect();
                }
                tokio::time::sleep(ms(10)).await;
            }
        })
        .await
        .expect("a leader emerges after the final heal");
    converge_among(&cluster, &final_members).await;
    final_reads(&cluster, &mut history, start).await;
    assert_states_identical(&cluster, &final_members);

    let confirmed = history
        .iter()
        .filter(|r| {
            matches!(
                r.op,
                OpKind::Write {
                    outcome: WriteOutcome::Ok,
                    ..
                }
            )
        })
        .count();
    if let Err(reason) = check_linearizable(&history) {
        panic!("seed {seed}: membership soak produced a violation:\n{reason}");
    }
    cluster.shutdown();
    (grows, shrinks, confirmed)
}

#[tokio::test(start_paused = true)]
async fn membership_churn_soak_stays_linearizable() {
    let (mut grows, mut shrinks, mut confirmed) = (0, 0, 0);
    for seed in [201, 202, 203] {
        let (g, s, c) = membership_soak(seed).await;
        eprintln!("seed {seed}: grows={g} shrinks={s} confirmed_writes={c}");
        grows += g;
        shrinks += s;
        confirmed += c;
    }
    // Vacuity guards: the seed set must really churn membership both ways
    // under a working workload, or the checker's silence means nothing.
    assert!(
        grows >= 2,
        "only {grows} committed grows across the seed set"
    );
    assert!(shrinks >= 1, "no committed shrink across the seed set");
    assert!(
        confirmed >= 15,
        "only {confirmed} confirmed writes across the seed set"
    );
}

// ---- removing the leader (§4.2.2) ----

#[tokio::test(start_paused = true)]
async fn removing_the_leader_commits_then_it_steps_down_and_survivors_elect() {
    let cluster = spawn_cluster(3, 153, low_loss_faults());
    let all = cluster.all_ids();
    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "before", 1).await;
    converge_among(&cluster, &all).await;

    // The leader proposes its OWN removal: it stops counting itself
    // immediately, keeps replicating, and the commit above proves the new
    // majority (2 of 2 followers) carried it without the leader's vote.
    commit_config(&cluster, &all, |m| {
        m.remove(&leader.id);
    })
    .await;

    // Step-down on commit: the deposed leader drops to follower without
    // waiting for anything else to happen.
    wait_until("the removed leader steps down", || {
        cluster.handle(leader.id).status().role != RoleKind::Leader
    })
    .await;

    // The survivors elect among themselves and keep serving writes; the
    // removed ex-leader (still running) never disrupts — asserted both here
    // and by the event-level election-safety observer at shutdown.
    let survivors: Vec<NodeId> = all.iter().copied().filter(|&id| id != leader.id).collect();
    let successor = cluster.wait_for_leader_among(&survivors).await;
    assert!(survivors.contains(&successor.id));
    assert!(successor.term > leader.term, "a fresh election happened");
    confirm_put(&cluster, &survivors, "after", 2).await;
    converge_among(&cluster, &survivors).await;
    assert_eq!(
        cluster.handle(leader.id).status().role,
        RoleKind::Follower,
        "the removed ex-leader must stay a silent follower"
    );
    cluster.shutdown();
}

/// The subtlest edit of the phase, made observable: a self-removing leader
/// must count commits by the NEW configuration's majority alone. Cluster of
/// 4, two followers severed: after the leader appends its own removal, the
/// new config {2,3,4} needs 2 of the 3 — but only one follower is
/// reachable, so counting the leader itself (the bug) is the ONLY way this
/// entry could commit inside the window. It must not.
#[tokio::test(start_paused = true)]
async fn self_removing_leader_does_not_count_itself_toward_commit() {
    let cluster = spawn_cluster(4, 162, low_loss_faults());
    let all = cluster.all_ids();
    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "before", 1).await;
    converge_among(&cluster, &all).await;

    let followers: Vec<NodeId> = all.iter().copied().filter(|&id| id != leader.id).collect();
    for &f in &followers[1..] {
        for &other in &all {
            if other != f {
                cluster.net.set_pair_blocked(f, other, true);
            }
        }
    }

    let mut members = cluster.handle(leader.id).membership();
    members.remove(&leader.id);
    let mut proposal = cluster
        .handle(leader.id)
        .propose(Command::ConfigChange { members })
        .await
        .expect("the leader accepts its own removal");
    // One reachable follower's ack + the (no longer counted) leader is NOT
    // a majority of the new 3-member config: the entry must stay pending.
    // (The window closes when CheckQuorum deposes the leader at ~300ms.)
    assert!(
        tokio::time::timeout(ms(200), &mut proposal.committed)
            .await
            .is_err(),
        "the removal committed with one follower ack — the leader counted itself"
    );

    // Heal: the entry replicates for real, commits under the true majority,
    // and the leader steps down.
    for &f in &followers[1..] {
        for &other in &all {
            if other != f {
                cluster.net.set_pair_blocked(f, other, false);
            }
        }
    }
    assert_eq!(proposal.committed.await, Ok(true));
    wait_until("the removed leader steps down", || {
        cluster.handle(leader.id).status().role != RoleKind::Leader
    })
    .await;
    cluster.wait_for_leader_among(&followers).await;
    confirm_put(&cluster, &followers, "after", 2).await;
    cluster.shutdown();
}

// ---- joiner catch-up, both paths ----

/// The InstallSnapshot path (the phase-14 payoff). The joiner is spawned
/// with snapshotting OFF, so it can never self-compact: the snapshot found
/// on its disk afterwards can only have arrived via InstallSnapshot, and
/// its retained log provably never held the compacted prefix.
#[tokio::test(start_paused = true)]
async fn joiner_catches_up_purely_via_install_snapshot() {
    let cluster = spawn_cluster_with_threshold(3, 154, low_loss_faults(), Some(8));
    let originals = cluster.all_ids();
    cluster.wait_for_leader().await;
    for i in 1..=20u64 {
        confirm_put(&cluster, &originals, &format!("k{i}"), i).await;
    }
    converge_among(&cluster, &originals).await;
    // 21+ applied entries at threshold 8 ⇒ every original's boundary is at
    // least 16 by construction (trigger fires at every 8-entry gap).

    cluster.add_node_with(4, None, 0);
    commit_config(&cluster, &originals, |m| {
        m.insert(4, member_addr(4));
    })
    .await;

    let all = cluster.all_ids();
    converge_among(&cluster, &all).await;
    assert_states_identical(&cluster, &all);
    for i in 1..=20u64 {
        assert_eq!(cluster.store(4).get(&format!("k{i}")), Some(json!(i)));
    }

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    // The proof. The joiner never compacts (threshold None), so this
    // snapshot arrived over the wire...
    let snapshot = cluster
        .disk_snapshot(4)
        .expect("the joiner can only own a snapshot via InstallSnapshot");
    assert!(
        snapshot.last_included_index >= 16,
        "boundary {} below what the survivors provably compacted",
        snapshot.last_included_index
    );
    // ...and the compacted prefix never existed on it as entries.
    let log = cluster.disk_log(4);
    assert!(
        log.iter().all(|e| e.index > snapshot.last_included_index),
        "the joiner's log reaches into the compacted prefix — something \
         backfilled what only a snapshot should carry"
    );
    if let Some(first) = log.first() {
        assert_eq!(first.index, snapshot.last_included_index + 1);
    }
}

/// The AppendEntries path: with snapshotting off nothing is ever compacted,
/// so the joiner backfills the whole history entry by entry — byte-identical
/// logs from index 1, and no snapshot file anywhere near it.
#[tokio::test(start_paused = true)]
async fn joiner_backfills_via_append_entries_when_nothing_was_compacted() {
    let cluster = spawn_cluster(3, 155, low_loss_faults());
    let originals = cluster.all_ids();
    cluster.wait_for_leader().await;
    for i in 1..=10u64 {
        confirm_put(&cluster, &originals, &format!("k{i}"), i).await;
    }

    cluster.add_node(4);
    commit_config(&cluster, &originals, |m| {
        m.insert(4, member_addr(4));
    })
    .await;

    let all = cluster.all_ids();
    converge_among(&cluster, &all).await;
    assert_states_identical(&cluster, &all);

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    assert!(
        cluster.disk_snapshot(4).is_none(),
        "no snapshot may exist on the AE-backfill path"
    );
    let joiner_log = cluster.disk_log(4);
    assert_eq!(joiner_log, cluster.disk_log(1), "logs must be identical");
    assert_eq!(joiner_log.first().map(|e| e.index), Some(1));
}

// ---- the phantom-member trap ----

/// A leader appends a ConfigChange that never reaches anyone, crashes out
/// of leadership (CheckQuorum), and later has the entry truncated by the
/// successor's log. Forgetting to rescan would leave the phantom member in
/// its quorum math — so the test makes the once-phantomed node win the next
/// election and commit a write under the correct 2-of-3 majority, which a
/// 3-of-4 phantom quorum could never satisfy.
#[tokio::test(start_paused = true)]
async fn truncated_config_change_rescans_membership() {
    let seed = 156;
    let cluster = spawn_cluster(3, seed, low_loss_faults());
    let all = cluster.all_ids();
    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "before", 1).await;
    converge_among(&cluster, &all).await;

    // Sever the leader's OUTBOUND request legs: nothing it appends can
    // replicate, and CheckQuorum will depose it.
    let followers: Vec<NodeId> = all.iter().copied().filter(|&id| id != leader.id).collect();
    for &f in &followers {
        cluster.net.set_link_blocked(leader.id, f, true);
    }

    // The doomed ConfigChange: adds a member that does not even exist.
    // Effective on append — the isolated leader now believes in a 4-member
    // cluster (majority 3).
    let mut phantom_members = cluster.handle(leader.id).membership();
    phantom_members.insert(9, member_addr(9));
    let proposal = cluster
        .handle(leader.id)
        .propose(Command::ConfigChange {
            members: phantom_members,
        })
        .await
        .expect("still leader inside the check-quorum window");
    assert_eq!(cluster.handle(leader.id).membership().len(), 4);

    // The followers stop hearing heartbeats, elect a successor, and move on.
    let successor = cluster.wait_for_leader_among(&followers).await;
    assert!(successor.term > leader.term);
    confirm_put(&cluster, &followers, "during", 2).await;

    // Heal: the successor's conflicting entries truncate the uncommitted
    // ConfigChange, which must force the membership rescan.
    for &f in &followers {
        cluster.net.set_link_blocked(leader.id, f, false);
    }
    assert_eq!(
        proposal.committed.await,
        Ok(false),
        "the phantom ConfigChange was truncated, never committed"
    );
    wait_until("the old leader rescans back to 3 members", || {
        cluster.handle(leader.id).membership().len() == 3
    })
    .await;
    converge_among(&cluster, &all).await;

    // Quorum behavior, not internals: crash the successor; the seed is
    // pinned so the once-phantomed node wins the election — possible only
    // under the rescanned 3-member config (a phantom 4-member view needs 3
    // votes and only 2 nodes are alive) — and commits with 2 of 3.
    cluster.crash(successor.id);
    let remaining: Vec<NodeId> = all
        .iter()
        .copied()
        .filter(|&id| id != successor.id)
        .collect();
    let final_leader = cluster.wait_for_leader_among(&remaining).await;
    assert_eq!(
        final_leader.id, leader.id,
        "seed {seed} chosen so the once-phantomed node leads; if this fails \
         after unrelated changes, re-pin the seed"
    );
    confirm_put(&cluster, &remaining, "after", 3).await;
    cluster.shutdown();
}

// ---- proposal-time validation ----

fn fixed_delay_faults() -> FaultConfig {
    FaultConfig {
        min_delay: ms(10),
        max_delay: ms(10),
        drop_probability: 0.0,
        duplicate_probability: 0.0,
        rpc_timeout: ms(50),
    }
}

#[tokio::test(start_paused = true)]
async fn config_changes_are_validated_and_one_at_a_time() {
    let cluster = spawn_cluster(3, 157, fixed_delay_faults());
    let all = cluster.all_ids();
    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "ready", 1).await;
    let handle = cluster.handle(leader.id);
    let members = handle.membership();

    // Not a single-server delta: two additions at once.
    let mut two_adds = members.clone();
    two_adds.insert(4, member_addr(4));
    two_adds.insert(5, member_addr(5));
    let err = handle
        .propose(Command::ConfigChange { members: two_adds })
        .await
        .expect_err("two additions at once must be rejected");
    assert!(
        matches!(err, ProposeError::InvalidConfigChange { .. }),
        "{err}"
    );

    // A zero-delta "change" is rejected too.
    let err = handle
        .propose(Command::ConfigChange {
            members: members.clone(),
        })
        .await
        .expect_err("an identical configuration must be rejected");
    assert!(
        matches!(err, ProposeError::InvalidConfigChange { .. }),
        "{err}"
    );

    // One at a time: a second change is refused while the first is still
    // uncommitted (10ms fixed delays leave a wide-open window).
    let mut add4 = members.clone();
    add4.insert(4, member_addr(4));
    let first = handle
        .propose(Command::ConfigChange {
            members: add4.clone(),
        })
        .await
        .expect("the first change is accepted");
    let mut add5 = add4.clone();
    add5.insert(5, member_addr(5));
    let err = handle
        .propose(Command::ConfigChange { members: add5 })
        .await
        .expect_err("a second change while the first is in flight must be rejected");
    assert!(
        matches!(err, ProposeError::InvalidConfigChange { .. }),
        "{err}"
    );
    assert_eq!(first.committed.await, Ok(true));

    // Once committed, the next single-server change is accepted again.
    cluster.add_node(4); // let the new member actually exist before removal
    commit_config(&cluster, &all, |m| {
        m.remove(&4);
    })
    .await;

    // Non-leaders refuse outright (NotLeader, not validation).
    let follower = all.iter().copied().find(|&id| id != leader.id).unwrap();
    let err = cluster
        .handle(follower)
        .propose(Command::ConfigChange {
            members: cluster.handle(follower).membership(),
        })
        .await
        .expect_err("followers do not take proposals");
    assert!(matches!(err, ProposeError::NotLeader { .. }));
    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn removing_the_last_member_is_rejected() {
    let cluster = spawn_cluster(1, 158, low_loss_faults());
    cluster.wait_for_leader().await;
    confirm_put(&cluster, &[1], "ready", 1).await;
    let err = cluster
        .handle(1)
        .propose(Command::ConfigChange {
            members: Membership::new(),
        })
        .await
        .expect_err("a cluster cannot remove its last member");
    assert!(
        matches!(err, ProposeError::InvalidConfigChange { .. }),
        "{err}"
    );
    cluster.shutdown();
}

/// The §4.1/phase-9 gate: no ConfigChange until this term's no-op has
/// committed. With fixed 10ms message delays there is a deterministic
/// window right after the election in which the leader exists but its
/// no-op is still uncommitted.
#[tokio::test(start_paused = true)]
async fn config_change_is_rejected_until_the_no_op_commits() {
    let cluster = spawn_cluster(3, 159, fixed_delay_faults());
    let leader = cluster.wait_for_leader().await;
    // Precondition, or the window was already gone and this proves nothing:
    // the no-op is appended but not yet committed.
    assert!(
        leader.commit_index < leader.last_log_index,
        "caught the leader after its no-op committed; pick another seed"
    );
    let handle = cluster.handle(leader.id);
    let mut add4 = handle.membership();
    add4.insert(4, member_addr(4));
    let err = handle
        .propose(Command::ConfigChange {
            members: add4.clone(),
        })
        .await
        .expect_err("no configuration change before the no-op commits");
    assert!(
        matches!(err, ProposeError::InvalidConfigChange { .. }),
        "{err}"
    );

    // Once the no-op commits, the very same change is accepted.
    wait_until("the leadership no-op commits", || {
        let s = cluster.handle(leader.id).status();
        s.commit_index >= leader.last_log_index && s.role == RoleKind::Leader
    })
    .await;
    cluster.add_node(4);
    let proposal = handle
        .propose(Command::ConfigChange { members: add4 })
        .await
        .expect("the gate opens once the no-op is committed");
    assert_eq!(proposal.committed.await, Ok(true));
    cluster.shutdown();
}

// ---- the availability guard (amendment: etcd-style strict reconfig) ----

/// The unrecoverable-brick case, closed: adding a member to a single-node
/// cluster makes the new majority 2-of-2 before the joiner has ever been
/// heard — if it never syncs, nothing commits again and CheckQuorum
/// permanently deposes the only node that could fix it. The guard refuses
/// outright (a not-yet-added member counts as unreachable), matching etcd's
/// strict reconfig check; growing from one node means static bootstrap.
#[tokio::test(start_paused = true)]
async fn growing_a_single_node_cluster_is_rejected() {
    let cluster = spawn_cluster(1, 163, low_loss_faults());
    cluster.wait_for_leader().await;
    confirm_put(&cluster, &[1], "ready", 1).await;
    let mut add2 = cluster.handle(1).membership();
    add2.insert(2, member_addr(2));
    let err = cluster
        .handle(1)
        .propose(Command::ConfigChange { members: add2 })
        .await
        .expect_err("growing a single-node cluster must be refused");
    assert!(
        matches!(err, ProposeError::InvalidConfigChange { .. }),
        "{err}"
    );
    // The refusal left the cluster fully functional.
    confirm_put(&cluster, &[1], "still-works", 2).await;
    cluster.shutdown();
}

/// Adding while degraded would stall: 3→4 raises the majority to 3 on
/// append, and with an original down only 2 originals + an uncaught-up
/// joiner can answer. The guard refuses until the down member is heard
/// again — and the same add (with no fourth process even running) is
/// accepted once all three originals are reachable.
#[tokio::test(start_paused = true)]
async fn add_is_rejected_while_a_member_is_unreachable() {
    let cluster = spawn_cluster(3, 164, low_loss_faults());
    let all = cluster.all_ids();
    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "ready", 1).await;
    converge_among(&cluster, &all).await;

    let down = *all.iter().find(|&&id| id != leader.id).unwrap();
    cluster.crash(down);
    // Let the crashed member's last contact age past the guard's window
    // (election_timeout_max) — but not so long that unrelated churn starts.
    tokio::time::sleep(ms(400)).await;
    let handle = cluster.handle(leader.id);
    assert_eq!(
        handle.status().role,
        RoleKind::Leader,
        "one lost follower must not depose the leader (CheckQuorum: 2 of 3)"
    );
    let mut add4 = handle.membership();
    add4.insert(4, member_addr(4));
    let err = handle
        .propose(Command::ConfigChange {
            members: add4.clone(),
        })
        .await
        .expect_err("adding while a member is unreachable must be refused");
    assert!(
        matches!(err, ProposeError::InvalidConfigChange { .. }),
        "{err}"
    );

    // Heal: once the restarted member is HEARD again, the identical add is
    // accepted and commits with the three originals (3 of the new 4).
    // Convergence is visible through the restarted node's status a few
    // milliseconds before its first ack reaches the leader, so retry —
    // guard rejections are side-effect-free by construction.
    cluster.restart(down).await;
    converge_among(&cluster, &all).await;
    let accepted = loop {
        match handle
            .propose(Command::ConfigChange {
                members: add4.clone(),
            })
            .await
        {
            Ok(proposal) => break proposal,
            Err(ProposeError::InvalidConfigChange { .. }) => tokio::time::sleep(ms(10)).await,
            Err(other) => panic!("unexpected rejection after heal: {other}"),
        }
    };
    assert_eq!(accepted.committed.await, Ok(true));
    cluster.shutdown();
}

/// Removals are guarded by the same rule, and it cuts both ways: removing a
/// LIVE member while another is down would strand the survivors (new
/// majority 2 of {leader, dead node} — unreachable), so it is refused; but
/// removing the DEAD member — the recovery operation — passes, because the
/// new configuration {leader, live follower} is fully reachable.
#[tokio::test(start_paused = true)]
async fn removal_is_rejected_when_it_would_strand_the_survivors() {
    let cluster = spawn_cluster(3, 165, low_loss_faults());
    let all = cluster.all_ids();
    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "ready", 1).await;
    converge_among(&cluster, &all).await;

    let followers: Vec<NodeId> = all.iter().copied().filter(|&id| id != leader.id).collect();
    let (live, dead) = (followers[0], followers[1]);
    cluster.crash(dead);
    tokio::time::sleep(ms(400)).await;

    let handle = cluster.handle(leader.id);
    let mut remove_live = handle.membership();
    remove_live.remove(&live);
    let err = handle
        .propose(Command::ConfigChange {
            members: remove_live,
        })
        .await
        .expect_err("removing a live member while another is down must be refused");
    assert!(
        matches!(err, ProposeError::InvalidConfigChange { .. }),
        "{err}"
    );

    // The recovery path stays open: drop the dead member instead.
    commit_config(&cluster, &[leader.id, live], |m| {
        m.remove(&dead);
    })
    .await;
    confirm_put(&cluster, &[leader.id, live], "after", 2).await;
    cluster.shutdown();
}

// ---- joiner silence ----

#[tokio::test(start_paused = true)]
async fn joiner_never_campaigns_before_a_config_includes_it() {
    let cluster = spawn_cluster(3, 160, low_loss_faults());
    let originals = cluster.all_ids();
    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &originals, "k", 1).await;

    cluster.add_node(4);
    // Many election timeouts' worth of virtual time: the joiner must sit in
    // silence — Follower, term 0, empty log — and the cluster must not
    // notice it exists.
    tokio::time::sleep(ms(5000)).await;
    let joiner = cluster.handle(4).status();
    assert_eq!(joiner.role, RoleKind::Follower, "joiners never campaign");
    assert_eq!(joiner.term, 0, "a joiner's term never moves on its own");
    assert_eq!(joiner.last_log_index, 0);
    let unchanged = cluster.wait_for_leader_among(&originals).await;
    assert_eq!(unchanged.id, leader.id, "the joiner must not disrupt");
    assert_eq!(unchanged.term, leader.term);

    // Adopted: it catches up, and from then on counts — after the leader
    // crashes, the remaining three (joiner included) elect and commit.
    commit_config(&cluster, &originals, |m| {
        m.insert(4, member_addr(4));
    })
    .await;
    converge_among(&cluster, &cluster.all_ids()).await;
    cluster.crash(leader.id);
    let remaining: Vec<NodeId> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != leader.id)
        .collect();
    cluster.wait_for_leader_among(&remaining).await;
    confirm_put(&cluster, &remaining, "post-crash", 2).await;
    cluster.shutdown();
}

// ---- the removed-server disruption scenario (design decision, §4.2.3) ----

/// The named stickiness re-evaluation deferred from phases 11/12. A removed
/// follower is left RUNNING: it stops receiving heartbeats (leaders never
/// send outside the configuration), times out forever, and probes with
/// pre-votes. The members deny — first by leader stickiness, and, in the
/// stickiness-lapsed window after the leader crashes, by the §5.4.1 log
/// check: the committed removal entry makes every member's log strictly
/// longer than the removed server's, and a pre-vote majority in the removed
/// server's own (stale, 4-member) view needs 3 grants it can never get. So
/// it never reaches a REAL candidacy, and the cluster's terms move only for
/// the one legitimate election. This is the evidence for the PLAN.md
/// decision: real votes stay non-sticky; no lease/force-flag machinery.
#[tokio::test(start_paused = true)]
async fn removed_follower_cannot_disrupt_the_members() {
    let cluster = spawn_cluster(4, 161, low_loss_faults());
    let all = cluster.all_ids();
    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "before", 1).await;
    converge_among(&cluster, &all).await;

    let victim = *all.iter().find(|&&id| id != leader.id).unwrap();
    commit_config(&cluster, &all, |m| {
        m.remove(&victim);
    })
    .await;
    let members: Vec<NodeId> = all.iter().copied().filter(|&id| id != victim).collect();
    // A couple more committed entries widen the log gap the removed server
    // can never close.
    confirm_put(&cluster, &members, "after", 2).await;
    let term_before = cluster.wait_for_leader_among(&members).await.term;

    // Phase 1: leader alive. 5 virtual seconds of the removed server timing
    // out and probing; stickiness denies it, the members' term never moves,
    // and the victim never even reaches a candidacy (PreVote persists
    // nothing, so its own term is frozen too).
    for _ in 0..10 {
        tokio::time::sleep(ms(500)).await;
        assert_ne!(
            cluster.handle(victim).status().role,
            RoleKind::Leader,
            "the removed server must never lead"
        );
        for status in cluster.statuses_among(&members) {
            assert_eq!(
                status.term, term_before,
                "a removed server moved the members' term — disruption"
            );
        }
    }

    // Phase 2: the stickiness-lapsed window. Crash the leader; while the
    // two surviving members elect, the removed server is free to probe with
    // no live leader protecting anyone — the log check alone must hold the
    // line.
    cluster.crash(leader.id);
    let survivors: Vec<NodeId> = members
        .iter()
        .copied()
        .filter(|&id| id != leader.id)
        .collect();
    let successor = cluster.wait_for_leader_among(&survivors).await;
    assert!(
        survivors.contains(&successor.id),
        "only members may win elections"
    );
    assert_ne!(cluster.handle(victim).status().role, RoleKind::Leader);
    confirm_put(&cluster, &survivors, "final", 3).await;
    // One legitimate election moved the term; nothing else did.
    for status in cluster.statuses_among(&survivors) {
        assert_eq!(status.term, successor.term);
    }
    assert_ne!(cluster.handle(victim).status().role, RoleKind::Leader);
    cluster.shutdown();
}
