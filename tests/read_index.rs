//! Linearizable reads via ReadIndex (§6.4), on the deterministic simulator.
//!
//! Covered: single-node immediate grants; grants reflecting committed
//! writes; the NotLeader rejection with a hint; the §6.4 no-op gate (a
//! fresh leader grants nothing until its term-start no-op commits and
//! applies); and the money test — a minority-partitioned leader whose state
//! is provably stale can never confirm a read, and stepping down on heal
//! resolves the hung ticket with an error instead of a stale value.

mod common;

use std::time::Duration;

use common::{low_loss_faults, ms, put, spawn_cluster, wait_until};
use rustkv::raft::node::ProposeError;
use rustkv::raft::transport::sim::FaultConfig;
use serde_json::json;

/// Proposes on `leader`, requires it to commit.
async fn confirmed_write(cluster: &common::TestCluster, leader: u64, key: &str, value: u64) {
    let proposal = cluster
        .handle(leader)
        .propose(put(key, value))
        .await
        .expect("leader accepts proposal");
    let committed = tokio::time::timeout(Duration::from_secs(5), proposal.committed)
        .await
        .expect("commit within 5s virtual")
        .expect("node alive");
    assert!(committed, "write must commit");
}

#[tokio::test(start_paused = true)]
async fn single_node_grants_reads_immediately() {
    let cluster = spawn_cluster(1, 71, low_loss_faults());
    let leader = cluster.wait_for_leader().await.id;
    confirmed_write(&cluster, leader, "solo", 1).await;

    let ticket = cluster.handle(leader).read().await.expect("leader accepts");
    // Its own majority: granted without any network round-trip.
    tokio::time::timeout(ms(1), ticket.granted)
        .await
        .expect("single-node read grants immediately")
        .expect("ticket resolves");
    assert_eq!(cluster.store(leader).get("solo"), Some(json!(1)));
    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn leader_grants_reads_that_reflect_committed_writes() {
    let cluster = spawn_cluster(3, 72, low_loss_faults());
    let leader = cluster.wait_for_leader().await.id;
    confirmed_write(&cluster, leader, "k", 7).await;

    let ticket = cluster.handle(leader).read().await.expect("leader accepts");
    tokio::time::timeout(Duration::from_secs(2), ticket.granted)
        .await
        .expect("read confirmed within 2s virtual")
        .expect("ticket resolves");
    // The proposal already resolved as applied-locally, so the granted read
    // must see the committed value.
    assert_eq!(cluster.store(leader).get("k"), Some(json!(7)));
    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn followers_reject_reads_with_a_leader_hint() {
    let cluster = spawn_cluster(3, 73, low_loss_faults());
    let leader = cluster.wait_for_leader().await.id;
    // Make sure every follower has heard from the leader (knows the hint).
    wait_until("all nodes know the leader", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.leader_id == Some(leader))
    })
    .await;

    for id in cluster.all_ids() {
        if id == leader {
            continue;
        }
        let err = cluster
            .handle(id)
            .read()
            .await
            .expect_err("followers must not accept reads");
        assert_eq!(
            err,
            ProposeError::NotLeader {
                leader_hint: Some(leader)
            },
            "node {id}"
        );
    }
    cluster.shutdown();
}

/// §6.4: a fresh leader must not grant reads before its term-start no-op
/// commits and applies — it doesn't yet know how far its predecessor
/// committed. Slow links stretch the window between "became leader" and
/// "no-op committed" so the gate is observable.
#[tokio::test(start_paused = true)]
async fn reads_wait_for_the_term_start_noop() {
    let faults = FaultConfig {
        min_delay: ms(20),
        max_delay: ms(40),
        drop_probability: 0.0,
        rpc_timeout: ms(200),
    };
    let cluster = spawn_cluster(3, 74, faults);

    // Catch the leader inside the window: role is Leader (status publishes
    // on the transition) but the no-op (its last_log_index) is uncommitted —
    // commit needs a >=40ms round trip while we poll every 5ms.
    let mut leader = None;
    wait_until("a leader whose no-op is not yet committed", || {
        for s in cluster.statuses_among(&cluster.all_ids()) {
            if s.role == rustkv::raft::node::RoleKind::Leader && s.commit_index < s.last_log_index {
                leader = Some(s);
                return true;
            }
        }
        false
    })
    .await;
    let leader = leader.unwrap();

    let mut ticket = cluster
        .handle(leader.id)
        .read()
        .await
        .expect("leader accepts");
    // Not granted while the no-op is uncommitted...
    tokio::time::timeout(ms(1), &mut ticket.granted)
        .await
        .expect_err("read must not be granted before the term-start no-op commits");
    // ...granted once it commits and applies.
    tokio::time::timeout(Duration::from_secs(2), ticket.granted)
        .await
        .expect("read confirmed within 2s virtual")
        .expect("ticket resolves");
    let status = cluster.handle(leader.id).status();
    assert!(
        status.commit_index >= leader.last_log_index,
        "grant implies the no-op committed"
    );
    cluster.shutdown();
}

/// The money test: a leader partitioned into a minority holds a provably
/// stale value. Its linearizable read must hang (it cannot confirm
/// leadership), never serving the stale state — and healing resolves the
/// ticket with an error (step-down), not a grant.
#[tokio::test(start_paused = true)]
async fn partitioned_leader_never_serves_a_stale_read() {
    let cluster = spawn_cluster(3, 75, low_loss_faults());
    let old_leader = cluster.wait_for_leader().await.id;
    confirmed_write(&cluster, old_leader, "k", 1).await;

    // Cut the leader off from both followers.
    let followers: Vec<u64> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != old_leader)
        .collect();
    for &f in &followers {
        cluster.net.set_pair_blocked(old_leader, f, true);
    }

    // The majority elects a successor and commits a newer value.
    let new_leader = cluster.wait_for_leader_among(&followers).await.id;
    confirmed_write(&cluster, new_leader, "k", 2).await;

    // The deposed-but-unaware leader still calls itself leader and still
    // holds k=1. It accepts the read (it can't know better) but must never
    // confirm it.
    assert_eq!(
        cluster.handle(old_leader).status().role,
        rustkv::raft::node::RoleKind::Leader,
        "old leader must not have learned of its deposition yet"
    );
    assert_eq!(cluster.store(old_leader).get("k"), Some(json!(1)));
    let mut ticket = cluster
        .handle(old_leader)
        .read()
        .await
        .expect("still believes it leads");
    tokio::time::timeout(Duration::from_secs(3), &mut ticket.granted)
        .await
        .expect_err("a minority leader must never confirm a linearizable read");

    // Heal: the old leader sees the higher term, steps down, and the pending
    // ticket resolves with an error — promptly, and never with a grant.
    for &f in &followers {
        cluster.net.set_pair_blocked(old_leader, f, false);
    }
    tokio::time::timeout(Duration::from_secs(5), ticket.granted)
        .await
        .expect("step-down must resolve the ticket promptly")
        .expect_err("ticket must resolve as an error, not a grant");

    // A retry against the new leader sees the new value.
    let ticket = cluster
        .handle(new_leader)
        .read()
        .await
        .expect("new leader accepts");
    tokio::time::timeout(Duration::from_secs(2), ticket.granted)
        .await
        .expect("read confirmed")
        .expect("ticket resolves");
    assert_eq!(cluster.store(new_leader).get("k"), Some(json!(2)));
    cluster.shutdown();
}
