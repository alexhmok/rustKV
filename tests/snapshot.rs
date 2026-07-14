//! Phase 14: snapshotting / log compaction + InstallSnapshot, on the
//! deterministic simulator (seeded, virtual time).
//!
//! Everything here opts in via `snapshot_threshold` — the feature is off by
//! default everywhere else, which is what keeps every pre-phase-14 seeded
//! schedule pinned. Scenarios:
//! - the applied-count trigger compacts every node, and the boundary +
//!   retained log on disk stay mutually consistent;
//! - a single node restarts from its own snapshot (restore-at-boot + replay
//!   of the retained tail);
//! - the money test: a crashed node whose log the survivors compacted PAST
//!   catches up via InstallSnapshot — its log never contains the compacted
//!   prefix, proving the snapshot path ran rather than AppendEntries
//!   backfill (which no longer has the entries to send);
//! - full request duplication: duplicated InstallSnapshots hit the
//!   follower's no-op guard and are harmless (phase 10's standing fault);
//! - the phase-13 cross-check, both paths: a tokened write whose
//!   application was compacted away still dedups its retry after (a) a
//!   restart from the node's own snapshot and (b) catch-up via
//!   InstallSnapshot — the sessions table rides the snapshot payload.

mod common;

use common::*;
use rustkv::raft::types::{LogIndex, NodeId};
use serde_json::json;

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

/// Asserts every node's full state (map + dedup sessions) is identical.
fn assert_states_identical(cluster: &TestCluster) {
    let reference = cluster.store(1).export();
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).export(),
            reference,
            "node {id}: state machine diverges"
        );
    }
}

// ---- the applied-count trigger ----

#[tokio::test(start_paused = true)]
async fn threshold_trigger_compacts_every_node() {
    let cluster = spawn_cluster_with_threshold(3, 71, low_loss_faults(), Some(8));
    for i in 1..=20u64 {
        confirm_put(&cluster, &cluster.all_ids(), &format!("k{i}"), i).await;
    }
    converge_among(&cluster, &cluster.all_ids()).await;
    assert_states_identical(&cluster);
    for i in 1..=20u64 {
        assert_eq!(cluster.store(1).get(&format!("k{i}")), Some(json!(i)));
    }

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    for id in cluster.all_ids() {
        let snapshot = cluster
            .disk_snapshot(id)
            .unwrap_or_else(|| panic!("node {id}: never compacted"));
        assert!(
            snapshot.last_included_index >= 8,
            "node {id}: boundary {} below the threshold's first trigger",
            snapshot.last_included_index
        );
        assert_eq!(snapshot.membership, None, "reserved until phase 15");
        let log = cluster.disk_log(id);
        // The retained log continues exactly at the boundary; the compacted
        // prefix is gone from disk.
        if let Some(first) = log.first() {
            assert_eq!(
                first.index,
                snapshot.last_included_index + 1,
                "node {id}: retained log must continue from its boundary"
            );
        }
    }
}

// ---- restart from one's own snapshot ----

#[tokio::test(start_paused = true)]
async fn single_node_restarts_from_its_own_snapshot() {
    let cluster = spawn_cluster_with_threshold(1, 72, low_loss_faults(), Some(4));
    for i in 1..=10u64 {
        confirm_put(&cluster, &[1], &format!("k{i}"), i).await;
    }
    cluster.crash(1);
    cluster.restart(1).await;
    // The reborn node re-elects itself; its no-op commit re-commits (and
    // re-applies) the retained tail on top of the restored snapshot.
    cluster.wait_for_leader().await;
    wait_until("state rebuilt from snapshot + retained log", || {
        (1..=10u64).all(|i| cluster.store(1).get(&format!("k{i}")) == Some(json!(i)))
    })
    .await;
    // And it keeps working (including further compaction) after the restore.
    confirm_put(&cluster, &[1], "k11", 11).await;
    assert_eq!(cluster.store(1).get("k11"), Some(json!(11)));

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let snapshot = cluster.disk_snapshot(1).expect("compacted");
    let log = cluster.disk_log(1);
    assert!(snapshot.last_included_index >= 4);
    if let Some(first) = log.first() {
        assert_eq!(first.index, snapshot.last_included_index + 1);
    }
}

// ---- the money test: catch-up via InstallSnapshot ----

/// Crash a follower, commit and compact PAST its entire log on the
/// survivors, restart it: it must converge, and its log must NEVER contain
/// the compacted prefix — AppendEntries backfill is impossible (the leader
/// no longer has those entries), so convergence proves InstallSnapshot ran.
async fn run_install_snapshot_catch_up(seed: u64, duplicate_probability: f64) {
    let mut faults = low_loss_faults();
    faults.duplicate_probability = duplicate_probability;
    let cluster = spawn_cluster_with_threshold(3, seed, faults, Some(8));
    let all = cluster.all_ids();

    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "k1", 1).await;
    confirm_put(&cluster, &all, "k2", 2).await;
    converge_among(&cluster, &all).await;

    let victim = *all.iter().find(|&&id| id != leader.id).unwrap();
    let victim_last = cluster.handle(victim).status().last_log_index;
    cluster.crash(victim);

    let survivors: Vec<NodeId> = all.iter().copied().filter(|&id| id != victim).collect();
    for i in 3..=20u64 {
        confirm_put(&cluster, &survivors, &format!("k{i}"), i).await;
    }
    converge_among(&cluster, &survivors).await;

    cluster.restart(victim).await;
    converge_among(&cluster, &all).await;
    assert_states_identical(&cluster);
    for i in 1..=20u64 {
        assert_eq!(
            cluster.store(victim).get(&format!("k{i}")),
            Some(json!(i)),
            "seed {seed}: k{i} missing on the caught-up node"
        );
    }

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    // Construction guard: the survivors really compacted past the victim's
    // entire log, so entries <= victim_last were unsendable.
    for &id in &survivors {
        let boundary = cluster
            .disk_snapshot(id)
            .expect("survivors compacted")
            .last_included_index;
        assert!(
            boundary > victim_last,
            "seed {seed}: survivor {id} boundary {boundary} not past the \
             victim's log ({victim_last}) — the scenario lost its teeth"
        );
    }
    // The proof: the victim holds a snapshot, and nothing in its log — its
    // own stale prefix included — is at or below its crash-time last index.
    let snapshot = cluster
        .disk_snapshot(victim)
        .expect("the victim caught up via a snapshot");
    assert!(snapshot.last_included_index > victim_last);
    let log = cluster.disk_log(victim);
    assert!(
        log.iter().all(|e| e.index > victim_last),
        "seed {seed}: the victim's log contains compacted-prefix entries — \
         something backfilled what only a snapshot should carry"
    );
    if let Some(first) = log.first() {
        assert_eq!(first.index, snapshot.last_included_index + 1);
    }
}

#[tokio::test(start_paused = true)]
async fn lagging_node_catches_up_via_install_snapshot() {
    for seed in [73, 74, 75] {
        run_install_snapshot_catch_up(seed, 0.0).await;
    }
}

/// Every request delivered twice: the duplicated InstallSnapshots hit the
/// follower's `last_included_index <= commit_index` no-op guard (and the
/// sim safety observer, asserted at shutdown, stays clean).
#[tokio::test(start_paused = true)]
async fn duplicated_install_snapshots_are_harmless() {
    run_install_snapshot_catch_up(76, 1.0).await;
}

// ---- the phase-13 cross-check: sessions ride the snapshot ----

/// The shared schedule: a tokened write, an interleaved conflicting write
/// under a different client, then enough filler that every live node
/// compacts both AWAY. Returns the log index the original tokened write
/// occupied. `k`'s final value must be 2 forever after — a retry of the
/// tokened `k=1` must dedup even though its application now lives only in
/// snapshots.
async fn tokened_then_conflicting_then_filler(cluster: &TestCluster, ids: &[NodeId]) -> LogIndex {
    let leader = cluster.wait_for_leader_among(ids).await;
    let original = cluster
        .handle(leader.id)
        .propose(put_with_token("k", 1, 7, 1))
        .await
        .expect("tokened write accepted");
    assert_eq!(original.committed.await, Ok(true));
    let proposal = cluster
        .handle(leader.id)
        .propose(put_with_token("k", 2, 8, 1))
        .await
        .expect("conflicting write accepted");
    assert_eq!(proposal.committed.await, Ok(true));
    for i in 1..=12u64 {
        confirm_put(cluster, ids, &format!("f{i}"), i).await;
    }
    original.index
}

/// Retries the tokened write and asserts the retry COMMITS (dedup is at
/// apply, never at propose) yet mutates nothing anywhere: `k` stays 2.
async fn retry_must_dedup_everywhere(cluster: &TestCluster, context: &str) {
    let all = cluster.all_ids();
    let leader = cluster.wait_for_leader_among(&all).await;
    let retry = cluster
        .handle(leader.id)
        .propose(put_with_token("k", 1, 7, 1))
        .await
        .expect("retry accepted");
    assert_eq!(retry.committed.await, Ok(true), "{context}: retry commits");
    converge_among(cluster, &all).await;
    assert_states_identical(cluster);
    for id in &all {
        assert_eq!(
            cluster.store(*id).get("k"),
            Some(json!(2)),
            "{context}: node {id} applied the retry — the dedup session \
             was lost across the snapshot"
        );
    }
}

/// Path (a): a node restarts from its OWN snapshot.json; the sessions table
/// must come back with it (the retained log no longer holds the original).
#[tokio::test(start_paused = true)]
async fn dedup_survives_restart_from_own_snapshot() {
    let cluster = spawn_cluster_with_threshold(3, 77, low_loss_faults(), Some(8));
    let all = cluster.all_ids();
    let original_index = tokened_then_conflicting_then_filler(&cluster, &all).await;
    converge_among(&cluster, &all).await;

    let leader = cluster.wait_for_leader().await;
    let victim = *all.iter().find(|&&id| id != leader.id).unwrap();
    cluster.crash(victim);
    cluster.restart(victim).await;
    converge_among(&cluster, &all).await;

    retry_must_dedup_everywhere(&cluster, "restart-from-own-snapshot").await;

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    // The restarted node could only have known (7, 1) applied through its
    // snapshot: the original entry is not in its retained log.
    let boundary = cluster
        .disk_snapshot(victim)
        .expect("victim compacted before the crash")
        .last_included_index;
    assert!(
        boundary >= original_index,
        "the original tokened write was never compacted — vacuous test"
    );
    assert!(
        cluster
            .disk_log(victim)
            .iter()
            .all(|e| e.index != original_index),
        "original entry still in the retained log — vacuous test"
    );
}

/// Path (b): a node that never saw the tokened write catches up via
/// InstallSnapshot; the sessions table must arrive in the payload.
#[tokio::test(start_paused = true)]
async fn dedup_survives_catch_up_via_install_snapshot() {
    let cluster = spawn_cluster_with_threshold(3, 78, low_loss_faults(), Some(8));
    let all = cluster.all_ids();

    let leader = cluster.wait_for_leader().await;
    let victim = *all.iter().find(|&&id| id != leader.id).unwrap();
    let victim_last = cluster.handle(victim).status().last_log_index;
    cluster.crash(victim);

    let survivors: Vec<NodeId> = all.iter().copied().filter(|&id| id != victim).collect();
    let original_index = tokened_then_conflicting_then_filler(&cluster, &survivors).await;
    converge_among(&cluster, &survivors).await;

    cluster.restart(victim).await;
    converge_among(&cluster, &all).await;

    retry_must_dedup_everywhere(&cluster, "catch-up-via-install-snapshot").await;

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    // The victim never held the original entry at all: everything at or
    // below its crash-time log was replaced by the snapshot.
    let log = cluster.disk_log(victim);
    assert!(log.iter().all(|e| e.index > victim_last));
    assert!(
        log.iter().all(|e| e.index != original_index),
        "original entry reached the victim as an entry — vacuous test"
    );
    assert!(
        cluster
            .disk_snapshot(victim)
            .expect("snapshot installed")
            .last_included_index
            >= original_index
    );
}
