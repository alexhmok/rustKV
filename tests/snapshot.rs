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

// ---- the dropped-sender window ----

/// Phase-14 amendment: a pending proposal whose entry is swallowed by an
/// INSTALLED snapshot resolves as `Err` (channel dropped — outcome unknown),
/// never `Ok(false)` and never `Ok(true)`. The terms that could have decided
/// committed-vs-replaced were compacted away cluster-wide, so any definite
/// answer would be a guess — and this schedule constructs the case where
/// `Ok(false)` ("definitely didn't happen, safe to retry") would be a lie:
/// the entry IS in the committed history and lands in the final state.
///
/// Schedule (the phase-12 severed-ack trick, as in tests/dedup.rs): the
/// proposal replicates to both followers but every ack dies, so the leader
/// never learns the outcome; CheckQuorum deposes it (pending deliberately
/// survives step-down, phase 5); the successor commits the entry
/// transitively, then commits and compacts far past it; on heal the old
/// leader is caught up via InstallSnapshot, which is the moment the pending
/// proposal's evidence disappears.
#[tokio::test(start_paused = true)]
async fn proposal_swallowed_by_installed_snapshot_resolves_ambiguous_not_false() {
    let cluster = spawn_cluster_with_threshold(3, 79, low_loss_faults(), Some(8));
    let all = cluster.all_ids();

    let leader = cluster.wait_for_leader().await;
    confirm_put(&cluster, &all, "v0", 0).await;
    converge_among(&cluster, &all).await;

    // Sever ONLY the follower→leader legs: AppendEntries still reach the
    // followers, every reply (and every follower-originated request) dies.
    let followers: Vec<NodeId> = all.iter().copied().filter(|&id| id != leader.id).collect();
    for &f in &followers {
        cluster.net.set_link_blocked(f, leader.id, true);
    }

    // The ambiguous proposal: durably appended on the leader, replicated to
    // both followers, never acked — its commit outcome is unknowable here.
    let mut pending = cluster
        .handle(leader.id)
        .propose(put("v1", 1))
        .await
        .expect("still leader inside the check-quorum window");
    let pending_index = pending.index;

    // CheckQuorum deposes the deaf leader; the followers elect a successor
    // whose no-op commits the entry transitively (case A: it DID happen).
    let successor = cluster.wait_for_leader_among(&followers).await;
    assert!(successor.term > leader.term);

    // While the old leader is dark, the proposal must stay unresolved...
    let still_pending = tokio::time::timeout(ms(2000), &mut pending.committed).await;
    assert!(
        still_pending.is_err(),
        "nothing may resolve the proposal while its node is cut off"
    );

    // ...and the survivors commit + compact PAST it (threshold 8, 12 more
    // writes), destroying the term evidence everywhere that has it.
    for i in 1..=12u64 {
        confirm_put(&cluster, &followers, &format!("f{i}"), i).await;
    }
    converge_among(&cluster, &followers).await;

    for &f in &followers {
        cluster.net.set_link_blocked(f, leader.id, false);
    }

    // On heal the successor's next_index for the old leader sits at or below
    // its snapshot boundary, so catch-up arrives as InstallSnapshot — and the
    // pending proposal's sender is dropped THEN, not resolved with a guess.
    let outcome = tokio::time::timeout(ms(3000), &mut pending.committed).await;
    match outcome {
        Ok(Err(_dropped)) => {} // ambiguous — the only honest answer
        Ok(Ok(false)) => panic!(
            "got the definite 'never committed' — a lie: the entry is in the \
             committed history (asserted below)"
        ),
        Ok(Ok(true)) => panic!("got a definite ack the node cannot justify"),
        Err(_) => panic!("proposal still unresolved 3s after heal — the drop never fired"),
    }

    // Case A made concrete: the write IS in the final state on every node,
    // including the deposed leader (delivered inside the snapshot payload).
    converge_among(&cluster, &all).await;
    wait_until("swallowed write visible on the deposed leader", || {
        cluster.store(leader.id).get("v1") == Some(json!(1))
    })
    .await;
    assert_states_identical(&cluster);

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    // The window was real: the entry itself is gone from the deposed
    // leader's disk — it arrived folded into a snapshot, not as an entry.
    let snapshot = cluster
        .disk_snapshot(leader.id)
        .expect("the deposed leader was caught up via InstallSnapshot");
    assert!(snapshot.last_included_index >= pending_index);
    assert!(
        cluster
            .disk_log(leader.id)
            .iter()
            .all(|e| e.index != pending_index),
        "the entry survived as an entry — the ambiguity window was never entered"
    );
}

// ---- the trailing window (phase-14 amendment) ----

/// With `snapshot_trailing` set, the boundary lags `last_applied` by at
/// least the window: the log always retains that many applied entries, and
/// a crash/restart on top of the lagging boundary replays the longer tail
/// correctly.
#[tokio::test(start_paused = true)]
async fn trailing_window_keeps_the_boundary_behind_the_applied_index() {
    let cluster = spawn_cluster_with_trailing(1, 81, low_loss_faults(), Some(4), 8);
    for i in 1..=30u64 {
        confirm_put(&cluster, &[1], &format!("k{i}"), i).await;
    }
    cluster.crash(1);
    cluster.restart(1).await;
    cluster.wait_for_leader().await;
    wait_until("state rebuilt from lagging snapshot + long tail", || {
        (1..=30u64).all(|i| cluster.store(1).get(&format!("k{i}")) == Some(json!(i)))
    })
    .await;

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let boundary = cluster
        .disk_snapshot(1)
        .expect("compacted repeatedly")
        .last_included_index;
    let log = cluster.disk_log(1);
    let last = log.last().expect("retained tail is nonempty").index;
    assert!(boundary >= 1);
    assert!(
        last - boundary >= 8,
        "boundary {boundary} closer than the trailing window to the tail {last}"
    );
    assert_eq!(log.first().unwrap().index, boundary + 1);
}

/// The payoff: a peer that falls behind by LESS than the trailing window
/// catches up through ordinary AppendEntries even though the leader has
/// compacted — the entries it needs were deliberately retained. (Contrast
/// with `lagging_node_catches_up_via_install_snapshot`, where trailing = 0
/// makes the same catch-up impossible without a snapshot.)
#[tokio::test(start_paused = true)]
async fn slow_live_peer_catches_up_via_entries_inside_the_trailing_window() {
    let cluster = spawn_cluster_with_trailing(3, 82, low_loss_faults(), Some(2), 16);
    let all = cluster.all_ids();

    let leader = cluster.wait_for_leader().await;
    for i in 1..=4u64 {
        confirm_put(&cluster, &all, &format!("k{i}"), i).await;
    }
    converge_among(&cluster, &all).await;

    // Isolate a follower (live, not crashed) and commit past it — but by
    // less than the trailing window, so its entries stay retained.
    let victim = *all.iter().find(|&&id| id != leader.id).unwrap();
    let victim_last = cluster.handle(victim).status().last_log_index;
    for &other in all.iter().filter(|&&id| id != victim) {
        cluster.net.set_pair_blocked(victim, other, true);
    }
    let survivors: Vec<NodeId> = all.iter().copied().filter(|&id| id != victim).collect();
    for i in 5..=22u64 {
        confirm_put(&cluster, &survivors, &format!("k{i}"), i).await;
    }
    converge_among(&cluster, &survivors).await;

    for &other in all.iter().filter(|&&id| id != victim) {
        cluster.net.set_pair_blocked(victim, other, false);
    }
    converge_among(&cluster, &all).await;
    assert_states_identical(&cluster);

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let final_last = cluster.disk_log(leader.id).last().unwrap().index;
    // Construction guard: the survivors DID compact while the victim was
    // cut off — but only to a boundary at or below the victim's log, which
    // is exactly what the trailing window is for.
    for &id in &survivors {
        let boundary = cluster
            .disk_snapshot(id)
            .expect("survivors compacted during the isolation")
            .last_included_index;
        assert!(
            boundary >= 1,
            "node {id} never compacted — vacuous scenario"
        );
        assert!(
            boundary <= victim_last,
            "node {id} compacted past the victim's log ({boundary} > \
             {victim_last}) — the window failed and this test proves nothing"
        );
        assert!(
            final_last - boundary >= 16,
            "node {id}: trailing guarantee violated"
        );
    }
    // The proof: every entry the victim missed reached it AS AN ENTRY — its
    // retained log holds the full isolation-window range, which a snapshot
    // catch-up would have folded away instead.
    let victim_log = cluster.disk_log(victim);
    for index in victim_last + 1..=final_last {
        assert!(
            victim_log.iter().any(|e| e.index == index),
            "index {index} missing from the victim's log — it must have \
             arrived via InstallSnapshot, not AppendEntries"
        );
    }
    if let Some(snapshot) = cluster.disk_snapshot(victim) {
        assert!(
            snapshot.last_included_index <= victim_last,
            "the victim's boundary covers entries it never held as entries"
        );
    }
}
