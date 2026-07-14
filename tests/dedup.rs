//! Phase 13 headline: exactly-once writes via client dedup tokens.
//!
//! The anomaly (lost update by resurrection): a write whose outcome was
//! ambiguous — the leader lost its majority before confirming — is retried
//! by the client after a leadership change. Both copies commit, and the
//! LATE duplicate's application silently clobbers a conflicting write that
//! another client had confirmed in between. Note that a naive "retry k=1
//! over k=1" schedule proves nothing in a last-writer-wins map: the
//! interleaved conflicting write is what makes the duplicate application
//! observable.
//!
//! The schedule (all deterministic, virtual time):
//! 1. Client A proposes k=1 on leader L; both followers' reply legs to L
//!    are severed immediately (the phase-12 trick): the entry REPLICATES
//!    to both followers but no ack ever lands, so it never commits on L.
//!    A's bounded wait expires — outcome Unknown.
//! 2. CheckQuorum (phase 12) deposes the deaf leader within
//!    ~election_timeout_max; a follower wins with the longer log and its
//!    no-op commits A's entry transitively. A's "failed" write is now
//!    applied, and A cannot know.
//! 3. Client B writes k=2 via the new leader — confirmed.
//! 4. A retries its write via the new leader — confirmed.
//!
//! `untokened_*` documents the anomaly surviving unchanged (token-less
//! writes keep today's at-least-once semantics); `tokened_*` is the
//! inversion: the retry's entry still COMMITS and occupies a log index
//! (dedup happens at apply, never at propose), but its application is
//! skipped by the state machine's sessions table — on every node, and
//! across a crash/restart (the table is rebuilt purely by log replay).

mod common;

use std::collections::HashMap;

use common::*;
use rustkv::raft::types::{Command, NodeId, Session};
use rustkv::store::SessionState;
use serde_json::json;

/// Waits until all nodes report the same last index, fully committed.
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

/// Drives the canonical ambiguous-retry schedule described in the module
/// docs, healing and converging at the end. `a_write` and `a_retry` are
/// client A's original write and its retry (both target key "k" with
/// value 1); what differs between the tests is whether they carry a token.
async fn run_ambiguous_retry(
    seed: u64,
    a_write: rustkv::raft::types::Command,
    a_retry: rustkv::raft::types::Command,
) -> TestCluster {
    let cluster = spawn_cluster(3, seed, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    let followers: Vec<NodeId> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != leader.id)
        .collect();

    // Client A's write. Severing the reply legs at the same virtual instant
    // as the propose means the AppendEntries requests still deliver (the
    // request direction is untouched) but every ack dies: replicated on
    // both followers, uncommitted on the leader.
    let pa = cluster.handle(leader.id).propose(a_write).await.unwrap();
    for &f in &followers {
        cluster.net.set_link_blocked(f, leader.id, true);
    }
    assert!(
        tokio::time::timeout(ms(1000), pa.committed).await.is_err(),
        "client A's outcome must be ambiguous (never confirmed on the deaf leader)"
    );

    // Within that wait, CheckQuorum deposed the deaf leader and a follower
    // won with the longer log; its no-op commits A's entry transitively.
    let new = cluster.wait_for_leader_among(&followers).await;
    assert!(
        new.term > leader.term,
        "a follower must have won a fresh election"
    );
    wait_until(
        "A's ambiguous write commits and applies transitively",
        || cluster.store(new.id).get("k").is_some(),
    )
    .await;

    // Client B's conflicting write, positively confirmed.
    let pb = cluster.handle(new.id).propose(put("k", 2)).await.unwrap();
    assert_eq!(
        tokio::time::timeout(ms(2000), pb.committed).await,
        Ok(Ok(true)),
        "client B's write must be confirmed"
    );

    // Client A retries its ambiguous write — also confirmed.
    let pr = cluster.handle(new.id).propose(a_retry).await.unwrap();
    assert_eq!(
        tokio::time::timeout(ms(2000), pr.committed).await,
        Ok(Ok(true)),
        "client A's retry must be confirmed"
    );

    for &f in &followers {
        cluster.net.set_link_blocked(f, leader.id, false);
    }
    converge(&cluster).await;
    cluster
}

/// Documented behavior, kept from phase 12: without a token the retry is a
/// second, independent write — it applies again, and B's confirmed k=2 is
/// silently destroyed by a write A believes FAILED. This is the at-least-
/// once semantics token-less clients keep.
#[tokio::test(start_paused = true)]
async fn untokened_ambiguous_retry_silently_destroys_a_confirmed_write() {
    let cluster = run_ambiguous_retry(71, put("k", 1), put("k", 1)).await;
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).get("k"),
            Some(json!(1)),
            "node {id}: the stale duplicate applied last, clobbering B's confirmed write"
        );
    }
    cluster.shutdown();
}

/// The phase-13 inversion: the identical schedule, but A's write and retry
/// carry the same token (client 1, seq 1). The retry still commits — the
/// log holds BOTH same-token entries — but its application is skipped, so
/// B's confirmed k=2 survives on every node, and the sessions table (also
/// state-machine state) is rebuilt purely by log replay after a restart.
#[tokio::test(start_paused = true)]
async fn tokened_retry_commits_twice_but_applies_once() {
    let cluster = run_ambiguous_retry(
        71,
        put_with_token("k", 1, 1, 1),
        put_with_token("k", 1, 1, 1),
    )
    .await;
    let expected_sessions = HashMap::from([(
        1u64,
        SessionState {
            max_seq: 1,
            recent: 0b1,
        },
    )]);
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).get("k"),
            Some(json!(2)),
            "node {id}: B's confirmed write must survive the stale duplicate"
        );
        assert_eq!(
            cluster.store(id).export().sessions,
            expected_sessions,
            "node {id}: client 1's highest applied seq is 1 (B was token-less)"
        );
    }

    // Crash/restart: the fresh state machine rebuilds map AND sessions by
    // replaying the log — including re-skipping the duplicate.
    let victim = cluster.all_ids()[0];
    cluster.crash(victim);
    cluster.restart(victim).await;
    wait_until("restarted node rebuilds its state from the log", || {
        cluster.store(victim).get("k") == Some(json!(2))
    })
    .await;
    assert_eq!(
        cluster.store(victim).export().sessions,
        expected_sessions,
        "sessions table must be rebuilt by replay alone"
    );

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    // The log really holds two entries with the same token: dedup happened
    // at apply, not at propose or commit.
    let same_token_entries = cluster
        .disk_log(victim)
        .iter()
        .filter(|e| {
            matches!(
                &e.command,
                Command::Put { key, session: Some(s), .. }
                    if key == "k" && *s == Session { client: 1, seq: 1 }
            )
        })
        .count();
    assert_eq!(
        same_token_entries, 2,
        "both the ambiguous original and the retry must occupy log indexes"
    );
}

/// A client that pipelines two INDEPENDENT ops with their own seqs (the
/// natural reading of the token headers as per-op idempotency keys) must
/// not lose one to arrival order. With the original client→max-seq table
/// this was a silent linearizability violation: if seq 6 applied first,
/// seq 5 was skipped as a "duplicate" yet still acked — a 201 for a write
/// that never happened and never will. The windowed exact-match table
/// applies both (concurrent ops from one client may linearize in either
/// order); only a true retry of an already-applied seq is skipped.
#[tokio::test(start_paused = true)]
async fn pipelined_ops_from_one_client_both_apply_regardless_of_order() {
    let cluster = spawn_cluster(3, 83, low_loss_faults());
    let leader = cluster.wait_for_leader().await;

    // Log order 6-then-5, forced by awaiting the first commit — the
    // arrival order a pipelining client cannot control.
    let p6 = cluster
        .handle(leader.id)
        .propose(put_with_token("b", 6, 9, 6))
        .await
        .unwrap();
    assert_eq!(p6.committed.await, Ok(true));
    let p5 = cluster
        .handle(leader.id)
        .propose(put_with_token("a", 5, 9, 5))
        .await
        .unwrap();
    assert_eq!(p5.committed.await, Ok(true), "op 5 is positively acked");

    // The ack must be honest: both writes really applied.
    wait_until("both pipelined writes apply everywhere", || {
        cluster.all_ids().iter().all(|&id| {
            cluster.store(id).get("a") == Some(json!(5))
                && cluster.store(id).get("b") == Some(json!(6))
        })
    })
    .await;

    // And dedup still works for genuine retries of either seq.
    let retry = cluster
        .handle(leader.id)
        .propose(put_with_token("a", 99, 9, 5))
        .await
        .unwrap();
    assert_eq!(retry.committed.await, Ok(true));
    let p7 = cluster
        .handle(leader.id)
        .propose(put_with_token("c", 7, 9, 7))
        .await
        .unwrap();
    assert_eq!(p7.committed.await, Ok(true));
    wait_until("post-retry write applies", || {
        cluster.store(leader.id).get("c") == Some(json!(7))
    })
    .await;
    assert_eq!(
        cluster.store(leader.id).get("a"),
        Some(json!(5)),
        "the retried seq 5 must be skipped, not re-applied"
    );

    cluster.shutdown();
}
