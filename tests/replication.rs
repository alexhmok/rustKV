//! Log-replication tests (§5.3–§5.4) on the simulated transport with virtual
//! time: deterministic per seed.
//!
//! Covered: propose→majority commit→all-nodes convergence with identical
//! on-disk logs; proposal rejection on non-leaders (with leader hint);
//! lagging-follower catch-up after isolation; conflicting uncommitted
//! entries truncated and replaced (Figure 7-style divergence, exercising
//! leader backtracking); a minority-partitioned leader accepting but never
//! committing (CP behavior); seed determinism of full replication outcomes;
//! confirmed writes surviving sustained 15% message loss; and RPC-level
//! AppendEntries conformance (idempotency, commit capping, gap rejection,
//! stale terms, conflict truncation).
//! NOT covered here: applying commits to the KV map (phase 5), crashes
//! mid-replication and deeper invariant checking (phase 6), message
//! duplication by the transport (the AE handler is duplicate-tolerant and
//! that path is exercised by the idempotency test, but the simulator never
//! duplicates in-flight messages).

mod common;

use common::*;
use rustkv::raft::Storage;
use rustkv::raft::node::{ProposeError, RaftNode, RoleKind};
use rustkv::raft::rpc::{AppendEntriesArgs, AppendEntriesReply, RpcRequest, RpcResponse};
use rustkv::raft::transport::Transport;
use rustkv::raft::transport::sim::{FaultConfig, SimNetwork, SimTransport};
use rustkv::raft::types::{LogEntry, LogIndex, NodeId, Term};

// ---- happy path ----

#[tokio::test(start_paused = true)]
async fn leader_replicates_and_commits_proposals() {
    let cluster = spawn_cluster(3, 21, low_loss_faults());
    let leader = cluster.wait_for_leader().await;

    for i in 1..=3u64 {
        let (term, index) = cluster
            .handle(leader.id)
            .propose(put(&format!("k{i}"), i))
            .await
            .expect("leader accepts proposals");
        assert_eq!(term, leader.term);
        assert_eq!(index, i, "indexes assigned sequentially");
    }

    wait_until("all nodes commit index 3", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 3 && s.last_log_index == 3)
    })
    .await;

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let reference = cluster.disk_log(leader.id);
    assert_eq!(reference.len(), 3);
    for (i, e) in reference.iter().enumerate() {
        let n = i as u64 + 1;
        assert_eq!(e.index, n);
        assert_eq!(e.term, leader.term);
        assert_eq!(e.command, put(&format!("k{n}"), n));
    }
    for id in cluster.all_ids() {
        assert_eq!(cluster.disk_log(id), reference, "node {id}: identical log");
    }
}

#[tokio::test(start_paused = true)]
async fn non_leader_rejects_proposals_with_leader_hint() {
    let cluster = spawn_cluster(3, 22, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    tokio::time::sleep(ms(200)).await; // followers learn the leader

    let follower = cluster
        .all_ids()
        .into_iter()
        .find(|&id| id != leader.id)
        .unwrap();
    let err = cluster
        .handle(follower)
        .propose(put("x", 1))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        ProposeError::NotLeader {
            leader_hint: Some(leader.id)
        }
    );
    cluster.shutdown();
}

// ---- catch-up and divergence ----

#[tokio::test(start_paused = true)]
async fn lagging_follower_catches_up_after_heal() {
    let cluster = spawn_cluster(3, 23, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    let follower = cluster
        .all_ids()
        .into_iter()
        .find(|&id| id != leader.id)
        .unwrap();
    let others: Vec<NodeId> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != follower)
        .collect();

    for i in 1..=2u64 {
        cluster
            .handle(leader.id)
            .propose(put(&format!("k{i}"), i))
            .await
            .unwrap();
    }
    wait_until("all nodes commit index 2", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 2)
    })
    .await;

    for &id in &others {
        cluster.net.set_pair_blocked(follower, id, true);
    }
    for i in 3..=5u64 {
        cluster
            .handle(leader.id)
            .propose(put(&format!("k{i}"), i))
            .await
            .unwrap();
    }
    wait_until("majority commits index 5", || {
        cluster
            .statuses_among(&others)
            .iter()
            .all(|s| s.commit_index == 5)
    })
    .await;
    let lagging = cluster.handle(follower).status();
    assert_eq!(lagging.commit_index, 2, "isolated node saw nothing new");
    assert_eq!(lagging.last_log_index, 2);

    for &id in &others {
        cluster.net.set_pair_blocked(follower, id, false);
    }
    // Reintegration may involve a re-election (the isolated node churned its
    // term up), but the committed entries must reach it regardless.
    wait_until("rejoined follower commits index 5", || {
        let s = cluster.handle(follower).status();
        s.commit_index == 5 && s.last_log_index == 5
    })
    .await;

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let reference = cluster.disk_log(leader.id);
    assert_eq!(reference.len(), 5);
    for id in cluster.all_ids() {
        assert_eq!(cluster.disk_log(id), reference, "node {id}: identical log");
    }
}

#[tokio::test(start_paused = true)]
async fn conflicting_uncommitted_entries_are_truncated_and_replaced() {
    let cluster = spawn_cluster(3, 24, low_loss_faults());
    let old_leader = cluster.wait_for_leader().await;
    let others: Vec<NodeId> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != old_leader.id)
        .collect();

    // A committed base entry everyone shares.
    cluster
        .handle(old_leader.id)
        .propose(put("base", 0))
        .await
        .unwrap();
    wait_until("all nodes commit the base entry", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 1)
    })
    .await;

    // Partition the leader into the minority; it happily appends two
    // proposals (indexes 2 and 3 in its old term) that can never commit.
    for &id in &others {
        cluster.net.set_pair_blocked(old_leader.id, id, true);
    }
    let (orphan_term, i) = cluster
        .handle(old_leader.id)
        .propose(put("orphan-a", 1))
        .await
        .unwrap();
    assert_eq!(i, 2);
    cluster
        .handle(old_leader.id)
        .propose(put("orphan-b", 2))
        .await
        .unwrap();
    assert_eq!(orphan_term, old_leader.term);

    // The majority elects a new leader and commits DIFFERENT entries at the
    // same indexes.
    let new_leader = cluster.wait_for_leader_among(&others).await;
    assert!(new_leader.term > old_leader.term);
    cluster
        .handle(new_leader.id)
        .propose(put("winner-a", 10))
        .await
        .unwrap();
    cluster
        .handle(new_leader.id)
        .propose(put("winner-b", 20))
        .await
        .unwrap();
    wait_until("majority commits index 3", || {
        cluster
            .statuses_among(&others)
            .iter()
            .all(|s| s.commit_index == 3)
    })
    .await;
    assert_eq!(
        cluster.handle(old_leader.id).status().commit_index,
        1,
        "minority leader never committed its orphans"
    );

    // Heal: the deposed leader's conflicting suffix must be truncated and
    // replaced via next_index backtracking (prev 3 fails → prev 2 fails →
    // prev 1 matches → truncate + append).
    for &id in &others {
        cluster.net.set_pair_blocked(old_leader.id, id, false);
    }
    wait_until("all nodes converge on the winner log", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 3 && s.last_log_index == 3)
    })
    .await;

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let healed = cluster.disk_log(old_leader.id);
    assert_eq!(healed.len(), 3);
    assert_eq!(healed[0].command, put("base", 0));
    assert_eq!(
        healed[1].command,
        put("winner-a", 10),
        "orphan-a was truncated"
    );
    assert_eq!(
        healed[2].command,
        put("winner-b", 20),
        "orphan-b was truncated"
    );
    assert_eq!(healed[1].term, new_leader.term);
    for id in cluster.all_ids() {
        assert_eq!(cluster.disk_log(id), healed, "node {id}: identical log");
    }
}

#[tokio::test(start_paused = true)]
async fn minority_leader_accepts_but_never_commits() {
    let cluster = spawn_cluster(3, 25, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    let others: Vec<NodeId> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != leader.id)
        .collect();

    cluster
        .handle(leader.id)
        .propose(put("base", 0))
        .await
        .unwrap();
    wait_until("all nodes commit the base entry", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 1)
    })
    .await;

    // Full isolation of the leader (CP: minority side must not make progress).
    for &id in &others {
        cluster.net.set_pair_blocked(leader.id, id, true);
    }
    let (_, index) = cluster
        .handle(leader.id)
        .propose(put("doomed", 9))
        .await
        .unwrap();
    assert_eq!(index, 2);

    // Its commit index must stay pinned for 3 virtual seconds of trying.
    for _ in 0..60 {
        tokio::time::sleep(ms(50)).await;
        assert_eq!(
            cluster.handle(leader.id).status().commit_index,
            1,
            "a minority leader must never commit"
        );
    }
    // Meanwhile the majority moved on.
    let new_leader = cluster.wait_for_leader_among(&others).await;
    assert!(new_leader.term > leader.term);

    // Heal, then let the new leader write; the doomed entry is overwritten.
    for &id in &others {
        cluster.net.set_pair_blocked(leader.id, id, false);
    }
    cluster
        .handle(new_leader.id)
        .propose(put("final", 1))
        .await
        .unwrap();
    wait_until("everyone converges on [base, final]", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 2 && s.last_log_index == 2)
    })
    .await;

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let reference = cluster.disk_log(new_leader.id);
    assert_eq!(reference.len(), 2);
    assert_eq!(reference[1].command, put("final", 1));
    for id in cluster.all_ids() {
        let log = cluster.disk_log(id);
        assert_eq!(log, reference, "node {id}: identical log");
        assert!(
            log.iter().all(|e| e.command != put("doomed", 9)),
            "node {id}: the never-committed entry must not survive"
        );
    }
}

// ---- determinism ----

async fn replication_outcome(seed: u64) -> (NodeId, Term, Vec<LogEntry>) {
    let cluster = spawn_cluster(3, seed, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    for i in 1..=3u64 {
        cluster
            .handle(leader.id)
            .propose(put(&format!("k{i}"), i))
            .await
            .unwrap();
    }
    wait_until("all nodes commit index 3", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 3)
    })
    .await;
    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    (leader.id, leader.term, cluster.disk_log(1))
}

#[tokio::test(start_paused = true)]
async fn same_seed_reproduces_the_same_replication_outcome() {
    assert_eq!(replication_outcome(77).await, replication_outcome(77).await);
}

// ---- confirmed writes survive message loss ----

/// Watches `leader` until it commits `index` in `term`. Returns false if it
/// loses leadership or moves terms first (outcome unknown).
async fn confirm_commit(
    cluster: &TestCluster,
    leader: NodeId,
    term: Term,
    index: LogIndex,
) -> bool {
    for _ in 0..2000 {
        let status = cluster.handle(leader).status();
        if status.term != term || status.role != RoleKind::Leader {
            return false;
        }
        if status.commit_index >= index {
            return true;
        }
        tokio::time::sleep(ms(5)).await;
    }
    false
}

#[tokio::test(start_paused = true)]
async fn confirmed_writes_survive_sustained_message_loss() {
    for seed in 0..3 {
        let faults = FaultConfig {
            min_delay: ms(1),
            max_delay: ms(15),
            drop_probability: 0.15,
            rpc_timeout: ms(40),
        };
        let cluster = spawn_cluster(3, seed, faults);

        // (term, index) of every write whose commit we positively observed.
        let mut confirmed: Vec<(Term, LogIndex, u64)> = Vec::new();
        let mut value = 0u64;
        while confirmed.len() < 10 {
            value += 1;
            // Re-proposing after an unconfirmed outcome may commit a value
            // twice — that's legal Raft; clients get dedup in later phases.
            loop {
                let leader = cluster.wait_for_leader().await;
                match cluster
                    .handle(leader.id)
                    .propose(put(&format!("v{value}"), value))
                    .await
                {
                    Err(_) => tokio::time::sleep(ms(20)).await,
                    Ok((term, index)) => {
                        if confirm_commit(&cluster, leader.id, term, index).await {
                            confirmed.push((term, index, value));
                            break;
                        }
                        // Unknown outcome: retry the same value.
                    }
                }
            }
        }

        // Quiesce, then verify every confirmed write is in every log.
        wait_until("all nodes converge", || {
            let statuses = cluster.statuses_among(&cluster.all_ids());
            let max_commit = statuses.iter().map(|s| s.commit_index).max().unwrap();
            statuses.iter().all(|s| s.commit_index == max_commit)
        })
        .await;
        cluster.shutdown();
        tokio::time::sleep(ms(200)).await;

        let reference = cluster.disk_log(1);
        for id in cluster.all_ids() {
            assert_eq!(
                cluster.disk_log(id),
                reference,
                "seed {seed}: identical logs"
            );
        }
        for &(term, index, value) in &confirmed {
            let entry = &reference[usize::try_from(index - 1).unwrap()];
            assert_eq!(
                (entry.term, &entry.command),
                (term, &put(&format!("v{value}"), value)),
                "seed {seed}: confirmed write (term {term}, index {index}) was lost"
            );
        }
        cluster.shutdown();
    }
}

// ---- RPC-level AppendEntries conformance ----

async fn append(
    transport: &SimTransport,
    to: NodeId,
    term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: Vec<LogEntry>,
    leader_commit: LogIndex,
) -> AppendEntriesReply {
    let request = RpcRequest::AppendEntries(AppendEntriesArgs {
        term,
        leader_id: 2,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
    });
    match transport.send(to, request).await.expect("rpc failed") {
        RpcResponse::AppendEntries(reply) => reply,
        other => panic!("unexpected response: {other:?}"),
    }
}

fn e(term: Term, index: LogIndex, key: &str) -> LogEntry {
    LogEntry {
        term,
        index,
        command: put(key, index),
    }
}

#[tokio::test(start_paused = true)]
async fn append_entries_rpc_conformance() {
    let net = SimNetwork::new(0, low_loss_faults());
    let dir = tempfile::tempdir().unwrap();
    let (t2, _rx2) = net.register(2);
    let (t1, rx1) = net.register(1);
    let node = RaftNode::spawn(
        passive_config(1, vec![2, 3]),
        Storage::open(dir.path()).unwrap(),
        t1,
        rx1,
    );

    // Initial append of two entries.
    let reply = append(&t2, 1, 1, 0, 0, vec![e(1, 1, "a"), e(1, 2, "b")], 0).await;
    assert!(reply.success);
    wait_until("entries stored", || {
        cluster_status(&node).last_log_index == 2
    })
    .await;
    assert_eq!(cluster_status(&node).commit_index, 0);

    // leader_commit is capped by what this RPC verified (prev=1 here), even
    // if the leader claims more.
    let reply = append(&t2, 1, 1, 1, 1, vec![], 5).await;
    assert!(reply.success);
    assert_eq!(
        cluster_status(&node).commit_index,
        1,
        "commit capped at verified prefix"
    );

    // Duplicate delivery is idempotent.
    let reply = append(&t2, 1, 1, 0, 0, vec![e(1, 1, "a"), e(1, 2, "b")], 1).await;
    assert!(reply.success);
    assert_eq!(
        cluster_status(&node).last_log_index,
        2,
        "no duplicate growth"
    );

    // A gap (prev beyond our log) is rejected so the leader backtracks.
    let reply = append(&t2, 1, 1, 5, 1, vec![e(1, 6, "x")], 1).await;
    assert!(!reply.success);
    assert_eq!(cluster_status(&node).last_log_index, 2);

    // A stale-term AppendEntries is rejected and told the current term.
    let reply = append(&t2, 1, 0, 0, 0, vec![], 0).await;
    assert!(!reply.success);
    assert_eq!(reply.term, 1);

    // A new leader (term 2) overwrites the uncommitted entry at index 2.
    let reply = append(&t2, 1, 2, 1, 1, vec![e(2, 2, "c")], 2).await;
    assert!(reply.success);
    wait_until("commit reaches 2", || {
        cluster_status(&node).commit_index == 2
    })
    .await;

    node.shutdown();
    tokio::time::sleep(ms(100)).await;
    let log = Storage::open(dir.path()).unwrap().entries().to_vec();
    assert_eq!(log.len(), 2);
    assert_eq!((log[0].term, &log[0].command), (1, &put("a", 1)));
    assert_eq!(
        (log[1].term, &log[1].command),
        (2, &put("c", 2)),
        "conflict was replaced"
    );
}

fn cluster_status(node: &rustkv::raft::node::RaftHandle) -> rustkv::raft::node::Status {
    node.status()
}
