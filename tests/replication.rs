//! Log-replication tests (§5.3–§5.4) on the simulated transport with virtual
//! time: deterministic per seed.
//!
//! Covered: propose→majority commit→apply with identical on-disk logs and
//! identical KV state machines; commit notification semantics (true on
//! commit, false on truncation); proposal rejection on non-leaders (with
//! leader hint); lagging-follower catch-up; Figure-7-style divergence with
//! conflicting uncommitted entries truncated and replaced (leader
//! backtracking); a minority-partitioned leader accepting but never
//! committing (CP); seed determinism; confirmed writes surviving 15% loss;
//! RPC-level AppendEntries conformance.
//! Log shape note: every election win appends a §8 no-op entry, so client
//! entries never sit at index 1 and index math below accounts for it.
//! NOT covered here: HTTP semantics (tests/http_api.rs, tests/cluster_http.rs),
//! crashes mid-replication and deeper invariant checks (phase 6), transport-
//! level message duplication (the AE handler's duplicate path is tested
//! directly, but the simulator never duplicates in flight).

mod common;

use common::*;
use rustkv::raft::Storage;
use rustkv::raft::node::{ProposeError, RaftNode};
use rustkv::raft::rpc::{AppendEntriesArgs, AppendEntriesReply, RpcRequest, RpcResponse};
use rustkv::raft::transport::Transport;
use rustkv::raft::transport::sim::{FaultConfig, SimNetwork, SimTransport};
use rustkv::raft::types::{Command, LogEntry, LogIndex, NodeId, Term};
use rustkv::store::KvStore;
use std::sync::Arc;

// ---- happy path ----

#[tokio::test(start_paused = true)]
async fn leader_replicates_commits_and_applies_proposals() {
    let cluster = spawn_cluster(3, 21, low_loss_faults());
    let leader = cluster.wait_for_leader().await;

    for i in 1..=3u64 {
        let proposal = cluster
            .handle(leader.id)
            .propose(put(&format!("k{i}"), i))
            .await
            .expect("leader accepts proposals");
        assert_eq!(proposal.term, leader.term);
        // Index 1 is the leader's no-op; client entries start at 2.
        assert_eq!(proposal.index, i + 1, "indexes assigned sequentially");
        assert_eq!(
            proposal.committed.await,
            Ok(true),
            "proposal {i} must commit"
        );
    }

    wait_until("all nodes commit and apply index 4", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 4 && s.last_log_index == 4)
    })
    .await;

    // Every state machine holds exactly the three written keys.
    for id in cluster.all_ids() {
        let snapshot = cluster.store(id).snapshot();
        assert_eq!(snapshot.len(), 3, "node {id}");
        for i in 1..=3u64 {
            assert_eq!(
                snapshot.get(&format!("k{i}")),
                Some(&serde_json::json!(i)),
                "node {id}: k{i}"
            );
        }
    }

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let reference = cluster.disk_log(leader.id);
    assert_eq!(reference.len(), 4);
    assert_eq!(
        reference[0].command,
        Command::Noop,
        "term opens with the §8 no-op"
    );
    for (i, e) in reference.iter().enumerate().skip(1) {
        assert_eq!(e.index, i as u64 + 1);
        assert_eq!(e.term, leader.term);
        assert_eq!(e.command, put(&format!("k{i}"), i as u64));
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
        let p = cluster
            .handle(leader.id)
            .propose(put(&format!("k{i}"), i))
            .await
            .unwrap();
        assert_eq!(p.committed.await, Ok(true));
    }

    for &id in &others {
        cluster.net.set_pair_blocked(follower, id, true);
    }
    for i in 3..=5u64 {
        let p = cluster
            .handle(leader.id)
            .propose(put(&format!("k{i}"), i))
            .await
            .unwrap();
        assert_eq!(p.committed.await, Ok(true), "majority still commits");
    }
    let lagging = cluster.handle(follower).status();
    assert!(lagging.commit_index < cluster.handle(leader.id).status().commit_index);
    assert_eq!(
        cluster.store(follower).get("k3"),
        None,
        "not applied while isolated"
    );

    for &id in &others {
        cluster.net.set_pair_blocked(follower, id, false);
    }
    // Reintegration may involve a re-election (the isolated node churned its
    // term up, and each election adds a no-op), so assert convergence
    // structurally rather than on absolute indexes.
    wait_until("cluster fully converges with k5 applied everywhere", || {
        let statuses = cluster.statuses_among(&cluster.all_ids());
        let max_last = statuses.iter().map(|s| s.last_log_index).max().unwrap();
        statuses
            .iter()
            .all(|s| s.commit_index == s.last_log_index && s.last_log_index == max_last)
            && cluster
                .all_ids()
                .iter()
                .all(|&id| cluster.store(id).get("k5").is_some())
    })
    .await;

    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).snapshot().len(),
            5,
            "node {id}: all five keys"
        );
    }
    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let reference = cluster.disk_log(leader.id);
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

    // A committed base entry everyone shares (index 2, after the no-op).
    let p = cluster
        .handle(old_leader.id)
        .propose(put("base", 0))
        .await
        .unwrap();
    assert_eq!(p.index, 2);
    assert_eq!(p.committed.await, Ok(true));

    // Partition the leader into the minority; it happily appends two
    // proposals (indexes 3 and 4 in its old term) that can never commit.
    for &id in &others {
        cluster.net.set_pair_blocked(old_leader.id, id, true);
    }
    let orphan_a = cluster
        .handle(old_leader.id)
        .propose(put("orphan-a", 1))
        .await
        .unwrap();
    let orphan_b = cluster
        .handle(old_leader.id)
        .propose(put("orphan-b", 2))
        .await
        .unwrap();
    assert_eq!((orphan_a.index, orphan_b.index), (3, 4));
    assert_eq!(orphan_a.term, old_leader.term);

    // The majority elects a new leader (its no-op takes index 3) and commits
    // DIFFERENT entries at the orphaned indexes.
    let new_leader = cluster.wait_for_leader_among(&others).await;
    assert!(new_leader.term > old_leader.term);
    let wa = cluster
        .handle(new_leader.id)
        .propose(put("winner-a", 10))
        .await
        .unwrap();
    let wb = cluster
        .handle(new_leader.id)
        .propose(put("winner-b", 20))
        .await
        .unwrap();
    assert_eq!((wa.index, wb.index), (4, 5));
    assert_eq!(wa.committed.await, Ok(true));
    assert_eq!(wb.committed.await, Ok(true));
    assert_eq!(
        cluster.handle(old_leader.id).status().commit_index,
        2,
        "minority leader never committed its orphans"
    );

    // Heal: the deposed leader's conflicting suffix must be truncated and
    // replaced via next_index backtracking, and the orphan proposals must
    // resolve to `false` (definitely never applied).
    for &id in &others {
        cluster.net.set_pair_blocked(old_leader.id, id, false);
    }
    wait_until("all nodes converge on the winner log", || {
        cluster
            .statuses_among(&cluster.all_ids())
            .iter()
            .all(|s| s.commit_index == 5 && s.last_log_index == 5)
    })
    .await;
    assert_eq!(
        orphan_a.committed.await,
        Ok(false),
        "orphan-a was truncated"
    );
    assert_eq!(
        orphan_b.committed.await,
        Ok(false),
        "orphan-b was truncated"
    );

    for id in cluster.all_ids() {
        let snapshot = cluster.store(id).snapshot();
        assert_eq!(snapshot.get("orphan-a"), None, "node {id}");
        assert_eq!(snapshot.get("orphan-b"), None, "node {id}");
        assert_eq!(
            snapshot.get("winner-a"),
            Some(&serde_json::json!(10)),
            "node {id}"
        );
        assert_eq!(
            snapshot.get("winner-b"),
            Some(&serde_json::json!(20)),
            "node {id}"
        );
    }

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let healed = cluster.disk_log(old_leader.id);
    assert_eq!(healed.len(), 5);
    assert_eq!(healed[3].command, put("winner-a", 10));
    assert_eq!(healed[4].command, put("winner-b", 20));
    assert_eq!(healed[3].term, new_leader.term);
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

    let p = cluster
        .handle(leader.id)
        .propose(put("base", 0))
        .await
        .unwrap();
    assert_eq!(p.committed.await, Ok(true));

    // Full isolation of the leader (CP: minority side must not make progress).
    for &id in &others {
        cluster.net.set_pair_blocked(leader.id, id, true);
    }
    let doomed = cluster
        .handle(leader.id)
        .propose(put("doomed", 9))
        .await
        .unwrap();

    // Its commit index must stay pinned for 3 virtual seconds of trying, and
    // the doomed write must never reach its state machine.
    for _ in 0..60 {
        tokio::time::sleep(ms(50)).await;
        assert_eq!(
            cluster.handle(leader.id).status().commit_index,
            2,
            "a minority leader must never commit"
        );
    }
    assert_eq!(cluster.store(leader.id).get("doomed"), None);
    let new_leader = cluster.wait_for_leader_among(&others).await;
    assert!(new_leader.term > leader.term);

    // Heal, then let the new leader write; the doomed entry is overwritten
    // and its proposal resolves false.
    for &id in &others {
        cluster.net.set_pair_blocked(leader.id, id, false);
    }
    let fin = cluster
        .handle(new_leader.id)
        .propose(put("final", 1))
        .await
        .unwrap();
    assert_eq!(fin.committed.await, Ok(true));
    wait_until("everyone converges", || {
        let statuses = cluster.statuses_among(&cluster.all_ids());
        statuses
            .iter()
            .all(|s| s.commit_index == fin.index && s.last_log_index == fin.index)
    })
    .await;
    assert_eq!(
        doomed.committed.await,
        Ok(false),
        "doomed write definitely not applied"
    );

    for id in cluster.all_ids() {
        let snapshot = cluster.store(id).snapshot();
        assert_eq!(snapshot.get("doomed"), None, "node {id}");
        assert_eq!(
            snapshot.get("final"),
            Some(&serde_json::json!(1)),
            "node {id}"
        );
    }
    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let reference = cluster.disk_log(new_leader.id);
    for id in cluster.all_ids() {
        assert_eq!(cluster.disk_log(id), reference, "node {id}: identical log");
    }
}

// ---- determinism ----

async fn replication_outcome(seed: u64) -> (NodeId, Term, Vec<LogEntry>) {
    let cluster = spawn_cluster(3, seed, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    for i in 1..=3u64 {
        let p = cluster
            .handle(leader.id)
            .propose(put(&format!("k{i}"), i))
            .await
            .unwrap();
        assert_eq!(p.committed.await, Ok(true));
    }
    wait_until("all nodes converge", || {
        let statuses = cluster.statuses_among(&cluster.all_ids());
        statuses
            .iter()
            .all(|s| s.commit_index == 4 && s.last_log_index == 4)
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

#[tokio::test(start_paused = true)]
async fn confirmed_writes_survive_sustained_message_loss() {
    for seed in 0..3 {
        let faults = FaultConfig {
            min_delay: ms(1),
            max_delay: ms(15),
            drop_probability: 0.15,
            duplicate_probability: 0.0,
            rpc_timeout: ms(40),
        };
        let cluster = spawn_cluster(3, seed, faults);

        // (term, index, value) of every write whose commit was confirmed.
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
                    Ok(p) => {
                        // Any other outcome (false / closed / timeout) is
                        // unknown-or-refused — retry the same value.
                        if let Ok(Ok(true)) = tokio::time::timeout(ms(2000), p.committed).await {
                            confirmed.push((p.term, p.index, value));
                            break;
                        }
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
    let store = Arc::new(KvStore::new());
    let node = RaftNode::spawn(
        passive_config(1, vec![2, 3]),
        Storage::open(dir.path()).unwrap(),
        t1,
        rx1,
        store.clone() as Arc<dyn rustkv::raft::node::StateMachine>,
    );

    // Initial append of two entries.
    let reply = append(&t2, 1, 1, 0, 0, vec![e(1, 1, "a"), e(1, 2, "b")], 0).await;
    assert!(reply.success);
    wait_until("entries stored", || node.status().last_log_index == 2).await;
    assert_eq!(node.status().commit_index, 0);
    assert_eq!(store.snapshot().len(), 0, "nothing applied before commit");

    // leader_commit is capped by what this RPC verified (prev=1 here), even
    // if the leader claims more.
    let reply = append(&t2, 1, 1, 1, 1, vec![], 5).await;
    assert!(reply.success);
    assert_eq!(
        node.status().commit_index,
        1,
        "commit capped at verified prefix"
    );
    assert_eq!(
        store.get("a"),
        Some(serde_json::json!(1)),
        "committed entry applied"
    );
    assert_eq!(store.get("b"), None, "uncommitted entry not applied");

    // Duplicate delivery is idempotent.
    let reply = append(&t2, 1, 1, 0, 0, vec![e(1, 1, "a"), e(1, 2, "b")], 1).await;
    assert!(reply.success);
    assert_eq!(node.status().last_log_index, 2, "no duplicate growth");

    // A gap (prev beyond our log) is rejected so the leader backtracks.
    let reply = append(&t2, 1, 1, 5, 1, vec![e(1, 6, "x")], 1).await;
    assert!(!reply.success);
    assert_eq!(node.status().last_log_index, 2);

    // A stale-term AppendEntries is rejected and told the current term.
    let reply = append(&t2, 1, 0, 0, 0, vec![], 0).await;
    assert!(!reply.success);
    assert_eq!(reply.term, 1);

    // A new leader (term 2) overwrites the uncommitted entry at index 2.
    let reply = append(&t2, 1, 2, 1, 1, vec![e(2, 2, "c")], 2).await;
    assert!(reply.success);
    wait_until("commit reaches 2", || node.status().commit_index == 2).await;
    assert_eq!(
        store.get("c"),
        Some(serde_json::json!(2)),
        "replacement applied"
    );
    assert_eq!(store.get("b"), None, "truncated entry never applied");

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
