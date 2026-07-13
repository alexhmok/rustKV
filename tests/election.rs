//! Leader-election tests on the simulated transport with virtual time
//! (`start_paused`): every scenario is deterministic given its seed.
//!
//! Covered: exactly-one-leader convergence across seeds, seed determinism,
//! heartbeat stability, leader crash and re-election, partition/heal, the
//! §5.4.1 election restriction (cluster-level and RPC-level), vote rules and
//! vote persistence across restart, at-most-one-leader-per-term under
//! message loss, and PreVote (§9.6, phase 11): disruption prevention for a
//! healed node (term never moves, leadership never churns), the RPC-level
//! grant rule (log check, no term adoption, leader stickiness), that
//! granting a pre-vote never suppresses the grantor's own elections, and
//! that cold starts still elect.
//! NOT covered here: log replication (tests/replication.rs) and durable-write
//! invariants under crashes mid-replication (phase 6). The
//! one-leader-per-term check here samples every 10ms of virtual time (kept
//! as a redundant cluster-level check); the airtight event-level version
//! lives in the sim transport since phase 10 and is asserted by
//! `TestCluster::shutdown` in every test below.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::*;
use rustkv::raft::Storage;
use rustkv::raft::node::{RaftNode, RoleKind, Status};
use rustkv::raft::rpc::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply, RpcRequest,
    RpcResponse,
};
use rustkv::raft::transport::Transport;
use rustkv::raft::transport::sim::{FaultConfig, SimNetwork, SimTransport};
use rustkv::raft::types::{HardState, NodeId, Term};

// ---- convergence and stability ----

#[tokio::test(start_paused = true)]
async fn elects_exactly_one_leader_and_converges_across_seeds() {
    for seed in 0..10 {
        let cluster = spawn_cluster(3, seed, low_loss_faults());
        let leader = cluster.wait_for_leader().await;
        // Let a few heartbeat rounds spread the leader's authority.
        tokio::time::sleep(ms(200)).await;

        let statuses = cluster.statuses_among(&cluster.all_ids());
        let leaders: Vec<_> = statuses
            .iter()
            .filter(|s| s.role == RoleKind::Leader)
            .collect();
        assert_eq!(leaders.len(), 1, "seed {seed}: exactly one leader");
        for status in &statuses {
            assert_eq!(status.term, leaders[0].term, "seed {seed}: all on one term");
            assert_eq!(
                status.leader_id,
                Some(leaders[0].id),
                "seed {seed}: agree on leader"
            );
        }
        assert_eq!(
            leaders[0].id, leader.id,
            "seed {seed}: leadership stable, no faults"
        );
        cluster.shutdown();
    }
}

async fn election_outcome(seed: u64) -> (NodeId, Term) {
    let cluster = spawn_cluster(3, seed, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    cluster.shutdown();
    (leader.id, leader.term)
}

#[tokio::test(start_paused = true)]
async fn same_seed_reproduces_the_same_election() {
    assert_eq!(election_outcome(42).await, election_outcome(42).await);
    assert_eq!(election_outcome(1234).await, election_outcome(1234).await);
}

#[tokio::test(start_paused = true)]
async fn heartbeats_prevent_spurious_reelections() {
    let cluster = spawn_cluster(3, 5, low_loss_faults());
    let leader = cluster.wait_for_leader().await;
    // 10 virtual seconds ≈ 40+ election timeouts with nothing going wrong.
    tokio::time::sleep(Duration::from_secs(10)).await;
    for status in cluster.statuses_among(&cluster.all_ids()) {
        assert_eq!(status.term, leader.term, "term never moved");
        assert_eq!(status.leader_id, Some(leader.id), "leadership never moved");
    }
    cluster.shutdown();
}

// ---- failures ----

#[tokio::test(start_paused = true)]
async fn leader_crash_triggers_reelection_at_higher_term() {
    let cluster = spawn_cluster(3, 11, low_loss_faults());
    let old = cluster.wait_for_leader().await;

    cluster.crash(old.id);
    let survivors: Vec<NodeId> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != old.id)
        .collect();
    let new = cluster.wait_for_leader_among(&survivors).await;

    assert_ne!(new.id, old.id);
    assert!(new.term > old.term, "new leader must hold a newer term");
    cluster.shutdown();
}

#[tokio::test(start_paused = true)]
async fn partitioned_leader_is_deposed_and_rejoins_as_follower() {
    let cluster = spawn_cluster(3, 3, low_loss_faults());
    let old = cluster.wait_for_leader().await;
    let others: Vec<NodeId> = cluster
        .all_ids()
        .into_iter()
        .filter(|&id| id != old.id)
        .collect();

    // Cut the leader off from the majority.
    for &id in &others {
        cluster.net.set_pair_blocked(old.id, id, true);
    }
    let new = cluster.wait_for_leader_among(&others).await;
    assert!(new.term > old.term, "majority side moves to a newer term");
    // Basic Raft: an isolated leader keeps believing it leads its old term
    // (harmless — phase 5 guarantees its writes can never commit).
    let isolated = cluster.handle(old.id).status();
    assert!(isolated.term <= new.term);

    // Heal: the old leader must step down; the cluster converges.
    for &id in &others {
        cluster.net.set_pair_blocked(old.id, id, false);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let statuses = cluster.statuses_among(&cluster.all_ids());
    let leaders: Vec<_> = statuses
        .iter()
        .filter(|s| s.role == RoleKind::Leader)
        .collect();
    assert_eq!(leaders.len(), 1, "exactly one leader after heal");
    assert_eq!(cluster.handle(old.id).status().role, RoleKind::Follower);
    for status in &statuses {
        assert_eq!(status.term, leaders[0].term);
        assert_eq!(status.leader_id, Some(leaders[0].id));
    }
    cluster.shutdown();
}

/// The PreVote headline (phase 11), inverting this test's phase-3 ancestor:
/// an isolated follower used to churn its term as a candidate and force a
/// re-election on heal. With PreVote it only pre-campaigns — its term NEVER
/// advances — so healing is disruption-free: the leader keeps leading and
/// the cluster term is unchanged.
#[tokio::test(start_paused = true)]
async fn isolated_follower_pre_campaigns_without_term_churn_and_heals_quietly() {
    let cluster = spawn_cluster(3, 8, low_loss_faults());
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

    for &id in &others {
        cluster.net.set_pair_blocked(follower, id, true);
    }
    // 5 virtual seconds ≈ 20+ election timeouts on the isolated node. Every
    // one starts a pre-campaign that can't win — and, unlike a candidacy,
    // bumps nothing: its term must stay pinned to the leader's the whole
    // time. The majority side keeps its leader and term throughout.
    for _ in 0..100 {
        tokio::time::sleep(ms(50)).await;
        let isolated = cluster.handle(follower).status();
        assert_ne!(isolated.role, RoleKind::Leader);
        assert_eq!(
            isolated.term, leader.term,
            "pre-campaigns must never advance the term"
        );
        for status in cluster.statuses_among(&others) {
            assert_eq!(status.term, leader.term, "majority side undisturbed");
            assert_eq!(status.leader_id, Some(leader.id));
        }
    }
    // Proof the timeouts really fired: the node is mid-pre-campaign.
    assert_eq!(
        cluster.handle(follower).status().role,
        RoleKind::PreCandidate
    );

    // Heal. The node's term matches the cluster's, its pre-votes are denied
    // (stickiness on the follower, leadership on the leader), and the first
    // heartbeat folds it back in — no re-election, no term movement.
    for &id in &others {
        cluster.net.set_pair_blocked(follower, id, false);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let statuses = cluster.statuses_among(&cluster.all_ids());
    let leaders: Vec<&Status> = statuses
        .iter()
        .filter(|s| s.role == RoleKind::Leader)
        .collect();
    assert_eq!(leaders.len(), 1, "exactly one leader after heal");
    assert_eq!(leaders[0].id, leader.id, "the old leader kept leading");
    assert_eq!(
        cluster.handle(follower).status().role,
        RoleKind::Follower,
        "the healed node rejoined as follower"
    );
    for status in &statuses {
        assert_eq!(status.term, leader.term, "cluster term never moved");
        assert_eq!(status.leader_id, Some(leader.id));
    }
    cluster.shutdown();
}

// ---- safety invariant under loss ----

#[tokio::test(start_paused = true)]
async fn at_most_one_leader_per_term_under_message_loss() {
    for seed in 0..5 {
        let faults = FaultConfig {
            min_delay: ms(1),
            max_delay: ms(15),
            drop_probability: 0.25,
            duplicate_probability: 0.0,
            rpc_timeout: ms(40),
        };
        let cluster = spawn_cluster(3, seed, faults);

        let mut leaders_by_term: HashMap<Term, NodeId> = HashMap::new();
        for _ in 0..600 {
            // 6 virtual seconds, sampled every 10ms.
            tokio::time::sleep(ms(10)).await;
            for status in cluster.statuses_among(&cluster.all_ids()) {
                if status.role == RoleKind::Leader {
                    let prev = leaders_by_term.entry(status.term).or_insert(status.id);
                    assert_eq!(
                        *prev, status.id,
                        "seed {seed}: two leaders observed in term {}",
                        status.term
                    );
                }
            }
        }
        assert!(
            !leaders_by_term.is_empty(),
            "seed {seed}: elections still succeed under loss"
        );
        cluster.shutdown();
    }
}

// ---- election restriction (§5.4.1) ----

#[tokio::test(start_paused = true)]
async fn node_with_stale_log_never_becomes_leader() {
    for seed in 0..5 {
        let cluster = spawn_cluster_with(3, seed, low_loss_faults(), |id, storage| {
            // Nodes 1 and 2 hold a committed-looking entry from term 1;
            // node 3 slept through it. All start at term 1.
            storage
                .save_hard_state(HardState {
                    current_term: 1,
                    voted_for: None,
                })
                .unwrap();
            if id != 3 {
                storage.append(&[entry(1, 1)]).unwrap();
            }
        });
        let leader = cluster.wait_for_leader().await;
        assert_ne!(
            leader.id, 3,
            "seed {seed}: a stale log must never win an election"
        );
        cluster.shutdown();
    }
}

// ---- RPC-level vote rules, driven by the test acting as a fake candidate ----

async fn request_vote(
    transport: &SimTransport,
    to: NodeId,
    term: Term,
    candidate_id: NodeId,
    last_log_index: u64,
    last_log_term: Term,
) -> RequestVoteReply {
    let request = RpcRequest::RequestVote(RequestVoteArgs {
        term,
        candidate_id,
        last_log_index,
        last_log_term,
    });
    match transport.send(to, request).await.expect("rpc failed") {
        RpcResponse::RequestVote(reply) => reply,
        other => panic!("unexpected response: {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn vote_rules_and_vote_persistence_across_restart() {
    let net = SimNetwork::new(0, low_loss_faults());
    let dir = tempfile::tempdir().unwrap();
    let (t2, _rx2) = net.register(2);
    let (t3, _rx3) = net.register(3);

    let (t1, rx1) = net.register(1);
    let node = RaftNode::spawn(
        passive_config(1, vec![2, 3]),
        Storage::open(dir.path()).unwrap(),
        t1,
        rx1,
        new_sm(),
    );

    // Candidate 2 gets the vote for term 1.
    let reply = request_vote(&t2, 1, 1, 2, 0, 0).await;
    assert!(reply.vote_granted);
    assert_eq!(reply.term, 1);
    // Same term: a competing candidate is refused...
    assert!(!request_vote(&t3, 1, 1, 3, 0, 0).await.vote_granted);
    // ...but the same candidate is re-granted (idempotent, §5.2).
    assert!(request_vote(&t2, 1, 1, 2, 0, 0).await.vote_granted);
    // A stale-term request is refused and told the current term.
    let reply = request_vote(&t2, 1, 0, 2, 0, 0).await;
    assert!(!reply.vote_granted);
    assert_eq!(reply.term, 1);

    // Restart the node from the same directory.
    node.shutdown();
    tokio::time::sleep(ms(50)).await;
    let (t1b, rx1b) = net.register(1);
    let node = RaftNode::spawn(
        passive_config(1, vec![2, 3]),
        Storage::open(dir.path()).unwrap(),
        t1b,
        rx1b,
        new_sm(),
    );
    assert_eq!(node.status().term, 1, "term survived the restart");

    // The vote in term 1 must survive too: candidate 3 still refused,
    // candidate 2 still granted.
    assert!(!request_vote(&t3, 1, 1, 3, 0, 0).await.vote_granted);
    assert!(request_vote(&t2, 1, 1, 2, 0, 0).await.vote_granted);
    node.shutdown();
}

#[tokio::test(start_paused = true)]
async fn election_restriction_rejects_stale_logs_at_rpc_level() {
    let net = SimNetwork::new(0, low_loss_faults());
    let dir = tempfile::tempdir().unwrap();
    let (t2, _rx2) = net.register(2);
    let (t3, _rx3) = net.register(3);

    // Node 1 has an entry from term 2 in its log.
    let mut storage = Storage::open(dir.path()).unwrap();
    storage
        .save_hard_state(HardState {
            current_term: 2,
            voted_for: None,
        })
        .unwrap();
    storage.append(&[entry(2, 1)]).unwrap();
    let (t1, rx1) = net.register(1);
    let node = RaftNode::spawn(passive_config(1, vec![2, 3]), storage, t1, rx1, new_sm());

    // A candidate with an empty log is refused despite its higher term —
    // and the node adopts that term (§5.1).
    let reply = request_vote(&t2, 1, 3, 2, 0, 0).await;
    assert!(!reply.vote_granted);
    assert_eq!(reply.term, 3);

    // A candidate whose log is exactly as up-to-date is granted.
    assert!(request_vote(&t3, 1, 3, 3, 1, 2).await.vote_granted);

    // Same last term but shorter log: refused (term 4, log (term 2, idx 0)).
    assert!(!request_vote(&t2, 1, 4, 2, 0, 2).await.vote_granted);

    // Newer last-log term beats a longer log: granted.
    assert!(request_vote(&t2, 1, 5, 2, 1, 3).await.vote_granted);
    node.shutdown();
}

// ---- PreVote (§9.6, phase 11) ----

async fn pre_vote(
    transport: &SimTransport,
    to: NodeId,
    prospective_term: Term,
    candidate_id: NodeId,
    last_log_index: u64,
    last_log_term: Term,
) -> RequestVoteReply {
    let request = RpcRequest::PreVote(RequestVoteArgs {
        term: prospective_term,
        candidate_id,
        last_log_index,
        last_log_term,
    });
    match transport.send(to, request).await.expect("rpc failed") {
        RpcResponse::PreVote(reply) => reply,
        other => panic!("unexpected response: {other:?}"),
    }
}

async fn heartbeat(
    transport: &SimTransport,
    to: NodeId,
    term: Term,
    leader_id: NodeId,
) -> AppendEntriesReply {
    let request = RpcRequest::AppendEntries(AppendEntriesArgs {
        term,
        leader_id,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
    });
    match transport.send(to, request).await.expect("rpc failed") {
        RpcResponse::AppendEntries(reply) => reply,
        other => panic!("unexpected response: {other:?}"),
    }
}

/// The pre-vote grant rule at RPC level: same §5.4.1 log comparison as a
/// real vote, prospective term must exceed the grantor's — and, unlike a
/// real vote, nothing moves: no term adoption, no recorded vote, grants
/// are unlimited.
#[tokio::test(start_paused = true)]
async fn pre_vote_denied_on_stale_log_and_never_moves_the_term() {
    let net = SimNetwork::new(0, low_loss_faults());
    let dir = tempfile::tempdir().unwrap();
    let (t2, _rx2) = net.register(2);
    let (t3, _rx3) = net.register(3);

    // Node 1 holds an entry from term 2 and has never heard from a leader —
    // so stickiness cannot be what denies anything below.
    let mut storage = Storage::open(dir.path()).unwrap();
    storage
        .save_hard_state(HardState {
            current_term: 2,
            voted_for: None,
        })
        .unwrap();
    storage.append(&[entry(2, 1)]).unwrap();
    let (t1, rx1) = net.register(1);
    let node = RaftNode::spawn(passive_config(1, vec![2, 3]), storage, t1, rx1, new_sm());

    // A stale (empty) log is denied despite the higher prospective term —
    // and, unlike a real RequestVote, the probe must not move node 1's term.
    let reply = pre_vote(&t2, 1, 3, 2, 0, 0).await;
    assert!(!reply.vote_granted);
    assert_eq!(reply.term, 2, "the reply reports the real current term");
    assert_eq!(node.status().term, 2, "a pre-vote never adopts a term");

    // Same last term but shorter log: denied.
    assert!(!pre_vote(&t2, 1, 3, 2, 0, 2).await.vote_granted);
    // Up-to-date log but a prospective term not beyond ours: denied.
    assert!(!pre_vote(&t2, 1, 2, 2, 1, 2).await.vote_granted);
    // Up-to-date log + higher prospective term: granted...
    assert!(pre_vote(&t2, 1, 3, 2, 1, 2).await.vote_granted);
    // ...to any number of askers — a pre-vote binds nobody, so there is no
    // one-grant-per-term rule...
    assert!(pre_vote(&t3, 1, 3, 3, 1, 2).await.vote_granted);
    // ...and none of it was recorded: the REAL vote for term 3 is still up
    // for grabs, and it (not the probes) is what finally moves the term.
    assert!(request_vote(&t3, 1, 3, 3, 1, 2).await.vote_granted);
    assert_eq!(node.status().term, 3);
    node.shutdown();
}

/// Leader stickiness: a node that heard from a valid leader within
/// election_timeout_min denies pre-votes even to a perfectly up-to-date
/// candidate. This — not the log check — is what stops an up-to-date healed
/// node from disrupting. Real votes are deliberately NOT sticky.
#[tokio::test(start_paused = true)]
async fn leader_stickiness_denies_pre_vote_despite_valid_log() {
    let net = SimNetwork::new(0, low_loss_faults());
    let dir = tempfile::tempdir().unwrap();
    let (t2, _rx2) = net.register(2);
    let (t3, _rx3) = net.register(3);
    let (t1, rx1) = net.register(1);
    let node = RaftNode::spawn(
        passive_config(1, vec![2, 3]),
        Storage::open(dir.path()).unwrap(),
        t1,
        rx1,
        new_sm(),
    );

    // Before any leader contact the same probe would succeed.
    assert!(pre_vote(&t3, 1, 1, 3, 0, 0).await.vote_granted);

    // Node 2 heartbeats node 1 as leader of term 1.
    assert!(heartbeat(&t2, 1, 1, 2).await.success);

    // Node 3's log is up to date and its prospective term is higher, but
    // node 1 just heard from a live leader: denied.
    let reply = pre_vote(&t3, 1, 2, 3, 0, 0).await;
    assert!(!reply.vote_granted, "stickiness must deny the pre-vote");
    assert_eq!(node.status().term, 1);

    // A REAL RequestVote for the same term still succeeds — stickiness
    // gates only the probe, never a binding vote (§5.2 is unchanged).
    assert!(request_vote(&t3, 1, 2, 3, 0, 0).await.vote_granted);
    node.shutdown();
}

/// Granting a pre-vote must NOT reset the grantor's election timer (a
/// pre-vote is not a vote): under a continuous stream of grantable probes
/// the node still times out and starts its own pre-campaign on schedule.
#[tokio::test(start_paused = true)]
async fn granting_pre_votes_does_not_suppress_own_elections() {
    let net = SimNetwork::new(0, low_loss_faults());
    let dir = tempfile::tempdir().unwrap();
    let (t2, _rx2) = net.register(2);
    let (t1, rx1) = net.register(1);
    // Real election timeouts (150–300ms); peers never answer, so the node
    // can only ever reach PreCandidate.
    let node = RaftNode::spawn(
        node_config(1, 3, 0),
        Storage::open(dir.path()).unwrap(),
        t1,
        rx1,
        new_sm(),
    );

    // Probe every few virtual ms. If a grant reset the timer the node would
    // still be a follower long after election_timeout_max.
    let mut granted = 0;
    let deadline = tokio::time::Instant::now() + ms(1000);
    while node.status().role == RoleKind::Follower {
        assert!(
            tokio::time::Instant::now() < deadline,
            "election timeout never fired — granting pre-votes must not reset the timer"
        );
        if pre_vote(&t2, 1, 1, 2, 0, 0).await.vote_granted {
            granted += 1;
        }
    }
    assert!(granted > 0, "the node was actually granting throughout");
    assert_eq!(node.status().role, RoleKind::PreCandidate);
    assert_eq!(node.status().term, 0, "pre-campaign: term still unmoved");
    node.shutdown();
}

/// Cold start: no leader has ever existed, so last_leader_contact is unset
/// everywhere and stickiness must not deadlock the very first election.
#[tokio::test(start_paused = true)]
async fn cold_start_still_elects_a_leader_through_pre_vote() {
    for seed in [0, 7, 21] {
        let cluster = spawn_cluster(3, seed, low_loss_faults());
        let leader = cluster.wait_for_leader().await;
        assert!(leader.term >= 1, "seed {seed}: a real election happened");
        cluster.shutdown();
    }
}
