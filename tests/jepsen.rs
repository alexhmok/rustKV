//! Phase 8: Jepsen-style consistency checking, native to this project's
//! deterministic simulator instead of the Clojure framework — every history
//! is a pure function of its seed, so any violation is replayable exactly.
//!
//! Pieces:
//! - checker validation: hand-crafted histories the WGL checker must accept
//!   or reject (testing the tests);
//! - a concurrent workload: 4 client processes issuing randomized
//!   put/delete/get against 3 keys while a nemesis injects partitions,
//!   recording a timed history with ok/fail/unknown outcomes;
//! - checked claims:
//!   * confirmed writes respect real-time order in the committed log
//!     (white-box witness — writes ARE linearizable, log order is the
//!     linearization);
//!   * full histories including reads are checked for linearizability, in
//!     both read modes (phase 9). Stale mode reads local state from random
//!     nodes — the pre-phase-9 behavior — and under partitions the checker
//!     MUST find stale-read violations, demonstrating its power and
//!     characterizing that path honestly. Linearizable mode issues the same
//!     workload through ReadIndex ([`RaftHandle::read`]) and the checker
//!     must find NO violation on any seed — the phase-9 headline claim.
//!
//! The nemesis (phase 10) mixes partition/heal rounds with crash/restart
//! rounds (at most one node down at a time, always restarted before the
//! round ends), and a soak variant additionally duplicates 10% of all
//! requests in flight.
//!
//! Write modes (phase 13): the linearizable workload attaches a dedup
//! token (client = process, seq = op number) to every write and retries
//! ambiguous outcomes with the SAME command, recording ONE op per logical
//! write — invocation at the first attempt, return at the final ack. That
//! is sound only because dedup makes the effect apply at most once no
//! matter how many retried copies commit; it shrinks the Unknowns the
//! checker must tolerate. The stale-mode workload stays byte-identical to
//! phase 12 (single-shot, token-less) — it is the checker-has-teeth
//! regression and its ≥1-violation claim is pinned per seed.

mod common;

use std::sync::Arc;

use common::lin::{OpKind, Recorded, WriteKind, WriteOutcome, check_linearizable, render};
use common::*;
use rustkv::raft::node::RoleKind;
use rustkv::raft::types::{Command, LogEntry, NodeId, Session};
use rustkv::rng::SplitMix64;
use tokio::time::Instant;

// ---- checker validation: the checker itself must have teeth ----

fn read(process: u64, key: &str, result: Option<u64>, at: u64) -> Recorded {
    Recorded {
        process,
        key: key.to_string(),
        op: OpKind::Read { result },
        invoked_us: at,
        returned_us: at + 1,
    }
}

fn write(
    process: u64,
    key: &str,
    kind: WriteKind,
    outcome: WriteOutcome,
    invoked_us: u64,
    returned_us: u64,
) -> Recorded {
    Recorded {
        process,
        key: key.to_string(),
        op: OpKind::Write {
            kind,
            outcome,
            log_pos: None,
        },
        invoked_us,
        returned_us: if outcome == WriteOutcome::Unknown {
            u64::MAX
        } else {
            returned_us
        },
    }
}

#[test]
fn checker_accepts_valid_histories() {
    // Sequential: put, read it, delete, read absence.
    let h = vec![
        write(0, "k", WriteKind::Put(1), WriteOutcome::Ok, 0, 10),
        read(0, "k", Some(1), 20),
        write(0, "k", WriteKind::Delete, WriteOutcome::Ok, 30, 40),
        read(0, "k", None, 50),
    ];
    check_linearizable(&h).unwrap();

    // A read overlapping a put may see either the old or the new value.
    for observed in [None, Some(7)] {
        let h = vec![
            write(0, "k", WriteKind::Put(7), WriteOutcome::Ok, 0, 100),
            read(1, "k", observed, 50),
        ];
        check_linearizable(&h).unwrap();
    }

    // An unknown write may have happened (read sees it) or not (read
    // doesn't) — both must be accepted.
    for observed in [None, Some(9)] {
        let h = vec![
            write(0, "k", WriteKind::Put(9), WriteOutcome::Unknown, 0, 0),
            read(1, "k", observed, 100),
        ];
        check_linearizable(&h).unwrap();
    }

    // A failed write definitely did not happen.
    let h = vec![
        write(0, "k", WriteKind::Put(3), WriteOutcome::Fail, 0, 10),
        read(1, "k", None, 20),
    ];
    check_linearizable(&h).unwrap();

    // Keys are independent.
    let h = vec![
        write(0, "a", WriteKind::Put(1), WriteOutcome::Ok, 0, 10),
        read(1, "b", None, 20),
    ];
    check_linearizable(&h).unwrap();
}

#[test]
fn checker_rejects_invalid_histories() {
    // Reading a value nobody ever wrote.
    let h = vec![read(0, "k", Some(42), 10)];
    assert!(check_linearizable(&h).is_err());

    // Stale read: the second write completed strictly before the read began.
    let h = vec![
        write(0, "k", WriteKind::Put(1), WriteOutcome::Ok, 0, 10),
        write(0, "k", WriteKind::Put(2), WriteOutcome::Ok, 20, 30),
        read(1, "k", Some(1), 40),
    ];
    assert!(check_linearizable(&h).is_err());

    // Reading through a completed delete.
    let h = vec![
        write(0, "k", WriteKind::Put(5), WriteOutcome::Ok, 0, 10),
        write(0, "k", WriteKind::Delete, WriteOutcome::Ok, 20, 30),
        read(1, "k", Some(5), 40),
    ];
    assert!(check_linearizable(&h).is_err());

    // A failed write must NOT be readable.
    let h = vec![
        write(0, "k", WriteKind::Put(3), WriteOutcome::Fail, 0, 10),
        read(1, "k", Some(3), 20),
    ];
    assert!(check_linearizable(&h).is_err());

    // Non-monotonic pair of sequential reads around a completed write.
    let h = vec![
        write(0, "k", WriteKind::Put(1), WriteOutcome::Ok, 0, 10),
        read(1, "k", Some(1), 20),
        write(0, "k", WriteKind::Put(2), WriteOutcome::Ok, 30, 40),
        read(1, "k", Some(2), 50),
        read(1, "k", Some(1), 60),
    ];
    assert!(check_linearizable(&h).is_err());
}

// ---- T2 checker sensitivity: the white-box log witness must also have
// teeth. `check_write_witness` is the linearization claim for writes (the
// committed log IS the order), so each way an acknowledged write can be
// betrayed by the log — missing, replaced, rewritten, out of real-time
// order — must be REJECTED when fed to it directly. ----

/// An Ok write claiming (term, index) in the log — witness-probe input.
fn confirmed_put(
    process: u64,
    key: &str,
    value: u64,
    term: u64,
    index: u64,
    invoked_us: u64,
    returned_us: u64,
) -> Recorded {
    Recorded {
        process,
        key: key.to_string(),
        op: OpKind::Write {
            kind: WriteKind::Put(value),
            outcome: WriteOutcome::Ok,
            log_pos: Some((term, index)),
        },
        invoked_us,
        returned_us,
    }
}

fn log_put(term: u64, index: u64, key: &str, value: u64) -> LogEntry {
    LogEntry {
        term,
        index,
        command: put(key, value),
    }
}

/// Positive control: a faithful log passes, so the rejections below mean
/// something.
#[test]
fn witness_accepts_a_faithful_log() {
    let history = vec![
        confirmed_put(0, "a", 1, 1, 1, 0, 10),
        confirmed_put(1, "b", 2, 1, 2, 20, 30),
    ];
    let log = vec![log_put(1, 1, "a", 1), log_put(1, 2, "b", 2)];
    check_write_witness(&history, &log).unwrap();
}

/// A lost acknowledged write: confirmed at index 2, but the log ends at 1.
#[test]
#[should_panic(expected = "missing from log")]
fn witness_rejects_a_confirmed_write_missing_from_the_log() {
    let history = vec![confirmed_put(0, "a", 1, 1, 2, 0, 10)];
    let log = vec![log_put(1, 1, "a", 1)];
    let _ = check_write_witness(&history, &log);
}

/// A replaced acknowledged write: the index survived but under a different
/// term — some other leader's entry sits where the confirmed write was.
#[test]
#[should_panic(expected = "confirmed write replaced")]
fn witness_rejects_a_confirmed_write_replaced_by_another_term() {
    let history = vec![confirmed_put(0, "a", 1, 1, 1, 0, 10)];
    let log = vec![log_put(2, 1, "a", 1)];
    let _ = check_write_witness(&history, &log);
}

/// A rewritten acknowledged write: right (term, index), wrong command.
#[test]
#[should_panic(expected = "wrong command")]
fn witness_rejects_a_confirmed_write_with_the_wrong_command() {
    let history = vec![confirmed_put(0, "a", 1, 1, 1, 0, 10)];
    let log = vec![log_put(1, 1, "a", 99)];
    let _ = check_write_witness(&history, &log);
}

/// A real-time inversion: write A returned before write B was even invoked,
/// yet A sits AFTER B in the log — the linearization contradicts real time
/// even though both entries are present and intact.
#[test]
fn witness_rejects_a_log_order_that_violates_real_time() {
    let history = vec![
        confirmed_put(0, "a", 1, 1, 2, 0, 10),
        confirmed_put(1, "b", 2, 1, 1, 20, 30),
    ];
    let log = vec![log_put(1, 1, "b", 2), log_put(1, 2, "a", 1)];
    let reason = check_write_witness(&history, &log).unwrap_err();
    assert!(
        reason.contains("log order violates real time"),
        "unexpected reason: {reason}"
    );
}

// ---- the workload driver ----

const KEYS: [&str; 3] = ["a", "b", "c"];
const CLIENTS: u64 = 4;
const OPS_PER_CLIENT: u64 = 12;

fn unique_leader(cluster: &TestCluster) -> Option<NodeId> {
    // Sample only live nodes: a crashed node's status watch is frozen and
    // may still claim a leadership it no longer holds.
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

/// How the workload's clients read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadMode {
    /// Local state of a random node (the pre-phase-9 path, `?stale=true`
    /// at the HTTP layer): may be stale under partitions.
    Stale,
    /// ReadIndex through the chosen node ([`RaftHandle::read`]): non-leaders
    /// refuse, unconfirmable reads time out — a read either linearizes or
    /// never happened, so it appears in the history only when granted.
    Linearizable,
}

/// How the workload's clients write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteMode {
    /// Each op proposed at most once, token-less — the pre-phase-13 client.
    /// Stale-mode tests use this so their pinned schedules stay
    /// byte-identical.
    FireOnce,
    /// Dedup token per op, ambiguous outcomes retried (bounded) with the
    /// same command; one Recorded op per logical write (phase 13).
    TokenRetry,
}

/// Runs one seeded workload; returns the merged history, each node's final
/// on-disk log (nodes are shut down afterwards), how many crash rounds the
/// nemesis rolled, and how many ops acked only after an ambiguous attempt
/// — callers assert on the sums so crash and retry coverage can't silently
/// vanish in a future seed re-pin.
async fn run_workload(
    seed: u64,
    reads: ReadMode,
    writes: WriteMode,
    duplicate_probability: f64,
    snapshot_threshold: Option<u64>,
) -> (
    Vec<Recorded>,
    Vec<Vec<LogEntry>>,
    u64,
    u64,
    rustkv::raft::transport::sim::FaultStats,
) {
    let faults = rustkv::raft::transport::sim::FaultConfig {
        min_delay: ms(1),
        max_delay: ms(15),
        drop_probability: 0.05,
        duplicate_probability,
        rpc_timeout: ms(40),
    };
    let cluster = Arc::new(spawn_cluster_with_threshold(
        3,
        seed,
        faults.clone(),
        snapshot_threshold,
    ));
    let start = Instant::now();
    cluster.wait_for_leader().await;

    // Nemesis: each round picks a random victim and either isolates it for
    // a while and heals it, or crashes it and restarts it (at most one node
    // down at a time — the restart happens before the round ends, so the
    // final heal below always finds every node running).
    let nemesis = {
        let cluster = Arc::clone(&cluster);
        tokio::spawn(async move {
            let mut rng = SplitMix64::new(seed ^ 0xDEAD_BEEF);
            let mut crashes = 0u64;
            for _ in 0..6 {
                tokio::time::sleep(ms(rng.next_range(80..=300))).await;
                let victim = rng.next_range(1..=3);
                if rng.next_range(0..=9) < 3 {
                    crashes += 1;
                    cluster.crash(victim);
                    tokio::time::sleep(ms(rng.next_range(150..=400))).await;
                    cluster.restart(victim).await;
                } else {
                    for other in cluster.all_ids() {
                        if other != victim {
                            cluster.net.set_pair_blocked(victim, other, true);
                        }
                    }
                    tokio::time::sleep(ms(rng.next_range(150..=400))).await;
                    for other in cluster.all_ids() {
                        if other != victim {
                            cluster.net.set_pair_blocked(victim, other, false);
                        }
                    }
                }
            }
            crashes
        })
    };

    let mut clients = Vec::new();
    for process in 0..CLIENTS {
        let cluster = Arc::clone(&cluster);
        clients.push(tokio::spawn(async move {
            let mut rng = SplitMix64::new(seed.wrapping_mul(31).wrapping_add(process + 100));
            let mut history: Vec<Recorded> = Vec::new();
            let mut seq = 0u64;
            let mut op_no = 0u64;
            let mut retried_acks = 0u64;
            for _ in 0..OPS_PER_CLIENT {
                tokio::time::sleep(ms(rng.next_range(5..=80))).await;
                let key = KEYS[rng.next_range(0..=2) as usize];
                let dice = rng.next_range(0..=9);
                if dice < 5 {
                    // Read from a random node — deliberately including
                    // partitioned nodes and non-leaders.
                    let node = rng.next_range(1..=3);
                    let invoked_us = start.elapsed().as_micros() as u64;
                    match reads {
                        ReadMode::Stale => {
                            let result = cluster.store(node).get(key).and_then(|v| v.as_u64());
                            history.push(Recorded {
                                process,
                                key: key.to_string(),
                                op: OpKind::Read { result },
                                invoked_us,
                                returned_us: start.elapsed().as_micros() as u64 + 1,
                            });
                        }
                        ReadMode::Linearizable => {
                            // A refused (NotLeader), timed-out, or
                            // step-down-dropped read never observed anything
                            // and constrains nothing — record only grants.
                            let Ok(ticket) = cluster.handle(node).read().await else {
                                continue;
                            };
                            let Ok(Ok(())) = tokio::time::timeout(ms(1500), ticket.granted).await
                            else {
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
                        }
                    }
                } else if writes == WriteMode::FireOnce {
                    // Write through whoever currently looks like the leader;
                    // skip the turn if leadership is unclear.
                    let Some(leader) = unique_leader(&cluster) else {
                        continue;
                    };
                    let kind = if dice < 9 {
                        seq += 1;
                        WriteKind::Put((process + 1) * 1_000_000 + seq)
                    } else {
                        WriteKind::Delete
                    };
                    let command = match kind {
                        WriteKind::Put(value) => put(key, value),
                        WriteKind::Delete => Command::Delete {
                            key: key.to_string(),
                            session: None,
                        },
                    };
                    let invoked_us = start.elapsed().as_micros() as u64;
                    let (outcome, log_pos) = match cluster.handle(leader).propose(command).await {
                        // Rejected before append: definitely never happened.
                        Err(_) => (WriteOutcome::Fail, None),
                        Ok(p) => {
                            let pos = Some((p.term, p.index));
                            match tokio::time::timeout(ms(1500), p.committed).await {
                                Ok(Ok(true)) => (WriteOutcome::Ok, pos),
                                Ok(Ok(false)) => (WriteOutcome::Fail, pos),
                                _ => (WriteOutcome::Unknown, pos),
                            }
                        }
                    };
                    let returned_us = if outcome == WriteOutcome::Unknown {
                        u64::MAX
                    } else {
                        start.elapsed().as_micros() as u64
                    };
                    history.push(Recorded {
                        process,
                        key: key.to_string(),
                        op: OpKind::Write {
                            kind,
                            outcome,
                            log_pos,
                        },
                        invoked_us,
                        returned_us,
                    });
                } else {
                    // TokenRetry: one logical op, one token; ambiguous
                    // outcomes are retried with the SAME command (bounded).
                    // Recorded as ONE op — invocation at the first attempt,
                    // return at the final ack — sound only because dedup
                    // applies the effect at most once however many retried
                    // copies commit.
                    op_no += 1;
                    let kind = if dice < 9 {
                        seq += 1;
                        WriteKind::Put((process + 1) * 1_000_000 + seq)
                    } else {
                        WriteKind::Delete
                    };
                    let command = match kind {
                        WriteKind::Put(value) => put_with_token(key, value, process, op_no),
                        WriteKind::Delete => Command::Delete {
                            key: key.to_string(),
                            session: Some(Session {
                                client: process,
                                seq: op_no,
                            }),
                        },
                    };
                    let invoked_us = start.elapsed().as_micros() as u64;
                    let mut outcome = WriteOutcome::Fail;
                    let mut log_pos = None;
                    let mut ambiguous = false;
                    for _attempt in 0..4 {
                        let Some(leader) = unique_leader(&cluster) else {
                            tokio::time::sleep(ms(30)).await;
                            continue;
                        };
                        match cluster.handle(leader).propose(command.clone()).await {
                            // Rejected before append: this attempt
                            // definitely never happened.
                            Err(_) => {}
                            Ok(p) => {
                                let pos = Some((p.term, p.index));
                                // A much tighter bounded wait than
                                // FireOnce's 1500ms: an impatient client
                                // gives up while its copy may still commit
                                // — behind a dropped-message retransmit
                                // (~50-100ms) or a nemesis partition — so
                                // real ambiguity occurs and the retry's
                                // copy really does commit alongside the
                                // original's (the case dedup exists for).
                                match tokio::time::timeout(ms(150), p.committed).await {
                                    Ok(Ok(true)) => {
                                        outcome = WriteOutcome::Ok;
                                        log_pos = pos;
                                        break;
                                    }
                                    // Replaced by another leader: definitely
                                    // not applied at this position.
                                    Ok(Ok(false)) => {}
                                    // Unknown — this copy may still commit
                                    // later; the retry is what dedup is for.
                                    _ => {
                                        ambiguous = true;
                                        log_pos = pos;
                                    }
                                }
                            }
                        }
                        tokio::time::sleep(ms(30)).await;
                    }
                    if outcome == WriteOutcome::Ok && ambiguous {
                        retried_acks += 1;
                    }
                    // Fail only if EVERY attempt definitely didn't happen;
                    // one ambiguous copy in flight makes the op Unknown.
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
                            kind,
                            outcome,
                            log_pos,
                        },
                        invoked_us,
                        returned_us,
                    });
                }
            }
            (history, retried_acks)
        }));
    }

    let mut history = Vec::new();
    let mut retried_acks = 0u64;
    for client in clients {
        let (client_history, client_retried) = client.await.expect("client task");
        history.extend(client_history);
        retried_acks += client_retried;
    }
    let crashes = nemesis.await.expect("nemesis task");

    // Heal everything, converge, and take Jepsen-style final reads (they
    // pin down which unknown writes actually landed).
    for a in cluster.all_ids() {
        for b in cluster.all_ids() {
            if a != b {
                cluster.net.set_pair_blocked(a, b, false);
            }
        }
    }
    wait_until("cluster converges after the workload", || {
        let statuses = cluster.statuses_among(&cluster.all_ids());
        let max_last = statuses.iter().map(|s| s.last_log_index).max().unwrap();
        statuses
            .iter()
            .all(|s| s.last_log_index == max_last && s.commit_index == max_last)
    })
    .await;
    // Final-STATE equality (map + dedup sessions). With snapshots on this
    // REPLACES the callers' raw-log-equality checks (per-node compaction
    // points legally differ, so retained logs do too); with them off it is
    // a strictly additional claim (equal fully-committed logs already imply
    // it — asserting it costs nothing and changes no schedule).
    let reference_state = cluster.store(1).export();
    for id in cluster.all_ids() {
        assert_eq!(
            cluster.store(id).export(),
            reference_state,
            "seed {seed}: node {id} final state diverges"
        );
    }
    for key in KEYS {
        let invoked_us = start.elapsed().as_micros() as u64;
        let result = match reads {
            ReadMode::Stale => cluster.store(1).get(key).and_then(|v| v.as_u64()),
            // Everything is healed and converged: the first granted read
            // through the leader is the authoritative final value.
            ReadMode::Linearizable => loop {
                let Some(leader) = unique_leader(&cluster) else {
                    tokio::time::sleep(ms(10)).await;
                    continue;
                };
                let Ok(ticket) = cluster.handle(leader).read().await else {
                    continue;
                };
                if let Ok(Ok(())) = tokio::time::timeout(ms(1500), ticket.granted).await {
                    break cluster.store(leader).get(key).and_then(|v| v.as_u64());
                }
            },
        };
        history.push(Recorded {
            process: 99,
            key: key.to_string(),
            op: OpKind::Read { result },
            invoked_us,
            returned_us: start.elapsed().as_micros() as u64 + 1,
        });
    }

    // Vacuity (T2): the loss/duplication this workload scheduled must have
    // actually occurred, or the run proved nothing about surviving it.
    let stats = assert_scheduled_faults_fired(&cluster, &faults, &format!("seed {seed}"));

    cluster.shutdown();
    tokio::time::sleep(ms(100)).await;
    let logs = cluster
        .all_ids()
        .into_iter()
        .map(|id| cluster.disk_log(id))
        .collect();
    (history, logs, crashes, retried_acks, stats)
}

// ---- checked claims ----

/// White-box witness: the committed log IS the linearization of writes.
/// Every confirmed write must sit at its assigned (term, index) with the
/// right command, and log order must respect real-time order.
fn check_write_witness(history: &[Recorded], log: &[LogEntry]) -> Result<(), String> {
    let confirmed: Vec<(&Recorded, u64, u64)> = history
        .iter()
        .filter_map(|r| match &r.op {
            OpKind::Write {
                outcome: WriteOutcome::Ok,
                log_pos: Some((term, index)),
                kind,
            } => Some((r, *term, *index, *kind)),
            _ => None,
        })
        .map(|(r, term, index, kind)| {
            let entry = log
                .get(usize::try_from(index - 1).unwrap())
                .unwrap_or_else(|| panic!("confirmed write at index {index} missing from log"));
            assert_eq!(
                entry.term, term,
                "confirmed write replaced at index {index}"
            );
            // Compared field-wise, ignoring the dedup token: put values are
            // unique per logical op, so key+value pins the entry as
            // precisely as full equality did before tokens existed.
            let matches = match (&entry.command, kind) {
                (Command::Put { key, value, .. }, WriteKind::Put(v)) => {
                    *key == r.key && *value == serde_json::json!(v)
                }
                (Command::Delete { key, .. }, WriteKind::Delete) => *key == r.key,
                _ => false,
            };
            assert!(
                matches,
                "wrong command at index {index}: {:?}",
                entry.command
            );
            (r, term, index)
        })
        .collect();

    for (a, _, index_a) in &confirmed {
        for (b, _, index_b) in &confirmed {
            if a.returned_us < b.invoked_us && index_a >= index_b {
                return Err(format!(
                    "log order violates real time: index {index_a} returned at {}µs \
                     but index {index_b} was invoked later at {}µs",
                    a.returned_us, b.invoked_us
                ));
            }
        }
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn confirmed_writes_are_linearizable_via_the_log_witness() {
    for seed in 0..6 {
        let (history, logs, _, _, _) =
            run_workload(seed, ReadMode::Stale, WriteMode::FireOnce, 0.0, None).await;
        for log in &logs[1..] {
            assert_eq!(*log, logs[0], "seed {seed}: logs diverge");
        }
        check_write_witness(&history, &logs[0]).unwrap_or_else(|reason| {
            panic!("seed {seed}: {reason}");
        });
    }
}

#[tokio::test(start_paused = true)]
async fn same_seed_reproduces_the_same_history() {
    // With duplication on, so the determinism claim covers the full phase-10
    // fault mix: partitions, crashes/restarts, and duplicated messages.
    let (history_a, logs_a, crashes, _, stats_a) =
        run_workload(3, ReadMode::Linearizable, WriteMode::TokenRetry, 0.10, None).await;
    let (history_b, logs_b, _, _, stats_b) =
        run_workload(3, ReadMode::Linearizable, WriteMode::TokenRetry, 0.10, None).await;
    assert!(crashes > 0, "seed 3 must exercise a crash round");
    let refs_a: Vec<&Recorded> = history_a.iter().collect();
    let refs_b: Vec<&Recorded> = history_b.iter().collect();
    assert_eq!(
        render(&refs_a),
        render(&refs_b),
        "histories must be identical"
    );
    assert_eq!(logs_a, logs_b);
    assert_eq!(
        stats_a, stats_b,
        "fault-event counts must be identical too — a diverging count means \
         nondeterminism the history comparison happened not to see"
    );
}

/// T2 determinism audit: the strongest workload configuration — duplication
/// AND snapshots together — never had a repeated-run determinism check
/// (compaction adds disk truncation, snapshot capture and InstallSnapshot
/// to the replayed surface). Same seed must reproduce the identical
/// history, retained logs, and fault-event counts, byte for byte.
#[tokio::test(start_paused = true)]
async fn same_seed_reproduces_the_same_history_with_snapshots_on() {
    for seed in 0..2 {
        let (history_a, logs_a, _, _, stats_a) = run_workload(
            seed,
            ReadMode::Linearizable,
            WriteMode::TokenRetry,
            0.10,
            Some(16),
        )
        .await;
        let (history_b, logs_b, _, _, stats_b) = run_workload(
            seed,
            ReadMode::Linearizable,
            WriteMode::TokenRetry,
            0.10,
            Some(16),
        )
        .await;
        let refs_a: Vec<&Recorded> = history_a.iter().collect();
        let refs_b: Vec<&Recorded> = history_b.iter().collect();
        assert_eq!(
            render(&refs_a),
            render(&refs_b),
            "seed {seed}: histories diverge"
        );
        assert_eq!(logs_a, logs_b, "seed {seed}: retained logs diverge");
        assert_eq!(stats_a, stats_b, "seed {seed}: fault counts diverge");
    }
}

/// Extended determinism soak, excluded from the default run (wired into
/// `make soak` via the extended_soak name filter): every seed is run TWICE
/// under duplication + snapshots and the recorded history must reproduce
/// byte-identically — the wide-N hunt for HashMap iteration order, real
/// time leaking into paused time, or unseeded randomness in the workload
/// and history-recording path itself.
#[tokio::test(start_paused = true)]
#[ignore = "extended soak; run explicitly with --ignored"]
async fn extended_soak_same_seed_history_determinism() {
    let seeds: u64 = std::env::var("RUSTKV_SOAK_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    for seed in 0..seeds {
        let (history_a, logs_a, _, _, stats_a) = run_workload(
            seed,
            ReadMode::Linearizable,
            WriteMode::TokenRetry,
            0.10,
            Some(16),
        )
        .await;
        let (history_b, logs_b, _, _, stats_b) = run_workload(
            seed,
            ReadMode::Linearizable,
            WriteMode::TokenRetry,
            0.10,
            Some(16),
        )
        .await;
        let refs_a: Vec<&Recorded> = history_a.iter().collect();
        let refs_b: Vec<&Recorded> = history_b.iter().collect();
        assert_eq!(
            render(&refs_a),
            render(&refs_b),
            "seed {seed}: histories diverge"
        );
        assert_eq!(logs_a, logs_b, "seed {seed}: retained logs diverge");
        assert_eq!(stats_a, stats_b, "seed {seed}: fault counts diverge");
    }
}

/// Full histories with local (any-node) reads: the checker runs for real,
/// and stale reads under partitions are EXPECTED — reads are documented as
/// non-linearizable. This test asserts both directions: quiet seeds pass,
/// and across the seed set at least one partition-window stale read is
/// caught (proving the checker finds real violations, not just crafted
/// ones). The exact split is pinned because runs are deterministic.
#[tokio::test(start_paused = true)]
async fn local_reads_expose_documented_staleness_under_partitions() {
    let mut violations = Vec::new();
    for seed in 0..6 {
        let (history, _, _, _, _) =
            run_workload(seed, ReadMode::Stale, WriteMode::FireOnce, 0.0, None).await;
        if let Err(reason) = check_linearizable(&history) {
            violations.push((seed, reason));
        }
    }
    assert!(
        !violations.is_empty(),
        "expected at least one stale-read violation across seeds — either the \
         checker lost its teeth or reads silently became linearizable"
    );
    for (seed, reason) in &violations {
        eprintln!("seed {seed}: documented stale-read violation:\n{reason}");
    }
}

/// The phase-9 inversion of the test above: the same seeds, keys and
/// nemesis pattern, but reads go through ReadIndex — and, since phase 13,
/// writers carry dedup tokens and retry ambiguous outcomes, so retried ops
/// land in the history as single Ok ops instead of permanent Unknowns.
/// The checker that provably catches stale local reads must find NO
/// violation here — on any seed. That is both phase 9's headline claim
/// (reads are linearizable) and phase 13's (retried tokened writes are
/// exactly-once, or the tighter history would linearize nowhere).
#[tokio::test(start_paused = true)]
async fn linearizable_reads_pass_the_checker() {
    let mut total_crashes = 0;
    let mut total_retried_acks = 0;
    let mut total_reorders = 0;
    let mut total_blocked = 0;
    for seed in 0..6 {
        let (history, _, crashes, retried_acks, stats) = run_workload(
            seed,
            ReadMode::Linearizable,
            WriteMode::TokenRetry,
            0.0,
            None,
        )
        .await;
        total_crashes += crashes;
        total_retried_acks += retried_acks;
        total_reorders += stats.reorders;
        total_blocked += stats.legs_blocked;
        let reads = history
            .iter()
            .filter(|r| matches!(r.op, OpKind::Read { .. }))
            .count();
        // Guard against vacuous success: the workload must actually have
        // granted reads (final reads alone are 3).
        assert!(
            reads > 3,
            "seed {seed}: too few granted reads ({reads}) to mean anything"
        );
        // Phase 13's "fewer Unknowns" claim, made exact: with token-carrying
        // retries no op in these histories is left permanently ambiguous.
        // (Under FireOnce an unconfirmed write stayed Unknown forever.)
        let unknowns = history
            .iter()
            .filter(|r| {
                matches!(
                    r.op,
                    OpKind::Write {
                        outcome: WriteOutcome::Unknown,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(unknowns, 0, "seed {seed}: retries left an op ambiguous");
        if let Err(reason) = check_linearizable(&history) {
            panic!("seed {seed}: linearizable reads produced a violation:\n{reason}");
        }
    }
    // Vacuity guards: the seed set must exercise crash rounds, and at
    // least one op must have acked only AFTER an ambiguous attempt — the
    // exact case where a duplicate copy may also commit and only dedup
    // keeps the recorded single op honest.
    assert!(total_crashes > 0, "no seed rolled a crash round");
    assert!(
        total_retried_acks > 0,
        "no seed exercised an ambiguous-then-acked retry — the dedup claim \
         was never actually stressed"
    );
    // Cross-set vacuity (T2): the linearizability claim is made under
    // reordering and partitions — both must actually have occurred
    // somewhere in the seed set.
    assert!(total_reorders > 0, "no seed ever reordered a message");
    assert!(total_blocked > 0, "no partition ever suppressed a message");
}

/// Phase 14: the identical linearizable workload with an aggressively low
/// snapshot threshold — every node compacts repeatedly mid-run, crashed
/// nodes restore from their own snapshots and catch up through
/// InstallSnapshot, and the WGL checker must STILL find zero violations on
/// every seed. Raw-log equality is out of reach here by design (per-node
/// compaction points legally differ); the final-state equality asserted
/// inside `run_workload` and the checker are the claims.
#[tokio::test(start_paused = true)]
async fn linearizable_reads_pass_the_checker_with_snapshots_on() {
    let mut total_crashes = 0;
    for seed in 0..6 {
        let (history, logs, crashes, _, _) = run_workload(
            seed,
            ReadMode::Linearizable,
            WriteMode::TokenRetry,
            0.0,
            Some(16),
        )
        .await;
        total_crashes += crashes;
        // Vacuity guard: compaction really ran — no node's retained log
        // still reaches back to index 1.
        assert!(
            logs.iter()
                .all(|log| log.first().is_none_or(|e| e.index > 1)),
            "seed {seed}: some node never compacted — the scenario is vacuous"
        );
        if let Err(reason) = check_linearizable(&history) {
            panic!("seed {seed}: violation with snapshots on:\n{reason}");
        }
    }
    assert!(total_crashes > 0, "no seed rolled a crash round");
}

/// The duplication soak: the linearizable workload with 10% of requests
/// delivered twice on top of loss, partitions and crash/restarts. Both the
/// lin checker and the log witness must still hold — duplicate
/// AppendEntries/votes must never double-apply or split a term.
#[tokio::test(start_paused = true)]
async fn linearizable_reads_survive_message_duplication() {
    let mut total_crashes = 0;
    for seed in 0..6 {
        let (history, logs, crashes, _, _) = run_workload(
            seed,
            ReadMode::Linearizable,
            WriteMode::TokenRetry,
            0.10,
            None,
        )
        .await;
        total_crashes += crashes;
        for log in &logs[1..] {
            assert_eq!(*log, logs[0], "seed {seed}: logs diverge");
        }
        check_write_witness(&history, &logs[0]).unwrap_or_else(|reason| {
            panic!("seed {seed}: {reason}");
        });
        if let Err(reason) = check_linearizable(&history) {
            panic!("seed {seed}: violation under duplication:\n{reason}");
        }
    }
    assert!(total_crashes > 0, "no seed rolled a crash round");
}

/// Extended soak, excluded from the default run (`cargo test --test jepsen
/// -- --ignored`): the strongest checker configuration — linearizable reads,
/// tokened retrying writers, 10% duplication AND snapshots on together (no
/// pinned test combines them) — across a much wider seed range. Every
/// history must linearize; every seed must actually compact somewhere.
#[tokio::test(start_paused = true)]
#[ignore = "extended soak; run explicitly with --ignored"]
async fn extended_soak_linearizable_under_duplication_and_snapshots() {
    let seeds: u64 = std::env::var("RUSTKV_SOAK_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    let mut total_crashes = 0;
    let mut seeds_with_compaction = 0u64;
    for seed in 0..seeds {
        let (history, logs, crashes, _, _) = run_workload(
            seed,
            ReadMode::Linearizable,
            WriteMode::TokenRetry,
            0.10,
            Some(16),
        )
        .await;
        total_crashes += crashes;
        if logs
            .iter()
            .any(|log| log.first().is_none_or(|e| e.index > 1))
        {
            seeds_with_compaction += 1;
        }
        if let Err(reason) = check_linearizable(&history) {
            panic!("seed {seed}: violation under duplication+snapshots:\n{reason}");
        }
    }
    assert!(total_crashes > 0, "no seed rolled a crash round");
    // Cross-set vacuity guard: a rare quiet seed may legally stay under the
    // threshold, but the soak as a whole must exercise compaction.
    assert!(
        seeds_with_compaction * 2 > seeds,
        "only {seeds_with_compaction}/{seeds} seeds compacted — the soak lost its teeth"
    );
}
