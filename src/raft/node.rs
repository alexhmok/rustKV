//! The Raft node: roles, terms, and leader election (§5.1–5.2, §5.4.1).
//!
//! One node = one event-loop task that owns all consensus state — storage,
//! role, timers — with no shared-state locking. It communicates only via
//! channels and the [`Transport`] trait:
//! - inbound RPCs arrive as [`Inbound`] values on the transport's channel;
//! - outbound RPCs are sent from short-lived spawned tasks that report
//!   replies back through an internal event channel (tagged with the term
//!   they were sent in, so stale replies are discarded);
//! - observers (tests now, the KV layer in phase 5) read a `watch` channel
//!   of [`Status`] snapshots.
//!
//! Determinism: the event loop uses `select! { biased; .. }` — tokio's
//! default randomized branch polling would make runs irreproducible. With a
//! fixed polling order, a seeded [`SplitMix64`] for election jitter, and the
//! simulated transport on a paused-time current-thread runtime, a scenario
//! is a pure function of its seeds.
//!
//! Storage errors are fail-stop: a node that cannot persist its state
//! panics (crashes) rather than continuing and risking a safety violation.
//!
//! Phase 3 scope: elections and empty-heartbeat AppendEntries only.
//! TODO(phase 4): replication state (next_index/match_index), entry
//! handling, commit advancement. TODO(phase 5): client command proposals.

use std::collections::HashSet;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep_until};

use super::rpc::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply, RpcRequest,
    RpcResponse,
};
use super::storage::Storage;
use super::transport::{Inbound, Transport, TransportError};
use super::types::{HardState, LogIndex, NodeId, Term};
use crate::rng::SplitMix64;

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub id: NodeId,
    /// The other cluster members. Fixed membership, from config (phase 7).
    pub peers: Vec<NodeId>,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    /// Seeds this node's election-timeout jitter; part of what makes a
    /// simulated scenario reproducible.
    pub timeout_seed: u64,
}

impl RaftConfig {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            peers,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            timeout_seed: id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleKind {
    Follower,
    Candidate,
    Leader,
}

/// A snapshot of a node's externally visible consensus state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    pub id: NodeId,
    pub term: Term,
    pub role: RoleKind,
    /// Who this node believes leads its current term (itself, if leader).
    pub leader_id: Option<NodeId>,
}

/// Handle to a running node.
pub struct RaftHandle {
    status: watch::Receiver<Status>,
    control: mpsc::UnboundedSender<Control>,
    task: JoinHandle<()>,
}

impl RaftHandle {
    pub fn status(&self) -> Status {
        *self.status.borrow()
    }

    pub fn watch(&self) -> watch::Receiver<Status> {
        self.status.clone()
    }

    /// Asks the node to stop cleanly (it finishes the current event first).
    pub fn shutdown(&self) {
        let _ = self.control.send(Control::Shutdown);
    }

    /// Kills the node mid-flight, dropping its transport inbox — the crash
    /// simulation used by tests. Restart by re-opening the same storage dir
    /// and spawning a fresh node.
    pub fn crash(&self) {
        self.task.abort();
    }
}

enum Control {
    Shutdown,
    // TODO(phase 5): Propose(Command, reply channel) for client writes.
}

/// Replies from outbound-RPC tasks, tagged with the term when sent.
enum Event {
    VoteReply {
        sent_term: Term,
        from: NodeId,
        result: Result<RpcResponse, TransportError>,
    },
    AppendReply {
        sent_term: Term,
        from: NodeId,
        result: Result<RpcResponse, TransportError>,
    },
}

enum Role {
    Follower,
    Candidate { votes: HashSet<NodeId> },
    // TODO(phase 4): Leader carries next_index/match_index per peer.
    Leader,
}

pub struct RaftNode<T: Transport + Clone> {
    config: RaftConfig,
    storage: Storage,
    transport: T,
    inbound: mpsc::UnboundedReceiver<Inbound>,
    role: Role,
    leader_id: Option<NodeId>,
    /// Volatile; rebuilt after restart. Advances in phase 4.
    commit_index: LogIndex,
    election_deadline: Instant,
    next_heartbeat: Instant,
    rng: SplitMix64,
    events_tx: mpsc::UnboundedSender<Event>,
    events_rx: mpsc::UnboundedReceiver<Event>,
    control_rx: mpsc::UnboundedReceiver<Control>,
    status_tx: watch::Sender<Status>,
}

impl<T: Transport + Clone> RaftNode<T> {
    /// Spawns the node's event loop and returns a handle to observe and
    /// control it.
    pub fn spawn(
        config: RaftConfig,
        storage: Storage,
        transport: T,
        inbound: mpsc::UnboundedReceiver<Inbound>,
    ) -> RaftHandle {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let hard_state = storage.hard_state();
        let (status_tx, status_rx) = watch::channel(Status {
            id: config.id,
            term: hard_state.current_term,
            role: RoleKind::Follower,
            leader_id: None,
        });
        tracing::info!(
            node = config.id,
            term = hard_state.current_term,
            voted_for = ?hard_state.voted_for,
            last_log_index = storage.last_index(),
            "raft node starting"
        );
        let rng = SplitMix64::new(config.timeout_seed);
        let node = RaftNode {
            config,
            storage,
            transport,
            inbound,
            role: Role::Follower,
            leader_id: None,
            commit_index: 0,
            election_deadline: Instant::now(),
            next_heartbeat: Instant::now(),
            rng,
            events_tx,
            events_rx,
            control_rx,
            status_tx,
        };
        let task = tokio::spawn(node.run());
        RaftHandle {
            status: status_rx,
            control: control_tx,
            task,
        }
    }

    async fn run(mut self) {
        self.reset_election_timer();
        loop {
            self.publish_status();
            tokio::select! {
                // biased: fixed polling order for reproducibility (see module docs).
                biased;

                ctl = self.control_rx.recv() => match ctl {
                    Some(Control::Shutdown) | None => break,
                },
                _ = sleep_until(self.election_deadline), if !self.is_leader() => {
                    self.on_election_timeout();
                }
                _ = sleep_until(self.next_heartbeat), if self.is_leader() => {
                    self.on_heartbeat_tick();
                }
                inbound = self.inbound.recv() => match inbound {
                    Some(inb) => self.handle_inbound(inb),
                    None => {
                        tracing::info!(node = self.config.id, "transport closed; stopping");
                        break;
                    }
                },
                // Never None: we hold an events_tx ourselves.
                event = self.events_rx.recv() => if let Some(ev) = event {
                    self.handle_event(ev);
                },
            }
        }
        self.publish_status();
        tracing::info!(
            node = self.config.id,
            term = self.current_term(),
            "raft node stopped"
        );
    }

    // ---- inbound RPCs ----

    fn handle_inbound(&mut self, inbound: Inbound) {
        let response = match inbound.request {
            RpcRequest::RequestVote(args) => {
                RpcResponse::RequestVote(self.handle_request_vote(args))
            }
            RpcRequest::AppendEntries(args) => {
                RpcResponse::AppendEntries(self.handle_append_entries(args))
            }
        };
        // The peer may have timed out and dropped the reply channel.
        let _ = inbound.reply.send(response);
    }

    /// §5.2 + the §5.4.1 election restriction.
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.current_term() {
            // Note: this resets our election timer even if the vote is then
            // refused — a slight liveness concession (a disruptive candidate
            // can delay us); PreVote would fix it and is out of scope.
            self.become_follower(args.term, None);
        }
        let term = self.current_term();
        if args.term < term {
            return RequestVoteReply {
                term,
                vote_granted: false,
            };
        }

        let hard_state = self.storage.hard_state();
        let can_vote =
            hard_state.voted_for.is_none() || hard_state.voted_for == Some(args.candidate_id);
        // §5.4.1: only vote for candidates whose log is at least as
        // up-to-date as ours (compare last terms, then last indexes).
        let log_up_to_date = (args.last_log_term, args.last_log_index)
            >= (self.storage.last_term(), self.storage.last_index());

        if can_vote && log_up_to_date {
            self.storage
                .save_hard_state(HardState {
                    current_term: term,
                    voted_for: Some(args.candidate_id),
                })
                .expect("cannot persist vote; fail-stop");
            self.reset_election_timer();
            tracing::info!(
                node = self.config.id,
                term,
                candidate = args.candidate_id,
                "vote granted"
            );
            RequestVoteReply {
                term,
                vote_granted: true,
            }
        } else {
            tracing::debug!(
                node = self.config.id,
                term,
                candidate = args.candidate_id,
                can_vote,
                log_up_to_date,
                "vote refused"
            );
            RequestVoteReply {
                term,
                vote_granted: false,
            }
        }
    }

    /// Phase 3: term handling + heartbeat recognition. The log consistency
    /// check is real but trivial while logs are empty.
    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let current = self.current_term();
        if args.term < current {
            return AppendEntriesReply {
                term: current,
                success: false,
            };
        }
        if args.term == current && self.is_leader() {
            // Two leaders in one term would break Raft's election safety.
            tracing::error!(
                node = self.config.id,
                term = current,
                other_leader = args.leader_id,
                "SAFETY VIOLATION: AppendEntries from another leader of our term"
            );
        }
        // AppendEntries at our term (or above) comes from the legitimate
        // leader of that term: adopt it and (re)become follower.
        self.become_follower(args.term, Some(args.leader_id));
        let term = self.current_term();

        let log_ok = self.storage.term(args.prev_log_index) == Some(args.prev_log_term);
        // TODO(phase 4): append/truncate entries, advance commit_index.
        AppendEntriesReply {
            term,
            success: log_ok,
        }
    }

    // ---- outbound-RPC replies ----

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::VoteReply {
                sent_term,
                from,
                result,
            } => {
                let reply = match result {
                    Ok(RpcResponse::RequestVote(reply)) => reply,
                    Ok(other) => {
                        tracing::warn!(
                            node = self.config.id,
                            from,
                            ?other,
                            "mismatched vote reply"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::trace!(node = self.config.id, from, %error, "vote rpc failed");
                        return;
                    }
                };
                if reply.term > self.current_term() {
                    self.become_follower(reply.term, None);
                    return;
                }
                if sent_term != self.current_term() || !reply.vote_granted {
                    return;
                }
                if let Role::Candidate { votes } = &mut self.role {
                    votes.insert(from);
                    tracing::debug!(
                        node = self.config.id,
                        term = sent_term,
                        from,
                        votes = votes.len(),
                        "vote received"
                    );
                    self.maybe_become_leader();
                }
            }
            Event::AppendReply {
                sent_term,
                from,
                result,
            } => {
                let reply = match result {
                    Ok(RpcResponse::AppendEntries(reply)) => reply,
                    Ok(other) => {
                        tracing::warn!(
                            node = self.config.id,
                            from,
                            ?other,
                            "mismatched append reply"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::trace!(node = self.config.id, from, %error, "append rpc failed");
                        return;
                    }
                };
                if reply.term > self.current_term() {
                    self.become_follower(reply.term, None);
                }
                // TODO(phase 4): use sent_term/from/success to drive
                // next_index backtracking and commit advancement.
                let _ = sent_term;
            }
        }
    }

    // ---- role transitions ----

    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        let old = self.storage.hard_state();
        debug_assert!(term >= old.current_term, "terms never move backwards");
        if term > old.current_term {
            self.storage
                .save_hard_state(HardState {
                    current_term: term,
                    voted_for: None,
                })
                .expect("cannot persist term; fail-stop");
        }
        let was = self.role_kind();
        self.role = Role::Follower;
        self.leader_id = leader_id;
        // Also resets when stepping down, so a deposed leader/candidate
        // waits a full randomized timeout before running again.
        self.reset_election_timer();
        if was != RoleKind::Follower || term > old.current_term {
            tracing::info!(node = self.config.id, term, from_role = ?was, ?leader_id, "became follower");
        }
    }

    fn on_election_timeout(&mut self) {
        let term = self.current_term() + 1;
        self.storage
            .save_hard_state(HardState {
                current_term: term,
                voted_for: Some(self.config.id),
            })
            .expect("cannot persist candidacy; fail-stop");
        self.role = Role::Candidate {
            votes: HashSet::from([self.config.id]),
        };
        self.leader_id = None;
        self.reset_election_timer();
        tracing::info!(
            node = self.config.id,
            term,
            "election timeout; starting election"
        );

        let args = RequestVoteArgs {
            term,
            candidate_id: self.config.id,
            last_log_index: self.storage.last_index(),
            last_log_term: self.storage.last_term(),
        };
        for &peer in &self.config.peers {
            let transport = self.transport.clone();
            let events = self.events_tx.clone();
            let args = args.clone();
            tokio::spawn(async move {
                let result = transport.send(peer, RpcRequest::RequestVote(args)).await;
                let _ = events.send(Event::VoteReply {
                    sent_term: term,
                    from: peer,
                    result,
                });
            });
        }
        // A single-node cluster wins its election immediately.
        self.maybe_become_leader();
    }

    fn maybe_become_leader(&mut self) {
        let votes = match &self.role {
            Role::Candidate { votes } => votes.len(),
            _ => return,
        };
        if votes < self.majority() {
            return;
        }
        let term = self.current_term();
        tracing::info!(node = self.config.id, term, votes, "became leader");
        self.role = Role::Leader;
        self.leader_id = Some(self.config.id);
        // TODO(phase 4): initialize next_index/match_index here.
        // Assert authority immediately; also schedules the next heartbeat.
        self.on_heartbeat_tick();
    }

    fn on_heartbeat_tick(&mut self) {
        let term = self.current_term();
        let args = AppendEntriesArgs {
            term,
            leader_id: self.config.id,
            prev_log_index: self.storage.last_index(),
            prev_log_term: self.storage.last_term(),
            entries: Vec::new(),
            leader_commit: self.commit_index,
        };
        for &peer in &self.config.peers {
            let transport = self.transport.clone();
            let events = self.events_tx.clone();
            let args = args.clone();
            tokio::spawn(async move {
                let result = transport.send(peer, RpcRequest::AppendEntries(args)).await;
                let _ = events.send(Event::AppendReply {
                    sent_term: term,
                    from: peer,
                    result,
                });
            });
        }
        self.next_heartbeat = Instant::now() + self.config.heartbeat_interval;
    }

    // ---- helpers ----

    fn current_term(&self) -> Term {
        self.storage.hard_state().current_term
    }

    fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader)
    }

    fn role_kind(&self) -> RoleKind {
        match self.role {
            Role::Follower => RoleKind::Follower,
            Role::Candidate { .. } => RoleKind::Candidate,
            Role::Leader => RoleKind::Leader,
        }
    }

    /// Votes needed to win: strict majority of the full cluster.
    fn majority(&self) -> usize {
        let cluster_size = self.config.peers.len() + 1;
        cluster_size / 2 + 1
    }

    fn reset_election_timer(&mut self) {
        let min = u64::try_from(self.config.election_timeout_min.as_micros())
            .expect("election timeout fits in u64 µs");
        let max = u64::try_from(self.config.election_timeout_max.as_micros())
            .expect("election timeout fits in u64 µs");
        assert!(min <= max, "election_timeout_min > election_timeout_max");
        self.election_deadline =
            Instant::now() + Duration::from_micros(self.rng.next_range(min..=max));
    }

    fn publish_status(&self) {
        let status = Status {
            id: self.config.id,
            term: self.current_term(),
            role: self.role_kind(),
            leader_id: self.leader_id,
        };
        self.status_tx.send_if_modified(|current| {
            if *current == status {
                false
            } else {
                *current = status;
                true
            }
        });
    }
}
