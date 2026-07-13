//! The Raft node: leader election and log replication (§5.1–5.4).
//!
//! One node = one event-loop task that owns all consensus state — storage,
//! role, timers, replication bookkeeping — with no shared-state locking. It
//! communicates only via channels and the [`Transport`] trait:
//! - inbound RPCs arrive as [`Inbound`] values on the transport's channel;
//! - outbound RPCs are sent from short-lived spawned tasks that report
//!   replies back through an internal event channel (tagged with the term
//!   and log position they were sent with, so stale replies are harmless);
//! - client proposals come in through the control channel
//!   ([`RaftHandle::propose`]);
//! - observers (tests, the KV layer in phase 5) read a `watch` channel of
//!   [`Status`] snapshots.
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
//! Phase 5 additions: committed entries are applied, in order, to a
//! [`StateMachine`] (the KV map), and every accepted proposal carries a
//! `committed` notification that resolves once the entry commits (or
//! resolves `false` if a leadership change truncated it). A new leader
//! appends a no-op entry (§8) so prior-term entries — and therefore the
//! applied state after restarts — commit promptly without client traffic.
//! Deliberately basic Raft: no PreVote/CheckQuorum, no linearizable reads
//! (ReadIndex/leases), no batching cap on AppendEntries payloads.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep_until};

use super::rpc::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply, RpcRequest,
    RpcResponse,
};
use super::storage::Storage;
use super::transport::{Inbound, Transport, TransportError};
use super::types::{Command, HardState, LogEntry, LogIndex, NodeId, Term};
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
    /// Highest log index known committed (volatile; re-learned after restart).
    pub commit_index: LogIndex,
    pub last_log_index: LogIndex,
}

/// Why a proposal was not accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeError {
    /// This node is not the leader; retry against `leader_hint` if present.
    NotLeader { leader_hint: Option<NodeId> },
    /// The node has shut down.
    Shutdown,
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::NotLeader {
                leader_hint: Some(id),
            } => {
                write!(f, "not the leader; try node {id}")
            }
            ProposeError::NotLeader { leader_hint: None } => {
                write!(f, "not the leader, and no leader is known")
            }
            ProposeError::Shutdown => write!(f, "raft node has shut down"),
        }
    }
}

impl std::error::Error for ProposeError {}

/// Where committed commands land. Applied exactly once per log position, in
/// log order, on every node (leaders and followers alike).
pub trait StateMachine: Send + Sync + 'static {
    fn apply(&self, entry: &LogEntry);
}

/// A write accepted into the leader's log (durably appended, NOT committed).
#[derive(Debug)]
pub struct Proposal {
    pub term: Term,
    pub index: LogIndex,
    /// Resolves `true` once the entry is committed and applied on this node,
    /// `false` if it was truncated/replaced by another leader and can never
    /// commit as proposed. May never resolve while the node is cut off from
    /// a majority — callers own the timeout (that IS the CP behavior).
    pub committed: oneshot::Receiver<bool>,
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

    /// Submits a command to the replicated log. On success the entry is
    /// durably appended on the leader; await [`Proposal::committed`] to learn
    /// whether it commits.
    pub async fn propose(&self, command: Command) -> Result<Proposal, ProposeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.control
            .send(Control::Propose {
                command,
                reply: reply_tx,
            })
            .map_err(|_| ProposeError::Shutdown)?;
        reply_rx.await.map_err(|_| ProposeError::Shutdown)?
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
    Propose {
        command: Command,
        reply: oneshot::Sender<Result<Proposal, ProposeError>>,
    },
}

/// A proposal whose commit outcome is still unknown.
struct PendingProposal {
    term: Term,
    index: LogIndex,
    committed: oneshot::Sender<bool>,
}

/// Replies from outbound-RPC tasks, tagged with what was sent so stale or
/// reordered replies can be interpreted safely.
enum Event {
    VoteReply {
        sent_term: Term,
        from: NodeId,
        result: Result<RpcResponse, TransportError>,
    },
    AppendReply {
        sent_term: Term,
        from: NodeId,
        /// prev_log_index of the AppendEntries this reply answers.
        sent_prev_index: LogIndex,
        /// How many entries that AppendEntries carried.
        sent_entries: u64,
        result: Result<RpcResponse, TransportError>,
    },
}

enum Role {
    Follower,
    Candidate {
        votes: HashSet<NodeId>,
    },
    Leader {
        /// Next log index to send to each peer (§5.3).
        next_index: HashMap<NodeId, LogIndex>,
        /// Highest log index known replicated on each peer.
        match_index: HashMap<NodeId, LogIndex>,
    },
}

pub struct RaftNode<T: Transport + Clone> {
    config: RaftConfig,
    storage: Storage,
    transport: T,
    inbound: mpsc::UnboundedReceiver<Inbound>,
    state_machine: Arc<dyn StateMachine>,
    role: Role,
    leader_id: Option<NodeId>,
    /// Volatile; re-learned from the leader (or majority) after restart.
    commit_index: LogIndex,
    /// Everything up to here has been applied to the state machine.
    last_applied: LogIndex,
    /// Local proposals awaiting their commit outcome. Survives step-down
    /// (a deposed leader's entry may still commit under its successor).
    pending: Vec<PendingProposal>,
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
        state_machine: Arc<dyn StateMachine>,
    ) -> RaftHandle {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let hard_state = storage.hard_state();
        let (status_tx, status_rx) = watch::channel(Status {
            id: config.id,
            term: hard_state.current_term,
            role: RoleKind::Follower,
            leader_id: None,
            commit_index: 0,
            last_log_index: storage.last_index(),
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
            state_machine,
            role: Role::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            pending: Vec::new(),
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
                    Some(Control::Propose { command, reply }) => {
                        self.handle_propose(command, reply);
                    }
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

    // ---- client proposals ----

    fn handle_propose(
        &mut self,
        command: Command,
        reply: oneshot::Sender<Result<Proposal, ProposeError>>,
    ) {
        if !self.is_leader() {
            let _ = reply.send(Err(ProposeError::NotLeader {
                leader_hint: self.leader_id,
            }));
            return;
        }
        let term = self.current_term();
        let index = self.storage.last_index() + 1;
        self.storage
            .append(&[LogEntry {
                term,
                index,
                command,
            }])
            .expect("cannot persist proposal; fail-stop");
        tracing::info!(node = self.config.id, term, index, "proposal appended");
        let (committed_tx, committed_rx) = oneshot::channel();
        self.pending.push(PendingProposal {
            term,
            index,
            committed: committed_tx,
        });
        let _ = reply.send(Ok(Proposal {
            term,
            index,
            committed: committed_rx,
        }));
        // A single-node cluster commits immediately; otherwise replicate now.
        self.maybe_advance_commit();
        for &peer in &self.config.peers {
            self.send_append(peer);
        }
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

    /// Full §5.3 AppendEntries: consistency check, duplicate-tolerant entry
    /// processing with conflict truncation, and commit advancement.
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

        // Log-matching consistency check: we must hold the leader's prev
        // entry. If not, the leader backtracks and retries.
        if self.storage.term(args.prev_log_index) != Some(args.prev_log_term) {
            tracing::debug!(
                node = self.config.id,
                term,
                prev_log_index = args.prev_log_index,
                prev_log_term = args.prev_log_term,
                last_log_index = self.storage.last_index(),
                "append rejected: log mismatch"
            );
            return AppendEntriesReply {
                term,
                success: false,
            };
        }

        // Walk the entries: skip what we already hold (duplicate/reordered
        // delivery), truncate our suffix at the first conflict, then append
        // the rest. Committed entries can never conflict (§5.3 + §5.4) —
        // enforced fail-stop below.
        let mut append_from = None;
        for entry in &args.entries {
            match self.storage.term(entry.index) {
                Some(existing) if existing == entry.term => continue,
                Some(_) => {
                    assert!(
                        entry.index > self.commit_index,
                        "SAFETY VIOLATION: asked to truncate committed entry {}",
                        entry.index
                    );
                    self.storage
                        .truncate_from(entry.index)
                        .expect("cannot truncate conflicting entries; fail-stop");
                    tracing::info!(
                        node = self.config.id,
                        term,
                        from_index = entry.index,
                        "truncated conflicting log suffix"
                    );
                    // Any of our own proposals in the truncated suffix can
                    // now never commit as proposed — tell their waiters.
                    self.resolve_pending();
                    append_from = Some(entry.index);
                    break;
                }
                None => {
                    append_from = Some(entry.index);
                    break;
                }
            }
        }
        if let Some(first) = append_from {
            let offset = usize::try_from(first - args.prev_log_index - 1)
                .expect("entry offset fits in usize");
            self.storage
                .append(&args.entries[offset..])
                .expect("cannot append entries; fail-stop");
            tracing::debug!(
                node = self.config.id,
                term,
                from_index = first,
                count = args.entries.len() - offset,
                "appended entries from leader"
            );
        }

        // Commit only up to what this RPC verified matches the leader.
        let last_verified = args.prev_log_index + args.entries.len() as u64;
        let new_commit = args.leader_commit.min(last_verified);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            tracing::debug!(
                node = self.config.id,
                term,
                commit_index = new_commit,
                "commit advanced"
            );
            self.apply_committed();
        }

        AppendEntriesReply {
            term,
            success: true,
        }
    }

    // ---- applying committed entries ----

    /// Applies everything in (last_applied, commit_index] to the state
    /// machine, in log order, then settles proposal waiters.
    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = self
                .storage
                .entry(self.last_applied)
                .expect("committed entries exist in the log")
                .clone();
            self.state_machine.apply(&entry);
            tracing::debug!(
                node = self.config.id,
                index = entry.index,
                entry_term = entry.term,
                "applied"
            );
        }
        self.resolve_pending();
    }

    /// Settles proposal waiters: `true` once committed (and, via
    /// [`Self::apply_committed`]'s ordering, already applied locally),
    /// `false` if the entry was truncated/replaced and can never commit.
    fn resolve_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut self.pending);
        for p in pending {
            if self.storage.term(p.index) != Some(p.term) {
                let _ = p.committed.send(false);
            } else if self.commit_index >= p.index {
                let _ = p.committed.send(true);
            } else {
                self.pending.push(p);
            }
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
                sent_prev_index,
                sent_entries,
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
                    return;
                }
                if sent_term != self.current_term() {
                    return; // reply to an RPC from an earlier term of ours
                }
                let last_index = self.storage.last_index();
                let Role::Leader {
                    next_index,
                    match_index,
                } = &mut self.role
                else {
                    return;
                };
                let mut resend = false;
                if reply.success {
                    // The peer confirmed it matches us up to prev + sent.
                    // max() because replies can arrive reordered.
                    let confirmed = sent_prev_index + sent_entries;
                    let matched = match_index.entry(from).or_insert(0);
                    if confirmed > *matched {
                        *matched = confirmed;
                        next_index.insert(from, confirmed + 1);
                        // Keep pushing if the peer is still behind.
                        resend = confirmed < last_index;
                    }
                } else {
                    // §5.3 backtracking: the peer diverges at or before
                    // sent_prev_index; step below it and retry immediately.
                    // min() so stale rejections never undo progress.
                    let next = next_index.entry(from).or_insert(1);
                    *next = (*next).min(sent_prev_index.max(1));
                    tracing::debug!(
                        node = self.config.id,
                        term = sent_term,
                        peer = from,
                        next_index = *next,
                        "append rejected by peer; backtracking"
                    );
                    resend = true;
                }
                if reply.success {
                    self.maybe_advance_commit();
                }
                if resend {
                    self.send_append(from);
                }
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
        // next_index points at the pre-no-op tail so the no-op ships in the
        // very first heartbeat without a backtracking round-trip.
        let next = self.storage.last_index() + 1;
        self.role = Role::Leader {
            next_index: self.config.peers.iter().map(|&p| (p, next)).collect(),
            match_index: self.config.peers.iter().map(|&p| (p, 0)).collect(),
        };
        self.leader_id = Some(self.config.id);
        // §8: commit a no-op at the start of the term. Under the §5.4.2 rule
        // this is what lets prior-term entries (and thus the KV state after
        // a restart) commit promptly even with no client traffic.
        self.storage
            .append(&[LogEntry {
                term,
                index: next,
                command: Command::Noop,
            }])
            .expect("cannot persist leadership no-op; fail-stop");
        // A single-node cluster commits it immediately.
        self.maybe_advance_commit();
        // Assert authority immediately; also schedules the next heartbeat.
        self.on_heartbeat_tick();
    }

    // ---- leader replication ----

    fn on_heartbeat_tick(&mut self) {
        for &peer in &self.config.peers {
            self.send_append(peer);
        }
        self.next_heartbeat = Instant::now() + self.config.heartbeat_interval;
    }

    /// Sends `peer` everything from its next_index (an empty batch doubles
    /// as the heartbeat). TODO(batching): sends the whole tail in one RPC;
    /// fine while compaction is out of scope and logs stay small.
    fn send_append(&self, peer: NodeId) {
        let Role::Leader { next_index, .. } = &self.role else {
            return;
        };
        let next = next_index
            .get(&peer)
            .copied()
            .unwrap_or_else(|| self.storage.last_index() + 1);
        let prev_log_index = next - 1;
        let prev_log_term = self
            .storage
            .term(prev_log_index)
            .expect("next_index stays within log bounds");
        let term = self.current_term();
        let args = AppendEntriesArgs {
            term,
            leader_id: self.config.id,
            prev_log_index,
            prev_log_term,
            entries: self.storage.entries_from(next).to_vec(),
            leader_commit: self.commit_index,
        };
        let sent_entries = args.entries.len() as u64;
        let transport = self.transport.clone();
        let events = self.events_tx.clone();
        tokio::spawn(async move {
            let result = transport.send(peer, RpcRequest::AppendEntries(args)).await;
            let _ = events.send(Event::AppendReply {
                sent_term: term,
                from: peer,
                sent_prev_index: prev_log_index,
                sent_entries,
                result,
            });
        });
    }

    /// Advances commit_index to the highest index replicated on a majority,
    /// but only for entries of the current term (§5.4.2) — prior-term
    /// entries commit transitively.
    fn maybe_advance_commit(&mut self) {
        let Role::Leader { match_index, .. } = &self.role else {
            return;
        };
        let mut replicated: Vec<LogIndex> = self
            .config
            .peers
            .iter()
            .map(|p| match_index.get(p).copied().unwrap_or(0))
            .collect();
        // The leader trivially holds its own whole log.
        replicated.push(self.storage.last_index());
        replicated.sort_unstable();
        let candidate = replicated[replicated.len() - self.majority()];

        if candidate > self.commit_index
            && self.storage.term(candidate) == Some(self.current_term())
        {
            self.commit_index = candidate;
            tracing::info!(
                node = self.config.id,
                term = self.current_term(),
                commit_index = candidate,
                "commit advanced"
            );
            self.apply_committed();
        }
    }

    // ---- helpers ----

    fn current_term(&self) -> Term {
        self.storage.hard_state().current_term
    }

    fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }

    fn role_kind(&self) -> RoleKind {
        match self.role {
            Role::Follower => RoleKind::Follower,
            Role::Candidate { .. } => RoleKind::Candidate,
            Role::Leader { .. } => RoleKind::Leader,
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
            commit_index: self.commit_index,
            last_log_index: self.storage.last_index(),
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
