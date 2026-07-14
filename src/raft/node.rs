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
//!
//! Phase 9: linearizable reads via ReadIndex (§6.4), with nothing new on the
//! wire. Each outbound AppendEntries is tagged with a local monotonic
//! sequence number; a read registered at seq `s` is leadership-confirmed
//! once a majority has answered an AppendEntries sent at seq >= `s` (any
//! reply at our term counts — even a log-mismatch rejection acknowledges our
//! authority). The read's index is `max(commit_index, term_start_index)` so
//! a fresh leader never serves before its no-op commits, and the ticket
//! resolves only once `last_applied` reaches it. Losing leadership drops all
//! pending read tickets (waiters get a retryable error, never a stale value).
//!
//! Phase 11: PreVote (§9.6 / thesis §4.2.3). An election timeout no longer
//! bumps the term; it starts a *pre-campaign* ([`Role::PreCandidate`]) that
//! probes peers with the prospective term `current_term + 1` — persisting
//! nothing, and leaving the node's own term untouched. Only a pre-vote
//! majority triggers the real election (term bump + durable self-vote).
//! Grantors apply the same §5.4.1 log check as a real vote plus *leader
//! stickiness*: a node that is the leader, or heard from a valid one within
//! `election_timeout_min`, denies the probe. Together these stop a healed
//! or partitioned node from ever disrupting a healthy leader — the term
//! churn phase 3 documented as expected is gone.
//!
//! Phase 14: snapshotting/log compaction + InstallSnapshot (§7, single-shot
//! — no chunking). After applying, once `last_applied` runs
//! `snapshot_threshold` entries past the snapshot boundary, the node
//! captures the state machine ([`StateMachine::snapshot`]) and compacts the
//! applied prefix — always at `last_applied`, never `commit_index`
//! (committed-but-unapplied entries are not in the state yet). The trigger
//! counts applied entries, so with a fixed threshold it is deterministic by
//! construction; `None` (the default) turns the feature off entirely. A
//! leader whose peer needs compacted entries (`next_index` at or below the
//! boundary) sends InstallSnapshot instead of AppendEntries; the follower
//! persists it (fsync before replying), restores its state machine, and
//! no-ops duplicates whose boundary it already committed. An InstallSnapshot
//! reply counts as CheckQuorum contact (it IS a reply at our term) but never
//! as a ReadIndex ack — leadership confirmation stays AppendEntries-seq-
//! tagged only.
//!
//! Phase 12: CheckQuorum (§6.2 leases, minus the read-lease half), PreVote's
//! matched pair. A leader that hasn't heard from a majority (itself plus
//! peers answering AppendEntries, tracked at the same site as `acked_seq`)
//! within `election_timeout_max` steps down at its CURRENT term — no bump —
//! at the next heartbeat tick, before sending. Its silence lets the
//! reachable side's stickiness expire and elect normally, restoring the
//! liveness under asymmetric partitions that PreVote's stickiness had
//! suppressed (a leader deaf to all acks used to stall the cluster forever:
//! followers kept hearing heartbeats, so nobody ever campaigned).
//! Deliberately basic Raft still: no batching cap on AppendEntries payloads.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep_until};

use super::rpc::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply, RpcRequest, RpcResponse,
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
    /// Compact the log once `last_applied` runs this many entries past the
    /// snapshot boundary (phase 14); must be >= 1. `None` (the default)
    /// disables snapshotting entirely — nothing is written, nothing changes
    /// on the wire.
    pub snapshot_threshold: Option<u64>,
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
            snapshot_threshold: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleKind {
    Follower,
    /// Probing for a pre-vote majority (§9.6); still at its old term.
    PreCandidate,
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

    /// The complete current state as an opaque JSON value (phase 14): the
    /// snapshot payload for compaction. Must capture everything a replay of
    /// the applied prefix would have produced — including bookkeeping like
    /// the dedup sessions table, not just user data.
    ///
    /// NOTE: `KvStore` also has an *inherent* map-only `snapshot()` used by
    /// tests; concrete calls resolve to that one, `dyn StateMachine` calls
    /// to this one (pinned by a unit test in store.rs).
    fn snapshot(&self) -> serde_json::Value;

    /// Replaces the entire state with a previously captured [`Self::snapshot`]
    /// (restore-at-boot and InstallSnapshot). A malformed payload is
    /// fail-stop territory — it only arrives via committed snapshots.
    fn restore(&self, state: &serde_json::Value);
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

/// A linearizable read in progress (§6.4 ReadIndex).
#[derive(Debug)]
pub struct ReadTicket {
    /// Resolves once this node has (a) confirmed it is still leader by
    /// hearing from a majority after the read was registered and (b) applied
    /// everything the read must reflect; the local state machine is then
    /// safe to read. If the node loses leadership first the sender is
    /// dropped and awaiting returns an error — retry against the new leader.
    /// May never resolve while the node is cut off from a majority — callers
    /// own the timeout (the same CP behavior as writes).
    pub granted: oneshot::Receiver<()>,
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

    /// Registers a linearizable read (§6.4 ReadIndex). Leader-only, like
    /// [`Self::propose`]. On success, await [`ReadTicket::granted`] before
    /// reading the local state machine.
    pub async fn read(&self) -> Result<ReadTicket, ProposeError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.control
            .send(Control::Read { reply: reply_tx })
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
    Read {
        reply: oneshot::Sender<Result<ReadTicket, ProposeError>>,
    },
}

/// A proposal whose commit outcome is still unknown.
struct PendingProposal {
    term: Term,
    index: LogIndex,
    committed: oneshot::Sender<bool>,
}

/// A registered read awaiting leadership confirmation + apply (§6.4).
struct PendingRead {
    /// Confirmed once a majority has answered an AppendEntries sent at
    /// seq >= this.
    needed_seq: u64,
    /// The state the read must reflect; grant only once last_applied
    /// reaches it. Captured once at registration.
    read_index: LogIndex,
    granted: oneshot::Sender<()>,
}

/// Replies from outbound-RPC tasks, tagged with what was sent so stale or
/// reordered replies can be interpreted safely.
// The shared `Reply` postfix is the point: every event IS a reply.
#[allow(clippy::enum_variant_names)]
enum Event {
    PreVoteReply {
        /// The *prospective* term the probe asked for (`current_term + 1`
        /// at send time) — a term this node has not adopted.
        sent_term: Term,
        from: NodeId,
        result: Result<RpcResponse, TransportError>,
    },
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
        /// The leader's heartbeat_seq when this AppendEntries was sent
        /// (ReadIndex leadership confirmation, §6.4).
        sent_seq: u64,
        result: Result<RpcResponse, TransportError>,
    },
    InstallSnapshotReply {
        sent_term: Term,
        from: NodeId,
        /// The boundary of the snapshot this reply answers: on success the
        /// follower holds everything through it.
        last_included_index: LogIndex,
        result: Result<RpcResponse, TransportError>,
    },
}

enum Role {
    Follower,
    /// Pre-campaigning (§9.6): counting non-binding pre-votes for the
    /// prospective term `current_term + 1`. Nothing is persisted and the
    /// node's own term does not move until the pre-vote majority arrives.
    PreCandidate {
        votes: HashSet<NodeId>,
    },
    Candidate {
        votes: HashSet<NodeId>,
    },
    Leader {
        /// Next log index to send to each peer (§5.3).
        next_index: HashMap<NodeId, LogIndex>,
        /// Highest log index known replicated on each peer.
        match_index: HashMap<NodeId, LogIndex>,
        /// Index of this term's leadership no-op (§8). Reads never serve
        /// below it (§6.4): a fresh leader doesn't yet know how far its
        /// predecessor committed.
        term_start_index: LogIndex,
        /// Monotonic tag on outbound AppendEntries within this leadership;
        /// bumped when a read registers so later acks prove later authority.
        heartbeat_seq: u64,
        /// Highest sent_seq each peer has answered (at our term).
        acked_seq: HashMap<NodeId, u64>,
        /// When each peer last answered an AppendEntries at our term —
        /// success or rejection, either is contact (CheckQuorum, phase 12).
        /// Initialized to leadership start so a fresh leader isn't deposed
        /// before its first acks can possibly arrive.
        last_contact: HashMap<NodeId, Instant>,
        /// Reads awaiting confirmation. Deliberately inside the role: losing
        /// leadership drops them, resolving every ticket with an error —
        /// unlike `pending` proposals, which survive step-down.
        pending_reads: Vec<PendingRead>,
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
    /// When the last valid AppendEntries (current-or-higher term) arrived.
    /// Leader stickiness: pre-votes are denied while this is fresher than
    /// `election_timeout_min`. Volatile — a restarted node grants again.
    last_leader_contact: Option<Instant>,
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
        // Restore-at-boot (phase 14): everything through the snapshot
        // boundary is committed and applied by definition — hand the state
        // machine its snapshot BEFORE any of the retained log replays, and
        // start commit_index/last_applied at the boundary (0 without a
        // snapshot, exactly as before). This is the single chokepoint: the
        // binary and every test restart path come through here.
        let snapshot_index = storage.snapshot_index();
        if let Some(snapshot) = storage.snapshot() {
            state_machine.restore(&snapshot.state);
            tracing::info!(
                node = config.id,
                last_included_index = snapshot.last_included_index,
                last_included_term = snapshot.last_included_term,
                "state machine restored from snapshot"
            );
        }
        let (status_tx, status_rx) = watch::channel(Status {
            id: config.id,
            term: hard_state.current_term,
            role: RoleKind::Follower,
            leader_id: None,
            commit_index: snapshot_index,
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
            commit_index: snapshot_index,
            last_applied: snapshot_index,
            pending: Vec::new(),
            last_leader_contact: None,
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
                    Some(Control::Read { reply }) => {
                        self.handle_read(reply);
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

    /// Registers a linearizable read (§6.4 ReadIndex).
    fn handle_read(&mut self, reply: oneshot::Sender<Result<ReadTicket, ProposeError>>) {
        let commit_index = self.commit_index;
        let Role::Leader {
            term_start_index,
            heartbeat_seq,
            pending_reads,
            ..
        } = &mut self.role
        else {
            let _ = reply.send(Err(ProposeError::NotLeader {
                leader_hint: self.leader_id,
            }));
            return;
        };
        // §6.4: never serve below this term's no-op.
        let read_index = commit_index.max(*term_start_index);
        // Bump before broadcasting: an ack only proves authority as of the
        // seq its RPC was sent with, so this read needs post-bump acks.
        *heartbeat_seq += 1;
        let needed_seq = *heartbeat_seq;
        let (granted_tx, granted_rx) = oneshot::channel();
        pending_reads.push(PendingRead {
            needed_seq,
            read_index,
            granted: granted_tx,
        });
        let _ = reply.send(Ok(ReadTicket {
            granted: granted_rx,
        }));
        tracing::debug!(
            node = self.config.id,
            term = self.current_term(),
            read_index,
            needed_seq,
            "linearizable read registered"
        );
        for &peer in &self.config.peers {
            self.send_append(peer);
        }
        // A single-node cluster is its own majority; grant immediately.
        self.resolve_reads();
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
            RpcRequest::PreVote(args) => RpcResponse::PreVote(self.handle_pre_vote(args)),
            RpcRequest::InstallSnapshot(args) => {
                RpcResponse::InstallSnapshot(self.handle_install_snapshot(args))
            }
        };
        // The peer may have timed out and dropped the reply channel.
        let _ = inbound.reply.send(response);
    }

    /// §5.2 + the §5.4.1 election restriction. Deliberately unchanged by
    /// PreVote: real votes are not sticky and still adopt higher terms.
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.current_term() {
            // Note: this resets our election timer even if the vote is then
            // refused — a liveness concession that PreVote (phase 11) makes
            // mostly moot: a candidate only reaches here after winning a
            // pre-vote round, which stickiness denies while a live leader
            // exists.
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

    /// PreVote (§9.6): "would you vote for me for `args.term`?". Grant only
    /// if the prospective term is beyond ours, the candidate's log passes
    /// the same §5.4.1 check as a real vote, and we have no reason to
    /// believe a valid leader exists (leader stickiness). Unlike a real
    /// vote this persists nothing, adopts no term, resets no election
    /// timer, and may be granted to any number of askers.
    fn handle_pre_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let term = self.current_term();
        let log_up_to_date = (args.last_log_term, args.last_log_index)
            >= (self.storage.last_term(), self.storage.last_index());
        // Stickiness is what stops an up-to-date healed node from
        // disrupting: the leader itself always denies (it IS the valid
        // leader), and everyone it reaches within election_timeout_min
        // denies too, so the disruptor can never assemble a majority.
        let leader_is_live = self.is_leader()
            || self
                .last_leader_contact
                .is_some_and(|at| at.elapsed() < self.config.election_timeout_min);
        let grant = args.term > term && log_up_to_date && !leader_is_live;
        tracing::debug!(
            node = self.config.id,
            term,
            candidate = args.candidate_id,
            prospective_term = args.term,
            grant,
            log_up_to_date,
            leader_is_live,
            "pre-vote"
        );
        RequestVoteReply {
            term,
            vote_granted: grant,
        }
    }

    /// InstallSnapshot (§7, phase 14): replaces our compacted-away past with
    /// the leader's snapshot. Persisted (fsynced) before replying, like every
    /// other RPC-visible state change.
    fn handle_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        let current = self.current_term();
        if args.term < current {
            return InstallSnapshotReply { term: current };
        }
        if args.term == current && self.is_leader() {
            tracing::error!(
                node = self.config.id,
                term = current,
                other_leader = args.leader_id,
                "SAFETY VIOLATION: InstallSnapshot from another leader of our term"
            );
        }
        self.become_follower(args.term, Some(args.leader_id));
        // Leader stickiness (§9.6): a snapshot is contact with a valid leader.
        self.last_leader_contact = Some(Instant::now());
        let term = self.current_term();
        let boundary = args.snapshot.last_included_index;

        // Idempotence guard: everything through the boundary is already
        // committed here, so re-installing (a duplicated or reordered
        // delivery — phase 10's standing fault) would rewind nothing and
        // rewrite disk for no reason. Success as a no-op: the reply's
        // meaning ("I hold everything through the boundary") is true.
        if boundary <= self.commit_index {
            tracing::debug!(
                node = self.config.id,
                term,
                last_included_index = boundary,
                commit_index = self.commit_index,
                "duplicate InstallSnapshot ignored"
            );
            return InstallSnapshotReply { term };
        }

        self.storage
            .install_snapshot(&args.snapshot)
            .expect("cannot persist snapshot; fail-stop");
        self.state_machine.restore(&args.snapshot.state);
        self.commit_index = boundary;
        self.last_applied = boundary;
        // Local proposals at or below the boundary are now unverifiable: the
        // compacted history may or may not hold them (their terms are gone).
        // Drop their senders — waiters get the retryable "unknown" error,
        // never a false "definitely didn't commit" (the entry may well be IN
        // the snapshot we just applied).
        self.pending.retain(|p| p.index > boundary);
        // Anything retained beyond the boundary resolves normally: if the
        // suffix was cleared (divergent), those entries can never commit.
        self.resolve_pending();
        tracing::info!(
            node = self.config.id,
            term,
            last_included_index = boundary,
            last_included_term = args.snapshot.last_included_term,
            from = args.leader_id,
            "installed snapshot from leader"
        );
        InstallSnapshotReply { term }
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
        // Leader stickiness (§9.6): even a log-mismatch rejection below is
        // contact with a valid leader — pre-votes are denied while fresh.
        self.last_leader_contact = Some(Instant::now());
        let term = self.current_term();

        // Log-matching consistency check: we must hold the leader's prev
        // entry. If not, the leader backtracks and retries. An index below
        // our snapshot boundary vacuously matches: compacted means committed,
        // and the leader of our (adopted) term holds every committed entry
        // (Leader Completeness) — without this, a follower that compacted
        // ahead of the leader's bookkeeping would reject probes forever.
        let prev_matches = match self.storage.term(args.prev_log_index) {
            Some(term) => term == args.prev_log_term,
            None => args.prev_log_index < self.storage.snapshot_index(),
        };
        if !prev_matches {
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
            if entry.index <= self.storage.snapshot_index() {
                // Compacted ⇒ committed ⇒ identical to what we applied; the
                // same vacuous-match argument as the prev check above.
                continue;
            }
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
        // last_applied advanced — pending reads may now be servable.
        self.resolve_reads();
        // ...and the applied prefix may have outgrown the snapshot threshold.
        // After resolve_pending, so nothing pending ever sits below the new
        // boundary (its outcome was decidable before the terms vanished).
        self.maybe_compact();
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

    /// Grants every pending read that is both leadership-confirmed (a
    /// majority answered an AppendEntries sent at seq >= needed_seq) and
    /// applied (last_applied >= read_index). No-op on non-leaders.
    fn resolve_reads(&mut self) {
        let majority = self.majority();
        let last_applied = self.last_applied;
        let Role::Leader {
            acked_seq,
            pending_reads,
            ..
        } = &mut self.role
        else {
            return;
        };
        if pending_reads.is_empty() {
            return;
        }
        let reads = std::mem::take(pending_reads);
        for read in reads {
            // Count ourselves: the leader trivially acknowledges itself.
            let acks = 1 + acked_seq
                .values()
                .filter(|&&seq| seq >= read.needed_seq)
                .count();
            if acks >= majority && last_applied >= read.read_index {
                let _ = read.granted.send(());
            } else {
                pending_reads.push(read);
            }
        }
    }

    // ---- outbound-RPC replies ----

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::PreVoteReply {
                sent_term,
                from,
                result,
            } => {
                let reply = match result {
                    Ok(RpcResponse::PreVote(reply)) => reply,
                    Ok(other) => {
                        tracing::warn!(
                            node = self.config.id,
                            from,
                            ?other,
                            "mismatched pre-vote reply"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::trace!(node = self.config.id, from, %error, "pre-vote rpc failed");
                        return;
                    }
                };
                if reply.term > self.current_term() {
                    // A denial carrying a newer term: adopt it (this is how
                    // a pre-candidate whose term fell behind catches up and
                    // becomes eligible for grants next timeout).
                    self.become_follower(reply.term, None);
                    return;
                }
                // Count only grants for THIS pre-campaign: the prospective
                // term must still be one beyond our (unmoved) current term.
                if sent_term != self.current_term() + 1 || !reply.vote_granted {
                    return;
                }
                if let Role::PreCandidate { votes } = &mut self.role {
                    votes.insert(from);
                    tracing::debug!(
                        node = self.config.id,
                        prospective_term = sent_term,
                        from,
                        votes = votes.len(),
                        "pre-vote received"
                    );
                    self.maybe_start_election();
                }
            }
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
                sent_seq,
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
                    acked_seq,
                    last_contact,
                    ..
                } = &mut self.role
                else {
                    return;
                };
                // Any reply at our term — success or log-mismatch rejection —
                // acknowledges our authority as of this RPC's send (§6.4),
                // and is contact for CheckQuorum (never derived from
                // match_index: a rejecting peer is still reachable).
                let acked = acked_seq.entry(from).or_insert(0);
                *acked = (*acked).max(sent_seq);
                last_contact.insert(from, Instant::now());
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
                // Acks advanced even if commit didn't — reads may confirm.
                self.resolve_reads();
            }
            Event::InstallSnapshotReply {
                sent_term,
                from,
                last_included_index,
                result,
            } => {
                let reply = match result {
                    Ok(RpcResponse::InstallSnapshot(reply)) => reply,
                    Ok(other) => {
                        tracing::warn!(
                            node = self.config.id,
                            from,
                            ?other,
                            "mismatched install-snapshot reply"
                        );
                        return;
                    }
                    Err(error) => {
                        tracing::trace!(
                            node = self.config.id, from, %error,
                            "install-snapshot rpc failed"
                        );
                        return;
                    }
                };
                if reply.term > self.current_term() {
                    self.become_follower(reply.term, None);
                    return;
                }
                if sent_term != self.current_term() {
                    return;
                }
                let last_index = self.storage.last_index();
                let Role::Leader {
                    next_index,
                    match_index,
                    last_contact,
                    ..
                } = &mut self.role
                else {
                    return;
                };
                // CheckQuorum: a snapshot reply at our term IS contact. But
                // deliberately NOT an acked_seq entry — ReadIndex leadership
                // confirmation stays AppendEntries-seq-tagged only, so a
                // snapshot-fed peer never confirms a read it didn't ack.
                last_contact.insert(from, Instant::now());
                // The follower now holds everything through the boundary
                // (including the duplicate case, where it already did).
                let matched = match_index.entry(from).or_insert(0);
                *matched = (*matched).max(last_included_index);
                let next = next_index.entry(from).or_insert(1);
                *next = (*next).max(last_included_index + 1);
                tracing::info!(
                    node = self.config.id,
                    term = sent_term,
                    peer = from,
                    last_included_index,
                    "snapshot installed on peer; resuming log replication"
                );
                self.maybe_advance_commit();
                if last_included_index < last_index {
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
        if let Role::Leader { pending_reads, .. } = &self.role
            && !pending_reads.is_empty()
        {
            tracing::debug!(
                node = self.config.id,
                term,
                failed_reads = pending_reads.len(),
                "stepping down; pending linearizable reads resolve as retryable errors"
            );
        }
        // Replacing the role drops any pending reads' senders — their
        // waiters get an error, never a hang or a stale value.
        self.role = Role::Follower;
        self.leader_id = leader_id;
        // Also resets when stepping down, so a deposed leader/candidate
        // waits a full randomized timeout before running again.
        self.reset_election_timer();
        if was != RoleKind::Follower || term > old.current_term {
            tracing::info!(node = self.config.id, term, from_role = ?was, ?leader_id, "became follower");
        }
    }

    /// Election timeout → pre-campaign (§9.6): probe for a pre-vote
    /// majority at the prospective term `current_term + 1`. Nothing is
    /// persisted and our own term does not move; only a majority of grants
    /// starts the real election.
    fn on_election_timeout(&mut self) {
        let prospective = self.current_term() + 1;
        self.role = Role::PreCandidate {
            votes: HashSet::from([self.config.id]),
        };
        self.leader_id = None;
        // Re-arm so a failed pre-campaign retries; one RNG draw per timeout.
        self.reset_election_timer();
        tracing::info!(
            node = self.config.id,
            term = self.current_term(),
            prospective_term = prospective,
            "election timeout; starting pre-campaign"
        );

        let args = RequestVoteArgs {
            term: prospective,
            candidate_id: self.config.id,
            last_log_index: self.storage.last_index(),
            last_log_term: self.storage.last_term(),
        };
        for &peer in &self.config.peers {
            let transport = self.transport.clone();
            let events = self.events_tx.clone();
            let args = args.clone();
            tokio::spawn(async move {
                let result = transport.send(peer, RpcRequest::PreVote(args)).await;
                let _ = events.send(Event::PreVoteReply {
                    sent_term: prospective,
                    from: peer,
                    result,
                });
            });
        }
        // A single-node cluster is its own pre-vote majority.
        self.maybe_start_election();
    }

    /// Promotes a pre-candidate holding a pre-vote majority to a real
    /// candidacy. No-op otherwise.
    fn maybe_start_election(&mut self) {
        let votes = match &self.role {
            Role::PreCandidate { votes } => votes.len(),
            _ => return,
        };
        if votes >= self.majority() {
            self.start_election();
        }
    }

    /// The real election (§5.2): durably bump the term with a self-vote and
    /// solicit binding votes. Reached only through a pre-vote majority.
    fn start_election(&mut self) {
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
            "pre-vote majority; starting election"
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
        let now = Instant::now();
        self.role = Role::Leader {
            next_index: self.config.peers.iter().map(|&p| (p, next)).collect(),
            match_index: self.config.peers.iter().map(|&p| (p, 0)).collect(),
            term_start_index: next,
            heartbeat_seq: 0,
            acked_seq: HashMap::new(),
            last_contact: self.config.peers.iter().map(|&p| (p, now)).collect(),
            pending_reads: Vec::new(),
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
        // CheckQuorum (phase 12), before sending: a leader that hasn't heard
        // from a majority — itself plus peers whose last AppendEntries reply
        // is within election_timeout_max — steps down at its CURRENT term
        // (a bump would just re-elect us into the same silence) and goes
        // quiet, letting the reachable side's stickiness expire and elect.
        // Piggybacked on the heartbeat tick: no new timer, no RNG draws.
        if let Role::Leader { last_contact, .. } = &self.role {
            let window = self.config.election_timeout_max;
            let heard = 1 + last_contact
                .values()
                .filter(|at| at.elapsed() < window)
                .count();
            if heard < self.majority() {
                let term = self.current_term();
                tracing::info!(
                    node = self.config.id,
                    term,
                    heard,
                    majority = self.majority(),
                    "check-quorum failed: no majority heard within election_timeout_max; \
                     stepping down"
                );
                self.become_follower(term, None);
                return;
            }
        }
        for &peer in &self.config.peers {
            self.send_append(peer);
        }
        self.next_heartbeat = Instant::now() + self.config.heartbeat_interval;
    }

    /// Sends `peer` everything from its next_index (an empty batch doubles
    /// as the heartbeat). TODO(batching): sends the whole tail in one RPC;
    /// fine while compaction is out of scope and logs stay small.
    fn send_append(&self, peer: NodeId) {
        let Role::Leader {
            next_index,
            heartbeat_seq,
            ..
        } = &self.role
        else {
            return;
        };
        let sent_seq = *heartbeat_seq;
        let next = next_index
            .get(&peer)
            .copied()
            .unwrap_or_else(|| self.storage.last_index() + 1);
        // The peer needs entries we compacted away (phase 14): only a
        // snapshot can catch it up. Checked BEFORE the prev_log_term lookup
        // below, which cannot answer at or below the boundary. Re-sent at
        // heartbeat pace while the peer lags — no rate limiting (documented
        // gap); duplicates are no-ops on the follower.
        if next <= self.storage.snapshot_index() {
            self.send_install_snapshot(peer);
            return;
        }
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
                sent_seq,
                result,
            });
        });
    }

    /// Ships the current snapshot to a peer whose next_index fell at or
    /// below the snapshot boundary. The payload is storage's in-memory copy
    /// (small by scope; replaced — never stale — on each compaction).
    fn send_install_snapshot(&self, peer: NodeId) {
        let snapshot = self
            .storage
            .snapshot()
            .expect("a nonzero snapshot boundary implies a snapshot")
            .clone();
        let term = self.current_term();
        let last_included_index = snapshot.last_included_index;
        tracing::debug!(
            node = self.config.id,
            term,
            peer,
            last_included_index,
            "peer is behind the snapshot boundary; sending InstallSnapshot"
        );
        let args = InstallSnapshotArgs {
            term,
            leader_id: self.config.id,
            snapshot,
        };
        let transport = self.transport.clone();
        let events = self.events_tx.clone();
        tokio::spawn(async move {
            let result = transport
                .send(peer, RpcRequest::InstallSnapshot(args))
                .await;
            let _ = events.send(Event::InstallSnapshotReply {
                sent_term: term,
                from: peer,
                last_included_index,
                result,
            });
        });
    }

    /// Compacts the applied prefix once it outgrows `snapshot_threshold`
    /// (phase 14). Called after every apply batch, so with a fixed threshold
    /// the compaction points are a pure function of the applied log —
    /// deterministic by construction, no size or timer triggers. Always at
    /// `last_applied`: commit_index may run ahead of what the state machine
    /// actually contains.
    fn maybe_compact(&mut self) {
        let Some(threshold) = self.config.snapshot_threshold else {
            return;
        };
        if self.last_applied - self.storage.snapshot_index() < threshold.max(1) {
            return;
        }
        let state = self.state_machine.snapshot();
        self.storage
            .compact_to(self.last_applied, state)
            .expect("cannot write snapshot; fail-stop");
        tracing::info!(
            node = self.config.id,
            term = self.current_term(),
            last_included_index = self.last_applied,
            "log compacted"
        );
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
            Role::PreCandidate { .. } => RoleKind::PreCandidate,
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
