//! In-memory simulated transport with seeded, controllable faults.
//!
//! A [`SimNetwork`] connects any number of registered nodes in one process.
//! Every message *leg* (request and reply separately) gets a random delay and
//! an independent drop decision from a single seeded [`SplitMix64`], so a run
//! is fully reproducible from its seed. Reordering is emergent: concurrent
//! sends draw independent delays, so a later send can overtake an earlier
//! one (asserted in tests).
//!
//! Determinism contract: run scenarios on a current-thread runtime with
//! virtual time (`#[tokio::test(start_paused = true)]`). All random decisions
//! for a send are drawn up front in one critical section — a fixed number of
//! draws per send, in task-scheduling order, independent of how the delays
//! later interleave.
//!
//! Fault control:
//! - [`FaultConfig`]: delay range, per-leg drop probability, request
//!   duplication probability, RPC timeout; swappable at runtime via
//!   [`SimNetwork::set_fault_config`].
//! - Directed link blocking ([`SimNetwork::set_link_blocked`] /
//!   `set_pair_blocked`) — the building block for phase 6 partitions.
//! - Crashes: dropping a node's `Inbound` receiver makes it a black hole
//!   (senders time out), like a dead process on a real network.
//!
//! Link state is sampled once per leg (request: at send; reply: when the
//! handler answers), so a block landing mid-flight does not retroactively
//! destroy a message already "on the wire".
//!
//! The network also acts as an event-level safety observer (phase 10):
//! every AppendEntries passing through the send path is inspected for
//! - Election Safety (§5.2): the message is a leadership claim
//!   `(term, leader_id)`; two different claimants for one term violate it;
//! - Log Matching (§5.3): every shipped entry claims "the entry at
//!   `(term, index)` is this command"; two different commands ever shipped
//!   under one `(term, index)` violate it;
//! - well-formedness: entries must continue contiguously from
//!   `prev_log_index` with non-decreasing terms never above the leader's.
//!
//! Only order-independent properties of message *contents* are checked:
//! send-observation order is task-scheduling order, not the order the Raft
//! core created the messages, so sequencing invariants (e.g. leader_commit
//! monotonicity) cannot be soundly asserted here. Violations are recorded
//! in [`SimNetwork::safety_violations`] for tests to assert on at teardown
//! — recorded, not panicked: sends run in spawned tasks, where a panic
//! would be silently swallowed with the task.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::raft::rpc::{AppendEntriesArgs, RpcRequest, RpcResponse};
use crate::raft::transport::{Inbound, Transport, TransportError};
use crate::raft::types::{Command, LogIndex, NodeId, Term};
use crate::rng::SplitMix64;

#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Delay bounds applied independently to each message leg.
    pub min_delay: Duration,
    pub max_delay: Duration,
    /// Probability that a given leg is silently lost.
    pub drop_probability: f64,
    /// Probability that a request is delivered twice. The duplicate is an
    /// independent copy with its own delay whose reply goes nowhere; it is
    /// delivered even if the primary copy is dropped (one copy lost, the
    /// other not), but never through a blocked link.
    pub duplicate_probability: f64,
    /// How long a sender waits for the reply before giving up.
    pub rpc_timeout: Duration,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            drop_probability: 0.0,
            duplicate_probability: 0.0,
            rpc_timeout: Duration::from_millis(100),
        }
    }
}

/// Counters of fault events that actually OCCURRED on this network, for
/// test vacuity guards (testing-regime T2): a test that schedules a fault
/// must be able to assert the fault really fired — a checker guarding a
/// fault that never happens is indistinguishable from one that works.
/// Pure observation: maintaining these consumes no RNG draws and adds no
/// delays, so pinned seeded schedules are unaffected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FaultStats {
    /// Primary sends that entered the network (one per [`Transport::send`]).
    pub sends: u64,
    /// Request legs lost to the drop dice (a leg both blocked and dropped
    /// counts as blocked — the block is what suppressed it).
    pub requests_dropped: u64,
    /// Reply legs lost to the drop dice (same tie-break as requests).
    pub replies_dropped: u64,
    /// Legs (either direction) suppressed by a blocked link.
    pub legs_blocked: u64,
    /// Duplicate request copies actually delivered into an inbox.
    pub duplicates_delivered: u64,
    /// Primary requests delivered before an earlier-sent primary request on
    /// the same directed link (the emergent reordering the module docs
    /// describe, observed rather than assumed).
    pub reorders: u64,
}

/// Largest serialized payload-bearing RPC seen crossing the network, per
/// kind (phase 20): the sim-level "wire size" — `serde_json` bytes of the
/// [`RpcRequest`], the same encoding the HTTP transport posts (its envelope
/// adds a small constant). Pure observation like [`FaultStats`]: no RNG
/// draws, no delays, so pinned seeded schedules are unaffected. Small
/// fixed-shape RPCs (votes, empty-batch heartbeats) are not measured —
/// only AppendEntries carrying entries and InstallSnapshot, the two kinds
/// whose size the phase-20 knobs exist to bound.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaxRpcBytes {
    /// Largest AppendEntries with a non-empty entries batch.
    pub append_entries: usize,
    /// Largest InstallSnapshot (single-shot or one chunk).
    pub install_snapshot: usize,
}

struct State {
    rng: SplitMix64,
    config: FaultConfig,
    nodes: HashMap<NodeId, mpsc::UnboundedSender<Inbound>>,
    /// Directed `(from, to)` pairs whose messages are dropped.
    blocked: HashSet<(NodeId, NodeId)>,
    /// First node seen claiming leadership of each term (via AppendEntries).
    leaders_per_term: HashMap<Term, NodeId>,
    /// First command seen shipped at each (term, index) (Log Matching §5.3:
    /// there must never be a second, different one).
    entries_seen: HashMap<(Term, LogIndex), Command>,
    /// Safety violations observed on the send path.
    violations: Vec<String>,
    /// Fault-event counters (see [`FaultStats`]).
    stats: FaultStats,
    /// Wire-size high-water marks (see [`MaxRpcBytes`]).
    max_rpc_bytes: MaxRpcBytes,
    /// Per directed link: sequence number for the next primary send, and the
    /// highest sequence delivered so far — a delivery below the high-water
    /// mark overtook an earlier send (a reorder).
    link_send_seq: HashMap<(NodeId, NodeId), u64>,
    link_delivered_seq: HashMap<(NodeId, NodeId), u64>,
}

/// The shared fabric. Cheap to clone; all clones drive the same network.
#[derive(Clone)]
pub struct SimNetwork {
    state: Arc<Mutex<State>>,
}

impl SimNetwork {
    pub fn new(seed: u64, config: FaultConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                rng: SplitMix64::new(seed),
                config,
                nodes: HashMap::new(),
                blocked: HashSet::new(),
                leaders_per_term: HashMap::new(),
                entries_seen: HashMap::new(),
                violations: Vec::new(),
                stats: FaultStats::default(),
                max_rpc_bytes: MaxRpcBytes::default(),
                link_send_seq: HashMap::new(),
                link_delivered_seq: HashMap::new(),
            })),
        }
    }

    /// Attaches a node, returning its outbound transport and the channel on
    /// which it receives RPCs. Re-registering an id replaces the old inbox
    /// (a restarted node, phase 6).
    pub fn register(&self, id: NodeId) -> (SimTransport, mpsc::UnboundedReceiver<Inbound>) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.lock().nodes.insert(id, tx);
        (
            SimTransport {
                id,
                state: Arc::clone(&self.state),
            },
            rx,
        )
    }

    /// Blocks or unblocks messages in the `from → to` direction only.
    pub fn set_link_blocked(&self, from: NodeId, to: NodeId, blocked: bool) {
        let mut st = self.lock();
        if blocked {
            st.blocked.insert((from, to));
        } else {
            st.blocked.remove(&(from, to));
        }
        tracing::debug!(from, to, blocked, "sim: link state changed");
    }

    /// Blocks or unblocks both directions between `a` and `b`.
    pub fn set_pair_blocked(&self, a: NodeId, b: NodeId, blocked: bool) {
        self.set_link_blocked(a, b, blocked);
        self.set_link_blocked(b, a, blocked);
    }

    /// Replaces the fault parameters for all subsequent sends.
    pub fn set_fault_config(&self, config: FaultConfig) {
        self.lock().config = config;
    }

    /// Safety violations seen so far: Election Safety (two distinct nodes
    /// claiming leadership of one term), Log Matching (two different
    /// commands ever shipped under one (term, index)), or a malformed
    /// AppendEntries (see module docs). Every message crossing the network
    /// is inspected (even ones later dropped — a send is a claim regardless
    /// of delivery), so unlike status sampling this cannot miss a
    /// sub-sample flicker. Empty on a correct Raft.
    pub fn safety_violations(&self) -> Vec<String> {
        self.lock().violations.clone()
    }

    /// Fault events that actually occurred so far (vacuity guards). On the
    /// paused-time current-thread runtime the counters are as deterministic
    /// as everything else — a pinned schedule pins its stats too.
    pub fn fault_stats(&self) -> FaultStats {
        self.lock().stats
    }

    /// Wire-size high-water marks seen so far (see [`MaxRpcBytes`]): what
    /// the phase-20 batch/chunk caps are asserted against. Deterministic
    /// per seed, like the fault stats.
    pub fn max_rpc_bytes(&self) -> MaxRpcBytes {
        self.lock().max_rpc_bytes
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("sim network lock poisoned")
    }
}

/// One node's handle for sending RPCs through the simulated network.
#[derive(Clone)]
pub struct SimTransport {
    id: NodeId,
    state: Arc<Mutex<State>>,
}

/// Everything random about one send, drawn up front (see module docs).
struct SendPlan {
    target: Option<mpsc::UnboundedSender<Inbound>>,
    req_blocked: bool,
    req_dropped: bool,
    req_delay: Duration,
    resp_dropped: bool,
    resp_delay: Duration,
    req_duplicated: bool,
    dup_delay: Duration,
    rpc_timeout: Duration,
    /// Per-link send sequence of this primary request, for reorder counting.
    seq: u64,
}

impl Transport for SimTransport {
    async fn send(&self, to: NodeId, req: RpcRequest) -> Result<RpcResponse, TransportError> {
        let from = self.id;
        let plan = {
            let mut st = self.state.lock().expect("sim network lock poisoned");
            inspect_append_entries(&mut st, &req);
            measure_rpc(&mut st, &req);
            let cfg = st.config.clone();
            st.stats.sends += 1;
            let seq = {
                let counter = st.link_send_seq.entry((from, to)).or_insert(0);
                *counter += 1;
                *counter
            };
            // Determinism contract: a fixed number of draws per send, all in
            // this critical section — the duplication draws are unconditional
            // even when duplicate_probability is 0.
            SendPlan {
                target: st.nodes.get(&to).cloned(),
                req_blocked: st.blocked.contains(&(from, to)),
                req_dropped: st.rng.next_bool(cfg.drop_probability),
                req_delay: draw_delay(&mut st.rng, &cfg),
                resp_dropped: st.rng.next_bool(cfg.drop_probability),
                resp_delay: draw_delay(&mut st.rng, &cfg),
                req_duplicated: st.rng.next_bool(cfg.duplicate_probability),
                dup_delay: draw_delay(&mut st.rng, &cfg),
                rpc_timeout: cfg.rpc_timeout,
                seq,
            }
        };
        let Some(target) = plan.target else {
            return Err(TransportError::Unreachable(to));
        };

        if plan.req_duplicated && !plan.req_blocked {
            // Fire-and-forget second copy on its own clock. Its reply
            // receiver is dropped immediately (handlers tolerate that), and
            // it shares nothing with the primary exchange's timeout — a
            // duplicate can arrive long after the sender gave up.
            let dup_target = target.clone();
            let dup_req = req.clone();
            let dup_delay = plan.dup_delay;
            let dup_state = Arc::clone(&self.state);
            tokio::spawn(async move {
                tokio::time::sleep(dup_delay).await;
                let (reply_tx, _discarded) = oneshot::channel();
                if dup_target
                    .send(Inbound {
                        from,
                        request: dup_req,
                        reply: reply_tx,
                    })
                    .is_ok()
                {
                    let mut st = dup_state.lock().expect("sim network lock poisoned");
                    st.stats.duplicates_delivered += 1;
                    tracing::trace!(from, to, "sim: duplicate request delivered");
                }
            });
        }

        let state = Arc::clone(&self.state);
        let exchange = async move {
            tokio::time::sleep(plan.req_delay).await;
            if plan.req_blocked || plan.req_dropped {
                {
                    let mut st = state.lock().expect("sim network lock poisoned");
                    if plan.req_blocked {
                        st.stats.legs_blocked += 1;
                    } else {
                        st.stats.requests_dropped += 1;
                    }
                }
                tracing::trace!(from, to, "sim: request leg dropped");
                return std::future::pending::<RpcResponse>().await;
            }
            let (reply_tx, reply_rx) = oneshot::channel();
            if target
                .send(Inbound {
                    from,
                    request: req,
                    reply: reply_tx,
                })
                .is_err()
            {
                // Inbox gone: the node crashed. Black hole, not an error.
                tracing::trace!(from, to, "sim: peer inbox closed");
                return std::future::pending::<RpcResponse>().await;
            }
            {
                // Delivered: a primary request landing below the link's
                // high-water sequence overtook an earlier send (reorder).
                let mut st = state.lock().expect("sim network lock poisoned");
                let delivered = st.link_delivered_seq.entry((from, to)).or_insert(0);
                if plan.seq < *delivered {
                    st.stats.reorders += 1;
                } else {
                    *delivered = plan.seq;
                }
            }
            let Ok(resp) = reply_rx.await else {
                // Node dropped the reply sender (crashed mid-handling).
                tracing::trace!(from, to, "sim: peer dropped reply");
                return std::future::pending::<RpcResponse>().await;
            };
            tokio::time::sleep(plan.resp_delay).await;
            let reply_blocked = {
                let st = state.lock().expect("sim network lock poisoned");
                st.blocked.contains(&(to, from))
            };
            if plan.resp_dropped || reply_blocked {
                {
                    let mut st = state.lock().expect("sim network lock poisoned");
                    if reply_blocked {
                        st.stats.legs_blocked += 1;
                    } else {
                        st.stats.replies_dropped += 1;
                    }
                }
                tracing::trace!(from, to, "sim: reply leg dropped");
                return std::future::pending::<RpcResponse>().await;
            }
            resp
        };

        tokio::time::timeout(plan.rpc_timeout, exchange)
            .await
            .map_err(|_| TransportError::Timeout)
    }
}

/// The event-level safety observer (see module docs). Only AppendEntries
/// carries claims; other variants fall through the match and are ignored
/// by construction. In particular a PreVote is a non-binding probe, not a
/// leadership claim — the observer must not (and does not) see it.
fn inspect_append_entries(st: &mut State, req: &RpcRequest) {
    let RpcRequest::AppendEntries(args) = req else {
        return;
    };
    // Election Safety (§5.2): "I lead term T" must have a unique claimant.
    match st.leaders_per_term.get(&args.term) {
        None => {
            st.leaders_per_term.insert(args.term, args.leader_id);
        }
        Some(&known) if known != args.leader_id => {
            st.violations.push(format!(
                "election safety violated: nodes {known} and {} both sent \
                 AppendEntries as leader of term {}",
                args.leader_id, args.term
            ));
        }
        Some(_) => {}
    }
    // Well-formedness: entries continue contiguously from prev_log_index
    // with non-decreasing terms, none newer than the sender's own term.
    check_shape(st, args);
    // Log Matching (§5.3): a (term, index) names one command, forever —
    // across every leader, retransmission and duplicate.
    for entry in &args.entries {
        match st.entries_seen.get(&(entry.term, entry.index)) {
            None => {
                st.entries_seen
                    .insert((entry.term, entry.index), entry.command.clone());
            }
            Some(seen) if *seen != entry.command => {
                st.violations.push(format!(
                    "log matching violated: two different commands shipped \
                     for (term {}, index {})",
                    entry.term, entry.index
                ));
            }
            Some(_) => {}
        }
    }
}

/// The wire-size observer (see [`MaxRpcBytes`]): serializes only the two
/// payload-bearing RPC kinds — every other variant is a few dozen bytes of
/// fixed shape, and skipping them keeps the observer near-free on
/// heartbeat-dominated runs.
fn measure_rpc(st: &mut State, req: &RpcRequest) {
    let slot = match req {
        RpcRequest::AppendEntries(args) if !args.entries.is_empty() => {
            &mut st.max_rpc_bytes.append_entries
        }
        RpcRequest::InstallSnapshot(_) => &mut st.max_rpc_bytes.install_snapshot,
        _ => return,
    };
    let bytes = serde_json::to_vec(req)
        .expect("rpc serialization cannot fail")
        .len();
    *slot = (*slot).max(bytes);
}

fn check_shape(st: &mut State, args: &AppendEntriesArgs) {
    let mut expected = args.prev_log_index + 1;
    let mut min_term = args.prev_log_term;
    for entry in &args.entries {
        if entry.index != expected {
            st.violations.push(format!(
                "malformed AppendEntries from node {}: entry index {} where \
                 {expected} was expected (prev_log_index {})",
                args.leader_id, entry.index, args.prev_log_index
            ));
            return;
        }
        if entry.term < min_term || entry.term > args.term {
            st.violations.push(format!(
                "malformed AppendEntries from node {}: entry (term {}, index \
                 {}) outside [{min_term}, {}]",
                args.leader_id, entry.term, entry.index, args.term
            ));
            return;
        }
        expected += 1;
        min_term = entry.term;
    }
}

fn draw_delay(rng: &mut SplitMix64, cfg: &FaultConfig) -> Duration {
    let lo = u64::try_from(cfg.min_delay.as_micros()).expect("min_delay fits in u64 µs");
    let hi = u64::try_from(cfg.max_delay.as_micros()).expect("max_delay fits in u64 µs");
    assert!(lo <= hi, "FaultConfig: min_delay > max_delay");
    Duration::from_micros(rng.next_range(lo..=hi))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::rpc::{
        AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
        RequestVoteArgs, RequestVoteReply,
    };
    use crate::raft::types::{Snapshot, Term};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::time::Instant;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn vote_req(term: Term) -> RpcRequest {
        RpcRequest::RequestVote(RequestVoteArgs {
            term,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        })
    }

    fn pre_vote_req(term: Term, candidate_id: NodeId) -> RpcRequest {
        RpcRequest::PreVote(RequestVoteArgs {
            term,
            candidate_id,
            last_log_index: 0,
            last_log_term: 0,
        })
    }

    fn ae_req(term: Term, leader_id: NodeId) -> RpcRequest {
        RpcRequest::AppendEntries(AppendEntriesArgs {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        })
    }

    /// Answers every inbound RPC immediately, echoing the request's term.
    fn spawn_echo(mut rx: mpsc::UnboundedReceiver<Inbound>) {
        tokio::spawn(async move {
            while let Some(inbound) = rx.recv().await {
                let resp = match &inbound.request {
                    RpcRequest::RequestVote(args) => RpcResponse::RequestVote(RequestVoteReply {
                        term: args.term,
                        vote_granted: true,
                    }),
                    RpcRequest::AppendEntries(args) => {
                        RpcResponse::AppendEntries(AppendEntriesReply {
                            term: args.term,
                            success: true,
                        })
                    }
                    RpcRequest::PreVote(args) => RpcResponse::PreVote(RequestVoteReply {
                        term: args.term,
                        vote_granted: true,
                    }),
                    RpcRequest::InstallSnapshot(args) => {
                        RpcResponse::InstallSnapshot(InstallSnapshotReply { term: args.term })
                    }
                };
                let _ = inbound.reply.send(resp);
            }
        });
    }

    fn fixed_delay_config(delay: Duration) -> FaultConfig {
        FaultConfig {
            min_delay: delay,
            max_delay: delay,
            drop_probability: 0.0,
            duplicate_probability: 0.0,
            rpc_timeout: ms(100),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn roundtrip_takes_exactly_both_legs_of_virtual_time() {
        let net = SimNetwork::new(42, fixed_delay_config(ms(5)));
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        spawn_echo(rx2);

        let start = Instant::now();
        let resp = t1.send(2, vote_req(3)).await.unwrap();
        assert_eq!(
            resp,
            RpcResponse::RequestVote(RequestVoteReply {
                term: 3,
                vote_granted: true
            })
        );
        assert_eq!(start.elapsed(), ms(10), "5ms request leg + 5ms reply leg");
    }

    #[tokio::test(start_paused = true)]
    async fn full_drop_rate_times_out_after_exactly_rpc_timeout() {
        let cfg = FaultConfig {
            drop_probability: 1.0,
            ..fixed_delay_config(ms(5))
        };
        let net = SimNetwork::new(7, cfg);
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        spawn_echo(rx2);

        let start = Instant::now();
        assert_eq!(t1.send(2, vote_req(1)).await, Err(TransportError::Timeout));
        assert_eq!(start.elapsed(), ms(100));
    }

    #[tokio::test(start_paused = true)]
    async fn unregistered_peer_is_unreachable_immediately() {
        let net = SimNetwork::new(0, FaultConfig::default());
        let (t1, _rx1) = net.register(1);

        let start = Instant::now();
        assert_eq!(
            t1.send(99, vote_req(1)).await,
            Err(TransportError::Unreachable(99))
        );
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn crashed_node_black_holes_messages() {
        let net = SimNetwork::new(0, fixed_delay_config(ms(1)));
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        drop(rx2); // node 2 "crashes"

        assert_eq!(t1.send(2, vote_req(1)).await, Err(TransportError::Timeout));
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_link_times_out_and_recovers_when_unblocked() {
        let net = SimNetwork::new(3, fixed_delay_config(ms(1)));
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        spawn_echo(rx2);

        net.set_pair_blocked(1, 2, true);
        assert_eq!(t1.send(2, vote_req(1)).await, Err(TransportError::Timeout));

        net.set_pair_blocked(1, 2, false);
        assert!(t1.send(2, vote_req(2)).await.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn fault_config_can_be_swapped_at_runtime() {
        let net = SimNetwork::new(
            9,
            FaultConfig {
                drop_probability: 1.0,
                ..fixed_delay_config(ms(1))
            },
        );
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        spawn_echo(rx2);

        assert_eq!(t1.send(2, vote_req(1)).await, Err(TransportError::Timeout));
        net.set_fault_config(fixed_delay_config(ms(1)));
        assert!(t1.send(2, vote_req(2)).await.is_ok());
    }

    /// 20 concurrent sends under 30% drop + jitter; the full observable
    /// outcome (per-message success and virtual completion time) must be a
    /// pure function of the seed.
    async fn lossy_scenario_trace(seed: u64) -> Vec<String> {
        let cfg = FaultConfig {
            min_delay: ms(1),
            max_delay: ms(20),
            drop_probability: 0.3,
            duplicate_probability: 0.0,
            rpc_timeout: ms(50),
        };
        let net = SimNetwork::new(seed, cfg);
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        spawn_echo(rx2);

        let start = Instant::now();
        let handles: Vec<_> = (0..20u64)
            .map(|i| {
                let t1 = t1.clone();
                tokio::spawn(async move {
                    let result = t1.send(2, vote_req(i)).await;
                    format!(
                        "msg {i}: ok={} at {}µs",
                        result.is_ok(),
                        start.elapsed().as_micros()
                    )
                })
            })
            .collect();
        let mut trace = Vec::new();
        for h in handles {
            trace.push(h.await.unwrap());
        }
        trace
    }

    #[tokio::test(start_paused = true)]
    async fn same_seed_reproduces_identical_trace() {
        let a = lossy_scenario_trace(1234).await;
        let b = lossy_scenario_trace(1234).await;
        assert_eq!(a, b);
        // Sanity: the scenario actually exercises both outcomes.
        assert!(a.iter().any(|l| l.contains("ok=true")));
        assert!(a.iter().any(|l| l.contains("ok=false")));
    }

    #[tokio::test(start_paused = true)]
    async fn different_seeds_diverge() {
        assert_ne!(lossy_scenario_trace(1).await, lossy_scenario_trace(2).await);
    }

    /// Arrival order at the receiver of two messages sent concurrently in a
    /// fixed order (terms 1 then 2), plus the network's fault stats — the
    /// reorder counter must agree with the observed order.
    async fn arrival_order_with_stats(seed: u64) -> (Vec<Term>, FaultStats) {
        let cfg = FaultConfig {
            min_delay: ms(1),
            max_delay: ms(20),
            drop_probability: 0.0,
            duplicate_probability: 0.0,
            rpc_timeout: ms(100),
        };
        let net = SimNetwork::new(seed, cfg);
        let (t1, _rx1) = net.register(1);
        let (_t2, mut rx2) = net.register(2);

        let ta = t1.clone();
        let h1 = tokio::spawn(async move { ta.send(2, vote_req(1)).await });
        let h2 = tokio::spawn(async move { t1.send(2, vote_req(2)).await });

        let mut order = Vec::new();
        for _ in 0..2 {
            let inbound = rx2.recv().await.unwrap();
            let RpcRequest::RequestVote(args) = &inbound.request else {
                panic!("unexpected rpc")
            };
            order.push(args.term);
            let _ = inbound
                .reply
                .send(RpcResponse::RequestVote(RequestVoteReply {
                    term: args.term,
                    vote_granted: true,
                }));
        }
        h1.await.unwrap().unwrap();
        h2.await.unwrap().unwrap();
        (order, net.fault_stats())
    }

    async fn arrival_order(seed: u64) -> Vec<Term> {
        arrival_order_with_stats(seed).await.0
    }

    #[tokio::test(start_paused = true)]
    async fn full_duplication_delivers_every_request_twice() {
        let cfg = FaultConfig {
            duplicate_probability: 1.0,
            ..fixed_delay_config(ms(2))
        };
        let net = SimNetwork::new(11, cfg);
        let (t1, _rx1) = net.register(1);
        let (_t2, mut rx2) = net.register(2);

        let sender = tokio::spawn(async move { t1.send(2, vote_req(7)).await });
        for copy in 0..2 {
            let inbound = rx2.recv().await.unwrap_or_else(|| panic!("copy {copy}"));
            let RpcRequest::RequestVote(args) = &inbound.request else {
                panic!("unexpected rpc");
            };
            assert_eq!(args.term, 7, "copy {copy} is byte-for-byte the request");
            // Answer both copies; the duplicate's reply sinks harmlessly
            // into its dropped receiver.
            let _ = inbound
                .reply
                .send(RpcResponse::RequestVote(RequestVoteReply {
                    term: args.term,
                    vote_granted: true,
                }));
        }
        assert!(
            sender.await.unwrap().is_ok(),
            "the primary exchange still completes normally"
        );
        // And no third copy ever shows up.
        tokio::time::timeout(ms(500), rx2.recv())
            .await
            .expect_err("exactly two copies");
    }

    /// 20 concurrent sends under drop + duplication; the per-send outcomes
    /// AND the receiver-side arrival count must be pure functions of the
    /// seed.
    async fn duplicated_lossy_trace(seed: u64) -> (Vec<String>, u64) {
        let cfg = FaultConfig {
            min_delay: ms(1),
            max_delay: ms(20),
            drop_probability: 0.2,
            duplicate_probability: 0.5,
            rpc_timeout: ms(50),
        };
        let net = SimNetwork::new(seed, cfg);
        let (t1, _rx1) = net.register(1);
        let (_t2, mut rx2) = net.register(2);
        let arrivals = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&arrivals);
        tokio::spawn(async move {
            while let Some(inbound) = rx2.recv().await {
                counter.fetch_add(1, Ordering::SeqCst);
                let RpcRequest::RequestVote(args) = &inbound.request else {
                    continue;
                };
                let _ = inbound
                    .reply
                    .send(RpcResponse::RequestVote(RequestVoteReply {
                        term: args.term,
                        vote_granted: true,
                    }));
            }
        });

        let start = Instant::now();
        let handles: Vec<_> = (0..20u64)
            .map(|i| {
                let t1 = t1.clone();
                tokio::spawn(async move {
                    let result = t1.send(2, vote_req(i)).await;
                    format!(
                        "msg {i}: ok={} at {}µs",
                        result.is_ok(),
                        start.elapsed().as_micros()
                    )
                })
            })
            .collect();
        let mut trace = Vec::new();
        for h in handles {
            trace.push(h.await.unwrap());
        }
        // Duplicates are fire-and-forget on their own clocks — let any
        // stragglers land before reading the arrival counter.
        tokio::time::sleep(ms(200)).await;
        (trace, arrivals.load(Ordering::SeqCst))
    }

    #[tokio::test(start_paused = true)]
    async fn duplication_schedule_is_reproducible_per_seed() {
        let (trace_a, arrivals_a) = duplicated_lossy_trace(77).await;
        let (trace_b, arrivals_b) = duplicated_lossy_trace(77).await;
        assert_eq!(trace_a, trace_b);
        assert_eq!(arrivals_a, arrivals_b);
        // Sanity: more arrivals than the 20 primary sends means duplicates
        // really landed (drops only push the count down).
        assert!(
            arrivals_a > 20,
            "expected duplicate deliveries, got {arrivals_a} arrivals"
        );
    }

    /// The event-level Election Safety observer must record forged
    /// conflicting leadership claims — and nothing else.
    #[tokio::test(start_paused = true)]
    async fn election_safety_interceptor_records_conflicting_claims() {
        let net = SimNetwork::new(0, fixed_delay_config(ms(1)));
        let (t1, _rx1) = net.register(1);
        let (t2, _rx2) = net.register(2);
        let (_t3, rx3) = net.register(3);
        spawn_echo(rx3);

        // Repeated claims by the same node, and claims for other terms, are
        // not violations. Neither are RequestVotes (candidates, not leaders).
        t1.send(3, ae_req(5, 1)).await.unwrap();
        t1.send(3, ae_req(5, 1)).await.unwrap();
        t1.send(3, ae_req(6, 1)).await.unwrap();
        t2.send(3, vote_req(5)).await.unwrap();
        assert_eq!(net.safety_violations(), Vec::<String>::new());

        // Forged conflict: node 2 also claims to lead term 5.
        t2.send(3, ae_req(5, 2)).await.unwrap();
        let violations = net.safety_violations();
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(violations[0].contains("term 5"), "{violations:?}");

        // A claim records at send time even if the message is then lost:
        // node 2 claiming node 1's term 6 from behind a blocked link still
        // conflicts.
        net.set_link_blocked(2, 3, true);
        let _ = t2.send(3, ae_req(6, 2)).await;
        assert_eq!(net.safety_violations().len(), 2);
    }

    /// A pre-vote is a probe, not a leadership claim: the safety observer
    /// must record nothing about PreVote traffic, however conflicting it
    /// looks — it ignores every non-AppendEntries variant by construction.
    #[tokio::test(start_paused = true)]
    async fn pre_votes_are_invisible_to_the_safety_observer() {
        let net = SimNetwork::new(0, fixed_delay_config(ms(1)));
        let (t1, _rx1) = net.register(1);
        let (t2, _rx2) = net.register(2);
        let (_t3, rx3) = net.register(3);
        spawn_echo(rx3);

        // Two nodes pre-voting for the same prospective term is normal
        // (grants are non-binding) — not an Election Safety conflict.
        t1.send(3, pre_vote_req(5, 1)).await.unwrap();
        t2.send(3, pre_vote_req(5, 2)).await.unwrap();
        // Nor does a pre-vote conflict with a REAL leadership claim for the
        // same term, in either order.
        t1.send(3, ae_req(5, 1)).await.unwrap();
        t2.send(3, pre_vote_req(5, 2)).await.unwrap();
        assert_eq!(net.safety_violations(), Vec::<String>::new());

        // Sanity that the observer is still awake: a conflicting REAL claim
        // for that term does record.
        t2.send(3, ae_req(5, 2)).await.unwrap();
        assert_eq!(net.safety_violations().len(), 1);
    }

    /// InstallSnapshot (phase 14) is, like PreVote, not an AppendEntries:
    /// the safety observer ignores it by construction — even when it looks
    /// like a conflicting leadership claim. (Its correctness is covered by
    /// the Raft-level snapshot tests; the observer's three checks are all
    /// defined over AppendEntries contents only.)
    #[tokio::test(start_paused = true)]
    async fn install_snapshots_are_invisible_to_the_safety_observer() {
        let net = SimNetwork::new(0, fixed_delay_config(ms(1)));
        let (t1, _rx1) = net.register(1);
        let (t2, _rx2) = net.register(2);
        let (_t3, rx3) = net.register(3);
        spawn_echo(rx3);

        let snap_req = |leader_id: NodeId| {
            RpcRequest::InstallSnapshot(InstallSnapshotArgs {
                term: 5,
                leader_id,
                snapshot: Snapshot {
                    last_included_index: 3,
                    last_included_term: 2,
                    membership: None,
                    state: serde_json::json!({}),
                },
                offset: 0,
                data: None,
                done: true,
            })
        };
        t1.send(3, snap_req(1)).await.unwrap();
        t2.send(3, snap_req(2)).await.unwrap();
        assert_eq!(net.safety_violations(), Vec::<String>::new());

        // Sanity that the observer is still awake for real AE claims.
        t1.send(3, ae_req(5, 1)).await.unwrap();
        t2.send(3, ae_req(5, 2)).await.unwrap();
        assert_eq!(net.safety_violations().len(), 1);
    }

    fn ae_with(
        term: Term,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<crate::raft::types::LogEntry>,
    ) -> RpcRequest {
        RpcRequest::AppendEntries(AppendEntriesArgs {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: 0,
        })
    }

    fn put_entry(term: Term, index: u64, value: u64) -> crate::raft::types::LogEntry {
        crate::raft::types::LogEntry {
            term,
            index,
            command: Command::Put {
                key: format!("k{index}"),
                value: serde_json::json!(value),
                session: None,
            },
        }
    }

    #[tokio::test(start_paused = true)]
    async fn log_matching_interceptor_records_conflicting_entries() {
        let net = SimNetwork::new(0, fixed_delay_config(ms(1)));
        let (t1, _rx1) = net.register(1);
        let (t2, _rx2) = net.register(2);
        let (_t3, rx3) = net.register(3);
        spawn_echo(rx3);

        // The same entry retransmitted (and duplicated) is not a violation;
        // neither is a different command at the same index under a NEW term
        // (that's a legal conflict overwrite).
        let e = put_entry(2, 1, 10);
        t1.send(3, ae_with(2, 1, 0, 0, vec![e.clone()]))
            .await
            .unwrap();
        t1.send(3, ae_with(2, 1, 0, 0, vec![e.clone()]))
            .await
            .unwrap();
        t2.send(3, ae_with(3, 2, 0, 0, vec![put_entry(3, 1, 99)]))
            .await
            .unwrap();
        assert_eq!(net.safety_violations(), Vec::<String>::new());

        // Forged: same (term 2, index 1) carrying a different command.
        t1.send(3, ae_with(2, 1, 0, 0, vec![put_entry(2, 1, 11)]))
            .await
            .unwrap();
        let violations = net.safety_violations();
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(
            violations[0].contains("log matching") && violations[0].contains("index 1"),
            "{violations:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_append_entries_are_recorded() {
        let net = SimNetwork::new(0, fixed_delay_config(ms(1)));
        let (t1, _rx1) = net.register(1);
        let (_t3, rx3) = net.register(3);
        spawn_echo(rx3);

        // A well-formed batch: contiguous from prev, terms non-decreasing.
        t1.send(
            3,
            ae_with(3, 1, 1, 1, vec![put_entry(2, 2, 1), put_entry(3, 3, 2)]),
        )
        .await
        .unwrap();
        assert_eq!(net.safety_violations(), Vec::<String>::new());

        // Gap after prev_log_index (same command as before, so only the
        // shape check — not log matching — can be what fires).
        t1.send(3, ae_with(3, 1, 1, 1, vec![put_entry(3, 3, 2)]))
            .await
            .unwrap();
        // Entry from a term newer than the sender claims to lead.
        t1.send(3, ae_with(3, 1, 3, 3, vec![put_entry(4, 4, 4)]))
            .await
            .unwrap();
        // Terms decreasing along the batch (below prev_log_term).
        t1.send(3, ae_with(3, 1, 3, 3, vec![put_entry(2, 4, 5)]))
            .await
            .unwrap();
        let violations = net.safety_violations();
        assert_eq!(violations.len(), 3, "{violations:?}");
        assert!(
            violations.iter().all(|v| v.contains("malformed")),
            "{violations:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn independent_delays_reorder_concurrent_messages() {
        let mut reordering_seed = None;
        for seed in 0..64 {
            if arrival_order(seed).await == [2, 1] {
                reordering_seed = Some(seed);
                break;
            }
        }
        let seed =
            reordering_seed.expect("some seed in 0..64 must reorder two concurrent messages");
        // And the reordering is reproducible, not a fluke of scheduling.
        assert_eq!(arrival_order(seed).await, [2, 1]);
    }

    // ---- fault stats (testing-regime T2): the vacuity counters must count
    // exactly the events that occurred — no phantom events, no missed ones,
    // and reading them must not perturb pinned schedules. ----

    /// The reorder counter must agree with directly observed arrival order,
    /// in both directions: zero on an in-order seed, nonzero (and exact) on
    /// a reordering one.
    #[tokio::test(start_paused = true)]
    async fn reorder_counter_matches_observed_arrival_order() {
        let mut saw_in_order = false;
        let mut saw_reordered = false;
        for seed in 0..64 {
            let (order, stats) = arrival_order_with_stats(seed).await;
            assert_eq!(stats.sends, 2);
            match order[..] {
                [1, 2] => {
                    assert_eq!(stats.reorders, 0, "seed {seed}: phantom reorder");
                    saw_in_order = true;
                }
                [2, 1] => {
                    assert_eq!(stats.reorders, 1, "seed {seed}: missed reorder");
                    saw_reordered = true;
                }
                _ => unreachable!(),
            }
            if saw_in_order && saw_reordered {
                return;
            }
        }
        panic!("seeds 0..64 must exercise both orderings");
    }

    /// Certain drops count exactly; a faultless exchange counts nothing.
    #[tokio::test(start_paused = true)]
    async fn drop_counters_count_exactly_the_dropped_legs() {
        // drop_probability 1.0: the one request leg is dropped, nothing else.
        let net = SimNetwork::new(
            7,
            FaultConfig {
                drop_probability: 1.0,
                ..fixed_delay_config(ms(5))
            },
        );
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        spawn_echo(rx2);
        assert_eq!(t1.send(2, vote_req(1)).await, Err(TransportError::Timeout));
        let stats = net.fault_stats();
        assert_eq!(
            (stats.sends, stats.requests_dropped, stats.replies_dropped),
            (1, 1, 0)
        );
        assert_eq!((stats.duplicates_delivered, stats.legs_blocked), (0, 0));

        // No faults at all: only the send counts.
        let net = SimNetwork::new(7, fixed_delay_config(ms(5)));
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        spawn_echo(rx2);
        t1.send(2, vote_req(1)).await.unwrap();
        assert_eq!(
            net.fault_stats(),
            FaultStats {
                sends: 1,
                ..FaultStats::default()
            }
        );
    }

    /// A blocked link counts as blocked (not dropped), and a duplicate
    /// delivery counts once.
    #[tokio::test(start_paused = true)]
    async fn blocked_and_duplicate_events_are_counted() {
        let net = SimNetwork::new(3, fixed_delay_config(ms(1)));
        let (t1, _rx1) = net.register(1);
        let (_t2, rx2) = net.register(2);
        spawn_echo(rx2);
        net.set_pair_blocked(1, 2, true);
        assert_eq!(t1.send(2, vote_req(1)).await, Err(TransportError::Timeout));
        let stats = net.fault_stats();
        assert_eq!(stats.legs_blocked, 1, "one suppressed request leg");
        assert_eq!(stats.requests_dropped, 0, "blocked is not dropped");

        let net = SimNetwork::new(
            11,
            FaultConfig {
                duplicate_probability: 1.0,
                ..fixed_delay_config(ms(2))
            },
        );
        let (t1, _rx1) = net.register(1);
        let (_t2, mut rx2) = net.register(2);
        let sender = tokio::spawn(async move { t1.send(2, vote_req(7)).await });
        for _ in 0..2 {
            let inbound = rx2.recv().await.unwrap();
            let RpcRequest::RequestVote(args) = &inbound.request else {
                panic!("unexpected rpc");
            };
            let _ = inbound
                .reply
                .send(RpcResponse::RequestVote(RequestVoteReply {
                    term: args.term,
                    vote_granted: true,
                }));
        }
        sender.await.unwrap().unwrap();
        let stats = net.fault_stats();
        assert_eq!((stats.sends, stats.duplicates_delivered), (1, 1));
    }

    /// The counters are part of the deterministic surface: same seed, same
    /// stats — byte for byte.
    #[tokio::test(start_paused = true)]
    async fn fault_stats_are_reproducible_per_seed() {
        async fn run(seed: u64) -> FaultStats {
            let cfg = FaultConfig {
                min_delay: ms(1),
                max_delay: ms(20),
                drop_probability: 0.2,
                duplicate_probability: 0.5,
                rpc_timeout: ms(50),
            };
            let net = SimNetwork::new(seed, cfg);
            let (t1, _rx1) = net.register(1);
            let (_t2, rx2) = net.register(2);
            spawn_echo(rx2);
            let handles: Vec<_> = (0..20u64)
                .map(|i| {
                    let t1 = t1.clone();
                    tokio::spawn(async move { t1.send(2, vote_req(i)).await.is_ok() })
                })
                .collect();
            for h in handles {
                h.await.unwrap();
            }
            // Let fire-and-forget duplicates land before reading.
            tokio::time::sleep(ms(200)).await;
            net.fault_stats()
        }
        let a = run(77).await;
        let b = run(77).await;
        assert_eq!(a, b);
        assert_eq!(a.sends, 20);
        // The mix must actually exercise every probabilistic counter.
        assert!(a.requests_dropped + a.replies_dropped > 0);
        assert!(a.duplicates_delivered > 0);
    }
}
