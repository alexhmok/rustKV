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
//! - [`FaultConfig`]: delay range, per-leg drop probability, RPC timeout;
//!   swappable at runtime via [`SimNetwork::set_fault_config`].
//! - Directed link blocking ([`SimNetwork::set_link_blocked`] /
//!   `set_pair_blocked`) — the building block for phase 6 partitions.
//! - Crashes: dropping a node's `Inbound` receiver makes it a black hole
//!   (senders time out), like a dead process on a real network.
//!
//! Link state is sampled once per leg (request: at send; reply: when the
//! handler answers), so a block landing mid-flight does not retroactively
//! destroy a message already "on the wire".

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::raft::rpc::{RpcRequest, RpcResponse};
use crate::raft::transport::{Inbound, Transport, TransportError};
use crate::raft::types::NodeId;
use crate::rng::SplitMix64;

#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Delay bounds applied independently to each message leg.
    pub min_delay: Duration,
    pub max_delay: Duration,
    /// Probability that a given leg is silently lost.
    pub drop_probability: f64,
    /// How long a sender waits for the reply before giving up.
    pub rpc_timeout: Duration,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
            drop_probability: 0.0,
            rpc_timeout: Duration::from_millis(100),
        }
    }
}

struct State {
    rng: SplitMix64,
    config: FaultConfig,
    nodes: HashMap<NodeId, mpsc::UnboundedSender<Inbound>>,
    /// Directed `(from, to)` pairs whose messages are dropped.
    blocked: HashSet<(NodeId, NodeId)>,
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
    rpc_timeout: Duration,
}

impl Transport for SimTransport {
    async fn send(&self, to: NodeId, req: RpcRequest) -> Result<RpcResponse, TransportError> {
        let from = self.id;
        let plan = {
            let mut st = self.state.lock().expect("sim network lock poisoned");
            let cfg = st.config.clone();
            SendPlan {
                target: st.nodes.get(&to).cloned(),
                req_blocked: st.blocked.contains(&(from, to)),
                req_dropped: st.rng.next_bool(cfg.drop_probability),
                req_delay: draw_delay(&mut st.rng, &cfg),
                resp_dropped: st.rng.next_bool(cfg.drop_probability),
                resp_delay: draw_delay(&mut st.rng, &cfg),
                rpc_timeout: cfg.rpc_timeout,
            }
        };
        let Some(target) = plan.target else {
            return Err(TransportError::Unreachable(to));
        };

        let state = Arc::clone(&self.state);
        let exchange = async move {
            tokio::time::sleep(plan.req_delay).await;
            if plan.req_blocked || plan.req_dropped {
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

fn draw_delay(rng: &mut SplitMix64, cfg: &FaultConfig) -> Duration {
    let lo = u64::try_from(cfg.min_delay.as_micros()).expect("min_delay fits in u64 µs");
    let hi = u64::try_from(cfg.max_delay.as_micros()).expect("max_delay fits in u64 µs");
    assert!(lo <= hi, "FaultConfig: min_delay > max_delay");
    Duration::from_micros(rng.next_range(lo..=hi))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::rpc::{AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
    use crate::raft::types::Term;
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
    /// fixed order (terms 1 then 2).
    async fn arrival_order(seed: u64) -> Vec<Term> {
        let cfg = FaultConfig {
            min_delay: ms(1),
            max_delay: ms(20),
            drop_probability: 0.0,
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
        order
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
}
