//! Shared helpers for the seeded cluster tests (election, replication,
//! fault, and jepsen-style tests). This module is compiled separately into
//! each test binary, so not every binary uses every item.
#![allow(dead_code)]

pub mod lin;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustkv::raft::Storage;
use rustkv::raft::node::{RaftConfig, RaftHandle, RaftNode, RoleKind, StateMachine, Status};
use rustkv::raft::transport::sim::{FaultConfig, SimNetwork};
use rustkv::raft::types::{
    Command, LogEntry, LogIndex, MemberAddr, NodeId, Session, Snapshot, Term,
};
use rustkv::store::KvStore;
use serde_json::json;
use tempfile::TempDir;

pub fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

pub fn low_loss_faults() -> FaultConfig {
    FaultConfig {
        min_delay: ms(1),
        max_delay: ms(10),
        drop_probability: 0.0,
        duplicate_probability: 0.0,
        rpc_timeout: ms(50),
    }
}

pub fn entry(term: Term, index: LogIndex) -> LogEntry {
    LogEntry {
        term,
        index,
        command: put(&format!("k{index}"), index),
    }
}

pub fn put(key: &str, value: u64) -> Command {
    Command::Put {
        key: key.to_string(),
        value: json!(value),
        session: None,
    }
}

/// Like [`put`], but carrying a dedup token (phase 13): the state machine
/// applies at most one command per (client, seq).
pub fn put_with_token(key: &str, value: u64, client: u64, seq: u64) -> Command {
    Command::Put {
        key: key.to_string(),
        value: json!(value),
        session: Some(Session { client, seq }),
    }
}

/// A fresh throwaway state machine for RPC-level tests that spawn RaftNode
/// directly.
pub fn new_sm() -> Arc<dyn StateMachine> {
    Arc::new(KvStore::new())
}

pub fn node_config(id: NodeId, n: u64, seed: u64) -> RaftConfig {
    let peers = (1..=n).filter(|&p| p != id).collect();
    let mut config = RaftConfig::new(id, peers);
    // Distinct, seed-derived jitter per node.
    config.timeout_seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(id);
    config
}

/// Config with effectively-infinite election timeouts, for RPC-level tests
/// where the node under test must never start its own elections.
pub fn passive_config(id: NodeId, peers: Vec<NodeId>) -> RaftConfig {
    let mut config = RaftConfig::new(id, peers);
    config.election_timeout_min = Duration::from_secs(3600);
    config.election_timeout_max = Duration::from_secs(7200);
    config
}

/// A simulated cluster. Everything mutable lives behind interior
/// mutability so that concurrent workload/nemesis tasks can share the
/// cluster via `Arc` and crash/restart nodes mid-run (phase 10), and so
/// membership tests can add/remove nodes mid-run (phase 15).
pub struct TestCluster {
    pub net: SimNetwork,
    nodes: Mutex<Vec<(NodeId, Arc<RaftHandle>)>>,
    stores: Mutex<Vec<(NodeId, Arc<KvStore>)>>,
    dirs: Mutex<Vec<(NodeId, TempDir)>>,
    seed: u64,
    /// Cluster size at spawn: the size of the BOOTSTRAP membership. Restarts
    /// derive their bootstrap config from this, never from the current node
    /// count — a node added later must not silently widen what a restarted
    /// original would bootstrap with (its log/snapshot carries the real
    /// membership anyway).
    initial_n: u64,
    /// Bumped on every restart so a reborn node gets fresh timeout jitter.
    incarnation: AtomicU64,
    /// Nodes crashed via [`Self::crash`] and not yet restarted. Their status
    /// watch freezes at the last published value, so workloads must exclude
    /// them from leader sampling.
    crashed: Mutex<HashSet<NodeId>>,
    /// Nodes spawned via [`Self::add_node`] (phase 15), with the snapshot
    /// settings they were given: a restart must rebuild them in join mode
    /// with the SAME settings (same reborn-node-divergence reasoning as the
    /// thresholds below).
    joined: Mutex<HashMap<NodeId, (Option<u64>, u64)>>,
    /// Every node's `RaftConfig.snapshot_threshold` (phase 14). Stored so
    /// [`Self::restart`] rebuilds the node with the SAME threshold — a
    /// reborn node silently reverting to `None` would diverge from the
    /// scenario under test.
    snapshot_threshold: Option<u64>,
    /// Every node's `RaftConfig.snapshot_trailing` (phase-14 amendment),
    /// preserved across restarts for the same reason.
    snapshot_trailing: u64,
    /// Every node's `RaftConfig.test_disable_reconfig_gates` (phase 18,
    /// harness-only), preserved across restarts/joins like the snapshot
    /// knobs. Default false everywhere; only the Ongaro-schedule tests
    /// switch it on.
    disable_reconfig_gates: bool,
}

/// Spawns nodes 1..=n; `prepare` can pre-populate each node's storage
/// (pre-existing log/term) before the node starts, and `snapshot_threshold`
/// switches on log compaction (phase 14; `None` = off, the default
/// everywhere so seeded schedules stay pinned).
pub fn spawn_cluster_full(
    n: u64,
    seed: u64,
    faults: FaultConfig,
    snapshot_threshold: Option<u64>,
    snapshot_trailing: u64,
    prepare: impl Fn(NodeId, &mut Storage),
) -> TestCluster {
    spawn_cluster_gated(
        n,
        seed,
        faults,
        snapshot_threshold,
        snapshot_trailing,
        false,
        prepare,
    )
}

/// [`spawn_cluster_full`] with the phase-18 harness switch: every node runs
/// with `RaftConfig.test_disable_reconfig_gates` set, so the Ongaro
/// disjoint-majority schedule becomes constructible. Only the expected-unsafe
/// leg of that test may use it.
pub fn spawn_cluster_without_reconfig_gates(n: u64, seed: u64, faults: FaultConfig) -> TestCluster {
    spawn_cluster_gated(n, seed, faults, None, 0, true, |_, _| {})
}

fn spawn_cluster_gated(
    n: u64,
    seed: u64,
    faults: FaultConfig,
    snapshot_threshold: Option<u64>,
    snapshot_trailing: u64,
    disable_reconfig_gates: bool,
    prepare: impl Fn(NodeId, &mut Storage),
) -> TestCluster {
    let net = SimNetwork::new(seed, faults);
    let mut nodes = Vec::new();
    let mut stores = Vec::new();
    let mut dirs = Vec::new();
    for id in 1..=n {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut storage = Storage::open(dir.path()).expect("storage");
        prepare(id, &mut storage);
        let (transport, inbound) = net.register(id);
        let store = Arc::new(KvStore::new());
        let mut config = node_config(id, n, seed);
        config.snapshot_threshold = snapshot_threshold;
        config.snapshot_trailing = snapshot_trailing;
        config.test_disable_reconfig_gates = disable_reconfig_gates;
        nodes.push((
            id,
            Arc::new(RaftNode::spawn(
                config,
                storage,
                transport,
                inbound,
                store.clone() as Arc<dyn StateMachine>,
            )),
        ));
        stores.push((id, store));
        dirs.push((id, dir));
    }
    TestCluster {
        net,
        nodes: Mutex::new(nodes),
        stores: Mutex::new(stores),
        dirs: Mutex::new(dirs),
        seed,
        initial_n: n,
        incarnation: AtomicU64::new(0),
        crashed: Mutex::new(HashSet::new()),
        joined: Mutex::new(HashMap::new()),
        snapshot_threshold,
        snapshot_trailing,
        disable_reconfig_gates,
    }
}

/// A placeholder address-book entry for sim-transport members: the sim
/// routes by id, so the addresses are only ever carried, never dialed.
pub fn member_addr(id: NodeId) -> MemberAddr {
    MemberAddr {
        raft: format!("node{id}:9080"),
        client: format!("http://node{id}:8080"),
    }
}

pub fn spawn_cluster_with(
    n: u64,
    seed: u64,
    faults: FaultConfig,
    prepare: impl Fn(NodeId, &mut Storage),
) -> TestCluster {
    spawn_cluster_full(n, seed, faults, None, 0, prepare)
}

pub fn spawn_cluster_with_threshold(
    n: u64,
    seed: u64,
    faults: FaultConfig,
    snapshot_threshold: Option<u64>,
) -> TestCluster {
    spawn_cluster_full(n, seed, faults, snapshot_threshold, 0, |_, _| {})
}

/// Like [`spawn_cluster_with_threshold`], with a trailing window: the
/// boundary stays at least `snapshot_trailing` applies behind, so peers
/// lagging by less catch up via entries instead of InstallSnapshot.
pub fn spawn_cluster_with_trailing(
    n: u64,
    seed: u64,
    faults: FaultConfig,
    snapshot_threshold: Option<u64>,
    snapshot_trailing: u64,
) -> TestCluster {
    spawn_cluster_full(
        n,
        seed,
        faults,
        snapshot_threshold,
        snapshot_trailing,
        |_, _| {},
    )
}

pub fn spawn_cluster(n: u64, seed: u64, faults: FaultConfig) -> TestCluster {
    spawn_cluster_with(n, seed, faults, |_, _| {})
}

impl TestCluster {
    /// The node's local KV state machine (what committed entries applied to).
    /// An `Arc` clone: a crashed node's store stays readable (frozen), a
    /// restarted node's is a fresh one.
    pub fn store(&self, id: NodeId) -> Arc<KvStore> {
        self.lock_stores()
            .iter()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1
            .clone()
    }

    /// The node's current handle (an `Arc` clone — after a restart this is
    /// the new incarnation's handle; a stale clone held across a restart
    /// still points at the dead task and errors on use).
    pub fn handle(&self, id: NodeId) -> Arc<RaftHandle> {
        self.lock_nodes()
            .iter()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1
            .clone()
    }

    pub fn statuses_among(&self, ids: &[NodeId]) -> Vec<Status> {
        self.lock_nodes()
            .iter()
            .filter(|(id, _)| ids.contains(id))
            .map(|(_, h)| h.status())
            .collect()
    }

    pub fn all_ids(&self) -> Vec<NodeId> {
        self.lock_dirs().iter().map(|(id, _)| *id).collect()
    }

    /// All nodes not currently crashed. Workloads sample leaders from these:
    /// a crashed node's status watch is frozen and may still claim Leader.
    pub fn alive_ids(&self) -> Vec<NodeId> {
        let crashed = self.lock_crashed();
        self.all_ids()
            .into_iter()
            .filter(|id| !crashed.contains(id))
            .collect()
    }

    fn lock_nodes(&self) -> std::sync::MutexGuard<'_, Vec<(NodeId, Arc<RaftHandle>)>> {
        self.nodes.lock().expect("nodes lock poisoned")
    }

    fn lock_stores(&self) -> std::sync::MutexGuard<'_, Vec<(NodeId, Arc<KvStore>)>> {
        self.stores.lock().expect("stores lock poisoned")
    }

    fn lock_crashed(&self) -> std::sync::MutexGuard<'_, HashSet<NodeId>> {
        self.crashed.lock().expect("crashed lock poisoned")
    }

    fn lock_dirs(&self) -> std::sync::MutexGuard<'_, Vec<(NodeId, TempDir)>> {
        self.dirs.lock().expect("dirs lock poisoned")
    }

    fn lock_joined(&self) -> std::sync::MutexGuard<'_, HashMap<NodeId, (Option<u64>, u64)>> {
        self.joined.lock().expect("joined lock poisoned")
    }

    /// Waits (virtual time) until exactly one of `ids` reports Leader.
    pub async fn wait_for_leader_among(&self, ids: &[NodeId]) -> Status {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let leaders: Vec<Status> = self
                    .statuses_among(ids)
                    .into_iter()
                    .filter(|s| s.role == RoleKind::Leader)
                    .collect();
                if leaders.len() == 1 {
                    return leaders[0];
                }
                tokio::time::sleep(ms(5)).await;
            }
        })
        .await
        .expect("no leader elected within 30s of virtual time")
    }

    pub async fn wait_for_leader(&self) -> Status {
        self.wait_for_leader_among(&self.all_ids()).await
    }

    /// Stops every node, then asserts the sim's event-level safety checks:
    /// Election Safety, Log Matching, and AppendEntries well-formedness,
    /// observed on every message that crossed the network the whole run.
    pub fn shutdown(&self) {
        for (_, handle) in self.lock_nodes().iter() {
            handle.shutdown();
        }
        let violations = self.net.safety_violations();
        assert!(
            violations.is_empty(),
            "sim-observed safety violations during the run:\n{}",
            violations.join("\n")
        );
    }

    /// Hard-kills node `id`: its task is aborted and its transport inbox
    /// becomes a black hole. Its status watch freezes at the last value —
    /// sample leaders via [`Self::alive_ids`] while anything is down.
    pub fn crash(&self, id: NodeId) {
        self.handle(id).crash();
        self.lock_crashed().insert(id);
    }

    /// Restarts a crashed node from its data directory with a fresh (empty)
    /// state machine — the KV state is rebuilt by re-applying the log once
    /// the commit index is re-learned. Sleeps briefly first so the aborted
    /// task has definitely been dropped and released its file handles.
    /// A node originally spawned via [`Self::add_node`] is rebuilt in join
    /// mode with its original snapshot settings.
    pub async fn restart(&self, id: NodeId) {
        tokio::time::sleep(ms(20)).await;
        let incarnation = self.incarnation.fetch_add(1, Ordering::SeqCst) + 1;
        let storage = {
            let dirs = self.lock_dirs();
            let dir = &dirs
                .iter()
                .find(|(nid, _)| *nid == id)
                .expect("no such node")
                .1;
            Storage::open(dir.path()).expect("reopen storage")
        };
        let (transport, inbound) = self.net.register(id);
        let store = Arc::new(KvStore::new());
        let mut config = node_config(id, self.initial_n, self.seed);
        config.timeout_seed ^= incarnation << 32;
        config.test_disable_reconfig_gates = self.disable_reconfig_gates;
        if let Some(&(threshold, trailing)) = self.lock_joined().get(&id) {
            config.join = true;
            config.snapshot_threshold = threshold;
            config.snapshot_trailing = trailing;
        } else {
            config.snapshot_threshold = self.snapshot_threshold;
            config.snapshot_trailing = self.snapshot_trailing;
        }
        let handle = Arc::new(RaftNode::spawn(
            config,
            storage,
            transport,
            inbound,
            store.clone() as Arc<dyn StateMachine>,
        ));
        self.lock_nodes()
            .iter_mut()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1 = handle;
        self.lock_stores()
            .iter_mut()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1 = store;
        self.lock_crashed().remove(&id);
    }

    /// Spawns a brand-new node in JOIN mode (phase 15): empty bootstrap
    /// membership, so it stays completely silent until a committed
    /// ConfigChange includes it — the caller proposes that change
    /// separately (the complete new membership, e.g. via
    /// [`RaftHandle::membership`] + insert). `snapshot_threshold`/`trailing`
    /// are per-joiner so tests can pick catch-up discriminators (a joiner
    /// that never self-compacts can only own a snapshot via
    /// InstallSnapshot).
    pub fn add_node_with(&self, id: NodeId, threshold: Option<u64>, trailing: u64) {
        assert!(
            !self.all_ids().contains(&id),
            "node {id} already exists in the harness"
        );
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = Storage::open(dir.path()).expect("storage");
        let (transport, inbound) = self.net.register(id);
        let store = Arc::new(KvStore::new());
        let mut config = node_config(id, self.initial_n, self.seed);
        config.join = true;
        config.snapshot_threshold = threshold;
        config.snapshot_trailing = trailing;
        config.test_disable_reconfig_gates = self.disable_reconfig_gates;
        let handle = Arc::new(RaftNode::spawn(
            config,
            storage,
            transport,
            inbound,
            store.clone() as Arc<dyn StateMachine>,
        ));
        self.lock_nodes().push((id, handle));
        self.lock_stores().push((id, store));
        self.lock_dirs().push((id, dir));
        self.lock_joined().insert(id, (threshold, trailing));
    }

    /// [`Self::add_node_with`] inheriting the cluster's snapshot settings.
    pub fn add_node(&self, id: NodeId) {
        self.add_node_with(id, self.snapshot_threshold, self.snapshot_trailing);
    }

    /// Tears a node out of the harness: shuts it down and forgets it (its
    /// temp dir is dropped). Call AFTER the ConfigChange removing it has
    /// committed — this is harness bookkeeping, not the removal itself.
    pub fn remove_node(&self, id: NodeId) {
        self.handle(id).shutdown();
        self.lock_nodes().retain(|(nid, _)| *nid != id);
        self.lock_stores().retain(|(nid, _)| *nid != id);
        self.lock_dirs().retain(|(nid, _)| *nid != id);
        self.lock_crashed().remove(&id);
        self.lock_joined().remove(&id);
    }

    /// Reads a node's log back from disk. Only call after the node has been
    /// shut down or crashed — a live node owns append handles to these files
    /// and replay-repair could race its writes.
    pub fn disk_log(&self, id: NodeId) -> Vec<LogEntry> {
        let dirs = self.lock_dirs();
        let dir = &dirs
            .iter()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1;
        Storage::open(dir.path())
            .expect("reopen storage")
            .entries()
            .to_vec()
    }

    /// Reads a node's snapshot back from disk (`None` if it never
    /// compacted). Same caveat as [`Self::disk_log`]: only after the node
    /// has been shut down or crashed.
    pub fn disk_snapshot(&self, id: NodeId) -> Option<Snapshot> {
        let dirs = self.lock_dirs();
        let dir = &dirs
            .iter()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1;
        Storage::open(dir.path())
            .expect("reopen storage")
            .snapshot()
            .cloned()
    }
}

/// Polls `pred` every 5ms of virtual time; panics after 60 virtual seconds.
pub async fn wait_until(what: &str, mut pred: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if pred() {
                return;
            }
            tokio::time::sleep(ms(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out (60s virtual) waiting for: {what}"))
}
