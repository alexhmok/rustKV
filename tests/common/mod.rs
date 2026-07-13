//! Shared helpers for the seeded cluster tests (election, replication, and
//! later fault tests). This module is compiled separately into each test
//! binary, so not every binary uses every item.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use rustkv::raft::Storage;
use rustkv::raft::node::{RaftConfig, RaftHandle, RaftNode, RoleKind, StateMachine, Status};
use rustkv::raft::transport::sim::{FaultConfig, SimNetwork};
use rustkv::raft::types::{Command, LogEntry, LogIndex, NodeId, Term};
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

pub struct TestCluster {
    pub net: SimNetwork,
    pub nodes: Vec<(NodeId, RaftHandle)>,
    pub stores: Vec<(NodeId, Arc<KvStore>)>,
    dirs: Vec<(NodeId, TempDir)>,
    seed: u64,
    /// Bumped on every restart so a reborn node gets fresh timeout jitter.
    incarnation: u64,
}

/// Spawns nodes 1..=n; `prepare` can pre-populate each node's storage
/// (pre-existing log/term) before the node starts.
pub fn spawn_cluster_with(
    n: u64,
    seed: u64,
    faults: FaultConfig,
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
        nodes.push((
            id,
            RaftNode::spawn(
                node_config(id, n, seed),
                storage,
                transport,
                inbound,
                store.clone() as Arc<dyn StateMachine>,
            ),
        ));
        stores.push((id, store));
        dirs.push((id, dir));
    }
    TestCluster {
        net,
        nodes,
        stores,
        dirs,
        seed,
        incarnation: 0,
    }
}

pub fn spawn_cluster(n: u64, seed: u64, faults: FaultConfig) -> TestCluster {
    spawn_cluster_with(n, seed, faults, |_, _| {})
}

impl TestCluster {
    /// The node's local KV state machine (what committed entries applied to).
    pub fn store(&self, id: NodeId) -> &Arc<KvStore> {
        &self
            .stores
            .iter()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1
    }

    pub fn handle(&self, id: NodeId) -> &RaftHandle {
        &self
            .nodes
            .iter()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1
    }

    pub fn statuses_among(&self, ids: &[NodeId]) -> Vec<Status> {
        self.nodes
            .iter()
            .filter(|(id, _)| ids.contains(id))
            .map(|(_, h)| h.status())
            .collect()
    }

    pub fn all_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|(id, _)| *id).collect()
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

    pub fn shutdown(&self) {
        for (_, handle) in &self.nodes {
            handle.shutdown();
        }
    }

    /// Hard-kills node `id`: its task is aborted and its transport inbox
    /// becomes a black hole. Its status watch freezes at the last value, so
    /// exclude crashed nodes from invariant sampling.
    pub fn crash(&self, id: NodeId) {
        self.handle(id).crash();
    }

    /// Restarts a crashed node from its data directory with a fresh (empty)
    /// state machine — the KV state is rebuilt by re-applying the log once
    /// the commit index is re-learned. Sleeps briefly first so the aborted
    /// task has definitely been dropped and released its file handles.
    pub async fn restart(&mut self, id: NodeId) {
        tokio::time::sleep(ms(20)).await;
        self.incarnation += 1;
        let n = self.nodes.len() as u64;
        let dir = &self
            .dirs
            .iter()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1;
        let storage = Storage::open(dir.path()).expect("reopen storage");
        let (transport, inbound) = self.net.register(id);
        let store = Arc::new(KvStore::new());
        let mut config = node_config(id, n, self.seed);
        config.timeout_seed ^= self.incarnation << 32;
        let handle = RaftNode::spawn(
            config,
            storage,
            transport,
            inbound,
            store.clone() as Arc<dyn StateMachine>,
        );
        self.nodes
            .iter_mut()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1 = handle;
        self.stores
            .iter_mut()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1 = store;
    }

    /// Reads a node's log back from disk. Only call after the node has been
    /// shut down or crashed — a live node owns append handles to these files
    /// and replay-repair could race its writes.
    pub fn disk_log(&self, id: NodeId) -> Vec<LogEntry> {
        let dir = &self
            .dirs
            .iter()
            .find(|(nid, _)| *nid == id)
            .expect("no such node")
            .1;
        Storage::open(dir.path())
            .expect("reopen storage")
            .entries()
            .to_vec()
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
