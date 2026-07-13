//! Shared helpers for the seeded cluster tests (election, replication, and
//! later fault tests). This module is compiled separately into each test
//! binary, so not every binary uses every item.
#![allow(dead_code)]

use std::time::Duration;

use rustkv::raft::Storage;
use rustkv::raft::node::{RaftConfig, RaftHandle, RaftNode, RoleKind, Status};
use rustkv::raft::transport::sim::{FaultConfig, SimNetwork};
use rustkv::raft::types::{Command, LogEntry, LogIndex, NodeId, Term};
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
    dirs: Vec<(NodeId, TempDir)>,
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
    let mut dirs = Vec::new();
    for id in 1..=n {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut storage = Storage::open(dir.path()).expect("storage");
        prepare(id, &mut storage);
        let (transport, inbound) = net.register(id);
        nodes.push((
            id,
            RaftNode::spawn(node_config(id, n, seed), storage, transport, inbound),
        ));
        dirs.push((id, dir));
    }
    TestCluster { net, nodes, dirs }
}

pub fn spawn_cluster(n: u64, seed: u64, faults: FaultConfig) -> TestCluster {
    spawn_cluster_with(n, seed, faults, |_, _| {})
}

impl TestCluster {
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
