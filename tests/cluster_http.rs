//! End-to-end tests of a 3-node cluster: real axum HTTP servers for the
//! client API, simulated transport between the Raft nodes.
//!
//! Covered: leader writes visible on every node; follower redirect (raw
//! 307 with Location, and reqwest auto-following it); delete through a
//! redirect; a minority-partitioned leader answering 504 without
//! acknowledging the write (CP at the HTTP level), the doomed key never
//! appearing.
//! NOT covered: node-to-node HTTP transport (phase 7), linearizable reads
//! (GETs are documented as possibly stale).
//! These tests run in real time (real sockets don't mix with paused time),
//! with shortened election timeouts; waits are poll-based, not seed-exact.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{low_loss_faults, node_config};
use rustkv::api::{ApiContext, router};
use rustkv::kv::KvNode;
use rustkv::raft::Storage;
use rustkv::raft::node::{RaftNode, RoleKind, StateMachine};
use rustkv::raft::transport::sim::SimNetwork;
use rustkv::raft::types::NodeId;
use rustkv::store::KvStore;
use serde_json::{Value, json};
use tempfile::TempDir;

const WRITE_TIMEOUT: Duration = Duration::from_millis(800);

/// These tests run in real time with tight election timeouts; running them
/// concurrently starves the runtime and causes spurious leadership churn.
/// Each test holds this lock for its duration.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct HttpNode {
    id: NodeId,
    url: String,
    kv: Arc<KvNode>,
}

struct HttpCluster {
    net: SimNetwork,
    nodes: Vec<HttpNode>,
    _dirs: Vec<TempDir>,
}

async fn spawn_http_cluster(n: u64, seed: u64) -> HttpCluster {
    let net = SimNetwork::new(seed, low_loss_faults());

    // Bind all listeners first so every node can know every client URL.
    let mut listeners = Vec::new();
    let mut urls: HashMap<NodeId, String> = HashMap::new();
    for id in 1..=n {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        urls.insert(
            id,
            format!("http://{}", listener.local_addr().expect("addr")),
        );
        listeners.push((id, listener));
    }

    let mut nodes = Vec::new();
    let mut dirs = Vec::new();
    for (id, listener) in listeners {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(KvStore::new());
        let storage = Storage::open(dir.path()).expect("storage");
        let (transport, inbound) = net.register(id);
        let mut config = node_config(id, n, seed);
        config.election_timeout_min = Duration::from_millis(50);
        config.election_timeout_max = Duration::from_millis(100);
        config.heartbeat_interval = Duration::from_millis(20);
        let raft = RaftNode::spawn(
            config,
            storage,
            transport,
            inbound,
            store.clone() as Arc<dyn StateMachine>,
        );
        let kv = KvNode::new(store, raft, WRITE_TIMEOUT);
        let ctx = Arc::new(ApiContext {
            kv: kv.clone(),
            peer_urls: urls.clone(),
        });
        tokio::spawn(async move {
            axum::serve(listener, router(ctx))
                .await
                .expect("test server error");
        });
        nodes.push(HttpNode {
            id,
            url: urls[&id].clone(),
            kv,
        });
        dirs.push(dir);
    }
    HttpCluster {
        net,
        nodes,
        _dirs: dirs,
    }
}

impl HttpCluster {
    async fn wait_for_leader(&self) -> &HttpNode {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let leaders: Vec<&HttpNode> = self
                .nodes
                .iter()
                .filter(|n| n.kv.status().role == RoleKind::Leader)
                .collect();
            if let [leader] = leaders[..] {
                return leader;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no leader within 10s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn followers(&self, leader: NodeId) -> Vec<&HttpNode> {
        self.nodes.iter().filter(|n| n.id != leader).collect()
    }
}

/// Polls GET on `url/key` until the expected outcome (Some(value) or None
/// for 404) holds; panics after 5 real seconds.
async fn wait_for_get(client: &reqwest::Client, url: &str, key: &str, expect: Option<&Value>) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resp = client.get(format!("{url}/{key}")).send().await.unwrap();
        match expect {
            Some(value) if resp.status() == 200 => {
                if &resp.json::<Value>().await.unwrap() == value {
                    return;
                }
            }
            None if resp.status() == 404 => return,
            _ => {}
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "GET {url}/{key} never reached expected state {expect:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn leader_write_becomes_visible_on_every_node() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 31).await;
    let leader = cluster.wait_for_leader().await;
    let client = reqwest::Client::new();
    let value = json!({"city": "sf"});

    let put = client
        .put(format!("{}/loc", leader.url))
        .json(&value)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    // Applied on the leader synchronously with the 201; followers apply as
    // heartbeats deliver the commit index.
    for node in &cluster.nodes {
        wait_for_get(&client, &node.url, "loc", Some(&value)).await;
    }
}

#[tokio::test]
async fn follower_redirects_writes_to_the_leader() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 32).await;
    let leader = cluster.wait_for_leader().await;
    tokio::time::sleep(Duration::from_millis(100)).await; // followers learn the leader
    let follower = cluster.followers(leader.id)[0];
    let value = json!({"via": "follower"});

    // Raw redirect: 307 with a Location pointing at the leader.
    let raw = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = raw
        .put(format!("{}/redirected", follower.url))
        .json(&value)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 307);
    let location = resp.headers()["location"].to_str().unwrap();
    assert_eq!(location, format!("{}/redirected", leader.url));

    // A standard client follows the 307 (re-sending the PUT body) and lands
    // the write.
    let client = reqwest::Client::new();
    let put = client
        .put(format!("{}/redirected", follower.url))
        .json(&value)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);
    for node in &cluster.nodes {
        wait_for_get(&client, &node.url, "redirected", Some(&value)).await;
    }
}

#[tokio::test]
async fn delete_through_a_follower_redirect() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 33).await;
    let leader = cluster.wait_for_leader().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let follower = cluster.followers(leader.id)[0];
    let client = reqwest::Client::new();
    let value = json!(1);

    let put = client
        .put(format!("{}/gone", leader.url))
        .json(&value)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);
    wait_for_get(&client, &follower.url, "gone", Some(&value)).await;

    let del = client
        .delete(format!("{}/gone", follower.url))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 204);
    for node in &cluster.nodes {
        wait_for_get(&client, &node.url, "gone", None).await;
    }
}

#[tokio::test]
async fn minority_partitioned_leader_times_out_writes_and_never_applies_them() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 34).await;
    let leader = cluster.wait_for_leader().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Baseline write so the cluster provably works before the partition.
    let put = client
        .put(format!("{}/alive", leader.url))
        .json(&json!(true))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    // Cut the leader off from both followers: it is now a minority of one.
    let follower_ids: Vec<NodeId> = cluster.followers(leader.id).iter().map(|n| n.id).collect();
    for &id in &follower_ids {
        cluster.net.set_pair_blocked(leader.id, id, true);
    }

    // CP: the write must NOT be acknowledged — 504 after the write timeout,
    // and the key must not be readable anywhere.
    let resp = client
        .put(format!("{}/doomed", leader.url))
        .json(&json!(9))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 504);
    for node in &cluster.nodes {
        let get = client
            .get(format!("{}/doomed", node.url))
            .send()
            .await
            .unwrap();
        assert_eq!(
            get.status(),
            404,
            "node {}: doomed key must not be visible",
            node.id
        );
    }

    // The majority side elects a new leader and keeps serving writes.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let new_leader_url = loop {
        let new_leader = cluster
            .nodes
            .iter()
            .find(|n| follower_ids.contains(&n.id) && n.kv.status().role == RoleKind::Leader);
        if let Some(node) = new_leader {
            break node.url.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "majority elected no leader"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let put = client
        .put(format!("{new_leader_url}/after-partition"))
        .json(&json!("ok"))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    // Heal: the doomed entry is truncated away, the new write is everywhere.
    for &id in &follower_ids {
        cluster.net.set_pair_blocked(leader.id, id, false);
    }
    for node in &cluster.nodes {
        wait_for_get(&client, &node.url, "after-partition", Some(&json!("ok"))).await;
        wait_for_get(&client, &node.url, "doomed", None).await;
        wait_for_get(&client, &node.url, "alive", Some(&json!(true))).await;
    }
}
