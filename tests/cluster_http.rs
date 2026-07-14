//! End-to-end tests of a 3-node cluster: real axum HTTP servers for the
//! client API, simulated transport between the Raft nodes.
//!
//! Covered: leader writes visible on every node (via `?stale=true` local
//! reads — the point is per-node replication, not read semantics); follower
//! redirect for writes AND linearizable GETs (raw 307 with Location, and
//! reqwest auto-following it); delete through a redirect; a
//! minority-partitioned leader answering 504 for writes and linearizable
//! reads without acknowledging either (CP at the HTTP level), the doomed
//! key never appearing; `/cluster/status`.
//! NOT covered: node-to-node HTTP transport (phase 7).
//! These tests run in real time (real sockets don't mix with paused time);
//! waits are poll-based and agreement-based, not seed-exact.

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

const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// These tests run in real time; concurrent heavyweight tests (including
/// other test binaries — cargo runs them in parallel processes) can starve
/// this process long enough to depose a leader. Two mitigations: tests in
/// this binary serialize on this lock, and election timeouts are generous
/// (200-400ms) so only a >400ms starvation gap can cause churn.
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
        config.election_timeout_min = Duration::from_millis(200);
        config.election_timeout_max = Duration::from_millis(400);
        config.heartbeat_interval = Duration::from_millis(50);
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
            peer_urls: Arc::new(std::sync::RwLock::new(urls.clone())),
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
    /// Waits until exactly one node is leader AND every node agrees on it —
    /// so follower requests reliably redirect to the right place.
    async fn wait_for_leader(&self) -> &HttpNode {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let statuses: Vec<_> = self.nodes.iter().map(|n| n.kv.status()).collect();
            let leaders: Vec<_> = statuses
                .iter()
                .filter(|s| s.role == RoleKind::Leader)
                .collect();
            if let [leader] = leaders[..]
                && statuses.iter().all(|s| s.leader_id == Some(leader.id))
            {
                return self.nodes.iter().find(|n| n.id == leader.id).unwrap();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no agreed leader within 15s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn followers(&self, leader: NodeId) -> Vec<&HttpNode> {
        self.nodes.iter().filter(|n| n.id != leader).collect()
    }

    /// Sends a raw (redirects disabled) request to a CURRENT follower and
    /// returns once it observes the 307 with a Location matching the
    /// CURRENT leader. Leadership is re-sampled per attempt: under parallel
    /// test load a step-down/re-election can land between sampling and the
    /// request (the documented CPU-starvation flake class), in which case
    /// the follower legitimately answers 503 or points at a newer leader —
    /// retried within a bounded deadline rather than hard-asserted.
    async fn assert_follower_redirects(&self, path: &str, body: Option<&Value>) {
        let raw = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let leader = self.wait_for_leader().await;
            let follower = self.followers(leader.id)[0];
            let request = match body {
                Some(value) => raw.put(format!("{}/{path}", follower.url)).json(value),
                None => raw.put(format!("{}/{path}", follower.url)),
            };
            let resp = request.send().await.unwrap();
            let location = resp
                .headers()
                .get("location")
                .and_then(|l| l.to_str().ok())
                .map(str::to_string);
            if resp.status() == 307
                && location.as_deref() == Some(&format!("{}/{path}", leader.url))
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no stable 307-to-leader observed for {path}: last status {} location {location:?}",
                resp.status()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Polls a LOCAL (`?stale=true`) GET on `url/key` until the expected outcome
/// (Some(value) or None for 404) holds; panics after 5 real seconds. Local
/// on purpose: these waits verify per-node replication, and a linearizable
/// GET would redirect to the leader and prove nothing about this node.
async fn wait_for_local_get(
    client: &reqwest::Client,
    url: &str,
    key: &str,
    expect: Option<&Value>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resp = client
            .get(format!("{url}/{key}?stale=true"))
            .send()
            .await
            .unwrap();
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
        wait_for_local_get(&client, &node.url, "loc", Some(&value)).await;
    }
}

#[tokio::test]
async fn follower_redirects_writes_to_the_leader() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 32).await;
    let value = json!({"via": "follower"});

    // Raw redirect: 307 with a Location pointing at the leader
    // (leadership-churn-tolerant probe; see assert_follower_redirects).
    cluster
        .assert_follower_redirects("redirected", Some(&value))
        .await;

    // A standard client follows the 307 (re-sending the PUT body) and lands
    // the write. Retried within a deadline: a re-election mid-flight can
    // surface as a retryable 503.
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader = cluster.wait_for_leader().await;
        let follower = cluster.followers(leader.id)[0];
        let put = client
            .put(format!("{}/redirected", follower.url))
            .json(&value)
            .send()
            .await
            .unwrap();
        if put.status() == 201 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "redirected PUT never landed: last status {}",
            put.status()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for node in &cluster.nodes {
        wait_for_local_get(&client, &node.url, "redirected", Some(&value)).await;
    }
}

#[tokio::test]
async fn follower_redirects_linearizable_reads_and_serves_status() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 35).await;
    let leader = cluster.wait_for_leader().await;
    let follower = cluster.followers(leader.id)[0];
    let client = reqwest::Client::new();
    let value = json!("fresh");

    let put = client
        .put(format!("{}/lin", leader.url))
        .json(&value)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    // Raw linearizable GET on a follower: 307 to the leader.
    let raw = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let resp = raw
        .get(format!("{}/lin", follower.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 307);
    let location = resp.headers()["location"].to_str().unwrap();
    assert_eq!(location, format!("{}/lin", leader.url));

    // A standard client follows it and gets the committed value; a miss
    // through the same path is a 404 from the leader.
    let got = client
        .get(format!("{}/lin", follower.url))
        .send()
        .await
        .unwrap();
    assert_eq!(got.status(), 200);
    assert_eq!(got.json::<Value>().await.unwrap(), value);
    let miss = client
        .get(format!("{}/absent", follower.url))
        .send()
        .await
        .unwrap();
    assert_eq!(miss.status(), 404);

    // /cluster/status reports the raft view without shadowing any key.
    let status = client
        .get(format!("{}/cluster/status", leader.url))
        .send()
        .await
        .unwrap();
    assert_eq!(status.status(), 200);
    let body = status.json::<Value>().await.unwrap();
    assert_eq!(body["id"], json!(leader.id));
    assert_eq!(body["role"], json!("Leader"));
    assert_eq!(body["leader_id"], json!(leader.id));
}

#[tokio::test]
async fn delete_through_a_follower_redirect() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 33).await;
    let leader = cluster.wait_for_leader().await;
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
    wait_for_local_get(&client, &follower.url, "gone", Some(&value)).await;

    let del = client
        .delete(format!("{}/gone", follower.url))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 204);
    for node in &cluster.nodes {
        wait_for_local_get(&client, &node.url, "gone", None).await;
    }
}

/// Phase 15: the cluster admin API. GET is served locally by any node;
/// PUT/DELETE are leader operations (307 from followers, like writes);
/// malformed bodies 400, unknown members 404, invalid deltas 409.
#[tokio::test]
async fn admin_membership_endpoints_crud_and_redirect() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 36).await;
    let leader = cluster.wait_for_leader().await;
    let client = reqwest::Client::new();

    // GET: any node answers with its own view of the bootstrap membership.
    for node in &cluster.nodes {
        let resp = client
            .get(format!("{}/cluster/members", node.url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body = resp.json::<Value>().await.unwrap();
        let members = body.as_object().unwrap();
        assert_eq!(members.len(), 3, "bootstrap membership");
        for id in ["1", "2", "3"] {
            assert!(members.contains_key(id), "missing member {id}");
        }
    }

    // Raw PUT on a follower: 307 with a Location on the leader
    // (leadership-churn-tolerant probe; see assert_follower_redirects).
    // NOTE: if a churn race lands the probe on a fresh leader it may
    // EXECUTE the add — the steps below tolerate "already done".
    let addr = json!({"raft": "127.0.0.1:1", "client": "http://127.0.0.1:1"});
    cluster
        .assert_follower_redirects("cluster/members/4", Some(&addr))
        .await;

    // Malformed body: 400, nothing proposed (leadership-independent: the
    // body is rejected before any write is attempted).
    let resp = client
        .put(format!("{}/cluster/members/4", leader.url))
        .body("not json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Add member 4 (no such process runs — the change itself commits with
    // 3 of the new 4 acking). A follower with redirects lands it too.
    // Bounded retry: mid-flight re-elections surface as retryable 503s and
    // no-op-gate 409s; the authoritative "it landed" signal is the
    // membership view (a 409 may also mean an earlier ambiguous attempt —
    // including the redirect probe above — already added it).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader_now = cluster.wait_for_leader().await;
        let follower_now = cluster.followers(leader_now.id)[0];
        let put = client
            .put(format!("{}/cluster/members/4", follower_now.url))
            .json(&addr)
            .send()
            .await
            .unwrap();
        let status = put.status();
        let view = client
            .get(format!("{}/cluster/members", leader_now.url))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        if view.as_object().unwrap().contains_key("4") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "add-member never landed: last status {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Every node's local view converges to the 4-member config.
    for node in &cluster.nodes {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let body = client
                .get(format!("{}/cluster/members", node.url))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            let members = body.as_object().unwrap();
            if members.len() == 4 && body["4"]["raft"] == json!("127.0.0.1:1") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "node {} never saw the 4-member config: {body}",
                node.id
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // Re-adding an existing member (an address change) is refused: 409.
    let resp = client
        .put(format!("{}/cluster/members/4", leader.url))
        .json(&addr)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    // Removing an unknown member: 404.
    let resp = client
        .delete(format!("{}/cluster/members/9", leader.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Remove member 4 through a follower redirect: view back to 3. Same
    // churn-tolerant shape as the add above (a 404 means an earlier
    // ambiguous attempt already removed it).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader_now = cluster.wait_for_leader().await;
        let follower_now = cluster.followers(leader_now.id)[0];
        let del = client
            .delete(format!("{}/cluster/members/4", follower_now.url))
            .send()
            .await
            .unwrap();
        let status = del.status();
        let view = client
            .get(format!("{}/cluster/members", leader_now.url))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        if !view.as_object().unwrap().contains_key("4") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "remove-member never landed: last status {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The cluster still serves ordinary writes after the round trip
    // (through whichever node leads now; redirects are followed).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader_now = cluster.wait_for_leader().await;
        let put = client
            .put(format!("{}/still-works", leader_now.url))
            .json(&json!(true))
            .send()
            .await
            .unwrap();
        if put.status() == 201 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "post-churn write never landed: last status {}",
            put.status()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn minority_partitioned_leader_times_out_writes_and_never_applies_them() {
    let _serial = SERIAL.lock().await;
    let cluster = spawn_http_cluster(3, 34).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Baseline write so the cluster provably works before the partition.
    // Redirects are DISABLED so a 201 proves the sampled node itself
    // served the write as leader — churn between sampling and the request
    // (the documented starvation flake class) surfaces as 307/503 and is
    // retried with a fresh sample instead of hard-asserted.
    let raw = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let leader = loop {
        let leader_now = cluster.wait_for_leader().await;
        let put = raw
            .put(format!("{}/alive", leader_now.url))
            .json(&json!(true))
            .send()
            .await
            .unwrap();
        if put.status() == 201 {
            break leader_now;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "baseline write never landed on a stable leader: last status {}",
            put.status()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Cut the leader off from both followers: it is now a minority of one.
    let follower_ids: Vec<NodeId> = cluster.followers(leader.id).iter().map(|n| n.id).collect();
    for &id in &follower_ids {
        cluster.net.set_pair_blocked(leader.id, id, true);
    }

    // CP: the write must NOT be acknowledged — 504 after the write timeout,
    // and the key must not be locally applied anywhere.
    let resp = client
        .put(format!("{}/doomed", leader.url))
        .json(&json!(9))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 504);
    for node in &cluster.nodes {
        let get = client
            .get(format!("{}/doomed?stale=true", node.url))
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

    // Reads are CP too (phase 9): the old leader must never serve the key it
    // holds. By now CheckQuorum (phase 12) has long deposed it — the doomed
    // PUT's 2s wait dwarfs the ~400ms step-down window — so instead of
    // hanging into a 504 it answers 503 immediately (deposed, no leader
    // known, retryable) while a stale read still answers locally.
    let get = client
        .get(format!("{}/alive", leader.url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.status(),
        503,
        "the self-deposed leader must refuse the linearizable read, not serve it"
    );
    let stale = client
        .get(format!("{}/alive?stale=true", leader.url))
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), 200);

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
        wait_for_local_get(&client, &node.url, "after-partition", Some(&json!("ok"))).await;
        wait_for_local_get(&client, &node.url, "doomed", None).await;
        wait_for_local_get(&client, &node.url, "alive", Some(&json!(true))).await;
    }
}
