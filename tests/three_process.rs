//! The full deployment shape: three separate OS processes of the actual
//! `rustkv` binary forming a cluster over the real HTTP transport, driven
//! purely through the client HTTP API.
//!
//! Covered: cluster formation, a write through any node (following
//! redirects) visible on all three, kill -9 of the leader process with
//! continued writes on the survivors, and the killed node rejoining from
//! its data directory with full state.
//! NOT covered: OS-level network partitions (that is what the Docker
//! Compose setup in the README is for).
//! Real-time test; generous poll-based waits.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;

struct ProcessCluster {
    children: HashMap<u64, Child>,
    client_urls: HashMap<u64, String>,
    base_port: u16,
    _dir: TempDir,
}

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn spawn_node(dir: &TempDir, base_port: u16, id: u64) -> Child {
    let client_port = base_port + id as u16;
    let raft_port = base_port + 100 + id as u16;
    let peers: Vec<String> = (1..=3)
        .filter(|&j| j != id)
        .map(|j| format!("{j}=127.0.0.1:{}", base_port + 100 + j as u16))
        .collect();
    let urls: Vec<String> = (1..=3)
        .filter(|&j| j != id)
        .map(|j| format!("{j}=http://127.0.0.1:{}", base_port + j as u16))
        .collect();
    Command::new(env!("CARGO_BIN_EXE_rustkv"))
        .env("RUSTKV_NODE_ID", id.to_string())
        .env("RUSTKV_LISTEN", format!("127.0.0.1:{client_port}"))
        .env("RUSTKV_RAFT_LISTEN", format!("127.0.0.1:{raft_port}"))
        .env("RUSTKV_DATA_DIR", dir.path().join(format!("node{id}")))
        .env("RUSTKV_PEERS", peers.join(","))
        .env("RUSTKV_PEER_CLIENT_URLS", urls.join(","))
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn rustkv process")
}

fn spawn_cluster(base_port: u16) -> ProcessCluster {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut children = HashMap::new();
    let mut client_urls = HashMap::new();
    for id in 1..=3u64 {
        children.insert(id, spawn_node(&dir, base_port, id));
        client_urls.insert(id, format!("http://127.0.0.1:{}", base_port + id as u16));
    }
    ProcessCluster {
        children,
        client_urls,
        base_port,
        _dir: dir,
    }
}

/// Finds the leader by asking a follower for a redirect (or getting a 201
/// directly). Returns the client URL that accepted a probe write.
async fn wait_for_working_write_path(
    client: &reqwest::Client,
    urls: &[String],
    probe_key: &str,
) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        for url in urls {
            let response = client
                .put(format!("{url}/{probe_key}"))
                .json(&json!("probe"))
                .send()
                .await;
            if let Ok(response) = response
                && response.status() == 201
            {
                return url.clone();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no node accepted a write within 20s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_value(client: &reqwest::Client, url: &str, key: &str, expect: &Value) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(response) = client.get(format!("{url}/{key}")).send().await
            && response.status() == 200
            && &response.json::<Value>().await.unwrap() == expect
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "GET {url}/{key} never returned {expect}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn three_process_cluster_survives_leader_kill_and_rejoin() {
    // Fixed ports (CARGO_BIN_EXE processes can't report ephemeral ports);
    // chosen high to avoid collisions.
    let mut cluster = spawn_cluster(21830);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let urls: Vec<String> = (1..=3).map(|id| cluster.client_urls[&id].clone()).collect();

    // A write through any node (redirects included) lands everywhere.
    wait_for_working_write_path(&client, &urls, "probe-1").await;
    let value = json!({"phase": 7});
    let put = client
        .put(format!("{}/durable", urls[0]))
        .json(&value)
        .send()
        .await
        .unwrap();
    assert!(
        put.status() == 201 || put.status() == 307,
        "unexpected status {}",
        put.status()
    );
    for url in &urls {
        wait_for_value(&client, url, "durable", &value).await;
    }

    // Identify and kill the leader process (the node that answers a write
    // directly with 201 must be the leader).
    let no_redirect = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut leader_id = None;
    for id in 1..=3u64 {
        let response = no_redirect
            .put(format!("{}/leader-probe", cluster.client_urls[&id]))
            .json(&json!(1))
            .send()
            .await
            .unwrap();
        if response.status() == 201 {
            leader_id = Some(id);
            break;
        }
    }
    let leader_id = leader_id.expect("some node must answer writes directly");
    let mut leader_process = cluster.children.remove(&leader_id).unwrap();
    leader_process.kill().unwrap();
    leader_process.wait().unwrap();

    // Survivors elect a new leader and keep serving writes.
    let survivor_urls: Vec<String> = (1..=3u64)
        .filter(|id| *id != leader_id)
        .map(|id| cluster.client_urls[&id].clone())
        .collect();
    wait_for_working_write_path(&client, &survivor_urls, "probe-2").await;
    let after_kill = json!({"after": "kill"});
    let put = client
        .put(format!("{}/after-kill", survivor_urls[0]))
        .json(&after_kill)
        .send()
        .await
        .unwrap();
    assert!(put.status() == 201 || put.status() == 307);
    for url in &survivor_urls {
        wait_for_value(&client, url, "after-kill", &after_kill).await;
    }

    // Restart the killed node from its data dir: it must rejoin and serve
    // both the old and the new values.
    let base_port = cluster.base_port;
    cluster
        .children
        .insert(leader_id, spawn_node(&cluster._dir, base_port, leader_id));
    let rejoined_url = cluster.client_urls[&leader_id].clone();
    wait_for_value(&client, &rejoined_url, "durable", &value).await;
    wait_for_value(&client, &rejoined_url, "after-kill", &after_kill).await;
}
