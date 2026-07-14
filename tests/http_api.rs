//! End-to-end tests of the client HTTP contract against a real single-node
//! raft-backed server on an ephemeral port: every write goes through the
//! persisted log and commits (majority of 1) before the response.
//!
//! Covered: the exact status codes of the contract, hit/miss, overwrite,
//! delete idempotency, invalid-JSON rejection, and KV state surviving a
//! restart (log replay + §8 no-op re-commit).
//! NOT covered (stated per project policy): concurrent access beyond
//! RwLock's guarantees, large bodies, unusual key encodings. Multi-node
//! behavior (redirects, partitions) lives in tests/cluster_http.rs.
//! These tests run in real time (real sockets don't mix with paused time);
//! election timeouts are shortened so each server is up in ~30ms.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustkv::api::{ApiContext, router};
use rustkv::kv::KvNode;
use rustkv::raft::Storage;
use rustkv::raft::node::{RaftConfig, RaftNode, RoleKind, StateMachine};
use rustkv::raft::transport::sim::{FaultConfig, SimNetwork};
use rustkv::store::KvStore;
use serde_json::{Value, json};
use tempfile::TempDir;

struct TestServer {
    base: String,
    kv: Arc<KvNode>,
    _dir: Option<TempDir>,
}

/// Boots a single-node raft-backed server over `data_dir` and waits until it
/// has elected itself, so the first write can't hit "no leader yet".
async fn spawn_server_in(data_dir: &Path) -> TestServer {
    let store = Arc::new(KvStore::new());
    let storage = Storage::open(data_dir).expect("storage");
    let net = SimNetwork::new(0, FaultConfig::default());
    let (transport, inbound) = net.register(1);
    let mut config = RaftConfig::new(1, Vec::new());
    config.election_timeout_min = Duration::from_millis(10);
    config.election_timeout_max = Duration::from_millis(20);
    let raft = RaftNode::spawn(
        config,
        storage,
        transport,
        inbound,
        store.clone() as Arc<dyn StateMachine>,
    );

    let mut status = raft.watch();
    tokio::time::timeout(Duration::from_secs(5), async {
        while status.borrow_and_update().role != RoleKind::Leader {
            status.changed().await.expect("raft node alive");
        }
    })
    .await
    .expect("single node elects itself");

    let kv = KvNode::new(store, raft, Duration::from_secs(5));
    let ctx = Arc::new(ApiContext {
        kv: kv.clone(),
        peer_urls: Arc::new(std::sync::RwLock::new(HashMap::new())),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, router(ctx))
            .await
            .expect("test server error");
    });
    TestServer {
        base: format!("http://{addr}"),
        kv,
        _dir: None,
    }
}

async fn spawn_server() -> TestServer {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut server = spawn_server_in(dir.path()).await;
    server._dir = Some(dir);
    server
}

#[tokio::test]
async fn get_missing_key_returns_404() {
    let server = spawn_server().await;
    let resp = reqwest::get(format!("{}/nope", server.base)).await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn put_then_get_roundtrips() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();
    let value = json!({"name": "alex", "n": 42, "nested": {"ok": true}});

    let put = client
        .put(format!("{}/user1", server.base))
        .json(&value)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    let get = client
        .get(format!("{}/user1", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.json::<Value>().await.unwrap(), value);
}

#[tokio::test]
async fn put_overwrites_existing_value() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    for (i, value) in [json!({"v": 1}), json!({"v": 2})].iter().enumerate() {
        let put = client
            .put(format!("{}/k", server.base))
            .json(value)
            .send()
            .await
            .unwrap();
        assert_eq!(put.status(), 201, "put #{i}");
    }

    let got: Value = client
        .get(format!("{}/k", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got, json!({"v": 2}));
}

#[tokio::test]
async fn delete_removes_key() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    client
        .put(format!("{}/k", server.base))
        .json(&json!({"v": 1}))
        .send()
        .await
        .unwrap();

    let del = client
        .delete(format!("{}/k", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 204);

    let get = client
        .get(format!("{}/k", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404);
}

#[tokio::test]
async fn delete_missing_key_is_idempotent_204() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();
    let del = client
        .delete(format!("{}/never", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 204);
}

#[tokio::test]
async fn put_invalid_json_returns_400_and_stores_nothing() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    let put = client
        .put(format!("{}/k", server.base))
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 400);

    let get = client
        .get(format!("{}/k", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404);
}

#[tokio::test]
async fn put_without_content_type_header_is_accepted() {
    // The contract only requires a valid JSON body, so a bare `curl -d` (which
    // sends application/x-www-form-urlencoded) must still work.
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    let put = client
        .put(format!("{}/k", server.base))
        .body(r#"{"a": 1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    let got: Value = client
        .get(format!("{}/k", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got, json!({"a": 1}));
}

// ---- dedup tokens (phase 13): X-Client-Id / X-Client-Seq ----

/// A retried PUT with the same token returns 201 both times but applies
/// once. The interleaved conflicting write is what makes the skip
/// observable: re-applying k=1 over k=1 would be invisible in an LWW map.
#[tokio::test]
async fn retried_put_with_same_token_applies_once() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    let tokened_put = || {
        client
            .put(format!("{}/k", server.base))
            .header("X-Client-Id", "1")
            .header("X-Client-Seq", "1")
            .json(&json!(1))
    };
    assert_eq!(tokened_put().send().await.unwrap().status(), 201);

    // Another client's confirmed, conflicting write (token-less).
    let conflicting = client
        .put(format!("{}/k", server.base))
        .json(&json!(2))
        .send()
        .await
        .unwrap();
    assert_eq!(conflicting.status(), 201);

    // The retry: still 201 (its entry commits) but the mutation is skipped.
    assert_eq!(tokened_put().send().await.unwrap().status(), 201);
    let got: Value = client
        .get(format!("{}/k", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got, json!(2), "the duplicate must not clobber the conflict");
}

/// Same shape for DELETE: a retried tokened delete must not destroy a key
/// re-created in between.
#[tokio::test]
async fn retried_delete_with_same_token_applies_once() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    client
        .put(format!("{}/k", server.base))
        .json(&json!(1))
        .send()
        .await
        .unwrap();
    let tokened_delete = || {
        client
            .delete(format!("{}/k", server.base))
            .header("X-Client-Id", "1")
            .header("X-Client-Seq", "1")
    };
    assert_eq!(tokened_delete().send().await.unwrap().status(), 204);

    // The key is re-created, then the delete is retried.
    client
        .put(format!("{}/k", server.base))
        .json(&json!(2))
        .send()
        .await
        .unwrap();
    assert_eq!(tokened_delete().send().await.unwrap().status(), 204);

    let get = client
        .get(format!("{}/k", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "the re-created key must survive");
    assert_eq!(get.json::<Value>().await.unwrap(), json!(2));
}

/// A higher seq from the same client is a NEW op and applies normally.
#[tokio::test]
async fn next_seq_from_same_client_applies() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    for (seq, value) in [("1", 1), ("2", 2)] {
        let put = client
            .put(format!("{}/k", server.base))
            .header("X-Client-Id", "7")
            .header("X-Client-Seq", seq)
            .json(&json!(value))
            .send()
            .await
            .unwrap();
        assert_eq!(put.status(), 201, "seq {seq}");
    }
    let got: Value = client
        .get(format!("{}/k", server.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got, json!(2));
}

/// Both-or-neither, both u64 — anything else is 400 and stores nothing.
#[tokio::test]
async fn malformed_token_headers_are_rejected_with_400() {
    let server = spawn_server().await;
    let client = reqwest::Client::new();

    let cases: [&[(&str, &str)]; 5] = [
        &[("X-Client-Id", "1")],                          // seq missing
        &[("X-Client-Seq", "1")],                         // id missing
        &[("X-Client-Id", "abc"), ("X-Client-Seq", "1")], // non-numeric id
        &[("X-Client-Id", "1"), ("X-Client-Seq", "-2")],  // negative seq
        &[("X-Client-Id", "1"), ("X-Client-Seq", "1.5")], // non-integer seq
    ];
    for headers in cases {
        let mut put = client.put(format!("{}/k", server.base)).json(&json!(1));
        let mut delete = client.delete(format!("{}/k", server.base));
        for (name, value) in headers {
            put = put.header(*name, *value);
            delete = delete.header(*name, *value);
        }
        assert_eq!(put.send().await.unwrap().status(), 400, "{headers:?}");
        assert_eq!(delete.send().await.unwrap().status(), 400, "{headers:?}");
    }

    let get = client
        .get(format!("{}/k", server.base))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "rejected writes must store nothing");
}

#[tokio::test]
async fn kv_state_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let client = reqwest::Client::new();

    let first = spawn_server_in(dir.path()).await;
    let put = client
        .put(format!("{}/greeting", first.base))
        .json(&json!({"hello": "world"}))
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);
    first.kv.raft().shutdown();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A fresh process over the same data dir: the log is replayed, the new
    // term's no-op commits it, and the state machine is rebuilt.
    let second = spawn_server_in(dir.path()).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let get = client
            .get(format!("{}/greeting", second.base))
            .send()
            .await
            .unwrap();
        if get.status() == 200 {
            assert_eq!(
                get.json::<Value>().await.unwrap(),
                json!({"hello": "world"})
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "state was not rebuilt from the log after restart"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
