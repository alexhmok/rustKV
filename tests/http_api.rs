//! End-to-end tests of the client HTTP contract against a real server bound to
//! an ephemeral port.
//!
//! Covered: the exact status codes of the contract, hit/miss, overwrite,
//! delete idempotency, and invalid-JSON rejection.
//! NOT covered (stated per project policy): concurrent access beyond RwLock's
//! guarantees, large bodies, unusual key encodings.

use std::sync::Arc;

use rustkv::api;
use rustkv::store::KvStore;
use serde_json::{Value, json};

/// Spawns the real router on 127.0.0.1:0 and returns its base URL.
async fn spawn_server() -> String {
    let store = Arc::new(KvStore::new());
    let app = api::router(store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("test server error");
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn get_missing_key_returns_404() {
    let base = spawn_server().await;
    let resp = reqwest::get(format!("{base}/nope")).await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn put_then_get_roundtrips() {
    let base = spawn_server().await;
    let client = reqwest::Client::new();
    let value = json!({"name": "alex", "n": 42, "nested": {"ok": true}});

    let put = client
        .put(format!("{base}/user1"))
        .json(&value)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    let get = client.get(format!("{base}/user1")).send().await.unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.json::<Value>().await.unwrap(), value);
}

#[tokio::test]
async fn put_overwrites_existing_value() {
    let base = spawn_server().await;
    let client = reqwest::Client::new();

    for (i, value) in [json!({"v": 1}), json!({"v": 2})].iter().enumerate() {
        let put = client
            .put(format!("{base}/k"))
            .json(value)
            .send()
            .await
            .unwrap();
        assert_eq!(put.status(), 201, "put #{i}");
    }

    let got: Value = client
        .get(format!("{base}/k"))
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
    let base = spawn_server().await;
    let client = reqwest::Client::new();

    client
        .put(format!("{base}/k"))
        .json(&json!({"v": 1}))
        .send()
        .await
        .unwrap();

    let del = client.delete(format!("{base}/k")).send().await.unwrap();
    assert_eq!(del.status(), 204);

    let get = client.get(format!("{base}/k")).send().await.unwrap();
    assert_eq!(get.status(), 404);
}

#[tokio::test]
async fn delete_missing_key_is_idempotent_204() {
    let base = spawn_server().await;
    let client = reqwest::Client::new();
    let del = client.delete(format!("{base}/never")).send().await.unwrap();
    assert_eq!(del.status(), 204);
}

#[tokio::test]
async fn put_invalid_json_returns_400_and_stores_nothing() {
    let base = spawn_server().await;
    let client = reqwest::Client::new();

    let put = client
        .put(format!("{base}/k"))
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 400);

    let get = client.get(format!("{base}/k")).send().await.unwrap();
    assert_eq!(get.status(), 404);
}

#[tokio::test]
async fn put_without_content_type_header_is_accepted() {
    // The contract only requires a valid JSON body, so a bare `curl -d` (which
    // sends application/x-www-form-urlencoded) must still work.
    let base = spawn_server().await;
    let client = reqwest::Client::new();

    let put = client
        .put(format!("{base}/k"))
        .body(r#"{"a": 1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    let got: Value = client
        .get(format!("{base}/k"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got, json!({"a": 1}));
}
