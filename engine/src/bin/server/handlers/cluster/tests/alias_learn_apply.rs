use super::*;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};

fn alias_learning_seed() -> Vec<(u64, String)> {
    vec![
        (1, "vertex adapter".to_string()),
        (10, "(adapter,adapters) 2024".to_string()),
        (20, "(adapter,adapters) 2023".to_string()),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn has_standalone_parity_strict_transport_and_off_runtime_locking() {
    let state = test_state(&alias_learning_seed());

    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send_raw(
            &request_state,
            req_empty("POST", "/_vocab/aliases/learn_and_apply?min_count=2"),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "coordinator alias learning must wait asynchronously for admission"
    );
    drop(held);

    let (status, headers, bytes) = request.await.expect("request task");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["activated"], 1, "{body}");
    assert_eq!(body["recompiled"], 3, "{body}");
    assert!(body["rebuilt"].is_null(), "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_number(), "{body}");
    assert_eq!(body["summary"]["active"], 1, "{body}");

    {
        let cluster = state.cluster.read();
        assert!(cluster
            .percolate("vertex adapters")
            .expect("percolate")
            .contains(&1));
        assert_eq!(
            cluster
                .vocab()
                .expect("installed vocab")
                .alias_summary()
                .active,
            1
        );
    }

    let lock_state = Arc::clone(&state);
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_thread = std::thread::spawn(move || {
        let _cluster = lock_state.cluster.write();
        held_tx.send(()).expect("held signal");
        release_rx.recv().expect("release signal");
    });
    held_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("cluster lock held");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send_raw(
            &request_state,
            req_empty("POST", "/_vocab/aliases/learn_and_apply?min_count=2"),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "the blocking worker should wait on the cluster write lock"
    );
    tokio::time::timeout(
        Duration::from_millis(100),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .expect("Tokio worker remained responsive");
    release_tx.send(()).expect("release cluster");
    lock_thread.join().expect("lock thread");
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);

    for path in [
        "/_vocab/aliases/learn_and_apply?min_count=0",
        "/_vocab/aliases/learn_and_apply?unknown=true",
        "/_vocab/aliases/learn_and_apply?min_count=2&min_count=3",
    ] {
        let (status, headers, bytes) = send_raw(&state, req_empty("POST", path)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
        assert_eq!(body["error"]["type"], "validation_error", "{body}");
    }

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("POST")
            .uri("/_vocab/aliases/learn_and_apply")
            .body(Body::from("{}"))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "validation_error", "{body}");

    let (status, headers, bytes) =
        send_raw(&state, req_empty("GET", "/_vocab/aliases/learn_and_apply")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed", "{body}");

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_learn_apply", "200"])
            .get(),
        2
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_learn_apply"])
            .get_sample_count(),
        7
    );
}
