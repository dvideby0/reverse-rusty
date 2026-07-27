use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_flush_shares_methods_controls_and_shard_response() {
    let state = test_state(&seed());
    let (status, body) = send(
        &state,
        req_empty("GET", "/_flush?force=true&wait_if_ongoing=true"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 3, "successful": 3, "failed": 0})
    );
    assert!(body.get("total_queries").is_none(), "{body}");
    assert!(body.get("base_segments").is_none(), "{body}");

    let (status, body) = send(
        &state,
        req_empty("POST", "/_flush?force=false&wait_if_ongoing=false"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_flush_uses_the_shared_strict_boundary() {
    let state = test_state(&seed());
    for path in [
        "/_flush?routing=one",
        "/_flush?force=maybe",
        "/_flush?wait_if_ongoing=true&wait_if_ongoing=false",
    ] {
        let (status, body) = send(&state, req_empty("POST", path)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "validation_error", "{body}");
    }

    let (status, body) = send(
        &state,
        Request::post("/_flush")
            .body(Body::from("{}"))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error", "{body}");

    let response = router(&state)
        .oneshot(req_empty("PUT", "/_flush"))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::ALLOW)
            .expect("allow"),
        "GET, POST"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_nonwaiting_flush_conflicts_with_an_explicit_flush() {
    let state = test_state(&seed());
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_state = Arc::clone(&state);
    let holder = std::thread::spawn(move || {
        let _held = lock_state.flush_serial.lock();
        locked_tx.send(()).expect("report held lock");
        release_rx.recv().expect("release held lock");
    });
    locked_rx.recv().expect("wait for held lock");
    let (status, body) = send(&state, req_empty("GET", "/_flush?wait_if_ongoing=false")).await;
    release_tx.send(()).expect("release held lock");
    holder.join().expect("lock holder");

    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["type"], "flush_in_progress_exception",
        "{body}"
    );
}
