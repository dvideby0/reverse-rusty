use super::*;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocabulary_learning_uses_the_strict_caller_corpus_contract_in_cluster_mode() {
    let state = test_state(&seed());
    let request = serde_json::json!({
        "queries": [
            [10, "(package,pkg) 2024"],
            [20, "(package,pkg) 2023"]
        ],
        "min_count": 2
    });
    let (status, headers, bytes) = send_raw(&state, req("POST", "/_vocab/learn", &request)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("learned vocab");
    assert_eq!(body["synonyms"].as_array().expect("synonyms").len(), 1);
    assert_eq!(body["synonyms"][0]["token"], "pkg");
    assert!(
        body["synonyms"]
            .as_array()
            .expect("synonyms")
            .iter()
            .all(|entry| entry["token"] != "uniquor"),
        "the dry run must not substitute the cluster's stored corpus"
    );

    let (status, headers, bytes) =
        send_raw(&state, req("POST", "/_vocab/learn", &serde_json::json!({}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "validation_error", "{body}");
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_learn", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_learn", "400"])
            .get(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn learn_and_apply_is_mode_consistent_bounded_and_off_runtime() {
    let queries = vec![
        (1, "vertex pkg".to_string()),
        (10, "(package,pkg) 2024".to_string()),
        (20, "(package,pkg) 2023".to_string()),
    ];
    let state = test_state(&queries);

    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send_raw(
            &request_state,
            req_empty("POST", "/_vocab/learn_and_apply?min_count=2"),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "coordinator learn-and-apply must wait asynchronously for admission"
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
    assert_eq!(body["recompiled"], 3, "{body}");
    assert!(body["rebuilt"].is_null(), "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_number(), "{body}");

    {
        let cluster = state.cluster.read();
        assert!(cluster
            .vocab()
            .is_some_and(|vocab| vocab.synonyms().iter().any(|entry| entry.token == "pkg")));
        assert!(cluster
            .percolate("vertex package")
            .expect("percolate")
            .contains(&1));
    }

    let (status, headers, bytes) = send_raw(
        &state,
        req_empty("POST", "/_vocab/learn_and_apply?unknown=true"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "validation_error", "{body}");

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_learn_apply", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_learn_apply", "400"])
            .get(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocabulary_read_is_complete_uncacheable_and_bodyless_for_head() {
    let state = test_state(&seed());
    let vocab = serde_json::json!({
        "synonyms": [
            {"token": "pkg", "canonical": "term:package", "kind": "generic"}
        ],
        "equivalences": [["ns", "north star"]],
        "number_context": ["model"]
    });
    let (status, put) = send(&state, req("PUT", "/_vocab", &vocab)).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    assert_eq!(put["acknowledged"], true, "{put}");
    assert_eq!(put["recompiled"], 3, "{put}");
    assert!(put["rebuilt"].is_null(), "{put}");
    assert!(put["took"].is_u64(), "{put}");
    assert!(put["took_ms"].is_number(), "{put}");

    let (status, headers, bytes) = send_raw(&state, req_empty("GET", "/_vocab")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers
            .get(header::CONTENT_LENGTH)
            .expect("GET content length")
            .to_str()
            .expect("ASCII length"),
        bytes.len().to_string()
    );
    let get_content_length = headers
        .get(header::CONTENT_LENGTH)
        .expect("GET content length")
        .clone();
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON vocab");
    assert_eq!(
        body["equivalences"],
        serde_json::json!([["ns", "north star"]])
    );
    assert_eq!(body["number_context"], serde_json::json!(["model"]));

    let (status, headers, bytes) = send_raw(&state, req_empty("HEAD", "/_vocab")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers.get(header::CONTENT_LENGTH),
        Some(&get_content_length),
        "HEAD must preserve the corresponding GET representation length"
    );
    assert!(bytes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocabulary_read_waits_asynchronously_for_shared_admission() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("permit");
    let request_state = Arc::clone(&state);
    let mut request =
        tokio::spawn(async move { send_raw(&request_state, req_empty("HEAD", "/_vocab")).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "coordinator vocabulary read must wait without blocking an async worker"
    );
    drop(held);

    let (status, headers, bytes) = request.await.expect("request task");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert!(bytes.is_empty());

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("POST")
            .uri("/_vocab")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD, PUT");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocabulary_write_is_strict_bounded_and_shares_async_admission() {
    let state = test_state(&seed());
    let vocab = serde_json::json!({
        "synonyms": [
            {"token": "pkg", "canonical": "term:package", "kind": "generic"}
        ]
    });

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("PUT")
            .uri("/_vocab?refresh=true")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(vocab.to_string()))
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

    let (status, headers, bytes) = send_raw(
        &state,
        Request::builder()
            .method("PUT")
            .uri("/_vocab")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{"))
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

    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send_raw(
            &request_state,
            Request::builder()
                .method("PUT")
                .uri("/_vocab")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(vocab.to_string()))
                .expect("request"),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "coordinator vocabulary write must wait asynchronously for administrative admission"
    );
    drop(held);

    let (status, headers, bytes) = request.await.expect("request task");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(body["recompiled"], 3, "{body}");
    assert!(body["rebuilt"].is_null(), "{body}");

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_put", "400"])
            .get(),
        2
    );
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_put", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_put"])
            .get_sample_count(),
        3
    );
}
