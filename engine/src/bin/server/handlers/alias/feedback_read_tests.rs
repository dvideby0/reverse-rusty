use super::feedback_read::{
    alias_feedback_read_method_not_allowed, get_alias_feedback, ALIAS_FEEDBACK_READ_BODY_LIMIT,
    ALIAS_FEEDBACK_READ_MAX_PAGE_SIZE,
};

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::{
    body::{Body, Bytes},
    extract::DefaultBodyLimit,
    http::{header, Method, Request, StatusCode},
    routing::get,
    Router,
};
use parking_lot::Mutex;
use reverse_rusty::{
    config::EngineConfig,
    segment::Engine,
    vocab::{AliasFeedback, Vocab},
};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn fixture_vocab() -> Vocab {
    serde_json::from_value(serde_json::json!({
        "aliases": {
            "entries": [
                {
                    "forms": ["aleph", "alpha"],
                    "provenance": "learned_distributional",
                    "kind": "single_token_distinct",
                    "status": "candidate",
                    "confidence": 0.9
                },
                {
                    "forms": ["beta", "bravo"],
                    "provenance": "learned_distributional",
                    "kind": "single_token_distinct",
                    "status": "candidate",
                    "confidence": 0.8
                }
            ]
        }
    }))
    .expect("fixture vocabulary")
}

fn test_state() -> Arc<AppState> {
    let config = EngineConfig {
        alias_feedback_capture: true,
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_vocab(fixture_vocab(), config).expect("fixture engine");
    engine.build_from_queries(&[
        (1, "vertex gamma".to_string()),
        (2, "delta gamma".to_string()),
    ]);
    let mut feedback = AliasFeedback::default();
    feedback.sync_tracked(engine.aliases().expect("aliases"), 256);
    let alpha = vec!["alpha".to_string(), "item".to_string()];
    let aleph = vec!["aleph".to_string(), "item".to_string()];
    feedback.observe(&alpha, &[1, 2]);
    feedback.observe(&aleph, &[1, 2]);

    let snapshot = Arc::new(engine.snapshot());
    let prom = PrometheusMetrics::new();
    Arc::new(AppState {
        engine: Mutex::new(engine),
        flush_serial: Mutex::new(()),
        backup_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_BACKUPS,
        )),
        health_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_HEALTH_REQUESTS,
        )),
        stats_permits: Arc::new(tokio::sync::Semaphore::new(
            crate::state::MAX_CONCURRENT_STATS,
        )),
        snapshot: ArcSwap::new(snapshot),
        pool: rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("pool"),
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(1)),
        exhaustive_jobs: crate::jobs::ExhaustiveJobs::for_tests(prom.clone()),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad: false,
        prom,
        slow_query_threshold_ms: 0,
        auth: None,
        feedback: Mutex::new(feedback),
        pit_tokens: crate::pit::PitTokens::generate(),
        pits: Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config: reverse_rusty::PitConfig::default(),
    })
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_vocab/aliases/feedback",
            get(get_alias_feedback)
                .layer(DefaultBodyLimit::max(body_limit))
                .fallback(alias_feedback_read_method_not_allowed::<AppState>),
        )
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024))
        .with_state(Arc::clone(state))
}

async fn send(
    state: &Arc<AppState>,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
    body_limit: usize,
) -> (StatusCode, axum::http::HeaderMap, Bytes) {
    let response = router(state, body_limit)
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .body(body.into())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (status, headers, bytes)
}

fn assert_error(
    status: StatusCode,
    headers: &axum::http::HeaderMap,
    body: &Bytes,
    expected: StatusCode,
    kind: &str,
) {
    assert_eq!(status, expected);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache control"),
        "no-store"
    );
    let value: serde_json::Value = serde_json::from_slice(body).expect("JSON error");
    assert_eq!(value["status"], expected.as_u16(), "{value}");
    assert_eq!(value["error"]["type"], kind, "{value}");
}

#[tokio::test]
async fn paged_feedback_is_timed_truthful_uncacheable_and_observed() {
    let state = test_state();
    let uri = "/_vocab/aliases/feedback?min_titles=1&min_queries=1&from=0&size=1";
    let (status, headers, bytes) = send(
        &state,
        Method::GET,
        uri,
        Body::empty(),
        ALIAS_FEEDBACK_READ_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    assert_eq!(
        headers
            .get(header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .expect("ASCII length"),
        bytes.len().to_string()
    );

    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON feedback");
    assert!(body["took"].as_u64().is_some(), "{body}");
    assert!(body["took_ms"].as_f64().is_some(), "{body}");
    assert_eq!(body["capture_enabled"], true);
    assert_eq!(body["count"], 2);
    assert_eq!(body["tracked_pairs"], 2);
    assert_eq!(body["min_overlap"], 0.5);
    assert_eq!(body["min_titles"], 1);
    assert_eq!(body["min_queries"], 1);
    assert_eq!(body["pairs"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["pairs"][0]["forms"],
        serde_json::json!(["aleph", "alpha"])
    );
    assert_eq!(body["pairs"][0]["validated"], true);

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_feedback_get", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_feedback_get"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn paging_preserves_total_and_head_representation_headers() {
    let state = test_state();
    let uri = "/_vocab/aliases/feedback?from=1&size=1";
    let (status, get_headers, body) = send(
        &state,
        Method::GET,
        uri,
        Body::empty(),
        ALIAS_FEEDBACK_READ_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON feedback");
    assert_eq!(value["count"], 2);
    assert_eq!(value["pairs"].as_array().unwrap().len(), 1);
    assert_eq!(
        value["pairs"][0]["forms"],
        serde_json::json!(["beta", "bravo"])
    );

    let (status, head_headers, head_body) = send(
        &state,
        Method::HEAD,
        uri,
        Body::empty(),
        ALIAS_FEEDBACK_READ_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        head_headers.get(header::CONTENT_TYPE),
        get_headers.get(header::CONTENT_TYPE)
    );
    assert!(
        head_headers.get(header::CONTENT_LENGTH).is_some(),
        "HEAD retains the dynamically timed representation length"
    );
    assert_eq!(head_headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    assert!(head_body.is_empty());
}

#[tokio::test]
async fn thresholds_paging_body_and_methods_are_strict_and_bounded() {
    let state = test_state();
    let invalid_uris = vec![
        "/_vocab/aliases/feedback?unknown=true".to_string(),
        "/_vocab/aliases/feedback?size=1&size=2".to_string(),
        "/_vocab/aliases/feedback?from=nope".to_string(),
        "/_vocab/aliases/feedback?min_overlap=NaN".to_string(),
        "/_vocab/aliases/feedback?min_overlap=-0.1".to_string(),
        "/_vocab/aliases/feedback?min_overlap=1.1".to_string(),
        "/_vocab/aliases/feedback?min_titles=0".to_string(),
        "/_vocab/aliases/feedback?min_queries=0".to_string(),
        format!(
            "/_vocab/aliases/feedback?size={}",
            ALIAS_FEEDBACK_READ_MAX_PAGE_SIZE + 1
        ),
    ];
    for uri in invalid_uris {
        let (status, headers, body) = send(
            &state,
            Method::GET,
            &uri,
            Body::empty(),
            ALIAS_FEEDBACK_READ_BODY_LIMIT,
        )
        .await;
        assert_error(
            status,
            &headers,
            &body,
            StatusCode::BAD_REQUEST,
            "validation_error",
        );
    }

    let (status, headers, body) = send(
        &state,
        Method::GET,
        "/_vocab/aliases/feedback",
        "{}",
        ALIAS_FEEDBACK_READ_BODY_LIMIT,
    )
    .await;
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::BAD_REQUEST,
        "validation_error",
    );

    let oversized = vec![b'x'; ALIAS_FEEDBACK_READ_BODY_LIMIT + 1];
    let (status, headers, body) = send(
        &state,
        Method::GET,
        "/_vocab/aliases/feedback",
        oversized,
        ALIAS_FEEDBACK_READ_BODY_LIMIT,
    )
    .await;
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let (status, headers, body) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/feedback",
        Body::empty(),
        ALIAS_FEEDBACK_READ_BODY_LIMIT,
    )
    .await;
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );
    assert_eq!(headers.get(header::ALLOW).unwrap(), "GET, HEAD");

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, headers, body) = tokio::time::timeout(
        Duration::from_secs(1),
        send(
            &state,
            Method::GET,
            "/_vocab/aliases/feedback",
            pending,
            ALIAS_FEEDBACK_READ_BODY_LIMIT,
        ),
    )
    .await
    .expect("body deadline");
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn waits_asynchronously_for_admission_and_closed_admission_fails() {
    let state = test_state();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::GET,
            "/_vocab/aliases/feedback?size=0",
            Body::empty(),
            ALIAS_FEEDBACK_READ_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "feedback read must wait asynchronously for administrative admission"
    );
    tokio::time::timeout(
        Duration::from_millis(100),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .expect("Tokio worker remained responsive");
    drop(held);
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);

    state.stats_permits.close();
    let (status, headers, body) = send(
        &state,
        Method::GET,
        "/_vocab/aliases/feedback",
        Body::empty(),
        ALIAS_FEEDBACK_READ_BODY_LIMIT,
    )
    .await;
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::SERVICE_UNAVAILABLE,
        "aliases_unavailable",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn feedback_lock_wait_runs_off_runtime_and_keeps_admission() {
    let state = test_state();
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let lock_state = Arc::clone(&state);
    let lock_thread = std::thread::spawn(move || {
        let _guard = lock_state.feedback.lock();
        locked_tx.send(()).expect("announce lock");
        release_rx.recv().expect("release lock");
    });
    locked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("feedback lock held");

    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::GET,
            "/_vocab/aliases/feedback",
            Body::empty(),
            ALIAS_FEEDBACK_READ_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "request must remain pending behind the feedback lock"
    );
    tokio::time::timeout(
        Duration::from_millis(100),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .expect("Tokio worker remained responsive");
    assert_eq!(state.stats_permits.available_permits(), 0);

    release_tx.send(()).expect("release feedback");
    lock_thread.join().expect("lock thread");
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);
    assert_eq!(state.stats_permits.available_permits(), 1);
}
