use super::health::{health, HEALTH_BODY_LIMIT, HEALTH_BODY_READ_TIMEOUT};

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::{
    body::{Body, Bytes},
    extract::DefaultBodyLimit,
    http::{header, Method, Request, StatusCode},
    routing::any,
    Router,
};
use parking_lot::Mutex;
use reverse_rusty::{config::EngineConfig, segment::Engine, vocab::Vocab, Normalizer};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn test_state() -> Arc<AppState> {
    let mut engine = Engine::with_config(
        Normalizer::default_vocab().expect("vocab"),
        EngineConfig::default(),
    );
    engine
        .try_insert_live("2024 acme keyboard", 7, 1)
        .expect("fixture insert");
    let snapshot = Arc::new(engine.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .expect("pool");
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
        pool,
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(1)),
        exhaustive_jobs: crate::jobs::ExhaustiveJobs::for_tests(prom.clone()),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad: false,
        prom,
        slow_query_threshold_ms: 0,
        auth: None,
        feedback: Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
        pit_tokens: crate::pit::PitTokens::generate(),
        pits: Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config: reverse_rusty::PitConfig::default(),
    })
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_health",
            any(health).layer(DefaultBodyLimit::max(body_limit)),
        )
        .with_state(Arc::clone(state))
}

async fn send(
    state: &Arc<AppState>,
    body_limit: usize,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
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

#[tokio::test]
async fn green_readiness_is_truthful_uncacheable_and_observed() {
    let state = test_state();
    let (status, headers, bytes) = send(
        &state,
        HEALTH_BODY_LIMIT,
        Method::GET,
        "/_health?wait_for_status=green&timeout=1s&level=cluster",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON health");
    assert_eq!(body["status"], "green");
    assert_eq!(body["mode"], "standalone");
    assert_eq!(body["timed_out"], false);
    assert_eq!(body["total_queries"], 1);
    assert_eq!(body["wal_healthy"], true);
    assert_eq!(body["persistence_healthy"], true);
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["health", "200"])
            .get(),
        1
    );
}

#[tokio::test]
async fn head_is_a_bodyless_readiness_probe() {
    let state = test_state();
    let (status, headers, bytes) = send(
        &state,
        HEALTH_BODY_LIMIT,
        Method::HEAD,
        "/_health",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn admission_precedes_buffering_an_untrusted_request_body() {
    let state = test_state();
    let held = Arc::clone(&state.health_permits)
        .acquire_many_owned(crate::state::MAX_CONCURRENT_HEALTH_REQUESTS as u32)
        .await
        .expect("all health permits");
    let pending_body = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());

    let (status, headers, bytes) = tokio::time::timeout(
        Duration::from_millis(100),
        send(
            &state,
            HEALTH_BODY_LIMIT,
            Method::GET,
            "/_health",
            pending_body,
        ),
    )
    .await
    .expect("admission rejects before polling the pending body");
    drop(held);

    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers.get(header::RETRY_AFTER).expect("retry"), "1");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "rejected_execution_exception");
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["health"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn slow_body_times_out_and_releases_health_admission() {
    let state = test_state();
    let pending_body = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());

    let (status, headers, bytes) = tokio::time::timeout(
        Duration::from_secs(1),
        send(
            &state,
            HEALTH_BODY_LIMIT,
            Method::GET,
            "/_health",
            pending_body,
        ),
    )
    .await
    .expect("the body read has its own deadline");

    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "request_timeout");
    let durations = state
        .prom
        .http_request_duration
        .with_label_values(&["health"]);
    assert_eq!(durations.get_sample_count(), 1);
    assert!(
        durations.get_sample_sum() >= HEALTH_BODY_READ_TIMEOUT.as_secs_f64() * 0.8,
        "body buffering must be included in the health duration metric"
    );
    assert_eq!(
        state.health_permits.available_permits(),
        crate::state::MAX_CONCURRENT_HEALTH_REQUESTS
    );

    let (status, _, _) = send(
        &state,
        HEALTH_BODY_LIMIT,
        Method::GET,
        "/_health",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "current_thread")]
async fn status_reached_after_the_deadline_is_still_timed_out() {
    let state = test_state();
    {
        let mut engine = state.engine.lock();
        assert!(
            engine.set_vocab(Vocab::new()).expect("vocab update") > 0,
            "the fixture must begin yellow"
        );
        state.snapshot.store(Arc::new(engine.snapshot()));
    }

    let request_state = Arc::clone(&state);
    let request = tokio::spawn(async move {
        send(
            &request_state,
            HEALTH_BODY_LIMIT,
            Method::GET,
            "/_health?wait_for_status=green&timeout=10ms",
            Body::empty(),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(1)).await;

    // Delay the single runtime worker beyond the route deadline, then make the
    // next snapshot green before the waiting handler can poll it. The deadline
    // must win even though the requested status is visible on that late poll.
    std::thread::sleep(Duration::from_millis(20));
    {
        let mut engine = state.engine.lock();
        assert_eq!(engine.recompile_stale_segments(), 1);
        state.snapshot.store(Arc::new(engine.snapshot()));
    }

    let (status, _, bytes) = request.await.expect("health task");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON health");
    assert_eq!(body["status"], "green");
    assert_eq!(body["timed_out"], true);
}

#[tokio::test]
async fn transport_and_controls_fail_loud() {
    let state = test_state();
    for uri in [
        "/_health?unknown=true",
        "/_health?wait_for_status=blue",
        "/_health?timeout=1",
        "/_health?level=shards",
    ] {
        let (status, headers, bytes) =
            send(&state, HEALTH_BODY_LIMIT, Method::GET, uri, Body::empty()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
        assert_eq!(body["error"]["type"], "validation_error", "{uri}: {body}");
    }

    let (status, _, bytes) = send(
        &state,
        HEALTH_BODY_LIMIT,
        Method::GET,
        "/_health",
        Body::from("not empty"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, headers, bytes) = send(
        &state,
        HEALTH_BODY_LIMIT,
        Method::POST,
        "/_health",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed");

    let oversized = vec![b'x'; HEALTH_BODY_LIMIT + 1];
    let (status, _, bytes) = send(
        &state,
        HEALTH_BODY_LIMIT,
        Method::GET,
        "/_health",
        Body::from(oversized),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "payload_too_large");
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["health"])
            .get_sample_count(),
        7,
        "method, validation, and extractor rejections all record duration"
    );
}
