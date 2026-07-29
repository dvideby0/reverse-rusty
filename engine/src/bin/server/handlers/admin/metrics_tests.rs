use super::metrics::{prometheus_metrics, METRICS_BODY_LIMIT, METRICS_BODY_READ_TIMEOUT};

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
use prometheus::{Encoder, TextEncoder};
use reverse_rusty::{segment::Engine, Normalizer};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn test_state() -> Arc<AppState> {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
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
        rank_profiles: Arc::new(reverse_rusty::RankProfiles::default()),
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
            "/_metrics",
            any(prometheus_metrics).layer(DefaultBodyLimit::max(body_limit)),
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
async fn scrape_is_truthful_prometheus_text_uncacheable_and_observed() {
    let state = test_state();
    let (status, headers, bytes) = send(
        &state,
        METRICS_BODY_LIMIT,
        Method::GET,
        "/_metrics",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body = String::from_utf8(bytes.to_vec()).expect("UTF-8 metrics");
    assert!(body.contains("# TYPE reverse_rusty_total_queries gauge"));
    assert!(body.contains("reverse_rusty_total_queries 1"));

    let (_, _, second) = send(
        &state,
        METRICS_BODY_LIMIT,
        Method::GET,
        "/_metrics",
        Body::empty(),
    )
    .await;
    let second = String::from_utf8(second.to_vec()).expect("UTF-8 metrics");
    assert!(
        second.contains("reverse_rusty_http_requests_total{endpoint=\"metrics\",status=\"200\"} 1"),
        "{second}"
    );
    assert!(
        second
            .contains("reverse_rusty_http_request_duration_seconds_count{endpoint=\"metrics\"} 1"),
        "{second}"
    );
}

#[tokio::test]
async fn head_runs_the_same_collection_but_has_no_body() {
    let state = test_state();
    let (status, headers, bytes) = send(
        &state,
        METRICS_BODY_LIMIT,
        Method::HEAD,
        "/_metrics",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn transport_is_strict_and_bounded() {
    let state = test_state();

    let (status, _, bytes) = send(
        &state,
        METRICS_BODY_LIMIT,
        Method::GET,
        "/_metrics?format=json",
        Body::empty(),
    )
    .await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let (status, _, bytes) = send(
        &state,
        METRICS_BODY_LIMIT,
        Method::GET,
        "/_metrics",
        Body::from("not empty"),
    )
    .await;
    assert_error(status, &bytes, StatusCode::BAD_REQUEST, "validation_error");

    let (status, headers, bytes) = send(
        &state,
        METRICS_BODY_LIMIT,
        Method::POST,
        "/_metrics",
        Body::empty(),
    )
    .await;
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD");
    assert_error(
        status,
        &bytes,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );

    let oversized = vec![b'x'; METRICS_BODY_LIMIT + 1];
    let (status, _, bytes) = send(
        &state,
        METRICS_BODY_LIMIT,
        Method::GET,
        "/_metrics",
        Body::from(oversized),
    )
    .await;
    assert_error(
        status,
        &bytes,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["metrics", "400"])
            .get(),
        2
    );
}

#[tokio::test]
async fn a_slow_body_has_its_own_deadline() {
    let state = test_state();
    let pending_body = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, headers, bytes) = tokio::time::timeout(
        Duration::from_secs(1),
        send(
            &state,
            METRICS_BODY_LIMIT,
            Method::GET,
            "/_metrics",
            pending_body,
        ),
    )
    .await
    .expect("metrics body deadline");

    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_error(
        status,
        &bytes,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
    let durations = state
        .prom
        .http_request_duration
        .with_label_values(&["metrics"]);
    assert_eq!(durations.get_sample_count(), 1);
    assert!(
        durations.get_sample_sum() >= METRICS_BODY_READ_TIMEOUT.as_secs_f64() * 0.8,
        "body buffering must be included in route duration"
    );
}

#[test]
fn cluster_gauges_remove_disappeared_shards_and_clamp_unsigned_values() {
    let prom = PrometheusMetrics::new();
    let transport = reverse_rusty::cluster::TransportMetricsSnapshot::default();
    prom.refresh_cluster_gauges(6, &[1, 2, 3], &transport);
    let first = render(&prom);
    assert!(first.contains("reverse_rusty_cluster_shard_queries{shard=\"2\"} 3"));

    prom.refresh_cluster_gauges(usize::MAX, &[9], &transport);
    assert_eq!(prom.total_queries.get(), i64::MAX);
    let second = render(&prom);
    assert!(second.contains("reverse_rusty_cluster_shard_queries{shard=\"0\"} 9"));
    assert!(!second.contains("reverse_rusty_cluster_shard_queries{shard=\"1\"}"));
    assert!(!second.contains("reverse_rusty_cluster_shard_queries{shard=\"2\"}"));
}

fn assert_error(status: StatusCode, bytes: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected);
    let body: serde_json::Value = serde_json::from_slice(bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
}

fn render(prom: &PrometheusMetrics) -> String {
    let encoder = TextEncoder::new();
    let mut bytes = Vec::new();
    encoder
        .encode(&prom.registry.gather(), &mut bytes)
        .expect("encode");
    String::from_utf8(bytes).expect("UTF-8")
}
