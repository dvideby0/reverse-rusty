use super::read::{get_vocab, vocab_method_not_allowed, VOCAB_READ_BODY_LIMIT};

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
use reverse_rusty::{config::EngineConfig, segment::Engine, vocab::Vocab};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn fixture_vocab() -> Vocab {
    serde_json::from_value(serde_json::json!({
        "synonyms": [
            {"token": "pkg", "canonical": "term:package", "kind": "generic"}
        ],
        "phrases": [
            {
                "tokens": ["north", "star"],
                "canonical": "brand:north_star",
                "kind": "brand"
            }
        ],
        "equivalences": [["ns", "north star"]],
        "punctuation": [{"ch": "-", "class": "fold"}],
        "number_context": ["model"]
    }))
    .expect("fixture vocab")
}

fn test_state() -> Arc<AppState> {
    let engine =
        Engine::with_vocab(fixture_vocab(), EngineConfig::default()).expect("fixture engine");
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

fn router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/_vocab",
            get(get_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_READ_BODY_LIMIT))
                .put(|body: Bytes| async move {
                    assert!(
                        body.len() > VOCAB_READ_BODY_LIMIT,
                        "PUT fixture must prove the GET-only limit is isolated"
                    );
                    StatusCode::NO_CONTENT
                })
                .fallback(vocab_method_not_allowed::<AppState>),
        )
        .with_state(Arc::clone(state))
}

async fn send(
    state: &Arc<AppState>,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
) -> (StatusCode, axum::http::HeaderMap, Bytes) {
    let response = router(state)
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
async fn get_is_round_trippable_uncacheable_and_observed() {
    let state = test_state();
    let (status, headers, bytes) = send(&state, Method::GET, "/_vocab", Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );

    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON vocab");
    assert_eq!(
        body,
        serde_json::to_value(fixture_vocab()).expect("fixture JSON")
    );
    let round_trip: Vocab = serde_json::from_slice(&bytes).expect("round-trip vocab");
    round_trip
        .to_normalizer()
        .expect("returned vocabulary must remain installable");

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_get", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_get"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn transport_is_strict_bounded_and_structured() {
    let state = test_state();

    let (status, get_error_headers, body) =
        send(&state, Method::GET, "/_vocab?from=0", Body::empty()).await;
    assert_error(status, &body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(
        get_error_headers
            .get(header::CONTENT_LENGTH)
            .expect("GET error content length")
            .to_str()
            .expect("ASCII length"),
        body.len().to_string()
    );
    let (status, head_error_headers, head_body) =
        send(&state, Method::HEAD, "/_vocab?from=0", Body::empty()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(head_body.is_empty());
    assert_eq!(
        head_error_headers.get(header::CONTENT_LENGTH),
        get_error_headers.get(header::CONTENT_LENGTH),
        "HEAD error must preserve the corresponding GET representation length"
    );

    let (status, _, body) = send(&state, Method::GET, "/_vocab", "{}").await;
    assert_error(status, &body, StatusCode::BAD_REQUEST, "validation_error");

    let oversized = vec![b'x'; VOCAB_READ_BODY_LIMIT + 1];
    let (status, _, body) = send(&state, Method::GET, "/_vocab", oversized.clone()).await;
    assert_error(
        status,
        &body,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let (status, _, body) = send(&state, Method::PUT, "/_vocab", oversized).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());

    let (status, headers, body) = send(&state, Method::POST, "/_vocab", Body::empty()).await;
    assert_error(
        status,
        &body,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD, PUT");

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, _, body) = tokio::time::timeout(
        Duration::from_secs(1),
        send(&state, Method::GET, "/_vocab", pending),
    )
    .await
    .expect("body deadline");
    assert_error(
        status,
        &body,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
}

#[tokio::test]
async fn head_is_bodyless_and_waits_asynchronously_for_shared_admission() {
    let state = test_state();
    let (get_status, get_headers, get_body) =
        send(&state, Method::GET, "/_vocab", Body::empty()).await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(
        get_headers
            .get(header::CONTENT_LENGTH)
            .expect("GET content length")
            .to_str()
            .expect("ASCII length"),
        get_body.len().to_string()
    );
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("permit");
    let request_state = Arc::clone(&state);
    let mut request =
        tokio::spawn(
            async move { send(&request_state, Method::HEAD, "/_vocab", Body::empty()).await },
        );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "vocabulary read should wait without blocking the runtime"
    );
    drop(held);

    let (status, headers, body) = request.await.expect("request task");
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
        get_headers.get(header::CONTENT_LENGTH),
        "HEAD must preserve the corresponding GET representation length"
    );
    assert!(body.is_empty());

    state.stats_permits.close();
    let (status, _, body) = send(&state, Method::GET, "/_vocab", Body::empty()).await;
    assert_error(
        status,
        &body,
        StatusCode::SERVICE_UNAVAILABLE,
        "vocab_unavailable",
    );
}

fn assert_error(status: StatusCode, body: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected);
    let body: serde_json::Value = serde_json::from_slice(body).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
    assert_eq!(body["status"], expected.as_u16(), "{body}");
}
