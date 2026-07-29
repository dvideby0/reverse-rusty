use super::{get_settings, settings_method_not_allowed, SETTINGS_READ_BODY_LIMIT};

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::{
    body::{Body, Bytes},
    extract::DefaultBodyLimit,
    http::{header, Method, Request, StatusCode},
    routing::{get, put},
    Router,
};
use parking_lot::Mutex;
use reverse_rusty::{config::EngineConfig, segment::Engine, Normalizer};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn test_state() -> (Arc<AppState>, EngineConfig) {
    let config = EngineConfig {
        max_segments: 17,
        broad_batch_size: 384,
        alias_feedback_capture: true,
        ..EngineConfig::default()
    };
    let engine = Engine::with_config(
        Normalizer::default_vocab().expect("normalizer"),
        config.clone(),
    );
    let snapshot = Arc::new(engine.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .expect("pool");
    let prom = PrometheusMetrics::new();
    (
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
        }),
        config,
    )
}

fn router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/_settings",
            get(get_settings)
                .layer(DefaultBodyLimit::max(SETTINGS_READ_BODY_LIMIT))
                .merge(put(|body: Bytes| async move {
                    assert!(
                        body.len() > SETTINGS_READ_BODY_LIMIT,
                        "PUT fixture must prove the GET-only limit is isolated"
                    );
                    StatusCode::NO_CONTENT
                }))
                .fallback(settings_method_not_allowed::<AppState>),
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
async fn live_defaults_flat_head_cache_and_telemetry_are_truthful() {
    let (state, config) = test_state();
    let (status, get_headers, get_bytes) =
        send(&state, Method::GET, "/_settings", Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get_headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        get_headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&get_bytes).expect("settings JSON");
    assert_eq!(
        body["settings"],
        serde_json::to_value(&config).expect("config JSON")
    );
    assert!(body.get("defaults").is_none(), "{body}");

    let (status, flat_headers, flat_bytes) = send(
        &state,
        Method::GET,
        "/_settings?flat_settings=true",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        flat_bytes, get_bytes,
        "native setting keys are already flat"
    );
    assert_eq!(
        flat_headers.get(header::CONTENT_LENGTH),
        get_headers.get(header::CONTENT_LENGTH)
    );

    let uri = "/_settings?include_defaults=true&flat_settings=false";
    let (status, defaults_headers, defaults_bytes) =
        send(&state, Method::GET, uri, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    let defaults: serde_json::Value =
        serde_json::from_slice(&defaults_bytes).expect("defaults JSON");
    assert_eq!(defaults["settings"]["max_segments"], 17);
    assert_eq!(
        defaults["defaults"],
        serde_json::to_value(EngineConfig::default()).expect("default config JSON")
    );

    let (status, head_headers, head_bytes) = send(&state, Method::HEAD, uri, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(head_bytes.is_empty());
    assert_eq!(
        head_headers.get(header::CONTENT_LENGTH),
        defaults_headers.get(header::CONTENT_LENGTH),
        "HEAD must preserve the corresponding GET representation length"
    );

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["settings_get", "200"])
            .get(),
        4
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["settings_get"])
            .get_sample_count(),
        4
    );
}

#[tokio::test]
async fn query_body_size_deadline_and_method_are_strict() {
    let (state, _) = test_state();
    for query in [
        "unknown=true",
        "include_defaults=true&include_defaults=false",
        "include_defaults=maybe",
        "flat_settings=true&flat_settings=false",
        "flat_settings=maybe",
    ] {
        let (status, headers, body) = send(
            &state,
            Method::GET,
            &format!("/_settings?{query}"),
            Body::empty(),
        )
        .await;
        assert_error(status, &body, StatusCode::BAD_REQUEST, "validation_error");
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
    }

    let (status, _, body) = send(&state, Method::GET, "/_settings", "{}").await;
    assert_error(status, &body, StatusCode::BAD_REQUEST, "validation_error");

    let oversized = vec![b'x'; SETTINGS_READ_BODY_LIMIT + 1];
    let (status, _, body) = send(&state, Method::GET, "/_settings", oversized.clone()).await;
    assert_error(
        status,
        &body,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );
    let (status, _, body) = send(&state, Method::PUT, "/_settings", oversized).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());

    let (status, headers, body) = send(&state, Method::POST, "/_settings", Body::empty()).await;
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
        send(&state, Method::GET, "/_settings", pending),
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
async fn admission_wait_is_async_and_closed_admission_fails() {
    let (state, _) = test_state();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(&request_state, Method::GET, "/_settings", Body::empty()).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "settings read should wait without blocking the runtime"
    );
    drop(held);
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);

    state.stats_permits.close();
    let (status, headers, body) = send(&state, Method::GET, "/_settings", Body::empty()).await;
    assert_error(
        status,
        &body,
        StatusCode::SERVICE_UNAVAILABLE,
        "settings_unavailable",
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
}

fn assert_error(status: StatusCode, body: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected);
    let body: serde_json::Value = serde_json::from_slice(body).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
    assert_eq!(body["status"], expected.as_u16(), "{body}");
}
