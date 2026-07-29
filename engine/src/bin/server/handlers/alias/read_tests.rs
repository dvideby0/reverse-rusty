use super::read::{alias_read_method_not_allowed, get_aliases, ALIAS_READ_BODY_LIMIT};

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
        "aliases": {
            "entries": [
                {
                    "forms": ["adapter", "adapters"],
                    "provenance": "declared_file",
                    "kind": "single_token_variant",
                    "status": "active",
                    "confidence": 1.0
                },
                {
                    "forms": ["couch", "sofa"],
                    "provenance": "learned_from_queries",
                    "kind": "single_token_distinct",
                    "status": "candidate",
                    "confidence": 0.5
                },
                {
                    "forms": ["old", "used"],
                    "provenance": "manual",
                    "kind": "single_token_distinct",
                    "status": "rejected",
                    "confidence": 1.0
                }
            ]
        }
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
            "/_vocab/aliases",
            get(get_aliases)
                .layer(DefaultBodyLimit::max(ALIAS_READ_BODY_LIMIT))
                .fallback(alias_read_method_not_allowed::<AppState>),
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
async fn get_returns_the_complete_governed_registry_and_is_observed() {
    let state = test_state();
    let (status, headers, bytes) =
        send(&state, Method::GET, "/_vocab/aliases", Body::empty()).await;
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
            .expect("content length")
            .to_str()
            .expect("ASCII length"),
        bytes.len().to_string()
    );

    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON aliases");
    assert_eq!(body["count"], 3, "{body}");
    assert_eq!(
        body["aliases"]["entries"]
            .as_array()
            .expect("entries")
            .len(),
        3
    );
    assert_eq!(
        body["summary"],
        serde_json::json!({"active": 1, "candidate": 1, "rejected": 1})
    );
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_get", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_get"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn paging_preserves_total_count_summary_and_head_representation_length() {
    let state = test_state();
    let uri = "/_vocab/aliases?from=1&size=1";
    let (status, get_headers, bytes) = send(&state, Method::GET, uri, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON aliases");
    assert_eq!(body["count"], 3, "{body}");
    assert_eq!(
        body["aliases"]["entries"],
        serde_json::json!([{
            "forms": ["couch", "sofa"],
            "provenance": "learned_from_queries",
            "kind": "single_token_distinct",
            "status": "candidate",
            "confidence": 0.5
        }])
    );
    assert_eq!(
        body["summary"],
        serde_json::json!({"active": 1, "candidate": 1, "rejected": 1})
    );

    let (status, head_headers, head_body) = send(&state, Method::HEAD, uri, Body::empty()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        head_headers.get(header::CONTENT_TYPE),
        get_headers.get(header::CONTENT_TYPE)
    );
    assert_eq!(
        head_headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        head_headers.get(header::CONTENT_LENGTH),
        get_headers.get(header::CONTENT_LENGTH),
        "HEAD must preserve the corresponding paged GET representation length"
    );
    assert!(head_body.is_empty());
}

#[tokio::test]
async fn transport_is_strict_bounded_and_structured() {
    let state = test_state();
    for uri in [
        "/_vocab/aliases?unknown=true",
        "/_vocab/aliases?size=1&size=2",
        "/_vocab/aliases?from=not-a-number",
    ] {
        let (status, _, body) = send(&state, Method::GET, uri, Body::empty()).await;
        assert_error(status, &body, StatusCode::BAD_REQUEST, "validation_error");
    }

    let (status, _, body) = send(&state, Method::GET, "/_vocab/aliases", "{}").await;
    assert_error(status, &body, StatusCode::BAD_REQUEST, "validation_error");

    let oversized = vec![b'x'; ALIAS_READ_BODY_LIMIT + 1];
    let (status, _, body) = send(&state, Method::GET, "/_vocab/aliases", oversized).await;
    assert_error(
        status,
        &body,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let (status, headers, body) =
        send(&state, Method::POST, "/_vocab/aliases", Body::empty()).await;
    assert_error(
        status,
        &body,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, HEAD");
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, _, body) = tokio::time::timeout(
        Duration::from_secs(1),
        send(&state, Method::GET, "/_vocab/aliases", pending),
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
async fn read_waits_asynchronously_for_shared_admission_and_closed_admission_fails() {
    let state = test_state();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::GET,
            "/_vocab/aliases?size=0",
            Body::empty(),
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "alias-registry read should wait without blocking the runtime"
    );
    drop(held);

    let (status, _, body) = request.await.expect("request task");
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON aliases");
    assert_eq!(body["count"], 3, "{body}");
    assert!(body["aliases"]["entries"]
        .as_array()
        .expect("entries")
        .is_empty());

    state.stats_permits.close();
    let (status, _, body) = send(&state, Method::GET, "/_vocab/aliases", Body::empty()).await;
    assert_error(
        status,
        &body,
        StatusCode::SERVICE_UNAVAILABLE,
        "aliases_unavailable",
    );
}

fn assert_error(status: StatusCode, body: &Bytes, expected: StatusCode, kind: &str) {
    assert_eq!(status, expected);
    let body: serde_json::Value = serde_json::from_slice(body).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
    assert_eq!(body["status"], expected.as_u16(), "{body}");
}
