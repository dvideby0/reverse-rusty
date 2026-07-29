use super::discover::{
    alias_discover_method_not_allowed, discover_aliases, ALIAS_DISCOVER_BODY_LIMIT,
    ALIAS_DISCOVER_MAX_PAIRS, ALIAS_DISCOVER_MAX_QUERIES, ALIAS_DISCOVER_MAX_VOCAB,
};

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::{
    body::{Body, Bytes},
    extract::DefaultBodyLimit,
    http::{header, Method, Request, StatusCode},
    routing::post,
    Router,
};
use parking_lot::Mutex;
use reverse_rusty::{segment::Engine, Normalizer};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn discovery_corpus() -> Vec<(u64, String)> {
    let mut queries = Vec::new();
    let mut id = 1u64;
    for i in 0..40 {
        queries.push((id, format!("zzns ctxp{} ctxb{}", i % 7, i % 5)));
        id += 1;
        queries.push((id, format!("zznorthstar ctxp{} ctxb{}", i % 7, i % 5)));
        id += 1;
    }
    for i in 0..200 {
        queries.push((id, format!("filler{i} junk{i}")));
        id += 1;
    }
    queries
}

fn test_state(queries: &[(u64, String)]) -> Arc<AppState> {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(queries);
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
            "/_vocab/aliases/discover",
            post(discover_aliases)
                .layer(DefaultBodyLimit::max(body_limit))
                .fallback(alias_discover_method_not_allowed::<AppState>),
        )
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024))
        .with_state(Arc::clone(state))
}

async fn send(
    state: &Arc<AppState>,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
    content_type: Option<&str>,
    body_limit: usize,
) -> (StatusCode, axum::http::HeaderMap, Bytes) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(content_type) = content_type {
        request = request.header(header::CONTENT_TYPE, content_type);
    }
    let response = router(state, body_limit)
        .oneshot(request.body(body.into()).expect("request"))
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
    kind: &str,
) -> serde_json::Value {
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache control"),
        "no-store"
    );
    let decoded: serde_json::Value = serde_json::from_slice(body).expect("JSON error");
    assert_eq!(decoded["status"], status.as_u16(), "{decoded}");
    assert_eq!(decoded["error"]["type"], kind, "{decoded}");
    decoded
}

fn assert_planted_pair(value: &serde_json::Value) {
    assert!(value["count"].as_u64().expect("count") >= 1, "{value}");
    let planted = value["proposals"]
        .as_array()
        .expect("proposals")
        .iter()
        .any(|proposal| {
            let forms: Vec<&str> = proposal["forms"]
                .as_array()
                .expect("forms")
                .iter()
                .map(|form| form.as_str().expect("form"))
                .collect();
            forms.contains(&"zzns") && forms.contains(&"zznorthstar")
        });
    assert!(planted, "planted pair must be proposed: {value}");
}

#[tokio::test]
async fn explicit_discovery_is_timed_uncacheable_compute_only_and_observed() {
    let state = test_state(&[(900, "vertex adapter".to_string())]);
    let body = serde_json::json!({"queries": discovery_corpus()});
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover",
        body.to_string(),
        Some("application/vnd.opensearch+json; compatible-with=2"),
        ALIAS_DISCOVER_BODY_LIMIT,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache control"),
        "no-store"
    );
    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert!(value["took"].is_u64(), "{value}");
    assert!(value["took_ms"].as_f64().expect("took_ms") >= 0.0);
    assert_planted_pair(&value);
    assert_eq!(state.engine.lock().alias_summary().candidate, 0);
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_discover", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_discover"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn bodyless_discovery_uses_a_snapshot_of_stored_sources() {
    let state = test_state(&discovery_corpus());
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover",
        Body::empty(),
        None,
        ALIAS_DISCOVER_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_planted_pair(&value);
}

#[tokio::test]
async fn transport_rejects_method_query_media_json_size_and_stalled_bodies() {
    let state = test_state(&[]);

    let (status, headers, bytes) = send(
        &state,
        Method::GET,
        "/_vocab/aliases/discover",
        Body::empty(),
        None,
        ALIAS_DISCOVER_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    assert_error(status, &headers, &bytes, "method_not_allowed");

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover?refresh=true",
        Body::empty(),
        None,
        ALIAS_DISCOVER_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(status, &headers, &bytes, "validation_error");

    for content_type in [None, Some("text/plain")] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/discover",
            "{}",
            content_type,
            ALIAS_DISCOVER_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_error(status, &headers, &bytes, "unsupported_media_type");
    }

    for invalid in [
        "{",
        r#"{"queries":[],"unknown":true}"#,
        r#"{"queries":"not-an-array"}"#,
    ] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/discover",
            invalid,
            Some("application/json"),
            ALIAS_DISCOVER_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_error(status, &headers, &bytes, "validation_error");
    }

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover",
        vec![b'x'; 129],
        Some("application/json"),
        128,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_error(status, &headers, &bytes, "payload_too_large");

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, headers, bytes) = tokio::time::timeout(
        Duration::from_secs(6),
        send(
            &state,
            Method::POST,
            "/_vocab/aliases/discover",
            pending,
            Some("application/json"),
            ALIAS_DISCOVER_BODY_LIMIT,
        ),
    )
    .await
    .expect("body deadline");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_error(status, &headers, &bytes, "request_timeout");
}

#[tokio::test]
async fn validation_rejects_ambiguous_corpora_and_unsafe_controls() {
    let state = test_state(&[]);
    let cases = [
        serde_json::json!({"queries": [[1, "a"], [1, "b"]]}),
        serde_json::json!({"queries": [[1, "("]]}),
        serde_json::json!({"queries": [], "min_token_freq": 0}),
        serde_json::json!({"queries": [], "min_similarity": -0.1}),
        serde_json::json!({"queries": [], "min_similarity": 1.1}),
        serde_json::json!({"queries": [], "max_pairs": ALIAS_DISCOVER_MAX_PAIRS + 1}),
        serde_json::json!({"queries": [], "max_vocab": 0}),
        serde_json::json!({"queries": [], "max_vocab": ALIAS_DISCOVER_MAX_VOCAB + 1}),
        serde_json::json!({"queries": [], "max_cooccurrence_rate": -0.1}),
        serde_json::json!({"queries": [], "max_cooccurrence_rate": 1.1}),
    ];
    for request in cases {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/discover",
            request.to_string(),
            Some("application/json"),
            ALIAS_DISCOVER_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{request}");
        assert_error(status, &headers, &bytes, "validation_error");
    }

    let long_query = "x".repeat(reverse_rusty::dsl::MAX_QUERY_LENGTH + 1);
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover",
        serde_json::json!({"queries": [[1, long_query]]}).to_string(),
        Some("application/json"),
        ALIAS_DISCOVER_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = assert_error(status, &headers, &bytes, "validation_error");
    assert!(
        error["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("maximum length"),
        "{error}"
    );
}

#[tokio::test]
async fn explicit_corpus_cardinality_is_bounded_before_discovery() {
    let state = test_state(&[]);
    let queries: Vec<(u64, String)> = (0..=ALIAS_DISCOVER_MAX_QUERIES as u64)
        .map(|id| (id, String::new()))
        .collect();
    let encoded =
        serde_json::to_vec(&serde_json::json!({"queries": queries})).expect("encoded corpus");
    assert!(encoded.len() < ALIAS_DISCOVER_BODY_LIMIT);
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover",
        encoded,
        Some("application/json"),
        ALIAS_DISCOVER_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = assert_error(status, &headers, &bytes, "validation_error");
    assert!(
        error["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("maximum is 100000"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn discovery_waits_asynchronously_for_admission_and_closed_admission_fails() {
    let state = test_state(&[]);
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::POST,
            "/_vocab/aliases/discover",
            r#"{"queries":[]}"#,
            Some("application/json"),
            ALIAS_DISCOVER_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "discovery must wait asynchronously for administrative admission"
    );
    tokio::time::timeout(
        Duration::from_millis(100),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .expect("Tokio worker remained responsive");
    drop(held);
    assert_eq!(
        request.await.expect("request task").0,
        StatusCode::OK,
        "admitted dry run"
    );

    state.stats_permits.close();
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover",
        r#"{"queries":[]}"#,
        Some("application/json"),
        ALIAS_DISCOVER_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_error(status, &headers, &bytes, "aliases_unavailable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn stored_source_lock_wait_runs_off_runtime_and_keeps_admission() {
    let state = test_state(&[]);
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let lock_state = Arc::clone(&state);
    let lock_thread = std::thread::spawn(move || {
        let _guard = lock_state.engine.lock();
        locked_tx.send(()).expect("announce lock");
        release_rx.recv().expect("release lock");
    });
    locked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("engine lock held");

    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::POST,
            "/_vocab/aliases/discover",
            Body::empty(),
            None,
            ALIAS_DISCOVER_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "request must remain pending behind the engine lock"
    );
    tokio::time::timeout(
        Duration::from_millis(100),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .expect("Tokio worker remained responsive");
    assert_eq!(
        state.stats_permits.available_permits(),
        0,
        "blocking worker owns admission while waiting for the engine"
    );

    release_tx.send(()).expect("release engine");
    lock_thread.join().expect("lock thread");
    assert_eq!(
        request.await.expect("request task").0,
        StatusCode::OK,
        "stored-corpus discovery completes after lock release"
    );
    assert_eq!(state.stats_permits.available_permits(), 1);
}
