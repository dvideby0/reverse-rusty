use super::learn::{
    learn_vocab, vocab_learn_method_not_allowed, VOCAB_LEARN_BODY_LIMIT,
    VOCAB_LEARN_MAX_NPMI_ITERATIONS, VOCAB_LEARN_MAX_NPMI_TOKENS, VOCAB_LEARN_MAX_QUERIES,
    VOCAB_LEARN_MAX_RELATIONSHIP_OBSERVATIONS,
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

fn test_state() -> Arc<AppState> {
    let engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
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
        feedback: Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
        pit_tokens: crate::pit::PitTokens::generate(),
        pits: Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config: reverse_rusty::PitConfig::default(),
    })
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_vocab/learn",
            post(learn_vocab)
                .layer(DefaultBodyLimit::max(body_limit))
                .fallback(vocab_learn_method_not_allowed::<AppState>),
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

#[tokio::test]
async fn dry_run_returns_a_round_trippable_uncacheable_vocab_and_metrics() {
    let state = test_state();
    let request = serde_json::json!({
        "queries": [
            [10, "(package,pkg) 2024"],
            [20, "(package,pkg) 2023"]
        ],
        "min_count": 2
    });
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/learn",
        request.to_string(),
        Some("application/vnd.elasticsearch+json; compatible-with=8"),
        VOCAB_LEARN_BODY_LIMIT,
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
    assert_eq!(value["synonyms"].as_array().expect("synonyms").len(), 1);
    assert_eq!(value["synonyms"][0]["token"], "pkg");
    let learned: reverse_rusty::vocab::Vocab =
        serde_json::from_slice(&bytes).expect("round-trip vocabulary");
    learned
        .to_normalizer()
        .expect("learned vocabulary must be installable");
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
            .http_request_duration
            .with_label_values(&["vocab_learn"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn threshold_counts_distinct_queries_not_repeated_clauses() {
    let state = test_state();
    let repeated = serde_json::json!({
        "queries": [[10, "(package,pkg) (package,pkg)"]],
        "min_count": 2
    });
    let (status, _, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/learn",
        repeated.to_string(),
        Some("application/json"),
        VOCAB_LEARN_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert!(value["synonyms"].as_array().expect("synonyms").is_empty());
    assert!(value["phrases"].as_array().expect("phrases").is_empty());
}

#[tokio::test]
async fn transport_rejects_method_query_media_json_size_and_stalled_bodies() {
    let state = test_state();
    let valid = r#"{"queries":[]}"#;

    let (status, headers, bytes) = send(
        &state,
        Method::GET,
        "/_vocab/learn",
        Body::empty(),
        None,
        VOCAB_LEARN_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    assert_error(status, &headers, &bytes, "method_not_allowed");

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/learn?refresh=true",
        valid,
        Some("application/json"),
        VOCAB_LEARN_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(status, &headers, &bytes, "validation_error");

    for content_type in [None, Some("text/plain")] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/learn",
            valid,
            content_type,
            VOCAB_LEARN_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_error(status, &headers, &bytes, "unsupported_media_type");
    }

    for invalid in [
        "{",
        r"{}",
        r#"{"queries":[],"unknown":true}"#,
        r#"{"queries":"not-an-array"}"#,
    ] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/learn",
            invalid,
            Some("application/json"),
            VOCAB_LEARN_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_error(status, &headers, &bytes, "validation_error");
    }

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/learn",
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
            "/_vocab/learn",
            pending,
            Some("application/json"),
            VOCAB_LEARN_BODY_LIMIT,
        ),
    )
    .await
    .expect("body deadline");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_error(status, &headers, &bytes, "request_timeout");
}

#[tokio::test]
async fn validation_rejects_ambiguous_or_unbounded_learning_controls() {
    let state = test_state();
    let cases = [
        serde_json::json!({"queries": [], "min_count": 0}),
        serde_json::json!({"queries": [[1, "a"], [1, "b"]]}),
        serde_json::json!({"queries": [[1, "("]]}),
        serde_json::json!({"queries": [], "npmi_tau": 0.5}),
        serde_json::json!({
            "queries": [],
            "corpus_phrases": true,
            "npmi_tau": 1.1
        }),
        serde_json::json!({
            "queries": [],
            "corpus_phrases": true,
            "npmi_min_count": 0
        }),
        serde_json::json!({
            "queries": [],
            "corpus_phrases": true,
            "npmi_iterations": 0
        }),
        serde_json::json!({
            "queries": [],
            "corpus_phrases": true,
            "npmi_iterations": VOCAB_LEARN_MAX_NPMI_ITERATIONS + 1
        }),
    ];
    for request in cases {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/learn",
            request.to_string(),
            Some("application/json"),
            VOCAB_LEARN_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{request}");
        assert_error(status, &headers, &bytes, "validation_error");
    }

    let long_query = "x".repeat(reverse_rusty::dsl::MAX_QUERY_LENGTH + 1);
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/learn",
        serde_json::json!({"queries": [[1, long_query]]}).to_string(),
        Some("application/json"),
        VOCAB_LEARN_BODY_LIMIT,
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
async fn corpus_cardinality_is_bounded_before_learning() {
    let state = test_state();
    let queries: Vec<(u64, String)> = (0..=VOCAB_LEARN_MAX_QUERIES as u64)
        .map(|id| (id, String::new()))
        .collect();
    let encoded = serde_json::to_vec(&serde_json::json!({"queries": queries}))
        .expect("encoded oversized corpus");
    assert!(encoded.len() < VOCAB_LEARN_BODY_LIMIT);
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/learn",
        encoded,
        Some("application/json"),
        VOCAB_LEARN_BODY_LIMIT,
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

#[tokio::test]
async fn relationship_and_phrase_work_are_bounded_before_learning() {
    let state = test_state();
    let members = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ._"
        .chars()
        .map(|member| member.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let group = format!("({members})");
    let query = std::iter::repeat_n(group, 50).collect::<Vec<_>>().join(" ");
    assert!(query.len() < reverse_rusty::dsl::MAX_QUERY_LENGTH);
    let relationship_request = serde_json::json!({
        "queries": [[1, query]],
        "learn_equivalences": true
    });
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/learn",
        relationship_request.to_string(),
        Some("application/json"),
        VOCAB_LEARN_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = assert_error(status, &headers, &bytes, "validation_error");
    assert!(
        error["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains(&VOCAB_LEARN_MAX_RELATIONSHIP_OBSERVATIONS.to_string()),
        "{error}"
    );

    let tokens_per_query = 251;
    let query = std::iter::repeat_n("a", tokens_per_query)
        .collect::<Vec<_>>()
        .join(" ");
    let query_count = VOCAB_LEARN_MAX_NPMI_TOKENS / tokens_per_query + 1;
    let queries: Vec<(u64, String)> = (0..query_count as u64)
        .map(|id| (id, query.clone()))
        .collect();
    let phrase_request = serde_json::json!({
        "queries": queries,
        "corpus_phrases": true
    });
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/learn",
        phrase_request.to_string(),
        Some("application/json"),
        VOCAB_LEARN_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = assert_error(status, &headers, &bytes, "validation_error");
    assert!(
        error["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains(&VOCAB_LEARN_MAX_NPMI_TOKENS.to_string()),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn learning_waits_asynchronously_for_admission_and_closed_admission_fails() {
    let state = test_state();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::POST,
            "/_vocab/learn",
            r#"{"queries":[]}"#,
            Some("application/json"),
            VOCAB_LEARN_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "learning must wait asynchronously for administrative admission"
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
        "/_vocab/learn",
        r#"{"queries":[]}"#,
        Some("application/json"),
        VOCAB_LEARN_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_error(status, &headers, &bytes, "vocab_unavailable");
}
