use super::discover::{ALIAS_DISCOVER_MAX_PAIRS, ALIAS_DISCOVER_MAX_VOCAB};
use super::discover_record::{
    alias_discover_record_method_not_allowed, discover_and_record_aliases,
    ALIAS_DISCOVER_RECORD_BODY_LIMIT,
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
use reverse_rusty::{
    segment::{Engine, MatchScratch},
    Normalizer,
};
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

fn test_state() -> Arc<AppState> {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&discovery_corpus());
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
            "/_vocab/aliases/discover_and_record",
            post(discover_and_record_aliases)
                .layer(DefaultBodyLimit::max(body_limit))
                .fallback(alias_discover_record_method_not_allowed::<AppState>),
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

fn matches(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

#[tokio::test]
async fn records_candidates_only_with_timing_truthful_persistence_and_observation() {
    let state = test_state();
    let before = {
        let engine = state.engine.lock();
        (
            engine.vocab_epoch(),
            ["zzns ctxp1 ctxb1", "zznorthstar ctxp2 ctxb2", "noise"]
                .map(|title| matches(&engine, title)),
        )
    };
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover_and_record",
        r#"{"min_token_freq":5}"#,
        Some("application/vnd.opensearch+json; compatible-with=2"),
        ALIAS_DISCOVER_RECORD_BODY_LIMIT,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert!(value["took"].is_u64(), "{value}");
    assert!(value["took_ms"].as_f64().expect("took_ms") >= 0.0);
    assert_eq!(value["acknowledged"], true);
    assert_eq!(value["persisted"], false);
    assert!(
        value["proposed"].as_u64().expect("proposed") >= 1,
        "{value}"
    );
    assert!(
        value["new_candidates"].as_u64().expect("new candidates") >= 1,
        "{value}"
    );
    assert_eq!(value["recompiled"], 0);
    assert_eq!(value["summary"]["active"], 0);
    assert!(value["summary"]["candidate"].as_u64().expect("candidate") >= 1);

    let after = {
        let engine = state.engine.lock();
        (
            engine.vocab_epoch(),
            ["zzns ctxp1 ctxb1", "zznorthstar ctxp2 ctxb2", "noise"]
                .map(|title| matches(&engine, title)),
        )
    };
    assert_eq!(before, after, "recording metadata must not change matching");
    assert!(
        state
            .snapshot
            .load()
            .vocab()
            .expect("published vocab")
            .alias_summary()
            .candidate
            >= 1
    );
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_discover_and_record", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_discover_and_record"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn rediscovery_is_idempotent_and_explicit_corpora_are_refused() {
    let state = test_state();
    for expected_new in [true, false] {
        let (status, _, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/discover_and_record",
            Body::empty(),
            None,
            ALIAS_DISCOVER_RECORD_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
        if expected_new {
            assert!(value["new_candidates"].as_u64().unwrap() >= 1, "{value}");
        } else {
            assert_eq!(value["new_candidates"], 0, "{value}");
            assert!(value["rediscovered"].as_u64().unwrap() >= 1, "{value}");
        }
    }

    let candidate_count = state.engine.lock().alias_summary().candidate;
    let published_before_error = state.snapshot.load_full();
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover_and_record",
        r#"{"queries":[]}"#,
        Some("application/json"),
        ALIAS_DISCOVER_RECORD_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = assert_error(status, &headers, &bytes, "validation_error");
    assert!(
        error["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("explicit corpus"),
        "{error}"
    );
    assert_eq!(
        state.engine.lock().alias_summary().candidate,
        candidate_count
    );
    assert!(
        Arc::ptr_eq(&published_before_error, &state.snapshot.load_full()),
        "a rejected request must not republish the engine snapshot"
    );
}

#[tokio::test]
async fn transport_rejects_method_query_media_json_size_and_stalled_bodies() {
    let state = test_state();

    let (status, headers, bytes) = send(
        &state,
        Method::GET,
        "/_vocab/aliases/discover_and_record",
        Body::empty(),
        None,
        ALIAS_DISCOVER_RECORD_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).unwrap(), "POST");
    assert_error(status, &headers, &bytes, "method_not_allowed");

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover_and_record?refresh=true",
        Body::empty(),
        None,
        ALIAS_DISCOVER_RECORD_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(status, &headers, &bytes, "validation_error");

    for content_type in [None, Some("text/plain")] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/discover_and_record",
            "{}",
            content_type,
            ALIAS_DISCOVER_RECORD_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_error(status, &headers, &bytes, "unsupported_media_type");
    }

    for invalid in ["{", r#"{"unknown":true}"#, r#"{"min_token_freq":"many"}"#] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/discover_and_record",
            invalid,
            Some("application/json"),
            ALIAS_DISCOVER_RECORD_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
        assert_error(status, &headers, &bytes, "validation_error");
    }

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover_and_record",
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
            "/_vocab/aliases/discover_and_record",
            pending,
            Some("application/json"),
            ALIAS_DISCOVER_RECORD_BODY_LIMIT,
        ),
    )
    .await
    .expect("body deadline");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_error(status, &headers, &bytes, "request_timeout");
}

#[tokio::test]
async fn unsafe_discovery_controls_are_rejected_before_mutation() {
    let state = test_state();
    let published_before = state.snapshot.load_full();
    let cases = [
        serde_json::json!({"min_token_freq": 0}),
        serde_json::json!({"min_similarity": -0.1}),
        serde_json::json!({"min_similarity": 1.1}),
        serde_json::json!({"max_pairs": ALIAS_DISCOVER_MAX_PAIRS + 1}),
        serde_json::json!({"max_vocab": 0}),
        serde_json::json!({"max_vocab": ALIAS_DISCOVER_MAX_VOCAB + 1}),
        serde_json::json!({"max_cooccurrence_rate": -0.1}),
        serde_json::json!({"max_cooccurrence_rate": 1.1}),
    ];
    for request in cases {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/discover_and_record",
            request.to_string(),
            Some("application/json"),
            ALIAS_DISCOVER_RECORD_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{request}");
        assert_error(status, &headers, &bytes, "validation_error");
    }
    assert_eq!(state.engine.lock().alias_summary().candidate, 0);
    assert!(
        Arc::ptr_eq(&published_before, &state.snapshot.load_full()),
        "validation errors must not publish snapshots"
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
            Method::POST,
            "/_vocab/aliases/discover_and_record",
            Body::empty(),
            None,
            ALIAS_DISCOVER_RECORD_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "request must wait asynchronously for administrative admission"
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
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/discover_and_record",
        Body::empty(),
        None,
        ALIAS_DISCOVER_RECORD_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_error(status, &headers, &bytes, "aliases_unavailable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn engine_lock_wait_runs_off_runtime_and_keeps_admission() {
    let state = test_state();
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
            "/_vocab/aliases/discover_and_record",
            Body::empty(),
            None,
            ALIAS_DISCOVER_RECORD_BODY_LIMIT,
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
    assert_eq!(state.stats_permits.available_permits(), 0);

    release_tx.send(()).expect("release engine");
    lock_thread.join().expect("lock thread");
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);
    assert_eq!(state.stats_permits.available_permits(), 1);
}
