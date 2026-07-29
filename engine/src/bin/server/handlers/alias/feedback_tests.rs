use super::feedback::{
    alias_feedback_apply_method_not_allowed, validate_and_apply_feedback,
    ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
};

use std::convert::Infallible;
use std::path::{Path, PathBuf};
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
    config::EngineConfig,
    segment::{Engine, MatchScratch},
    vocab::{AliasFeedback, Vocab},
};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn fixture_vocab() -> Vocab {
    serde_json::from_value(serde_json::json!({
        "aliases": {
            "entries": [{
                "forms": ["aleph", "alpha"],
                "provenance": "learned_distributional",
                "kind": "single_token_distinct",
                "status": "candidate",
                "confidence": 0.8
            }]
        }
    }))
    .expect("fixture vocabulary")
}

fn fixture_engine(config: EngineConfig) -> (Engine, AliasFeedback) {
    let mut engine = Engine::with_vocab(fixture_vocab(), config).expect("fixture engine");
    engine.build_from_queries(&[
        (1, "vertex gamma".to_string()),
        (2, "vertex delta".to_string()),
        (3, "alpha widget".to_string()),
    ]);
    let mut feedback = AliasFeedback::default();
    feedback.sync_tracked(engine.aliases().expect("aliases"), 256);
    feedback.observe(&["aleph".into(), "vertex".into()], &[1, 2]);
    feedback.observe(&["alpha".into(), "vertex".into()], &[1, 2]);
    (engine, feedback)
}

fn state_with_engine(engine: Engine, feedback: AliasFeedback) -> Arc<AppState> {
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

fn test_state() -> Arc<AppState> {
    let (engine, feedback) = fixture_engine(EngineConfig {
        alias_feedback_capture: true,
        ..EngineConfig::default()
    });
    state_with_engine(engine, feedback)
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_vocab/aliases/validate_and_apply",
            post(validate_and_apply_feedback)
                .layer(DefaultBodyLimit::max(body_limit))
                .fallback(alias_feedback_apply_method_not_allowed::<AppState>),
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
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(body).expect("JSON error");
    assert_eq!(value["status"], expected.as_u16(), "{value}");
    assert_eq!(value["error"]["type"], kind, "{value}");
}

fn matches(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

#[tokio::test]
async fn stamp_retry_and_explicit_activation_are_truthful_and_observed() {
    let state = test_state();
    let before_epoch = state.engine.lock().vocab_epoch();
    assert!(
        !matches(&state.engine.lock(), "aleph widget").contains(&3),
        "candidate is not active before explicit activation"
    );
    let uri = "/_vocab/aliases/validate_and_apply?min_overlap=1&min_titles=1&min_queries=1";
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        uri,
        Body::empty(),
        ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["result"], "updated");
    assert_eq!(body["persisted"], false);
    assert_eq!(body["min_overlap"], 1.0);
    assert_eq!(body["min_titles"], 1);
    assert_eq!(body["min_queries"], 1);
    assert_eq!(body["activate"], false);
    assert_eq!(body["validated"], 1);
    assert_eq!(body["stamped"], 1);
    assert_eq!(body["activated"], 0);
    assert_eq!(body["recompiled"], 0);
    assert_eq!(state.engine.lock().vocab_epoch(), before_epoch);
    assert!(!matches(&state.engine.lock(), "aleph widget").contains(&3));

    let (status, _, bytes) = send(
        &state,
        Method::POST,
        uri,
        Body::empty(),
        ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let retry: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON retry");
    assert_eq!(retry["result"], "noop");
    assert_eq!(retry["validated"], 1);
    assert_eq!(retry["stamped"], 0);

    let activate = format!("{uri}&activate=true");
    let (status, _, bytes) = send(
        &state,
        Method::POST,
        &activate,
        Body::empty(),
        ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON activation");
    assert_eq!(body["result"], "updated");
    assert_eq!(body["stamped"], 0);
    assert_eq!(body["activated"], 1);
    assert_eq!(body["recompiled"], 3);
    assert!(matches(&state.engine.lock(), "aleph widget").contains(&3));
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_validate_and_apply", "200"])
            .get(),
        3
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_validate_and_apply"])
            .get_sample_count(),
        3
    );
}

#[tokio::test]
async fn query_body_size_deadline_and_method_are_strict() {
    let state = test_state();
    for query in [
        "unknown=1",
        "min_overlap=1.1",
        "min_titles=0",
        "min_queries=0",
        "activate=true&activate=false",
    ] {
        let uri = format!("/_vocab/aliases/validate_and_apply?{query}");
        let (status, headers, body) = send(
            &state,
            Method::POST,
            &uri,
            Body::empty(),
            ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
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
        Method::POST,
        "/_vocab/aliases/validate_and_apply",
        "{}",
        ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    )
    .await;
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::BAD_REQUEST,
        "validation_error",
    );

    let (status, headers, body) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/validate_and_apply",
        "oversized",
        4,
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
        Method::GET,
        "/_vocab/aliases/validate_and_apply",
        Body::empty(),
        ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    )
    .await;
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    );
    assert_eq!(headers.get(header::ALLOW).unwrap(), "POST");

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, headers, body) = tokio::time::timeout(
        Duration::from_secs(1),
        send(
            &state,
            Method::POST,
            "/_vocab/aliases/validate_and_apply",
            pending,
            ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
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
            Method::POST,
            "/_vocab/aliases/validate_and_apply?min_titles=1&min_queries=1",
            Body::empty(),
            ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "validation must wait asynchronously for administrative admission"
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
        Method::POST,
        "/_vocab/aliases/validate_and_apply",
        Body::empty(),
        ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
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
            Method::POST,
            "/_vocab/aliases/validate_and_apply?min_titles=1&min_queries=1",
            Body::empty(),
            ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
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

    let (engine_locked_tx, engine_locked_rx) = std::sync::mpsc::sync_channel(0);
    let (engine_release_tx, engine_release_rx) = std::sync::mpsc::sync_channel(0);
    let engine_state = Arc::clone(&state);
    let engine_thread = std::thread::spawn(move || {
        let _guard = engine_state.engine.lock();
        engine_locked_tx.send(()).expect("announce engine lock");
        engine_release_rx.recv().expect("release engine lock");
    });
    engine_locked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("engine lock held");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::POST,
            "/_vocab/aliases/validate_and_apply?min_titles=1&min_queries=1",
            Body::empty(),
            ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
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
    engine_release_tx.send(()).expect("release engine");
    engine_thread.join().expect("engine thread");
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);
    assert_eq!(state.stats_permits.available_permits(), 1);
}

fn temp_dir(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "rr-alias-feedback-apply-{tag}-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

fn poison_next_source_write(data_dir: &Path) {
    let generation = std::fs::read_dir(data_dir)
        .expect("read data dir")
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter_map(|name| {
            name.strip_prefix("sources_g")
                .and_then(|rest| rest.strip_suffix(".dat"))
                .and_then(|value| value.parse::<u64>().ok())
        })
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .expect("source generation");
    std::fs::create_dir(data_dir.join(format!("sources_g{generation:020}.sources.tmp")))
        .expect("poison source temp path");
}

#[tokio::test]
async fn durable_activation_failure_is_live_published_and_not_acknowledged() {
    let root = temp_dir("durability");
    let data_dir = root.join("data");
    let (engine, feedback) = fixture_engine(EngineConfig {
        alias_feedback_capture: true,
        data_dir: Some(data_dir.clone()),
        ..EngineConfig::default()
    });
    poison_next_source_write(&data_dir);
    let state = state_with_engine(engine, feedback);
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/validate_and_apply?min_overlap=1&min_titles=1&min_queries=1&activate=true",
        Body::empty(),
        ALIAS_FEEDBACK_APPLY_BODY_LIMIT,
    )
    .await;
    assert_error(
        status,
        &headers,
        &bytes,
        StatusCode::SERVICE_UNAVAILABLE,
        "persistence_unavailable",
    );
    let engine = state.engine.lock();
    assert!(!engine.persistence_healthy());
    assert!(!engine.has_stale_segments());
    assert!(matches(&engine, "aleph widget").contains(&3));
    drop(engine);
    assert_eq!(
        state
            .snapshot
            .load()
            .vocab()
            .expect("published vocabulary")
            .alias_summary()
            .active,
        1
    );

    drop(state);
    std::fs::remove_dir_all(root).expect("remove temp root");
}
