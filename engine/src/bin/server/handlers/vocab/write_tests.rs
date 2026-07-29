use super::{
    get_vocab, put_vocab, vocab_method_not_allowed, VOCAB_READ_BODY_LIMIT, VOCAB_WRITE_BODY_LIMIT,
};

use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use axum::{
    body::{Body, Bytes},
    extract::DefaultBodyLimit,
    http::{header, Request, StatusCode},
    routing::{get, put},
    Router,
};
use parking_lot::Mutex;
use reverse_rusty::{
    config::EngineConfig,
    segment::{Engine, MatchScratch},
    Normalizer,
};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn state_with_engine(engine: Engine) -> Arc<AppState> {
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

fn memory_state() -> Arc<AppState> {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&[(7, "pkg".to_string())]);
    state_with_engine(engine)
}

fn router(state: &Arc<AppState>, write_limit: usize) -> Router {
    Router::new()
        .route(
            "/_vocab",
            get(get_vocab)
                .layer(DefaultBodyLimit::max(VOCAB_READ_BODY_LIMIT))
                .merge(put(put_vocab).layer(DefaultBodyLimit::max(write_limit)))
                .fallback(vocab_method_not_allowed::<AppState>),
        )
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024))
        .with_state(Arc::clone(state))
}

fn replacement() -> serde_json::Value {
    serde_json::json!({
        "synonyms": [
            {"token": "pkg", "canonical": "term:package", "kind": "generic"}
        ],
        "phrases": [],
        "equivalences": [],
        "punctuation": [],
        "aliases": {"entries": []}
    })
}

async fn send(
    state: &Arc<AppState>,
    uri: &str,
    body: impl Into<Body>,
    content_type: Option<&str>,
    write_limit: usize,
) -> (StatusCode, axum::http::HeaderMap, Bytes) {
    let mut request = Request::builder().method("PUT").uri(uri);
    if let Some(content_type) = content_type {
        request = request.header(header::CONTENT_TYPE, content_type);
    }
    let response = router(state, write_limit)
        .oneshot(request.body(body.into()).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (status, headers, body)
}

fn assert_error(status: StatusCode, headers: &axum::http::HeaderMap, body: &Bytes, kind: &str) {
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache control"),
        "no-store"
    );
    let decoded: serde_json::Value = serde_json::from_slice(body).expect("JSON error");
    assert_eq!(decoded["status"], status.as_u16(), "{decoded}");
    assert_eq!(decoded["error"]["type"], kind, "{decoded}");
}

fn assert_matches(engine: &Engine, title: &str, logical: u64) {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    assert!(out.contains(&logical), "{title:?} did not match {logical}");
}

#[tokio::test]
async fn replacement_is_synchronous_round_trippable_and_observable() {
    let state = memory_state();
    let (status, headers, body) = send(
        &state,
        "/_vocab",
        replacement().to_string(),
        Some("application/vnd.elasticsearch+json; compatible-with=8"),
        VOCAB_WRITE_BODY_LIMIT,
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
    let decoded: serde_json::Value = serde_json::from_slice(&body).expect("JSON response");
    assert_eq!(decoded["acknowledged"], true);
    assert_eq!(decoded["recompiled"], 1);
    assert!(decoded["rebuilt"].is_null(), "{decoded}");
    assert!(decoded["took"].is_u64(), "{decoded}");
    assert!(decoded["took_ms"].is_number(), "{decoded}");

    let installed = state
        .snapshot
        .load()
        .vocab()
        .cloned()
        .expect("published vocabulary");
    assert_eq!(
        serde_json::to_value(installed).expect("installed JSON"),
        replacement()
    );
    assert_matches(&state.engine.lock(), "package", 7);
    assert!(!state.engine.lock().has_stale_segments());

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_put", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_put"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn replacement_canonicalizes_duplicate_physical_histories() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine
        .try_insert_live("pkg", 7, 1)
        .expect("first additive insert");
    engine.flush();
    engine
        .try_insert_live("pkg", 7, 2)
        .expect("second additive insert");
    assert_eq!(engine.num_live_queries(), 2, "two physical live rows");
    assert_eq!(engine.live_sources().len(), 1, "one current logical source");
    let state = state_with_engine(engine);

    let (status, _, body) = send(
        &state,
        "/_vocab",
        replacement().to_string(),
        Some("application/json"),
        VOCAB_WRITE_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let decoded: serde_json::Value = serde_json::from_slice(&body).expect("JSON response");
    assert_eq!(decoded["recompiled"], 1, "{decoded}");
    let engine = state.engine.lock();
    assert_eq!(
        engine.num_live_queries(),
        1,
        "rebuild canonicalizes the duplicate history"
    );
    assert!(!engine.has_stale_segments());
    assert_matches(&engine, "package", 7);
    drop(engine);
    assert!(
        state.snapshot.load().has_live_query(7),
        "coherent replacement snapshot was published"
    );
}

#[tokio::test]
async fn transport_rejects_query_media_json_size_and_stalled_bodies() {
    let state = memory_state();
    let valid = replacement().to_string();

    let (status, headers, body) = send(
        &state,
        "/_vocab?refresh=true",
        valid.clone(),
        Some("application/json"),
        VOCAB_WRITE_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(status, &headers, &body, "validation_error");

    for content_type in [None, Some("text/plain")] {
        let (status, headers, body) = send(
            &state,
            "/_vocab",
            valid.clone(),
            content_type,
            VOCAB_WRITE_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_error(status, &headers, &body, "unsupported_media_type");
    }

    for invalid in ["{", r#"{"synonyms":[],"unknown":[]}"#] {
        let (status, headers, body) = send(
            &state,
            "/_vocab",
            invalid,
            Some("application/json"),
            VOCAB_WRITE_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_error(status, &headers, &body, "validation_error");
    }

    let (status, headers, body) = send(
        &state,
        "/_vocab",
        vec![b'x'; 129],
        Some("application/json"),
        128,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_error(status, &headers, &body, "payload_too_large");

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, headers, body) = tokio::time::timeout(
        Duration::from_secs(6),
        send(
            &state,
            "/_vocab",
            pending,
            Some("application/json"),
            VOCAB_WRITE_BODY_LIMIT,
        ),
    )
    .await
    .expect("body deadline");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_error(status, &headers, &body, "request_timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn admission_and_engine_lock_waits_do_not_block_the_async_runtime() {
    let state = memory_state();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            "/_vocab",
            replacement().to_string(),
            Some("application/json"),
            VOCAB_WRITE_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "vocabulary write must wait asynchronously for administrative admission"
    );
    drop(held);
    assert_eq!(
        request.await.expect("request task").0,
        StatusCode::OK,
        "admitted replacement"
    );

    let lock_state = Arc::clone(&state);
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_thread = std::thread::spawn(move || {
        let _engine = lock_state.engine.lock();
        held_tx.send(()).expect("held signal");
        release_rx.recv().expect("release signal");
    });
    held_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("engine lock held");

    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            "/_vocab",
            replacement().to_string(),
            Some("application/json"),
            VOCAB_WRITE_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "the blocking worker should wait on the engine lock"
    );
    tokio::time::timeout(
        Duration::from_millis(100),
        tokio::time::sleep(Duration::from_millis(5)),
    )
    .await
    .expect("Tokio worker remained responsive");
    release_tx.send(()).expect("release engine");
    lock_thread.join().expect("lock thread");
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);

    state.stats_permits.close();
    let (status, headers, body) = send(
        &state,
        "/_vocab",
        replacement().to_string(),
        Some("application/json"),
        VOCAB_WRITE_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_error(status, &headers, &body, "vocab_unavailable");
}

fn temp_dir(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("rr-vocab-write-{tag}-{}", uuid::Uuid::new_v4()));
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
async fn durable_rebuild_failure_is_live_but_never_acknowledged() {
    let root = temp_dir("durability");
    let data_dir = root.join("data");
    let config = EngineConfig {
        data_dir: Some(data_dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(Normalizer::default_vocab().expect("normalizer"), config);
    engine.build_from_queries(&[(7, "pkg".to_string())]);
    poison_next_source_write(&data_dir);
    let state = state_with_engine(engine);

    let (status, headers, body) = send(
        &state,
        "/_vocab",
        replacement().to_string(),
        Some("application/json"),
        VOCAB_WRITE_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_error(status, &headers, &body, "persistence_unavailable");
    let decoded: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert!(
        decoded["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("live"),
        "{decoded}"
    );

    let engine = state.engine.lock();
    assert!(!engine.persistence_healthy());
    assert!(!engine.has_stale_segments());
    assert_matches(&engine, "package", 7);
    drop(engine);
    assert!(
        state.snapshot.load().vocab().is_some(),
        "coherent live replacement must still be published"
    );

    drop(state);
    std::fs::remove_dir_all(root).expect("remove temp root");
}
