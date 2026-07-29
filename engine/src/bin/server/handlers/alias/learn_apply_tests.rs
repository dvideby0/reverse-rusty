use super::learn_apply::{
    alias_learn_apply_method_not_allowed, learn_and_apply_aliases, ALIAS_LEARN_APPLY_BODY_LIMIT,
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
    Normalizer,
};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn state_with_engine(engine: Engine) -> Arc<AppState> {
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

fn seeded_engine(config: EngineConfig) -> Engine {
    let mut engine = Engine::with_config(Normalizer::default_vocab().expect("normalizer"), config);
    engine.build_from_queries(&[
        (1, "vertex adapter".to_string()),
        (10, "(adapter,adapters) 2024".to_string()),
        (20, "(adapter,adapters) 2023".to_string()),
    ]);
    engine
}

fn memory_state() -> Arc<AppState> {
    state_with_engine(seeded_engine(EngineConfig::default()))
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_vocab/aliases/learn_and_apply",
            post(learn_and_apply_aliases)
                .layer(DefaultBodyLimit::max(body_limit))
                .fallback(alias_learn_apply_method_not_allowed::<AppState>),
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

fn assert_matches(engine: &Engine, title: &str, logical: u64) {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    assert!(out.contains(&logical), "{title:?} did not match {logical}");
}

#[tokio::test]
async fn own_corpus_alias_learning_applies_synchronously_and_is_observable() {
    let state = memory_state();
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/learn_and_apply?min_count=2",
        Body::empty(),
        ALIAS_LEARN_APPLY_BODY_LIMIT,
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
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["activated"], 1, "{body}");
    assert_eq!(body["recompiled"], 3, "{body}");
    assert!(body["rebuilt"].is_null(), "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_number(), "{body}");
    assert_eq!(body["summary"]["active"], 1, "{body}");

    let engine = state.engine.lock();
    assert_matches(&engine, "vertex adapters", 1);
    assert!(!engine.has_stale_segments());
    assert!(engine.vocab().is_some_and(|vocab| {
        vocab
            .aliases()
            .active_groups()
            .iter()
            .any(|forms| forms == &vec!["adapter".to_string(), "adapters".to_string()])
    }));
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
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_learn_apply", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_learn_apply"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn transport_and_min_count_are_strict_and_bounded() {
    let state = memory_state();

    let (status, headers, bytes) = send(
        &state,
        Method::GET,
        "/_vocab/aliases/learn_and_apply",
        Body::empty(),
        ALIAS_LEARN_APPLY_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    assert_error(status, &headers, &bytes, "method_not_allowed");

    for uri in [
        "/_vocab/aliases/learn_and_apply?unknown=true",
        "/_vocab/aliases/learn_and_apply?min_count=0",
        "/_vocab/aliases/learn_and_apply?min_count=two",
        "/_vocab/aliases/learn_and_apply?min_count=2&min_count=3",
    ] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            uri,
            Body::empty(),
            ALIAS_LEARN_APPLY_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_error(status, &headers, &bytes, "validation_error");
    }

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/learn_and_apply",
        "{}",
        ALIAS_LEARN_APPLY_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error(status, &headers, &bytes, "validation_error");

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/learn_and_apply",
        vec![b'x'; 129],
        128,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_error(status, &headers, &bytes, "payload_too_large");

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, headers, bytes) = tokio::time::timeout(
        Duration::from_secs(2),
        send(
            &state,
            Method::POST,
            "/_vocab/aliases/learn_and_apply",
            pending,
            ALIAS_LEARN_APPLY_BODY_LIMIT,
        ),
    )
    .await
    .expect("body deadline");
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_error(status, &headers, &bytes, "request_timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn admission_and_engine_lock_waits_do_not_block_tokio() {
    let state = memory_state();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::POST,
            "/_vocab/aliases/learn_and_apply",
            Body::empty(),
            ALIAS_LEARN_APPLY_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "alias learn-and-apply must wait asynchronously for administrative admission"
    );
    drop(held);
    assert_eq!(request.await.expect("request task").0, StatusCode::OK);

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
            Method::POST,
            "/_vocab/aliases/learn_and_apply",
            Body::empty(),
            ALIAS_LEARN_APPLY_BODY_LIMIT,
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
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/learn_and_apply",
        Body::empty(),
        ALIAS_LEARN_APPLY_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_error(status, &headers, &bytes, "aliases_unavailable");
}

fn temp_dir(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "rr-alias-learn-apply-{tag}-{}",
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
async fn durable_commit_failure_is_live_but_not_acknowledged() {
    let root = temp_dir("durability");
    let data_dir = root.join("data");
    let engine = seeded_engine(EngineConfig {
        data_dir: Some(data_dir.clone()),
        ..EngineConfig::default()
    });
    poison_next_source_write(&data_dir);
    let state = state_with_engine(engine);

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/learn_and_apply",
        Body::empty(),
        ALIAS_LEARN_APPLY_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let body = assert_error(status, &headers, &bytes, "persistence_unavailable");
    assert!(
        body["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("live"),
        "{body}"
    );

    let engine = state.engine.lock();
    assert!(!engine.persistence_healthy());
    assert!(!engine.has_stale_segments());
    assert_matches(&engine, "vertex adapters", 1);
    drop(engine);
    assert_eq!(
        state
            .snapshot
            .load()
            .vocab()
            .expect("published vocabulary")
            .alias_summary()
            .active,
        1,
        "the coherent live state must still be published"
    );

    drop(state);
    std::fs::remove_dir_all(root).expect("remove temp root");
}
