use super::flush_route;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use arc_swap::ArcSwap;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{header, Method, Request, StatusCode};
use axum::routing::any;
use axum::Router;
use parking_lot::Mutex;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;
use reverse_rusty::Normalizer;
use std::sync::Arc;
use tower::ServiceExt;

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
        snapshot: ArcSwap::new(snapshot),
        pool,
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

fn state_with_memtable() -> Arc<AppState> {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine.try_insert_live("1994 topps", 7, 1).expect("insert");
    state_with_engine(engine)
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route("/_flush", any(flush_route))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(Arc::clone(state))
}

async fn send(
    state: &Arc<AppState>,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
    let response = router(state, 64 * 1024)
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
    let body = serde_json::from_slice(&bytes).expect("JSON response");
    (status, headers, body)
}

#[tokio::test]
async fn get_and_post_flush_are_es_familiar_and_idempotent() {
    let state = state_with_memtable();
    let (status, _, body) = send(
        &state,
        Method::GET,
        "/_flush?force=true&wait_if_ongoing=true",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 1, "successful": 1, "failed": 0})
    );
    assert_eq!(body["total_queries"], 1);
    assert_eq!(body["base_segments"], 1);
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 0);

    let (status, _, body) = send(
        &state,
        Method::POST,
        "/_flush?force=false&wait_if_ongoing=false",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["base_segments"], 1, "a clean flush is a no-op");
}

#[tokio::test]
async fn transport_controls_are_strict_and_precede_mutation() {
    let state = state_with_memtable();
    for uri in [
        "/_flush?routing=one",
        "/_flush?force=maybe",
        "/_flush?wait_if_ongoing=true&wait_if_ongoing=false",
    ] {
        let (status, _, body) = send(&state, Method::POST, uri, Body::empty()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "validation_error", "{body}");
        assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);
    }

    let (status, _, body) = send(&state, Method::POST, "/_flush", "{}").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error", "{body}");
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);

    let (status, headers, body) = send(&state, Method::PUT, "/_flush", Body::empty()).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET, POST");
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);

    let response = router(&state, 4)
        .oneshot(
            Request::post("/_flush")
                .body(Body::from("12345"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);
}

#[tokio::test]
async fn nonwaiting_flush_conflicts_only_with_an_explicit_flush() {
    let state = state_with_memtable();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_state = Arc::clone(&state);
    let holder = std::thread::spawn(move || {
        let _held = lock_state.flush_serial.lock();
        locked_tx.send(()).expect("report held lock");
        release_rx.recv().expect("release held lock");
    });
    locked_rx.recv().expect("wait for held lock");
    let (status, _, body) = send(
        &state,
        Method::GET,
        "/_flush?wait_if_ongoing=false",
        Body::empty(),
    )
    .await;
    release_tx.send(()).expect("release held lock");
    holder.join().expect("lock holder");

    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["type"], "flush_in_progress_exception",
        "{body}"
    );
    assert_eq!(state.snapshot.load().metrics().memtable_entries, 1);
}

#[cfg(unix)]
#[tokio::test]
async fn durable_failure_is_a_failed_shard_and_never_acknowledged() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("rr-flush-api-{}", uuid::Uuid::new_v4()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(Normalizer::default_vocab().expect("vocab"), config);
    engine.try_insert_live("1994 topps", 7, 1).expect("insert");
    let state = state_with_engine(engine);

    let segments = dir.join("segments");
    let original = std::fs::metadata(&segments)
        .expect("segments")
        .permissions();
    std::fs::set_permissions(&segments, std::fs::Permissions::from_mode(0o555))
        .expect("make segments read-only");
    let (status, _, body) = send(&state, Method::POST, "/_flush", Body::empty()).await;
    std::fs::set_permissions(&segments, original).expect("restore permissions");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["acknowledged"], false);
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 1, "successful": 0, "failed": 1})
    );
    assert!(
        state.snapshot.load().has_live_query(7),
        "the in-memory fallback remains readable"
    );

    drop(state);
    let _ = std::fs::remove_dir_all(dir);
}
