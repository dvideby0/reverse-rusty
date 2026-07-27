use super::{backup_route, execute_backup_for_test};
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use arc_swap::ArcSwap;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::routing::any;
use axum::Router;
use parking_lot::Mutex;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;
use reverse_rusty::storage;
use reverse_rusty::Normalizer;
use std::path::{Path, PathBuf};
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

fn durable_state(root: &Path) -> Arc<AppState> {
    let config = EngineConfig {
        data_dir: Some(root.join("data")),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(Normalizer::default_vocab().expect("normalizer"), config);
    engine.build_from_queries(&[(7, "1994 topps".into())]);
    state_with_engine(engine)
}

fn temp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("rr-backup-route-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_backup",
            any(backup_route).layer(DefaultBodyLimit::max(body_limit)),
        )
        .with_state(Arc::clone(state))
}

async fn send(
    state: &Arc<AppState>,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
    content_type: bool,
) -> (StatusCode, HeaderMap, serde_json::Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if content_type {
        request = request.header(header::CONTENT_TYPE, "application/json");
    }
    let response = router(state, super::BACKUP_BODY_LIMIT)
        .oneshot(request.body(body.into()).expect("request"))
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
async fn durable_backup_waits_for_verified_commit_and_reports_timing() {
    let root = temp_root("success");
    let state = durable_state(&root);
    let dest = root.join("backup");
    let request = serde_json::json!({"dest": dest}).to_string();

    let (status, _, body) = send(&state, Method::POST, "/_backup", request, true).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["dest"], dest.to_string_lossy().as_ref());
    assert!(body.get("epoch").is_none(), "{body}");
    storage::verify_backup(&dest).expect("HTTP backup verifies");

    drop(state);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn strict_transport_rejects_invalid_requests_before_backup() {
    let root = temp_root("strict");
    let state = durable_state(&root);

    for (uri, raw_body) in [
        (
            "/_backup?routing=one",
            serde_json::json!({"dest": root.join("query")}).to_string(),
        ),
        (
            "/_backup",
            serde_json::json!({"dest": root.join("unknown"), "unknown": true}).to_string(),
        ),
        ("/_backup", r#"{"dest":"one","dest":"two"}"#.to_string()),
        ("/_backup", r#"{"dest":"   "}"#.to_string()),
        ("/_backup", r#"{"dest":"\u0000"}"#.to_string()),
        ("/_backup", "{".to_string()),
    ] {
        let (status, _, body) = send(&state, Method::POST, uri, raw_body, true).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "validation_error", "{body}");
    }

    let method_dest = root.join("method");
    let (status, headers, body) = send(
        &state,
        Method::PUT,
        "/_backup",
        serde_json::json!({"dest": method_dest}).to_string(),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    assert!(!method_dest.exists());

    let media_dest = root.join("media");
    let (status, _, body) = send(
        &state,
        Method::POST,
        "/_backup",
        serde_json::json!({"dest": media_dest}).to_string(),
        false,
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{body}");
    assert_eq!(body["error"]["type"], "unsupported_media_type", "{body}");
    assert!(!media_dest.exists());

    let response = router(&state, 16)
        .oneshot(
            Request::post("/_backup")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"dest": root.join("oversized")}).to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("oversize response body");
    let body: serde_json::Value =
        serde_json::from_slice(&bytes).expect("structured oversize response");
    assert_eq!(body["error"]["type"], "payload_too_large", "{body}");

    drop(state);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn storage_preconditions_have_stable_error_types() {
    let root = temp_root("preconditions");
    let in_memory = state_with_engine(Engine::new(
        Normalizer::default_vocab().expect("normalizer"),
    ));
    let (status, _, body) = send(
        &in_memory,
        Method::POST,
        "/_backup",
        serde_json::json!({"dest": root.join("in-memory")}).to_string(),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "not_durable", "{body}");

    let durable = durable_state(&root);
    let existing = root.join("existing");
    std::fs::create_dir(&existing).expect("existing destination");
    let (status, _, body) = send(
        &durable,
        Method::POST,
        "/_backup",
        serde_json::json!({"dest": existing}).to_string(),
        true,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "dest_exists", "{body}");

    drop(in_memory);
    drop(durable);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test]
async fn dropped_request_does_not_cancel_admitted_backup_or_block_timers() {
    let root = temp_root("dropped");
    let state = durable_state(&root);
    let dest = root.join("backup");
    let held_state = Arc::clone(&state);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _engine = held_state.engine.lock();
        locked_tx.send(()).expect("report held engine");
        release_rx.recv().expect("release held engine");
    });
    locked_rx.recv().expect("wait for held engine");

    let mut admitted = Box::pin(execute_backup_for_test(Arc::clone(&state), dest.clone()));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), admitted.as_mut())
            .await
            .is_err(),
        "the async timer must run while the blocking worker waits for the writer"
    );
    assert_eq!(
        state.backup_permits.available_permits(),
        0,
        "the detached blocking worker must own backup admission"
    );

    let queued_dest = root.join("queued-backup");
    let mut queued = Box::pin(execute_backup_for_test(
        Arc::clone(&state),
        queued_dest.clone(),
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), queued.as_mut())
            .await
            .is_err(),
        "a second backup waits asynchronously instead of entering the blocking pool"
    );
    drop(queued);
    assert!(
        !queued_dest.exists(),
        "cancelling a request still waiting for admission must not launch its backup"
    );

    drop(admitted);
    release_tx.send(()).expect("release engine");
    holder.join().expect("lock holder");

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !dest.exists() || state.backup_permits.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached admitted backup completes and releases admission");
    storage::verify_backup(&dest).expect("detached backup verifies");
    assert!(!queued_dest.exists());

    drop(state);
    std::fs::remove_dir_all(root).expect("cleanup");
}
