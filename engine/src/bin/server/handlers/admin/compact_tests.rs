use super::compact::execute_native_for_test;
use super::{compact_route, force_merge_route};
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

fn engine_with_two_segments() -> Engine {
    let config = EngineConfig {
        max_segments: 8,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(Normalizer::default_vocab().expect("vocab"), config);
    engine.build_from_queries(&[(1, "1994 topps".into())]);
    engine.bulk_ingest(&[(2, "1986 fleer".into())]);
    assert_eq!(engine.metrics().base_segments, 2);
    engine
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route("/_compact", any(compact_route))
        .route("/_forcemerge", any(force_merge_route))
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
async fn native_compact_really_forces_all_segments_below_policy_threshold() {
    let state = state_with_engine(engine_with_two_segments());
    let (status, _, body) = send(&state, Method::POST, "/_compact", Body::empty()).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 1, "successful": 1, "failed": 0})
    );
    assert_eq!(body["segments_merged"], 2);
    assert_eq!(body["entries_before"], 2);
    assert_eq!(body["entries_after"], 2);
    assert_eq!(body["tombstones_reclaimed"], 0);
    assert_eq!(state.snapshot.load().metrics().base_segments, 1);

    let (status, _, body) = send(&state, Method::POST, "/_compact", Body::empty()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);
    assert_eq!(body["message"], "nothing to compact");
}

#[tokio::test]
async fn force_merge_default_uses_policy_and_max_one_forces_all() {
    let policy_state = state_with_engine(engine_with_two_segments());
    let (status, _, body) = send(&policy_state, Method::POST, "/_forcemerge", Body::empty()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["message"], "no segment merge needed");
    assert_eq!(
        policy_state.snapshot.load().metrics().base_segments,
        2,
        "the default follows the configured merge policy"
    );

    let force_state = state_with_engine(engine_with_two_segments());
    let (status, _, body) = send(
        &force_state,
        Method::POST,
        "/_forcemerge?max_num_segments=1",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["segments_merged"], 2);
    assert_eq!(body["_shards"]["successful"], 1);
    assert_eq!(force_state.snapshot.load().metrics().base_segments, 1);
}

#[tokio::test]
async fn force_merge_honors_the_familiar_flush_control() {
    let make_state = || {
        let config = EngineConfig {
            auto_compact_on_flush: false,
            auto_compact_on_ingest: false,
            ..EngineConfig::default()
        };
        let mut engine = Engine::with_config(Normalizer::default_vocab().expect("vocab"), config);
        engine.build_from_queries(&[(1, "1994 topps".into())]);
        engine
            .try_insert_live("1986 fleer", 2, 1)
            .expect("memtable insert");
        state_with_engine(engine)
    };

    let default_state = make_state();
    let (status, _, body) = send(&default_state, Method::POST, "/_forcemerge", Body::empty()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(default_state.snapshot.load().metrics().memtable_entries, 0);
    assert_eq!(default_state.snapshot.load().metrics().base_segments, 2);

    let no_flush_state = make_state();
    let (status, _, body) = send(
        &no_flush_state,
        Method::POST,
        "/_forcemerge?flush=false",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(no_flush_state.snapshot.load().metrics().memtable_entries, 1);
    assert_eq!(no_flush_state.snapshot.load().metrics().base_segments, 1);

    let target_state = make_state();
    let (status, _, body) = send(
        &target_state,
        Method::POST,
        "/_forcemerge?max_num_segments=1",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["segments_merged"], 2);
    assert_eq!(target_state.snapshot.load().metrics().memtable_entries, 0);
    assert_eq!(
        target_state.snapshot.load().metrics().base_segments,
        1,
        "the target includes the delta sealed by the default flush"
    );
}

#[tokio::test]
async fn dropping_the_request_does_not_cancel_or_hide_admitted_work() {
    let state = state_with_engine(engine_with_two_segments());
    let held_state = Arc::clone(&state);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _engine = held_state.engine.lock();
        locked_tx.send(()).expect("report held engine");
        release_rx.recv().expect("release held engine");
    });
    locked_rx.recv().expect("wait for held engine");

    let mut admitted = Box::pin(execute_native_for_test(Arc::clone(&state)));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), admitted.as_mut())
            .await
            .is_err(),
        "the worker should be admitted and waiting for the held writer lock"
    );
    drop(admitted);
    release_tx.send(()).expect("release engine");
    holder.join().expect("lock holder");

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while state.snapshot.load().metrics().base_segments != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached maintenance publishes its completed snapshot");
}

#[tokio::test]
async fn controls_body_and_methods_are_strict_before_mutation() {
    let state = state_with_engine(engine_with_two_segments());
    for uri in [
        "/_compact?max_num_segments=1",
        "/_forcemerge?routing=one",
        "/_forcemerge?max_num_segments=0",
        "/_forcemerge?max_num_segments=2",
        "/_forcemerge?only_expunge_deletes=true",
        "/_forcemerge?wait_for_completion=false",
        "/_forcemerge?flush=true&flush=false",
    ] {
        let (status, _, body) = send(&state, Method::POST, uri, Body::empty()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        assert_eq!(state.snapshot.load().metrics().base_segments, 2);
    }

    for uri in ["/_compact", "/_forcemerge"] {
        let (status, _, body) = send(&state, Method::POST, uri, "{}").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        assert_eq!(body["error"]["type"], "validation_error", "{body}");
        assert_eq!(state.snapshot.load().metrics().base_segments, 2);

        let (status, headers, body) = send(&state, Method::GET, uri, Body::empty()).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{uri}: {body}");
        assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
        assert_eq!(state.snapshot.load().metrics().base_segments, 2);
    }

    let response = router(&state, 4)
        .oneshot(
            Request::post("/_compact")
                .body(Body::from("12345"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(state.snapshot.load().metrics().base_segments, 2);
}

#[cfg(unix)]
#[tokio::test]
async fn durable_failure_rolls_back_and_is_a_failed_shard() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("rr-compact-api-{}", uuid::Uuid::new_v4()));
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(Normalizer::default_vocab().expect("vocab"), config);
    engine.build_from_queries(&[(1, "1994 topps".into())]);
    engine.bulk_ingest(&[(2, "1986 fleer".into())]);
    let state = state_with_engine(engine);

    let segments = dir.join("segments");
    let original = std::fs::metadata(&segments)
        .expect("segments")
        .permissions();
    std::fs::set_permissions(&segments, std::fs::Permissions::from_mode(0o555))
        .expect("make segments read-only");
    let (status, _, body) = send(&state, Method::POST, "/_compact", Body::empty()).await;
    std::fs::set_permissions(&segments, original).expect("restore permissions");

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["acknowledged"], false);
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 1, "successful": 0, "failed": 1})
    );
    assert_eq!(
        state.snapshot.load().metrics().base_segments,
        2,
        "failed compaction keeps both source segments"
    );
    assert!(state.snapshot.load().has_live_query(1));
    assert!(state.snapshot.load().has_live_query(2));

    drop(state);
    let _ = std::fs::remove_dir_all(dir);
}
