use super::{apply_settings_patch, put_settings, SETTINGS_WRITE_BODY_LIMIT};

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use axum::{
    body::{Body, Bytes},
    extract::DefaultBodyLimit,
    http::{header, Method, Request, StatusCode},
    routing::put,
    Router,
};
use parking_lot::Mutex;
use reverse_rusty::{config::EngineConfig, segment::Engine, Normalizer};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn test_state() -> (Arc<AppState>, EngineConfig) {
    let config = EngineConfig {
        max_segments: 17,
        broad_batch_size: 384,
        ..EngineConfig::default()
    };
    let engine = Engine::with_config(
        Normalizer::default_vocab().expect("normalizer"),
        config.clone(),
    );
    let snapshot = Arc::new(engine.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .expect("pool");
    let prom = PrometheusMetrics::new();
    (
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
        }),
        config,
    )
}

fn router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/_settings",
            put(put_settings).layer(DefaultBodyLimit::max(SETTINGS_WRITE_BODY_LIMIT)),
        )
        .with_state(Arc::clone(state))
}

fn request(
    method: Method,
    uri: &str,
    content_type: Option<&str>,
    body: impl Into<Body>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    builder.body(body.into()).expect("request")
}

async fn send(
    state: &Arc<AppState>,
    request: Request<Body>,
) -> (StatusCode, axum::http::HeaderMap, Bytes) {
    let response = router(state)
        .oneshot(request)
        .await
        .expect("router response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (status, headers, bytes)
}

fn json_request(uri: &str, body: impl Into<Body>) -> Request<Body> {
    request(Method::PUT, uri, Some("application/json"), body)
}

#[tokio::test]
async fn update_is_live_coherent_uncacheable_and_observed() {
    let (state, original) = test_state();
    let (status, headers, bytes) = send(
        &state,
        request(
            Method::PUT,
            "/_settings?flat_settings=true&timeout=0",
            Some("application/vnd.reverse-rusty+json; charset=utf-8"),
            r#"{"max_segments":23,"holes_ratio_threshold":0.4}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "application/json"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("settings JSON");
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["persistent"], false, "{body}");
    assert_eq!(body["settings"]["max_segments"], 23, "{body}");
    assert_eq!(body["settings"]["holes_ratio_threshold"], 0.4, "{body}");
    assert_eq!(
        body["settings"]["broad_batch_size"], original.broad_batch_size,
        "{body}"
    );

    let snapshot = state.snapshot.load_full();
    assert_eq!(snapshot.config().max_segments, 23);
    assert!((snapshot.config().holes_ratio_threshold - 0.4).abs() < f64::EPSILON);
    assert_eq!(state.engine.lock().config().max_segments, 23);
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["settings_put", "200"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["settings_put"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn query_media_json_semantics_size_and_body_deadline_are_strict() {
    let (state, original) = test_state();
    for query in [
        "unknown=true",
        "flat_settings=true&flat_settings=false",
        "flat_settings=maybe",
        "timeout=1s&timeout=2s",
        "timeout=soon",
        "timeout=31s",
        "master_timeout=1s",
    ] {
        let (status, headers, body) = send(
            &state,
            json_request(&format!("/_settings?{query}"), r#"{"max_segments":18}"#),
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

    for content_type in [None, Some("text/plain")] {
        let (status, headers, body) = send(
            &state,
            request(
                Method::PUT,
                "/_settings",
                content_type,
                r#"{"max_segments":18}"#,
            ),
        )
        .await;
        assert_error(
            status,
            &headers,
            &body,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
        );
    }

    for (raw, kind) in [
        ("{", "validation_error"),
        ("[]", "validation_error"),
        ("{}", "settings_error"),
        (
            r#"{"max_segments":18,"max_segments":19}"#,
            "validation_error",
        ),
        (r#"{"max_segments":"many"}"#, "settings_error"),
        (r#"{"max_segments":0}"#, "settings_error"),
        (r#"{"bogus":1}"#, "settings_error"),
        (r#"{"retention_lease_ttl_secs":30}"#, "settings_error"),
        (r#"{"settings":{"max_segments":18}}"#, "settings_error"),
        (r#"{"transient":{"max_segments":18}}"#, "settings_error"),
        (r#"{"persistent":{"max_segments":18}}"#, "settings_error"),
    ] {
        let (status, headers, body) = send(&state, json_request("/_settings", raw)).await;
        assert_error(status, &headers, &body, StatusCode::BAD_REQUEST, kind);
    }

    let oversized = vec![b'x'; SETTINGS_WRITE_BODY_LIMIT + 1];
    let (status, headers, body) = send(&state, json_request("/_settings", oversized)).await;
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    );

    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, headers, body) = tokio::time::timeout(
        Duration::from_secs(6),
        send(&state, json_request("/_settings", pending)),
    )
    .await
    .expect("fixed body deadline");
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );

    assert_eq!(
        state.snapshot.load().config().max_segments,
        original.max_segments,
        "every rejected request must leave the live view untouched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn admission_and_engine_lock_waits_are_bounded_off_runtime() {
    let (state, original) = test_state();

    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let started = Instant::now();
    let (status, headers, body) = send(
        &state,
        json_request("/_settings?timeout=25ms", r#"{"max_segments":18}"#),
    )
    .await;
    assert!(started.elapsed() < Duration::from_millis(150));
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
    drop(held);

    let lock_state = Arc::clone(&state);
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let blocker = std::thread::spawn(move || {
        let _engine = lock_state.engine.lock();
        locked_tx.send(()).expect("announce held lock");
        std::thread::sleep(Duration::from_millis(250));
    });
    locked_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("engine lock held");
    let started = Instant::now();
    let (status, headers, body) = send(
        &state,
        json_request("/_settings?timeout=25ms", r#"{"max_segments":19}"#),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(150),
        "engine-lock waiting escaped spawn_blocking and stalled Tokio"
    );
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::REQUEST_TIMEOUT,
        "request_timeout",
    );
    blocker.join().expect("lock holder");
    assert_eq!(
        state.snapshot.load().config().max_segments,
        original.max_segments
    );

    let (status, _, body) = send(
        &state,
        json_request("/_settings?timeout=1s", r#"{"max_segments":20}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(state.snapshot.load().config().max_segments, 20);

    state.stats_permits.close();
    let (status, headers, body) =
        send(&state, json_request("/_settings", r#"{"max_segments":21}"#)).await;
    assert_error(
        status,
        &headers,
        &body,
        StatusCode::SERVICE_UNAVAILABLE,
        "settings_unavailable",
    );
    assert_eq!(state.snapshot.load().config().max_segments, 20);
}

#[test]
fn patch_policy_is_complete_atomic_and_range_checked() {
    let dynamic = patch(
        r#"{
            "max_segments":16,
            "auto_compact_on_flush":false,
            "holes_ratio_threshold":0.5,
            "broad_batch_size":512,
            "broad_columnar":false,
            "broad_materialize":false,
            "broad_prefilter":false,
            "dedup_bodies":false,
            "max_percolate_batch":50000,
            "compaction_reanchor":true,
            "alias_feedback_capture":true,
            "alias_feedback_max_pairs":64
        }"#,
    );
    let cfg = apply_settings_patch(EngineConfig::default(), &dynamic).expect("dynamic patch");
    assert_eq!(cfg.max_segments, 16);
    assert!(!cfg.auto_compact_on_flush);
    assert!((cfg.holes_ratio_threshold - 0.5).abs() < f64::EPSILON);
    assert_eq!(cfg.broad_batch_size, 512);
    assert!(!cfg.broad_columnar);
    assert!(!cfg.broad_materialize);
    assert!(!cfg.broad_prefilter);
    assert!(!cfg.dedup_bodies);
    assert_eq!(cfg.max_percolate_batch, 50_000);
    assert!(cfg.compaction_reanchor);
    assert!(cfg.alias_feedback_capture);
    assert_eq!(cfg.alias_feedback_max_pairs, 64);

    for raw in [
        r#"{"broad_batch_size":0}"#,
        r#"{"wal_sync_on_write":true}"#,
        r#"{"retention_lease_ttl_secs":30}"#,
        r#"{"bogus":1}"#,
        r#"{"max_segments":"lots"}"#,
        r#"{"max_segments":0,"holes_ratio_threshold":2.0}"#,
        r#"{"max_segments":12,"data_dir":"/tmp/x"}"#,
    ] {
        assert!(
            apply_settings_patch(EngineConfig::default(), &patch(raw)).is_err(),
            "{raw} must be rejected"
        );
    }
}

fn patch(raw: &str) -> serde_json::Map<String, serde_json::Value> {
    serde_json::from_str(raw).expect("test patch")
}

fn assert_error(
    status: StatusCode,
    headers: &axum::http::HeaderMap,
    body: &Bytes,
    expected: StatusCode,
    kind: &str,
) {
    assert_eq!(status, expected);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    let body: serde_json::Value = serde_json::from_slice(body).expect("JSON error");
    assert_eq!(body["error"]["type"], kind, "{body}");
    assert_eq!(body["status"], expected.as_u16(), "{body}");
}
