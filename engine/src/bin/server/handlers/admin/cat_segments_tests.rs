use super::{cat_segments, human_bytes, ByteUnit, CAT_SEGMENTS_BODY_LIMIT};

use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::{
    body::{Body, Bytes},
    extract::DefaultBodyLimit,
    http::{header, Method, Request, StatusCode},
    routing::any,
    Router,
};
use parking_lot::Mutex;
use reverse_rusty::{config::EngineConfig, segment::Engine, Normalizer};
use tower::ServiceExt;

use crate::{metrics::PrometheusMetrics, state::AppState};

fn state_with_segments() -> Arc<AppState> {
    let mut engine = Engine::with_config(
        Normalizer::default_vocab().expect("vocab"),
        EngineConfig {
            auto_compact_on_flush: false,
            ..EngineConfig::default()
        },
    );
    engine
        .try_insert_live("1994 acme", 7, 1)
        .expect("first base insert");
    engine
        .try_insert_live("1986 vertex", 8, 1)
        .expect("second base insert");
    engine.flush();
    assert_eq!(
        engine.delete_by_logical_id(8).expect("delete"),
        1,
        "the base fixture must retain one tombstoned row"
    );
    for id in 20..30 {
        engine
            .try_insert_live(&format!("entity item {id}"), id, 1)
            .expect("memtable insert");
    }

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

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_cat/segments",
            any(cat_segments).layer(DefaultBodyLimit::max(body_limit)),
        )
        .with_state(Arc::clone(state))
}

async fn send_raw(
    state: &Arc<AppState>,
    body_limit: usize,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
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

async fn send_json(
    state: &Arc<AppState>,
    body_limit: usize,
    method: Method,
    uri: &str,
    body: impl Into<Body>,
) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
    let (status, headers, bytes) = send_raw(state, body_limit, method, uri, body).await;
    let body = serde_json::from_slice(&bytes).expect("JSON response");
    (status, headers, body)
}

#[test]
fn byte_units_are_binary_and_stable() {
    assert_eq!(human_bytes(0), "0b");
    assert_eq!(human_bytes(512), "512b");
    assert_eq!(human_bytes(1024), "1kb");
    assert_eq!(human_bytes(1_572_864), "1.5mb");
    assert_eq!(ByteUnit::parse(Some("kb")).expect("kb").render(2_500), "2");
    assert_eq!(
        ByteUnit::parse(Some("k")).expect("k alias").render(2_500),
        "2"
    );
    assert_eq!(
        ByteUnit::parse(Some("b")).expect("bytes").render(2_500),
        "2500"
    );
}

#[tokio::test]
async fn default_table_is_headerless_truthful_and_uncacheable() {
    let state = state_with_segments();
    let (status, headers, bytes) = send_raw(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::GET,
        "/_cat/segments",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "text/plain; charset=utf-8"
    );

    let table = String::from_utf8(bytes.to_vec()).expect("UTF-8 table");
    let lines: Vec<&str> = table.lines().collect();
    assert_eq!(lines.len(), 2, "{table}");
    assert!(!lines[0].contains("segment"), "{table}");
    let base: Vec<&str> = lines[0].split_whitespace().collect();
    assert_eq!(
        &base[..6],
        ["0", "memory", "2", "1", "1", "50.00%"],
        "{table}"
    );
    assert!(lines[1].contains("memtable"), "{table}");
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cat_segments", "200"])
            .get(),
        1
    );
}

#[tokio::test]
async fn common_cat_controls_select_alias_sort_and_preserve_json_key_order() {
    let state = state_with_segments();
    let (status, _, bytes) = send_raw(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::GET,
        "/_cat/segments?v&h=segment,kind,docs.count,docs.deleted,holes.percent",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let table = String::from_utf8(bytes.to_vec()).expect("UTF-8 table");
    let header: Vec<&str> = table
        .lines()
        .next()
        .expect("header")
        .split_whitespace()
        .collect();
    assert_eq!(
        header,
        [
            "segment",
            "kind",
            "docs.count",
            "docs.deleted",
            "holes.percent"
        ]
    );

    let (status, _, body) = send_json(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::GET,
        "/_cat/segments?format=json&h=doc*&s=docs.deleted:desc",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body.as_array().expect("rows");
    assert_eq!(
        rows[0],
        serde_json::json!({"docs.count": "1", "docs.deleted": "1"})
    );
    assert_eq!(
        rows[1],
        serde_json::json!({"docs.count": "10", "docs.deleted": "0"})
    );

    let (status, _, bytes) = send_raw(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::GET,
        "/_cat/segments?format=json&h=docs.count,segment&s=entries:desc",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let raw = String::from_utf8(bytes.to_vec()).expect("JSON text");
    assert!(
        raw.starts_with("[{\"docs.count\":\"10\",\"segment\":\"1\"}"),
        "requested JSON key order and numeric sort must be preserved: {raw}"
    );
}

#[tokio::test]
async fn bytes_control_and_memory_split_are_exact() {
    let state = state_with_segments();
    let (status, _, body) = send_json(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::GET,
        "/_cat/segments?format=json&bytes=b&h=size.memory,memory.payload,memory.overhead",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for row in body.as_array().expect("rows") {
        let total = row["size.memory"]
            .as_str()
            .expect("total string")
            .parse::<u64>()
            .expect("total number");
        let payload = row["memory.payload"]
            .as_str()
            .expect("payload string")
            .parse::<u64>()
            .expect("payload number");
        let overhead = row["memory.overhead"]
            .as_str()
            .expect("overhead string")
            .parse::<u64>()
            .expect("overhead number");
        assert_eq!(total, payload + overhead);
    }
}

#[tokio::test]
async fn help_describes_schema_without_collecting_rows() {
    let state = state_with_segments();
    let (status, _, bytes) = send_raw(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::GET,
        "/_cat/segments?help",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let help = String::from_utf8(bytes.to_vec()).expect("UTF-8 help");
    assert!(help.contains("docs.count"), "{help}");
    assert!(help.contains("alive,dc"), "{help}");
    assert!(help.contains("size.memory"), "{help}");

    let (status, _, body) = send_json(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::GET,
        "/_cat/segments?help&format=json",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.as_array().expect("schema").len(), 11);
}

#[tokio::test]
async fn transport_and_controls_fail_loud() {
    let state = state_with_segments();
    for uri in [
        "/_cat/segments?unknown=true",
        "/_cat/segments?format=yaml",
        "/_cat/segments?v=maybe",
        "/_cat/segments?h=no_such_column",
        "/_cat/segments?s=no_such_column",
        "/_cat/segments?s=entries:sideways",
        "/_cat/segments?bytes=xb",
        "/_cat/segments?help&h=segment",
        "/_cat/segments?help&v",
        "/_cat/segments?help&bytes=b",
    ] {
        let (status, headers, body) = send_json(
            &state,
            CAT_SEGMENTS_BODY_LIMIT,
            Method::GET,
            uri,
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        assert_eq!(body["error"]["type"], "validation_error", "{uri}: {body}");
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
    }

    let (status, _, body) = send_json(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::GET,
        "/_cat/segments",
        "not empty",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, headers, body) = send_json(
        &state,
        CAT_SEGMENTS_BODY_LIMIT,
        Method::POST,
        "/_cat/segments",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(body["error"]["type"], "method_not_allowed");
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET");

    let (status, _, body) = send_json(&state, 4, Method::GET, "/_cat/segments", "12345").await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["error"]["type"], "payload_too_large");
}
