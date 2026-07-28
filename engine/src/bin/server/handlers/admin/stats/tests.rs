use super::{cat_stats, stats};
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use arc_swap::ArcSwap;
use axum::body::{Body, Bytes};
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

fn state_with_tombstone() -> Arc<AppState> {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine
        .try_insert_live("1994 topps", 7, 1)
        .expect("first insert");
    engine
        .try_insert_live("1986 fleer", 8, 1)
        .expect("second insert");
    assert_eq!(
        engine.delete_by_logical_id(8).expect("delete"),
        1,
        "the stats fixture must retain one tombstoned row"
    );
    state_with_engine(engine)
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_stats",
            any(stats).layer(DefaultBodyLimit::max(body_limit)),
        )
        .route(
            "/_cat/stats",
            any(cat_stats).layer(DefaultBodyLimit::max(body_limit)),
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

async fn send(
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_is_truthful_familiar_and_uncacheable() {
    let state = state_with_tombstone();
    let (status, headers, body) = send(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_stats",
        Body::empty(),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        headers.get(header::CACHE_CONTROL),
        Some(&"no-store".parse().unwrap())
    );
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_f64(), "{body}");
    assert_eq!(
        body["_shards"],
        serde_json::json!({"total": 1, "successful": 1, "failed": 0})
    );
    assert_eq!(body["mode"], "standalone");
    assert_eq!(body["total_queries"], 2);
    assert_eq!(body["live_queries"], 1);
    assert_eq!(body["tombstoned_queries"], 1);
    assert_eq!(body["translog"]["operations"], 0);
    assert_eq!(body["translog"]["size_in_bytes"], 0);

    let memory = body["memory"].as_object().expect("memory object");
    for field in [
        "exact_bytes",
        "index_bytes",
        "filter_bytes",
        "dict_bytes",
        "query_store_bytes",
        "logical_index_bytes",
        "alive_bytes",
        "total_resident_bytes",
    ] {
        assert!(memory[field].is_u64(), "missing memory.{field}: {body}");
    }
    let components: u64 = [
        "exact_bytes",
        "index_bytes",
        "filter_bytes",
        "dict_bytes",
        "query_store_bytes",
        "logical_index_bytes",
        "alive_bytes",
    ]
    .into_iter()
    .map(|field| memory[field].as_u64().expect("component"))
    .sum();
    assert_eq!(memory["total_resident_bytes"], components);
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["stats", "200"])
            .get(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_transport_rejects_query_body_method_and_oversize() {
    let state = state_with_tombstone();

    let (status, headers, body) = send(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_stats?level=shards",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");

    let (status, _, body) = send(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_stats",
        "not empty",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, headers, body) = send(
        &state,
        super::STATS_BODY_LIMIT,
        Method::POST,
        "/_stats",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(body["error"]["type"], "method_not_allowed");
    assert_eq!(headers.get(header::ALLOW).unwrap(), "GET");

    let (status, _, body) = send(&state, 4, Method::GET, "/_stats", "12345").await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["error"]["type"], "payload_too_large");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_projects_the_durable_wal_as_translog() {
    let root = std::env::temp_dir().join(format!("rr-stats-wal-{}", uuid::Uuid::new_v4()));
    let config = EngineConfig {
        data_dir: Some(root.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(Normalizer::default_vocab().expect("normalizer"), config);
    engine
        .try_insert_live("1994 topps", 7, 1)
        .expect("durable insert");
    let state = state_with_engine(engine);

    let (status, _, body) = send(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_stats",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["translog"]["operations"], 1, "{body}");
    assert!(
        body["translog"]["size_in_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0),
        "{body}"
    );

    drop(state);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_for_stats_admission_is_async_and_cancellable() {
    let state = state_with_tombstone();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("hold stats slot");
    let app = router(&state, super::STATS_BODY_LIMIT);
    let request = Request::builder()
        .method(Method::GET)
        .uri("/_stats")
        .body(Body::empty())
        .expect("request");
    let pending = tokio::spawn(async move { app.oneshot(request).await });

    tokio::task::yield_now().await;
    assert!(!pending.is_finished(), "request should wait for admission");
    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        tokio::time::sleep(std::time::Duration::from_millis(1)),
    )
    .await
    .expect("the runtime must remain responsive");

    pending.abort();
    let _ = pending.await;
    drop(held);
    tokio::task::yield_now().await;
    assert_eq!(state.stats_permits.available_permits(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cat_stats_is_truthful_complete_and_uncacheable() {
    let state = state_with_tombstone();
    let (status, headers, bytes) = send_raw(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_cat/stats",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap(),
        "text/plain; charset=utf-8"
    );
    let text = std::str::from_utf8(&bytes).expect("UTF-8 CAT table");
    let rows: std::collections::HashMap<&str, &str> = text
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((fields.next()?, fields.next()?))
        })
        .collect();
    assert_eq!(rows["mode"], "standalone");
    assert_eq!(rows["queries.physical"], "2");
    assert_eq!(rows["queries.live"], "1");
    assert_eq!(rows["queries.tombstoned"], "1");
    assert!(rows["took_ms"].parse::<f64>().is_ok());
    assert_eq!(rows["translog.operations"], "0");
    assert_eq!(rows["translog.size_in_bytes"], "0");

    let memory_components: usize = [
        "memory.exact_bytes",
        "memory.index_bytes",
        "memory.filter_bytes",
        "memory.dict_bytes",
        "memory.query_store_bytes",
        "memory.logical_index_bytes",
        "memory.alive_bytes",
    ]
    .into_iter()
    .map(|field| rows[field].parse::<usize>().expect("memory bytes"))
    .sum();
    assert_eq!(
        rows["memory.total_resident_bytes"]
            .parse::<usize>()
            .expect("total resident bytes"),
        memory_components
    );
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cat_stats", "200"])
            .get(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cat_stats_honors_common_cat_controls() {
    let state = state_with_tombstone();

    let (status, _, bytes) = send_raw(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_cat/stats?v&h=m",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = std::str::from_utf8(&bytes).expect("UTF-8 CAT table");
    assert_eq!(text.lines().next(), Some("metric"));
    assert!(text.lines().any(|line| line == "queries.live"));
    assert!(
        text.lines().all(|line| !line.contains(' ')),
        "one selected column must render alone: {text}"
    );

    let (status, _, bytes) = send_raw(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_cat/stats?h=met*",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = std::str::from_utf8(&bytes).expect("UTF-8 CAT table");
    assert!(text.lines().any(|line| line == "queries.live"));
    assert!(
        text.lines().all(|line| !line.contains(' ')),
        "wildcard-selected metric column must render alone: {text}"
    );

    let (status, headers, body) = send(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_cat/stats?format=json&h=metric,value&s=metric:desc",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        headers.get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let rows = body.as_array().expect("JSON CAT rows");
    assert_eq!(rows[0]["metric"], "would_be_hot");
    assert!(rows.iter().all(|row| {
        row.get("metric").is_some()
            && row.get("value").is_some()
            && row.as_object().is_some_and(|object| object.len() == 2)
    }));

    let (status, _, bytes) = send_raw(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_cat/stats?format=json&h=value,metric&s=metric",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let json = std::str::from_utf8(&bytes).expect("UTF-8 JSON");
    assert!(
        json.starts_with(r#"[{"value":"10000","metric":"batch.max"}"#),
        "JSON must preserve the requested h order: {json}"
    );

    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("hold stats slot");
    let help = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        send_raw(
            &state,
            super::STATS_BODY_LIMIT,
            Method::GET,
            "/_cat/stats?help",
            Body::empty(),
        ),
    )
    .await
    .expect("help must not wait for stats admission");
    assert_eq!(help.0, StatusCode::OK);
    let help = std::str::from_utf8(&help.2).expect("UTF-8 help");
    assert!(
        help.starts_with("metric   | m        | native Reverse Rusty statistic name\n"),
        "shared rendering must preserve CAT stats help alignment: {help}"
    );
    assert!(help.contains("metric"));
    assert!(help.contains("statistic name"));
    drop(held);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cat_stats_transport_and_controls_fail_loud() {
    let state = state_with_tombstone();
    for uri in [
        "/_cat/stats?unknown=true",
        "/_cat/stats?format=yaml",
        "/_cat/stats?v=maybe",
        "/_cat/stats?h=no_such_column",
        "/_cat/stats?s=no_such_column",
        "/_cat/stats?help&h=metric",
        "/_cat/stats?help&v",
    ] {
        let (status, headers, body) = send(
            &state,
            super::STATS_BODY_LIMIT,
            Method::GET,
            uri,
            Body::empty(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        assert_eq!(body["error"]["type"], "validation_error", "{uri}: {body}");
        assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    }

    let (status, _, body) = send(
        &state,
        super::STATS_BODY_LIMIT,
        Method::GET,
        "/_cat/stats",
        "not empty",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, headers, body) = send(
        &state,
        super::STATS_BODY_LIMIT,
        Method::POST,
        "/_cat/stats",
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(headers.get(header::ALLOW).unwrap(), "GET");

    let (status, _, body) = send(&state, 4, Method::GET, "/_cat/stats", "12345").await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["error"]["type"], "payload_too_large");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cat_stats_shares_cancellable_stats_admission() {
    let state = state_with_tombstone();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("hold stats slot");
    let app = router(&state, super::STATS_BODY_LIMIT);
    let request = Request::builder()
        .method(Method::GET)
        .uri("/_cat/stats")
        .body(Body::empty())
        .expect("request");
    let pending = tokio::spawn(async move { app.oneshot(request).await });

    tokio::task::yield_now().await;
    assert!(
        !pending.is_finished(),
        "CAT request should wait for admission"
    );
    pending.abort();
    let _ = pending.await;
    drop(held);
    tokio::task::yield_now().await;
    assert_eq!(state.stats_permits.available_permits(), 1);
}
