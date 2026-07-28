use super::import::{alias_import_method_not_allowed, import_aliases, ALIAS_IMPORT_BODY_LIMIT};

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
    engine.build_from_queries(&[(7, "package adapter".to_string())]);
    engine
}

fn memory_state() -> Arc<AppState> {
    state_with_engine(seeded_engine(EngineConfig::default()))
}

fn router(state: &Arc<AppState>, body_limit: usize) -> Router {
    Router::new()
        .route(
            "/_vocab/aliases/import",
            post(import_aliases)
                .layer(DefaultBodyLimit::max(body_limit))
                .fallback(alias_import_method_not_allowed::<AppState>),
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

fn decode(body: &Bytes) -> serde_json::Value {
    serde_json::from_slice(body).expect("JSON response")
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
    let decoded = decode(body);
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
async fn native_import_applies_synchronously_and_identical_reimport_is_noop() {
    let state = memory_state();
    let document = serde_json::json!({
        "synonyms": "package, pkg\nadapter, adaptor",
        "format": "solr",
        "expand": true
    })
    .to_string();

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/import?refresh=true",
        document.clone(),
        Some("application/vnd.reverse-rusty+json"),
        ALIAS_IMPORT_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache control"),
        "no-store"
    );
    let body = decode(&bytes);
    assert_eq!(body["acknowledged"], true, "{body}");
    assert_eq!(body["result"], "updated", "{body}");
    assert_eq!(body["rules"], 2, "{body}");
    assert_eq!(body["activated"], 2, "{body}");
    assert_eq!(body["recompiled"], 1, "{body}");
    assert!(body["rebuilt"].is_null(), "{body}");
    assert!(body["took"].is_u64(), "{body}");
    assert!(body["took_ms"].is_number(), "{body}");
    assert_matches(&state.engine.lock(), "pkg adaptor", 7);
    assert_eq!(
        state
            .snapshot
            .load()
            .vocab()
            .expect("published vocab")
            .alias_summary()
            .active,
        2
    );

    let published = state.snapshot.load_full();
    let (status, _, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/import",
        document,
        Some("application/json"),
        ALIAS_IMPORT_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body = decode(&bytes);
    assert_eq!(body["result"], "noop", "{body}");
    assert_eq!(body["activated"], 0, "{body}");
    assert_eq!(body["recompiled"], 0, "{body}");
    assert!(
        Arc::ptr_eq(&published, &state.snapshot.load_full()),
        "a no-op import must not republish an identical snapshot"
    );

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_import", "200"])
            .get(),
        2
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_import"])
            .get_sample_count(),
        2
    );
}

#[tokio::test]
async fn elasticsearch_synonyms_set_shape_is_accepted_without_faking_rule_ids() {
    let state = memory_state();
    let body = serde_json::json!({
        "synonyms_set": [
            {"id": "package-rule", "synonyms": "package, pkg"},
            {"id": "adapter-rule", "synonyms": "adapter, adaptor"}
        ]
    });
    let (status, _, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/import",
        body.to_string(),
        Some("application/json"),
        ALIAS_IMPORT_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let decoded = decode(&bytes);
    assert_eq!(decoded["result"], "updated", "{decoded}");
    assert_eq!(decoded["rules"], 2, "{decoded}");
    assert_matches(&state.engine.lock(), "pkg adaptor", 7);
    assert!(
        state
            .engine
            .lock()
            .aliases()
            .expect("registry")
            .entries()
            .iter()
            .all(|entry| entry.forms.len() == 2),
        "ES rule IDs are request metadata, not fabricated registry keys"
    );
}

#[tokio::test]
async fn transport_and_solr_rules_are_strict_and_bounded() {
    let state = memory_state();
    let valid = serde_json::json!({"synonyms": "package, pkg"}).to_string();

    let (status, headers, bytes) = send(
        &state,
        Method::GET,
        "/_vocab/aliases/import",
        Body::empty(),
        None,
        ALIAS_IMPORT_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "POST");
    assert_error(status, &headers, &bytes, "method_not_allowed");

    for uri in [
        "/_vocab/aliases/import?refresh=false",
        "/_vocab/aliases/import?unknown=true",
        "/_vocab/aliases/import?refresh=true&refresh=true",
    ] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            uri,
            valid.clone(),
            Some("application/json"),
            ALIAS_IMPORT_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_error(status, &headers, &bytes, "validation_error");
    }

    for content_type in [None, Some("text/plain")] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/import",
            valid.clone(),
            content_type,
            ALIAS_IMPORT_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_error(status, &headers, &bytes, "unsupported_media_type");
    }

    for invalid in [
        "{",
        r#"{"synonyms":"a =>"}"#,
        r#"{"synonyms":"a, a"}"#,
        r#"{"synonyms":"a, b","unknown":true}"#,
        r#"{"synonyms":"a, b","synonyms_set":{"synonyms":"c, d"}}"#,
        r#"{"synonyms":null}"#,
        r#"{"synonyms":"a, b","synonyms_set":null}"#,
        r#"{"synonyms":"a, b","format":null}"#,
        r#"{"synonyms":"a, b","expand":null}"#,
        r#"{"synonyms_set":[]}"#,
        r#"{"synonyms_set":{"id":null,"synonyms":"a, b"}}"#,
        r#"{"synonyms_set":{"synonyms":"a, b\nc, d"}}"#,
        r#"{"synonyms_set":[{"id":"same","synonyms":"a,b"},{"id":"same","synonyms":"c,d"}]}"#,
        r#"{"synonyms":"a, b","format":"wordnet"}"#,
        r#"{"synonyms":"a, b","expand":false}"#,
    ] {
        let (status, headers, bytes) = send(
            &state,
            Method::POST,
            "/_vocab/aliases/import",
            invalid,
            Some("application/json"),
            ALIAS_IMPORT_BODY_LIMIT,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
        assert_error(status, &headers, &bytes, "validation_error");
    }

    let oversized_raw_id = serde_json::json!({
        "synonyms_set": {
            "id": format!("{}x", " ".repeat(256)),
            "synonyms": "a, b"
        }
    })
    .to_string();
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/import",
        oversized_raw_id,
        Some("application/json"),
        ALIAS_IMPORT_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body = assert_error(status, &headers, &bytes, "validation_error");
    assert!(body["error"]["reason"]
        .as_str()
        .expect("reason")
        .contains("may not exceed 256 bytes"));

    let too_many_rules = serde_json::json!({
        "synonyms_set": (0..=reverse_rusty::vocab::MAX_ALIAS_IMPORT_RULES)
            .map(|index| serde_json::json!({
                "id": format!("rule-{index}"),
                "synonyms": "a, b"
            }))
            .collect::<Vec<_>>()
    })
    .to_string();
    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/import",
        too_many_rules,
        Some("application/json"),
        ALIAS_IMPORT_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body = assert_error(status, &headers, &bytes, "validation_error");
    assert!(body["error"]["reason"]
        .as_str()
        .expect("reason")
        .contains("at most 10000"));

    let (status, headers, bytes) = send(
        &state,
        Method::POST,
        "/_vocab/aliases/import",
        vec![b'x'; 129],
        Some("application/json"),
        128,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_error(status, &headers, &bytes, "payload_too_large");
}

#[tokio::test]
async fn stalled_body_has_a_request_deadline() {
    let state = memory_state();
    let pending = Body::from_stream(tokio_stream::pending::<Result<Bytes, Infallible>>());
    let (status, headers, bytes) = tokio::time::timeout(
        Duration::from_secs(6),
        send(
            &state,
            Method::POST,
            "/_vocab/aliases/import",
            pending,
            Some("application/json"),
            ALIAS_IMPORT_BODY_LIMIT,
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
    let request_body = serde_json::json!({"synonyms": "package, pkg"}).to_string();
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("admin permit");
    let request_state = Arc::clone(&state);
    let queued_body = request_body.clone();
    let mut request = tokio::spawn(async move {
        send(
            &request_state,
            Method::POST,
            "/_vocab/aliases/import",
            queued_body,
            Some("application/json"),
            ALIAS_IMPORT_BODY_LIMIT,
        )
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut request)
            .await
            .is_err(),
        "alias import must wait asynchronously for administrative admission"
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
            "/_vocab/aliases/import",
            request_body,
            Some("application/json"),
            ALIAS_IMPORT_BODY_LIMIT,
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
        "/_vocab/aliases/import",
        serde_json::json!({"synonyms": "adapter, adaptor"}).to_string(),
        Some("application/json"),
        ALIAS_IMPORT_BODY_LIMIT,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_error(status, &headers, &bytes, "aliases_unavailable");
}

fn temp_dir(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("rr-alias-import-{tag}-{}", uuid::Uuid::new_v4()));
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
async fn durable_commit_failure_is_live_published_and_not_acknowledged() {
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
        "/_vocab/aliases/import",
        serde_json::json!({"synonyms": "package, pkg"}).to_string(),
        Some("application/json"),
        ALIAS_IMPORT_BODY_LIMIT,
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
    assert_matches(&engine, "pkg adapter", 7);
    drop(engine);
    assert_eq!(
        state
            .snapshot
            .load()
            .vocab()
            .expect("published vocab")
            .alias_summary()
            .active,
        1
    );

    drop(state);
    std::fs::remove_dir_all(root).expect("remove temp root");
}
