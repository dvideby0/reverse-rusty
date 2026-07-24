//! Handler-level tests for the `PUT /_doc/{id}` atomic upsert (ADR-067): the ES
//! 201-created / 200-updated status split, replace-by-id visible through the
//! published snapshot, and DELETE reporting one live copy after a re-PUT (the
//! ADR-064 audit observed `deleted_count: 2` on the pre-fix additive path).

use super::{delete_doc, get_doc, put_doc};
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Json;
use axum::Router;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::Normalizer;
use std::sync::Arc;
use tower::ServiceExt;

fn state() -> Arc<AppState> {
    let eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    state_with_engine(eng)
}

fn state_with_engine(eng: Engine) -> Arc<AppState> {
    let snap = Arc::new(eng.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    let prom = PrometheusMetrics::new();
    Arc::new(AppState {
        engine: parking_lot::Mutex::new(eng),
        snapshot: arc_swap::ArcSwap::new(snap),
        pool,
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(2)),
        exhaustive_jobs: crate::jobs::ExhaustiveJobs::for_tests(prom.clone()),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad: true,
        prom,
        slow_query_threshold_ms: 0,
        auth: None,
        feedback: parking_lot::Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
        pit_tokens: crate::pit::PitTokens::generate(),
        pits: parking_lot::Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config: reverse_rusty::PitConfig::default(),
    })
}

fn put_body(query: &str) -> super::PutDocBody {
    serde_json::from_value(serde_json::json!({ "query": query })).expect("valid body")
}

/// Run `put_doc` and return (status, parsed JSON body).
async fn do_put(state: &Arc<AppState>, id: u64, query: &str) -> (StatusCode, serde_json::Value) {
    let resp = put_doc(
        State(Arc::clone(state)),
        Path(id),
        Ok(Query(super::PutDocParams::default())),
        Json(put_body(query)),
    )
    .await
    .into_response();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    (status, json)
}

fn matches_in_snapshot(state: &Arc<AppState>, title: &str) -> Vec<u64> {
    let snap = state.snapshot.load();
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    snap.match_title(title, &mut s, &mut out, true);
    out.sort_unstable();
    out
}

fn doc_router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/_doc/{id}", get(get_doc).put(put_doc).delete(delete_doc))
        .with_state(Arc::clone(state))
}

async fn route_doc(
    state: &Arc<AppState>,
    request: Request<Body>,
) -> (StatusCode, axum::body::Bytes) {
    let response = doc_router(state)
        .oneshot(request)
        .await
        .expect("router response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (status, body)
}

fn put_request(path: &str, body: &serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("PUT request")
}

async fn route_put_json(
    state: &Arc<AppState>,
    path: &str,
    body: &serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let (status, bytes) = route_doc(state, put_request(path, body)).await;
    let json = serde_json::from_slice(&bytes).expect("JSON response");
    (status, json)
}

#[tokio::test]
async fn get_doc_is_es_shaped_filterable_and_head_aware() {
    let state = state();
    let put = Request::builder()
        .method("PUT")
        .uri("/_doc/7")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "query": "topps chrome",
                "version": 42,
                "tags": {
                    "tenant": "acme",
                    "colors": ["red", "blue"],
                    "active": true,
                    "é": "accent"
                }
            })
            .to_string(),
        ))
        .expect("PUT request");
    assert_eq!(route_doc(&state, put).await.0, StatusCode::CREATED);

    let get = Request::builder()
        .uri("/_doc/7")
        .body(Body::empty())
        .expect("GET request");
    let (status, bytes) = route_doc(&state, get).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("GET json");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_id"], 7);
    assert_eq!(body["_version"], 42);
    assert_eq!(body["found"], true);
    assert_eq!(body["_source"]["query"], "topps chrome");
    assert_eq!(body["_source"]["tags"]["tenant"], "acme");
    assert_eq!(body["_source"]["tags"]["active"], "true");
    assert_eq!(body["_source"]["tags"]["é"], "accent");
    assert_eq!(
        body["_source"]["tags"]["colors"],
        serde_json::json!(["blue", "red"])
    );

    let filtered = Request::builder()
        .uri("/_doc/7?_source_includes=tags.colors")
        .body(Body::empty())
        .expect("filtered GET");
    let (status, bytes) = route_doc(&state, filtered).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("filtered json");
    assert!(body["_source"].get("query").is_none());
    assert_eq!(
        body["_source"]["tags"],
        serde_json::json!({"colors": ["blue", "red"]})
    );

    let unicode_question = Request::builder()
        .uri("/_doc/7?_source_includes=tags.%3F")
        .body(Body::empty())
        .expect("Unicode wildcard GET");
    let (status, bytes) = route_doc(&state, unicode_question).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("Unicode wildcard json");
    assert_eq!(
        body["_source"],
        serde_json::json!({"tags": {"é": "accent"}}),
        "`?` must consume one Unicode character, not one UTF-8 byte"
    );

    let excluded = Request::builder()
        .uri("/_doc/7?_source_excludes=tags.col*")
        .body(Body::empty())
        .expect("excluded GET");
    let (status, bytes) = route_doc(&state, excluded).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("excluded json");
    assert_eq!(body["_source"]["query"], "topps chrome");
    assert!(body["_source"]["tags"].get("colors").is_none());
    assert_eq!(body["_source"]["tags"]["tenant"], "acme");

    let source_disabled = Request::builder()
        .uri("/_doc/7?_source=false")
        .body(Body::empty())
        .expect("source-disabled GET");
    let (status, bytes) = route_doc(&state, source_disabled).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("source-disabled json");
    assert!(body.get("_source").is_none());
    assert_eq!(body["_version"], 42);

    for (path, expected) in [
        ("/_doc/7", StatusCode::OK),
        ("/_doc/8", StatusCode::NOT_FOUND),
    ] {
        let head = Request::builder()
            .method("HEAD")
            .uri(path)
            .body(Body::empty())
            .expect("HEAD request");
        let (status, bytes) = route_doc(&state, head).await;
        assert_eq!(status, expected);
        assert!(bytes.is_empty(), "HEAD response must be bodyless");
    }

    let missing = Request::builder()
        .uri("/_doc/8")
        .body(Body::empty())
        .expect("missing GET");
    let (status, bytes) = route_doc(&state, missing).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("missing json");
    assert_eq!(
        body,
        serde_json::json!({"_index": "queries", "_id": 8, "found": false})
    );

    let unsupported = Request::builder()
        .uri("/_doc/7?preference=local")
        .body(Body::empty())
        .expect("unsupported query parameter");
    assert_eq!(
        route_doc(&state, unsupported).await.0,
        StatusCode::BAD_REQUEST,
        "unsupported ES parameters must fail instead of being silently ignored"
    );
}

#[tokio::test]
async fn get_doc_does_not_report_a_live_row_as_missing_when_its_source_is_unavailable() {
    use reverse_rusty::config::EngineConfig;

    let dir = std::env::temp_dir().join(format!(
        "reverse-rusty-get-doc-source-guard-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        retain_source: false,
        ..EngineConfig::default()
    };
    {
        let mut engine =
            Engine::with_config(Normalizer::default_vocab().expect("vocab"), config.clone());
        engine
            .try_insert_live("topps chrome", 7, 1)
            .expect("insert");
        engine.flush();
    }
    std::fs::remove_file(dir.join("sources.dat")).expect("remove source store");
    let engine = Engine::open(Normalizer::default_vocab().expect("vocab"), config).expect("reopen");
    assert!(engine.snapshot().has_live_query(7));
    let state = state_with_engine(engine);

    let request = Request::builder()
        .uri("/_doc/7")
        .body(Body::empty())
        .expect("GET request");
    let (status, bytes) = route_doc(&state, request).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("error json");
    assert_eq!(body["error"]["type"], "source_unavailable");

    let head = Request::builder()
        .method("HEAD")
        .uri("/_doc/7")
        .body(Body::empty())
        .expect("HEAD request");
    let (status, bytes) = route_doc(&state, head).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "HEAD existence comes from the live exact index, not sources.dat"
    );
    assert!(bytes.is_empty(), "HEAD response must be bodyless");

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn put_doc_is_created_then_updated_with_replace_semantics() {
    let state = state();

    // First PUT: 201 created.
    let (status, body) = do_put(&state, 7, "michael jordan").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_id"], 7);
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "created");
    assert!(body.get("error").is_none());
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7));

    // Re-PUT with different semantics: 200 updated, and the snapshot flips
    // atomically — the old version stops matching exactly when the new starts
    // (one lock, one publish; no matches-under-either-version window).
    let (status, body) = do_put(&state, 7, "lebron james").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "updated");
    assert!(
        !matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7),
        "old semantics must stop matching after the re-PUT"
    );
    assert!(matches_in_snapshot(&state, "2003 topps lebron james rookie").contains(&7));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_doc_create_only_is_atomic_and_never_overwrites() {
    let state = state();
    let first_body = serde_json::json!({"query":"michael jordan","version":7});
    let second_body = serde_json::json!({"query":"lebron james","version":8});
    let first = route_put_json(&state, "/_doc/7?op_type=create", &first_body);
    let second = route_put_json(&state, "/_doc/7?op_type=create", &second_body);
    let (a, b) = tokio::join!(first, second);
    let mut statuses = [a.0, b.0];
    statuses.sort_by_key(StatusCode::as_u16);
    assert_eq!(statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);

    let (created, conflict) = if a.0 == StatusCode::CREATED {
        (a.1, b.1)
    } else {
        (b.1, a.1)
    };
    assert_eq!(created["_index"], "queries");
    assert!(
        created["_version"] == 7 || created["_version"] == 8,
        "the winning caller's display version is returned"
    );
    assert_eq!(
        conflict["error"]["type"],
        "version_conflict_engine_exception"
    );

    let jordan = matches_in_snapshot(&state, "1986 fleer michael jordan rookie");
    let lebron = matches_in_snapshot(&state, "2003 topps lebron james rookie");
    assert_ne!(
        jordan.contains(&7),
        lebron.contains(&7),
        "exactly one create-only body must become live"
    );

    let (status, after) = route_put_json(
        &state,
        "/_doc/7?op_type=create",
        &serde_json::json!({"query":"wayne gretzky","version":9}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(after["error"]["type"], "version_conflict_engine_exception");
    assert!(
        !matches_in_snapshot(&state, "1979 opc wayne gretzky rookie").contains(&7),
        "a conflict must not replace the winning document"
    );
    let (status, malformed_conflict) = route_put_json(
        &state,
        "/_doc/7?op_type=create",
        &serde_json::json!({"query":"("}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        malformed_conflict["error"]["type"], "version_conflict_engine_exception",
        "an existing id is the decisive create-only error in both server modes"
    );
}

#[tokio::test]
async fn put_doc_validates_query_parameters_and_accepts_refresh_policies() {
    let state = state();
    for (id, refresh) in [(11, "false"), (12, "true"), (13, "wait_for")] {
        let (status, body) = route_put_json(
            &state,
            &format!("/_doc/{id}?refresh={refresh}&op_type=index"),
            &serde_json::json!({"query":format!("topps chrome {id}")}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert!(
            matches_in_snapshot(&state, &format!("topps chrome {id}")).contains(&id),
            "every accepted refresh policy has immediate visibility"
        );
    }

    for path in [
        "/_doc/20?refresh=immediate",
        "/_doc/21?op_type=overwrite",
        "/_doc/22?routing=custom",
    ] {
        let (status, body) =
            route_put_json(&state, path, &serde_json::json!({"query":"michael jordan"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
    }
    for id in [20, 21, 22] {
        assert!(
            !matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&id),
            "invalid query parameters must reject before mutation"
        );
    }
}

#[tokio::test]
async fn delete_after_reput_reports_one_copy() {
    let state = state();
    do_put(&state, 7, "michael jordan").await;
    do_put(&state, 7, "lebron james").await;

    let resp = delete_doc(State(Arc::clone(&state)), Path(7))
        .await
        .into_response();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(
        json["deleted_count"], 1,
        "replace-by-id leaves exactly one live copy (the audit observed 2)"
    );
}

// -- memtable_flush_threshold honored by REST PUT (ADR-073, ADR-064 item 5) --

#[tokio::test]
async fn put_doc_honors_memtable_flush_threshold() {
    // Pre-fix the REST PUT path bypassed the only `maybe_flush` call site, so
    // the knob was INERT for single-doc HTTP writes: memtable + WAL grew until
    // a manual /_flush. With threshold 2, the third PUT must have produced at
    // least one sealed segment — and every query must keep matching across the
    // flush boundary.
    use reverse_rusty::config::EngineConfig;
    let cfg = EngineConfig {
        memtable_flush_threshold: 2,
        ..EngineConfig::default()
    };
    let eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    let state = state_with_engine(eng);

    do_put(&state, 1, "michael jordan").await;
    do_put(&state, 2, "lebron james").await;
    do_put(&state, 3, "wayne gretzky").await;
    // A re-PUT (the upsert path) must honor the threshold too.
    do_put(&state, 2, "mario lemieux").await;

    assert!(
        state.engine.lock().num_segments() > 0,
        "threshold-2 PUTs must auto-flush the memtable into a segment"
    );
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&1));
    assert!(matches_in_snapshot(&state, "1985 opc mario lemieux rookie").contains(&2));
    assert!(matches_in_snapshot(&state, "1979 opc wayne gretzky rookie").contains(&3));
    assert!(
        !matches_in_snapshot(&state, "2003 topps lebron james rookie").contains(&2),
        "the upserted-away version must stay dead across the flush"
    );
}

// -- Tag-value coercion + loud rejects (ADR-073, closing ADR-064 item 4) ----

/// Shorthand: run `extract_ingest_tags` over a JSON body's top-level object.
fn tags_of(body: &serde_json::Value) -> Result<Vec<(String, String)>, String> {
    let obj = body.as_object().expect("test body is an object");
    super::extract_ingest_tags(obj)
}

#[test]
fn scalar_tag_values_coerce_canonically() {
    // Numbers and bools coerce to their canonical JSON text (the ES keyword
    // behavior); strings pass through. Both the `tags` object and ES-style
    // sibling fields take the same rule.
    let mut tags = tags_of(&serde_json::json!({
        "query": "q",
        "tags": {"priority": 7, "active": true, "tier": "gold"},
        "category": 42.5,
    }))
    .expect("scalars must coerce, not error");
    tags.sort();
    assert_eq!(
        tags,
        vec![
            ("active".to_string(), "true".to_string()),
            ("category".to_string(), "42.5".to_string()),
            ("priority".to_string(), "7".to_string()),
            ("tier".to_string(), "gold".to_string()),
        ]
    );
}

#[test]
fn null_tag_values_are_skipped_not_errors() {
    // An explicit null is the ES "no value" — the key carries no tag, top-level
    // or as an array element; `"tags": null` means no tags at all.
    let tags = tags_of(&serde_json::json!({
        "query": "q",
        "tags": {"status": null},
        "colors": ["red", null, 3],
    }))
    .expect("null is skip, not an error");
    assert_eq!(
        tags,
        vec![
            ("colors".to_string(), "red".to_string()),
            ("colors".to_string(), "3".to_string()),
        ]
    );
    assert_eq!(
        tags_of(&serde_json::json!({"query": "q", "tags": null})).expect("tags:null is no tags"),
        vec![]
    );
}

#[test]
fn empty_tag_keys_fail_loud() {
    // An empty KEY rejects (codex retro-review, ADR-075 family): an empty
    // `priority_key` means "no priority term" (the gRPC wire cannot express it),
    // so an empty-key tag would be reachable by SOME ranking paths and not others.
    // Both intake shapes — the `tags` object and an ES-style sibling field.
    let err = tags_of(&serde_json::json!({"query": "q", "tags": {"": "v"}}))
        .expect_err("an empty tag key in `tags` must reject");
    assert!(err.contains("non-empty"), "names the rule (got: {err})");
    assert!(
        tags_of(&serde_json::json!({"query": "q", "": "v"})).is_err(),
        "an empty sibling-field key must reject too"
    );
}

#[test]
fn typed_priority_is_strict_mirrored_and_conflict_checked() {
    let object = serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": "-50"},
        "tags": {"tenant": "acme"}
    });
    let (tags, rank) =
        super::extract_ranked_ingest(object.as_object().expect("object")).expect("typed priority");
    assert_eq!(rank, Some(reverse_rusty::RankValues { priority: -50 }));
    assert!(tags.contains(&("priority".to_string(), "-50".to_string())));

    let matching = serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": 50},
        "tags": {"priority": "50"}
    });
    assert!(super::extract_ranked_ingest(matching.as_object().expect("object")).is_ok());

    let conflict = serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": 50},
        "tags": {"priority": "49"}
    });
    let (kind, _) =
        super::extract_ranked_ingest(conflict.as_object().expect("object")).expect_err("conflict");
    assert_eq!(kind, "invalid_rank_value");
}

#[test]
fn typed_priority_rejects_non_integer_json_and_overflow() {
    for value in [
        serde_json::json!(1.5),
        serde_json::json!(true),
        serde_json::Value::Null,
        serde_json::json!([]),
        serde_json::json!({}),
        serde_json::json!("9223372036854775808"),
    ] {
        let object = serde_json::json!({
            "query": "topps chrome",
            "rank_fields": {"priority": value}
        });
        let (kind, _) = super::extract_ranked_ingest(object.as_object().expect("object"))
            .expect_err("invalid typed rank");
        assert_eq!(kind, "invalid_rank_value");
    }
}

#[tokio::test]
async fn put_doc_typed_priority_reaches_bounded_ranker_and_errors_are_structured() {
    let state = state();
    let body: super::PutDocBody = serde_json::from_value(serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": 50}
    }))
    .expect("typed body");
    let response = put_doc(
        State(Arc::clone(&state)),
        Path(77),
        Ok(Query(super::PutDocParams::default())),
        Json(body),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::CREATED);

    let snap = state.snapshot.load();
    let program = snap
        .compile_rank_program(&reverse_rusty::RankProgramSpec::default())
        .expect("priority program");
    let ranked = snap
        .try_match_title_top_k(
            "2020 topps chrome",
            reverse_rusty::TopKOptions::default(),
            &program,
            &reverse_rusty::exact::TagPredicate::empty(),
            &mut MatchScratch::new(),
            None,
        )
        .expect("ranked match");
    assert_eq!(
        ranked.hits[0],
        reverse_rusty::RankedHit {
            logical_id: 77,
            score: 50
        }
    );

    let invalid: super::PutDocBody = serde_json::from_value(serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": 1.5}
    }))
    .expect("invalid rank still decodes at DTO layer");
    let response = put_doc(
        State(Arc::clone(&state)),
        Path(78),
        Ok(Query(super::PutDocParams::default())),
        Json(invalid),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(json["error"]["type"], "invalid_rank_value");
}

#[test]
fn structured_tag_values_fail_loud() {
    // Pre-fix these were dropped SILENTLY, leaving the query unreachable by any
    // filter on the key (the ADR-064 item-4 finding). Now they are hard errors.
    assert!(
        tags_of(&serde_json::json!({"query": "q", "tags": {"meta": {"x": 1}}})).is_err(),
        "object tag value must error"
    );
    assert!(
        tags_of(&serde_json::json!({"query": "q", "colors": [["nested"]]})).is_err(),
        "nested array tag element must error"
    );
    assert!(
        tags_of(&serde_json::json!({"query": "q", "tags": ["not", "an", "object"]})).is_err(),
        "a non-object `tags` field must error (was silently ignored)"
    );
}

#[tokio::test]
async fn put_doc_rejects_structured_tag_value_with_400() {
    let state = state();
    let body: super::PutDocBody = serde_json::from_value(serde_json::json!({
        "query": "michael jordan",
        "tags": {"meta": {"x": 1}},
    }))
    .expect("body deserializes");
    let resp = put_doc(
        State(Arc::clone(&state)),
        Path(7),
        Ok(Query(super::PutDocParams::default())),
        Json(body),
    )
    .await
    .into_response();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    // Nothing was ingested: the engine never saw the doc.
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").is_empty());
}

#[tokio::test]
async fn rejected_reput_leaves_old_version_live() {
    let state = state();
    do_put(&state, 7, "michael jordan").await;

    // A parse error never reaches the engine; the old version stays live.
    let (status, _) = do_put(&state, 7, "(").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7));

    // A class-D rejection (negation-only) also leaves the old version live.
    let (status, body) = do_put(&state, 7, "-graded").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["result"], "rejected");
    assert!(
        matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7),
        "a failed replace must never delete"
    );
}
