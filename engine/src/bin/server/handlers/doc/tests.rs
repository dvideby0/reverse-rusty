//! Handler-level tests for the `PUT /_doc/{id}` atomic upsert (ADR-067): the ES
//! 201-created / 200-updated status split, replace-by-id visible through the
//! published snapshot, and DELETE reporting one live copy after a re-PUT (the
//! ADR-064 audit observed `deleted_count: 2` on the pre-fix additive path).

use super::{delete_doc, put_doc};
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::Normalizer;
use std::sync::Arc;

fn state() -> Arc<AppState> {
    let eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
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
    let resp = put_doc(State(Arc::clone(state)), Path(id), Json(put_body(query)))
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

#[tokio::test]
async fn put_doc_is_created_then_updated_with_replace_semantics() {
    let state = state();

    // First PUT: 201 created.
    let (status, body) = do_put(&state, 7, "michael jordan").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["result"], "created");
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7));

    // Re-PUT with different semantics: 200 updated, and the snapshot flips
    // atomically — the old version stops matching exactly when the new starts
    // (one lock, one publish; no matches-under-either-version window).
    let (status, body) = do_put(&state, 7, "lebron james").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "updated");
    assert!(
        !matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7),
        "old semantics must stop matching after the re-PUT"
    );
    assert!(matches_in_snapshot(&state, "2003 topps lebron james rookie").contains(&7));
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
    let snap = Arc::new(eng.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    let prom = PrometheusMetrics::new();
    let state = Arc::new(AppState {
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
    });

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
    let response = put_doc(State(Arc::clone(&state)), Path(77), Json(body))
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
    let response = put_doc(State(Arc::clone(&state)), Path(78), Json(invalid))
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
    let resp = put_doc(State(Arc::clone(&state)), Path(7), Json(body))
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
