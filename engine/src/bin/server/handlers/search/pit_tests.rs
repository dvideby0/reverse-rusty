//! Handler-level tests for the ADR-113 PIT + cursor flow on `/v2/_search` and
//! `POST/DELETE /v2/_pit` (single-node mode): the exit gate over HTTP (paged
//! concatenation ≡ one shot, pinned across engine mutation), the status
//! contract (409 stale / 400 mismatch / 429 cap), and the named batch rejects.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;

use crate::handlers::pit::{close_pit, open_pit};
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;

use super::v2::{v2_mpercolate, v2_search, V2MPercolateBody, V2SearchBody};

use reverse_rusty::segment::Engine;
use reverse_rusty::{Normalizer, RankValues};

/// 25 ranked queries all matching "2020 topps chrome update", with score ties
/// so the id tie-break is exercised across page boundaries.
fn ranked_corpus() -> Engine {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    for id in 1..=25u64 {
        let priority = (id % 5) as i64 * 10;
        eng.try_insert_live_ranked(
            "topps chrome",
            id,
            1,
            &[("priority".to_string(), priority.to_string())],
            Some(RankValues { priority }),
        )
        .expect("insert");
        if id == 13 {
            eng.flush();
        }
    }
    eng
}

fn pit_state(eng: Engine, pit_config: reverse_rusty::PitConfig) -> Arc<AppState> {
    let snap = Arc::new(eng.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    Arc::new(AppState {
        engine: parking_lot::Mutex::new(eng),
        snapshot: arc_swap::ArcSwap::new(snap),
        pool,
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(2)),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad: false,
        prom: PrometheusMetrics::new(),
        slow_query_threshold_ms: 0,
        auth: None,
        feedback: parking_lot::Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
        pit_tokens: crate::pit::PitTokens::generate(),
        pits: parking_lot::Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config,
    })
}

fn state() -> Arc<AppState> {
    pit_state(ranked_corpus(), reverse_rusty::PitConfig::default())
}

fn v2_body(value: serde_json::Value) -> V2SearchBody {
    serde_json::from_value(value).expect("valid v2 body")
}

async fn open(state: &Arc<AppState>, keep_alive_s: Option<u64>) -> Result<String, StatusCode> {
    let body = keep_alive_s.map(|s| {
        Json(serde_json::from_value(serde_json::json!({ "keep_alive_s": s })).expect("body"))
    });
    match open_pit(State(Arc::clone(state)), body).await {
        Ok(response) => {
            let json = serde_json::to_value(response.0).expect("open json");
            Ok(json["pit_id"].as_str().expect("pit_id string").to_string())
        }
        Err((status, _)) => Err(status),
    }
}

/// Run one v2 search built from `base` + the page shape, returning the JSON
/// response or the error (status, kind).
async fn run(
    state: &Arc<AppState>,
    mut base: serde_json::Value,
    page: Option<(&str, serde_json::Value)>,
) -> Result<serde_json::Value, (StatusCode, String)> {
    if let Some((key, value)) = page {
        base[key] = value;
    }
    match v2_search(State(Arc::clone(state)), Json(v2_body(base))).await {
        Ok(response) => Ok(serde_json::to_value(response.0).expect("response json")),
        Err((status, body)) => {
            let json = serde_json::to_value(body.0).expect("error json");
            Err((
                status,
                json["error"]["type"].as_str().unwrap_or("").to_string(),
            ))
        }
    }
}

fn hits_of(json: &serde_json::Value) -> Vec<(u64, i64)> {
    json["hits"]["hits"]
        .as_array()
        .expect("hits array")
        .iter()
        .map(|hit| {
            (
                hit["_id"].as_u64().expect("id"),
                hit["_score"].as_i64().expect("score"),
            )
        })
        .collect()
}

fn base_body(size: usize, include_source: bool) -> serde_json::Value {
    serde_json::json!({
        "document": {"title": "2020 topps chrome update"},
        "size": size,
        "include_source": include_source,
        "rank": {"priority_field": "priority"},
    })
}

/// The HTTP exit gate: open a PIT, page to exhaustion following next_cursor,
/// mutating + republishing the engine between pages — the concatenation equals
/// the one-shot over the same PIT, totals are page-invariant, and a fresh
/// (non-PIT) search sees the mutated world.
#[tokio::test]
async fn pit_pages_concatenate_and_pin_across_mutation() {
    let state = state();
    let pit = open(&state, None).await.expect("open pit");

    let one_shot = run(
        &state,
        base_body(1_000, false),
        Some(("pit", serde_json::json!({"id": pit}))),
    )
    .await
    .expect("one-shot over the pit");
    let expected = hits_of(&one_shot);
    assert_eq!(expected.len(), 25);
    assert_eq!(
        one_shot["hits"]["total"],
        serde_json::json!({"value": 25, "relation": "eq"})
    );
    assert!(
        one_shot["next_cursor"].is_null(),
        "a short page (25 hits < size 1000) is the end of the stream — no cursor"
    );

    let mut pages: Vec<(u64, i64)> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut fresh_id = 100u64;
    loop {
        let page = match &cursor {
            None => Some(("pit", serde_json::json!({"id": pit}))),
            Some(token) => Some(("cursor", serde_json::json!(token))),
        };
        let json = run(&state, base_body(7, false), page).await.expect("page");
        assert_eq!(
            json["hits"]["total"],
            serde_json::json!({"value": 25, "relation": "eq"}),
            "pinned totals are page-invariant"
        );
        pages.extend(hits_of(&json));

        // Mutate + republish between pages: the pinned PIT must not care.
        {
            let mut eng = state.engine.lock();
            eng.delete_by_logical_id(pages[0].0).expect("delete");
            eng.try_insert_live_ranked(
                "topps chrome",
                fresh_id,
                1,
                &[("priority".to_string(), "990".to_string())],
                Some(RankValues { priority: 990 }),
            )
            .expect("insert");
            fresh_id += 1;
            eng.flush();
            let _ = eng.compact_all();
        }
        state.publish_snapshot();

        match json["next_cursor"].as_str() {
            Some(token) => cursor = Some(token.to_string()),
            None => break,
        }
    }
    assert_eq!(pages, expected, "concatenated pages equal the one-shot");

    // A fresh non-PIT search sees the mutated world (the first winner was
    // deleted, a 990-priority query now leads).
    let live = run(&state, base_body(1_000, false), None)
        .await
        .expect("live search");
    let live_hits = hits_of(&live);
    assert!(live_hits.iter().any(|&(id, _)| id >= 100));
    assert!(!live_hits.iter().any(|&(id, _)| id == expected[0].0));
    assert!(live["next_cursor"].is_null());
}

/// One short assert kept out of the loop above: a full FINAL page (hits ==
/// size exactly at the end of the stream) mints a cursor whose next page is
/// empty with no cursor — the stream terminates cleanly, no dup, no gap.
#[tokio::test]
async fn exactly_full_final_page_terminates_with_an_empty_page() {
    let state = state();
    let pit = open(&state, None).await.expect("open pit");
    // 25 rows, size 25: the single page is full, so a cursor is minted.
    let full = run(
        &state,
        base_body(25, false),
        Some(("pit", serde_json::json!({"id": pit}))),
    )
    .await
    .expect("full page");
    assert_eq!(hits_of(&full).len(), 25);
    let token = full["next_cursor"]
        .as_str()
        .expect("cursor minted")
        .to_string();
    let after = run(
        &state,
        base_body(25, false),
        Some(("cursor", serde_json::json!(token))),
    )
    .await
    .expect("empty terminal page");
    assert_eq!(hits_of(&after).len(), 0);
    assert!(after["next_cursor"].is_null());
    assert_eq!(
        after["hits"]["total"],
        serde_json::json!({"value": 25, "relation": "eq"})
    );
}

/// Deleting a next-page winner between pages: ids/scores stay pinned, but
/// `_source` enrichment is current-view fail-closed — the include_source page
/// fails typed (500 source_unavailable) while include_source=false is green.
#[tokio::test]
async fn deleted_winner_source_enrichment_fails_closed_under_pit() {
    let state = state();
    let pit = open(&state, None).await.expect("open pit");
    // The full pinned ranking, so the victim can be chosen from page 2's rows.
    let one_shot = run(
        &state,
        base_body(1_000, false),
        Some(("pit", serde_json::json!({"id": pit.clone()}))),
    )
    .await
    .expect("one-shot ranking");
    let ranking = hits_of(&one_shot);
    let page1 = run(
        &state,
        base_body(7, true),
        Some(("pit", serde_json::json!({"id": pit}))),
    )
    .await
    .expect("page 1 with sources");
    let token = page1["next_cursor"].as_str().expect("cursor").to_string();

    // Delete a page-2 winner live (rank position 8 in the pinned order).
    let victim = ranking[7].0;
    {
        let mut eng = state.engine.lock();
        eng.delete_by_logical_id(victim).expect("delete");
    }
    state.publish_snapshot();

    // Page 2 with sources: the pinned match set still contains the victim,
    // whose source is now gone — fail closed.
    let err = run(
        &state,
        base_body(7, true),
        Some(("cursor", serde_json::json!(token.clone()))),
    )
    .await
    .expect_err("deleted winner must fail source enrichment");
    assert_eq!(err.0, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(err.1, "source_unavailable");

    // The same page without enrichment still serves the pinned ids.
    let page2 = run(
        &state,
        base_body(7, false),
        Some(("cursor", serde_json::json!(token))),
    )
    .await
    .expect("id-only page");
    assert!(hits_of(&page2).iter().any(|&(id, _)| id == victim));
}

#[tokio::test]
async fn token_and_page_shape_failures_are_typed() {
    let state = state();
    let pit = open(&state, None).await.expect("open pit");

    // Garbage cursor: 400 validation; tampered/foreign-key tokens: 409 stale.
    let err = run(
        &state,
        base_body(7, false),
        Some(("cursor", serde_json::json!("zz"))),
    )
    .await
    .expect_err("garbage cursor");
    assert_eq!(
        (err.0, err.1.as_str()),
        (StatusCode::BAD_REQUEST, "validation_error")
    );

    let foreign = crate::pit::PitTokens::generate().mint_pit(reverse_rusty::PitId(0));
    let err = run(
        &state,
        base_body(7, false),
        Some(("pit", serde_json::json!({"id": foreign}))),
    )
    .await
    .expect_err("foreign-key pit is stale (the restart semantics)");
    assert_eq!(
        (err.0, err.1.as_str()),
        (StatusCode::CONFLICT, "stale_cursor")
    );

    // pit + cursor together: 400.
    let mut both = base_body(7, false);
    both["pit"] = serde_json::json!({"id": pit.clone()});
    both["cursor"] = serde_json::json!("beef");
    let err = run(&state, both, None).await.expect_err("pit+cursor");
    assert_eq!(err.0, StatusCode::BAD_REQUEST);

    // `from` stays a named 400.
    let mut from = base_body(7, false);
    from["from"] = serde_json::json!(10);
    let err = run(&state, from, None).await.expect_err("from rejected");
    assert_eq!(err.0, StatusCode::BAD_REQUEST);

    // A closed PIT is stale for both page shapes.
    let page1 = run(
        &state,
        base_body(7, false),
        Some(("pit", serde_json::json!({"id": pit.clone()}))),
    )
    .await
    .expect("page before close");
    let token = page1["next_cursor"].as_str().expect("cursor").to_string();
    let closed = close_pit(
        State(Arc::clone(&state)),
        Json(serde_json::from_value(serde_json::json!({"pit_id": pit})).expect("close body")),
    )
    .await
    .expect("close");
    assert_eq!(
        serde_json::to_value(closed.0).expect("json")["closed"],
        true
    );
    let err = run(
        &state,
        base_body(7, false),
        Some(("cursor", serde_json::json!(token))),
    )
    .await
    .expect_err("cursor after close");
    assert_eq!(
        (err.0, err.1.as_str()),
        (StatusCode::CONFLICT, "stale_cursor")
    );
}

#[tokio::test]
async fn cursor_fingerprint_mismatch_is_a_named_400() {
    let state = state();
    let pit = open(&state, None).await.expect("open pit");
    let page1 = run(
        &state,
        base_body(7, false),
        Some(("pit", serde_json::json!({"id": pit}))),
    )
    .await
    .expect("page 1");
    let token = page1["next_cursor"].as_str().expect("cursor").to_string();

    // Changed title / scope / rank / filter each 400 as cursor_mismatch.
    for mutation in [
        serde_json::json!({"document": {"title": "different title entirely"}}),
        serde_json::json!({"query_scope": "with_broad"}),
        serde_json::json!({"rank": {"priority_field": "priority",
                                     "boosts": [{"key": "t", "value": "v", "boost": 5}]}}),
        serde_json::json!({"filter": {"tier": "gold"}}),
    ] {
        let mut body = base_body(7, false);
        for (key, value) in mutation.as_object().expect("mutation object") {
            body[key] = value.clone();
        }
        body["cursor"] = serde_json::json!(token.clone());
        let err = run(&state, body, None).await.expect_err("mismatch");
        assert_eq!(
            (err.0, err.1.as_str()),
            (StatusCode::BAD_REQUEST, "cursor_mismatch")
        );
    }

    // size / timeout / track_total_hits_up_to may vary per page.
    let mut resized = base_body(3, false);
    resized["timeout_ms"] = serde_json::json!(9_000);
    resized["track_total_hits_up_to"] = serde_json::json!(9_999);
    resized["cursor"] = serde_json::json!(token);
    assert!(run(&state, resized, None).await.is_ok());
}

#[tokio::test]
async fn pit_admission_caps_are_typed() {
    let state = pit_state(
        ranked_corpus(),
        reverse_rusty::PitConfig {
            max_open: 1,
            ..reverse_rusty::PitConfig::default()
        },
    );
    let _first = open(&state, None).await.expect("first pit");
    assert_eq!(
        open(&state, None).await.expect_err("cap"),
        StatusCode::TOO_MANY_REQUESTS
    );
    // Over-max keep-alive (default ceiling 600s) is the client's error.
    let default_state = self::state();
    assert_eq!(
        open(&default_state, Some(10_000))
            .await
            .expect_err("keep_alive too large"),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn mpercolate_names_pit_and_cursor_rejects() {
    let state = state();
    for key in ["pit", "cursor"] {
        let mut body = serde_json::json!({
            "documents": [{"title": "2020 topps chrome update"}],
            "rank": {"priority_field": "priority"},
        });
        body[key] = serde_json::json!("anything");
        let body: V2MPercolateBody = serde_json::from_value(body).expect("batch body");
        let result = v2_mpercolate(State(Arc::clone(&state)), Json(body)).await;
        match result {
            Ok(_) => panic!("named batch reject expected for `{key}`"),
            Err(err) => assert_eq!(err.0, StatusCode::BAD_REQUEST),
        }
    }
}
