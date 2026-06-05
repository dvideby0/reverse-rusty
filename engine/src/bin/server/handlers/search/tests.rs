//! Handler-level tests for POST /_mpercolate: request validation, the empty
//! batch no-op, the responses[] envelope shape, and — the load-bearing one —
//! that each per-document response is identical to the per-title path
//! (`match_title`), so the batch endpoint can't silently diverge from
//! `/_search`. The library already proves batch == scalar (tests/broad_batch);
//! this proves the HTTP layer threads results through in order and unchanged.
use super::mpercolate::{mpercolate, MPercolateBody};
use super::percolate::{search, SearchBody};
use super::DocBody;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use axum::extract::State;
use axum::Json;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::Normalizer;
use std::sync::Arc;

fn corpus() -> (Engine, Vec<String>) {
    let data = generate(&GenConfig {
        num_queries: 5_000,
        num_titles: 300,
        broad_query_frac: 0.1,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0BA7_C0DE,
        num_players: 2_000,
        num_sets: 1_000,
    });
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&data.queries);
    (eng, data.titles)
}

fn state_with(eng: Engine, include_broad: bool) -> Arc<AppState> {
    let snap = Arc::new(eng.snapshot());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    Arc::new(AppState {
        engine: parking_lot::Mutex::new(eng),
        snapshot: arc_swap::ArcSwap::new(snap),
        pool,
        include_broad,
        prom: PrometheusMetrics::new(),
        slow_query_threshold_ms: 0,
    })
}

fn body(docs: Option<Vec<&str>>, include_broad: Option<bool>, profile: bool) -> MPercolateBody {
    MPercolateBody {
        documents: docs.map(|v| {
            v.into_iter()
                .map(|t| DocBody {
                    title: t.to_string(),
                })
                .collect()
        }),
        filter: None,
        query: None,
        include_broad,
        include_source: Some(false),
        // Large cap so no per-document truncation can mask a result mismatch.
        size: Some(1_000_000),
        from: None,
        rank: None,
        timeout_ms: None,
        profile: Some(profile),
    }
}

#[tokio::test]
async fn missing_documents_is_400() {
    let (eng, _) = corpus();
    let state = state_with(eng, false);
    let err = mpercolate(State(state), Json(body(None, None, false)))
        .await
        .err()
        .expect("missing documents must error");
    assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn search_rejects_batch_over_max_percolate_batch() {
    // A multi-doc `/_search` must reject an oversized batch with 400 before
    // scheduling work, exactly like `/_mpercolate` (ADR-052) — otherwise it is
    // bounded only by the HTTP body size. A tiny cap keeps the test small.
    use reverse_rusty::config::EngineConfig;
    let cfg = EngineConfig {
        max_percolate_batch: 2,
        ..EngineConfig::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    eng.build_from_queries(&[(1u64, "michael jordan".to_string())]);
    let state = state_with(eng, false);

    // 3 documents > cap of 2 ⇒ 400 before any matching runs.
    let over: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "a"}, {"title": "b"}, {"title": "c"}],
        "include_source": false,
    }))
    .expect("valid SearchBody");
    let err = search(State(Arc::clone(&state)), Json(over))
        .await
        .err()
        .expect("a batch over max_percolate_batch must 400");
    assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);

    // A batch AT the cap is accepted (the guard is strictly `>`).
    let at_cap: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "a"}, {"title": "b"}],
        "include_source": false,
    }))
    .expect("valid SearchBody");
    assert!(
        search(State(state), Json(at_cap)).await.is_ok(),
        "a batch at the cap must be accepted"
    );
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let (eng, _) = corpus();
    let state = state_with(eng, true);
    let resp = mpercolate(State(state), Json(body(Some(Vec::new()), None, true)))
        .await
        .expect("empty batch is a valid no-op")
        .0;
    assert!(resp.responses.is_empty());
    assert!(resp.broad.is_none(), "no work => no broad summary");
}

// Reads the ES-convention `_id` field on hits (clippy::used_underscore_binding).
#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn responses_are_byte_identical_to_per_title_search() {
    let (eng, titles) = corpus();
    // Capture a snapshot of the same state for the per-title baseline before
    // the engine moves into the AppState.
    let baseline = Arc::new(eng.snapshot());
    let state = state_with(eng, true);

    let batch: Vec<&str> = titles.iter().take(150).map(String::as_str).collect();
    // include_broad=true exercises the columnar broad lane through the endpoint.
    let resp = mpercolate(
        State(Arc::clone(&state)),
        Json(body(Some(batch.clone()), Some(true), true)),
    )
    .await
    .expect("ok")
    .0;

    assert_eq!(
        resp.responses.len(),
        batch.len(),
        "one response per document"
    );

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut summed = 0u32;
    for (i, title) in batch.iter().enumerate() {
        out.clear();
        baseline.match_title(title, &mut scratch, &mut out, true);
        let mut expected = out.clone();
        expected.sort_unstable();
        expected.dedup();

        let item = &resp.responses[i];
        let mut got: Vec<u64> = item.hits.hits.iter().map(|h| h._id).collect();
        got.sort_unstable();
        assert_eq!(
            got, expected,
            "document {i} ({title}) diverged from per-title search"
        );
        assert_eq!(item.hits.total, expected.len(), "total mismatch at {i}");
        summed += expected.len() as u32;
    }

    // Top-level broad summary present (profile=true) and internally consistent.
    let broad = resp.broad.expect("profile=true => broad summary");
    assert_eq!(broad.strategy, "columnar");
    assert_eq!(broad.batch_size, 256);
    assert_eq!(
        broad.total_matches, summed,
        "summary total must equal the per-document sum"
    );
}

// -- Ranking + pagination (ADR-059) ----------------------------------------

/// A small engine where three queries all match `"2020 topps chrome update"`,
/// each carrying distinct `priority`/`tier` tags — the fixture for ranking.
fn tagged_state() -> Arc<AppState> {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.insert_live_with_tags(
        "topps chrome",
        1,
        1,
        &[
            ("priority".to_string(), "10".to_string()),
            ("tier".to_string(), "gold".to_string()),
        ],
    );
    eng.insert_live_with_tags(
        "topps chrome",
        2,
        1,
        &[("priority".to_string(), "50".to_string())],
    );
    eng.insert_live_with_tags(
        "topps chrome",
        3,
        1,
        &[("tier".to_string(), "gold".to_string())],
    );
    state_with(eng, false)
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn mpercolate_ranks_by_priority_and_truncates_to_size() {
    let state = tagged_state();
    let req: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 topps chrome update"}],
        "rank": {"priority_key": "priority"},
        "size": 2
    }))
    .expect("valid body");
    let resp = mpercolate(State(state), Json(req)).await.expect("ok").0;
    let item = &resp.responses[0];
    assert_eq!(item.hits.total, 3, "total is the untruncated match count");
    let ids: Vec<u64> = item.hits.hits.iter().map(|h| h._id).collect();
    assert_eq!(ids, vec![2, 1], "size=2 → top two by priority (50, 10)");
    assert_eq!(item.hits.hits[0]._score, Some(50));
    assert_eq!(item.hits.hits[1]._score, Some(10));
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn mpercolate_from_offsets_into_ranked_hits() {
    let state = tagged_state();
    let req: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 topps chrome update"}],
        "rank": {"priority_key": "priority"},
        "from": 1,
        "size": 10
    }))
    .expect("valid body");
    let resp = mpercolate(State(state), Json(req)).await.expect("ok").0;
    let ids: Vec<u64> = resp.responses[0].hits.hits.iter().map(|h| h._id).collect();
    // ranked order is [2, 1, 3]; from=1 drops the first → [1, 3].
    assert_eq!(ids, vec![1, 3]);
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn ranking_preserves_the_matched_set_and_score_is_opt_in() {
    let state = tagged_state();
    let ranked: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 topps chrome update"}],
        "rank": {"priority_key": "priority", "boosts": [{"key": "tier", "value": "gold", "boost": 100}]},
        "size": 100
    }))
    .expect("valid body");
    let unranked: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 topps chrome update"}],
        "size": 100
    }))
    .expect("valid body");
    let r = mpercolate(State(Arc::clone(&state)), Json(ranked))
        .await
        .expect("ok")
        .0;
    let u = mpercolate(State(state), Json(unranked))
        .await
        .expect("ok")
        .0;

    let mut rset: Vec<u64> = r.responses[0].hits.hits.iter().map(|h| h._id).collect();
    let mut uset: Vec<u64> = u.responses[0].hits.hits.iter().map(|h| h._id).collect();
    rset.sort_unstable();
    uset.sort_unstable();
    assert_eq!(
        rset, uset,
        "ranking must not add or drop a match (recall guard)"
    );

    assert!(
        u.responses[0].hits.hits.iter().all(|h| h._score.is_none()),
        "unranked hits carry no _score (byte-identical response)"
    );
    assert!(
        r.responses[0].hits.hits.iter().all(|h| h._score.is_some()),
        "ranked hits all carry a _score"
    );
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn search_single_doc_ranks_additively_with_boost() {
    let state = tagged_state();
    let req: SearchBody = serde_json::from_value(serde_json::json!({
        "document": {"title": "2020 topps chrome update"},
        "rank": {"priority_key": "priority", "boosts": [{"key": "tier", "value": "gold", "boost": 100}]}
    }))
    .expect("valid body");
    let resp = search(State(state), Json(req)).await.expect("ok").0;
    let ids: Vec<u64> = resp.hits.hits.iter().map(|h| h._id).collect();
    // additive: 1 = 10+100, 3 = 0+100, 2 = 50 → [1, 3, 2].
    assert_eq!(ids, vec![1, 3, 2]);
    assert_eq!(resp.hits.hits[0]._score, Some(110));
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn search_multi_doc_truncates_per_slot_by_size() {
    let state = tagged_state();
    let req: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 topps chrome update"}],
        "size": 1,
        "rank": {"priority_key": "priority"}
    }))
    .expect("valid body");
    let resp = search(State(state), Json(req)).await.expect("ok").0;
    let slots = resp.slots.expect("multi-doc response has slots");
    assert_eq!(
        slots[0].total, 3,
        "per-slot total preserves the untruncated count"
    );
    assert_eq!(
        slots[0].hits.len(),
        1,
        "per-slot hits truncated to size=1 (ADR-059)"
    );
    assert_eq!(
        slots[0].hits[0]._id, 2,
        "the surviving hit is the top by priority"
    );
}
