//! Handler-level tests for POST /_mpercolate: request validation, the empty
//! batch no-op, the responses[] envelope shape, and — the load-bearing one —
//! that each per-document response is identical to the per-title path
//! (`match_title`), so the batch endpoint can't silently diverge from
//! `/_search`. The library already proves batch == scalar (tests/broad_batch);
//! this proves the HTTP layer threads results through in order and unchanged.
use super::mpercolate::{mpercolate, MPercolateBody};
use super::percolate::{search, SearchBody};
use super::v2::{v2_mpercolate, v2_search, V2MPercolateBody, V2SearchBody};
use super::DocBody;
use crate::metrics::PrometheusMetrics;
use crate::state::AppState;
use axum::extract::State;
use axum::response::IntoResponse;
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
    let prom = PrometheusMetrics::new();
    Arc::new(AppState {
        engine: parking_lot::Mutex::new(eng),
        snapshot: arc_swap::ArcSwap::new(snap),
        pool,
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(2)),
        exhaustive_jobs: crate::jobs::ExhaustiveJobs::for_tests(prom.clone()),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad,
        prom,
        slow_query_threshold_ms: 0,
        auth: None,
        feedback: parking_lot::Mutex::new(reverse_rusty::vocab::AliasFeedback::default()),
        pit_tokens: crate::pit::PitTokens::generate(),
        pits: parking_lot::Mutex::new(reverse_rusty::PitRegistry::new()),
        pit_config: reverse_rusty::PitConfig::default(),
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

// -- Per-request include_broad on /_search (ADR-073, ADR-064 item 6) --------

/// The engine-truth match set for `title` at a given broad setting.
fn expected_ids(state: &Arc<AppState>, title: &str, include_broad: bool) -> Vec<u64> {
    let snap = state.snapshot.load();
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    snap.match_title(title, &mut s, &mut out, include_broad);
    out.sort_unstable();
    out
}

#[tokio::test]
async fn search_honors_per_request_include_broad() {
    // Pre-fix `/_search` honored only the server-wide --include-broad and an
    // `include_broad` body field was SILENTLY ignored (serde unknown-field
    // tolerance) — with broad off, class-C hits read as missing data.
    // `/_mpercolate` and the cluster handlers already had the override.
    let (eng, titles) = corpus();
    let state = state_with(eng, false); // server default: broad OFF

    // A title whose match set differs with the broad lane on — the probe that
    // makes the override observable.
    let title = titles
        .iter()
        .find(|t| expected_ids(&state, t, true).len() > expected_ids(&state, t, false).len())
        .expect("corpus(broad_frac=0.1) has a broad-affected title")
        .clone();
    let with_broad = expected_ids(&state, &title, true);
    let without_broad = expected_ids(&state, &title, false);

    // Absent ⇒ the server default (off).
    let ids = search_ids(&state, serde_json::json!({"document": {"title": title}}))
        .await
        .expect("ok");
    assert_eq!(ids, without_broad);
    // Per-request true overrides the off default.
    let ids = search_ids(
        &state,
        serde_json::json!({"document": {"title": title}, "include_broad": true}),
    )
    .await
    .expect("ok");
    assert_eq!(
        ids, with_broad,
        "include_broad:true must surface class-C hits"
    );

    // And the reverse: on a broad-ON server, per-request false suppresses —
    // through the multi-doc arm, so both handler paths honor the override.
    let (eng2, _) = corpus();
    let state_on = state_with(eng2, true);
    let req = serde_json::json!({"documents": [{"title": title}], "include_broad": false});
    let ids = search_ids(&state_on, req).await.expect("ok");
    assert_eq!(
        ids, without_broad,
        "include_broad:false must suppress broad"
    );
}

// -- Tag-value coercion on the filter path (ADR-073, ADR-064 item 4) --------

/// Run `/_search` with a JSON body, returning the sorted hit ids (Ok) or the
/// HTTP status (Err).
// Reads the ES-convention `_id` field on hits (clippy::used_underscore_binding).
#[allow(clippy::used_underscore_binding)]
async fn search_ids(
    state: &Arc<AppState>,
    body: serde_json::Value,
) -> Result<Vec<u64>, axum::http::StatusCode> {
    let req: SearchBody = serde_json::from_value(body).expect("valid SearchBody");
    match search(State(Arc::clone(state)), Json(req)).await {
        Ok(resp) => {
            let mut ids: Vec<u64> = resp.0.hits.hits.iter().map(|h| h._id).collect();
            ids.sort_unstable();
            Ok(ids)
        }
        Err((status, _)) => Err(status),
    }
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn numeric_tag_ingest_meets_numeric_filter() {
    // The load-bearing agreement (ADR-073): ingest and filter coerce through the
    // SAME canonical rule, so a numeric category ingested as `7` is reachable by
    // a filter sending `7` OR `"7"` — pre-fix the ingest side silently dropped
    // the tag, making the query unreachable by ANY filter on that key.
    let state = state_with(
        Engine::new(Normalizer::default_vocab().expect("vocab")),
        false,
    );
    let body: crate::handlers::doc::PutDocBody = serde_json::from_value(serde_json::json!({
        "query": "michael jordan",
        "tags": {"category": 7, "active": true},
    }))
    .expect("body deserializes");
    let resp = crate::handlers::doc::put_doc(
        axum::extract::State(Arc::clone(&state)),
        axum::extract::Path(1u64),
        Ok(axum::extract::Query(
            crate::handlers::doc::PutDocParams::default(),
        )),
        Json(body),
    )
    .await
    .into_response();
    assert_eq!(resp.status(), axum::http::StatusCode::CREATED);

    let title = serde_json::json!({"title": "1986 fleer michael jordan rookie"});
    // Native filter, number and string forms, plus the coerced bool.
    for filter in [
        serde_json::json!({"category": 7}),
        serde_json::json!({"category": "7"}),
        serde_json::json!({"category": [7]}),
        serde_json::json!({"active": true}),
    ] {
        let ids = search_ids(
            &state,
            serde_json::json!({"document": title, "filter": filter}),
        )
        .await
        .expect("filter coerces, not 400");
        assert_eq!(ids, vec![1], "filter {filter} must reach the tagged query");
    }
    // ES envelope: bool.filter terms with a numeric value.
    let ids = search_ids(
        &state,
        serde_json::json!({"query": {"bool": {
            "must": {"percolate": {"document": title}},
            "filter": [{"terms": {"category": [7]}}],
        }}}),
    )
    .await
    .expect("ES terms coerce");
    assert_eq!(ids, vec![1]);
    // A different number does NOT match (coercion is exact, not fuzzy).
    let ids = search_ids(
        &state,
        serde_json::json!({"document": title, "filter": {"category": 8}}),
    )
    .await
    .expect("ok");
    assert!(ids.is_empty(), "category 8 must not match a category-7 tag");
}

#[tokio::test]
async fn unanswerable_filter_values_are_400_not_silently_dropped() {
    // Pre-fix a non-string ARRAY ELEMENT was silently dropped from the filter
    // (widening the predicate); scalars already 400'd. Now everything without a
    // canonical scalar form is a loud 400 on every filter shape.
    let state = state_with(
        Engine::new(Normalizer::default_vocab().expect("vocab")),
        false,
    );
    let title = serde_json::json!({"title": "anything"});
    for (label, body) in [
        (
            "native null",
            serde_json::json!({"document": title, "filter": {"category": null}}),
        ),
        (
            "native object",
            serde_json::json!({"document": title, "filter": {"category": {"x": 1}}}),
        ),
        (
            "native nested array element",
            serde_json::json!({"document": title, "filter": {"category": [["a"]]}}),
        ),
        (
            "native null array element",
            serde_json::json!({"document": title, "filter": {"category": ["a", null]}}),
        ),
        (
            "ES terms null element",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"document": title}},
                "filter": [{"terms": {"category": ["a", null]}}],
            }}}),
        ),
        (
            "ES term null",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"document": title}},
                "filter": [{"term": {"category": null}}],
            }}}),
        ),
        // A clause carrying TWO queries silently dropped the second pre-fix —
        // the widening direction (review catch); ES errors on the shape too.
        (
            "ES clause with both terms and term",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"document": title}},
                "filter": [{"terms": {"a": ["x"]}, "term": {"b": "y"}}],
            }}}),
        ),
        // An empty `terms` object was a silent no-op clause; ES rejects it.
        (
            "ES empty terms clause",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"document": title}},
                "filter": [{"terms": {}}],
            }}}),
        ),
    ] {
        let err = search_ids(&state, body).await.expect_err(label);
        assert_eq!(err, axum::http::StatusCode::BAD_REQUEST, "{label}");
    }
}

// ---- cooperative cancellation + bounded concurrency (ADR-099) ----------------

/// Poll a counter until it reaches `want` (the cancellation is recorded inside the
/// blocking closure, which may finish AFTER the handler already answered 408).
async fn wait_for_count(
    counter: &prometheus::core::GenericCounter<prometheus::core::AtomicU64>,
    want: u64,
) {
    for _ in 0..200 {
        if counter.get() >= want {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "cancellation counter never reached {want} (got {}) — the armed work did not record stopping",
        counter.get()
    );
}

#[tokio::test]
async fn explicit_zero_timeout_cancels_work_and_408s() {
    let (eng, titles) = corpus();
    let state = state_with(eng, false);

    // An explicit timeout_ms arms cooperative cancellation (ADR-099); 0ms is expired
    // by the time the blocking closure runs, so its FIRST deadline check fires —
    // deterministic, no timing sensitivity.
    let req: SearchBody = serde_json::from_value(serde_json::json!({
        "document": {"title": titles[0]},
        "include_source": false,
        "timeout_ms": 0,
    }))
    .expect("valid SearchBody");
    let err = search(State(Arc::clone(&state)), Json(req))
        .await
        .err()
        .expect("a zero timeout must 408");
    assert_eq!(err.0, axum::http::StatusCode::REQUEST_TIMEOUT);

    // The work actually stopped AND recorded it (the closure-side counter).
    let counter = state
        .prom
        .match_cancellations_total
        .with_label_values(&["search"]);
    wait_for_count(&counter, 1).await;
}

#[tokio::test]
async fn mpercolate_explicit_zero_timeout_cancels_and_408s() {
    let (eng, titles) = corpus();
    let state = state_with(eng, false);
    let mut b = body(
        Some(titles.iter().take(8).map(String::as_str).collect()),
        None,
        false,
    );
    b.timeout_ms = Some(0);
    let err = mpercolate(State(Arc::clone(&state)), Json(b))
        .await
        .err()
        .expect("a zero timeout must 408");
    assert_eq!(err.0, axum::http::StatusCode::REQUEST_TIMEOUT);
    let counter = state
        .prom
        .match_cancellations_total
        .with_label_values(&["mpercolate"]);
    wait_for_count(&counter, 1).await;
}

#[tokio::test]
async fn no_explicit_timeout_stays_unarmed() {
    let (eng, titles) = corpus();
    let state = state_with(eng, false);
    let req: SearchBody = serde_json::from_value(serde_json::json!({
        "document": {"title": titles[0]},
        "include_source": false,
    }))
    .expect("valid SearchBody");
    let resp = search(State(Arc::clone(&state)), Json(req)).await;
    assert!(resp.is_ok(), "the unarmed default path must serve normally");
    assert_eq!(
        state
            .prom
            .match_cancellations_total
            .with_label_values(&["search"])
            .get(),
        0,
        "no explicit timeout_ms ⇒ never armed ⇒ never cancelled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_permit_serializes_but_both_searches_succeed() {
    let (eng, titles) = corpus();
    let mut state_arc = state_with(eng, false);
    {
        // A single permit: two concurrent searches must queue, not fail — the
        // semaphore wait sits inside each request's own timeout budget.
        let state = Arc::get_mut(&mut state_arc).expect("sole owner");
        state.search_permits = Some(std::sync::Arc::new(tokio::sync::Semaphore::new(1)));
    }
    let state = state_arc;

    let mk = |t: &str| -> SearchBody {
        serde_json::from_value(serde_json::json!({
            "document": {"title": t},
            "include_source": false,
        }))
        .expect("valid SearchBody")
    };
    let (a, b) = tokio::join!(
        search(State(Arc::clone(&state)), Json(mk(&titles[0]))),
        search(State(Arc::clone(&state)), Json(mk(&titles[1]))),
    );
    assert!(a.is_ok() && b.is_ok(), "both queued searches must succeed");
    assert_eq!(
        state.prom.search_permits_in_use.get(),
        0,
        "all permits released after the work completed"
    );
}

fn ranked_engine() -> Engine {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    for (id, priority) in [(1, 5), (2, 50), (3, -7)] {
        engine
            .try_insert_live_ranked(
                "topps chrome",
                id,
                1,
                &[("priority".into(), priority.to_string())],
                Some(reverse_rusty::RankValues { priority }),
            )
            .expect("ranked insert");
    }
    engine
}

fn v2_body(value: serde_json::Value) -> V2SearchBody {
    serde_json::from_value(value).expect("valid v2 body")
}

#[tokio::test]
async fn v2_defaults_rank_by_priority_and_enrich_winners_only() {
    let state = state_with(ranked_engine(), false);
    let response = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "2020 topps chrome update"}
        }))),
    )
    .await
    .expect("v2 response");
    let json = serde_json::to_value(response.0).expect("response json");
    assert_eq!(json["complete"], true);
    assert_eq!(json["query_scope"], "standard");
    assert_eq!(
        json["_shards"],
        serde_json::json!({"total":1,"successful":1,"failed":0})
    );
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value":3,"relation":"eq"})
    );
    assert_eq!(json["hits"]["hits"][0]["_id"], 2);
    assert_eq!(json["hits"]["hits"][0]["_score"], 50);
    assert!(json["hits"]["hits"][0]["_source"]["query"].is_string());
}

#[tokio::test]
async fn v2_threshold_size_zero_and_unsupported_modes_are_explicit() {
    let state = state_with(ranked_engine(), false);
    let response = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "size": 0,
            "track_total_hits_up_to": 1,
            "include_source": false
        }))),
    )
    .await
    .expect("count-only response");
    let json = serde_json::to_value(response.0).expect("response json");
    assert_eq!(json["hits"]["hits"], serde_json::json!([]));
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value":1,"relation":"gte"})
    );

    let error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "result_mode": "all"
        }))),
    )
    .await
    .err()
    .expect("all is deferred");
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn v2_enforces_rank_bounds_and_unknown_fields() {
    let state = state_with(ranked_engine(), false);
    for body in [
        serde_json::json!({
            "document": {"title": "topps chrome"},
            "size": 10001
        }),
        serde_json::json!({
            "document": {"title": "topps chrome"},
            "track_total_hits_up_to": 10001
        }),
        serde_json::json!({
            "document": {"title": "topps chrome"},
            "rank": {"priority_field": "price"}
        }),
        serde_json::json!({
            "document": {"title": "topps chrome"},
            "from": 1
        }),
    ] {
        let error = v2_search(State(Arc::clone(&state)), Json(v2_body(body)))
            .await
            .err()
            .expect("request must reject");
        assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn v2_source_enrichment_is_fail_closed_and_can_be_disabled() {
    use reverse_rusty::config::EngineConfig;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "reverse_rusty_v2_source_failure_{}_{}",
        std::process::id(),
        nonce
    ));
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine =
            Engine::with_config(Normalizer::default_vocab().expect("vocab"), config.clone());
        engine
            .try_insert_live_ranked(
                "topps chrome",
                1,
                1,
                &[("priority".into(), "9".into())],
                Some(reverse_rusty::RankValues { priority: 9 }),
            )
            .expect("ranked insert");
        engine.flush();
    }
    std::fs::remove_file(dir.join("sources.dat")).expect("remove source store");
    let engine = Engine::open(Normalizer::default_vocab().expect("vocab"), config)
        .expect("source-less reopen");
    let state = state_with(engine, false);

    let error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"}
        }))),
    )
    .await
    .err()
    .expect("default source enrichment must fail closed");
    assert_eq!(error.0, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let error_json = serde_json::to_value(error.1 .0).expect("error json");
    assert_eq!(error_json["error"]["type"], "source_unavailable");

    let response = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "include_source": false
        }))),
    )
    .await
    .expect("source-disabled request");
    let response_json = serde_json::to_value(response.0).expect("response json");
    assert_eq!(
        response_json["hits"]["hits"].as_array().map(Vec::len),
        Some(1)
    );

    let explanation_error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "include_source": false,
            "explain": true
        }))),
    )
    .await
    .err()
    .expect("requested explanation must fail closed without source");
    let explanation_json = serde_json::to_value(explanation_error.1 .0).expect("error json");
    assert_eq!(explanation_json["error"]["type"], "explanation_unavailable");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn v2_deadline_includes_ranked_permit_queue() {
    let mut state = state_with(ranked_engine(), false);
    Arc::get_mut(&mut state)
        .expect("unique state")
        .ranked_search_permits = Arc::new(tokio::sync::Semaphore::new(0));
    let error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"},
            "timeout_ms": 1
        }))),
    )
    .await
    .err()
    .expect("permit queue must consume the deadline");
    assert_eq!(error.0, axum::http::StatusCode::REQUEST_TIMEOUT);
    assert_eq!(state.prom.ranked_search_permits_in_use.get(), 0);
}

#[tokio::test]
async fn v2_enrichment_cap_is_shared_and_fail_closed() {
    let mut state = state_with(ranked_engine(), false);
    Arc::get_mut(&mut state)
        .expect("unique state")
        .max_ranked_enrichment_bytes = 1;
    let error = v2_search(
        State(Arc::clone(&state)),
        Json(v2_body(serde_json::json!({
            "document": {"title": "topps chrome"}
        }))),
    )
    .await
    .err()
    .expect("winner source exceeds one-byte enrichment cap");
    assert_eq!(error.0, axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    let json = serde_json::to_value(error.1 .0).expect("error json");
    assert_eq!(json["error"]["type"], "rank_enrichment_limit");
}

fn v2_batch_body(value: serde_json::Value) -> V2MPercolateBody {
    serde_json::from_value(value).expect("valid v2 batch body")
}

#[tokio::test]
async fn v2_mpercolate_per_slot_equals_v2_search_and_shares_winner_sources() {
    let state = state_with(ranked_engine(), false);
    let titles = [
        "2020 topps chrome update",
        "no match at all",
        "2020 topps chrome update",
    ];
    let batch = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": titles.iter().map(|t| serde_json::json!({"title": t})).collect::<Vec<_>>()
        }))),
    )
    .await
    .expect("batch response");
    let batch_json = serde_json::to_value(batch.0).expect("batch json");
    assert_eq!(batch_json["complete"], true);
    assert_eq!(batch_json["responses"].as_array().map(Vec::len), Some(3));
    for (i, title) in titles.iter().enumerate() {
        let single = v2_search(
            State(Arc::clone(&state)),
            Json(v2_body(serde_json::json!({"document": {"title": title}}))),
        )
        .await
        .expect("single response");
        let single_json = serde_json::to_value(single.0).expect("single json");
        assert_eq!(
            batch_json["responses"][i]["hits"], single_json["hits"],
            "slot {i} must equal its /v2/_search result"
        );
        assert_eq!(
            batch_json["responses"][i]["_shards"], single_json["_shards"],
            "slot {i} shard echo"
        );
    }
}

#[tokio::test]
async fn v2_mpercolate_named_unsupported_shapes_and_empty_batch() {
    let state = state_with(ranked_engine(), false);
    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": [{"title": "topps chrome"}],
            "explain": true
        }))),
    )
    .await
    else {
        panic!("explain must be a named 400");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "document": {"title": "topps chrome"}
        }))),
    )
    .await
    else {
        panic!("the singular document shape must be a named 400");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({}))),
    )
    .await
    else {
        panic!("a MISSING documents field must be a named 400, not an empty 200");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": [{"title": "topps chrome", "size": 1}]
        }))),
    )
    .await
    else {
        panic!("a per-document option must be a named 400, never silently discarded");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let empty = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({"documents": []}))),
    )
    .await
    .expect("empty batch is a 200");
    let json = serde_json::to_value(empty.0).expect("empty json");
    assert_eq!(json["responses"], serde_json::json!([]));
    assert_eq!(json["complete"], true);
}
