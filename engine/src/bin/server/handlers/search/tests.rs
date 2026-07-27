//! Handler-level tests for POST /_mpercolate: request validation, the empty
//! batch no-op, the responses[] envelope shape, and — the load-bearing one —
//! that each per-document response is identical to the per-title path
//! (`match_title`), so the batch endpoint can't silently diverge from
//! `/_search`. The library already proves batch == scalar (tests/broad_batch);
//! this proves the HTTP layer threads results through in order and unchanged.
use super::mpercolate::{mpercolate, mpercolate_route, MPercolateBody, MPercolateDoc};
use super::percolate::{search, search_route, SearchBody};
use super::v2::{v2_mpercolate, v2_search, V2MPercolateBody, V2SearchBody};
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
        flush_serial: parking_lot::Mutex::new(()),
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
                .map(|t| MPercolateDoc {
                    title: t.to_string(),
                })
                .collect()
        }),
        filter: None,
        query: None,
        include_broad,
        include_source: Some(false),
        source: None,
        // Large cap so no per-document truncation can mask a result mismatch.
        size: Some(1_000_000),
        from: None,
        rank: None,
        timeout_ms: None,
        timeout: None,
        profile: Some(profile),
        explain: None,
        allow_partial_search_results: None,
    }
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

// -- Per-request include_broad on /_search (ADR-073, ADR-064 item 6) --------

/// The engine-truth match set for `title` at a given broad setting.
#[allow(clippy::used_underscore_binding)]
fn expected_ids(state: &Arc<AppState>, title: &str, include_broad: bool) -> Vec<u64> {
    let snap = state.snapshot.load();
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    snap.match_title(title, &mut s, &mut out, include_broad);
    out.sort_unstable();
    out
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

fn v2_batch_body(value: serde_json::Value) -> V2MPercolateBody {
    serde_json::from_value(value).expect("valid v2 batch body")
}

mod basic;
mod batch;
mod execution;
mod filtered;
mod mpercolate_contract;
mod ranked;
