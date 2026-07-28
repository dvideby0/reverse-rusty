//! Broad-lane batch/columnar evaluation — the load-bearing correctness test.
//!
//! The columnar broad-batch path MUST return, per title, EXACTLY the same match
//! set as the scalar per-title path (`match_title(include_broad=true)`). This is
//! a pure performance change; any divergence is a correctness bug (a false
//! negative or false positive). These tests assert that equivalence across the
//! shapes that matter: single vs multi segment, memtable, tombstones, any-of,
//! forbidden, batch-boundary sizes, a batch-size sweep, and the
//! `Inline`/`Columnar` kill-switch. Data generation is seeded (ADR-008).

use reverse_rusty::gen::{generate, Dataset, GenConfig};
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
use reverse_rusty::{EngineConfig, Normalizer};

fn gen(seed: u64, num_queries: usize, num_titles: usize, broad_frac: f64) -> Dataset {
    generate(&GenConfig {
        num_queries,
        num_titles,
        broad_query_frac: broad_frac,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_entities: (num_queries / 40).max(2_000),
        num_collections: (num_queries / 100).max(1_000),
    })
}

/// Single base segment (build_from_queries).
fn build_single(data: &Dataset) -> Engine {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&data.queries);
    eng
}

/// Several base segments + a memtable tail — exercises the cross-segment union
/// (mirrors the oracle's multi-segment builder).
fn build_multi(data: &Dataset) -> Engine {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    let n = data.queries.len();
    let c = n / 4;
    eng.build_from_queries(&data.queries[..c]);
    eng.bulk_ingest(&data.queries[c..2 * c]);
    eng.bulk_ingest(&data.queries[2 * c..3 * c]);
    for (id, text) in &data.queries[3 * c..] {
        eng.insert_live(text, *id, 1);
    }
    eng
}

/// The scalar per-title baseline: the contract the batch path must reproduce.
fn scalar_baseline(eng: &Engine, titles: &[String], include_broad: bool) -> Vec<Vec<u64>> {
    let snap = eng.snapshot();
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut res = Vec::with_capacity(titles.len());
    for t in titles {
        out.clear();
        snap.match_title(t, &mut scratch, &mut out, include_broad);
        let mut r = out.clone();
        r.sort_unstable();
        r.dedup();
        res.push(r);
    }
    res
}

fn batch_result(eng: &Engine, titles: &[String], opts: BatchMatchOptions) -> Vec<Vec<u64>> {
    let snap = eng.snapshot();
    let mut res = vec![Vec::new(); titles.len()];
    for (idx, mut ids) in snap.match_titles_batch(titles, opts) {
        ids.sort_unstable();
        ids.dedup();
        res[idx] = ids;
    }
    res
}

fn assert_equiv(
    eng: &Engine,
    titles: &[String],
    include_broad: bool,
    batch_size: usize,
    strat: BroadStrategy,
    materialize: bool,
    prefilter: bool,
) {
    let scalar = scalar_baseline(eng, titles, include_broad);
    let batch = batch_result(
        eng,
        titles,
        BatchMatchOptions {
            include_broad,
            broad_batch_size: batch_size,
            broad_strategy: strat,
            broad_materialize: materialize,
            broad_prefilter: prefilter,
        },
    );
    assert_eq!(batch.len(), scalar.len(), "length mismatch");
    for (i, (b, s)) in batch.iter().zip(scalar.iter()).enumerate() {
        assert_eq!(
            b, s,
            "title {i} mismatch (broad={include_broad}, batch_size={batch_size}, strategy={strat:?}, materialize={materialize}, prefilter={prefilter})"
        );
    }
}

/// Sweep batch sizes (incl. word boundaries 64/65 and the degenerate 1, plus
/// sizes around `titles.len()` to catch chunk off-by-ones), for both
/// `include_broad` values and both strategies.
fn run_matrix(eng: &Engine, titles: &[String]) {
    let n = titles.len().max(1);
    let sizes = [1usize, 2, 7, 63, 64, 65, 256, n, n + 1, 2 * n + 3];
    for &bs in &sizes {
        // broad ON, columnar: the case that matters — materialization AND the
        // count-gate prefilter (lever 5a) each swept both ways; the
        // (materialize=false, prefilter=false) cell is exactly the pre-lever
        // full-verification path.
        assert_equiv(eng, titles, true, bs, BroadStrategy::Columnar, true, true);
        assert_equiv(eng, titles, true, bs, BroadStrategy::Columnar, true, false);
        assert_equiv(eng, titles, true, bs, BroadStrategy::Columnar, false, true);
        assert_equiv(eng, titles, true, bs, BroadStrategy::Columnar, false, false);
        // broad OFF: the batch wrapper must not perturb the selective lane.
        assert_equiv(eng, titles, false, bs, BroadStrategy::Columnar, true, true);
        // Inline strategy (kill-switch) must also equal scalar.
        assert_equiv(eng, titles, true, bs, BroadStrategy::Inline, true, true);
    }
}

// ---- Filtered percolation (ADR-049): the columnar batch path must apply the SAME tag
// filter as the scalar path, including the pure-anchor materialization fast path. ----

const FILTER_CATS: [&str; 4] = ["items", "coins", "stamps", "comics"];

fn tags_for(logical: u64) -> Vec<(String, String)> {
    vec![(
        "category".to_string(),
        FILTER_CATS[(logical as usize) % FILTER_CATS.len()].to_string(),
    )]
}

fn build_single_tagged(data: &Dataset) -> Engine {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    let tags: Vec<Vec<(String, String)>> = data.queries.iter().map(|(l, _)| tags_for(*l)).collect();
    eng.try_build_from_queries_with_tags(&data.queries, &tags)
        .expect("tagged build");
    eng
}

fn scalar_filtered(
    eng: &Engine,
    titles: &[String],
    filter: &[(String, Vec<String>)],
) -> Vec<Vec<u64>> {
    let snap = eng.snapshot();
    let pred = snap.compile_tag_predicate(filter);
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    titles
        .iter()
        .map(|t| {
            out.clear();
            snap.match_title_filtered(t, &mut scratch, &mut out, true, &pred);
            let mut r = out.clone();
            r.sort_unstable();
            r.dedup();
            r
        })
        .collect()
}

fn batch_filtered(
    eng: &Engine,
    titles: &[String],
    opts: BatchMatchOptions,
    filter: &[(String, Vec<String>)],
) -> Vec<Vec<u64>> {
    let snap = eng.snapshot();
    let pred = snap.compile_tag_predicate(filter);
    let mut res = vec![Vec::new(); titles.len()];
    for (idx, mut ids) in snap.match_titles_batch_filtered(titles, opts, &pred) {
        ids.sort_unstable();
        ids.dedup();
        res[idx] = ids;
    }
    res
}

// ---- The batch count-gate pre-reject (lever 5a of the Broad-Query Cost Program):
// a necessary-condition filter, so under-reject is the only possible error direction —
// results must be identical with the prefilter on or off, and the meter must prove the
// skip actually fires on the shape it exists for. ----

/// A hand-built corpus where the prefilter provably bites: class-C queries carry TWO
/// any-of groups — the cover anchors on the more-selective group, the other group is a
/// verify-only condition — and half the titles lack that second group entirely. Reached
/// via their anchor, those candidates can never match any such title, which is exactly
/// what the count-gate detects at batch level.
fn prefilter_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let mut queries: Vec<(u64, String)> = Vec::new();
    // 24 two-group class-C queries. Every distinct query-side feature in this corpus is
    // common-mask hot (fewer than 64 features total), so any-of groups classify C
    // (broad lane) and the queries are NOT pure-anchor (two groups -> full verification).
    for i in 0..24u64 {
        queries.push((i, "(alpha,beta) (gamma,delta)".to_string()));
    }
    // Filler queries inflate gamma/delta frequency so the (alpha,beta) group is the
    // more-selective cover choice (anchors = alpha, beta; gamma/delta stay verify-only).
    for i in 0..30u64 {
        queries.push((1_000 + i, format!("gamma delta filler{i}")));
    }
    let mut titles = Vec::new();
    for i in 0..40 {
        // Anchor-bearing titles WITHOUT the second group: reached, never matching.
        titles.push(format!("alpha item number {i}"));
    }
    for i in 0..8 {
        // Titles bearing both groups: these must keep matching (the over-reject guard).
        titles.push(format!("alpha gamma item {i}"));
    }
    (queries, titles)
}

// ---- The hot tier (class H, ADR-105): batch ≡ scalar with the always-visible,
// columnar-evaluated tier in play — including the load-bearing broad-OFF cell
// (the hot columnar pass must run and agree even when the broad lane is off),
// the Inline kill-switch, the materialize (vacuous-accept) kill-switch on the
// tail-anchored population, and the ADR-061 multi-word-alias forced-inline path. ----

/// θ small enough that the generated corpus's Zipf-head entities classify H at
/// this scale (asserted, not assumed).
const HOT_THETA: u32 = 64;

fn build_multi_hot(data: &Dataset) -> Engine {
    let cfg = reverse_rusty::config::EngineConfig {
        hot_anchor_threshold: HOT_THETA,
        ..Default::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    let n = data.queries.len();
    let c = n / 4;
    eng.build_from_queries(&data.queries[..c]);
    eng.bulk_ingest(&data.queries[c..2 * c]);
    eng.bulk_ingest(&data.queries[2 * c..3 * c]);
    for (id, text) in &data.queries[3 * c..] {
        eng.insert_live(text, *id, 1);
    }
    assert!(
        eng.class_counts()[4] > 0,
        "θ={HOT_THETA} produced no class H — degenerate hot corpus"
    );
    eng
}

#[path = "broad_batch/basic.rs"]
mod basic;
#[path = "broad_batch/filtered.rs"]
mod filtered;
#[path = "broad_batch/hot.rs"]
mod hot;
#[path = "broad_batch/prefilter.rs"]
mod prefilter;
