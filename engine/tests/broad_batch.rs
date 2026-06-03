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
use reverse_rusty::Normalizer;

fn gen(seed: u64, num_queries: usize, num_titles: usize, broad_frac: f64) -> Dataset {
    generate(&GenConfig {
        num_queries,
        num_titles,
        broad_query_frac: broad_frac,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: (num_queries / 40).max(2_000),
        num_sets: (num_queries / 100).max(1_000),
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
        },
    );
    assert_eq!(batch.len(), scalar.len(), "length mismatch");
    for (i, (b, s)) in batch.iter().zip(scalar.iter()).enumerate() {
        assert_eq!(
            b, s,
            "title {i} mismatch (broad={include_broad}, batch_size={batch_size}, strategy={strat:?}, materialize={materialize})"
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
        // broad ON, columnar: the case that matters — BOTH materialization modes
        // must equal scalar (pure-anchor fast path on, and forced through full
        // verification when off).
        assert_equiv(eng, titles, true, bs, BroadStrategy::Columnar, true);
        assert_equiv(eng, titles, true, bs, BroadStrategy::Columnar, false);
        // broad OFF: the batch wrapper must not perturb the selective lane.
        assert_equiv(eng, titles, false, bs, BroadStrategy::Columnar, true);
        // Inline strategy (kill-switch) must also equal scalar.
        assert_equiv(eng, titles, true, bs, BroadStrategy::Inline, true);
    }
}

#[test]
fn batch_equals_scalar_single_segment() {
    let data = gen(0xB0A7, 20_000, 2_000, 0.05);
    let eng = build_single(&data);
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_equals_scalar_multi_segment_memtable() {
    let data = gen(0x00C0_FFEE, 20_000, 2_000, 0.05);
    let eng = build_multi(&data);
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_equals_scalar_with_tombstones() {
    let data = gen(0xDEAD, 20_000, 2_000, 0.05);
    let mut eng = build_multi(&data);
    // Delete ~30% by logical id — tombstones across base segments + memtable.
    for (id, _) in data.queries.iter().take(data.queries.len() * 3 / 10) {
        let _ = eng.delete_by_logical_id(*id);
    }
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_equals_scalar_high_broad_fraction() {
    // Heavier broad population stresses the broad lane (more reachable broad
    // queries per batch, more pure-anchor + non-pure mix).
    let data = gen(0x5EED, 15_000, 1_500, 0.30);
    let eng = build_multi(&data);
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_inline_equals_columnar() {
    // Independent of the scalar baseline: the two strategies must agree.
    let data = gen(0xA11CE, 12_000, 1_000, 0.15);
    let eng = build_multi(&data);
    for &bs in &[1usize, 64, 256, 999] {
        let inline = batch_result(
            &eng,
            &data.titles,
            BatchMatchOptions {
                include_broad: true,
                broad_batch_size: bs,
                broad_strategy: BroadStrategy::Inline,
                broad_materialize: true,
            },
        );
        let columnar = batch_result(
            &eng,
            &data.titles,
            BatchMatchOptions {
                include_broad: true,
                broad_batch_size: bs,
                broad_strategy: BroadStrategy::Columnar,
                broad_materialize: true,
            },
        );
        assert_eq!(inline, columnar, "Inline != Columnar at batch_size {bs}");
    }
}

#[test]
fn batch_materialize_on_equals_off() {
    // The pure-anchor materialization fast path is a kill-switch: turning it off
    // forces those queries through full bitmap verification, which must yield
    // byte-identical results (only slower). Independent of the scalar baseline.
    let data = gen(0x11_1A7E, 12_000, 1_000, 0.25);
    let eng = build_multi(&data);
    for &bs in &[1usize, 64, 256, 999] {
        let opts = |materialize| BatchMatchOptions {
            include_broad: true,
            broad_batch_size: bs,
            broad_strategy: BroadStrategy::Columnar,
            broad_materialize: materialize,
        };
        let on = batch_result(&eng, &data.titles, opts(true));
        let off = batch_result(&eng, &data.titles, opts(false));
        assert_eq!(on, off, "materialize on != off at batch_size {bs}");
    }
}

#[test]
fn batch_empty_and_singleton() {
    let data = gen(0xE3, 5_000, 500, 0.1);
    let eng = build_single(&data);

    // Empty batch: no panic, empty result.
    let empty: Vec<String> = Vec::new();
    let r = eng.snapshot().match_titles_batch(
        &empty,
        BatchMatchOptions {
            include_broad: true,
            ..Default::default()
        },
    );
    assert!(r.is_empty());

    // Singleton batch equals scalar for that one title.
    let one = vec![data.titles[0].clone()];
    assert_equiv(&eng, &one, true, 256, BroadStrategy::Columnar, true);
    assert_equiv(&eng, &one, true, 1, BroadStrategy::Columnar, true);
}

#[test]
fn batch_size_never_changes_results() {
    // The same corpus at wildly different batch sizes must yield identical
    // per-title results (batch size is a performance knob, never a semantic one).
    let data = gen(0x1234, 10_000, 1_000, 0.2);
    let eng = build_multi(&data);
    let reference = batch_result(
        &eng,
        &data.titles,
        BatchMatchOptions {
            include_broad: true,
            broad_batch_size: 256,
            broad_strategy: BroadStrategy::Columnar,
            broad_materialize: true,
        },
    );
    for &bs in &[1usize, 3, 64, 65, 1000, 5000] {
        let other = batch_result(
            &eng,
            &data.titles,
            BatchMatchOptions {
                include_broad: true,
                broad_batch_size: bs,
                broad_strategy: BroadStrategy::Columnar,
                broad_materialize: true,
            },
        );
        assert_eq!(other, reference, "results changed at batch_size {bs}");
    }
}

// ---- Filtered percolation (ADR-049): the columnar batch path must apply the SAME tag
// filter as the scalar path, including the pure-anchor materialization fast path. ----

const FILTER_CATS: [&str; 4] = ["cards", "coins", "stamps", "comics"];

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

#[test]
fn batch_equals_scalar_under_tag_filter_including_materialized_pure_anchors() {
    // A high broad fraction so the columnar broad lane (and its pure-anchor
    // materialization fast path) is well exercised.
    let data = gen(0x00F1_17E5, 24_000, 2_500, 0.18);
    let eng = build_single_tagged(&data);

    let filters: [Vec<(String, Vec<String>)>; 3] = [
        vec![("category".to_string(), vec!["cards".to_string()])],
        vec![(
            "category".to_string(),
            vec!["cards".to_string(), "coins".to_string()],
        )],
        // a value never ingested ⇒ ∅ on both paths
        vec![("category".to_string(), vec!["nonexistent".to_string()])],
    ];

    let mut saw_nonempty = false;
    for filter in &filters {
        // `materialize` on AND off — `true` drives the pure-anchor fast path that the
        // Step-5 fix had to teach to honor the filter.
        for &materialize in &[true, false] {
            let scalar = scalar_filtered(&eng, &data.titles, filter);
            let batch = batch_filtered(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad: true,
                    broad_batch_size: 128,
                    broad_strategy: BroadStrategy::Columnar,
                    broad_materialize: materialize,
                },
                filter,
            );
            assert_eq!(
                scalar, batch,
                "batch ≠ scalar under filter {filter:?} (materialize={materialize})"
            );
            if scalar.iter().any(|r| !r.is_empty()) {
                saw_nonempty = true;
            }
        }
    }
    assert!(saw_nonempty, "degenerate: no filter matched anything");
}
