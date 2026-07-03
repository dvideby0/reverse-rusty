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
fn batch_equals_scalar_with_class_d_lane() {
    // Class-D always-candidates (ADR-068) ride the broad lane under the
    // universal signature: the batch kernel probes it ONCE per batch, the scalar
    // path once per title — the full matrix (columnar/inline × materialize ×
    // batch sizes × broad on/off) must stay byte-identical with them stored.
    use reverse_rusty::config::EngineConfig;
    use reverse_rusty::gen::gen_class_d_queries;
    let data = gen(0xD1A5, 12_000, 1_200, 0.10);
    let mut eng = Engine::with_config(
        Normalizer::default_vocab().expect("vocab"),
        EngineConfig {
            accept_class_d: true,
            ..EngineConfig::default()
        },
    );
    let n = data.queries.len();
    let c = n / 4;
    eng.build_from_queries(&data.queries[..c]);
    eng.bulk_ingest(&data.queries[c..2 * c]);
    eng.bulk_ingest(&data.queries[2 * c..3 * c]);
    for (id, text) in &data.queries[3 * c..] {
        eng.insert_live(text, *id, 1);
    }
    // Negation-only queries across every layout: sealed base segments AND the
    // live memtable tail.
    for (i, q) in gen_class_d_queries(0xD1A5_D00D, 150).iter().enumerate() {
        eng.insert_live(q, 2_000_000 + i as u64, 1);
    }
    eng.flush();
    for (i, q) in gen_class_d_queries(0xD1A5_BEEF, 150).iter().enumerate() {
        eng.insert_live(q, 3_000_000 + i as u64, 1);
    }
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
                broad_prefilter: true,
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
                broad_prefilter: true,
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
            broad_prefilter: true,
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
    assert_equiv(&eng, &one, true, 256, BroadStrategy::Columnar, true, true);
    assert_equiv(&eng, &one, true, 1, BroadStrategy::Columnar, true, true);
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
            broad_prefilter: true,
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
                broad_prefilter: true,
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
                    broad_prefilter: true,
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

#[test]
fn prefilter_on_equals_off_and_bites() {
    let (queries, titles) = prefilter_corpus();
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    let opts = |bs: usize, prefilter: bool| BatchMatchOptions {
        include_broad: true,
        broad_batch_size: bs,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: true,
        broad_prefilter: prefilter,
    };

    // Results identical across the sweep, prefilter on == off == scalar.
    for &bs in &[1usize, 7, 64, 256] {
        let on = batch_result(&eng, &titles, opts(bs, true));
        let off = batch_result(&eng, &titles, opts(bs, false));
        assert_eq!(on, off, "prefilter changed results at batch_size {bs}");
        let scalar = scalar_baseline(&eng, &titles, true);
        assert_eq!(on, scalar, "batch != scalar at batch_size {bs}");
    }
    // The both-group titles must actually match (the corpus is not degenerate, and
    // the prefilter did not over-reject the satisfiable shape).
    let on = batch_result(&eng, &titles, opts(1, true));
    assert!(
        on[40..].iter().all(|r| r.iter().any(|&id| id < 24)),
        "a both-group title lost its class-C matches"
    );
    assert!(
        on[..40].iter().all(|r| r.iter().all(|&id| id >= 1_000)),
        "an anchor-only title matched a two-group query"
    );

    // The meter: per-title batches make every anchor-only title a gamma/delta-free
    // batch, so the skip fires; off => the counter is structurally zero.
    let stats_on = eng.match_titles_batch_stats(&titles, opts(1, true));
    assert!(
        stats_on.broad_prefilter_skipped > 0,
        "prefilter never fired on the shape built to trigger it"
    );
    let stats_off = eng.match_titles_batch_stats(&titles, opts(1, false));
    assert_eq!(stats_off.broad_prefilter_skipped, 0, "off must never skip");
    // Skipping only ever removes full bitmap evaluations, never candidates.
    assert!(stats_on.broad_queries_evaluated < stats_off.broad_queries_evaluated);
    assert_eq!(stats_on.broad_candidates, stats_off.broad_candidates);
}

#[test]
fn prefilter_never_skips_class_d() {
    // A class-D always-candidate has EMPTY positives — the count-gate's clauses all
    // pass vacuously, so it can never be prefilter-skipped (skipping it would be
    // gating on MUST_NOT). Lane-on corpus of negation-only queries: the counter must
    // stay zero and every title without the forbidden token must keep its matches.
    let cfg = reverse_rusty::config::EngineConfig {
        accept_class_d: true,
        ..Default::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    let queries: Vec<(u64, String)> = (0..12u64)
        .map(|i| (i, format!("-junktoken{}", i % 3)))
        .collect();
    eng.build_from_queries(&queries);

    let titles: Vec<String> = (0..20)
        .map(|i| {
            if i % 4 == 0 {
                format!("clean listing junktoken0 number {i}")
            } else {
                format!("clean listing number {i}")
            }
        })
        .collect();

    let opts = |prefilter: bool| BatchMatchOptions {
        include_broad: true,
        broad_batch_size: 4,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: true,
        broad_prefilter: prefilter,
    };
    let on = batch_result(&eng, &titles, opts(true));
    let off = batch_result(&eng, &titles, opts(false));
    let scalar = scalar_baseline(&eng, &titles, true);
    assert_eq!(on, off, "prefilter changed class-D results");
    assert_eq!(on, scalar, "batch != scalar on the class-D corpus");
    assert!(
        on.iter().any(|r| !r.is_empty()),
        "degenerate: no class-D query matched"
    );

    let stats = eng.match_titles_batch_stats(&titles, opts(true));
    assert_eq!(
        stats.broad_prefilter_skipped, 0,
        "a class-D always-candidate was prefilter-skipped"
    );
}

// ---- The hot tier (class H, ADR-105): batch ≡ scalar with the always-visible,
// columnar-evaluated tier in play — including the load-bearing broad-OFF cell
// (the hot columnar pass must run and agree even when the broad lane is off),
// the Inline kill-switch, the materialize (vacuous-accept) kill-switch on the
// tail-anchored population, and the ADR-061 multi-word-alias forced-inline path. ----

/// θ small enough that the generated corpus's Zipf-head players classify H at
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

#[test]
fn batch_equals_scalar_with_hot_tier() {
    let data = gen(0x0407_B0A7, 20_000, 2_000, 0.05);
    let eng = build_multi_hot(&data);
    run_matrix(&eng, &data.titles);
}

#[test]
fn hot_inline_equals_columnar() {
    // The shared kill-switch: BroadStrategy::Inline forces the hot tier through
    // the per-title match_into path; results must equal the columnar pass.
    let data = gen(0x0407_A11C, 20_000, 1_000, 0.05);
    let eng = build_multi_hot(&data);
    for &bs in &[1usize, 64, 256, 999] {
        for include_broad in [false, true] {
            let inline = batch_result(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad,
                    broad_batch_size: bs,
                    broad_strategy: BroadStrategy::Inline,
                    broad_materialize: true,
                    broad_prefilter: true,
                },
            );
            let columnar = batch_result(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad,
                    broad_batch_size: bs,
                    broad_strategy: BroadStrategy::Columnar,
                    broad_materialize: true,
                    broad_prefilter: true,
                },
            );
            assert_eq!(
                inline, columnar,
                "hot Inline != Columnar (bs={bs}, include_broad={include_broad})"
            );
        }
    }
}

#[test]
fn hot_materialize_on_equals_off_and_fast_path_fires() {
    // The vacuous accept for class H rides `pure_tail_anchor` (a θ-hot anchor
    // has NO mask bit, so `is_pure_anchor` is structurally false for it — the
    // trap this test exists to catch): materialize on ≡ off, AND the on-run
    // provably evaluates fewer queries through the full bitmap path.
    let data = gen(0x0407_3A7E, 20_000, 1_000, 0.05);
    let eng = build_multi_hot(&data);
    let opts = |materialize| BatchMatchOptions {
        include_broad: false, // isolate the hot lane
        broad_batch_size: 256,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: materialize,
        broad_prefilter: true,
    };
    let on = batch_result(&eng, &data.titles, opts(true));
    let off = batch_result(&eng, &data.titles, opts(false));
    assert_eq!(on, off, "hot materialize on != off");

    let st_on = eng.match_titles_batch_stats(&data.titles, opts(true));
    let st_off = eng.match_titles_batch_stats(&data.titles, opts(false));
    assert!(
        st_on.hot_queries_evaluated < st_off.hot_queries_evaluated,
        "the tail-anchored vacuous accept never fired \
         (on={} off={} — is_pure_anchor vs pure_tail_anchor trap?)",
        st_on.hot_queries_evaluated,
        st_off.hot_queries_evaluated
    );
    assert!(st_on.hot_batches > 0, "hot columnar pass never ran");
}

#[test]
fn hot_multiword_alias_forced_inline() {
    // An active multi-word alias forces the single-view columnar kernel off
    // (ADR-061); the hot tier must ride the same forced-inline path and still
    // equal the scalar baseline on both visibility modes.
    let data = gen(0x0407_A11A, 20_000, 800, 0.05);
    let mut eng = build_multi_hot(&data);
    // Activate a declared multi-word alias (the ADR-061 two-view trigger): the
    // Solr import path auto-activates it and recompiles the stored queries.
    eng.import_alias_synonyms("ny => new york")
        .expect("import + apply aliases");
    assert!(
        eng.class_counts()[4] > 0,
        "the alias recompile lost the class-H population"
    );

    for include_broad in [false, true] {
        let scalar = scalar_baseline(&eng, &data.titles, include_broad);
        for &bs in &[1usize, 64, 256] {
            let batch = batch_result(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad,
                    broad_batch_size: bs,
                    broad_strategy: BroadStrategy::Columnar,
                    broad_materialize: true,
                    broad_prefilter: true,
                },
            );
            assert_eq!(
                batch, scalar,
                "alias-forced-inline hot batch != scalar (bs={bs}, broad={include_broad})"
            );
        }
    }
}
