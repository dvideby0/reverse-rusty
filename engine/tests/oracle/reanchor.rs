//! Compaction-that-improves (ADR-056): re-anchoring differential oracle.

use crate::harness::*;
use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
use std::collections::HashSet;

/// Compaction-that-improves (ADR-056): re-anchoring a query whose feature frequencies
/// drifted must produce EXACTLY the same matches. A controlled corpus drives a guaranteed
/// anchor flip (so we can assert re-anchoring actually fired, not silently no-op'd), and
/// the title sweep + brute oracle prove the flip is lossless for every query shape
/// (class-A flip, any-of, forbidden, broad pure-anchor).
#[test]
fn compaction_reanchoring_preserves_correctness() {
    use reverse_rusty::EngineConfig;

    // Re-anchoring ON; manual compaction so we control timing.
    let cfg = EngineConfig {
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        compaction_reanchor: true,
        ..EngineConfig::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("built-in vocab"), cfg);

    // --- Build corpus (pass A bumps freq, then the mask is frozen). 70 "filler" features
    //     at build-frequency 3 occupy the top-64, so anything appearing fewer than 3 times
    //     at build can never be hot — pinning `alpha`/`bravo` as NON-hot for the life of the
    //     engine even after their frequency later drifts. ---
    let mut build: Vec<(u64, String)> = Vec::new();
    let mut id = 0u64;
    for k in 0..70u64 {
        for r in 0..3u64 {
            build.push((id, format!("fz{k} pad{k}x{r}")));
            id += 1;
        }
    }
    // Watched query: required {alpha, bravo}. At build alpha(freq 1) is rarer than
    // bravo(freq 2), so its anchor is the arity-1 sig on `alpha` (class A).
    let watched = id;
    build.push((watched, "alpha bravo".to_string()));
    id += 1;
    build.push((id, "bravo loner".to_string())); // a 2nd bravo so bravo(2) > alpha(1) at build
    id += 1;
    let q_anyof = id;
    build.push((q_anyof, "(red,blue,green) widget".to_string()));
    id += 1;
    let q_forbidden = id;
    build.push((q_forbidden, "gadget -broken".to_string()));
    id += 1;
    let q_broad = id; // single hot feature (`fz0`) -> class C broad, pure-anchor
    build.push((q_broad, "fz0".to_string()));

    eng.build_from_queries(&build); // base segment 0; finalizes the frozen mask

    // --- Drift: pile frequency onto `alpha` so it overtakes `bravo`. The mask is frozen,
    //     so `alpha` stays non-hot; only the rarest-required ORDER changes for the watched
    //     query, which must flip its anchor from `alpha` to `bravo` on the next compaction. ---
    for d in 0..20u64 {
        eng.insert_live(&format!("alpha driftpad{d}"), 10_000 + d, 1);
    }
    eng.flush(); // base segment 1 (now freq(alpha)=21 > freq(bravo)=2)
    assert!(
        eng.num_segments() > 2,
        "need multiple base segments to compact"
    );

    // Titles exercising each query shape.
    let titles = [
        "alpha bravo extra",  // matches watched (both required present)
        "alpha alone",        // must NOT match watched (no bravo)
        "blue widget here",   // matches any-of
        "gadget ok",          // matches forbidden-bearing query (no `broken`)
        "gadget broken unit", // must NOT match (forbidden `broken` present)
        "fz0 whatever",       // matches broad query
        "unrelated title text",
    ];

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let pre: Vec<HashSet<u64>> = titles
        .iter()
        .map(|t| {
            eng.match_title(t, &mut s, &mut out, true);
            out.iter().copied().collect()
        })
        .collect();

    // --- Compact with re-anchoring; it MUST fire. ---
    let report = eng.compact_all().expect("compaction ran");
    eprintln!(
        "reanchor compaction: merged={} before={} after={} reanchored={}",
        report.segments_merged, report.entries_before, report.entries_after, report.reanchored
    );
    assert!(
        report.reanchored > 0,
        "re-anchoring did not fire — the drifted watched query should have flipped its anchor"
    );

    // Pre == post for every title (zero FN and zero FP vs the pre-compaction engine).
    for (i, t) in titles.iter().enumerate() {
        eng.match_title(t, &mut s, &mut out, true);
        let post: HashSet<u64> = out.iter().copied().collect();
        assert_eq!(post, pre[i], "re-anchoring changed matches for {t:?}");
    }

    // The watched query specifically: still matched via its NEW anchor when both required
    // features are present, still rejected when only `alpha` is — the flipped cover is lossless.
    eng.match_title("alpha bravo here", &mut s, &mut out, true);
    assert!(
        out.contains(&watched),
        "watched query lost after re-anchor (FALSE NEGATIVE)"
    );
    eng.match_title("alpha here", &mut s, &mut out, true);
    assert!(
        !out.contains(&watched),
        "watched query over-matched after re-anchor (missing-required not enforced)"
    );

    // --- Differential brute-force oracle over the FINAL live set. ---
    let final_set: Vec<(u64, String)> = build
        .iter()
        .cloned()
        .chain((0..20u64).map(|d| (10_000 + d, format!("alpha driftpad{d}"))))
        .collect();
    let brute = Brute::build(&final_set);
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let oracle_titles = [
        "alpha bravo extra",
        "alpha alone",
        "blue widget here",
        "red widget",
        "green widget",
        "gadget ok",
        "gadget broken",
        "fz0 x",
        "fz7 y",
        "nothing relevant",
    ];
    let mut false_neg = 0usize;
    let mut false_pos = 0usize;
    let mut total_truth = 0usize;
    for t in oracle_titles {
        eng.match_title(t, &mut s, &mut out, true);
        let eng_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(t, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for x in &truth {
            if !eng_set.contains(x) {
                false_neg += 1;
            }
        }
        for x in &eng_set {
            if !truth.contains(x) {
                false_pos += 1;
            }
        }
    }
    assert_eq!(false_neg, 0, "re-anchoring introduced FALSE NEGATIVES");
    assert_eq!(false_pos, 0, "re-anchoring introduced false positives");
    assert!(total_truth > 0, "degenerate test: no matches");
}

/// The cluster-safety guarantee (ADR-056): when the dict is FROZEN (frequencies never
/// drift), re-running the anchor optimizer reproduces every cover exactly, so re-anchoring
/// is a guaranteed no-op (`reanchored == 0`) and therefore can never change a query's shard
/// placement or within-shard retrievability. A cluster shard indexes against ONE frozen
/// shared dict and never bumps frequency — modeled here at the segment level by freezing the
/// dict and then compiling segments against it read-only.
#[test]
fn reanchoring_is_a_noop_under_a_frozen_dict() {
    use reverse_rusty::segment::Segment;

    let norm = Normalizer::default_vocab().expect("built-in vocab");
    let cfg = GenConfig {
        num_queries: 6_000,
        num_titles: 1,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xF02E_0056,
        num_players: 700,
        num_sets: 300,
    };
    let data = generate(&cfg);

    // Build + finalize the dict from the corpus (mutating extract). FROZEN after this.
    let mut dict = Dict::new();
    let mut lc = String::new();
    let mut exes: Vec<Extracted> = Vec::new();
    for (_, text) in &data.queries {
        if let Ok(ast) = reverse_rusty::dsl::parse(text) {
            exes.push(extract(&ast, &norm, &mut dict, &mut lc));
        }
    }
    dict.finalize_mask(); // no further bump_freq — frequencies are now fixed

    // Compile two segments against the frozen dict, read-only (the cluster-shard path:
    // `add_compiled` only reads the dict, never bumps frequency).
    let half = exes.len() / 2;
    let mut a = Segment::new();
    let mut b = Segment::new();
    for (i, ex) in exes.iter().enumerate() {
        let seg = if i < half { &mut a } else { &mut b };
        seg.add_compiled(ex, &[], &dict, i as u64, 1, false);
    }
    assert!(
        !a.is_empty() && !b.is_empty(),
        "both segments should hold queries"
    );

    // Re-anchoring against the SAME frozen dict must reproduce every cover exactly.
    let (merged, reanchored) = Segment::compact_from_reanchored(&[&a, &b], &dict);
    assert_eq!(
        merged.len(),
        a.len() + b.len(),
        "no entries lost in the no-op merge"
    );
    assert_eq!(
        reanchored, 0,
        "re-anchoring must be a no-op when the dict (and thus frequencies) is frozen"
    );
}

/// Regression (Codex review): re-anchoring must never demote a main-lane query into the broad
/// lane, which the DEFAULT percolate path (`include_broad = false`) skips. A query compiled
/// before the mask was finalized (`insert_live` on an empty engine → `flush`) sits in main with
/// `req_mask == 0`; a later `bulk_ingest` finalizes the mask and can make that query's sole
/// anchor hot. Re-anchoring it alone would reclassify it class C (broad-only) and hide it on the
/// default path — a false negative. The demote-guard keeps it in main, so it stays findable.
#[test]
fn reanchoring_never_demotes_a_main_query_into_the_broad_lane() {
    use reverse_rusty::EngineConfig;

    let cfg = EngineConfig {
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        compaction_reanchor: true,
        ..EngineConfig::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("built-in vocab"), cfg);

    // 1) insert_live on an EMPTY engine: the mask is not finalized yet, so this single-feature
    //    query is class A in the MAIN index (req_mask == 0).
    let watched = 1u64;
    eng.insert_live("rareword", watched, 1);
    eng.flush(); // seal into base segment 0

    // Baseline: findable on the DEFAULT path (include_broad = false) BEFORE compaction.
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("rareword title", &mut s, &mut out, false);
    assert!(
        out.contains(&watched),
        "precondition: the query must match on the default path before compaction"
    );

    // 2) bulk_ingest a batch that makes `rareword` HOT and finalizes the mask.
    let batch: Vec<(u64, String)> = (0..200u64)
        .map(|i| (1000 + i, format!("rareword filler{i}")))
        .collect();
    eng.bulk_ingest(&batch); // first finalize_mask() — `rareword` is now in the 64-hot set
    assert!(
        eng.num_segments() > 2,
        "need multiple base segments to compact"
    );

    // 3) Compact with re-anchoring. `rareword` is now hot; re-anchoring the watched query alone
    //    would make it class C (broad) — the guard must keep it in the main lane instead.
    let report = eng.compact_all().expect("compaction ran");
    eprintln!("demote-guard test: reanchored={}", report.reanchored);

    // 4) The watched query is STILL findable on the DEFAULT path (include_broad = false).
    eng.match_title("rareword title", &mut s, &mut out, false);
    assert!(
        out.contains(&watched),
        "FALSE NEGATIVE: re-anchoring demoted a main query into the broad lane (default path skips it)"
    );
}

/// Re-anchoring on a realistic, multi-segment corpus (built across several `bulk_ingest`s,
/// so global frequencies drift while the mask stays frozen) preserves matches exactly —
/// per-title AND through the columnar broad-lane batch path over the rebuilt broad index —
/// and stays zero-false-negative against the brute oracle.
#[test]
fn compaction_reanchoring_matches_oracle_at_scale() {
    use reverse_rusty::EngineConfig;

    let gcfg = GenConfig {
        num_queries: 30_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5CA1_E056,
        num_players: 2_500,
        num_sets: 1_000,
    };
    let data = generate(&gcfg);
    let q = &data.queries;
    let n = q.len();
    let chunk = n / 4;

    let cfg = EngineConfig {
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        compaction_reanchor: true,
        ..EngineConfig::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("built-in vocab"), cfg);
    // Each ingest bumps global frequencies (the mask is frozen after the first build), so by
    // compaction time the early segments' anchors have drifted.
    eng.build_from_queries(&q[..chunk]);
    eng.bulk_ingest(&q[chunk..2 * chunk]);
    eng.bulk_ingest(&q[2 * chunk..3 * chunk]);
    eng.bulk_ingest(&q[3 * chunk..]);
    assert!(eng.num_segments() > 2, "need multiple base segments");

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let pre: Vec<HashSet<u64>> = data
        .titles
        .iter()
        .map(|t| {
            eng.match_title(t, &mut s, &mut out, true);
            out.iter().copied().collect()
        })
        .collect();

    let report = eng.compact_all().expect("compaction ran");
    eprintln!(
        "scale reanchor: entries_after={} reanchored={}",
        report.entries_after, report.reanchored
    );

    // Post == pre for every title (zero FN, zero FP vs the pre-compaction engine).
    let mut mismatches = 0usize;
    for (i, t) in data.titles.iter().enumerate() {
        eng.match_title(t, &mut s, &mut out, true);
        let post: HashSet<u64> = out.iter().copied().collect();
        if post != pre[i] {
            mismatches += 1;
        }
    }
    assert_eq!(mismatches, 0, "re-anchoring changed match results at scale");

    // The columnar broad batch path over the RE-ANCHORED broad index must equal scalar.
    let snap = eng.snapshot();
    let opts = BatchMatchOptions {
        include_broad: true,
        broad_batch_size: 256,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: true,
        broad_prefilter: true,
    };
    let mut batch_sets: Vec<HashSet<u64>> = vec![HashSet::new(); data.titles.len()];
    for (idx, ids) in snap.match_titles_batch(&data.titles, opts) {
        batch_sets[idx] = ids.into_iter().collect();
    }
    let mut batch_mismatches = 0usize;
    for (i, t) in data.titles.iter().enumerate() {
        eng.match_title(t, &mut s, &mut out, true);
        let scalar: HashSet<u64> = out.iter().copied().collect();
        if batch_sets[i] != scalar {
            batch_mismatches += 1;
        }
    }
    assert_eq!(
        batch_mismatches, 0,
        "batch != scalar over the re-anchored segment"
    );

    // Differential brute oracle (zero false negatives) over the full title set.
    let brute = Brute::build(q);
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut total_truth = 0usize;
    for t in &data.titles {
        eng.match_title(t, &mut s, &mut out, true);
        let eng_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(t, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for x in &truth {
            if !eng_set.contains(x) {
                false_neg += 1;
            }
        }
    }
    assert_eq!(
        false_neg, 0,
        "re-anchoring introduced FALSE NEGATIVES at scale"
    );
    assert!(total_truth > 0, "degenerate test");
}
