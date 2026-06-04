//! Differential oracle: the CONTRACT verification.
//!
//! For a synthetic dataset, compute ground truth with a brute-force matcher
//! (check every query's extracted features against every title) and compare to
//! the engine's output. We assert:
//!   * ZERO false negatives  (every true match is returned)  <-- the hard requirement
//!   * ZERO false positives  (the exact matcher is exact)
//!
//! The brute-force side uses its own Dict/Normalizer *instances* and independently
//! reimplements candidate retrieval + exact verification — so an index / retrieval /
//! verify bug can't hide here. It does NOT independently verify the FRONT END: it calls
//! the engine's own `dsl::parse`, `compile::extract`, and `Normalizer` (and, except in
//! `zero_false_negatives_with_populated_vocab`, the empty `default_vocab`). The parser,
//! extractor, and normalization-model semantics are pinned instead by the spec-authored
//! golden tests in `src/{dsl,normalize,compile}.rs` (`mod golden`). See DECISIONS.md ADR-050.

use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::{Dict, FeatureKind};
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::{Normalizer, NormalizerBuilder};
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
use std::collections::HashSet;

/// Independent ground-truth matcher over extracted queries.
struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    fn build(queries: &[(u64, String)]) -> Self {
        Self::build_with(
            queries,
            Normalizer::default_vocab().expect("built-in vocab"),
        )
    }

    /// Build the brute reference with an explicit normalizer vocabulary. The default
    /// `build` uses the empty `default_vocab` (so the phrase/synonym/grader paths are
    /// never exercised); `zero_false_negatives_with_populated_vocab` passes a populated
    /// one so they are. See docs/DECISIONS.md ADR-050.
    fn build_with(queries: &[(u64, String)], norm: Normalizer) -> Self {
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                // mirror the engine's class-D rejection: no required & no anyof
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue;
                }
                qs.push((*logical, ex));
            }
        }
        dict.finalize_mask();
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    fn matches(&self, title: &str, lc: &mut String, feats: &mut Vec<u32>) -> HashSet<u64> {
        self.norm.match_features(title, &self.dict, lc, feats);
        let present = |f: u32| feats.binary_search(&f).is_ok();
        let mut out = HashSet::new();
        for (logical, ex) in &self.queries {
            if ex.required.iter().all(|&f| present(f))
                && !ex.forbidden.iter().any(|&f| present(f))
                && ex.anyof.iter().all(|g| g.iter().any(|&f| present(f)))
            {
                out.insert(*logical);
            }
        }
        out
    }
}

#[test]
fn zero_false_negatives_against_oracle() {
    let cfg = GenConfig {
        num_queries: 40_000,
        num_titles: 4_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x00AB_CDEF,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);

    // engine
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&data.queries);

    // oracle
    let brute = Brute::build(&data.queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut total_truth = 0usize;
    let mut total_engine = 0usize;
    let mut false_neg = 0usize;
    let mut false_pos = 0usize;

    for title in &data.titles {
        eng.match_title(title, &mut s, &mut out, true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);

        total_truth += truth.len();
        total_engine += engine_set.len();

        for t in &truth {
            if !engine_set.contains(t) {
                false_neg += 1;
            }
        }
        for e in &engine_set {
            if !truth.contains(e) {
                false_pos += 1;
            }
        }
    }

    eprintln!(
        "oracle: truth_matches={total_truth} engine_matches={total_engine} false_neg={false_neg} false_pos={false_pos}"
    );
    assert_eq!(false_neg, 0, "FALSE NEGATIVES detected — contract violated");
    assert_eq!(false_pos, 0, "false positives — exact matcher is not exact");
    assert!(total_truth > 0, "degenerate test: no matches at all");
}

// ---- Filtered percolation (ADR-049) differential oracle ----

const CATEGORIES: [&str; 6] = ["cards", "coins", "stamps", "comics", "toys", "art"];
const STATUSES: [&str; 3] = ["active", "inactive", "archived"];

/// Deterministic per-query tags, a pure function of the logical id so the engine and the
/// brute reference assign identical metadata with no shared state.
fn tags_for(logical: u64) -> Vec<(String, String)> {
    let cat = CATEGORIES[(logical % CATEGORIES.len() as u64) as usize];
    let status = STATUSES[((logical / 7) % STATUSES.len() as u64) as usize];
    vec![
        ("category".to_string(), cat.to_string()),
        ("status".to_string(), status.to_string()),
    ]
}

/// Reference filter semantics: AND across keys, OR within a key's value set.
fn passes_filter(qtags: &[(String, String)], filter: &[(String, Vec<String>)]) -> bool {
    filter.iter().all(|(k, vals)| {
        qtags
            .iter()
            .any(|(qk, qv)| qk == k && vals.iter().any(|v| v == qv))
    })
}

/// A small deterministic sweep of filters keyed off `i` — single category (the dominant
/// production pattern), a two-value category set, category+status, and a category value
/// that was never ingested (must return ∅).
fn filters_for(i: usize) -> Vec<Vec<(String, Vec<String>)>> {
    let c1 = CATEGORIES[i % CATEGORIES.len()].to_string();
    let c2 = CATEGORIES[(i + 1) % CATEGORIES.len()].to_string();
    let st = STATUSES[i % STATUSES.len()].to_string();
    vec![
        vec![("category".to_string(), vec![c1.clone()])],
        vec![("category".to_string(), vec![c1.clone(), c2])],
        vec![
            ("category".to_string(), vec![c1]),
            ("status".to_string(), vec![st]),
        ],
        vec![("category".to_string(), vec!["never-ingested".to_string()])],
    ]
}

#[test]
fn filtered_percolation_matches_oracle_and_only_removes() {
    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0049_0049,
        num_players: 2_500,
        num_sets: 1_000,
    };
    let data = generate(&cfg);

    // engine, built WITH per-query tags (parallel to data.queries)
    let tags: Vec<Vec<(String, String)>> = data.queries.iter().map(|(l, _)| tags_for(*l)).collect();
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.try_build_from_queries_with_tags(&data.queries, &tags)
        .expect("tagged build");
    let snap = eng.snapshot();

    let brute = Brute::build(&data.queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut checked = 0usize;
    let mut nonempty_filtered = 0usize;
    for (ti, title) in data.titles.iter().enumerate() {
        // unfiltered baseline (engine + truth)
        let unfiltered: HashSet<u64> = {
            snap.match_title(title, &mut s, &mut out, true);
            out.iter().copied().collect()
        };
        let truth = brute.matches(title, &mut blc, &mut bfeats);

        for filter in filters_for(ti) {
            let pred = snap.compile_tag_predicate(&filter);
            snap.match_title_filtered(title, &mut s, &mut out, true, &pred);
            let engine_filtered: HashSet<u64> = out.iter().copied().collect();

            // reference = brute matches that also satisfy the tag filter
            let brute_filtered: HashSet<u64> = truth
                .iter()
                .copied()
                .filter(|l| passes_filter(&tags_for(*l), &filter))
                .collect();

            assert_eq!(
                engine_filtered, brute_filtered,
                "filtered set diverged from oracle (title {ti}, filter {filter:?})"
            );

            // monotonicity: filtering only ever REMOVES, never adds or drops a wanted
            // in-scope match. Every removed id must itself fail the filter.
            assert!(
                engine_filtered.is_subset(&unfiltered),
                "filter added a match not in the unfiltered set"
            );
            for removed in unfiltered.difference(&engine_filtered) {
                assert!(
                    !passes_filter(&tags_for(*removed), &filter),
                    "filter removed id {removed} that actually satisfies it (false negative)"
                );
            }
            checked += 1;
            if !engine_filtered.is_empty() {
                nonempty_filtered += 1;
            }
        }
    }
    eprintln!("filtered oracle: {checked} (title,filter) pairs, {nonempty_filtered} non-empty");
    assert!(
        nonempty_filtered > 0,
        "degenerate: no filter ever matched anything"
    );
}

/// The columnar BATCH path (`match_titles_batch`) must ALSO satisfy the contract
/// against the INDEPENDENT brute-force oracle — not merely agree with the per-title
/// path (that equivalence is `tests/broad_batch.rs`). Multi-segment + memtable so
/// the batch broad lane unions reachable broad queries across every segment.
/// Additive: the per-title oracle above is untouched.
#[test]
fn batch_path_zero_false_negatives_against_oracle() {
    let cfg = GenConfig {
        num_queries: 40_000,
        num_titles: 4_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0BA7_C0DE,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);

    // Multi-segment engine: base segments + an unflushed memtable tail.
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let n = data.queries.len();
    let c = n / 4;
    eng.build_from_queries(&data.queries[..c]);
    eng.bulk_ingest(&data.queries[c..2 * c]);
    eng.bulk_ingest(&data.queries[2 * c..3 * c]);
    for (id, text) in &data.queries[3 * c..] {
        eng.insert_live(text, *id, 1);
    }

    let brute = Brute::build(&data.queries);

    // Columnar batch path, broad ON.
    let snap = eng.snapshot();
    let results = snap.match_titles_batch(
        &data.titles,
        BatchMatchOptions {
            include_broad: true,
            broad_batch_size: 256,
            broad_strategy: BroadStrategy::Columnar,
            broad_materialize: true,
        },
    );
    let mut per_title: Vec<HashSet<u64>> = vec![HashSet::new(); data.titles.len()];
    for (idx, ids) in results {
        per_title[idx] = ids.into_iter().collect();
    }

    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut total_truth = 0usize;
    let mut false_neg = 0usize;
    let mut false_pos = 0usize;
    for (ti, title) in data.titles.iter().enumerate() {
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        let got = &per_title[ti];
        total_truth += truth.len();
        for t in &truth {
            if !got.contains(t) {
                false_neg += 1;
            }
        }
        for g in got {
            if !truth.contains(g) {
                false_pos += 1;
            }
        }
    }
    eprintln!(
        "batch oracle: truth_matches={total_truth} false_neg={false_neg} false_pos={false_pos}"
    );
    assert_eq!(
        false_neg, 0,
        "batch path FALSE NEGATIVES detected — contract violated"
    );
    assert_eq!(
        false_pos, 0,
        "batch path false positives — exact matcher not exact"
    );
    assert!(total_truth > 0, "degenerate test: no matches at all");
}

/// Multi-segment path must produce EXACTLY the same matches as a single
/// from-scratch build over the final live query set — proving build_from_queries
/// + bulk_ingest + flush + insert_live/tombstone compose losslessly.
#[test]
fn multi_segment_identical_to_single_build() {
    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 3_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5E6_3A77,
        num_players: 2_500,
        num_sets: 1_000,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let n = q.len();
    assert!(n > 5_000, "need a sizeable corpus");

    // Partition: initial build batch, 3 bulk_ingest batches, plus a set of
    // "updates" (re-add some EXISTING logical ids with new text).
    let n_init = n / 3;
    let rest = &q[n_init..];
    let chunk = rest.len() / 3;
    let b0 = &rest[..chunk];
    let b1 = &rest[chunk..2 * chunk];
    let b2 = &rest[2 * chunk..];

    // pick some logical ids from the initial batch to update with new text
    let new_text =
        "1994 upper deck michael jordan sp preview psa 10 -(auto,signed,sgc,bgs)".to_string();
    let mut updates: Vec<(u64, String)> = Vec::new();
    let mut i = 7usize;
    while i < n_init && updates.len() < 200 {
        let logical = q[i].0;
        updates.push((logical, new_text.clone()));
        i += 53;
    }
    let updated_ids: HashSet<u64> = updates.iter().map(|(l, _)| *l).collect();

    // ---- multi-segment engine ----
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&q[..n_init]); // base segment 0
    eng.bulk_ingest(b0); // base segment 1
    eng.bulk_ingest(b1); // base segment 2
                         // exercise the memtable + flush in the middle of the lifecycle
    for (logical, text) in &updates {
        // tombstone the old copy of this logical id (it lives in base segment 0)
        // by finding its local id is non-trivial across segments, so instead we
        // insert the new version and tombstone the OLD one we just superseded.
        let _ = eng.insert_live(text, *logical, 2);
    }
    eng.flush(); // seal the updates' memtable into a base segment
    eng.bulk_ingest(b2); // base segment after flush
                         // a second round of live updates that stay in the (new) memtable, unflushed
    let mut mt_old: Vec<u32> = Vec::new();
    for (logical, text) in updates.iter().take(50) {
        if let Some(local) = eng.insert_live(text, *logical, 3) {
            mt_old.push(local);
        }
    }
    // We now need the OLD copies (original text) of every updated id removed.
    // The originals live in base segment 0; tombstone them there. Find each
    // updated id's local position in segment 0 by rebuilding the mapping: in
    // build_from_queries, queries are added in order (skipping class-D), so we
    // tombstone via logical-id scan using a helper below.
    tombstone_originals(&mut eng, &q[..n_init], &updated_ids);

    // ---- reference engine: single build over FINAL live set ----
    // final live set = every original query, but updated ids use the NEW text.
    let mut final_set: Vec<(u64, String)> = Vec::with_capacity(n);
    for (logical, text) in q {
        if updated_ids.contains(logical) {
            final_set.push((*logical, new_text.clone()));
        } else {
            final_set.push((*logical, text.clone()));
        }
    }
    let mut reference = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    reference.build_from_queries(&final_set);

    // brute-force oracle over the same final live set (zero-false-negative check)
    let brute = Brute::build(&final_set);

    let mut s_eng = MatchScratch::new();
    let mut s_ref = MatchScratch::new();
    let mut out_eng = Vec::new();
    let mut out_ref = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut total_truth = 0usize;
    let mut mismatches = 0usize;
    let mut false_neg = 0usize;

    for title in &data.titles {
        eng.match_title(title, &mut s_eng, &mut out_eng, true);
        reference.match_title(title, &mut s_ref, &mut out_ref, true);
        let set_eng: HashSet<u64> = out_eng.iter().copied().collect();
        let set_ref: HashSet<u64> = out_ref.iter().copied().collect();

        if set_eng != set_ref {
            mismatches += 1;
            if mismatches <= 3 {
                eprintln!(
                    "MISMATCH on {:?}\n  multi-seg only: {:?}\n  reference only: {:?}",
                    title,
                    set_eng.difference(&set_ref).collect::<Vec<_>>(),
                    set_ref.difference(&set_eng).collect::<Vec<_>>(),
                );
            }
        }

        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !set_eng.contains(t) {
                false_neg += 1;
            }
        }
    }

    eprintln!(
        "multi-seg test: segments={} updates={} truth_matches={} mismatches={} false_neg={}",
        eng.num_segments(),
        updated_ids.len(),
        total_truth,
        mismatches,
        false_neg
    );
    let _ = mt_old;
    assert_eq!(
        mismatches, 0,
        "multi-segment engine returned a DIFFERENT match set than a single from-scratch build"
    );
    assert_eq!(
        false_neg, 0,
        "multi-segment engine has FALSE NEGATIVES vs brute-force oracle"
    );
    assert!(total_truth > 0, "degenerate test: no matches at all");
}

/// Tombstone the ORIGINAL (pre-update) copies of `updated_ids` that live in the
/// first base segment. Reconstructs segment-0 local ids by replaying the build
/// order (queries added in order, class-D skipped) with an independent matcher.
fn tombstone_originals(eng: &mut Engine, build_batch: &[(u64, String)], updated: &HashSet<u64>) {
    let norm = Normalizer::default_vocab().expect("built-in vocab");
    let mut dict = Dict::new();
    let mut lc = String::new();
    let mut local: u32 = 0;
    for (logical, text) in build_batch {
        if let Ok(ast) = reverse_rusty::dsl::parse(text) {
            let ex = extract(&ast, &norm, &mut dict, &mut lc);
            // mirror class-D rejection (these are NOT assigned a local id)
            if ex.required.is_empty() && ex.anyof.is_empty() {
                continue;
            }
            if updated.contains(logical) {
                eng.tombstone_in(0, local).unwrap();
            }
            local += 1;
        }
    }
}

/// Compaction must produce EXACTLY the same matches as the pre-compacted engine.
/// Builds a multi-segment engine with updates and tombstones, compacts it, and
/// verifies the compacted engine matches both the pre-compaction engine and the
/// brute-force oracle. This is the core correctness test for compaction: it
/// proves that `Segment::compact_from` preserves every alive entry's exact data
/// and signature postings, drops only tombstoned entries, and introduces no
/// false negatives or false positives.
#[test]
fn compaction_preserves_correctness() {
    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 3_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xC0_AC7,
        num_players: 2_500,
        num_sets: 1_000,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let n = q.len();

    // Build 5 base segments + some tombstones (simulating update churn)
    let chunk = n / 5;
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&q[..chunk]); // segment 0
    eng.bulk_ingest(&q[chunk..2 * chunk]); // segment 1
    eng.bulk_ingest(&q[2 * chunk..3 * chunk]); // segment 2
    eng.bulk_ingest(&q[3 * chunk..4 * chunk]); // segment 3
    eng.bulk_ingest(&q[4 * chunk..]); // segment 4

    // Simulate updates: re-insert some queries with new text, tombstone originals
    let new_text = "1994 upper deck michael jordan sp preview psa 10 -(auto,signed)".to_string();
    let mut updated_ids: HashSet<u64> = HashSet::new();
    let mut i = 11usize;
    while i < chunk && updated_ids.len() < 150 {
        updated_ids.insert(q[i].0);
        i += 41;
    }
    for &logical in &updated_ids {
        let _ = eng.insert_live(&new_text, logical, 2);
    }
    eng.flush(); // seal updates into a 6th base segment
    tombstone_originals(&mut eng, &q[..chunk], &updated_ids);

    let pre_compact_segments = eng.num_segments();
    assert!(
        pre_compact_segments > 4,
        "need multiple segments to test compaction"
    );

    // Snapshot pre-compaction matches
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut pre_matches: Vec<HashSet<u64>> = Vec::with_capacity(data.titles.len());
    for title in &data.titles {
        eng.match_title(title, &mut s, &mut out, true);
        pre_matches.push(out.iter().copied().collect());
    }

    // COMPACT — merge all base segments into one
    let report = eng.compact_all();
    assert!(report.is_some(), "compaction should have run");
    let report = report.unwrap();
    eprintln!(
        "compaction: merged={} segs, before={} entries, after={} entries, reclaimed={} tombstones",
        report.segments_merged,
        report.entries_before,
        report.entries_after,
        report.tombstones_reclaimed
    );
    assert!(
        report.tombstones_reclaimed > 0,
        "should have reclaimed some tombstones"
    );
    // base segments collapsed to 1 + memtable = 2
    assert_eq!(
        eng.num_segments(),
        2,
        "post-compact should be 1 base + 1 memtable"
    );

    // Verify post-compaction matches are identical to pre-compaction
    let mut post_mismatches = 0usize;
    for (ti, title) in data.titles.iter().enumerate() {
        eng.match_title(title, &mut s, &mut out, true);
        let post_set: HashSet<u64> = out.iter().copied().collect();
        if post_set != pre_matches[ti] {
            post_mismatches += 1;
            if post_mismatches <= 3 {
                eprintln!(
                    "MISMATCH on {:?}\n  pre-compact only: {:?}\n  post-compact only: {:?}",
                    title,
                    pre_matches[ti].difference(&post_set).collect::<Vec<_>>(),
                    post_set.difference(&pre_matches[ti]).collect::<Vec<_>>(),
                );
            }
        }
    }
    assert_eq!(post_mismatches, 0, "compaction changed match results");

    // Also verify against brute-force oracle (zero false negatives)
    let mut final_set: Vec<(u64, String)> = Vec::with_capacity(n);
    for (logical, text) in q {
        if updated_ids.contains(logical) {
            final_set.push((*logical, new_text.clone()));
        } else {
            final_set.push((*logical, text.clone()));
        }
    }
    let brute = Brute::build(&final_set);
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut total_truth = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut s, &mut out, true);
        let eng_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !eng_set.contains(t) {
                false_neg += 1;
            }
        }
    }
    eprintln!(
        "compaction oracle: truth={} false_neg={} segments_before={} segments_after={}",
        total_truth,
        false_neg,
        pre_compact_segments,
        eng.num_segments()
    );
    assert_eq!(
        false_neg, 0,
        "compaction introduced FALSE NEGATIVES — contract violated"
    );
    assert!(total_truth > 0, "degenerate test: no matches");
}

/// Compact_range merges a subset of segments correctly — the rest are untouched.
#[test]
fn compact_range_preserves_correctness() {
    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xBA_93E,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let chunk = q.len() / 4;

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&q[..chunk]);
    eng.bulk_ingest(&q[chunk..2 * chunk]);
    eng.bulk_ingest(&q[2 * chunk..3 * chunk]);
    eng.bulk_ingest(&q[3 * chunk..]);
    assert_eq!(eng.num_segments(), 5); // 4 base + 1 memtable

    // Snapshot pre-compaction matches
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut pre_matches: Vec<HashSet<u64>> = Vec::with_capacity(data.titles.len());
    for title in &data.titles {
        eng.match_title(title, &mut s, &mut out, true);
        pre_matches.push(out.iter().copied().collect());
    }

    // Compact only segments [1..3) — merge segments 1 and 2
    let report = eng.compact_range(1, 3);
    assert!(report.is_some());
    assert_eq!(eng.num_segments(), 4); // 3 base + 1 memtable

    // Verify identical matches
    let mut mismatches = 0usize;
    for (ti, title) in data.titles.iter().enumerate() {
        eng.match_title(title, &mut s, &mut out, true);
        let post_set: HashSet<u64> = out.iter().copied().collect();
        if post_set != pre_matches[ti] {
            mismatches += 1;
        }
    }
    assert_eq!(mismatches, 0, "compact_range changed match results");
}

#[test]
fn spec_example_matches_expected() {
    let norm = Normalizer::default_vocab().expect("built-in vocab");
    let q = "1994 (upper deck,UD) michael jordan sp (preview,previews) \
        -(auto,autograph,signed,dna,signature) PSA 10 -(sgc,bgs)";
    let mut eng = Engine::new(norm);
    eng.build_from_queries(&[(1, q.to_string())]);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();

    let pass = [
        "1994 Upper Deck Michael Jordan SP Preview PSA GEM MT 10",
        "1994 UD Michael Jordan SP Previews PSA 10",
        "vintage 1994 upper deck michael jordan sp preview psa 10 sharp",
    ];
    for t in pass {
        eng.match_title(t, &mut s, &mut out, true);
        assert!(out.contains(&1), "expected match for {t:?}, got {out:?}");
    }

    let fail = [
        "1994 Upper Deck Michael Jordan SP Preview PSA 10 auto", // forbidden
        "1994 Upper Deck Michael Jordan SP Preview BGS 9.5", // wrong grader/grade + forbidden bgs
        "1993 Upper Deck Michael Jordan SP Preview PSA 10",  // wrong year
        "1994 Topps Michael Jordan SP Preview PSA 10",       // wrong brand
    ];
    for t in fail {
        eng.match_title(t, &mut s, &mut out, true);
        assert!(
            !out.contains(&1),
            "did NOT expect match for {t:?}, got {out:?}"
        );
    }
}

// ---- Vocab-rich oracle pass (ADR-050) ----

/// A populated normalizer vocabulary aligned to the synthetic generator's surface
/// forms (`gen.rs`): multiword player/brand phrases, single-token brand, brand-alt,
/// and card-term synonyms, plus graders and grade words. The default oracle runs the
/// empty `default_vocab`, so the multiword-phrase / synonym / grader normalization
/// machinery is never exercised on either side; this builds it so the differential
/// check covers that machinery end-to-end. Both the engine and the brute reference use
/// it, so they still agree by construction unless the engine's index/verify diverges.
fn gen_vocab() -> Normalizer {
    use reverse_rusty::gen::{BRANDS, BRAND_ALT, CARD_TERMS, GRADERS, PLAYERS};
    let mut b = NormalizerBuilder::new();
    for p in PLAYERS {
        let canon = format!("player:{}", p.replace(' ', "_"));
        let toks: Vec<&str> = p.split(' ').collect();
        b.add_phrase(&toks, &canon, FeatureKind::Player);
    }
    for brand in BRANDS {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        let toks: Vec<&str> = brand.split(' ').collect();
        if toks.len() > 1 {
            b.add_phrase(&toks, &canon, FeatureKind::Brand);
        } else {
            b.add_synonym(toks[0], &canon, FeatureKind::Brand);
        }
    }
    // Alternate brand surface forms (e.g. "ud" -> brand:upper_deck) converge onto the
    // same canonical as the full brand at the matching index.
    for (alt, brand) in BRAND_ALT.iter().zip(BRANDS.iter()) {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        b.add_synonym(alt, &canon, FeatureKind::Brand);
    }
    for ct in CARD_TERMS {
        b.add_synonym(ct, &format!("card_term:{ct}"), FeatureKind::Category);
    }
    for g in GRADERS {
        b.add_grader(g);
    }
    b.add_grade_word("gem");
    b.add_grade_word("mint");
    b.build().expect("gen vocab automaton")
}

/// Same contract as `zero_false_negatives_against_oracle`, but engine AND brute are
/// built with a POPULATED vocab (`gen_vocab`) instead of the empty `default_vocab`.
/// This exercises the multiword-phrase / synonym / grader normalization paths the
/// default oracle never reaches (ADR-050). Still a coherence check (shared front-end),
/// so it complements — does not replace — the spec-authored golden tests in
/// `src/{dsl,normalize,compile}.rs`.
#[test]
fn zero_false_negatives_with_populated_vocab() {
    let cfg = GenConfig {
        num_queries: 40_000,
        num_titles: 4_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x1234_5678,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(gen_vocab());
    eng.build_from_queries(&data.queries);

    let brute = Brute::build_with(&data.queries, gen_vocab());

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut total_truth = 0usize;
    let mut false_neg = 0usize;
    let mut false_pos = 0usize;

    for title in &data.titles {
        eng.match_title(title, &mut s, &mut out, true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !engine_set.contains(t) {
                false_neg += 1;
            }
        }
        for e in &engine_set {
            if !truth.contains(e) {
                false_pos += 1;
            }
        }
    }

    eprintln!(
        "vocab-rich oracle: truth_matches={total_truth} false_neg={false_neg} false_pos={false_pos}"
    );
    assert_eq!(
        false_neg, 0,
        "FALSE NEGATIVES with populated vocab — contract violated"
    );
    assert_eq!(
        false_pos, 0,
        "false positives with populated vocab — exact matcher not exact"
    );
    assert!(
        total_truth > 0,
        "degenerate test: no matches with populated vocab"
    );
}

/// The contract under NPMI corpus phrase induction (ADR-053): build with the EMPTY
/// `default_vocab`, then self-derive entity phrases from the live corpus and apply them
/// (`learn_and_apply_with(corpus_phrases=true)`). Ground truth uses the engine's OWN
/// learned normalizer — gluing applies the same normalizer to queries (recompile) and
/// titles (match), so engine ≡ brute with ZERO false negatives. Proves the induced
/// phrases flow through `set_vocab` + `recompile_stale_segments` losslessly.
#[test]
fn zero_false_negatives_after_corpus_phrase_learn_and_apply() {
    use reverse_rusty::vocab::CorpusLearnConfig;

    let cfg = GenConfig {
        num_queries: 8_000,
        num_titles: 2_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0C0F_FEE5,
        num_players: 600,
        num_sets: 300,
    };
    let data = generate(&cfg);

    // Generator corpus + a guaranteed-strong planted collocation ("zenith zonk"), so the
    // induction is never vacuous: a block of queries placing the pair adjacently, and
    // titles containing it.
    let mut queries = data.queries.clone();
    let base_id = queries.iter().map(|(l, _)| *l).max().unwrap_or(0) + 1;
    for i in 0..40u64 {
        queries.push((base_id + i, format!("zenith zonk plant{i}")));
    }
    let mut titles = data.titles.clone();
    for i in 0..40 {
        titles.push(format!("zenith zonk extra{i}"));
    }

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&queries);
    let learn_cfg = CorpusLearnConfig {
        corpus_phrases: true,
        npmi_min_count: 3,
        ..Default::default()
    };
    let recompiled = eng
        .learn_and_apply_with(&learn_cfg)
        .expect("corpus-phrase learn_and_apply");
    assert!(recompiled > 0, "learn_and_apply must recompile the corpus");

    // The learned vocab must carry the planted phrase (non-vacuous induction).
    let learned = eng
        .vocab()
        .expect("vocab set after learn_and_apply")
        .clone();
    assert!(
        learned
            .phrases()
            .iter()
            .any(|p| p.tokens == vec!["zenith".to_string(), "zonk".to_string()]),
        "the planted zenith/zonk collocation must be induced"
    );

    let brute = Brute::build_with(
        &queries,
        learned.to_normalizer().expect("learned normalizer builds"),
    );

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut total_truth = 0usize;
    let mut false_neg = 0usize;
    let mut false_pos = 0usize;

    for title in &titles {
        eng.match_title(title, &mut s, &mut out, true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !engine_set.contains(t) {
                false_neg += 1;
            }
        }
        for e in &engine_set {
            if !truth.contains(e) {
                false_pos += 1;
            }
        }
    }

    eprintln!(
        "corpus-phrase oracle: phrases={} truth={total_truth} false_neg={false_neg} false_pos={false_pos}",
        learned.phrases().len()
    );
    assert_eq!(
        false_neg, 0,
        "FALSE NEGATIVES after corpus-phrase learn — contract violated"
    );
    assert_eq!(
        false_pos, 0,
        "false positives after corpus-phrase learn — exact matcher not exact"
    );
    assert!(total_truth > 0, "degenerate test: no matches");
}

/// ADR-053 recall-first: corpus phrase induction is ADDITIVE, so a query referencing a
/// COMPONENT of an induced phrase keeps matching titles that contain the phrase. (Collapse —
/// the old behavior — would have dropped this candidate, which is the cardinal sin for a
/// recall-first stage-one matcher.)
#[test]
fn corpus_phrase_induction_preserves_component_query_recall() {
    use reverse_rusty::vocab::CorpusLearnConfig;

    let mut queries: Vec<(u64, String)> = vec![(1, "deck".into())]; // requires just "deck"
    for i in 0..40u64 {
        queries.push((100 + i, format!("upper deck u{i}"))); // plant the "upper deck" phrase
    }
    let title = "1994 upper deck rookie"; // contains "upper deck" adjacently

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(title, &mut s, &mut out, true);
    let before: HashSet<u64> = out.iter().copied().collect();
    assert!(
        before.contains(&1),
        "before induction, the 'deck' query matches a title containing 'deck'"
    );

    let cfg = CorpusLearnConfig {
        corpus_phrases: true,
        npmi_min_count: 3,
        ..Default::default()
    };
    eng.learn_and_apply_with(&cfg).expect("corpus-phrase learn");
    // The induced phrase is recorded as ADDITIVE.
    assert!(
        eng.vocab()
            .expect("vocab")
            .phrases()
            .iter()
            .any(|p| p.tokens == vec!["upper".to_string(), "deck".to_string()] && p.additive),
        "the induced 'upper deck' phrase must be additive"
    );

    eng.match_title(title, &mut s, &mut out, true);
    let after: HashSet<u64> = out.iter().copied().collect();
    assert!(
        after.contains(&1),
        "AFTER additive induction, the 'deck' query STILL matches (component recall preserved)"
    );
    assert!(
        before.is_subset(&after),
        "additive corpus phrases must not drop a prior match"
    );
}

/// Characterization (NOT a bug): inducing `upper deck` makes a query *phrased* "upper deck"
/// require the adjacent phrase, so it no longer matches a title where the two tokens are
/// NON-adjacent. This is the intended re-tokenization; for genuine entities (which appear
/// adjacent in real titles) it is negligible. Pinned so the tradeoff is explicit — and
/// contrasts with ADR-054 alias expansion, which is fully monotonic.
#[test]
fn corpus_phrase_induction_tightens_phrase_query_to_adjacency() {
    use reverse_rusty::vocab::CorpusLearnConfig;

    let mut queries: Vec<(u64, String)> = vec![(1, "upper deck".into())];
    for i in 0..40u64 {
        queries.push((100 + i, format!("upper deck u{i}")));
    }
    let nonadjacent = "upper blue deck"; // upper and deck present but NOT adjacent

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(nonadjacent, &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "before induction, 'upper deck' matches a non-adjacent title (AND of bare terms)"
    );

    let cfg = CorpusLearnConfig {
        corpus_phrases: true,
        npmi_min_count: 3,
        ..Default::default()
    };
    eng.learn_and_apply_with(&cfg).expect("corpus-phrase learn");
    eng.match_title(nonadjacent, &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "after induction, the phrase-form query tightens to adjacency (documented residual)"
    );
}

/// Equivalence learning via expansion-not-collapse (ADR-054): declaring `rc ≡ rookie` and
/// applying it must make a query phrased with one form match a title bearing the other —
/// while NEVER dropping a prior match (the match set only grows; FN-safe).
#[test]
fn equivalence_expansion_grows_matches_and_is_fn_safe() {
    use reverse_rusty::vocab::Vocab;

    // A corpus where "rc" and "rookie" are distinct features (empty default vocab). Extra
    // queries ensure both tokens are interned in the dict.
    let mut queries: Vec<(u64, String)> = vec![
        (1, "1994 fleer rc".into()),     // requires rc
        (2, "1994 fleer rookie".into()), // requires rookie
    ];
    for i in 0..20u64 {
        queries.push((100 + i, format!("rc card{i}")));
        queries.push((200 + i, format!("rookie card{i}")));
    }
    let rookie_title = "1994 fleer rookie psa 10"; // has rookie, NOT rc

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(rookie_title, &mut s, &mut out, true);
    let before: HashSet<u64> = out.iter().copied().collect();
    assert!(
        !before.contains(&1),
        "before the equivalence, the rc-query must not match a rookie-only title"
    );

    // Declare rc ≡ rookie and apply via expansion (set_vocab installs it; recompile expands).
    let mut v = Vocab::new();
    v.add_equivalence(&["rc", "rookie"]);
    eng.set_vocab(v).expect("set_vocab");
    eng.recompile_stale_segments();

    eng.match_title(rookie_title, &mut s, &mut out, true);
    let after: HashSet<u64> = out.iter().copied().collect();
    assert!(
        after.contains(&1),
        "after rc≡rookie, the rc-query matches a rookie title (expansion grew the match set)"
    );
    assert!(
        before.is_subset(&after),
        "expansion must never drop a prior match (FN-safe / monotone)"
    );
}

/// The structural safety claim for expansion (ADR-054): even a WRONG (nonsense) equivalence
/// can only add false positives — it must NEVER drop a true match. We apply a garbage
/// equivalence and assert every match the ORIGINAL (unexpanded) queries had still survives.
#[test]
fn wrong_equivalence_never_causes_false_negatives() {
    use reverse_rusty::vocab::Vocab;

    let cfg = GenConfig {
        num_queries: 8_000,
        num_titles: 2_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0BAD_0E00,
        num_players: 600,
        num_sets: 300,
    };
    let data = generate(&cfg);

    // Intern two unrelated nonsense tokens so the bogus equivalence resolves to real ids.
    let mut queries = data.queries.clone();
    for i in 0..20u64 {
        queries.push((9_000_000 + i, format!("wibble u{i}")));
        queries.push((9_100_000 + i, format!("wobble u{i}")));
    }

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    // Ground truth under the ORIGINAL semantics (no equivalence).
    let brute = Brute::build(&queries);

    // Apply a nonsense equivalence and recompile.
    let mut v = Vocab::new();
    v.add_equivalence(&["wibble", "wobble"]);
    eng.set_vocab(v).expect("set_vocab");
    eng.recompile_stale_segments();

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut total_truth = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut s, &mut out, true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats); // original semantics
        total_truth += truth.len();
        for t in &truth {
            if !engine_set.contains(t) {
                false_neg += 1;
            }
        }
    }
    assert_eq!(
        false_neg, 0,
        "expansion of a WRONG equivalence must never drop a true match (structural FN-safety)"
    );
    assert!(total_truth > 0, "degenerate test: no matches");
}

/// The learned source end-to-end (ADR-054): `learn_and_apply_with(learn_equivalences=true)`
/// turns the corpus's any-of groups into an equivalence applied via expansion, so a query
/// phrased with one form then matches a title bearing the other.
#[test]
fn learned_equivalence_via_expansion_matches_both_forms() {
    use reverse_rusty::vocab::CorpusLearnConfig;

    let mut queries: Vec<(u64, String)> = vec![(1, "1994 fleer rc".into())];
    for i in 0..6u64 {
        queries.push((100 + i, "(rc,rookie)".into())); // declare the any-of >= min_count
    }
    for i in 0..20u64 {
        queries.push((200 + i, format!("rookie u{i}")));
        queries.push((300 + i, format!("rc u{i}")));
    }
    let rookie_title = "1994 fleer rookie psa 10";

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(rookie_title, &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "before learning, the rc-query must not match a rookie title"
    );

    let cfg = CorpusLearnConfig {
        anyof_min_count: 2,
        learn_equivalences: true,
        ..Default::default()
    };
    eng.learn_and_apply_with(&cfg)
        .expect("learn_and_apply equivalences");
    assert!(
        !eng.vocab().expect("vocab").equivalences().is_empty(),
        "an equivalence group must be learned from the any-of corpus"
    );

    eng.match_title(rookie_title, &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "after learning rc≡rookie via expansion, the rc-query matches a rookie title"
    );
}

/// Equivalences declared on the vocab BEFORE the initial build must be applied during
/// `build_from_queries` (not only via a later `set_vocab`). Regression for the gap where the
/// single-engine initial build skipped equivalence resolution.
#[test]
fn initial_build_applies_declared_equivalences() {
    use reverse_rusty::vocab::Vocab;
    use reverse_rusty::EngineConfig;

    let mut v = Vocab::new();
    v.add_equivalence(&["rc", "rookie"]);
    let mut eng = Engine::with_vocab(v, EngineConfig::default()).expect("with_vocab");

    let mut queries: Vec<(u64, String)> = vec![(1, "1994 fleer rc".into())];
    for i in 0..10u64 {
        queries.push((100 + i, format!("rc u{i}")));
        queries.push((200 + i, format!("rookie u{i}")));
    }
    eng.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("1994 fleer rookie psa 10", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "initial build must apply declared equivalences: the rc-query matches a rookie title"
    );
}
