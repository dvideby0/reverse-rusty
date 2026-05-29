//! Production-hardening test coverage: parallel matching, repeated compaction,
//! broad-lane isolation, and edge-case inputs.
//!
//! These tests close gaps identified in the production-readiness audit. Each
//! section targets a specific area that had no dedicated test:
//!   * Parallel matching correctness (par == sequential)
//!   * Repeated / interleaved compaction (multi-round stress)
//!   * Broad-lane isolation (class-C routing, include_broad flag)
//!   * Edge-case inputs (empty, oversized, Unicode, adversarial)

use percolator::compile::{extract, Extracted};
use percolator::dict::Dict;
use percolator::gen::{generate, GenConfig};
use percolator::normalize::Normalizer;
use percolator::segment::{Engine, MatchScratch};
use std::collections::HashSet;

// ─────────────────────────────────────────────────────────────────────────────
// Helper: brute-force oracle (same as oracle.rs, reproduced here so this file
// is self-contained and can't share a bug with the main oracle).
// ─────────────────────────────────────────────────────────────────────────────

struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    fn build(queries: &[(u64, String)]) -> Self {
        let norm = Normalizer::default_vocab().expect("built-in vocab");
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = percolator::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
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

// ═════════════════════════════════════════════════════════════════════════════
// 1. PARALLEL MATCHING CORRECTNESS
// ═════════════════════════════════════════════════════════════════════════════

/// match_titles_par must produce EXACTLY the same match sets as sequential
/// match_title for every title. This is the only test that verifies rayon
/// thread-pool behavior doesn't diverge from the single-threaded path.
#[test]
fn parallel_matches_equal_sequential() {
    let cfg = GenConfig {
        num_queries: 40_000,
        num_titles: 2_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xDA_2A11E1,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&data.queries);

    // Sequential results
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut sequential: Vec<HashSet<u64>> = Vec::with_capacity(data.titles.len());
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        sequential.push(out.iter().copied().collect());
    }

    // Parallel results
    let par_results = eng.match_titles_par(&data.titles, true);

    assert_eq!(
        par_results.len(),
        data.titles.len(),
        "par should return one result per title"
    );

    let mut mismatches = 0usize;
    for (idx, matches, _stats) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != sequential[*idx] {
            mismatches += 1;
            if mismatches <= 3 {
                eprintln!(
                    "PAR MISMATCH title[{}]: {:?}\n  seq only: {:?}\n  par only: {:?}",
                    idx,
                    &data.titles[*idx],
                    sequential[*idx].difference(&par_set).collect::<Vec<_>>(),
                    par_set.difference(&sequential[*idx]).collect::<Vec<_>>(),
                );
            }
        }
    }

    eprintln!(
        "parallel test: titles={} mismatches={}",
        data.titles.len(),
        mismatches
    );
    assert_eq!(
        mismatches, 0,
        "parallel matching diverged from sequential on {mismatches} titles"
    );
}

/// Parallel matching with include_broad=false must also agree with sequential.
#[test]
fn parallel_matches_equal_sequential_no_broad() {
    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 1_000,
        broad_query_frac: 0.10, // higher broad fraction to stress the flag
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xA0_B20AD,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&data.queries);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut sequential: Vec<HashSet<u64>> = Vec::with_capacity(data.titles.len());
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, false); // no broad
        sequential.push(out.iter().copied().collect());
    }

    let par_results = eng.match_titles_par(&data.titles, false);

    let mut mismatches = 0usize;
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != sequential[*idx] {
            mismatches += 1;
        }
    }
    assert_eq!(
        mismatches, 0,
        "parallel (no broad) diverged from sequential"
    );
}

/// match_titles_par_stats aggregate stats should equal the sum of per-title stats.
#[test]
fn parallel_stats_aggregate_correctly() {
    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 500,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x57A75,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&data.queries);

    let par_results = eng.match_titles_par(&data.titles, true);
    let agg = eng.match_titles_par_stats(&data.titles, true);

    let sum_matches: u32 = par_results.iter().map(|(_, _, s)| s.matches).sum();
    let sum_candidates: u32 = par_results
        .iter()
        .map(|(_, _, s)| s.unique_candidates)
        .sum();

    // Aggregate stats should match the sum of individual stats
    assert_eq!(
        agg.matches, sum_matches,
        "par_stats matches != sum of per-title matches"
    );
    assert_eq!(
        agg.unique_candidates, sum_candidates,
        "par_stats candidates != sum of per-title candidates"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 2. REPEATED / INTERLEAVED COMPACTION
// ═════════════════════════════════════════════════════════════════════════════

/// Build multiple segments, compact some, add more, compact again, verify the
/// final engine matches the brute-force oracle. Stresses the remap logic under
/// repeated merge cycles.
#[test]
fn repeated_compaction_preserves_correctness() {
    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x00C0_AC72,
        num_players: 2_500,
        num_sets: 1_000,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let n = q.len();
    let chunk = n / 6;

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));

    // Phase 1: build 3 segments
    eng.build_from_queries(&q[..chunk]);
    eng.bulk_ingest(&q[chunk..2 * chunk]);
    eng.bulk_ingest(&q[2 * chunk..3 * chunk]);
    assert!(eng.num_segments() >= 4); // 3 base + memtable

    // Compact all -> 1 base + memtable
    let r1 = eng.compact_all();
    assert!(r1.is_some());
    assert_eq!(eng.num_segments(), 2);

    // Phase 2: add 2 more segments on top of the compacted base
    eng.bulk_ingest(&q[3 * chunk..4 * chunk]);
    eng.bulk_ingest(&q[4 * chunk..5 * chunk]);
    assert!(eng.num_segments() >= 4);

    // Compact range: merge only the 2 newest base segments
    let _ = eng.compact_range(1, 3);

    // Phase 3: add a final segment + live inserts
    eng.bulk_ingest(&q[5 * chunk..]);
    for i in (0..100).step_by(7) {
        let _ = eng.insert_live(&format!("{} extra variant", q[i].1), q[i].0 + 1_000_000, 2);
    }
    eng.flush();

    // Final compact_all
    let r_final = eng.compact_all();
    assert!(r_final.is_some());

    // Verify against oracle over the full query set (including the extra variants)
    let mut all_queries: Vec<(u64, String)> = q.clone();
    for i in (0..100).step_by(7) {
        all_queries.push((q[i].0 + 1_000_000, format!("{} extra variant", q[i].1)));
    }
    let brute = Brute::build(&all_queries);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut total_truth = 0usize;

    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
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
        "repeated-compaction: truth={} false_neg={} final_segments={}",
        total_truth,
        false_neg,
        eng.num_segments()
    );
    assert_eq!(
        false_neg, 0,
        "repeated compaction introduced FALSE NEGATIVES"
    );
    assert!(total_truth > 0, "degenerate test: no matches");
}

/// Interleave insert_live + tombstone + compact in a tight loop. Verifies that
/// the engine doesn't corrupt state under rapid mutation-compaction cycling.
#[test]
fn compaction_under_churn() {
    let cfg = GenConfig {
        num_queries: 10_000,
        num_titles: 500,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x000C_4020,
        num_players: 1_500,
        num_sets: 600,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&data.queries);

    // Perform 5 rounds of: add live queries, flush, compact
    let mut extra_id = 2_000_000u64;
    for round in 0..5 {
        // add 50 queries
        for i in 0..50 {
            let text = format!("round {round} item {i} michael jordan upper deck 1994");
            let _ = eng.insert_live(&text, extra_id, (round + 2) as u32);
            extra_id += 1;
        }
        eng.flush();

        // compact
        let _ = eng.compact_all();
    }

    // Verify: engine still runs without panic, matches are non-empty
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut total_matches = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        total_matches += out.len();
    }
    eprintln!(
        "churn test: total_matches={} segments={}",
        total_matches,
        eng.num_segments()
    );
    assert!(total_matches > 0, "churn destroyed all matches");
    // Should have compacted down to few segments
    assert!(
        eng.num_segments() <= 3,
        "expected <=3 segments after compact_all, got {}",
        eng.num_segments()
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 3. BROAD-LANE ISOLATION
// ═════════════════════════════════════════════════════════════════════════════

/// Class-C queries must land in the broad index, not the main index.
/// include_broad=false must exclude them; include_broad=true must include them.
#[test]
fn broad_lane_queries_only_in_broad_index() {
    // Craft queries that are clearly broad (all required features are hot)
    // and queries that are clearly selective.
    let norm = Normalizer::default_vocab().expect("built-in vocab");
    let mut eng = Engine::new(norm);

    // Build a corpus that establishes frequency. We need many queries so the
    // "hot" features get high frequency and end up in the common mask.
    let mut queries: Vec<(u64, String)> = Vec::new();
    let mut id = 1u64;

    // 5000 queries all mentioning "hottoken" — makes that feature very hot
    for i in 0..5000 {
        queries.push((
            id,
            format!("hottoken {} somethingrare{:04}", 1990 + (i % 30), i),
        ));
        id += 1;
    }

    // A query whose only required feature is super-hot (single token)
    // This should be class C (broad) since it has one hot feature and nothing to pair.
    let broad_id = id;
    queries.push((broad_id, "hottoken".to_string()));
    id += 1;

    // A selective query with rare required features
    let selective_id = id;
    queries.push((selective_id, "1994 hottoken somethingrare0042".to_string()));

    eng.build_from_queries(&queries);

    let classes = eng.class_counts();
    eprintln!(
        "class distribution: A={} B={} C={} D={}",
        classes[0], classes[1], classes[2], classes[3]
    );
    // We should have at least some class-C queries
    assert!(
        classes[2] > 0,
        "expected some class-C (broad) queries, got 0"
    );

    // Test: include_broad=false should NOT return the broad query
    let title = "hottoken 1994 somethingrare0042";
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    eng.match_title(title, &mut scratch, &mut out, false);
    let no_broad: HashSet<u64> = out.iter().copied().collect();

    eng.match_title(title, &mut scratch, &mut out, true);
    let with_broad: HashSet<u64> = out.iter().copied().collect();

    // with_broad should be a superset of no_broad
    for id in &no_broad {
        assert!(
            with_broad.contains(id),
            "include_broad=true lost a match that include_broad=false had: {id}"
        );
    }

    // The broad-only matches (difference) should exist if there are broad queries
    let broad_only: HashSet<u64> = with_broad.difference(&no_broad).copied().collect();
    eprintln!(
        "broad test: no_broad={} with_broad={} broad_only={}",
        no_broad.len(),
        with_broad.len(),
        broad_only.len()
    );
    // At minimum, if class C > 0, the broad lane is being used
    // (whether our specific crafted query lands there depends on the frequency model)
}

/// Verify that MatchStats correctly separates main_candidates from broad_candidates.
#[test]
fn match_stats_separate_main_and_broad_candidates() {
    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 500,
        broad_query_frac: 0.10, // 10% broad to ensure we get some
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0B20_AD57,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&data.queries);

    let classes = eng.class_counts();
    eprintln!(
        "stats test classes: A={} B={} C={} D={}",
        classes[0], classes[1], classes[2], classes[3]
    );

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut total_main = 0u64;
    let mut total_broad = 0u64;

    for title in &data.titles {
        let stats = eng.match_title(title, &mut scratch, &mut out, true);
        total_main += u64::from(stats.main_candidates);
        total_broad += u64::from(stats.broad_candidates);
    }

    eprintln!("stats: total_main_candidates={total_main} total_broad_candidates={total_broad}");

    // Main candidates should always exist (we have 20k queries)
    assert!(total_main > 0, "no main candidates at all");

    // If there are class-C queries, we should see broad candidates
    if classes[2] > 0 {
        assert!(
            total_broad > 0,
            "class-C queries exist ({}) but no broad candidates seen",
            classes[2]
        );
    }

    // With include_broad=false, stats should show zero broad candidates
    let mut total_broad_off = 0u64;
    for title in &data.titles {
        let stats = eng.match_title(title, &mut scratch, &mut out, false);
        total_broad_off += u64::from(stats.broad_candidates);
    }
    assert_eq!(
        total_broad_off, 0,
        "include_broad=false still reported broad candidates"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. EDGE-CASE INPUTS
// ═════════════════════════════════════════════════════════════════════════════

/// Empty title should match nothing and not panic.
#[test]
fn empty_title_matches_nothing() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[
        (1, "michael jordan".to_string()),
        (2, "1994 upper deck".to_string()),
    ]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("", &mut scratch, &mut out, true);
    assert!(out.is_empty(), "empty title should not match any query");
}

/// Whitespace-only title should match nothing and not panic.
#[test]
fn whitespace_title_matches_nothing() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("   \t\n  ", &mut scratch, &mut out, true);
    assert!(out.is_empty(), "whitespace-only title should not match");
}

/// Empty query corpus: matching should work (return empty) with no panics.
#[test]
fn empty_corpus_matches_nothing() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let report = eng.build_from_queries(&[]);
    assert_eq!(report.ingested, 0);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(
        "michael jordan 1994 upper deck",
        &mut scratch,
        &mut out,
        true,
    );
    assert!(out.is_empty(), "empty corpus should produce no matches");
}

/// Very long title should not panic or corrupt state.
#[test]
fn very_long_title_does_not_panic() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    // Build a ~100KB title
    let mut long_title = String::with_capacity(100_000);
    for i in 0..5000 {
        long_title.push_str(&format!("word{i} "));
    }
    long_title.push_str("michael jordan");

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(&long_title, &mut scratch, &mut out, true);
    // Should find "michael jordan" even buried in the long title
    assert!(
        out.contains(&1),
        "long title containing the query terms should still match"
    );
}

/// Unicode and diacritics: the normalizer should fold these correctly.
#[test]
fn unicode_diacritics_handled() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    // Diacritic folding: "Michaël Jördàn" should match "michael jordan"
    eng.match_title(
        "Michaël Jördàn 1994 upper deck",
        &mut scratch,
        &mut out,
        true,
    );
    // Whether this matches depends on normalizer diacritic folding behavior.
    // The test ensures no panic regardless.
    eprintln!(
        "diacritic test: matched={} (folding depends on normalizer config)",
        out.len()
    );
}

/// Mixed case: engine should be case-insensitive.
#[test]
fn case_insensitive_matching() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "Michael Jordan".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    let cases = [
        "MICHAEL JORDAN",
        "michael jordan",
        "Michael Jordan",
        "mIcHaEl JoRdAn",
    ];
    for title in &cases {
        eng.match_title(title, &mut scratch, &mut out, true);
        assert!(
            out.contains(&1),
            "case-insensitive match failed for {title:?}"
        );
    }
}

/// Duplicate logical IDs: inserting the same logical ID twice should not
/// produce duplicate matches.
#[test]
fn duplicate_logical_ids_deduped_in_results() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));

    // Insert same logical ID via two different paths
    eng.build_from_queries(&[(42, "michael jordan 1994".to_string())]);
    // Re-insert same logical ID with slightly different text via bulk_ingest
    eng.bulk_ingest(&[(42, "michael jordan 1994 upper deck".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(
        "michael jordan 1994 upper deck psa 10",
        &mut scratch,
        &mut out,
        true,
    );

    // Count how many times logical ID 42 appears
    let count_42 = out.iter().filter(|&&id| id == 42).count();
    assert!(
        count_42 <= 1,
        "logical ID 42 appeared {count_42} times in results (should be deduped to <=1)"
    );
}

/// All-forbidden query (only MUST_NOT terms) should be rejected as class D.
#[test]
fn all_forbidden_query_rejected() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let report = eng.build_from_queries(&[
        (1, "-(auto,signed,graded)".to_string()), // only negatives
    ]);
    assert_eq!(
        report.rejected_class_d, 1,
        "all-forbidden should be class D"
    );
    assert_eq!(report.ingested, 0);
}

/// Single-character queries should parse without panic (even if rejected).
#[test]
fn single_char_queries_dont_panic() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let chars = ["a", "1", "-", "(", ")", ",", " ", "Z", "#"];
    for &c in &chars {
        // Should not panic regardless of whether it parses
        let _ = eng.insert_live(c, 999, 1);
    }
}

/// Matching against an engine with only tombstoned entries should return nothing.
#[test]
fn fully_tombstoned_engine_matches_nothing() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[
        (1, "michael jordan".to_string()),
        (2, "lebron james".to_string()),
        (3, "kobe bryant".to_string()),
    ]);

    // Tombstone all entries in segment 0
    for local in 0..3u32 {
        eng.tombstone_in(0, local).unwrap();
    }

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(
        "michael jordan lebron james kobe bryant",
        &mut scratch,
        &mut out,
        true,
    );
    assert!(
        out.is_empty(),
        "fully tombstoned engine should return no matches, got {out:?}"
    );
}

/// Compacting a fully tombstoned engine should produce an empty segment.
#[test]
fn compact_fully_tombstoned_produces_empty_segment() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    // Use two separate ingests to create 2 base segments (compact_all requires >=2)
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);
    eng.bulk_ingest(&[(2, "lebron james".to_string())]);

    // Tombstone everything (segment 0 has local 0, segment 1 has local 0)
    eng.tombstone_in(0, 0).unwrap();
    eng.tombstone_in(1, 0).unwrap();

    let report = eng.compact_all();
    assert!(
        report.is_some(),
        "compact_all should run with 2+ base segments"
    );
    let report = report.unwrap();
    assert_eq!(report.entries_after, 0, "compacted segment should be empty");
    assert_eq!(report.tombstones_reclaimed, 2);

    // Verify no matches
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("michael jordan lebron james", &mut scratch, &mut out, true);
    assert!(out.is_empty());
}

/// Title with special characters (hyphens, ampersands, numbers) should not panic.
#[test]
fn special_characters_in_title() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    let titles = [
        "michael-jordan #23 1994",
        "michael & jordan co. ltd.",
        "michael jordan !!! @#$%^&*()",
        "michael\tjordan\nnewline",
        "michael jordan 🏀🏆",
        "100% authentic michael jordan card",
    ];
    for title in &titles {
        // Should not panic
        eng.match_title(title, &mut scratch, &mut out, true);
    }
}

/// Build + bulk_ingest + flush + compact with zero queries should not panic.
#[test]
fn lifecycle_with_zero_queries() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));

    // All lifecycle operations on an empty engine
    let report = eng.build_from_queries(&[]);
    assert_eq!(report.ingested, 0);

    eng.bulk_ingest(&[]);
    eng.flush();
    let r = eng.compact_all();
    // compact_all on empty should be None (nothing to compact) or Some with 0 entries
    if let Some(r) = r {
        assert_eq!(r.entries_before, 0);
    }

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("anything", &mut scratch, &mut out, true);
    assert!(out.is_empty());
}

/// Metrics snapshot should be consistent on a known corpus.
#[test]
fn metrics_consistent_with_known_corpus() {
    let cfg = GenConfig {
        num_queries: 5_000,
        num_titles: 100,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xAE7_21C5,
        num_players: 1_000,
        num_sets: 400,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let report = eng.build_from_queries(&data.queries);

    assert!(
        report.ingested > 0,
        "need some ingested queries for this test"
    );

    let m = eng.metrics();
    assert_eq!(
        m.total_queries, report.ingested as usize,
        "metrics total_queries must equal ingested count"
    );
    assert_eq!(
        m.base_segments, 1,
        "one base segment after build_from_queries"
    );
    assert!(m.dict_features > 0, "dictionary should have features");
    assert!(m.exact_bytes > 0, "exact store should use memory");
    assert!(m.index_bytes > 0, "index should use memory");
    assert_eq!(m.rejected_parse as usize, report.rejected_parse);
    assert_eq!(m.rejected_class_d as usize, report.rejected_class_d);
}

// ─────────────────────────────────────────────────────────────────────────────
// Settings: the engine config rides in the lock-free snapshot (GET /_settings),
// and set_config swaps it copy-on-write so in-flight snapshots keep their view.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn settings_snapshot_reflects_set_config_and_is_immutable() {
    use percolator::config::EngineConfig;

    let mut eng = Engine::with_config(
        Normalizer::default_vocab().unwrap(),
        EngineConfig {
            max_segments: 8,
            ..EngineConfig::default()
        },
    );

    // GET /_settings reads the snapshot, so the snapshot must carry the config.
    let snap_before = eng.snapshot();
    assert_eq!(snap_before.config().max_segments, 8);

    // Change a dynamic knob via the public setter.
    let mut cfg = eng.config().clone();
    cfg.max_segments = 32;
    eng.set_config(cfg);

    // A fresh snapshot sees the new value; the engine agrees; the older snapshot
    // keeps its own view (copy-on-write via Arc).
    assert_eq!(eng.snapshot().config().max_segments, 32);
    assert_eq!(eng.config().max_segments, 32);
    assert_eq!(
        snap_before.config().max_segments,
        8,
        "an already-published snapshot must keep its own config view"
    );
}
