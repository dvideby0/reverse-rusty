// ═════════════════════════════════════════════════════════════════════════════
// 1. PARALLEL MATCHING CORRECTNESS
// ═════════════════════════════════════════════════════════════════════════════

use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

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
