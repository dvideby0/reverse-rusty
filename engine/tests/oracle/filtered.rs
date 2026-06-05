//! Filtered percolation (ADR-049) differential oracle.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

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
