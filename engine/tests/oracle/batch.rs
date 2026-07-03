//! The columnar BATCH-path differential oracle.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine};
use std::collections::HashSet;

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
            broad_prefilter: true,
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
