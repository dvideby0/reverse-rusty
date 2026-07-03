//! Differential oracle over an ADVERSARIAL (messy) corpus.
//!
//! The clean generator emits lowercase, single-spaced, punctuation-free ASCII — the
//! easiest surface the normalizer can face, and several escaped bugs (whitespace runs,
//! punctuation handling, boundary-invalid phrase matches) lived exactly in the gap
//! between that and real listing data. This suite re-runs the zero-FN / zero-FP
//! differential over `gen::messify_dataset` output: case noise, diacritics, whitespace
//! runs (spaces + tabs), punctuation spliced into and around tokens, unicode junk
//! (emoji/CJK/trademark), duplicated tokens, out-of-dict tokens, and rare
//! >64-distinct-feature padded titles.
//!
//! Scope note: the brute reference shares the normalizer with the engine (ADR-050), so
//! this oracle pins the ENGINE PIPELINE (compile → signatures → index → verify) and
//! crash-safety under adversarial bytes; query↔title normalizer *divergence* on the same
//! data is pinned reference-free by `tests/adversarial/` (self-match + metamorphic).

use crate::harness::*;
use reverse_rusty::gen::{generate, messify_dataset, GenConfig, Rng};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
use std::collections::HashSet;

fn messy_dataset(seed: u64) -> reverse_rusty::gen::Dataset {
    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: 2_500,
        num_sets: 1_000,
    };
    let mut data = generate(&cfg);
    let clean_titles = data.titles.clone();
    let clean_queries: Vec<String> = data.queries.iter().map(|(_, q)| q.clone()).collect();

    let mut rng = Rng::new(seed ^ 0x4D45_5353); // "MESS"
    messify_dataset(&mut rng, &mut data, 0.8, 0.5);

    // Guard: the mess must actually land, or this suite silently degrades to the clean one.
    let perturbed_titles = data
        .titles
        .iter()
        .zip(&clean_titles)
        .filter(|(m, c)| m != c)
        .count();
    let perturbed_queries = data
        .queries
        .iter()
        .zip(&clean_queries)
        .filter(|((_, m), c)| m != *c)
        .count();
    assert!(
        perturbed_titles * 2 > data.titles.len(),
        "messify perturbed only {perturbed_titles}/{} titles — adversarial mode is a no-op",
        data.titles.len()
    );
    assert!(
        perturbed_queries * 4 > data.queries.len(),
        "messify perturbed only {perturbed_queries}/{} queries — adversarial mode is a no-op",
        data.queries.len()
    );
    data
}

/// Per-title path over the messy corpus, multi-segment + live memtable tail: the contract
/// (zero FN, zero FP vs the independent brute) must hold on adversarial bytes too.
#[test]
fn zero_false_negatives_with_messy_corpus() {
    let data = messy_dataset(0x00DE_FACE);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let n = data.queries.len();
    let c = n / 3;
    eng.build_from_queries(&data.queries[..c]);
    eng.bulk_ingest(&data.queries[c..2 * c]);
    for (id, text) in &data.queries[2 * c..] {
        eng.insert_live(text, *id, 1);
    }

    let brute = Brute::build(&data.queries);

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
        false_neg += truth.iter().filter(|t| !engine_set.contains(t)).count();
        false_pos += engine_set.iter().filter(|e| !truth.contains(e)).count();
    }

    eprintln!("messy oracle: truth={total_truth} false_neg={false_neg} false_pos={false_pos}");
    assert_eq!(
        false_neg, 0,
        "FALSE NEGATIVES on the messy corpus — contract violated"
    );
    assert_eq!(false_pos, 0, "false positives on the messy corpus");
    assert!(
        total_truth > 0,
        "degenerate test: the messy corpus produced no matches at all"
    );
}

/// The columnar batch path must satisfy the same contract on the messy corpus (broad ON,
/// so the broad-lane batch evaluator sees adversarial bytes as well).
#[test]
fn batch_path_zero_false_negatives_with_messy_corpus() {
    let data = messy_dataset(0x0BAD_F00D);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let n = data.queries.len();
    let c = n / 2;
    eng.build_from_queries(&data.queries[..c]);
    eng.bulk_ingest(&data.queries[c..]);

    let brute = Brute::build(&data.queries);

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
    for (title, engine_set) in data.titles.iter().zip(&per_title) {
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        false_neg += truth.iter().filter(|t| !engine_set.contains(t)).count();
        false_pos += engine_set.iter().filter(|e| !truth.contains(e)).count();
    }

    eprintln!(
        "messy batch oracle: truth={total_truth} false_neg={false_neg} false_pos={false_pos}"
    );
    assert_eq!(false_neg, 0, "batch path FN on the messy corpus");
    assert_eq!(false_pos, 0, "batch path FP on the messy corpus");
    assert!(total_truth > 0, "degenerate test: no matches");
}
