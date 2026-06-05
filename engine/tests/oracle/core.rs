//! Core per-title differential oracle + the worked spec example.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

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
