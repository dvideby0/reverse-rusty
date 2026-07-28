//! Vocab-rich oracle pass (ADR-050) + punctuation-equivalence folding (ADR-058)
//! + the parity-mode number-context knob (ADR-069).

use crate::harness::*;
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::{Normalizer, NormalizerBuilder, PunctClass};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::Vocab;
use std::collections::HashSet;

/// A populated normalizer vocabulary aligned to the synthetic generator's surface
/// forms (`gen.rs`): multiword entity/brand phrases, single-token brand, brand-alt,
/// and generic attribute synonyms. The default oracle runs the
/// empty `default_vocab`, so the multiword-phrase and synonym normalization
/// machinery is never exercised on either side; this builds it so the differential
/// check covers that machinery end-to-end. Both the engine and the brute reference use
/// it, so they still agree by construction unless the engine's index/verify diverges.
fn gen_vocab() -> Normalizer {
    use reverse_rusty::gen::{ATTRIBUTES, BRANDS, BRAND_ALT, ENTITIES};
    let mut b = NormalizerBuilder::new();
    for p in ENTITIES {
        let canon = format!("entity:{}", p.replace(' ', "_"));
        let toks: Vec<&str> = p.split(' ').collect();
        b.add_phrase(&toks, &canon, FeatureKind::Entity);
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
    // Alternate brand surface forms (e.g. "ns" -> brand:north_star) converge onto the
    // same canonical as the full brand at the matching index.
    for (alt, brand) in BRAND_ALT.iter().zip(BRANDS.iter()) {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        b.add_synonym(alt, &canon, FeatureKind::Brand);
    }
    for ct in ATTRIBUTES {
        b.add_synonym(ct, &format!("attribute:{ct}"), FeatureKind::Category);
    }
    b.build().expect("gen vocab automaton")
}

/// Same contract as `zero_false_negatives_against_oracle`, but engine AND brute are
/// built with a POPULATED vocab (`gen_vocab`) instead of the empty `default_vocab`.
/// This exercises the multiword-phrase and synonym normalization paths the
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
        num_entities: 3_000,
        num_collections: 1_200,
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

/// Punctuation-equivalence folding (ADR-058). A folding normalizer (ascii + curly
/// apostrophe + mid-word hyphen -> `PunctClass::Fold`) is just a *different* shared
/// normalizer, so the lossless cover still holds: build the engine AND an independent
/// brute oracle under it and they agree exactly (zero FN/FP) over punctuated data —
/// including the forbidden-term and any-of paths. The payoff: a joined-form query
/// (`obrien`) now matches every punctuated variant, which the DEFAULT normalizer misses.
#[test]
fn zero_false_negatives_with_punctuation_folding() {
    fn fold_vocab() -> Normalizer {
        NormalizerBuilder::new()
            .punct('\'', PunctClass::Fold)
            .punct('\u{2019}', PunctClass::Fold)
            .punct('-', PunctClass::Fold)
            .build()
            .expect("folding normalizer")
    }

    let queries = vec![
        (1u64, "obrien".to_string()),            // joined-form required term
        (2u64, "mcdonald -replica".to_string()), // required + forbidden
        (3u64, "oneill new".to_string()),        // two required terms
        (4u64, "(obrien|oneill)".to_string()),   // any-of group
    ];
    let titles = vec![
        "O\u{2019}Brien new".to_string(), // curly apostrophe  -> q1, q4
        "O'Brien manual".to_string(),     // ascii apostrophe  -> q1, q4
        "O-Brien".to_string(),            // mid-word hyphen   -> q1, q4
        "OBrien".to_string(),             // already joined    -> q1, q4
        "Ronald McDonald".to_string(),    // -> q2
        "Mc-Donald replica".to_string(),  // folds to mcdonald but excluded by -replica
        "O'Neill new".to_string(),        // -> q3, q4
        "nothing here".to_string(),       // -> {}
    ];

    let mut eng = Engine::new(fold_vocab());
    eng.build_from_queries(&queries);
    let brute = Brute::build_with(&queries, fold_vocab());

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut total_truth = 0usize;
    for title in &titles {
        eng.match_title(title, &mut s, &mut out, true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        assert_eq!(
            engine_set, truth,
            "engine != oracle for title {title:?} under punctuation folding"
        );
    }
    assert!(total_truth > 0, "degenerate: folding produced no matches");

    // Recall win: the joined-form query (`obrien`, id 1) matches every punctuated variant.
    for title in ["O\u{2019}Brien new", "O'Brien manual", "O-Brien", "OBrien"] {
        eng.match_title(title, &mut s, &mut out, true);
        assert!(
            out.contains(&1),
            "folding should match `obrien` against {title:?}"
        );
    }

    // Contrast: the DEFAULT normalizer misses the punctuated variant (proves folding is
    // doing the work — the apostrophe splits `obrien` into `o`/`brien` by default).
    let mut def = Engine::new(Normalizer::default_vocab().expect("default vocab"));
    def.build_from_queries(&queries);
    def.match_title("O'Brien manual", &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "default normalizer must NOT match `obrien` against an apostrophized title"
    );
}

/// An empty number-context list makes four-digit year typing position-independent.
/// That is just one shared-normalizer configuration, so the engine and independent
/// brute oracle must still agree exactly across positive, forbidden, and any-of paths.
#[test]
fn zero_false_negatives_with_empty_number_context() {
    fn parity_norm() -> Normalizer {
        NormalizerBuilder::new()
            .number_context_words(&[])
            .build()
            .expect("parity normalizer")
    }

    let queries = vec![
        (1u64, "1995 acme".to_string()),
        (2u64, "model 1995 textured".to_string()),
        (3u64, "acme -1995".to_string()),
        (4u64, "(1995|1996) acme".to_string()),
    ];
    let titles = vec![
        "acme model 1995".to_string(),
        "1995 acme".to_string(),
        "textured model series 1995".to_string(),
        "acme 2001".to_string(),
        "nothing here".to_string(),
    ];

    let mut eng = Engine::new(parity_norm());
    eng.build_from_queries(&queries);
    let brute = Brute::build_with(&queries, parity_norm());

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut total_truth = 0usize;
    for title in &titles {
        eng.match_title(title, &mut s, &mut out, true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        assert_eq!(
            engine_set, truth,
            "engine != oracle for title {title:?} with number-context disabled"
        );
    }
    assert!(
        total_truth > 0,
        "degenerate: parity mode produced no matches"
    );

    eng.match_title("acme model 1995", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "empty number-context: a year must keep the same type in every position"
    );
    eng.match_title("textured model series 1995", &mut s, &mut out, true);
    assert!(
        out.contains(&2),
        "empty number-context: query and title year typing must remain symmetric"
    );

    // Contrast with an explicitly configured numeric identifier context.
    let contextual = NormalizerBuilder::new()
        .number_context_words(&["model"])
        .build()
        .expect("contextual normalizer");
    let mut contextual_engine = Engine::new(contextual);
    contextual_engine.build_from_queries(&queries);
    contextual_engine.match_title("acme model 1995", &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "configured context must keep the adjacent number generic"
    );
}

/// Number-context changes apply live through the vocabulary machinery: `set_vocab`
/// recompiles stored queries under the replacement shared normalizer.
#[test]
fn number_context_knob_applies_live_via_set_vocab() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("default vocab"));
    eng.try_insert_live("1995 acme", 1, 1).expect("insert");

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("acme model 1995", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "empty default context must preserve year typing"
    );

    let mut v = Vocab::new();
    v.set_number_context_words(&["model"]);
    eng.set_vocab(v).expect("set_vocab with a number context");

    eng.match_title("acme model 1995", &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "configured context must keep the adjacent number generic"
    );

    eng.set_vocab(Vocab::new()).expect("restore default vocab");
    eng.match_title("acme model 1995", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "restoring the empty default context must restore year typing"
    );
}
