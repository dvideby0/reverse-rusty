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
        (2u64, "mcdonald -reprint".to_string()), // required + forbidden
        (3u64, "oneill rookie".to_string()),     // two required terms
        (4u64, "(obrien|oneill)".to_string()),   // any-of group
    ];
    let titles = vec![
        "O\u{2019}Brien rookie".to_string(), // curly apostrophe  -> q1, q4
        "O'Brien auto".to_string(),          // ascii apostrophe  -> q1, q4
        "O-Brien".to_string(),               // mid-word hyphen   -> q1, q4
        "OBrien".to_string(),                // already joined    -> q1, q4
        "Ronald McDonald".to_string(),       // -> q2
        "Mc-Donald reprint".to_string(),     // folds to mcdonald but excluded by -reprint
        "O'Neill rookie".to_string(),        // -> q3, q4
        "nothing here".to_string(),          // -> {}
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
    for title in ["O\u{2019}Brien rookie", "O'Brien auto", "O-Brien", "OBrien"] {
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
    def.match_title("O'Brien auto", &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "default normalizer must NOT match `obrien` against an apostrophized title"
    );
}

/// The parity-mode number-context knob (ADR-069, ADR-064 item 3). Disabling the `pop`
/// number-context demotion is just a *different* shared normalizer — number typing becomes
/// position-insensitive (a 4-digit year is `year:N` everywhere) — so the lossless cover
/// still holds: engine and an independent brute oracle agree exactly (zero FN/FP),
/// including the forbidden-year and any-of paths. The payoff: the audit's one residual
/// false-negative class closes in BOTH directions — a query-side year now matches a
/// title-side `pop`-adjacent year, and a query-side `pop`-adjacent year now matches a
/// title-side year in any position.
#[test]
fn zero_false_negatives_with_number_context_disabled() {
    fn parity_norm() -> Normalizer {
        NormalizerBuilder::new()
            .number_context_words(&[])
            .build()
            .expect("parity normalizer")
    }

    let queries = vec![
        (1u64, "1995 topps".to_string()), // year-position year (audit direction 1)
        (2u64, "pop 1995 refractor".to_string()), // pop-adjacent year (audit direction 2)
        (3u64, "topps -1995".to_string()), // forbidden year
        (4u64, "(1995|1996) topps".to_string()), // any-of over years
    ];
    let titles = vec![
        "topps pop 1995".to_string(), // -> q1, q4 (NOT q3: its 1995 is a year now)
        "1995 topps".to_string(),     // -> q1, q4
        "refractor pop list 1995".to_string(), // -> q2
        "topps 2001".to_string(),     // -> q3
        "nothing here".to_string(),   // -> {}
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

    // Audit direction 1 closed: the year-position query matches the pop-adjacent title year.
    eng.match_title("topps pop 1995", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "parity mode: `1995 topps` must match a `pop`-adjacent title year"
    );
    // Audit direction 2 closed: the pop-adjacent query year matches a title year elsewhere.
    eng.match_title("refractor pop list 1995", &mut s, &mut out, true);
    assert!(
        out.contains(&2),
        "parity mode: a `pop`-adjacent query year must match a title year in any position"
    );

    // Contrast: the DEFAULT normalizer misses both directions (proves the knob does the
    // work — `pop 1995` demotes to `term:1995` on whichever side carries it).
    let mut def = Engine::new(Normalizer::default_vocab().expect("default vocab"));
    def.build_from_queries(&queries);
    def.match_title("topps pop 1995", &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "default: the demoted title year must NOT satisfy the query-side year"
    );
    def.match_title("refractor pop list 1995", &mut s, &mut out, true);
    assert!(
        !out.contains(&2),
        "default: the demoted query year must NOT be satisfied by a title-side year"
    );
}

/// The knob applies LIVE through the vocab machinery (ADR-069): `set_vocab` with an
/// explicit empty number-context list recompiles already-stored queries under the new
/// normalizer (the ADR-046 mech-2 path), so a query stored under the default demotion
/// starts matching `pop`-adjacent title years — no restart, no rebuild. This is what
/// "vocab-persisted knob" buys over a construction-time-only flag.
#[test]
fn number_context_knob_applies_live_via_set_vocab() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("default vocab"));
    eng.try_insert_live("1995 topps", 1, 1).expect("insert");

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("topps pop 1995", &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "before the flip: the demoted title year must not match"
    );

    let mut v = Vocab::new();
    v.set_number_context_words(&[]);
    eng.set_vocab(v).expect("set_vocab with the parity knob");

    eng.match_title("topps pop 1995", &mut s, &mut out, true);
    assert!(
        out.contains(&1),
        "after the flip: the stored query must be recompiled and match"
    );
    // And the flip is reversible: restoring a default vocab re-demotes.
    eng.set_vocab(Vocab::new()).expect("restore default vocab");
    eng.match_title("topps pop 1995", &mut s, &mut out, true);
    assert!(
        !out.contains(&1),
        "after restoring the default: the demotion is back"
    );
}
