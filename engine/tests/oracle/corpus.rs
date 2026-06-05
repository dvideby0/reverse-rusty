//! NPMI corpus phrase induction (ADR-053) differential oracle + characterizations.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

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
