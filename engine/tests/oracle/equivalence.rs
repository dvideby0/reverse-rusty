//! Equivalence learning via expansion-not-collapse (ADR-054) differential oracle.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

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
