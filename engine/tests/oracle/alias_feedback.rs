//! Match-feedback alias validation (ADR-103) — the engine-level integration oracle.
//!
//! The loop under test: candidates (from ADR-102 discovery) → passive capture of the live
//! title→query match stream → per-pair behavioral evidence (bottom-k sketches + the
//! degenerate-evidence exclusion) → `validated` → evidence stamped / explicitly activated
//! through the proven expansion path. Load-bearing claims: a genuine substitute pair validates
//! and its activation is FN-safe (widening-only); a tracked pair whose two forms satisfy
//! DISJOINT query populations never validates; stamping alone changes no match result.

use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::{AliasFeedback, AliasStatus, DistributionalConfig, FeedbackEvidence};
use std::collections::HashSet;

fn matched(eng: &mut Engine, s: &mut MatchScratch, title: &str) -> Vec<u64> {
    let mut out = Vec::new();
    eng.match_title(title, s, &mut out, true);
    out
}

/// The ADR-102 salted corpus shape: a planted substitute family over shared context queries
/// that do NOT name the forms (`ctxp*`-anchored queries provide the behavioral evidence), plus
/// enough filler for discovery to work.
fn corpus() -> Vec<(u64, String)> {
    let cfg = GenConfig {
        num_queries: 6_000,
        num_titles: 1,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0FEE_D103,
        num_players: 900,
        num_sets: 400,
    };
    let data = generate(&cfg);
    let mut queries = data.queries;
    let mut id = queries.iter().map(|(i, _)| *i).max().unwrap_or(0) + 1;
    // The substitute family (discovery input): zzud / zzupperdeck over shared contexts.
    for i in 0..40u64 {
        queries.push((id, format!("zzud ctxp{} ctxb{}", i % 7, i % 5)));
        id += 1;
        queries.push((id, format!("zzupperdeck ctxp{} ctxb{}", i % 7, i % 5)));
        id += 1;
    }
    // Context-only demand queries — the behavioral evidence BOTH forms' titles satisfy
    // (they never name the forms, so the report-time exclusion keeps them).
    for i in 0..7u64 {
        queries.push((id, format!("ctxp{i}")));
        id += 1;
    }
    queries
}

/// Drive the passive-capture loop the server handlers run: percolate a title stream where the
/// same underlying products appear under BOTH surface forms, feeding (title tokens, matched
/// ids) into the aggregator.
fn capture(eng: &mut Engine, fb: &mut AliasFeedback, n: usize) {
    let mut s = MatchScratch::new();
    for i in 0..n {
        for form in ["zzud", "zzupperdeck"] {
            let title = format!("{form} ctxp{} ctxb{} psa 10", i % 7, i % 5);
            let ids = matched(eng, &mut s, &title);
            let toks = reverse_rusty::corpus::tokenize(&title);
            fb.observe(&toks, &ids);
        }
    }
}

#[test]
fn genuine_pair_validates_and_activation_is_fn_safe() {
    let mut queries = corpus();
    let probe_q = 9_900_001u64;
    queries.push((probe_q, "zzud ctxp0".into()));
    let cross_title = "zzupperdeck ctxp0 ctxb0 psa 10";

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    eng.discover_aliases_and_record(&DistributionalConfig::default())
        .expect("discover + record");
    let forms = {
        let mut f = vec!["zzud".to_string(), "zzupperdeck".to_string()];
        f.sort();
        f
    };
    assert!(
        eng.aliases()
            .expect("vocab")
            .entries()
            .iter()
            .any(|e| e.forms == forms && e.status == AliasStatus::Candidate),
        "premise: discovery filed the pair as a candidate"
    );

    // Passive capture over a mixed-form title stream.
    let mut fb = AliasFeedback::default();
    fb.sync_tracked(
        eng.vocab().expect("vocab").aliases(),
        eng.config().alias_feedback_max_pairs,
    );
    capture(&mut eng, &mut fb, 200);

    // The report validates the pair over the surviving (non-form-referencing) evidence.
    let snap = eng.snapshot();
    let rows = fb.report(0.5, 50, 3, |id| snap.get_query_source(id));
    let row = rows
        .iter()
        .find(|r| r.forms == forms)
        .expect("the pair is tracked");
    assert!(
        row.validated,
        "same-demand populations must validate; got {row:?}"
    );
    assert!(row.excluded > 0, "form-referencing queries were excluded");

    // Baseline before activation; stamping alone must change nothing.
    let mut s = MatchScratch::new();
    let all_titles: Vec<String> = (0..40)
        .flat_map(|i| {
            [
                format!("zzud ctxp{} ctxb{} psa 10", i % 7, i % 5),
                format!("zzupperdeck ctxp{} ctxb{} psa 10", i % 7, i % 5),
            ]
        })
        .collect();
    let before: Vec<HashSet<u64>> = all_titles
        .iter()
        .map(|t| matched(&mut eng, &mut s, t).into_iter().collect())
        .collect();
    assert!(!before[0].is_empty(), "premise: the stream matches queries");
    let epoch0 = eng.vocab_epoch();

    let evidence = FeedbackEvidence {
        overlap: row.overlap,
        titles_a: row.titles_a,
        titles_b: row.titles_b,
        queries_sampled: row.sampled_a.min(row.sampled_b),
    };
    let stamp_only = eng
        .apply_alias_feedback(&[(forms.clone(), evidence)], false)
        .expect("stamp");
    assert_eq!((stamp_only.stamped, stamp_only.activated), (1, 0));
    assert_eq!(stamp_only.recompiled, 0, "stamping is metadata-only");
    assert_eq!(eng.vocab_epoch(), epoch0, "no epoch bump on stamping");
    assert!(
        !matched(&mut eng, &mut s, cross_title).contains(&probe_q),
        "stamping must not activate anything"
    );
    for (t, want) in all_titles.iter().zip(&before) {
        let got: HashSet<u64> = matched(&mut eng, &mut s, t).into_iter().collect();
        assert_eq!(&got, want, "stamping changes no match result ({t})");
    }

    // Explicit activation: the cross-form match appears, everything prior is preserved.
    let act = eng
        .apply_alias_feedback(&[(forms.clone(), evidence)], true)
        .expect("activate");
    assert_eq!(act.activated, 1);
    assert!(
        matched(&mut eng, &mut s, cross_title).contains(&probe_q),
        "after activation the zzud query matches the zzupperdeck title"
    );
    for (t, want) in all_titles.iter().zip(&before) {
        let got: HashSet<u64> = matched(&mut eng, &mut s, t).into_iter().collect();
        assert!(got.is_superset(want), "activation is widening-only ({t})");
    }
}

#[test]
fn disjoint_population_candidate_does_not_validate() {
    // A tracked candidate whose two forms satisfy DISJOINT query demand: form A titles match
    // only A-context queries, form B titles only B-context queries. Overlap ≈ 0 ⇒ never
    // validated — the behavioral signal rejects a pair that merely LOOKED distributional.
    // (An identical-demand co-hyponym like psa/bgs is kept out earlier: the ADR-102
    // co-occurrence penalty stops it from ever becoming a candidate — the pipeline composes.)
    let mut queries: Vec<(u64, String)> = Vec::new();
    let mut id = 1u64;
    for i in 0..30 {
        queries.push((id, format!("actx{i}")));
        id += 1;
        queries.push((id, format!("bctx{i}")));
        id += 1;
    }
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    // Plant the candidate directly (as a discovery false positive would).
    let mut vocab = eng.vocab().cloned().unwrap_or_default();
    vocab.record_distributional_candidates(
        &[reverse_rusty::vocab::DiscoveredPair {
            forms: vec!["zzfoo".into(), "zzbar".into()],
            similarity: 0.9,
            cooccurrence_rate: 0.0,
        }],
        &Normalizer::default_vocab().expect("vocab"),
        &reverse_rusty::dict::Dict::new(),
    );
    eng.set_vocab(vocab).expect("set_vocab");
    eng.recompile_stale_segments();

    let mut fb = AliasFeedback::default();
    fb.sync_tracked(eng.vocab().expect("vocab").aliases(), 256);
    let mut s = MatchScratch::new();
    for i in 0..100 {
        let ta = format!("zzfoo actx{}", i % 30);
        let tb = format!("zzbar bctx{}", i % 30);
        let ia = matched(&mut eng, &mut s, &ta);
        let ib = matched(&mut eng, &mut s, &tb);
        fb.observe(&reverse_rusty::corpus::tokenize(&ta), &ia);
        fb.observe(&reverse_rusty::corpus::tokenize(&tb), &ib);
    }
    let snap = eng.snapshot();
    let rows = fb.report(0.5, 50, 3, |id| snap.get_query_source(id));
    let row = &rows[0];
    assert!(
        row.titles_a >= 50 && row.titles_b >= 50,
        "plenty of evidence"
    );
    assert!(
        !row.validated,
        "disjoint demand must not validate; got {row:?}"
    );
    assert!(row.overlap < 0.1, "overlap near zero; got {}", row.overlap);
}
