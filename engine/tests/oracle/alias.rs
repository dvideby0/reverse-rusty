//! Learned-alias evolution — Phase 1 (ADR-060) differential oracle.
//!
//! Proves the governance layer over equivalence expansion is **zero-false-negative**: a safe
//! single-token variant auto-activates and makes both surface forms match; conservative kinds
//! (learned category alternatives, multi-word) are recorded as candidates and never silently
//! affect matching; the alias-ID-stability fix keeps an alias active across a future insert; and
//! the live apply recompiles existing queries without a restart.

use crate::harness::*;
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::{AliasKind, AliasProvenance, AliasStatus, Vocab};
use std::collections::HashSet;

/// Find a registry entry by its (canonical, sorted) forms.
fn find_entry<'a>(eng: &'a Engine, forms: &[&str]) -> Option<&'a reverse_rusty::vocab::AliasEntry> {
    let mut want: Vec<String> = forms.iter().map(|s| (*s).to_string()).collect();
    want.sort();
    eng.aliases()
        .expect("vocab installed")
        .entries()
        .iter()
        .find(|e| e.forms == want)
}

fn matched(eng: &mut Engine, s: &mut MatchScratch, title: &str) -> HashSet<u64> {
    let mut out = Vec::new();
    eng.match_title(title, s, &mut out, true);
    out.iter().copied().collect()
}

/// (1) A single-token *variant* any-of group (shared ≥3-char prefix) is learned and
/// auto-activated, and the alias then makes a query phrased with one form match a title bearing
/// the other — zero false negatives.
#[test]
fn learns_single_token_alias_from_anyof_group() {
    // `autograph`/`autographs` are a plural variant ⇒ SingleTokenVariant ⇒ auto-active.
    let mut queries: Vec<(u64, String)> = vec![(1, "fleer autograph".into())];
    for i in 0..6u64 {
        queries.push((100 + i, "(autograph,autographs)".into())); // any-of seen >= min_count
    }
    for i in 0..10u64 {
        queries.push((200 + i, format!("autograph u{i}")));
        queries.push((300 + i, format!("autographs u{i}")));
    }
    let title = "fleer autographs psa 10"; // has autographs, NOT autograph

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    assert!(
        !matched(&mut eng, &mut s, title).contains(&1),
        "before learning, the autograph-query must not match an autographs-only title"
    );

    let report = eng
        .learn_aliases_and_apply(2)
        .expect("learn + apply aliases");
    assert!(
        report.activated >= 1,
        "a single-token variant group must auto-activate (activated={})",
        report.activated
    );
    let entry = find_entry(&eng, &["autograph", "autographs"]).expect("alias learned");
    assert_eq!(entry.kind, AliasKind::SingleTokenVariant);
    assert_eq!(entry.status, AliasStatus::Active);
    assert_eq!(entry.provenance, AliasProvenance::LearnedFromQueries);

    assert!(
        matched(&mut eng, &mut s, title).contains(&1),
        "after learning autograph≡autographs, the autograph-query matches an autographs title"
    );
}

/// (2) A multi-form learned **category alternative** (`(psa, bgs, sgc)` — distinct single
/// tokens, no shared prefix) is recorded as a review candidate, never silently activated.
#[test]
fn does_not_auto_activate_category_alternatives() {
    let mut queries: Vec<(u64, String)> = Vec::new();
    for i in 0..6u64 {
        queries.push((100 + i, "(psa,bgs,sgc) card".into()));
    }
    // Filler so the tokens are interned and the queries are non-degenerate.
    for i in 0..10u64 {
        queries.push((200 + i, format!("psa u{i}")));
        queries.push((300 + i, format!("bgs u{i}")));
    }

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    let report = eng
        .learn_aliases_and_apply(2)
        .expect("learn + apply aliases");
    assert_eq!(
        report.activated, 0,
        "a learned 3-form category alternative must NOT auto-activate"
    );
    let entry = find_entry(&eng, &["bgs", "psa", "sgc"]).expect("group recorded as candidate");
    assert_eq!(entry.kind, AliasKind::SingleTokenDistinct);
    assert_eq!(entry.status, AliasStatus::Candidate);
    assert!(
        eng.aliases().expect("vocab").active_groups().is_empty(),
        "no category-alternative group may be active"
    );
}

/// (3) The alias-ID-stability fix (the embedded real bug): an alias installed on a FRESH index
/// — before its forms are interned — must stay active when a LATER live insert interns one of
/// those forms as a dense id. Without the fix the equivalence map is keyed by the form's
/// *synthetic* id, the dense insert never matches it, and the alias silently dies (a false
/// negative). This test fails on the pre-fix code.
#[test]
fn alias_ids_are_stable_after_future_insert() {
    // Build a vocab whose registry holds ONE active single-token variant alias, classified
    // against an empty dict (so the forms are NOT yet interned).
    let cls_norm = Normalizer::default_vocab().expect("vocab");
    let cls_dict = Dict::new();
    let mut v = Vocab::new();
    let status = v.aliases_mut().add_classified(
        &["autograph".into(), "autographs".into()],
        AliasProvenance::Manual,
        1.0,
        &cls_norm,
        &cls_dict,
    );
    assert_eq!(
        status,
        Some(AliasStatus::Active),
        "variant alias must be active"
    );

    // Fresh engine, no queries yet ⇒ the dict is empty when the alias is installed.
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.set_vocab(v).expect("set_vocab");

    // A LATER insert interns `autograph` as a dense id. The fix interned both alias forms at
    // activation, so the equivalence map already keys on that same dense id.
    eng.try_insert_live("autograph card9", 1, 1)
        .expect("insert");

    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "autographs card9").contains(&1),
        "alias must survive a future insert: an autographs title matches the autograph query"
    );
}

/// (4) Applying an alias recompiles already-stored queries in place — no restart, no full
/// rebuild — so an existing query gains the alias's reach immediately, zero false negatives.
#[test]
fn vocab_apply_recompiles_existing_queries_without_restart() {
    let mut queries: Vec<(u64, String)> = vec![(1, "fleer autograph".into())];
    for i in 0..6u64 {
        queries.push((200 + i, format!("autographs u{i}"))); // intern `autographs`
    }
    let title = "fleer autographs psa 10";

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    assert!(
        !matched(&mut eng, &mut s, title).contains(&1),
        "before the alias, the autograph-query must not match an autographs title"
    );

    // Import a declared single-token alias and apply it live.
    let report = eng
        .import_alias_synonyms("autograph, autographs")
        .expect("import + apply");
    assert!(
        report.recompiled >= 1,
        "the existing query must be recompiled in place (recompiled={})",
        report.recompiled
    );

    assert!(
        matched(&mut eng, &mut s, title).contains(&1),
        "the pre-existing query matches the autographs title after a live apply (no restart)"
    );
}

/// (5) A multi-word alias is recorded as a candidate (a token-graph problem deferred to Phase
/// 2) and is NOT activated — even when declared — so the Phase-1 matcher is untouched.
#[test]
fn multiword_alias_candidate_is_recorded_but_not_activated() {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    // Seed a stored query so the engine has a corpus; `ud` and `upper deck` stay distinct.
    eng.build_from_queries(&[(1, "ud rare".into()), (2, "upper deck rare".into())]);

    let report = eng
        .import_alias_synonyms("ud => upper deck")
        .expect("import");
    assert_eq!(
        report.activated, 0,
        "a multi-word alias must never auto-activate (Phase 2)"
    );
    let entry = find_entry(&eng, &["ud", "upper deck"]).expect("multi-word group recorded");
    assert_eq!(entry.kind, AliasKind::MultiWord);
    assert_eq!(entry.status, AliasStatus::Candidate);
    assert!(
        eng.aliases().expect("vocab").active_groups().is_empty(),
        "no multi-word group may be active in Phase 1"
    );

    // The matcher is untouched: a `ud` query does NOT reach an `upper deck` title via the alias.
    let mut s = MatchScratch::new();
    assert!(
        !matched(&mut eng, &mut s, "upper deck rare").contains(&1),
        "the multi-word alias must not silently bridge ud → upper deck"
    );
}

/// At scale: applying an active registry alias is FN-safe — even a nonsense variant alias can
/// only ADD candidates, never drop a true match the original semantics had. Mirrors the ADR-054
/// wrong-equivalence proof but drives it through the ADR-060 registry apply path.
#[test]
fn alias_registry_application_is_fn_safe_at_scale() {
    let cfg = GenConfig {
        num_queries: 6_000,
        num_titles: 2_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0A11_A500,
        num_players: 500,
        num_sets: 250,
    };
    let data = generate(&cfg);

    // Intern two nonsense single-token *variants* (shared prefix ⇒ auto-active when declared).
    let mut queries = data.queries.clone();
    for i in 0..20u64 {
        queries.push((9_000_000 + i, format!("wibblea u{i}")));
        queries.push((9_100_000 + i, format!("wibbleb u{i}")));
    }

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let brute = Brute::build(&queries); // ground truth under ORIGINAL semantics (no alias)

    let report = eng
        .import_alias_synonyms("wibblea, wibbleb")
        .expect("import + apply");
    assert!(
        report.activated >= 1,
        "the nonsense variant must auto-activate"
    );

    let mut s = MatchScratch::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let (mut false_neg, mut total_truth) = (0usize, 0usize);
    for title in &data.titles {
        let engine_set = matched(&mut eng, &mut s, title);
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        false_neg += truth.iter().filter(|t| !engine_set.contains(t)).count();
    }
    assert_eq!(
        false_neg, 0,
        "applying a registry alias must never drop a true match (structural FN-safety)"
    );
    assert!(total_truth > 0, "degenerate test: no matches");
}
