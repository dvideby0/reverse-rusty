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

/// The fresh-persistent-startup variant of the ID-stability fix (Codex review): a server started
/// on an empty data dir with a `--vocab-file` carrying an active alias lands in `adopt_vocab` with
/// nothing compiled yet. The alias forms must be interned there too, or the first live insert
/// (mutating extract → dense id) diverges from the synthetic-keyed equivalence map and the alias
/// silently dies. `Engine::new` reproduces the "fresh, no queries" precondition.
#[test]
fn adopt_vocab_on_fresh_engine_keeps_alias_active_after_insert() {
    let cls_norm = Normalizer::default_vocab().expect("vocab");
    let cls_dict = Dict::new();
    let mut v = Vocab::new();
    v.aliases_mut().add_classified(
        &["autograph".into(), "autographs".into()],
        AliasProvenance::Manual,
        1.0,
        &cls_norm,
        &cls_dict,
    );

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab")); // fresh: no queries
    eng.adopt_vocab(v).expect("adopt_vocab");
    eng.try_insert_live("autograph card9", 1, 1)
        .expect("insert");

    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "autographs card9").contains(&1),
        "adopt on a fresh engine must intern alias forms so the alias survives a future insert"
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

/// Build an engine over `queries`, then import + apply the given Solr alias lines. A declared
/// multi-word alias now auto-activates (ADR-061). Returns the engine and the resulting vocab so a
/// brute reference can be built with the identical alias semantics.
fn engine_with_aliases(queries: &[(u64, String)], solr: &str) -> (Engine, Vocab) {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(queries);
    eng.import_alias_synonyms(solr)
        .expect("import + apply aliases");
    let vocab = eng.vocab().expect("vocab installed").clone();
    (eng, vocab)
}

/// (5) ADR-061: a **declared** multi-word alias now ACTIVATES (the Phase-2 matcher expresses it),
/// and it matches **bidirectionally** — a query phrased one way reaches a title phrased the other —
/// with zero false negatives. (Phase 1 recorded it as an inert candidate; this is the replacement.)
#[test]
fn multiword_alias_activates_and_matches_bidirectionally() {
    let queries: Vec<(u64, String)> = vec![
        (1, "ny mets".into()),          // single-token alias form `ny`
        (2, "new york yankees".into()), // multi-word alias form `new york`
    ];
    let (mut eng, _vocab) = engine_with_aliases(&queries, "ny => new york");

    let entry = find_entry(&eng, &["new york", "ny"]).expect("multi-word group recorded");
    assert_eq!(entry.kind, AliasKind::MultiWord);
    assert_eq!(
        entry.status,
        AliasStatus::Active,
        "a declared multi-word alias auto-activates in Phase 2"
    );

    let mut s = MatchScratch::new();
    // Forward: a `ny` query reaches a `new york` title.
    assert!(
        matched(&mut eng, &mut s, "new york mets").contains(&1),
        "ny query must match a new york title (alias forward)"
    );
    // Reverse: a `new york` query reaches a `ny` title.
    assert!(
        matched(&mut eng, &mut s, "ny yankees").contains(&2),
        "new york query must match a ny title (alias reverse)"
    );
}

/// (6) THE WALL (the case the abandoned flat-set attempt broke): a forbidden multi-word phrase is
/// checked against the canonical leftmost-longest view `N(T)`, NOT the overlapping superset. So
/// `foo -"new york"` MATCHES `foo new york city` (the canonical parse reads `new york city`, which
/// does not contain `new york`) but is correctly rejected on a literal `foo new york` title. A
/// single-view (or naive-superset) matcher would wrongly reject the city title — a false negative
/// in the most sacred area.
#[test]
fn multiword_alias_forbidden_uses_canonical_view() {
    let queries: Vec<(u64, String)> = vec![(1, "foo -\"new york\"".into())];
    let (mut eng, vocab) = engine_with_aliases(&queries, "ny => new york\nnyc => new york city");
    let brute = Brute::build_with_vocab(&queries, &vocab);

    let mut s = MatchScratch::new();
    let (mut lc, mut bf) = (String::new(), Vec::new());
    for (title, want) in [
        ("foo new york city", true), // canonical = new york city; new york NOT forbidden-present
        ("foo new york", false),     // literal new york IS forbidden-present
        ("foo brooklyn", true),      // unrelated → matches
    ] {
        let got = matched(&mut eng, &mut s, title).contains(&1);
        assert_eq!(got, want, "engine `foo -\"new york\"` vs `{title}`");
        assert_eq!(
            brute.matches(title, &mut lc, &mut bf).contains(&1),
            want,
            "brute `foo -\"new york\"` vs `{title}`"
        );
    }
}

/// (7) Overlapping / nested aliases (`new york` ⊂ `new york city`): the title positive superset
/// adds the nested entity, so a `new york` query finds a `new york city` title. A leftmost-longest
/// single view would miss it (it reads only the longer entity) — the retrieval-side FN the two
/// views fix.
#[test]
fn multiword_alias_overlapping_nested_retrieval() {
    let queries: Vec<(u64, String)> = vec![(1, "new york yankees".into())];
    let (mut eng, vocab) = engine_with_aliases(&queries, "ny => new york\nnyc => new york city");
    let brute = Brute::build_with_vocab(&queries, &vocab);

    let mut s = MatchScratch::new();
    let (mut lc, mut bf) = (String::new(), Vec::new());
    let title = "new york city yankees";
    assert!(
        matched(&mut eng, &mut s, title).contains(&1),
        "a new york query must match a new york city title (overlap superset)"
    );
    assert!(
        brute.matches(title, &mut lc, &mut bf).contains(&1),
        "brute agrees the nested alias retrieves"
    );
}

/// (8b) Activating a multi-word alias must not DISPLACE a pre-existing overlapping phrase (codex
/// R6). With a declared `york city` phrase, activating `ny ⇒ new york` adds `new york` to the
/// leftmost-longest automaton, which would otherwise suppress `york city` on a `new york city`
/// title — a false negative for a `york city` query. The positive superset must re-include the
/// displaced phrase entity.
#[test]
fn activating_alias_does_not_drop_an_overlapping_existing_phrase() {
    let mut v = Vocab::new();
    v.add_phrase(
        &["york", "city"],
        "term:york_city",
        reverse_rusty::dict::FeatureKind::Generic,
    );
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.set_vocab(v).expect("install the york city phrase");
    eng.build_from_queries(&[(1, "york city".into())]);

    let title = "new york city yankees";
    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, title).contains(&1),
        "baseline: before the alias, a york city query matches a new york city title"
    );

    eng.import_alias_synonyms("ny => new york")
        .expect("apply the new york alias");
    assert!(
        matched(&mut eng, &mut s, title).contains(&1),
        "activating the new york alias must not drop the overlapping york city query (FN-safety)"
    );
}

/// (8c) A displaced alias phrase keeps its COMPONENTS in the positive view (codex R7). When an
/// overlapping COLLAPSING phrase (`new york`) displaces an alias phrase (`york city`) from the
/// leftmost-longest parse — consuming the shared `york` token — a stored `york` query must still
/// match: the maximal positive view re-emits every token feature, so `term:york` is present.
#[test]
fn displaced_alias_phrase_keeps_its_components() {
    let mut v = Vocab::new();
    v.add_phrase(
        &["new", "york"],
        "term:new_york",
        reverse_rusty::dict::FeatureKind::Generic,
    ); // a COLLAPSING phrase (consumes new + york)
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.set_vocab(v).expect("install the new york phrase");
    eng.build_from_queries(&[(1, "york".into())]); // a component-token query
    eng.import_alias_synonyms("yc => york city")
        .expect("apply the york city alias");

    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "new york city").contains(&1),
        "a york query must match new york city even though `new york` collapses `york` and \
         `york city` is displaced from the leftmost-longest parse"
    );
}

/// (8d) An alias matches a title with whitespace runs (`new  york`) via the positive-view overlap
/// scan, which collapses runs — WITHOUT collapsing the canonical/compile features (so persisted
/// segments stay byte-identical across versions, codex R8). Recall-safe: the overlap pass only
/// adds to `P(T)`.
#[test]
fn multiword_alias_matches_a_double_space_title() {
    let queries: Vec<(u64, String)> = vec![(1, "ny mets".into())];
    let (mut eng, _vocab) = engine_with_aliases(&queries, "ny => new york");
    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "new  york mets").contains(&1),
        "the ny alias must match a double-spaced `new  york` title (positive-view overlap scan)"
    );
}

/// (8) The title side stays additive: a pre-existing component-token query (`york`) still matches a
/// `new york` title after the alias activates — the alias must never drop a component match.
#[test]
fn multiword_alias_title_is_additive_for_components() {
    let queries: Vec<(u64, String)> = vec![(1, "york".into())];
    let (mut eng, _vocab) = engine_with_aliases(&queries, "ny => new york");
    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "new york mets").contains(&1),
        "component-token query `york` matches a `new york` title (additive title side)"
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

/// (9) The Phase-2 differential oracle: over a query mix that exercises bidirectional, overlapping,
/// forbidden-over-multi-word, component-token, and any-of cases, the engine is **exactly** the
/// alias-aware brute (both no false negatives AND no false positives) for every title.
#[test]
fn multiword_alias_differential_matches_brute() {
    let queries: Vec<(u64, String)> = vec![
        (1, "ny mets".into()),
        (2, "new york yankees".into()),
        (3, "new york -mets".into()),
        (4, "foo -\"new york\"".into()),
        (5, "york".into()),
        (6, "new york city subway".into()),
        (7, "(ny,boston) finals".into()),
        (8, "brooklyn".into()),
    ];
    let (mut eng, vocab) = engine_with_aliases(&queries, "ny => new york\nnyc => new york city");
    let brute = Brute::build_with_vocab(&queries, &vocab);

    let titles = [
        "new york mets opening day",
        "ny yankees world series",
        "new york city subway map",
        "foo new york city skyline",
        "foo new york state",
        "boston finals run",
        "brooklyn bridge",
        "york peppermint pattie",
        "ny mets vs boston",
        "new york city",
    ];
    let mut s = MatchScratch::new();
    let (mut lc, mut bf) = (String::new(), Vec::new());
    for title in titles {
        let mut got: Vec<u64> = matched(&mut eng, &mut s, title).into_iter().collect();
        let mut want: Vec<u64> = brute.matches(title, &mut lc, &mut bf).into_iter().collect();
        got.sort_unstable();
        want.sort_unstable();
        assert_eq!(got, want, "engine must equal brute for title `{title}`");
    }
}

/// (10) At scale: activating a multi-word alias is FN-safe. Every match the original (no-alias)
/// semantics had survives, while the two-view normalization + verify run on every generated title.
#[test]
fn multiword_alias_application_is_fn_safe_at_scale() {
    let cfg = GenConfig {
        num_queries: 6_000,
        num_titles: 2_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0A11_A5E2,
        num_players: 500,
        num_sets: 250,
    };
    let data = generate(&cfg);

    let mut queries = data.queries.clone();
    for i in 0..20u64 {
        queries.push((9_000_000 + i, format!("new york u{i}"))); // multi-word alias form
        queries.push((9_100_000 + i, format!("ny u{i}"))); // single-token alias form
    }

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let brute = Brute::build(&queries); // ground truth under ORIGINAL (no-alias) semantics

    let report = eng
        .import_alias_synonyms("ny => new york")
        .expect("import + apply");
    assert!(
        report.activated >= 1,
        "a declared multi-word alias must auto-activate"
    );

    let mut s = MatchScratch::new();
    let (mut blc, mut bfeats) = (String::new(), Vec::new());
    let (mut false_neg, mut total_truth) = (0usize, 0usize);
    for title in &data.titles {
        let engine_set = matched(&mut eng, &mut s, title);
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        false_neg += truth.iter().filter(|t| !engine_set.contains(t)).count();
    }
    assert_eq!(
        false_neg, 0,
        "activating a multi-word alias must never drop a true match (FN-safety at scale)"
    );
    assert!(total_truth > 0, "degenerate test: no matches");
}
