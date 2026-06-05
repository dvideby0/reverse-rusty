//! Unit tests for the alias registry, its structural classifier, and the Solr import.

use super::classify::{classify_kind, default_status_for, AliasKind};
use super::solr::parse_solr_synonyms;
use super::{AliasProvenance, AliasRegistry, AliasStatus};
use crate::dict::Dict;
use crate::normalize::Normalizer;

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("default normalizer")
}

fn forms(fs: &[&str]) -> Vec<String> {
    fs.iter().map(|s| (*s).to_string()).collect()
}

// ── Classifier ───────────────────────────────────────────────────────────────

#[test]
fn single_token_variant_pair_is_variant_kind() {
    let dict = Dict::new();
    // Plurals / truncations share a >=3 char prefix → variant.
    assert_eq!(
        classify_kind(&forms(&["refractor", "refractors"]), &norm(), &dict),
        AliasKind::SingleTokenVariant
    );
    assert_eq!(
        classify_kind(&forms(&["autograph", "autographed"]), &norm(), &dict),
        AliasKind::SingleTokenVariant
    );
}

#[test]
fn distinct_single_tokens_are_not_variants() {
    let dict = Dict::new();
    // Graders: no shared prefix → distinct (the category-alternatives case).
    assert_eq!(
        classify_kind(&forms(&["psa", "bgs", "sgc"]), &norm(), &dict),
        AliasKind::SingleTokenDistinct
    );
    // A 2-form distinct pair is still "distinct", not a variant.
    assert_eq!(
        classify_kind(&forms(&["psa", "bgs"]), &norm(), &dict),
        AliasKind::SingleTokenDistinct
    );
}

#[test]
fn multi_token_form_is_multiword_kind() {
    let dict = Dict::new();
    assert_eq!(
        classify_kind(&forms(&["ud", "upper deck"]), &norm(), &dict),
        AliasKind::MultiWord
    );
}

#[test]
fn phrase_backed_multiword_form_stays_multiword() {
    // Even when the vocab has a phrase rule that folds "upper deck" into ONE feature, the raw
    // surface form is still multi-word and must classify as MultiWord (a Phase-2 candidate) — the
    // Phase-1 boundary can't depend on which phrases happen to exist (Codex review, ADR-060).
    use crate::normalize::NormalizerBuilder;
    let mut b = NormalizerBuilder::new();
    b.add_phrase(
        &["upper", "deck"],
        "term:upper_deck",
        crate::dict::FeatureKind::Generic,
    );
    let n = b.build().expect("normalizer");
    let mut dict = Dict::new();
    let mut lc = String::new();
    // Sanity: the phrase really does fold "upper deck" to a single feature.
    assert_eq!(
        n.compile_features("upper deck", &mut dict, &mut lc).len(),
        1
    );
    assert_eq!(
        classify_kind(&forms(&["ud", "upper deck"]), &n, &dict),
        AliasKind::MultiWord
    );
}

#[test]
fn mixed_known_kinds_are_mixedkind() {
    // Intern two forms with different KNOWN kinds, then a group spanning them is MixedKind.
    let mut dict = Dict::new();
    let n = norm();
    let mut lc = String::new();
    // compile_features interns; force a Brand and a Player kind via the dict directly.
    let brand = dict.intern("term:topps", crate::dict::FeatureKind::Brand);
    let player = dict.intern("term:jordan", crate::dict::FeatureKind::Player);
    assert_ne!(brand, player);
    // The forms must normalize to exactly those interned features.
    let tb = n.compile_features_readonly("topps", &dict, &mut lc);
    let tj = n.compile_features_readonly("jordan", &dict, &mut lc);
    assert_eq!(tb, vec![brand]);
    assert_eq!(tj, vec![player]);
    assert_eq!(
        classify_kind(&forms(&["topps", "jordan"]), &n, &dict),
        AliasKind::MixedKind
    );
}

// ── Auto-activation policy ─────────────────────────────────────────────────────

#[test]
fn policy_activates_variants_and_declared_distincts_only() {
    use AliasKind::{MixedKind, MultiWord, SingleTokenDistinct, SingleTokenVariant};
    use AliasProvenance::{DeclaredFile, LearnedFromQueries, Manual};
    use AliasStatus::{Active, Candidate};

    // Variants: active from any source.
    assert_eq!(
        default_status_for(SingleTokenVariant, LearnedFromQueries),
        Active
    );
    assert_eq!(default_status_for(SingleTokenVariant, DeclaredFile), Active);
    // Distinct single tokens: declared/manual active, learned → candidate.
    assert_eq!(
        default_status_for(SingleTokenDistinct, DeclaredFile),
        Active
    );
    assert_eq!(default_status_for(SingleTokenDistinct, Manual), Active);
    assert_eq!(
        default_status_for(SingleTokenDistinct, LearnedFromQueries),
        Candidate
    );
    // Multi-word / mixed: never auto-active.
    assert_eq!(default_status_for(MultiWord, DeclaredFile), Candidate);
    assert_eq!(default_status_for(MixedKind, Manual), Candidate);
}

// ── Solr parsing ──────────────────────────────────────────────────────────────

#[test]
fn solr_parses_lists_mappings_and_comments() {
    let text = "\
# a comment line
refractor, refractors

ipod, i-pod, i pod
foozball => foosball
sea biscuit, sea biscit => seabiscuit
";
    let groups = parse_solr_synonyms(text);
    // refractor/refractors
    assert!(groups
        .iter()
        .any(|g| g == &forms(&["refractor", "refractors"])));
    // ipod list (sorted): "i pod", "i-pod", "ipod"
    assert!(groups
        .iter()
        .any(|g| g.contains(&"ipod".to_string()) && g.contains(&"i pod".to_string())));
    // mapping unioned bidirectionally
    assert!(groups
        .iter()
        .any(|g| g == &forms(&["foosball", "foozball"])));
    // multi-word mapping union
    assert!(groups
        .iter()
        .any(|g| g.contains(&"seabiscuit".to_string()) && g.contains(&"sea biscuit".to_string())));
    // the comment line produced no group
    assert!(!groups.iter().any(|g| g.iter().any(|f| f.contains('#'))));
}

#[test]
fn solr_escaped_comma_is_literal() {
    let groups = parse_solr_synonyms(r"a\,b, c");
    assert_eq!(groups, vec![forms(&["a,b", "c"])]);
}

// ── Registry behavior ──────────────────────────────────────────────────────────

#[test]
fn registry_active_groups_only_includes_active_single_token() {
    let mut reg = AliasRegistry::new();
    let n = norm();
    let dict = Dict::new();

    // variant → active
    reg.add_classified(
        &forms(&["refractor", "refractors"]),
        AliasProvenance::LearnedFromQueries,
        0.5,
        &n,
        &dict,
    );
    // learned distinct → candidate
    reg.add_classified(
        &forms(&["psa", "bgs", "sgc"]),
        AliasProvenance::LearnedFromQueries,
        0.5,
        &n,
        &dict,
    );
    // declared multi-word → candidate (matcher can't express it)
    reg.add_classified(
        &forms(&["ud", "upper deck"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );

    let active = reg.active_groups();
    assert_eq!(active, vec![forms(&["refractor", "refractors"])]);
    let s = reg.summary();
    assert_eq!((s.active, s.candidate, s.rejected), (1, 2, 0));
}

#[test]
fn declared_distinct_activates_but_learned_does_not() {
    let n = norm();
    let dict = Dict::new();

    let mut learned = AliasRegistry::new();
    assert_eq!(
        learned.add_classified(
            &forms(&["psa", "bgs"]),
            AliasProvenance::LearnedFromQueries,
            0.5,
            &n,
            &dict
        ),
        Some(AliasStatus::Candidate)
    );

    let mut declared = AliasRegistry::new();
    assert_eq!(
        declared.add_classified(
            &forms(&["psa", "bgs"]),
            AliasProvenance::DeclaredFile,
            1.0,
            &n,
            &dict
        ),
        Some(AliasStatus::Active)
    );
}

#[test]
fn declared_import_upgrades_a_learned_candidate() {
    let n = norm();
    let dict = Dict::new();
    let mut reg = AliasRegistry::new();
    // First learned as a candidate (distinct single tokens).
    reg.add_classified(
        &forms(&["psa", "bgs"]),
        AliasProvenance::LearnedFromQueries,
        0.5,
        &n,
        &dict,
    );
    assert!(reg.active_groups().is_empty());
    // An operator then declares the same pair → upgraded to active.
    reg.import_solr("psa, bgs", &n, &dict);
    assert_eq!(reg.active_groups(), vec![forms(&["bgs", "psa"])]);
}

#[test]
fn reimport_reports_zero_newly_active() {
    let n = norm();
    let dict = Dict::new();
    let mut reg = AliasRegistry::new();
    // First import activates the variant pair.
    assert_eq!(reg.import_solr("refractor, refractors", &n, &dict), 1);
    // Re-importing the same (already-active) group activates nothing new — idempotent.
    assert_eq!(reg.import_solr("refractor, refractors", &n, &dict), 0);
    assert_eq!(reg.len(), 1, "a re-import must not duplicate the entry");
}

#[test]
fn reject_blocks_reactivation_by_relearn() {
    let n = norm();
    let dict = Dict::new();
    let mut reg = AliasRegistry::new();
    reg.add_classified(
        &forms(&["refractor", "refractors"]),
        AliasProvenance::LearnedFromQueries,
        0.5,
        &n,
        &dict,
    );
    assert!(reg.reject(&forms(&["refractor", "refractors"])));
    assert!(reg.active_groups().is_empty());
    // A re-learn must NOT resurrect a rejected group.
    let acts = reg.learn_from_queries(
        &(0..5)
            .map(|i| (i, "(refractor,refractors)".to_string()))
            .collect::<Vec<_>>(),
        2,
        &n,
        &dict,
    );
    assert_eq!(acts, 0);
    assert!(reg.active_groups().is_empty());
}

#[test]
fn activate_refuses_multiword() {
    let n = norm();
    let dict = Dict::new();
    let mut reg = AliasRegistry::new();
    reg.add_classified(
        &forms(&["ud", "upper deck"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    // Even an explicit activate can't turn on a kind the Phase-1 matcher can't express.
    assert!(!reg.activate(&forms(&["ud", "upper deck"])));
    assert!(reg.active_groups().is_empty());
}

#[test]
fn json_round_trips() {
    let n = norm();
    let dict = Dict::new();
    let mut reg = AliasRegistry::new();
    reg.add_classified(
        &forms(&["refractor", "refractors"]),
        AliasProvenance::LearnedFromQueries,
        0.5,
        &n,
        &dict,
    );
    reg.add_classified(
        &forms(&["ud", "upper deck"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    let json = serde_json::to_string(&reg).unwrap();
    let back: AliasRegistry = serde_json::from_str(&json).unwrap();
    assert_eq!(back.len(), 2);
    assert_eq!(back.active_groups(), reg.active_groups());
}
