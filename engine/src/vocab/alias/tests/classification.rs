use super::*;

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
    // surface form is still multi-word and must classify as MultiWord (a learned candidate) — the
    // classifier boundary can't depend on which phrases happen to exist (Codex review, ADR-060).
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

#[test]
fn cross_kind_multiword_is_mixedkind_not_multiword() {
    // ADR-061 (codex review): a multi-word group whose forms resolve to DIFFERENT known kinds (a
    // Brand phrase ≡ a Player phrase) must classify as MixedKind — a review candidate — NOT
    // auto-activate as MultiWord. The mixed-kind check runs before the multi-word classification,
    // and resolves the kinds of multi-word forms too.
    use crate::normalize::NormalizerBuilder;
    let mut b = NormalizerBuilder::new();
    b.add_phrase(
        &["upper", "deck"],
        "brand:upper_deck",
        crate::dict::FeatureKind::Brand,
    );
    b.add_phrase(
        &["michael", "jordan"],
        "player:mj",
        crate::dict::FeatureKind::Player,
    );
    let n = b.build().expect("normalizer");
    // Intern each phrase entity with its kind so the forms resolve to KNOWN (non-Generic) kinds.
    let mut dict = Dict::new();
    let mut lc = String::new();
    let _ = n.compile_features("upper deck", &mut dict, &mut lc);
    let _ = n.compile_features("michael jordan", &mut dict, &mut lc);
    assert_eq!(
        classify_kind(&forms(&["upper deck", "michael jordan"]), &n, &dict),
        AliasKind::MixedKind,
        "a cross-kind multi-word group must not bypass the MixedKind refusal"
    );
}

#[test]
fn unexpressible_single_token_forms_are_candidates_not_active() {
    // ADR-061 (codex review): a single-token form that does NOT reduce to exactly one feature
    // cannot be registered as an alias phrase, and `resolve_equivalences` would drop it — so it
    // must classify as MixedKind (a review candidate), never auto-activate a group that would be
    // reported active yet silently never match.
    use crate::normalize::NormalizerBuilder;

    // (a) Zero-feature form: an all-punctuation surface cleans to nothing.
    let n = norm();
    let dict = Dict::new();
    assert_eq!(
        classify_kind(&forms(&["foo", "@@@"]), &n, &dict),
        AliasKind::MixedKind,
        "a zero-feature single-token form must stay a candidate"
    );

    // (b) Fused grader: `psa10` resolves to grader:psa + grade:10 (one cleaned token, two
    //     features) — the case codex flagged.
    let g = NormalizerBuilder::new().grader("psa").build().unwrap();
    let gdict = Dict::new();
    assert_eq!(
        classify_kind(&forms(&["psa10", "card"]), &g, &gdict),
        AliasKind::MixedKind,
        "a fused-grader single-token form must stay a candidate"
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
    // Multi-word (ADR-061): declared/manual active, learned → candidate (like distinct tokens).
    assert_eq!(default_status_for(MultiWord, DeclaredFile), Active);
    assert_eq!(default_status_for(MultiWord, Manual), Active);
    assert_eq!(default_status_for(MultiWord, LearnedFromQueries), Candidate);
    // Mixed-kind: never auto-active (the matcher still can't express it safely).
    assert_eq!(default_status_for(MixedKind, Manual), Candidate);
    assert_eq!(default_status_for(MixedKind, DeclaredFile), Candidate);
}

#[test]
fn distributional_provenance_never_auto_activates() {
    // ADR-102: review-first, ALWAYS — even a pair the structural classifier calls a clear
    // variant (which auto-activates from every other source) stays a candidate.
    use AliasKind::{MixedKind, MultiWord, SingleTokenDistinct, SingleTokenVariant};
    use AliasProvenance::LearnedDistributional;
    use AliasStatus::Candidate;
    for kind in [
        SingleTokenVariant,
        SingleTokenDistinct,
        MultiWord,
        MixedKind,
    ] {
        assert_eq!(
            default_status_for(kind, LearnedDistributional),
            Candidate,
            "{kind:?} from distributional discovery must land as a review candidate"
        );
    }
}

#[test]
fn distributional_rediscovery_respects_rejection_and_cannot_promote() {
    // A rejected group stays rejected on re-discovery; a re-discovered candidate only maxes
    // its confidence (the same-rank promotion branch requires a computed Active, which the
    // distributional provenance never produces).
    let n = norm();
    let d = Dict::new();
    let mut reg = AliasRegistry::default();

    // Seed a candidate via discovery, reject it, re-discover: stays rejected.
    assert_eq!(
        reg.add_classified(
            &forms(&["refractor", "refractors"]),
            AliasProvenance::LearnedDistributional,
            0.7,
            &n,
            &d
        ),
        Some(AliasStatus::Candidate),
        "even a variant-looking pair lands Candidate from discovery"
    );
    assert!(reg.reject(&forms(&["refractor", "refractors"])));
    assert_eq!(
        reg.add_classified(
            &forms(&["refractor", "refractors"]),
            AliasProvenance::LearnedDistributional,
            0.9,
            &n,
            &d
        ),
        Some(AliasStatus::Rejected),
        "re-discovery must not resurrect an operator rejection"
    );

    // A re-discovered candidate refreshes confidence upward, never status.
    assert_eq!(
        reg.add_classified(
            &forms(&["ud", "upperdeck"]),
            AliasProvenance::LearnedDistributional,
            0.6,
            &n,
            &d
        ),
        Some(AliasStatus::Candidate)
    );
    assert_eq!(
        reg.add_classified(
            &forms(&["ud", "upperdeck"]),
            AliasProvenance::LearnedDistributional,
            0.8,
            &n,
            &d
        ),
        Some(AliasStatus::Candidate),
        "same-rank re-add with a Candidate default cannot promote"
    );
    let e = reg
        .entries()
        .iter()
        .find(|e| e.forms == forms(&["ud", "upperdeck"]))
        .expect("entry recorded");
    assert!(
        (e.confidence - 0.8).abs() < 1e-12,
        "confidence reconciles by max (got {})",
        e.confidence
    );
    assert_eq!(e.status, AliasStatus::Candidate);

    // A later DECLARED import of the same group still upgrades it (higher trust wins) — the
    // distributional seed must not block the existing reconciliation ladder.
    assert_eq!(
        reg.add_classified(
            &forms(&["ud", "upperdeck"]),
            AliasProvenance::DeclaredFile,
            1.0,
            &n,
            &d
        ),
        Some(AliasStatus::Active),
        "declared provenance outranks and re-decides"
    );
}

// ── Solr parsing ──────────────────────────────────────────────────────────────
