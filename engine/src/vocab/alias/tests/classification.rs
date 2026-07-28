use super::*;

#[test]
fn single_token_variant_pair_is_variant_kind() {
    let dict = Dict::new();
    // Plurals / truncations share a >=3 char prefix → variant.
    assert_eq!(
        classify_kind(&forms(&["premium", "premiums"]), &norm(), &dict),
        AliasKind::SingleTokenVariant
    );
    assert_eq!(
        classify_kind(&forms(&["adapter", "adapters"]), &norm(), &dict),
        AliasKind::SingleTokenVariant
    );
}

#[test]
fn distinct_single_tokens_are_not_variants() {
    let dict = Dict::new();
    // Unrelated categories share no prefix and remain distinct.
    assert_eq!(
        classify_kind(&forms(&["red", "blue", "green"]), &norm(), &dict),
        AliasKind::SingleTokenDistinct
    );
    // A 2-form distinct pair is still "distinct", not a variant.
    assert_eq!(
        classify_kind(&forms(&["red", "blue"]), &norm(), &dict),
        AliasKind::SingleTokenDistinct
    );
}

#[test]
fn multi_token_form_is_multiword_kind() {
    let dict = Dict::new();
    assert_eq!(
        classify_kind(&forms(&["cordless", "wireless mouse"]), &norm(), &dict),
        AliasKind::MultiWord
    );
}

#[test]
fn phrase_backed_multiword_form_stays_multiword() {
    // Even when the vocab has a phrase rule that folds "wireless mouse" into ONE feature, the raw
    // surface form is still multi-word and must classify as MultiWord (a learned candidate) — the
    // classifier boundary can't depend on which phrases happen to exist (Codex review, ADR-060).
    use crate::normalize::NormalizerBuilder;
    let mut b = NormalizerBuilder::new();
    b.add_phrase(
        &["wireless", "mouse"],
        "entity:wireless_mouse",
        crate::dict::FeatureKind::Generic,
    );
    let n = b.build().expect("normalizer");
    let mut dict = Dict::new();
    let mut lc = String::new();
    // Sanity: the phrase really does fold the form to a single feature.
    assert_eq!(
        n.compile_features("wireless mouse", &mut dict, &mut lc)
            .len(),
        1
    );
    assert_eq!(
        classify_kind(&forms(&["cordless", "wireless mouse"]), &n, &dict),
        AliasKind::MultiWord
    );
}

#[test]
fn mixed_known_kinds_are_mixedkind() {
    // Intern two forms with different KNOWN kinds, then a group spanning them is MixedKind.
    let mut dict = Dict::new();
    let n = norm();
    let mut lc = String::new();
    // Force a Brand and an Entity kind via the dict directly.
    let brand = dict.intern("term:acme", crate::dict::FeatureKind::Brand);
    let entity = dict.intern("term:widget", crate::dict::FeatureKind::Entity);
    assert_ne!(brand, entity);
    // The forms must normalize to exactly those interned features.
    let tb = n.compile_features_readonly("acme", &dict, &mut lc);
    let tj = n.compile_features_readonly("widget", &dict, &mut lc);
    assert_eq!(tb, vec![brand]);
    assert_eq!(tj, vec![entity]);
    assert_eq!(
        classify_kind(&forms(&["acme", "widget"]), &n, &dict),
        AliasKind::MixedKind
    );
}

#[test]
fn cross_kind_multiword_is_mixedkind_not_multiword() {
    // ADR-061 (codex review): a multi-word group whose forms resolve to DIFFERENT known kinds (a
    // Brand phrase equivalent to an Entity phrase must classify as MixedKind — a review candidate — NOT
    // auto-activate as MultiWord. The mixed-kind check runs before the multi-word classification,
    // and resolves the kinds of multi-word forms too.
    use crate::normalize::NormalizerBuilder;
    let mut b = NormalizerBuilder::new();
    b.add_phrase(
        &["acme", "labs"],
        "brand:acme_labs",
        crate::dict::FeatureKind::Brand,
    );
    b.add_phrase(
        &["wireless", "mouse"],
        "entity:wireless_mouse",
        crate::dict::FeatureKind::Entity,
    );
    let n = b.build().expect("normalizer");
    // Intern each phrase entity with its kind so the forms resolve to KNOWN (non-Generic) kinds.
    let mut dict = Dict::new();
    let mut lc = String::new();
    let _ = n.compile_features("acme labs", &mut dict, &mut lc);
    let _ = n.compile_features("wireless mouse", &mut dict, &mut lc);
    assert_eq!(
        classify_kind(&forms(&["acme labs", "wireless mouse"]), &n, &dict),
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
    // (a) Zero-feature form: an all-punctuation surface cleans to nothing.
    let n = norm();
    let dict = Dict::new();
    assert_eq!(
        classify_kind(&forms(&["foo", "@@@"]), &n, &dict),
        AliasKind::MixedKind,
        "a zero-feature single-token form must stay a candidate"
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
            &forms(&["premium", "premiums"]),
            AliasProvenance::LearnedDistributional,
            0.7,
            &n,
            &d
        ),
        Some(AliasStatus::Candidate),
        "even a variant-looking pair lands Candidate from discovery"
    );
    assert!(reg.reject(&forms(&["premium", "premiums"])));
    assert_eq!(
        reg.add_classified(
            &forms(&["premium", "premiums"]),
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
            &forms(&["north", "northstar"]),
            AliasProvenance::LearnedDistributional,
            0.6,
            &n,
            &d
        ),
        Some(AliasStatus::Candidate)
    );
    assert_eq!(
        reg.add_classified(
            &forms(&["north", "northstar"]),
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
        .find(|e| e.forms == forms(&["north", "northstar"]))
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
            &forms(&["north", "northstar"]),
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
