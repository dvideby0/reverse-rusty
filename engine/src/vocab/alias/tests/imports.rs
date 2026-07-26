use super::*;

#[test]
fn reimport_upgrades_a_persisted_candidate_but_never_downgrades() {
    // ADR-061 (codex R7): a same-provenance re-import re-applies the current policy's default,
    // adopting a now-active status (so a persisted Phase-1 multi-word candidate activates when its
    // synonym file is re-imported under the Phase-2 policy) — but never downgrades a status, so a
    // re-learn cannot undo a manual activation.
    let n = norm();
    let dict = Dict::new();

    // (a) Upgrade: model a persisted declared multi-word Candidate, then re-import the same file.
    let mut reg = AliasRegistry::new();
    reg.add_classified(
        &forms(&["ny", "new york"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    reg.entries[0].status = AliasStatus::Candidate; // model the Phase-1 persisted state
    let status = reg.add_classified(
        &forms(&["ny", "new york"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    assert_eq!(
        status,
        Some(AliasStatus::Active),
        "re-importing the same declared file activates a persisted multi-word candidate"
    );

    // (b) No downgrade: a manually-activated learned distinct stays active across a re-learn.
    let mut reg2 = AliasRegistry::new();
    reg2.add_classified(
        &forms(&["psa", "bgs"]),
        AliasProvenance::LearnedFromQueries,
        0.5,
        &n,
        &dict,
    );
    assert!(reg2.activate(&forms(&["psa", "bgs"])), "manual activate");
    reg2.add_classified(
        &forms(&["psa", "bgs"]),
        AliasProvenance::LearnedFromQueries,
        0.9,
        &n,
        &dict,
    );
    assert_eq!(
        reg2.entries[0].status,
        AliasStatus::Active,
        "a re-learn must not downgrade a manual activation"
    );
}

#[test]
fn reimport_promotion_adopts_fresh_kind_but_active_keeps_kind() {
    // Codex R10: when a same-provenance re-import PROMOTES a candidate, it must adopt the fresh
    // `kind` too — a persisted entry can carry a stale classification (e.g. MixedKind from an
    // older classifier / dict state), and promoting the status alone would report Active while
    // `is_active_for_matching` keeps ignoring it (no equivalence, no phrase, silently dead).
    let n = norm();
    let dict = Dict::new();

    // (a) Promotion adopts the fresh kind: model a persisted declared candidate whose stored
    // kind is a stale MixedKind, then re-import the same declared file.
    let mut reg = AliasRegistry::new();
    reg.add_classified(
        &forms(&["ny", "new york"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    reg.entries[0].status = AliasStatus::Candidate;
    reg.entries[0].kind = AliasKind::MixedKind; // stale persisted classification
    let status = reg.add_classified(
        &forms(&["ny", "new york"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    assert_eq!(status, Some(AliasStatus::Active));
    assert_eq!(
        reg.entries[0].kind,
        AliasKind::MultiWord,
        "promotion must adopt the fresh kind, or the alias is active-but-unmatchable"
    );
    assert!(reg.entries[0].is_active_for_matching());
    assert_eq!(
        reg.active_alias_forms(),
        forms(&["new york", "ny"]),
        "the promoted multi-word group must reach the normalizer registration list"
    );

    // (b) An ALREADY-active entry keeps its kind on re-import (codex R9): re-classification must
    // not perturb a live entry's stored kind.
    let mut reg2 = AliasRegistry::new();
    reg2.add_classified(
        &forms(&["ny", "new york"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    assert_eq!(reg2.entries[0].status, AliasStatus::Active);
    reg2.entries[0].kind = AliasKind::SingleTokenDistinct; // simulate a divergent stored kind
    reg2.add_classified(
        &forms(&["ny", "new york"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    assert_eq!(
        reg2.entries[0].kind,
        AliasKind::SingleTokenDistinct,
        "an already-active entry's kind is preserved across a re-import"
    );
}

#[test]
fn demotion_is_status_only_so_a_repaired_vocab_reactivates() {
    // Codex R14: `demote_unexpressible` must not stamp `MixedKind` — that would dead-end the
    // entry, since `activate` structurally refuses that kind even after the operator repairs
    // the vocabulary. Status-only demotion keeps the path back open.
    let dict = Dict::new();
    let plain = norm(); // default punctuation: '-' splits ⇒ "psa-10" is a 2-token form
    let mut reg = AliasRegistry::new();
    let st = reg.add_classified(
        &forms(&["x", "psa-10"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &plain,
        &dict,
    );
    assert_eq!(st, Some(AliasStatus::Active));

    // The vocabulary then mutates: `psa` becomes a grader and `-` folds, so the form cleans
    // to the fused token `psa10` (several features) — unexpressible.
    let mut b = NormalizerBuilder::new();
    b.add_grader("psa");
    b.fold_punctuation('-');
    let broken = b.build().expect("broken norm");
    assert_eq!(reg.demote_unexpressible(&broken, &dict), 1);
    assert_eq!(reg.entries[0].status, AliasStatus::Candidate);
    assert_eq!(
        reg.entries[0].kind,
        AliasKind::MultiWord,
        "demotion is status-only: the kind snapshot is preserved"
    );

    // Repair: with the kind intact, manual activation succeeds again, and the entry is
    // expressible under the repaired vocabulary (no re-demotion).
    assert!(
        reg.activate(&forms(&["x", "psa-10"])),
        "a repaired candidate must re-activate"
    );
    assert_eq!(
        reg.demote_unexpressible(&plain, &dict),
        0,
        "expressible again under the repaired vocabulary"
    );
}
