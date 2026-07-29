use super::*;

#[test]
fn json_round_trips() {
    let n = norm();
    let dict = Dict::new();
    let mut reg = AliasRegistry::new();
    reg.add_classified(
        &forms(&["premium", "premiums"]),
        AliasProvenance::LearnedFromQueries,
        0.5,
        &n,
        &dict,
    );
    reg.add_classified(
        &forms(&["ns", "north star"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    let json = serde_json::to_string(&reg).unwrap();
    let back: AliasRegistry = serde_json::from_str(&json).unwrap();
    assert_eq!(back.len(), 2);
    assert_eq!(back.active_groups(), reg.active_groups());

    // ADR-102: the distributional provenance round-trips (snake_case, one-directional compat —
    // an old binary cannot read this JSON, the repo's stated format-forward stance).
    reg.add_classified(
        &forms(&["deluxe", "deluxeplus"]),
        AliasProvenance::LearnedDistributional,
        0.66,
        &n,
        &dict,
    );
    let json = serde_json::to_string(&reg).unwrap();
    assert!(json.contains("learned_distributional"));
    let back: AliasRegistry = serde_json::from_str(&json).unwrap();
    let e = back
        .entries()
        .iter()
        .find(|e| e.forms == forms(&["deluxe", "deluxeplus"]))
        .expect("distributional entry survives the round-trip");
    assert_eq!(e.provenance, AliasProvenance::LearnedDistributional);
    assert_eq!(e.status, AliasStatus::Candidate);
}

// ── Match-feedback (ADR-103) registry surface ─────────────────────────────────

#[test]
fn record_feedback_stamps_evidence_and_maxes_confidence() {
    use crate::vocab::FeedbackEvidence;
    let n = norm();
    let d = Dict::new();
    let mut reg = AliasRegistry::default();
    reg.add_classified(
        &forms(&["ns", "northstar"]),
        AliasProvenance::LearnedDistributional,
        0.6,
        &n,
        &d,
    );
    let ev = FeedbackEvidence {
        overlap: 0.85,
        titles_a: 100,
        titles_b: 90,
        queries_sampled: 40,
    };
    assert!(reg.record_feedback(&forms(&["ns", "northstar"]), ev));
    let e = &reg.entries()[0];
    assert_eq!(e.feedback, Some(ev));
    assert!(
        (e.confidence - 0.85).abs() < 1e-12,
        "confidence raised to overlap"
    );
    assert_eq!(
        e.status,
        AliasStatus::Candidate,
        "stamping never changes status"
    );
    assert!(
        !reg.record_feedback(&forms(&["ns", "northstar"]), ev),
        "an identical evidence retry is a no-op"
    );
    // A NaN overlap must not poison confidence.
    let nan = FeedbackEvidence {
        overlap: f64::NAN,
        ..ev
    };
    assert!(reg.record_feedback(&forms(&["ns", "northstar"]), nan));
    assert!((reg.entries()[0].confidence - 0.85).abs() < 1e-12);
    assert!(!reg.record_feedback(&forms(&["nope", "missing"]), ev));
}

#[test]
fn activate_validated_refuses_rejected_and_mixed_kind() {
    let n = norm();
    let d = Dict::new();
    let mut reg = AliasRegistry::default();
    reg.add_classified(
        &forms(&["ns", "northstar"]),
        AliasProvenance::LearnedDistributional,
        0.9,
        &n,
        &d,
    );
    reg.add_classified(
        &forms(&["pkg", "new"]),
        AliasProvenance::LearnedDistributional,
        0.9,
        &n,
        &d,
    );
    assert!(reg.reject(&forms(&["pkg", "new"])));

    // The automated path promotes a candidate…
    assert!(reg.activate_validated(&forms(&["ns", "northstar"])));
    // …idempotently: an already-active entry reports false, so a racing second validate
    // pass never triggers a spurious full-recompile apply (codex).
    assert!(!reg.activate_validated(&forms(&["ns", "northstar"])));
    // …but must never resurrect an operator rejection (unlike the operator `activate`).
    assert!(!reg.activate_validated(&forms(&["pkg", "new"])));
    assert_eq!(
        reg.entries()
            .iter()
            .find(|e| e.forms == forms(&["new", "pkg"]))
            .unwrap()
            .status,
        AliasStatus::Rejected
    );
}

#[test]
fn feedback_field_round_trips_and_old_json_reads_none() {
    use crate::vocab::FeedbackEvidence;
    let n = norm();
    let d = Dict::new();
    let mut reg = AliasRegistry::default();
    reg.add_classified(
        &forms(&["ns", "northstar"]),
        AliasProvenance::LearnedDistributional,
        0.6,
        &n,
        &d,
    );
    // Old JSON (no `feedback` key) reads back as None.
    let old_json = serde_json::to_string(&reg).unwrap();
    assert!(!old_json.contains("feedback"), "absent until stamped");
    let back: AliasRegistry = serde_json::from_str(&old_json).unwrap();
    assert_eq!(back.entries()[0].feedback, None);
    // Stamped evidence round-trips.
    reg.record_feedback(
        &forms(&["ns", "northstar"]),
        FeedbackEvidence {
            overlap: 0.7,
            titles_a: 60,
            titles_b: 55,
            queries_sampled: 25,
        },
    );
    let json = serde_json::to_string(&reg).unwrap();
    let back: AliasRegistry = serde_json::from_str(&json).unwrap();
    assert!(back.entries()[0].feedback.is_some());
}
