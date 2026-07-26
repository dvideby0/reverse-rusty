use super::*;

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
fn registry_active_groups_includes_variants_and_declared_multiword() {
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
    // declared multi-word → active (the Phase-2 matcher expresses it, ADR-061)
    reg.add_classified(
        &forms(&["ud", "upper deck"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );

    let active = reg.active_groups();
    assert_eq!(
        active,
        vec![
            forms(&["refractor", "refractors"]),
            forms(&["ud", "upper deck"])
        ]
    );
    // EVERY active entry's forms are offered for phrase registration (the builder re-derives
    // multi-wordness from the live punct table and skips the single-token ones, codex R11).
    assert_eq!(
        reg.active_alias_forms(),
        forms(&["refractor", "refractors", "ud", "upper deck"])
    );
    let s = reg.summary();
    assert_eq!((s.active, s.candidate, s.rejected), (2, 1, 0));
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
fn activate_accepts_multiword_refuses_mixed_kind() {
    let n = norm();
    let mut dict = Dict::new();
    let mut lc = String::new();
    // Intern two different KNOWN kinds so {topps, jordan} classifies as MixedKind.
    dict.intern("term:topps", crate::dict::FeatureKind::Brand);
    dict.intern("term:jordan", crate::dict::FeatureKind::Player);
    let _ = n.compile_features_readonly("topps", &dict, &mut lc);
    let mut reg = AliasRegistry::new();

    // A learned multi-word group lands as a candidate; explicit activate now succeeds (ADR-061).
    reg.add_classified(
        &forms(&["ny", "new york"]),
        AliasProvenance::LearnedFromQueries,
        0.5,
        &n,
        &dict,
    );
    assert!(
        reg.activate(&forms(&["ny", "new york"])),
        "multi-word activates in Phase 2"
    );
    assert_eq!(reg.active_alias_forms(), forms(&["new york", "ny"]));

    // Mixed-kind is still refused — the matcher cannot express a cross-kind expansion.
    reg.add_classified(
        &forms(&["topps", "jordan"]),
        AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &dict,
    );
    assert!(
        !reg.activate(&forms(&["jordan", "topps"])),
        "mixed-kind activation is refused"
    );
}
