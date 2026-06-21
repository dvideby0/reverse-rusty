use super::*;
use crate::dict::Dict;

#[test]
fn learn_discovers_synonyms_from_anyof_groups() {
    let queries: Vec<(u64, String)> = (0..20)
        .map(|i| (i, format!("(rookie,rc) somethingunique{i:03}")))
        .collect();
    let vocab = learn_from_queries(&queries, 2);
    assert!(
        !vocab.synonyms.is_empty() || !vocab.phrases.is_empty(),
        "should learn at least one synonym from repeated any-of groups"
    );
}

#[test]
fn learn_ignores_below_threshold() {
    let queries = vec![(1, "(alpha,beta) stuff".to_string())];
    let vocab = learn_from_queries(&queries, 5);
    assert!(
        vocab.is_empty(),
        "single occurrence should be below threshold of 5"
    );
}

#[test]
fn equivalence_only_vocab_is_not_empty() {
    // An expansion-mode vocab carrying only equivalence groups (no synonyms/
    // phrases/graders/grade-words) must NOT report empty: those groups are
    // recall-bearing and a "skip if empty" guard would otherwise drop them.
    let mut vocab = Vocab::new();
    assert!(vocab.is_empty(), "a fresh vocab starts empty");
    vocab.add_equivalence(&["rc", "rookie"]);
    assert!(
        !vocab.is_empty(),
        "a vocab with only an equivalence group must not be empty"
    );
    assert_eq!(vocab.len(), 1, "the equivalence group must be counted");
}

#[test]
fn learn_ignores_negated_groups() {
    let queries: Vec<(u64, String)> = (0..20)
        .map(|i| (i, format!("-(badterm,anotherbad) good{i:03}")))
        .collect();
    let vocab = learn_from_queries(&queries, 2);
    assert!(
        vocab.is_empty(),
        "negated groups should not produce synonyms"
    );
}

#[test]
fn learn_canonical_selection_is_deterministic_under_collision() {
    // `abk` co-occurs equally (5x each) with two eligible canonicals `abcd` and `abxy`.
    // Within each group the longer member is canonical, so we get the competing pairs
    // (abcd, abk)=5 and (abxy, abk)=5. The kept canonical must be the deterministic
    // tie-break winner (count tied -> lexicographically smallest canonical = "abcd"),
    // identical on every run regardless of HashMap iteration order. A non-deterministic
    // pick would let two replicas / a pre-vs-post-crash rebuild canonicalize `abk` to
    // different FeatureIds, breaking cluster consistency + the durability oracle.
    let mut queries: Vec<(u64, String)> = Vec::new();
    let mut id = 0u64;
    for _ in 0..5 {
        queries.push((id, "(abk,abcd) padonea".to_string()));
        id += 1;
        queries.push((id, "(abk,abxy) padtwob".to_string()));
        id += 1;
    }

    let first = learn_from_queries(&queries, 2);
    let chosen = first
        .synonyms
        .iter()
        .find(|s| s.token == "abk")
        .map(|s| s.canonical.clone());
    assert_eq!(
        chosen.as_deref(),
        Some("term:abcd"),
        "tie-break winner must be the lexicographically smallest canonical"
    );

    for _ in 0..40 {
        let v = learn_from_queries(&queries, 2);
        let again = v
            .synonyms
            .iter()
            .find(|s| s.token == "abk")
            .map(|s| s.canonical.clone());
        assert_eq!(
            again, chosen,
            "canonical selection must be identical on every run (hash-order independent)"
        );
    }
}

#[test]
fn learn_discovers_phrase_synonyms() {
    let queries: Vec<(u64, String)> = (0..20)
        .map(|i| (i, format!("(\"michael jordan\",mj) rare{i:03}")))
        .collect();
    let vocab = learn_from_queries(&queries, 2);
    let has_phrase = vocab
        .phrases
        .iter()
        .any(|p| p.tokens == vec!["michael", "jordan"]);
    assert!(has_phrase, "should learn 'michael jordan' as a phrase");
    let has_syn = vocab.synonyms.iter().any(|s| s.token == "mj");
    assert!(has_syn, "should learn 'mj' as a synonym");
}

#[test]
fn manual_synonym_management() {
    let mut vocab = Vocab::new();
    vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
    assert_eq!(vocab.synonyms().len(), 1);
    assert!(vocab.get_synonym("rc").is_some());

    // duplicate is ignored
    vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
    assert_eq!(vocab.synonyms().len(), 1);

    assert!(vocab.remove_synonym("rc"));
    assert!(vocab.synonyms().is_empty());
    assert!(!vocab.remove_synonym("nonexistent"));
}

#[test]
fn manual_phrase_management() {
    let mut vocab = Vocab::new();
    vocab.add_phrase(&["upper", "deck"], "term:upper_deck", FeatureKind::Generic);
    assert_eq!(vocab.phrases().len(), 1);

    // duplicate is ignored
    vocab.add_phrase(&["upper", "deck"], "term:upper_deck", FeatureKind::Generic);
    assert_eq!(vocab.phrases().len(), 1);

    assert!(vocab.remove_phrase(&["upper", "deck"]));
    assert!(vocab.phrases().is_empty());
}

#[test]
fn json_round_trip() {
    let mut vocab = Vocab::new();
    vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
    vocab.add_phrase(
        &["michael", "jordan"],
        "term:michael_jordan",
        FeatureKind::Generic,
    );
    vocab.add_grader("psa");
    vocab.add_grade_word("gem");

    let json = vocab.to_json().unwrap();
    let restored = Vocab::from_json(&json).unwrap();
    assert_eq!(restored.synonyms().len(), 1);
    assert_eq!(restored.phrases().len(), 1);
    assert_eq!(restored.graders().len(), 1);
    assert_eq!(restored.grade_words().len(), 1);
}

#[test]
fn to_normalizer_produces_valid_normalizer() {
    let mut vocab = Vocab::new();
    vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
    vocab.add_phrase(&["upper", "deck"], "term:upper_deck", FeatureKind::Generic);
    let norm = vocab.to_normalizer().expect("should build normalizer");

    let mut dict = crate::dict::Dict::new();
    let mut lc = String::new();
    let feats = norm.compile_features("upper deck rc", &mut dict, &mut lc);
    assert!(feats.len() >= 2, "should produce features for known vocab");
}

#[test]
fn merge_combines_vocabs() {
    let mut v1 = Vocab::new();
    v1.add_synonym("rc", "term:rookie", FeatureKind::Category);
    v1.add_grader("psa");

    let mut v2 = Vocab::new();
    v2.add_synonym("ud", "term:upper_deck", FeatureKind::Generic);
    v2.add_synonym("rc", "term:different", FeatureKind::Generic); // duplicate token
    v2.add_grader("bgs");

    v1.merge(&v2);
    assert_eq!(v1.synonyms().len(), 2); // rc + ud
    assert_eq!(v1.graders().len(), 2); // psa + bgs
                                       // rc should keep original mapping (first wins)
    assert_eq!(v1.get_synonym("rc").unwrap().canonical, "term:rookie");
}

// ---- punctuation rules (ADR-058) ----

#[test]
fn punctuation_rules_round_trip_and_drive_folding() {
    let mut vocab = Vocab::new();
    vocab.fold_punctuation('\'');
    vocab.set_punct_class('\u{2019}', PunctClass::Fold);
    vocab.fold_punctuation('-');

    // JSON round-trip preserves the rules ...
    let json = vocab.to_json().unwrap();
    let restored = Vocab::from_json(&json).unwrap();
    assert_eq!(restored.punctuation().len(), 3);

    // ... and the restored vocab's normalizer actually folds.
    let norm = restored.to_normalizer().expect("normalizer");
    let mut dict = crate::dict::Dict::new();
    let mut lc = String::new();
    let feats = norm.compile_features("O'Brien", &mut dict, &mut lc);
    let names: Vec<String> = feats.iter().map(|&id| dict.name(id).to_string()).collect();
    assert_eq!(
        names,
        vec!["term:obrien".to_string()],
        "apostrophe should fold"
    );
}

#[test]
fn vocab_without_punctuation_field_is_default_behavior() {
    // Old vocab JSON predating ADR-058 has no `punctuation` key; it must deserialize
    // to an empty rule set => the historical (split) behavior, byte-identical.
    let json = r#"{ "synonyms": [], "phrases": [], "graders": [], "grade_words": [] }"#;
    let vocab = Vocab::from_json(json).unwrap();
    assert!(vocab.punctuation().is_empty());

    let norm = vocab.to_normalizer().expect("normalizer");
    let mut dict = crate::dict::Dict::new();
    let mut lc = String::new();
    let feats = norm.compile_features("O'Brien", &mut dict, &mut lc);
    let mut names: Vec<String> = feats.iter().map(|&id| dict.name(id).to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["term:brien".to_string(), "term:o".to_string()],
        "default splits the apostrophe"
    );
}

#[test]
fn merge_combines_punctuation_rules_first_wins() {
    let mut v1 = Vocab::new();
    v1.set_punct_class('\'', PunctClass::Fold);

    let mut v2 = Vocab::new();
    v2.set_punct_class('\'', PunctClass::Split); // duplicate char
    v2.set_punct_class('-', PunctClass::Fold);

    v1.merge(&v2);
    assert_eq!(v1.punctuation().len(), 2); // ' (kept from v1) + -
    let apostrophe = v1.punctuation().iter().find(|r| r.ch == '\'').unwrap();
    assert_eq!(apostrophe.class, PunctClassSer::Fold, "v1's rule wins");
}

// ---- number-context words (ADR-069) ----

#[test]
fn number_context_words_round_trip_and_drive_typing() {
    let mut vocab = Vocab::new();
    vocab.set_number_context_words(&[]); // parity mode: disable the demotion

    // JSON round-trip preserves the explicit empty list (it must NOT collapse back to
    // the `["pop"]` default — `Some([])` and `None` are different knob states) ...
    let json = vocab.to_json().unwrap();
    let restored = Vocab::from_json(&json).unwrap();
    assert_eq!(restored.number_context_words(), Some(&[][..]));

    // ... and the restored vocab's normalizer types position-insensitively.
    let norm = restored.to_normalizer().expect("normalizer");
    let mut dict = crate::dict::Dict::new();
    let mut lc = String::new();
    let feats = norm.compile_features("pop 1995", &mut dict, &mut lc);
    let mut names: Vec<String> = feats.iter().map(|&id| dict.name(id).to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["term:pop".to_string(), "year:1995".to_string()],
        "parity mode: the pop-adjacent year stays a year"
    );
}

#[test]
fn vocab_without_number_context_field_is_default_behavior() {
    // Old vocab JSON predating ADR-069 has no `number_context` key; it must deserialize
    // to `None` => the historical `["pop"]` demotion, byte-identical.
    let json = r#"{ "synonyms": [], "phrases": [], "graders": [], "grade_words": [] }"#;
    let vocab = Vocab::from_json(json).unwrap();
    assert!(vocab.number_context_words().is_none());

    let norm = vocab.to_normalizer().expect("normalizer");
    let mut dict = crate::dict::Dict::new();
    let mut lc = String::new();
    let feats = norm.compile_features("pop 1995", &mut dict, &mut lc);
    let mut names: Vec<String> = feats.iter().map(|&id| dict.name(id).to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec!["term:1995".to_string(), "term:pop".to_string()],
        "default demotes the pop-adjacent year"
    );
}

#[test]
fn merge_number_context_first_wins() {
    let mut v1 = Vocab::new();
    v1.set_number_context_words(&[]);
    let mut v2 = Vocab::new();
    v2.set_number_context_words(&["qty"]);

    v1.merge(&v2);
    assert_eq!(
        v1.number_context_words(),
        Some(&[][..]),
        "an explicitly-set list survives a merge"
    );

    let mut v3 = Vocab::new();
    v3.merge(&v2);
    assert_eq!(
        v3.number_context_words(),
        Some(&["qty".to_string()][..]),
        "an unset vocab adopts the other's list"
    );
}

#[test]
fn empty_vocab_builds_valid_normalizer() {
    let vocab = Vocab::new();
    let norm = vocab.to_normalizer().expect("empty vocab should build");
    let mut dict = crate::dict::Dict::new();
    let mut lc = String::new();
    let feats = norm.compile_features("hello world", &mut dict, &mut lc);
    assert_eq!(feats.len(), 2, "should produce generic features");
}

#[test]
fn engine_with_vocab() {
    let mut vocab = Vocab::new();
    vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);

    let eng = crate::segment::Engine::with_vocab(vocab, crate::config::EngineConfig::default())
        .expect("should build engine from vocab");
    assert!(eng.vocab().is_some());
    assert_eq!(eng.vocab().unwrap().synonyms().len(), 1);
}

#[test]
fn snapshot_carries_vocab_for_lock_free_reads() {
    // The lock-free read path (GET /_vocab via ArcSwap) depends on the vocab
    // living in EngineSnapshot — not just on the Engine behind the write
    // mutex (ADR-016). Verify the snapshot reflects the vocab at snapshot
    // time, and that a published snapshot is immutable across a later
    // set_vocab (an older snapshot keeps its own view).
    let mut vocab = Vocab::new();
    vocab.add_synonym("rc", "term:rookie", FeatureKind::Category);
    let mut eng = crate::segment::Engine::with_vocab(vocab, crate::config::EngineConfig::default())
        .expect("should build engine from vocab");

    // Snapshot taken now sees the initial vocab.
    let snap_v1 = eng.snapshot();
    assert_eq!(
        snap_v1.vocab().map(|v| v.synonyms().len()),
        Some(1),
        "snapshot must carry the vocab so /_vocab can read it lock-free"
    );

    // Swap in a larger vocab on the engine.
    let mut vocab2 = Vocab::new();
    vocab2.add_synonym("rc", "term:rookie", FeatureKind::Category);
    vocab2.add_synonym("ud", "term:upper_deck", FeatureKind::Generic);
    eng.set_vocab(vocab2).expect("set_vocab should succeed");

    // A fresh snapshot reflects the update; the old snapshot is unchanged.
    let snap_v2 = eng.snapshot();
    assert_eq!(snap_v2.vocab().map(|v| v.synonyms().len()), Some(2));
    assert_eq!(
        snap_v1.vocab().map(|v| v.synonyms().len()),
        Some(1),
        "an already-published snapshot must keep its own vocab view"
    );
}

#[test]
fn snapshot_vocab_is_none_without_vocab() {
    // An engine built without a vocab (the default path) has no snapshot
    // vocab; GET /_vocab then serves Vocab::default().
    let eng = crate::segment::Engine::new(
        crate::normalize::Normalizer::default_vocab().expect("default vocab"),
    );
    assert!(eng.snapshot().vocab().is_none());
}

#[test]
fn corpus_learn_default_off_equals_anyof_only() {
    // The default CorpusLearnConfig disables NPMI, so the composer must be
    // byte-identical to any-of learning alone (the back-compat guarantee, ADR-053).
    let queries: Vec<(u64, String)> = (0..30)
        .map(|i| (i, format!("(rookie,rc) upper deck unique{i:03}")))
        .collect();
    let cfg = CorpusLearnConfig {
        anyof_min_count: 2,
        ..Default::default()
    };
    let composed = learn_vocab_from_corpus(&queries, &cfg);
    let anyof_only = learn_from_queries(&queries, 2);
    assert_eq!(
        composed.to_json().unwrap(),
        anyof_only.to_json().unwrap(),
        "with corpus_phrases off the composer must equal any-of learning alone"
    );
}

#[test]
fn corpus_learn_on_adds_npmi_phrases() {
    // No any-of groups -> any-of learning finds nothing; turning on NPMI induces the
    // repeated adjacent "upper deck" entity as a phrase.
    let queries: Vec<(u64, String)> = (0..30)
        .map(|i| (i, format!("upper deck unique{i:03}")))
        .collect();
    let off = learn_vocab_from_corpus(
        &queries,
        &CorpusLearnConfig {
            anyof_min_count: 2,
            ..Default::default()
        },
    );
    assert!(
        off.phrases().is_empty(),
        "no any-of groups -> no phrases when NPMI is off"
    );
    let on = learn_vocab_from_corpus(
        &queries,
        &CorpusLearnConfig {
            anyof_min_count: 2,
            corpus_phrases: true,
            npmi_min_count: 3,
            ..Default::default()
        },
    );
    assert!(
        on.phrases()
            .iter()
            .any(|p| p.tokens == vec!["upper".to_string(), "deck".to_string()]),
        "NPMI on must induce the upper/deck phrase"
    );
}

#[test]
fn learns_equivalences_from_anyof_groups() {
    let queries: Vec<(u64, String)> = (0..10)
        .map(|i| (i, format!("(rookie,rc) card{i:03}")))
        .collect();
    let groups = learn_equivalences_from_queries(&queries, 2);
    assert!(
        groups
            .iter()
            .any(|g| g.contains(&"rc".to_string()) && g.contains(&"rookie".to_string())),
        "an any-of group seen >= min_count must be learned as an equivalence group"
    );
    // Below threshold -> nothing learned.
    assert!(learn_equivalences_from_queries(&queries, 11).is_empty());
}

#[test]
fn corpus_learn_equivalences_mode_emits_groups_not_synonyms() {
    let queries: Vec<(u64, String)> = (0..10)
        .map(|i| (i, format!("(rookie,rc) card{i:03}")))
        .collect();
    let cfg = CorpusLearnConfig {
        anyof_min_count: 2,
        learn_equivalences: true,
        ..Default::default()
    };
    let v = learn_vocab_from_corpus(&queries, &cfg);
    assert!(
        v.synonyms().is_empty() && v.phrases().is_empty(),
        "expansion mode must not emit collapse synonyms/phrases"
    );
    assert!(
        !v.equivalences().is_empty(),
        "expansion mode must emit equivalence groups"
    );
}

#[test]
fn learn_equivalences_reinforces_pairs_across_group_sizes() {
    // (rc,rookie) once + (rc,rookie,rcfull) once: pair-level counting reinforces rc≡rookie
    // (count 2), so it survives min_count=2 — exact-group counting would see two distinct
    // groups (count 1 each) and learn nothing.
    let queries = vec![
        (1u64, "(rc,rookie)".to_string()),
        (2u64, "(rc,rookie,rcfull)".to_string()),
    ];
    let groups = learn_equivalences_from_queries(&queries, 2);
    assert!(
        groups
            .iter()
            .any(|g| g.contains(&"rc".to_string()) && g.contains(&"rookie".to_string())),
        "rc≡rookie must reinforce across the two differently-sized any-of groups"
    );
}

#[test]
fn resolve_equivalences_unions_overlapping_groups() {
    // Overlapping declared groups [aaa,bbb] + [bbb,ccc] must resolve to ONE transitive
    // group {aaa,bbb,ccc}, not order-dependently overwrite the shared member.
    let mut v = Vocab::new();
    v.add_equivalence(&["aaa", "bbb"]);
    v.add_equivalence(&["bbb", "ccc"]);
    let norm = crate::normalize::Normalizer::default_vocab().expect("vocab");
    let dict = Dict::new();
    let map = v.resolve_equivalences(&norm, &dict);

    let mut lc = String::new();
    let fa = norm.compile_features_readonly("aaa", &dict, &mut lc)[0];
    let fb = norm.compile_features_readonly("bbb", &dict, &mut lc)[0];
    let fc = norm.compile_features_readonly("ccc", &dict, &mut lc)[0];

    let ga = map.get(&fa).expect("aaa resolved");
    assert!(
        ga.contains(&fa) && ga.contains(&fb) && ga.contains(&fc),
        "aaa/bbb/ccc must merge into one transitive group"
    );
    assert_eq!(
        map.get(&fa),
        map.get(&fc),
        "aaa and ccc share the merged group (transitive via bbb)"
    );
}
