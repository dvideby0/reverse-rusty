//! ADR-120 quoted-clause semantics: analyzed adjacency, not unordered
//! membership. Kept separate from the broad randomized oracle so the DSL
//! contract is pinned by hand.

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
use reverse_rusty::{EngineConfig, Normalizer, Vocab};
use reverse_rusty_ref_matcher::{RefMatcher, RefVocab};

fn matched_with_broad(engine: &Engine, title: &str, include_broad: bool) -> Vec<u64> {
    let snapshot = engine.snapshot();
    let mut scratch = MatchScratch::new();
    let mut ids = Vec::new();
    snapshot.match_title(title, &mut scratch, &mut ids, include_broad);
    ids.sort_unstable();
    ids
}

fn matched(engine: &Engine, title: &str) -> Vec<u64> {
    matched_with_broad(engine, title, true)
}

#[test]
fn required_and_forbidden_quotes_preserve_normalized_adjacency() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    let report = engine.build_from_queries(&[
        (1, "\"red shoe\"".to_string()),
        (2, "red shoe".to_string()),
        (3, "item -\"for parts\"".to_string()),
    ]);
    assert_eq!(report.ingested, 3);

    assert_eq!(matched(&engine, "red shoe"), vec![1, 2]);
    assert_eq!(
        matched(&engine, "red-shoe"),
        vec![1, 2],
        "default split punctuation is an analyzed token boundary"
    );
    assert_eq!(
        matched(&engine, "red leather shoe"),
        vec![2],
        "the quoted clause must not degrade to red AND shoe"
    );
    assert_eq!(matched(&engine, "shoe red"), vec![2]);

    assert_eq!(matched(&engine, "item for parts"), Vec::<u64>::new());
    assert_eq!(matched(&engine, "item for spare parts"), vec![3]);
}

#[test]
fn quoted_semantics_agree_with_the_independent_front_end() {
    let queries = vec![
        (1, "\"red shoe\"".to_string()),
        (2, "red shoe".to_string()),
        (3, "item -\"for parts\"".to_string()),
    ];
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&queries);
    let reference = RefMatcher::build(&queries, RefVocab::default_vocab());

    for title in [
        "red shoe",
        "red-shoe",
        "red leather shoe",
        "shoe red",
        "item for parts",
        "item for spare parts",
    ] {
        let engine_ids: std::collections::HashSet<u64> =
            matched(&engine, title).into_iter().collect();
        assert_eq!(
            engine_ids,
            reference.matches(title),
            "independent phrase oracle drift for {title:?}"
        );
    }
}

#[test]
fn aliases_form_alternate_paths_without_weakening_adjacency() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&[
        (1, "\"new york\" knicks".to_string()),
        (2, "foo -\"new york\"".to_string()),
    ]);
    engine
        .import_alias_synonyms("ny => new york\nnyc => new york city")
        .expect("activate aliases");

    assert_eq!(matched(&engine, "ny knicks"), vec![1]);
    assert_eq!(matched(&engine, "new york knicks"), vec![1]);
    assert!(
        matched(&engine, "new vintage york knicks").is_empty(),
        "alias alternatives must not turn the phrase back into conjunction"
    );
    assert!(
        matched(&engine, "foo new york").is_empty(),
        "canonical forbidden phrase is present"
    );
    assert_eq!(
        matched(&engine, "foo new york city"),
        vec![2],
        "forbidden aliases retain ADR-061's canonical leftmost-longest policy"
    );
}

#[test]
fn punctuation_folding_is_shared_by_phrase_queries_and_titles() {
    let mut vocab = Vocab::new();
    vocab.fold_punctuation('-');
    let mut engine = Engine::with_vocab(vocab, EngineConfig::default()).expect("vocab");
    engine.build_from_queries(&[(1, "\"red-shoe\"".to_string())]);

    assert_eq!(matched(&engine, "red-shoe"), vec![1]);
    assert_eq!(matched(&engine, "redshoe"), vec![1]);
    assert!(
        matched(&engine, "red shoe").is_empty(),
        "Fold joins tokens on both sides; it is not Split"
    );
}

#[test]
fn fused_grader_features_are_composite_phrase_edges_not_alternatives() {
    let mut builder = Normalizer::builder();
    builder.add_grader("psa");
    builder.add_grader("bgs");
    let queries = vec![
        (1, "\"psa10\"".to_string()),
        (2, "\"psa 10\"".to_string()),
        (3, "\"psa\"".to_string()),
    ];
    let mut engine = Engine::new(builder.build().expect("normalizer"));
    engine.build_from_queries(&queries);
    let reference = RefMatcher::build(
        &queries,
        RefVocab::default_vocab().grader("psa").grader("bgs"),
    );

    for (title, expected) in [
        ("psa10", vec![1, 2, 3]),
        ("psa 10", vec![1, 2, 3]),
        ("psa 9", vec![3]),
        ("bgs 10", vec![]),
    ] {
        assert_eq!(matched(&engine, title), expected, "engine: {title}");
        assert_eq!(
            reference.matches(title),
            expected.into_iter().collect(),
            "independent reference: {title}"
        );
    }
}

#[test]
fn grader_composites_do_not_skip_quoted_interior_tokens() {
    let mut builder = Normalizer::builder();
    builder.add_grader("psa");
    let queries = vec![
        (1, "\"psa foo 10\"".to_string()),
        (2, "\"psa gem mint 10\"".to_string()),
        (3, "item -\"psa foo 10\"".to_string()),
        (4, "\"psa 10\"".to_string()),
    ];
    let mut engine = Engine::new(builder.build().expect("normalizer"));
    engine.build_from_queries(&queries);
    let reference = RefMatcher::build(&queries, RefVocab::default_vocab().grader("psa"));

    for (title, expected) in [
        ("psa foo 10", vec![1]),
        ("psa bar 10", vec![]),
        ("psa 10", vec![4]),
        ("psa10", vec![4]),
        ("psa gem mint 10", vec![2]),
        ("psa zip zap 10", vec![]),
        ("item psa foo 10", vec![1]),
        ("item psa bar 10", vec![3]),
        ("item psa 10", vec![3, 4]),
    ] {
        assert_eq!(matched(&engine, title), expected, "engine: {title}");
        assert_eq!(
            reference.matches(title),
            expected.into_iter().collect(),
            "independent reference: {title}"
        );
    }
}

#[test]
fn phrase_rows_force_batch_exactness_instead_of_entering_the_flat_kernel() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&[
        (1, "\"red shoe\"".to_string()),
        (2, "item -\"for parts\"".to_string()),
    ]);
    let titles: Vec<String> = [
        "red shoe",
        "red leather shoe",
        "item for parts",
        "item for spare parts",
    ]
    .into_iter()
    .map(ToString::to_string)
    .collect();
    let expected: Vec<Vec<u64>> = titles.iter().map(|title| matched(&engine, title)).collect();
    let mut actual = vec![Vec::new(); titles.len()];
    for (index, mut ids) in engine.snapshot().match_titles_batch(
        &titles,
        BatchMatchOptions {
            include_broad: true,
            broad_strategy: BroadStrategy::Columnar,
            ..BatchMatchOptions::default()
        },
    ) {
        ids.sort_unstable();
        actual[index] = ids;
    }
    assert_eq!(actual, expected);
}

#[test]
fn deleting_the_last_phrase_row_restores_columnar_batch_mode() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&[(1, "\"red shoe\"".to_string()), (2, "common".to_string())]);
    let titles = vec!["common".to_string()];
    let options = BatchMatchOptions {
        include_broad: true,
        broad_strategy: BroadStrategy::Columnar,
        ..BatchMatchOptions::default()
    };

    let before = engine.match_titles_batch_stats(&titles, options);
    assert_eq!(
        before.broad_batches, 0,
        "a live phrase row must force positioned scalar verification"
    );
    assert_eq!(engine.delete_by_logical_id(1).expect("delete phrase"), 1);
    let after = engine.match_titles_batch_stats(&titles, options);
    assert!(
        after.broad_batches > 0,
        "a dead phrase program must not keep a phrase-free memory segment in scalar mode"
    );
}

#[test]
fn in_memory_compaction_preserves_phrase_matching_for_every_merge_variant() {
    for (name, config) in [
        (
            "mechanical",
            EngineConfig {
                dedup_bodies: false,
                ..EngineConfig::default()
            },
        ),
        (
            "grouped",
            EngineConfig {
                dedup_bodies: true,
                ..EngineConfig::default()
            },
        ),
        (
            "reanchored",
            EngineConfig {
                dedup_bodies: true,
                compaction_reanchor: true,
                ..EngineConfig::default()
            },
        ),
    ] {
        let mut engine =
            Engine::with_config(Normalizer::default_vocab().expect("normalizer"), config);
        engine.bulk_ingest(&[
            (1, "\"#\"".to_string()),
            (2, "\"red shoe\"".to_string()),
            (3, "\"red shoe\"".to_string()),
        ]);
        engine.bulk_ingest(&[(4, "filler".to_string())]);

        assert_eq!(matched_with_broad(&engine, "#", false), vec![1], "{name}");
        assert_eq!(
            matched_with_broad(&engine, "red leather shoe", false),
            Vec::<u64>::new(),
            "{name}"
        );
        assert!(engine.compact_all().is_some(), "{name}: compacted");
        assert_eq!(
            matched_with_broad(&engine, "#", false),
            vec![1],
            "{name}: graph-only phrase cover must remain probeable"
        );
        assert_eq!(
            matched_with_broad(&engine, "red shoe", false),
            vec![2, 3],
            "{name}: required phrase must remain exact"
        );
        assert_eq!(
            matched_with_broad(&engine, "red leather shoe", false),
            Vec::<u64>::new(),
            "{name}: compaction must not flatten adjacency"
        );
    }
}

#[test]
fn required_phrases_remain_visible_without_the_broad_lane() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&[(1, "\"red shoe\"".to_string())]);

    assert_eq!(
        matched_with_broad(&engine, "red shoe", false),
        vec![1],
        "a semantically-selective phrase must not become broad-only because its labels are hot"
    );
}

#[test]
fn mixed_hot_term_and_required_phrase_remain_visible_without_the_broad_lane() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&[(1, "\"red shoe\" common".to_string())]);

    assert_eq!(
        matched_with_broad(&engine, "red shoe common", false),
        vec![1],
        "a phrase proxy must keep a mixed query default-visible when its sole flat anchor is hot"
    );
}

#[test]
fn repeated_phrase_labels_do_not_hide_bare_rows_at_the_hot_boundary() {
    let mut queries = Vec::new();
    let mut logical = 1u64;
    for i in 0..64 {
        for _ in 0..3 {
            queries.push((logical, format!("f{i}")));
            logical += 1;
        }
    }
    queries.push((logical, "\"x a\" \"x b\" \"x c\"".to_string()));
    logical += 1;
    let bare_x = logical;
    queries.push((bare_x, "x".to_string()));

    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine.build_from_queries(&queries);

    assert_eq!(
        matched_with_broad(&engine, "x", false),
        vec![bare_x],
        "one phrase-bearing query must contribute one x document-frequency count"
    );
}

#[test]
fn positive_phrase_graph_includes_stateful_raw_token_paths() {
    let mut builder = Normalizer::builder();
    builder.add_grader("psa");
    builder.add_phrase_alias(&["psa", "x"], "term:px", FeatureKind::Generic);
    let queries = vec![(1, "\"psa x 10\"".to_string())];
    let mut engine = Engine::new(builder.build().expect("normalizer"));
    engine.build_from_queries(&queries);

    assert_eq!(matched_with_broad(&engine, "psa x 10", false), vec![1]);

    let reference = RefMatcher::build(
        &queries,
        RefVocab::default_vocab().grader("psa").phrase(
            "psa x",
            "term:px",
            reverse_rusty_ref_matcher::vocab::PhraseMode::Alias,
        ),
    );
    assert_eq!(reference.matches("psa x 10"), [1].into_iter().collect());
}

#[test]
fn graph_only_probe_labels_do_not_widen_bare_term_semantics() {
    let mut builder = Normalizer::builder();
    builder.add_grade_word("gem");
    builder.add_synonym("stone", "term:gem", FeatureKind::Generic);
    let mut engine = Engine::new(builder.build().expect("normalizer"));
    engine.build_from_queries(&[(1, "stone".to_string()), (2, "\"red shoe\"".to_string())]);

    assert!(
        matched_with_broad(&engine, "gem", false).is_empty(),
        "enabling positioned matching must not make graph-hole labels satisfy ordinary rows"
    );
}

#[test]
fn repeated_graders_do_not_create_nonadjacent_phrase_shortcuts() {
    let mut builder = Normalizer::builder();
    builder.add_grader("psa");
    builder.add_phrase_alias(&["a", "x"], "term:a_x", FeatureKind::Generic);
    builder.add_phrase(&["x", "psa"], "term:x_psa", FeatureKind::Generic);
    let queries = vec![(1, "\"foo psa10 bar\"".to_string())];
    let mut engine = Engine::new(builder.build().expect("normalizer"));
    engine.build_from_queries(&queries);
    let title = "foo psa a x psa 10 bar";

    assert!(
        matched_with_broad(&engine, title, false).is_empty(),
        "alternate analyzer paths must not let a fused quoted grade skip title positions"
    );

    let reference = RefMatcher::build(
        &queries,
        RefVocab::default_vocab()
            .grader("psa")
            .phrase(
                "a x",
                "term:a_x",
                reverse_rusty_ref_matcher::vocab::PhraseMode::Alias,
            )
            .phrase(
                "x psa",
                "term:x_psa",
                reverse_rusty_ref_matcher::vocab::PhraseMode::Collapse,
            ),
    );
    assert!(reference.matches(title).is_empty());
}

#[test]
fn positioned_state_cap_fails_open_only_for_positive_graphs() {
    let mut builder = Normalizer::builder();
    builder.add_grader("psa");
    // Activates the force-additive positive graph where alternate grader starts
    // are retained; the alias itself need not occur in this adversarial title.
    builder.add_phrase_alias(
        &["unused", "alias"],
        "term:unused_alias",
        FeatureKind::Generic,
    );
    let mut engine = Engine::new(builder.build().expect("normalizer"));
    engine.build_from_queries(&[
        (1, "\"red shoe\"".to_string()),
        (2, "red -\"shoe boot\"".to_string()),
    ]);

    let graders = std::iter::repeat_n("psa", 65).collect::<Vec<_>>().join(" ");
    let incomplete_positive = format!("red {graders} boot");
    assert_eq!(
        matched_with_broad(&engine, &incomplete_positive, true),
        vec![1, 2],
        "bounded positive analysis must over-match rather than drop a candidate"
    );

    let forbidden_still_exact = format!("red {graders} shoe boot");
    assert_eq!(
        matched_with_broad(&engine, &forbidden_still_exact, true),
        vec![1],
        "positive incompleteness must not disable the complete canonical forbidden graph"
    );
}

#[test]
fn cluster_replicates_graph_only_phrase_covers_for_flat_routing() {
    let mut queries: Vec<(u64, String)> = (0..70).map(|i| (i + 1, format!("filler_{i}"))).collect();
    queries.push((999, "\"#\"".to_string()));
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: false,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(
        Normalizer::default_vocab().expect("normalizer"),
        &cfg,
        &queries,
    )
    .expect("cluster");

    for title in ["#", " # ", "##", "# #", "###", " # # ", "#  #  #"] {
        assert_eq!(
            cluster.percolate(title).expect("percolate"),
            vec![999],
            "graph-only cover missed through cluster routing for {title:?}"
        );
    }
}

#[test]
fn cluster_replicates_mixed_phrase_proxy_covers_for_flat_routing() {
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: false,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(
        Normalizer::default_vocab().expect("normalizer"),
        &cfg,
        &[(1, "\"red shoe\" common".to_string())],
    )
    .expect("cluster");

    assert_eq!(
        cluster
            .percolate("red shoe common")
            .expect("percolate mixed phrase"),
        vec![1],
        "a mixed phrase proxy must be replicated rather than ring-placed by graph labels"
    );
}
