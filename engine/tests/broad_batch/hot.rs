use super::*;

#[test]
fn batch_equals_scalar_with_hot_tier() {
    let data = gen(0x0407_B0A7, 20_000, 2_000, 0.05);
    let eng = build_multi_hot(&data);
    run_matrix(&eng, &data.titles);
}

#[test]
fn hot_inline_equals_columnar() {
    // The shared kill-switch: BroadStrategy::Inline forces the hot tier through
    // the per-title match_into path; results must equal the columnar pass.
    let data = gen(0x0407_A11C, 20_000, 1_000, 0.05);
    let eng = build_multi_hot(&data);
    for &bs in &[1usize, 64, 256, 999] {
        for include_broad in [false, true] {
            let inline = batch_result(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad,
                    broad_batch_size: bs,
                    broad_strategy: BroadStrategy::Inline,
                    broad_materialize: true,
                    broad_prefilter: true,
                },
            );
            let columnar = batch_result(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad,
                    broad_batch_size: bs,
                    broad_strategy: BroadStrategy::Columnar,
                    broad_materialize: true,
                    broad_prefilter: true,
                },
            );
            assert_eq!(
                inline, columnar,
                "hot Inline != Columnar (bs={bs}, include_broad={include_broad})"
            );
        }
    }
}

#[test]
fn hot_materialize_on_equals_off_and_fast_path_fires() {
    // The vacuous accept for class H rides `pure_tail_anchor` (a θ-hot anchor
    // has NO mask bit, so `is_pure_anchor` is structurally false for it — the
    // trap this test exists to catch): materialize on ≡ off, AND the on-run
    // provably evaluates fewer queries through the full bitmap path.
    let data = gen(0x0407_3A7E, 20_000, 1_000, 0.05);
    let eng = build_multi_hot(&data);
    let opts = |materialize| BatchMatchOptions {
        include_broad: false, // isolate the hot lane
        broad_batch_size: 256,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: materialize,
        broad_prefilter: true,
    };
    let on = batch_result(&eng, &data.titles, opts(true));
    let off = batch_result(&eng, &data.titles, opts(false));
    assert_eq!(on, off, "hot materialize on != off");

    let st_on = eng.match_titles_batch_stats(&data.titles, opts(true));
    let st_off = eng.match_titles_batch_stats(&data.titles, opts(false));
    assert!(
        st_on.hot_queries_evaluated < st_off.hot_queries_evaluated,
        "the tail-anchored vacuous accept never fired \
         (on={} off={} — is_pure_anchor vs pure_tail_anchor trap?)",
        st_on.hot_queries_evaluated,
        st_off.hot_queries_evaluated
    );
    assert!(st_on.hot_batches > 0, "hot columnar pass never ran");
}

#[test]
fn hot_multiword_alias_forced_inline() {
    // An active multi-word alias forces the single-view columnar kernel off
    // (ADR-061); the hot tier must ride the same forced-inline path and still
    // equal the scalar baseline on both visibility modes.
    let data = gen(0x0407_A11A, 20_000, 800, 0.05);
    let mut eng = build_multi_hot(&data);
    // Activate a declared multi-word alias (the ADR-061 two-view trigger): the
    // Solr import path auto-activates it and recompiles the stored queries.
    eng.import_alias_synonyms("ny => new york")
        .expect("import + apply aliases");
    assert!(
        eng.class_counts()[4] > 0,
        "the alias recompile lost the class-H population"
    );

    for include_broad in [false, true] {
        let scalar = scalar_baseline(&eng, &data.titles, include_broad);
        for &bs in &[1usize, 64, 256] {
            let batch = batch_result(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad,
                    broad_batch_size: bs,
                    broad_strategy: BroadStrategy::Columnar,
                    broad_materialize: true,
                    broad_prefilter: true,
                },
            );
            assert_eq!(
                batch, scalar,
                "alias-forced-inline hot batch != scalar (bs={bs}, broad={include_broad})"
            );
        }
    }
}

#[test]
fn compound_anyof_members_match_identically_in_columnar_and_scalar_paths() {
    let queries = vec![
        (1, "(red shoe,boot)".to_string()),
        (2, "marker -(red shoe,boot)".to_string()),
        (3, "(red shoe,red boot)".to_string()),
        (4, "-(red shoe,boot)".to_string()),
    ];
    let titles: Vec<String> = [
        "red shoe marker",
        "boot marker",
        "red hat marker",
        "shoe marker",
        "red boot",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    let mut engine = Engine::with_config(
        Normalizer::default_vocab().expect("normalizer"),
        EngineConfig {
            accept_class_d: true,
            ..EngineConfig::default()
        },
    );
    engine.build_from_queries(&queries);

    let expected = vec![vec![1, 3], vec![1], vec![2, 4], vec![2, 4], vec![1, 3]];
    let scalar = scalar_baseline(&engine, &titles, true);
    assert_eq!(scalar, expected);
    for materialize in [false, true] {
        for prefilter in [false, true] {
            let columnar = batch_result(
                &engine,
                &titles,
                BatchMatchOptions {
                    include_broad: true,
                    broad_batch_size: 64,
                    broad_strategy: BroadStrategy::Columnar,
                    broad_materialize: materialize,
                    broad_prefilter: prefilter,
                },
            );
            assert_eq!(
                columnar, expected,
                "compound predicate drift (materialize={materialize}, prefilter={prefilter})"
            );
        }
    }
}
