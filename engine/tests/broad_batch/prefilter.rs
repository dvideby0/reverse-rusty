use super::*;

#[test]
fn prefilter_on_equals_off_and_bites() {
    let (queries, titles) = prefilter_corpus();
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);

    let opts = |bs: usize, prefilter: bool| BatchMatchOptions {
        include_broad: true,
        broad_batch_size: bs,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: true,
        broad_prefilter: prefilter,
    };

    // Results identical across the sweep, prefilter on == off == scalar.
    for &bs in &[1usize, 7, 64, 256] {
        let on = batch_result(&eng, &titles, opts(bs, true));
        let off = batch_result(&eng, &titles, opts(bs, false));
        assert_eq!(on, off, "prefilter changed results at batch_size {bs}");
        let scalar = scalar_baseline(&eng, &titles, true);
        assert_eq!(on, scalar, "batch != scalar at batch_size {bs}");
    }
    // The both-group titles must actually match (the corpus is not degenerate, and
    // the prefilter did not over-reject the satisfiable shape).
    let on = batch_result(&eng, &titles, opts(1, true));
    assert!(
        on[40..].iter().all(|r| r.iter().any(|&id| id < 24)),
        "a both-group title lost its class-C matches"
    );
    assert!(
        on[..40].iter().all(|r| r.iter().all(|&id| id >= 1_000)),
        "an anchor-only title matched a two-group query"
    );

    // The meter: per-title batches make every anchor-only title a gamma/delta-free
    // batch, so the skip fires; off => the counter is structurally zero.
    let stats_on = eng.match_titles_batch_stats(&titles, opts(1, true));
    assert!(
        stats_on.broad_prefilter_skipped > 0,
        "prefilter never fired on the shape built to trigger it"
    );
    let stats_off = eng.match_titles_batch_stats(&titles, opts(1, false));
    assert_eq!(stats_off.broad_prefilter_skipped, 0, "off must never skip");
    // Skipping only ever removes full bitmap evaluations, never candidates.
    assert!(stats_on.broad_queries_evaluated < stats_off.broad_queries_evaluated);
    assert_eq!(stats_on.broad_candidates, stats_off.broad_candidates);
}

#[test]
fn prefilter_never_skips_class_d() {
    // A class-D always-candidate has EMPTY positives — the count-gate's clauses all
    // pass vacuously, so it can never be prefilter-skipped (skipping it would be
    // gating on MUST_NOT). Lane-on corpus of negation-only queries: the counter must
    // stay zero and every title without the forbidden token must keep its matches.
    let cfg = reverse_rusty::config::EngineConfig {
        accept_class_d: true,
        ..Default::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    let queries: Vec<(u64, String)> = (0..12u64)
        .map(|i| (i, format!("-junktoken{}", i % 3)))
        .collect();
    eng.build_from_queries(&queries);

    let titles: Vec<String> = (0..20)
        .map(|i| {
            if i % 4 == 0 {
                format!("clean listing junktoken0 number {i}")
            } else {
                format!("clean listing number {i}")
            }
        })
        .collect();

    let opts = |prefilter: bool| BatchMatchOptions {
        include_broad: true,
        broad_batch_size: 4,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: true,
        broad_prefilter: prefilter,
    };
    let on = batch_result(&eng, &titles, opts(true));
    let off = batch_result(&eng, &titles, opts(false));
    let scalar = scalar_baseline(&eng, &titles, true);
    assert_eq!(on, off, "prefilter changed class-D results");
    assert_eq!(on, scalar, "batch != scalar on the class-D corpus");
    assert!(
        on.iter().any(|r| !r.is_empty()),
        "degenerate: no class-D query matched"
    );

    let stats = eng.match_titles_batch_stats(&titles, opts(true));
    assert_eq!(
        stats.broad_prefilter_skipped, 0,
        "a class-D always-candidate was prefilter-skipped"
    );
}
