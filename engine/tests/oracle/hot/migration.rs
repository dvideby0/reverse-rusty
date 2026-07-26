use super::*;

/// Claim 3 on a CONTROLLED corpus: compaction is the migration seam, with
/// deterministic margins. `margintok` (freq 100, unmasked):
///   - built θ=0 ⇒ class A; a θ=50 re-anchoring merge PROMOTES A→H;
///   - re-merged at θ=150 the plan says A (100 < 150) but 100 > 150/2 = 75 ⇒
///     the margin BLOCKS the demotion (no merge-to-merge thrash);
///   - re-merged at θ=300 (100 ≤ 150) the demotion clears ⇒ H→A.
///
/// Results are identical at every step, on both visibility modes.
#[test]
fn compaction_migrates_main_to_hot_and_back_margin_gated() {
    let (mut queries, mut id) = masked_filler_corpus(200);
    // SINGLE-token queries so margintok IS the rarest required anchor
    // (freq 100, unmasked — the fillers above hold every mask bit).
    for _ in 0..100 {
        queries.push((id, "margintok".to_string()));
        id += 1;
    }
    let titles: Vec<String> = vec![
        "margintok something".into(),
        "fillertok9 uniq3".into(),
        "unrelated listing".into(),
    ];
    let mk = |theta: u32| EngineConfig {
        hot_anchor_threshold: theta,
        compaction_reanchor: true,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };

    // Built θ=0: two base segments (build + bulk), everything main-lane.
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), mk(0));
    let half = queries.len() / 2;
    eng.build_from_queries(&queries[..half]);
    eng.bulk_ingest(&queries[half..]);
    assert_eq!(eng.class_counts()[4], 0);
    let before_broad = per_title_sets(&eng, &titles, true);
    let before_sel = per_title_sets(&eng, &titles, false);
    // The margintok queries all match title 0 — the population under test.
    assert!(
        before_sel[0].len() >= 100,
        "corpus not matching as designed"
    );

    // ---- A→H at θ=50 ----
    eng.set_config(mk(50));
    let r1 = eng.compact_all().expect("first compaction");
    assert!(r1.hot_promoted >= 100, "margintok population must promote");
    assert_eq!(r1.hot_demoted, 0);
    let h_after_promote = eng.class_counts()[4];
    assert!(h_after_promote >= 100);
    assert_eq!(per_title_sets(&eng, &titles, true), before_broad);
    assert_eq!(per_title_sets(&eng, &titles, false), before_sel);

    // ---- margin band at θ=150: freq 100 ∈ (75, 150) ⇒ nothing moves ----
    eng.set_config(mk(150));
    eng.bulk_ingest(&queries[..8]); // a second segment so a merge happens
    let r2 = eng.compact_all().expect("second compaction");
    assert_eq!(
        r2.hot_demoted, 0,
        "margin band (freq > θ/2) must block demotion"
    );
    assert_eq!(per_title_sets(&eng, &titles, true), before_broad);
    assert_eq!(per_title_sets(&eng, &titles, false), before_sel);
    assert!(
        eng.class_counts()[4] >= h_after_promote,
        "H population shrank inside the margin band"
    );

    // ---- demotion clears at θ=300: freq 100 ≤ 150 ----
    eng.set_config(mk(300));
    eng.bulk_ingest(&queries[..8]);
    let r3 = eng.compact_all().expect("third compaction");
    assert!(r3.hot_demoted >= 100, "demotion must clear past the margin");
    assert_eq!(
        eng.class_counts()[4],
        0,
        "every hot entry demotes once θ dwarfs all frequencies"
    );
    assert_eq!(per_title_sets(&eng, &titles, true), before_broad);
    assert_eq!(per_title_sets(&eng, &titles, false), before_sel);
}

/// θ=0 must DRAIN the hot tier at the next re-anchoring compaction — the
/// knob's documented "0 = off" contract covers sealed entries, not just new
/// writes. (The θ/2 hysteresis margin is `worst <= 0` at θ=0, which no real
/// anchor satisfies — without the explicit θ=0 arm every stored class-H entry
/// would stay hot forever: the codex-review P2.)
#[test]
fn compaction_drains_hot_tier_when_theta_disabled() {
    let (mut queries, mut id) = masked_filler_corpus(200);
    for _ in 0..100 {
        queries.push((id, "draintok".to_string()));
        id += 1;
    }
    let titles: Vec<String> = vec!["draintok something".into(), "fillertok9 uniq3".into()];
    let mk = |theta: u32| EngineConfig {
        hot_anchor_threshold: theta,
        compaction_reanchor: true,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };

    // Built θ=50 across two segments: the draintok population lands in class H.
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), mk(50));
    let half = queries.len() / 2;
    eng.build_from_queries(&queries[..half]);
    eng.bulk_ingest(&queries[half..]);
    assert!(
        eng.class_counts()[4] >= 100,
        "degenerate: draintok population did not classify H"
    );
    let before_broad = per_title_sets(&eng, &titles, true);
    let before_sel = per_title_sets(&eng, &titles, false);

    // θ back to 0 (off): the next re-anchoring merge drains every H entry.
    eng.set_config(mk(0));
    eng.bulk_ingest(&queries[..8]); // a second segment so a merge happens
    let r = eng.compact_all().expect("draining compaction");
    assert!(r.hot_demoted >= 100, "θ=0 must demote the hot population");
    assert_eq!(
        eng.class_counts()[4],
        0,
        "θ=0 left sealed entries in the hot tier"
    );
    assert_eq!(per_title_sets(&eng, &titles, true), before_broad);
    assert_eq!(per_title_sets(&eng, &titles, false), before_sel);
}

/// The per-merge work cap bounds lane moves, and repeated merges converge to
/// the same end state with results untouched at every intermediate step.
#[test]
fn migration_work_cap_bounds_per_merge_and_converges() {
    let (mut queries, mut id) = masked_filler_corpus(200);
    // DISTINCT-body any-of queries whose deciding anchor group carries `captok`
    // (freq 40, unmasked — the fillers own every mask bit): θ=30 re-derives each
    // to class H via the mixed-any-of rule. The unique `crare{i}` member keeps
    // every body distinct: identical bodies would body-group under dedup
    // (ADR-106) and migrate as ONE leader decision + N cap-EXEMPT adoptions,
    // which is exactly what this test must not conflate with the cap it pins —
    // the per-merge bound on posting-REBUILD work.
    for i in 0..40u64 {
        queries.push((id, format!("(captok,crare{i})")));
        id += 1;
    }
    let titles: Vec<String> = vec!["captok anything".into(), "fillertok8 uniq2".into()];
    let capped = |cap: usize| EngineConfig {
        hot_anchor_threshold: 30,
        hot_migration_max_moves: cap,
        compaction_reanchor: true,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };

    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), capped(7));
    // Build θ-OFF first (capped(7) has θ=30 — so build under a θ=0 config, then
    // switch): everything starts in main.
    eng.set_config(EngineConfig {
        compaction_reanchor: true,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    });
    let half = queries.len() / 2;
    eng.build_from_queries(&queries[..half]);
    eng.bulk_ingest(&queries[half..]);
    assert_eq!(eng.class_counts()[4], 0);
    let before = per_title_sets(&eng, &titles, true);

    eng.set_config(capped(7));
    let mut rounds = 0usize;
    loop {
        eng.bulk_ingest(&queries[..4]); // ensure ≥2 segments each round
        let r = eng.compact_all().expect("capped compaction");
        assert!(
            r.hot_promoted <= 7,
            "work cap exceeded in one merge ({} > 7)",
            r.hot_promoted
        );
        rounds += 1;
        assert!(rounds < 200, "migration failed to converge");
        if r.hot_promoted == 0 {
            break;
        }
        // Intermediate states must be reader-correct throughout.
        assert_eq!(
            per_title_sets(&eng, &titles, true),
            before,
            "capped migration changed results mid-convergence"
        );
    }
    // Exactly the captok population migrates (the two-token filler queries
    // anchor on their unique tail token and never move), across several capped
    // merges, with results untouched.
    assert_eq!(eng.class_counts()[4], 40);
    assert_eq!(per_title_sets(&eng, &titles, true), before);
    assert!(rounds > 1, "cap never actually split the migration");
}
