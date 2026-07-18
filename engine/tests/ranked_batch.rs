//! ADR-112 ranked-batch differential — the load-bearing batch≡scalar proof.
//!
//! `EngineSnapshot::try_match_titles_batch_top_k` must return, per title,
//! EXACTLY the bounded result of `try_match_title_top_k` for that title (which
//! is itself differentially proven against collect-all + sort + truncate in
//! `tests/ranking.rs`). Rank COUNTERS (`rank_stats`) are deliberately not
//! compared: heap-replacement/evaluation counts are emission-order-dependent
//! and the columnar pass legitimately reorders emissions; winners and totals
//! are order-invariant and are the contract. Data generation is seeded.

use reverse_rusty::exact::TagPredicate;
use reverse_rusty::gen::{generate, Dataset, GenConfig};
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
use reverse_rusty::{
    EngineSnapshot, Normalizer, QueryScope, RankProgramSpec, RankedMatchError, TopKAdmissionError,
    TopKOptions, TotalHits,
};

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("vocab")
}

fn tag(k: &str, v: &str) -> (String, String) {
    (k.to_string(), v.to_string())
}

fn gen_data(seed: u64, num_queries: usize, num_titles: usize, broad_frac: f64) -> Dataset {
    generate(&GenConfig {
        num_queries,
        num_titles,
        broad_query_frac: broad_frac,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: (num_queries / 40).max(2_000),
        num_sets: (num_queries / 100).max(1_000),
    })
}

fn tags_for(id: u64) -> Vec<(String, String)> {
    vec![
        tag("priority", &(((id as i64) % 23) - 11).to_string()),
        tag(
            "tier",
            if id.is_multiple_of(3) {
                "gold"
            } else {
                "plain"
            },
        ),
    ]
}

/// Multi-shape tagged engine: a built base segment, a bulk-ingested segment,
/// and live memtable inserts — with class D accepted and a low hot-θ so the
/// batch spans the A/B/C/D/H lanes.
fn build_tagged_multi(data: &Dataset) -> Engine {
    let mut eng = Engine::with_config(
        norm(),
        reverse_rusty::config::EngineConfig {
            accept_class_d: true,
            hot_anchor_threshold: 2,
            ..reverse_rusty::config::EngineConfig::default()
        },
    );
    let n = data.queries.len();
    let c = n / 3;
    let tags: Vec<Vec<(String, String)>> = data.queries[..c]
        .iter()
        .map(|(id, _)| tags_for(*id))
        .collect();
    eng.try_build_from_queries_with_tags(&data.queries[..c], &tags)
        .expect("tagged build");
    for (id, text) in &data.queries[c..2 * c] {
        eng.insert_live_with_tags(text, *id, 1, &tags_for(*id));
    }
    eng.flush();
    for (id, text) in &data.queries[2 * c..] {
        eng.insert_live_with_tags(text, *id, 1, &tags_for(*id));
    }
    eng
}

fn program_spec() -> RankProgramSpec {
    RankProgramSpec {
        priority_field: Some("priority".into()),
        boosts: vec![("tier".into(), "gold".into(), 100)],
    }
}

/// The scalar per-title bounded reference.
fn scalar_top_k(
    snap: &EngineSnapshot,
    title: &str,
    options: TopKOptions,
    program: &reverse_rusty::CompiledRankProgram,
    pred: &TagPredicate,
) -> (Vec<(u64, i64)>, TotalHits) {
    let mut scratch = MatchScratch::new();
    let got = snap
        .try_match_title_top_k(title, options, program, pred, &mut scratch, None)
        .expect("scalar bounded reference");
    (
        got.hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect(),
        got.total_hits,
    )
}

fn assert_batch_equals_scalar(
    snap: &EngineSnapshot,
    titles: &[String],
    batch_opts: BatchMatchOptions,
    options: TopKOptions,
    program: &reverse_rusty::CompiledRankProgram,
    pred: &TagPredicate,
    label: &str,
) {
    let batch = snap
        .try_match_titles_batch_top_k(titles, batch_opts, options, program, pred, None)
        .expect("batch bounded result");
    assert_eq!(batch.titles.len(), titles.len(), "{label}: slot count");
    let mut expected_sum = 0u64;
    for (i, title) in titles.iter().enumerate() {
        let (hits, total) = scalar_top_k(snap, title, options, program, pred);
        let got: Vec<(u64, i64)> = batch.titles[i]
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        assert_eq!(got, hits, "{label}: title {i} winners diverge");
        assert_eq!(
            batch.titles[i].total_hits, total,
            "{label}: title {i} total diverges"
        );
        expected_sum = expected_sum.saturating_add(total.value);
    }
    let expected_matches = u32::try_from(expected_sum).unwrap_or(u32::MAX);
    assert_eq!(
        batch.stats.matches, expected_matches,
        "{label}: aggregate matches must be the saturating sum of per-title totals"
    );
}

#[test]
fn batch_top_k_equals_per_title_scalar_across_lanes_and_options() {
    let data = gen_data(0xAD12_0001, 10_000, 300, 0.06);
    let eng = build_tagged_multi(&data);
    let snap = eng.snapshot();
    let program = snap.compile_rank_program(&program_spec()).expect("program");
    let empty = TagPredicate::empty();

    for scope in [QueryScope::Standard, QueryScope::WithBroad] {
        for &strategy in &[BroadStrategy::Columnar, BroadStrategy::Inline] {
            for &(materialize, prefilter) in &[(true, true), (false, false)] {
                for &batch_size in &[1usize, 256] {
                    let batch_opts = BatchMatchOptions {
                        include_broad: false, // overridden by query_scope
                        broad_batch_size: batch_size,
                        broad_strategy: strategy,
                        broad_materialize: materialize,
                        broad_prefilter: prefilter,
                    };
                    for &size in &[0usize, 1, 47] {
                        for &threshold in &[3u64, 10_000] {
                            let options = TopKOptions {
                                search_after: None,
                                size,
                                track_total_hits_up_to: threshold,
                                query_scope: scope,
                            };
                            assert_batch_equals_scalar(
                                &snap,
                                &data.titles,
                                batch_opts,
                                options,
                                &program,
                                &empty,
                                &format!(
                                    "scope={scope:?} strat={strategy:?} mat={materialize} \
                                     pre={prefilter} bs={batch_size} k={size} th={threshold}"
                                ),
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn filtered_batch_top_k_equals_filtered_scalar() {
    let data = gen_data(0xAD12_0002, 6_000, 200, 0.05);
    let eng = build_tagged_multi(&data);
    let snap = eng.snapshot();
    let program = snap.compile_rank_program(&program_spec()).expect("program");
    let pred = snap.compile_tag_predicate(&[("tier".to_string(), vec!["gold".to_string()])]);
    for scope in [QueryScope::Standard, QueryScope::WithBroad] {
        let options = TopKOptions {
            search_after: None,
            size: 25,
            track_total_hits_up_to: 10_000,
            query_scope: scope,
        };
        assert_batch_equals_scalar(
            &snap,
            &data.titles,
            BatchMatchOptions::default(),
            options,
            &program,
            &pred,
            &format!("filtered scope={scope:?}"),
        );
    }
}

#[test]
fn dedup_heavy_batch_top_k_equals_scalar() {
    // ADR-106 canonical-body groups: many identical semantic bodies with
    // distinct ids/priorities share one posting per in-memory segment; the
    // batch kernel's per-member emission fan-out must stay per-title exact.
    let mut eng = Engine::with_config(
        norm(),
        reverse_rusty::config::EngineConfig {
            accept_class_d: true,
            hot_anchor_threshold: 2,
            ..reverse_rusty::config::EngineConfig::default()
        },
    );
    for id in 1..=300u64 {
        eng.insert_live_with_tags("zzdup zzcard", id, 1, &tags_for(id));
    }
    // A tombstoned member must not emit; a dead leader must not drop members.
    eng.delete_by_logical_id(7).expect("tombstone member");
    eng.delete_by_logical_id(1).expect("tombstone leader");
    for id in 301..=340u64 {
        eng.insert_live_with_tags("zzother zzterm", id, 1, &tags_for(id));
    }
    let snap = eng.snapshot();
    let program = snap.compile_rank_program(&program_spec()).expect("program");
    let empty = TagPredicate::empty();
    let titles: Vec<String> = vec![
        "zzdup zzcard listing".into(),
        "zzother zzterm listing".into(),
        "zzdup zzcard zzother zzterm".into(),
        "no match here".into(),
    ];
    for &size in &[0usize, 10, 500] {
        let options = TopKOptions {
            search_after: None,
            size,
            track_total_hits_up_to: 10_000,
            query_scope: QueryScope::WithBroad,
        };
        assert_batch_equals_scalar(
            &snap,
            &titles,
            BatchMatchOptions::default(),
            options,
            &program,
            &empty,
            &format!("dedup k={size}"),
        );
    }
}

#[test]
fn multiword_alias_forced_inline_batch_equals_scalar() {
    // An active multi-word alias forces the columnar kernel off (ADR-061); the
    // ranked batch must ride the same forced-inline two-view path and still
    // equal the scalar bounded reference.
    let data = gen_data(0xAD12_0003, 6_000, 200, 0.05);
    let mut eng = build_tagged_multi(&data);
    eng.import_alias_synonyms("ny => new york")
        .expect("import + apply aliases");
    let snap = eng.snapshot();
    let program = snap.compile_rank_program(&program_spec()).expect("program");
    let empty = TagPredicate::empty();
    for scope in [QueryScope::Standard, QueryScope::WithBroad] {
        let options = TopKOptions {
            search_after: None,
            size: 20,
            track_total_hits_up_to: 10_000,
            query_scope: scope,
        };
        assert_batch_equals_scalar(
            &snap,
            &data.titles,
            BatchMatchOptions::default(),
            options,
            &program,
            &empty,
            &format!("alias-forced-inline scope={scope:?}"),
        );
    }
}

#[test]
fn batch_admission_rejects_before_matching() {
    let mut eng = Engine::new(norm());
    eng.insert_live("topps chrome", 1, 1);
    let snap = eng.snapshot();
    let program = snap
        .compile_rank_program(&RankProgramSpec::default())
        .expect("program");
    let empty = TagPredicate::empty();

    // Title-count ceiling.
    let too_many: Vec<String> = (0..=reverse_rusty::MAX_RANKED_BATCH_TITLES)
        .map(|i| format!("t{i}"))
        .collect();
    let err = snap
        .try_match_titles_batch_top_k(
            &too_many,
            BatchMatchOptions::default(),
            TopKOptions::default(),
            &program,
            &empty,
            None,
        )
        .expect_err("title ceiling must reject");
    assert!(matches!(
        err,
        RankedMatchError::Admission(TopKAdmissionError::BatchTitlesTooLarge { .. })
    ));

    // Aggregate heap budget: MAX_TOP_K × 105 titles > 2^20 rows.
    let titles: Vec<String> = (0..105).map(|i| format!("t{i}")).collect();
    let err = snap
        .try_match_titles_batch_top_k(
            &titles,
            BatchMatchOptions::default(),
            TopKOptions {
                search_after: None,
                size: reverse_rusty::MAX_TOP_K,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &program,
            &empty,
            None,
        )
        .expect_err("heap budget must reject");
    assert!(matches!(
        err,
        RankedMatchError::Admission(TopKAdmissionError::BatchHeapBudgetExceeded { .. })
    ));

    // The scalar per-title bounds still apply.
    let err = snap
        .try_match_titles_batch_top_k(
            &titles,
            BatchMatchOptions::default(),
            TopKOptions {
                search_after: None,
                size: reverse_rusty::MAX_TOP_K + 1,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &program,
            &empty,
            None,
        )
        .expect_err("size ceiling must reject");
    assert!(matches!(
        err,
        RankedMatchError::Admission(TopKAdmissionError::SizeTooLarge { .. })
    ));

    // ADR-113: a pagination boundary is a single-title cursor primitive —
    // batch requests must reject it loudly, never silently drop it.
    let err = snap
        .try_match_titles_batch_top_k(
            &["one title"],
            BatchMatchOptions::default(),
            TopKOptions {
                search_after: Some((0, 0)),
                ..TopKOptions::default()
            },
            &program,
            &empty,
            None,
        )
        .expect_err("search_after must reject on the batch path");
    assert!(matches!(
        err,
        RankedMatchError::Admission(TopKAdmissionError::BatchSearchAfterUnsupported)
    ));
}

#[test]
fn expired_deadline_fails_the_whole_batch() {
    let data = gen_data(0xAD12_0004, 4_000, 100, 0.05);
    let eng = build_tagged_multi(&data);
    let snap = eng.snapshot();
    let program = snap.compile_rank_program(&program_spec()).expect("program");
    let empty = TagPredicate::empty();
    let expired = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(1))
        .expect("clock is past the epoch");
    let err = snap
        .try_match_titles_batch_top_k(
            &data.titles,
            BatchMatchOptions::default(),
            TopKOptions {
                search_after: None,
                size: 10,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::WithBroad,
            },
            &program,
            &empty,
            Some(expired),
        )
        .expect_err("an expired deadline must cancel the whole batch");
    assert!(matches!(err, RankedMatchError::Cancelled(_)));
}
