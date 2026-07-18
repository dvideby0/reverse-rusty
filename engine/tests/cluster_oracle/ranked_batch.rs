//! ADR-112 distributed ranked title batching: the batch API must return, per
//! title, EXACTLY what the single-title distributed top-K returns (which is
//! itself proven ≡ the standalone engine in `ranking.rs`), while fanning ONE
//! call per involved shard. The batch fetch must dedup cross-title winners and
//! charge its one credit per DELIVERED occurrence.

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ClusterRankedError};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::{
    QueryScope, RankProgramSpec, TopKAdmissionError, TopKOptions, MAX_RANKED_BATCH_TITLES,
};

fn ranked_tags(queries: &[(u64, String)]) -> Vec<Vec<(String, String)>> {
    queries
        .iter()
        .map(|(l, _)| {
            let mut tags = tags_for(*l);
            if l % 5 == 0 {
                tags.push(("priority".to_string(), (l % 97).to_string()));
            }
            tags
        })
        .collect()
}

fn rank_program() -> RankProgramSpec {
    RankProgramSpec {
        priority_field: Some("priority".to_string()),
        boosts: vec![
            ("category".to_string(), "cards".to_string(), 1_000),
            ("status".to_string(), "active".to_string(), -250),
        ],
    }
}

/// The load-bearing batch differential: batch ≡ per-title distributed top-K ≡
/// standalone bounded reference, across shard counts, K, thresholds, and both
/// visibility scopes — with the batch's per-shard fan bounded to one call.
#[test]
fn distributed_batch_top_k_matches_per_title_and_single_node() {
    let (queries, titles) = build_corpus();
    let tags = ranked_tags(&queries);
    let program = rank_program();

    let mut reference = Engine::new(vocab());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged reference build");
    let reference = reference.snapshot();
    let reference_program = reference
        .compile_rank_program(&program)
        .expect("reference rank program");
    let predicate = reverse_rusty::exact::TagPredicate::empty();
    let mut scratch = MatchScratch::new();

    let batch_titles: Vec<String> = titles.iter().take(48).cloned().collect();
    for &shards in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards: shards,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
            .expect("tagged cluster build");
        let cluster_program = cluster
            .compile_rank_program(&program)
            .expect("cluster rank program");

        for scope in [QueryScope::Standard, QueryScope::WithBroad] {
            for &size in &[0usize, 1, 3, 16] {
                for &threshold in &[1u64, 10_000] {
                    let options = TopKOptions {
                        size,
                        track_total_hits_up_to: threshold,
                        query_scope: scope,
                    };
                    let batch = cluster
                        .try_percolate_filtered_top_k_batch(
                            &batch_titles,
                            &[],
                            options,
                            &cluster_program,
                            None,
                        )
                        .expect("distributed batch top k");
                    assert_eq!(batch.titles.len(), batch_titles.len());
                    assert!(
                        batch.fanned_shard_calls <= shards,
                        "one batch call per involved shard"
                    );
                    let mut expected_rows = 0usize;
                    for (i, title) in batch_titles.iter().enumerate() {
                        let single = cluster
                            .try_percolate_filtered_top_k(
                                title,
                                &[],
                                options,
                                &cluster_program,
                                None,
                            )
                            .expect("single distributed top k");
                        let got: Vec<(u64, i64, u32)> = batch.titles[i]
                            .hits
                            .iter()
                            .map(|hit| (hit.logical_id, hit.score, hit.owner_position))
                            .collect();
                        let want: Vec<(u64, i64, u32)> = single
                            .hits
                            .iter()
                            .map(|hit| (hit.logical_id, hit.score, hit.owner_position))
                            .collect();
                        assert_eq!(
                            got, want,
                            "shards={shards} scope={scope:?} K={size} th={threshold} title={i}"
                        );
                        assert_eq!(batch.titles[i].total_hits, single.total_hits);
                        assert_eq!(batch.titles[i].routed_shards, single.routed_shards);
                        expected_rows += single.shard_rows_received;

                        let standalone = reference
                            .try_match_title_top_k(
                                title,
                                options,
                                &reference_program,
                                &predicate,
                                &mut scratch,
                                None,
                            )
                            .expect("standalone top k");
                        let brute: Vec<(u64, i64)> = standalone
                            .hits
                            .iter()
                            .map(|hit| (hit.logical_id, hit.score))
                            .collect();
                        let batch_rows: Vec<(u64, i64)> = batch.titles[i]
                            .hits
                            .iter()
                            .map(|hit| (hit.logical_id, hit.score))
                            .collect();
                        assert_eq!(batch_rows, brute, "batch != standalone reference");
                        assert_eq!(batch.titles[i].total_hits, standalone.total_hits);
                    }
                    assert_eq!(
                        batch.shard_rows_received, expected_rows,
                        "batch rows must be the sum of per-title bounded rows"
                    );
                }
            }
        }
    }
}

/// Cross-title winner dedup in the batch fetch: the same logical id winning
/// for several titles is fetched once, every title still receives its source,
/// and the ONE credit is charged per DELIVERED occurrence (a credit that fits
/// one delivery but not all of them must 413, never partially enrich).
#[test]
fn batch_fetch_dedups_cross_title_winners_and_charges_delivered_bytes() {
    let queries = vec![
        (1u64, "topps chrome".to_string()),
        (2u64, "zzunrelated zzterm".to_string()),
    ];
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("cluster build");
    let program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("program");
    let titles = vec![
        "2020 topps chrome update".to_string(),
        "1998 topps chrome refractor".to_string(),
        "topps chrome box".to_string(),
    ];
    let options = TopKOptions {
        size: 5,
        track_total_hits_up_to: 10_000,
        query_scope: QueryScope::Standard,
    };
    let batch = cluster
        .try_percolate_filtered_top_k_batch(&titles, &[], options, &program, None)
        .expect("batch top k");
    for (i, title) in batch.titles.iter().enumerate() {
        assert_eq!(
            title
                .hits
                .iter()
                .map(|hit| hit.logical_id)
                .collect::<Vec<_>>(),
            vec![1],
            "title {i} must be won by the shared query"
        );
    }

    let source_len = "topps chrome".len();
    let sources = cluster
        .fetch_ranked_sources_batch_bounded(&batch, 3 * source_len, None)
        .expect("exact-fit batch fetch");
    assert_eq!(sources.len(), titles.len());
    for title_sources in &sources {
        assert_eq!(title_sources.as_slice(), ["topps chrome".to_string()]);
    }

    // One occurrence fits; three deliveries do not — delivered-occurrence
    // charging must refuse rather than partially enrich.
    let err = cluster
        .fetch_ranked_sources_batch_bounded(&batch, source_len, None)
        .expect_err("under-credit batch fetch must refuse");
    assert!(matches!(err, ClusterRankedError::EnrichmentLimit { .. }));
}

/// Cluster-level batch admission is checked before any routing or shard work.
#[test]
fn batch_admission_rejects_before_fanning() {
    let queries = vec![(1u64, "topps chrome".to_string())];
    let cfg = ClusterConfig {
        num_shards: 2,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("cluster build");
    let program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("program");

    let too_many: Vec<String> = (0..=MAX_RANKED_BATCH_TITLES)
        .map(|i| format!("t{i}"))
        .collect();
    let err = cluster
        .try_percolate_filtered_top_k_batch(&too_many, &[], TopKOptions::default(), &program, None)
        .expect_err("title ceiling must reject");
    assert!(matches!(
        err,
        ClusterRankedError::Admission(TopKAdmissionError::BatchTitlesTooLarge { .. })
    ));

    let titles: Vec<String> = (0..105).map(|i| format!("t{i}")).collect();
    let err = cluster
        .try_percolate_filtered_top_k_batch(
            &titles,
            &[],
            TopKOptions {
                size: reverse_rusty::MAX_TOP_K,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &program,
            None,
        )
        .expect_err("heap budget must reject");
    assert!(matches!(
        err,
        ClusterRankedError::Admission(TopKAdmissionError::BatchHeapBudgetExceeded { .. })
    ));

    // Codex-review regression: the honest charge is routed title-shard PAIRS.
    // Two-anchor titles route to ~2 shards each, so 60 titles × fanout ≈ 120
    // routed pairs × K=10_000 exceeds the 2^20 budget even though the naive
    // titles × K charge (600k rows) admits.
    let two_anchor: Vec<String> = (0..60).map(|_| "topps chrome".to_string()).collect();
    match cluster.try_percolate_filtered_top_k_batch(
        &two_anchor,
        &[],
        TopKOptions {
            size: reverse_rusty::MAX_TOP_K,
            track_total_hits_up_to: 10_000,
            query_scope: QueryScope::Standard,
        },
        &program,
        None,
    ) {
        Err(ClusterRankedError::Admission(TopKAdmissionError::BatchHeapBudgetExceeded {
            requested_rows,
            ..
        })) => assert!(
            requested_rows > (60u64 * reverse_rusty::MAX_TOP_K as u64),
            "the routed charge must exceed the naive per-title charge"
        ),
        Ok(batch) => {
            // Routing collapsed to one shard per title on this corpus: the
            // routed charge equals the admitted naive charge — verify.
            assert!(batch.titles.iter().all(|title| title.routed_shards <= 1));
        }
        Err(other) => panic!("unexpected admission outcome: {other:?}"),
    }
}
