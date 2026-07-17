//! Cluster ranking (ADR-059/075): the coordinator compiles the `RankSpec` once against
//! the shared frozen tag space, each shard scores its own matched ids, and the merged
//! scored set must equal the single-node engine's `rank` over the same corpus — with
//! the ranked id set IDENTICAL to the unranked one (ranking reorders, never gates).

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::{QueryScope, RankProgramSpec, RankSpec, TopKOptions};
use std::collections::HashSet;

/// Corpus tags extended with a numeric `priority` tag on a slice of queries — interned
/// at build (priority scoring reads the tag's VALUE string via `key_value`, which only
/// an interned tag has; the synthetic boundary is pinned by
/// `synthetic_tags_boost_but_do_not_priority_score`).
fn ranked_tags_parallel(queries: &[(u64, String)]) -> Vec<Vec<(String, String)>> {
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

fn rank_spec() -> RankSpec {
    RankSpec {
        priority_key: Some("priority".to_string()),
        boosts: vec![
            ("category".to_string(), "cards".to_string(), 1000),
            ("status".to_string(), "active".to_string(), 250),
        ],
    }
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

/// ADR-110's load-bearing differential: ownership filtering happens before each
/// shard heap, so merging at most K rows per routed shard must equal standalone
/// collect-all semantics for every K/threshold combination (including count-only).
#[test]
fn distributed_bounded_top_k_matches_single_node() {
    let (queries, titles) = build_corpus();
    let tags = ranked_tags_parallel(&queries);
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

        for title in titles.iter().take(40) {
            for &size in &[0usize, 1, 3, 16] {
                for &threshold in &[0u64, 1, 10_000] {
                    let options = TopKOptions {
                        size,
                        track_total_hits_up_to: threshold,
                        query_scope: QueryScope::WithBroad,
                    };
                    let want = reference
                        .try_match_title_top_k(
                            title,
                            options,
                            &reference_program,
                            &predicate,
                            &mut scratch,
                            None,
                        )
                        .expect("standalone top k");
                    let got = cluster
                        .try_percolate_filtered_top_k(title, &[], options, &cluster_program, None)
                        .expect("distributed top k");
                    let got_rows: Vec<(u64, i64)> = got
                        .hits
                        .iter()
                        .map(|hit| (hit.logical_id, hit.score))
                        .collect();
                    let want_rows: Vec<(u64, i64)> = want
                        .hits
                        .iter()
                        .map(|hit| (hit.logical_id, hit.score))
                        .collect();
                    assert_eq!(got_rows, want_rows, "shards={shards}, K={size}");
                    assert_eq!(got.total_hits, want.total_hits, "shards={shards}, K={size}");
                    assert!(
                        got.shard_rows_received <= size.saturating_mul(got.routed_shards),
                        "coordinator received more than K rows per routed position"
                    );
                }
            }
        }
    }
}

/// Pin every placement/cost class in the bounded differential, including the
/// opt-in universal class-D lane and ADR-105's always-visible class-H tier.
#[test]
fn distributed_top_k_covers_every_visibility_and_cost_class() {
    const THETA: u32 = 32;
    let (mut queries, mut titles) = build_corpus();
    queries.push((99_999_999, "-autograph".to_string()));
    titles.push("1994 topps chrome psa 10".to_string());
    titles.push("1994 topps chrome autograph psa 10".to_string());
    let tags = ranked_tags_parallel(&queries);
    let engine_config = EngineConfig {
        accept_class_d: true,
        hot_anchor_threshold: THETA,
        ..EngineConfig::default()
    };
    let mut reference = Engine::with_config(vocab(), engine_config.clone());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("all-class reference build");
    let reference_counts = reference.class_counts();
    for (class, count) in ["A", "B", "C", "D", "H"].into_iter().zip(reference_counts) {
        assert!(count > 0, "fixture stored no class-{class} queries");
    }
    let reference = reference.snapshot();
    let raw_program = rank_program();
    let reference_program = reference
        .compile_rank_program(&raw_program)
        .expect("reference program");

    let mut config = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    config.per_shard = engine_config;
    let cluster = ClusterEngine::build_with_tags(vocab(), &config, &queries, &tags)
        .expect("all-class cluster build");
    let cluster_counts = cluster.class_counts().expect("class counts");
    for (class, count) in ["A", "B", "C", "D", "H"].into_iter().zip(cluster_counts) {
        assert!(count > 0, "cluster stored no class-{class} queries");
    }
    let cluster_program = cluster
        .compile_rank_program(&raw_program)
        .expect("cluster program");
    let options = TopKOptions {
        size: 32,
        track_total_hits_up_to: 10_000,
        query_scope: QueryScope::WithBroad,
    };
    let mut scratch = MatchScratch::new();
    for title in titles.iter().take(80).chain(titles.iter().rev().take(2)) {
        let want = reference
            .try_match_title_top_k(
                title,
                options,
                &reference_program,
                &reverse_rusty::exact::TagPredicate::empty(),
                &mut scratch,
                None,
            )
            .expect("reference top k");
        let got = cluster
            .try_percolate_filtered_top_k(title, &[], options, &cluster_program, None)
            .expect("cluster top k");
        assert_eq!(
            got.hits
                .iter()
                .map(|hit| (hit.logical_id, hit.score))
                .collect::<Vec<_>>(),
            want.hits
                .iter()
                .map(|hit| (hit.logical_id, hit.score))
                .collect::<Vec<_>>()
        );
        assert_eq!(got.total_hits, want.total_hits);
        assert!(got.shard_rows_received <= options.size * got.routed_shards);
    }
}

#[test]
fn global_threshold_overflow_and_generation_drift_fail_closed() {
    let queries: Vec<(u64, String)> = (0..100u64)
        .map(|id| (id + 1, format!("zzthreshold{id}")))
        .collect();
    let title = (0..100u64)
        .map(|id| format!("zzthreshold{id}"))
        .collect::<Vec<_>>()
        .join(" ");
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("cluster build");
    let program = cluster
        .compile_rank_program(&RankProgramSpec {
            priority_field: None,
            boosts: Vec::new(),
        })
        .expect("rank program");
    let exact = cluster
        .try_percolate_filtered_top_k(
            &title,
            &[],
            TopKOptions {
                size: 100,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::WithBroad,
            },
            &program,
            None,
        )
        .expect("exact owner census");
    assert_eq!(exact.total_hits, reverse_rusty::TotalHits::exact(100));
    let mut owner_counts = [0u64; 3];
    for hit in &exact.hits {
        owner_counts[hit.owner_position as usize] += 1;
    }
    let threshold = *owner_counts.iter().max().expect("owners");
    assert!(threshold < 100, "fixture must distribute ownership");
    let bounded = cluster
        .try_percolate_filtered_top_k(
            &title,
            &[],
            TopKOptions {
                size: 5,
                track_total_hits_up_to: threshold,
                query_scope: QueryScope::WithBroad,
            },
            &program,
            None,
        )
        .expect("thresholded top k");
    assert_eq!(
        bounded.total_hits,
        reverse_rusty::TotalHits::lower_bound(threshold),
        "individually exact shard totals whose global sum crosses the threshold must merge to gte"
    );

    cluster.resize(4).expect("in-process resize");
    assert!(
        cluster.fetch_ranked_sources(&exact, None).is_err(),
        "phase-two enrichment must reject placement generation drift"
    );
}

#[test]
fn top_k_preserves_dynamic_vocab_canonical_members_and_current_view_fetch() {
    let seed = vec![(1u64, "seedterm".to_string())];
    let cfg = ClusterConfig {
        num_shards: 3,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster build");
    cluster
        .add_query_with_tags(
            10,
            "zzdynamicterm",
            &[("tier".to_string(), "gold".to_string())],
        )
        .expect("dynamic add 10");
    cluster
        .add_query_with_tags(
            11,
            "zzdynamicterm",
            &[("tier".to_string(), "silver".to_string())],
        )
        .expect("canonical-body member 11");
    let raw = RankProgramSpec {
        priority_field: None,
        boosts: vec![
            ("tier".to_string(), "gold".to_string(), 20),
            ("tier".to_string(), "silver".to_string(), -5),
        ],
    };
    let program = cluster.compile_rank_program(&raw).expect("rank program");
    let ranked = cluster
        .try_percolate_filtered_top_k(
            "zzdynamicterm",
            &[],
            TopKOptions {
                size: 10,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &program,
            None,
        )
        .expect("dynamic top k");
    let rows: Vec<(u64, i64)> = ranked
        .hits
        .iter()
        .map(|hit| (hit.logical_id, hit.score))
        .collect();
    assert_eq!(rows, vec![(10, 20), (11, -5)]);
    assert_eq!(
        cluster.fetch_ranked_sources(&ranked, None).expect("fetch"),
        vec!["zzdynamicterm".to_string(), "zzdynamicterm".to_string()]
    );
    assert!(matches!(
        cluster.fetch_ranked_sources_bounded(&ranked, 2 * "zzdynamicterm".len() - 1, None),
        Err(reverse_rusty::cluster::ClusterRankedError::EnrichmentLimit { .. })
    ));
    assert_eq!(
        cluster
            .fetch_ranked_sources_bounded(&ranked, 2 * "zzdynamicterm".len(), None)
            .expect("exact byte credit"),
        vec!["zzdynamicterm".to_string(), "zzdynamicterm".to_string()]
    );

    cluster.remove_query(10).expect("delete winner");
    assert!(
        cluster.fetch_ranked_sources(&ranked, None).is_err(),
        "a missing current-view winner source invalidates all enrichment"
    );
}

/// The cluster ranking differential: across K, for a sample of titles, the cluster's
/// scored set ≡ the single-node engine's `rank` over the same tagged corpus (same
/// ids, same scores), the ranked id set ≡ the unranked percolate (recall guard), and
/// the `(score desc, _id asc)` presentation order is byte-identical to single-node.
#[test]
fn cluster_ranking_matches_single_node_and_preserves_recall() {
    let (queries, titles) = build_corpus();
    let tags = ranked_tags_parallel(&queries);
    let spec = rank_spec();

    // Single-node tagged reference: the ranking ground truth.
    let mut reference = Engine::new(vocab());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged single-node build");
    let ref_snap = reference.snapshot();
    let cspec = ref_snap.compile_rank_spec(&spec);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();

    let mut scored_nonzero = 0usize;
    for &k in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
            .expect("tagged cluster build");

        for (ti, title) in titles.iter().take(120).enumerate() {
            // Cluster: scored rows (sorted by id) + the unranked set.
            let (got, _stats) = cluster
                .percolate_filtered_ranked(title, &[], true, &spec)
                .expect("ranked percolate");
            let unranked: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();

            // Recall guard: ranking reorders, never gates — identical id set.
            let ranked_ids: HashSet<u64> = got.iter().map(|&(id, _)| id).collect();
            assert_eq!(
                ranked_ids, unranked,
                "K={k}: ranked id set must equal the unranked set (title {ti})"
            );

            // Score differential: cluster scored rows ≡ single-node rank over the
            // same matched set.
            ref_snap.match_title_filtered(
                title,
                &mut s,
                &mut out,
                true,
                &reverse_rusty::exact::TagPredicate::empty(),
            );
            let mut want = ref_snap.rank(&out, &cspec);
            want.sort_unstable_by_key(|&(id, _)| id);
            assert_eq!(
                got, want,
                "K={k}: cluster scores diverge from single-node (title {ti})"
            );

            // Presentation order: the same (score desc, _id asc) total order.
            let mut got_order = got.clone();
            got_order.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let mut want_order = want;
            want_order.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            assert_eq!(got_order, want_order, "K={k}: order diverges (title {ti})");
            if got_order.iter().any(|&(_, s)| s != 0) {
                scored_nonzero += 1;
            }
        }
    }
    assert!(
        scored_nonzero > 0,
        "degenerate: no title ever produced a non-zero score"
    );
}

/// The synthetic-tag ranking boundary (ADR-075, pinned not hidden): a post-freeze tag
/// resolves to a synthetic id with NO stored string, so a BOOST on it fires (boost
/// matching is id-equality) but a synthetic `priority` tag contributes 0 (priority
/// needs the value STRING, which only an interned tag has). Single-node behaves
/// identically given the same frozen tag space.
#[test]
fn synthetic_tags_boost_but_do_not_priority_score() {
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    // Untagged build ⇒ empty frozen tag dict ⇒ every live tag below is synthetic.
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");
    cluster
        .add_query_with_tags(
            100,
            "zzrankalpha",
            &[
                ("region".to_string(), "emea".to_string()),
                ("priority".to_string(), "500".to_string()),
            ],
        )
        .expect("tagged live add");

    let spec = RankSpec {
        priority_key: Some("priority".to_string()),
        boosts: vec![("region".to_string(), "emea".to_string(), 70)],
    };
    let (got, _) = cluster
        .percolate_filtered_ranked("zzrankalpha", &[], true, &spec)
        .expect("ranked percolate");
    assert_eq!(
        got,
        vec![(100, 70)],
        "synthetic boost fires (id-equality); synthetic priority contributes 0 \
         (no recoverable value string)"
    );
}

/// The merge's dedup-keeps-any-copy assumption, pinned (review note): a selective
/// any-of query fans one logical id to SEVERAL shards, and the coordinator merge
/// dedups by id after an UNSTABLE sort — which copy survives is arbitrary, so the
/// invariant that makes that safe (copies are version-identical, hence score-equal)
/// must hold observably. A title containing both any-of members probes both holding
/// shards; the merged row's score must be the exact tag-derived value regardless of
/// which shard's copy won the dedup.
#[test]
fn fanned_out_copies_merge_with_one_exact_score() {
    let (mut queries, _titles) = build_corpus();
    // Pure any-of of two rare members ⇒ Selective placement on BOTH members' shards.
    let q_fan = 9_500_001u64;
    queries.push((q_fan, "(zzfanleft,zzfanright)".into()));
    let mut tags = ranked_tags_parallel(&queries);
    // Deterministic interned tags for the fanned query: boost(1000-eligible) + priority.
    *tags.last_mut().expect("tags for q_fan") = vec![
        ("category".to_string(), "cards".to_string()),
        ("priority".to_string(), "33".to_string()),
    ];

    for &k in &[3usize, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
            .expect("tagged cluster build");

        // Both members in the title ⇒ both holding shards probed ⇒ two copies merge.
        let (scored, _) = cluster
            .percolate_filtered_ranked("zzfanleft zzfanright psa", &[], true, &rank_spec())
            .expect("ranked percolate");
        let rows: Vec<&(u64, i64)> = scored.iter().filter(|&&(id, _)| id == q_fan).collect();
        assert_eq!(
            rows.len(),
            1,
            "K={k}: exactly one merged row for the fanned id"
        );
        assert_eq!(
            rows[0].1,
            1000 + 33,
            "K={k}: the merged score is the exact tag-derived value (cards boost + priority), \
             whichever copy survived the dedup"
        );
    }
}

/// Ranking composes with a tag filter: the scored set is exactly the FILTERED set
/// (filter first, then score) — never a score for a filtered-out query.
#[test]
fn ranking_composes_with_filter() {
    let (queries, titles) = build_corpus();
    let tags = ranked_tags_parallel(&queries);
    let spec = rank_spec();
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
        .expect("tagged cluster build");

    let filter = vec![("category".to_string(), vec!["cards".to_string()])];
    let mut nonempty = 0usize;
    for title in titles.iter().take(60) {
        let (scored, _) = cluster
            .percolate_filtered_ranked(title, &filter, true, &spec)
            .expect("ranked filtered percolate");
        let filtered: HashSet<u64> = cluster
            .percolate_filtered(title, &filter)
            .unwrap()
            .into_iter()
            .collect();
        let scored_ids: HashSet<u64> = scored.iter().map(|&(id, _)| id).collect();
        assert_eq!(
            scored_ids, filtered,
            "ranked+filtered id set must equal the filtered set"
        );
        nonempty += usize::from(!scored.is_empty());
    }
    assert!(nonempty > 0, "degenerate: filter never matched");
}
