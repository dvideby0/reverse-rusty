//! ADR-112 real-transport verification: the streamed `PercolateTopKBatch` wire
//! (per-title frames + completeness summary) must deliver, per title, exactly
//! the single-RPC distributed result and the standalone reference; frames obey
//! the exact protobuf result cap; deadline and admission fail the whole batch.

use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ClusterRankedError, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::{QueryScope, RankProgramSpec, TopKOptions};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

fn spawn_pending(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<reverse_rusty::Normalizer>,
    count: usize,
    cap: usize,
) -> Vec<String> {
    let mut endpoints = Vec::with_capacity(count);
    let _enter = rt.enter();
    for _ in 0..count {
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
        let address: SocketAddr = incoming.local_addr().expect("local address");
        let server = ShardServer::pending(Arc::clone(norm), EngineConfig::default())
            .with_max_grpc_result_bytes(cap)
            .expect("valid result cap");
        rt.spawn(server.serve_with_incoming(incoming));
        endpoints.push(format!("http://{address}"));
    }
    endpoints
}

type Corpus = (Vec<(u64, String)>, Vec<Vec<(String, String)>>);

fn corpus() -> Corpus {
    let mut queries: Vec<(u64, String)> = (1..=60)
        .map(|id| (id, "topps chrome".to_string()))
        .collect();
    queries.push((61, "bowman draft".to_string()));
    queries.push((62, "zzabsent zzterm".to_string()));
    let tags: Vec<Vec<(String, String)>> = queries
        .iter()
        .map(|(id, _)| {
            vec![(
                "tier".to_string(),
                match id % 3 {
                    0 => "gold",
                    1 => "silver",
                    _ => "bronze",
                }
                .to_string(),
            )]
        })
        .collect();
    (queries, tags)
}

fn program() -> RankProgramSpec {
    RankProgramSpec {
        priority_field: None,
        boosts: vec![
            ("tier".to_string(), "gold".to_string(), 17),
            ("tier".to_string(), "silver".to_string(), -9),
        ],
    }
}

#[test]
fn grpc_batch_top_k_matches_single_rpcs_and_reference() {
    let (queries, tags) = corpus();
    let raw = program();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let tag_dict = frozen_tag_dict_over(&tags);

    let mut reference = Engine::new(vocab());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("reference build");
    let reference = reference.snapshot();
    let reference_program = reference.compile_rank_program(&raw).expect("rank program");
    let mut scratch = MatchScratch::new();

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints = spawn_pending(
        &rt,
        &norm,
        3,
        reverse_rusty::cluster::DEFAULT_MAX_GRPC_RESULT_BYTES,
    );
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster =
        ClusterEngine::connect_remote(norm, dict, tag_dict, &cfg, &endpoints, rt.handle())
            .expect("remote cluster");
    cluster
        .ingest_with_tags(&queries, &tags)
        .expect("wire ingest");
    let cluster_program = cluster.compile_rank_program(&raw).expect("rank program");

    let titles: Vec<String> = vec![
        "2020 topps chrome update".to_string(),
        "bowman draft chrome".to_string(),
        "no match at all".to_string(),
        "1998 topps chrome refractor".to_string(),
    ];
    for scope in [QueryScope::Standard, QueryScope::WithBroad] {
        for &size in &[0usize, 1, 7, 25] {
            let options = TopKOptions {
                size,
                track_total_hits_up_to: 13,
                query_scope: scope,
            };
            let batch = cluster
                .try_percolate_filtered_top_k_batch(&titles, &[], options, &cluster_program, None)
                .expect("remote batch top k");
            assert_eq!(batch.titles.len(), titles.len());
            if size > 0 {
                assert!(
                    batch.shard_result_bytes > 0,
                    "remote frame bytes are measured"
                );
            }
            for (i, title) in titles.iter().enumerate() {
                let single = cluster
                    .try_percolate_filtered_top_k(title, &[], options, &cluster_program, None)
                    .expect("remote single top k");
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
                assert_eq!(got, want, "scope={scope:?} K={size} title={i}");
                assert_eq!(batch.titles[i].total_hits, single.total_hits);

                let standalone = reference
                    .try_match_title_top_k(
                        title,
                        options,
                        &reference_program,
                        &reverse_rusty::exact::TagPredicate::empty(),
                        &mut scratch,
                        None,
                    )
                    .expect("standalone reference");
                let brute: Vec<(u64, i64)> = standalone
                    .hits
                    .iter()
                    .map(|hit| (hit.logical_id, hit.score))
                    .collect();
                let rows: Vec<(u64, i64)> = batch.titles[i]
                    .hits
                    .iter()
                    .map(|hit| (hit.logical_id, hit.score))
                    .collect();
                assert_eq!(rows, brute, "wire batch != standalone reference");
            }

            // Whole-batch winner enrichment over the wire under one credit.
            let sources = cluster
                .fetch_ranked_sources_batch_bounded(&batch, 16 * 1024 * 1024, None)
                .expect("batch winner fetch");
            for (i, title_sources) in sources.iter().enumerate() {
                assert_eq!(title_sources.len(), batch.titles[i].hits.len());
            }
        }
    }
}

#[test]
fn grpc_batch_caps_reject_oversize_frames() {
    let queries: Vec<(u64, String)> = (1..=300)
        .map(|id| (id, "topps chrome".to_string()))
        .collect();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints = spawn_pending(&rt, &norm, 1, 256);
    let cfg = ClusterConfig {
        num_shards: 1,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        dict,
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("remote cluster");
    cluster.ingest(&queries).expect("ingest");
    let cluster_program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("program");
    let titles = vec!["2020 topps chrome update".to_string(); 3];
    let err = cluster
        .try_percolate_filtered_top_k_batch(
            &titles,
            &[],
            TopKOptions {
                size: 300,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &cluster_program,
            None,
        )
        .expect_err("a 256-byte frame cap must refuse 300 bounded rows");
    assert!(
        matches!(err, ClusterRankedError::Shard(_)),
        "cap violation surfaces as a loud shard failure, never truncation"
    );
}

#[test]
fn grpc_batch_expired_deadline_fails_the_whole_batch() {
    let queries: Vec<(u64, String)> = vec![(1, "topps chrome".to_string())];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints = spawn_pending(
        &rt,
        &norm,
        1,
        reverse_rusty::cluster::DEFAULT_MAX_GRPC_RESULT_BYTES,
    );
    let cfg = ClusterConfig {
        num_shards: 1,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        dict,
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("remote cluster");
    cluster.ingest(&queries).expect("ingest");
    let cluster_program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("program");
    let expired = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(5))
        .expect("clock past epoch");
    let err = cluster
        .try_percolate_filtered_top_k_batch(
            &["2020 topps chrome update".to_string()],
            &[],
            TopKOptions::default(),
            &cluster_program,
            Some(expired),
        )
        .expect_err("expired deadline must fail the whole batch");
    assert!(matches!(err, ClusterRankedError::DeadlineExceeded));
}

/// Codex-review regression: a batch can hold more than `MAX_TOP_K` distinct
/// winners on ONE owner (disjoint per-title winner sets), and the wire fetch
/// handler rejects id lists above that ceiling — the coordinator must chunk
/// owner groups while carrying the one credit forward.
#[test]
fn grpc_batch_fetch_chunks_oversized_owner_groups() {
    let mut queries: Vec<(u64, String)> = (1..=6_500u64)
        .map(|id| (id, "topps chrome".to_string()))
        .collect();
    queries.extend((6_501..=13_000u64).map(|id| (id, "bowman draft".to_string())));
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints = spawn_pending(
        &rt,
        &norm,
        1,
        reverse_rusty::cluster::DEFAULT_MAX_GRPC_RESULT_BYTES,
    );
    let cfg = ClusterConfig {
        num_shards: 1,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        dict,
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("remote cluster");
    cluster.ingest(&queries).expect("ingest");
    let cluster_program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("program");
    let titles = vec![
        "2020 topps chrome update".to_string(),
        "bowman draft chrome".to_string(),
    ];
    let batch = cluster
        .try_percolate_filtered_top_k_batch(
            &titles,
            &[],
            TopKOptions {
                size: 6_500,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &cluster_program,
            None,
        )
        .expect("batch top k");
    let distinct: std::collections::HashSet<u64> = batch
        .titles
        .iter()
        .flat_map(|title| title.hits.iter().map(|hit| hit.logical_id))
        .collect();
    assert!(
        distinct.len() > reverse_rusty::MAX_TOP_K,
        "fixture must exceed the per-request fetch ceiling (got {})",
        distinct.len()
    );
    let sources = cluster
        .fetch_ranked_sources_batch_bounded(&batch, 64 * 1024 * 1024, None)
        .expect("oversized owner group must be chunked, not rejected");
    for (i, title_sources) in sources.iter().enumerate() {
        assert_eq!(title_sources.len(), batch.titles[i].hits.len());
    }
}
