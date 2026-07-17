//! ADR-110 real-transport verification: bounded owned shard rows, coordinator
//! merge, winner-only source streaming, and exact protobuf result caps.

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

#[test]
fn grpc_top_k_and_winner_fetch_match_single_node() {
    let queries: Vec<(u64, String)> = (1..=80)
        .map(|id| (id, "topps chrome".to_string()))
        .collect();
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
    let raw = RankProgramSpec {
        priority_field: None,
        boosts: vec![
            ("tier".to_string(), "gold".to_string(), 17),
            ("tier".to_string(), "silver".to_string(), -9),
        ],
    };
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

    for &size in &[0usize, 1, 7, 25] {
        for &threshold in &[0u64, 13, 10_000] {
            let options = TopKOptions {
                size,
                track_total_hits_up_to: threshold,
                query_scope: QueryScope::Standard,
            };
            let want = reference
                .try_match_title_top_k(
                    "2020 topps chrome update",
                    options,
                    &reference_program,
                    &reverse_rusty::exact::TagPredicate::empty(),
                    &mut scratch,
                    None,
                )
                .expect("local top k");
            let got = cluster
                .try_percolate_filtered_top_k(
                    "2020 topps chrome update",
                    &[],
                    options,
                    &cluster_program,
                    None,
                )
                .expect("remote top k");
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
            assert_eq!(got_rows, want_rows);
            assert_eq!(got.total_hits, want.total_hits);
            assert!(got.shard_rows_received <= size * got.routed_shards);
            assert!(
                got.shard_rows_received <= size,
                "replicated class-B rows have one request owner before shard heaps"
            );
            assert!(
                got.hits
                    .iter()
                    .map(|hit| hit.owner_position)
                    .all(|owner| got
                        .hits
                        .first()
                        .is_none_or(|first| owner == first.owner_position)),
                "all replicated winners must name the one routed emission owner"
            );
            assert!(
                got.shard_result_bytes > 0,
                "remote reply bytes are measured"
            );

            let sources = cluster
                .fetch_ranked_sources(&got, None)
                .expect("winner source stream");
            assert_eq!(sources.len(), got.hits.len());
            assert!(sources.iter().all(|source| source == "topps chrome"));
        }
    }

    cluster
        .add_query_with_tags(
            9_999,
            "zzwirepriority",
            &[("priority".to_string(), "-123".to_string())],
        )
        .expect("post-freeze typed-priority-compatible add");
    let priority_program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("priority program");
    let priority = cluster
        .try_percolate_filtered_top_k(
            "zzwirepriority",
            &[],
            TopKOptions {
                size: 1,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &priority_program,
            None,
        )
        .expect("post-freeze priority over wire");
    assert_eq!(priority.hits[0].score, -123);
}

#[test]
fn grpc_caps_reject_oversize_top_k_and_fetch_items() {
    // Many bounded rows overflow a deliberately tiny unary-reply cap.
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
    assert!(
        cluster.percolate("topps chrome").is_err(),
        "legacy all-id percolate reply must obey the same protobuf cap"
    );
    let program = cluster
        .compile_rank_program(&RankProgramSpec {
            priority_field: None,
            boosts: Vec::new(),
        })
        .expect("program");
    let error = cluster
        .try_percolate_filtered_top_k(
            "topps chrome",
            &[],
            TopKOptions {
                size: 300,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &program,
            None,
        )
        .expect_err("oversize top-k reply must fail");
    assert!(matches!(error, ClusterRankedError::Shard(_)));

    // Aggregate source credit is enforced while the server stream is drained,
    // independently of the larger per-item protobuf cap. Two 12-byte sources do
    // not fit in 23 bytes; the stream fails closed rather than buffering all rows.
    let bounded = cluster
        .try_percolate_filtered_top_k(
            "topps chrome",
            &[],
            TopKOptions {
                size: 3,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::Standard,
            },
            &program,
            None,
        )
        .expect("small bounded reply");
    assert!(matches!(
        cluster.fetch_ranked_sources_bounded(&bounded, 23, None),
        Err(ClusterRankedError::EnrichmentLimit { .. })
    ));

    // A separate node's top-k reply fits, but one winner source stream item does not.
    let long_source = format!("({})", vec!["zzlongsource"; 30].join(","));
    let long_queries = vec![(999u64, long_source.clone())];
    let long_dict = frozen_dict_over(&long_queries, &norm);
    let endpoints = spawn_pending(&rt, &norm, 1, 256);
    let cluster = ClusterEngine::connect_remote(
        norm,
        long_dict,
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("source-cap cluster");
    cluster.ingest(&long_queries).expect("long source ingest");
    assert_eq!(
        cluster.num_queries().expect("count"),
        1,
        "long query stored"
    );
    let program = cluster
        .compile_rank_program(&RankProgramSpec {
            priority_field: None,
            boosts: Vec::new(),
        })
        .expect("program");
    let ranked = cluster
        .try_percolate_filtered_top_k(
            "zzlongsource",
            &[],
            TopKOptions {
                size: 1,
                track_total_hits_up_to: 10_000,
                query_scope: QueryScope::WithBroad,
            },
            &program,
            None,
        )
        .expect("small top-k reply");
    assert_eq!(ranked.hits.len(), 1);
    assert!(
        cluster.fetch_ranked_sources(&ranked, None).is_err(),
        "oversize fetch stream item must invalidate enrichment"
    );
}
