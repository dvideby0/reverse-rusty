//! ADR-163: named CPU profiles cross the gRPC topology only when the shard
//! resolves and echoes the coordinator's exact semantic fingerprint.

use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ClusterRankedError, RemoteShard, ShardError, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::delivery::{ChunkSink, ChunkSinkError, MatchChunk};
use reverse_rusty::{QueryScope, RankProfiles, RankProgramSpec, TopKOptions};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

fn profiles(intercept: i64) -> Arc<RankProfiles> {
    Arc::new(
        RankProfiles::from_json_slice(
            format!(
                r#"{{
                  "version":1,
                  "profiles":{{
                    "linear_v1":{{
                      "kind":"linear",
                      "intercept":{intercept},
                      "weights":[
                        {{"feature":"query_positive_terms","weight":100}},
                        {{"feature":"positive_coverage_milli","weight":1}}
                      ]
                    }}
                  }}
                }}"#
            )
            .as_bytes(),
        )
        .expect("profile registry"),
    )
}

fn spawn_pending(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<reverse_rusty::Normalizer>,
    profiles: &Arc<RankProfiles>,
    count: usize,
) -> Vec<String> {
    let mut endpoints = Vec::with_capacity(count);
    let _enter = rt.enter();
    for _ in 0..count {
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
        let address: SocketAddr = incoming.local_addr().expect("local address");
        let server = ShardServer::pending(Arc::clone(norm), EngineConfig::default())
            .with_rank_profiles(Arc::clone(profiles));
        rt.spawn(server.serve_with_incoming(incoming));
        endpoints.push(format!("http://{address}"));
    }
    endpoints
}

fn rank_spec() -> RankProgramSpec {
    RankProgramSpec {
        profile: Some("linear_v1".into()),
        priority_field: None,
        boosts: Vec::new(),
    }
}

fn options() -> TopKOptions {
    TopKOptions {
        search_after: None,
        size: 10,
        track_total_hits_up_to: 100,
        query_scope: QueryScope::WithBroad,
    }
}

fn build_cluster(
    rt: &tokio::runtime::Runtime,
    shard_profiles: &Arc<RankProfiles>,
) -> (ClusterEngine, Vec<(u64, String)>) {
    let queries = vec![
        (1, "acme".to_string()),
        (2, "acme chrome".to_string()),
        (3, "acme chrome 2024".to_string()),
    ];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let tag_dict = frozen_tag_dict_over(&vec![Vec::new(); queries.len()]);
    let endpoints = spawn_pending(rt, &norm, shard_profiles, 3);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote_exclusive(
        norm,
        dict,
        tag_dict,
        &cfg,
        &endpoints,
        rt.handle(),
        RemoteShard::new_coordinator_id(),
    )
    .expect("remote cluster");
    cluster.ingest(&queries).expect("wire ingest");
    (cluster, queries)
}

#[derive(Default)]
struct RecordingSink {
    chunks: Vec<MatchChunk>,
}

impl ChunkSink for RecordingSink {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        self.chunks.push(chunk.clone());
        Ok(())
    }
}

#[test]
fn rich_profile_is_attested_across_scalar_batch_and_exhaustive_rpc_paths() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let profiles = profiles(7);
    let (cluster, _) = build_cluster(&rt, &profiles);
    let program = cluster
        .compile_rank_program_with_profiles(&rank_spec(), &profiles)
        .expect("rank program");

    let scalar = cluster
        .try_percolate_filtered_top_k("acme chrome 2024", &[], options(), &program, None)
        .expect("remote rich top-k");
    assert_eq!(scalar.hits.len(), 3);
    assert!(
        scalar
            .hits
            .windows(2)
            .all(|rows| rows[0].score > rows[1].score),
        "specificity profile should produce a strict order: {:?}",
        scalar.hits
    );

    let titles = ["acme chrome 2024", "acme chrome"];
    let batch = cluster
        .try_percolate_filtered_top_k_batch(&titles, &[], options(), &program, None)
        .expect("remote rich batch");
    assert_eq!(batch.titles[0].hits, scalar.hits);
    assert_eq!(batch.titles[1].hits.len(), 2);

    let mut sink = RecordingSink::default();
    let exhaustive = cluster
        .try_percolate_filtered_all(
            titles[0],
            &[],
            QueryScope::WithBroad,
            Some(&program),
            2,
            None,
            &mut sink,
        )
        .expect("remote rich exhaustive");
    assert_eq!(exhaustive.summary.exact_total, 3);
    assert!(sink
        .chunks
        .iter()
        .flat_map(|chunk| &chunk.matches)
        .all(|member| member.score.is_some()));
}

#[test]
fn divergent_shard_profile_fingerprint_fails_before_scores_are_accepted() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let coordinator_profiles = profiles(7);
    let shard_profiles = profiles(8);
    let (cluster, _) = build_cluster(&rt, &shard_profiles);
    let program = cluster
        .compile_rank_program_with_profiles(&rank_spec(), &coordinator_profiles)
        .expect("coordinator rank program");

    let error = cluster
        .try_percolate_filtered_top_k("acme chrome 2024", &[], options(), &program, None)
        .expect_err("model drift must fail closed");
    assert!(matches!(
        error,
        ClusterRankedError::Shard(ShardError::Protocol(ref detail))
            if detail.contains("fingerprint mismatch")
    ));
}
