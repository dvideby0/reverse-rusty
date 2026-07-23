//! ADR-114 real-transport exhaustive stream equivalence.

use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, RemoteShard, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::delivery::{ChunkSink, ChunkSinkError, DeliveryChecksum, MatchChunk};
use reverse_rusty::{QueryScope, RankProgramSpec};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

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
fn grpc_exhaustive_chunks_equal_the_exact_cluster_set() {
    let queries: Vec<(u64, String)> = (1..=80)
        .map(|id| (id, "topps chrome".to_string()))
        .collect();
    let tags: Vec<Vec<(String, String)>> = queries
        .iter()
        .map(|(id, _)| {
            vec![(
                "tier".to_string(),
                if id % 2 == 0 { "gold" } else { "silver" }.to_string(),
            )]
        })
        .collect();
    let raw = RankProgramSpec {
        priority_field: None,
        boosts: vec![("tier".into(), "gold".into(), 11)],
    };
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let tag_dict = frozen_tag_dict_over(&tags);
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
    cluster
        .ingest_with_tags(&queries, &tags)
        .expect("wire ingest");
    let program = cluster.compile_rank_program(&raw).expect("rank program");
    let expected = cluster
        .percolate_with_broad("2020 topps chrome update", true)
        .expect("compatibility result");

    let mut sink = RecordingSink::default();
    let result = cluster
        .try_percolate_filtered_all(
            "2020 topps chrome update",
            &[],
            QueryScope::WithBroad,
            Some(&program),
            7,
            None,
            &mut sink,
        )
        .expect("remote exhaustive result");
    assert!(sink.chunks.iter().all(|chunk| chunk.matches.len() <= 7));
    assert_eq!(
        sink.chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        (0..sink.chunks.len() as u64).collect::<Vec<_>>()
    );
    let mut delivered: Vec<u64> = sink
        .chunks
        .iter()
        .flat_map(|chunk| chunk.matches.iter().map(|member| member.logical_id))
        .collect();
    delivered.sort_unstable();
    assert_eq!(delivered, expected);
    assert!(sink
        .chunks
        .iter()
        .flat_map(|chunk| &chunk.matches)
        .all(|member| member.score == Some(if member.logical_id % 2 == 0 { 11 } else { 0 })));
    let mut checksum = DeliveryChecksum::default();
    for member in sink.chunks.iter().flat_map(|chunk| &chunk.matches) {
        checksum.observe(*member);
    }
    assert_eq!(result.summary.exact_total, expected.len() as u64);
    assert_eq!(result.summary.chunk_count, sink.chunks.len() as u64);
    assert_eq!(result.summary.checksum, checksum);
    assert!(cluster
        .transport_metrics()
        .methods
        .iter()
        .any(|method| method.method == "percolate_all" && method.calls > 0));
}

#[test]
fn grpc_exhaustive_frame_cap_fails_loud_without_completion() {
    let queries: Vec<(u64, String)> = (1..=40)
        .map(|id| (id, "topps chrome".to_string()))
        .collect();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints = spawn_pending(&rt, &norm, 1, 128);
    let cluster = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        dict,
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        &endpoints,
        rt.handle(),
        RemoteShard::new_coordinator_id(),
    )
    .expect("remote cluster");
    cluster.ingest(&queries).expect("ingest");

    let mut sink = RecordingSink::default();
    assert!(
        cluster
            .try_percolate_filtered_all(
                "topps chrome",
                &[],
                QueryScope::Standard,
                None,
                40,
                None,
                &mut sink,
            )
            .is_err(),
        "oversize frame must fail the exact stream"
    );
    assert!(sink.chunks.is_empty(), "no capped frame may escape");
}

#[test]
fn grpc_compatibility_coordinator_cannot_attest_exhaustive_completion() {
    let queries = vec![(1, "topps chrome".to_string())];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints = spawn_pending(
        &rt,
        &norm,
        1,
        reverse_rusty::cluster::DEFAULT_MAX_GRPC_RESULT_BYTES,
    );
    let cluster = ClusterEngine::connect_remote(
        norm,
        dict,
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 1,
            ..ClusterConfig::default()
        },
        &endpoints,
        rt.handle(),
    )
    .expect("compatibility coordinator");
    cluster.ingest(&queries).expect("compatibility ingest");

    let mut sink = RecordingSink::default();
    let error = cluster
        .try_percolate_filtered_all(
            "topps chrome",
            &[],
            QueryScope::Standard,
            None,
            8,
            None,
            &mut sink,
        )
        .expect_err("an unleased coordinator cannot attest an exact remote view");
    assert!(
        error.to_string().contains("exclusive coordinator lease"),
        "unexpected refusal: {error}"
    );
    assert!(sink.chunks.is_empty());
}

#[test]
fn grpc_shard_set_rejects_a_second_coordinator_even_while_empty() {
    let queries: Vec<(u64, String)> = (1..=24)
        .map(|id| (id, "topps chrome".to_string()))
        .collect();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let tag_dict = empty_tag_dict();
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let endpoints = spawn_pending(
        &rt,
        &norm,
        2,
        reverse_rusty::cluster::DEFAULT_MAX_GRPC_RESULT_BYTES,
    );
    let config = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };

    let first_id = RemoteShard::new_coordinator_id();
    let first = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        &config,
        &endpoints,
        rt.handle(),
        first_id,
    )
    .expect("fresh coordinator");
    // Both coordinators would otherwise see zero rows and declare their
    // process-local mutation barriers authoritative. The shard-node lease must
    // reject the second one before either can mutate or certify the shared set.
    let second = ClusterEngine::connect_remote_exclusive(
        norm,
        dict,
        tag_dict,
        &config,
        &endpoints,
        rt.handle(),
        RemoteShard::new_coordinator_id(),
    );
    let Err(error) = second else {
        panic!("a live shard set accepted a second remote coordinator");
    };
    assert!(
        error.to_string().contains("exclusively leased"),
        "unexpected refusal: {error}"
    );
    first
        .ingest(&queries)
        .expect("the lease owner remains usable");
}

#[test]
fn grpc_existing_exclusive_client_reclaims_a_restarted_durable_shard() {
    let queries = vec![
        (1, "topps chrome".to_string()),
        (2, "topps chrome update".to_string()),
    ];
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let dir = server_dir("exclusive_lease_restart");

    // Keep the coordinator/client runtime separate: dropping the server
    // runtime terminates both the accept loop and all existing HTTP/2
    // connections, faithfully modelling a shard-process crash.
    let client_rt = tokio::runtime::Runtime::new().expect("client runtime");
    let server_rt = tokio::runtime::Runtime::new().expect("first server runtime");
    let addr = {
        let _enter = server_rt.enter();
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind initial shard");
        let addr = incoming.local_addr().expect("initial shard address");
        let server =
            ShardServer::pending_durable(Arc::clone(&norm), EngineConfig::default(), dir.clone());
        server_rt.spawn(server.serve_with_incoming(incoming));
        addr
    };
    wait_until_listening(addr);
    let endpoint = format!("http://{addr}");
    let cluster = ClusterEngine::connect_remote_exclusive(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &ClusterConfig {
            num_shards: 1,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &[endpoint],
        client_rt.handle(),
        RemoteShard::new_coordinator_id(),
    )
    .expect("exclusive remote cluster");
    cluster.ingest(&queries).expect("wire ingest");
    cluster.flush().expect("durable flush");

    drop(server_rt);
    let server_rt = tokio::runtime::Runtime::new().expect("second server runtime");
    {
        let _enter = server_rt.enter();
        let incoming = TcpIncoming::bind(addr).expect("rebind restarted shard");
        let server =
            ShardServer::open_durable(Arc::clone(&norm), EngineConfig::default(), dir.clone())
                .expect("reopen durable shard");
        server_rt.spawn(server.serve_with_incoming(incoming));
    }
    wait_until_listening(addr);

    // Exercise the bespoke streaming seam first. Its initial ordinary RPC is
    // rejected by the fresh process's empty lease, then the retained
    // claim-only fingerprint client reclaims ownership and reissues before
    // any provisional chunk escapes.
    let mut sink = RecordingSink::default();
    let delivered = cluster
        .try_percolate_filtered_all(
            "topps chrome update",
            &[],
            QueryScope::Standard,
            None,
            1,
            None,
            &mut sink,
        )
        .expect("exhaustive stream after shard restart");
    assert_eq!(delivered.summary.exact_total, 2);

    // Restart once more so the ordinary unary call seam independently proves
    // the same reclaim-and-retry path.
    drop(server_rt);
    let server_rt = tokio::runtime::Runtime::new().expect("third server runtime");
    {
        let _enter = server_rt.enter();
        let incoming = TcpIncoming::bind(addr).expect("rebind shard a second time");
        let server =
            ShardServer::open_durable(Arc::clone(&norm), EngineConfig::default(), dir.clone())
                .expect("reopen durable shard a second time");
        server_rt.spawn(server.serve_with_incoming(incoming));
    }
    wait_until_listening(addr);
    let mut matches = cluster
        .percolate("topps chrome update")
        .expect("unary percolate after second shard restart");
    matches.sort_unstable();
    assert_eq!(matches, vec![1, 2]);

    drop(cluster);
    drop(server_rt);
    drop(client_rt);
    let _ = std::fs::remove_dir_all(dir);
}
