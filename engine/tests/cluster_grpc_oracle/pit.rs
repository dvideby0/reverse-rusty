//! ADR-113: a remote/gRPC assembly REFUSES point-in-time pagination typed —
//! wire PIT is a named later increment. The refusal must be loud (never a
//! silent current-view page into a cursor stream), leak no registry state,
//! and leave normal current-view reads untouched.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ClusterPitError, ClusterRankedError, ShardError, ShardServer,
};
use reverse_rusty::compile::extract;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::{PitConfig, RankProgramSpec, TopKOptions};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

#[test]
fn grpc_assembly_refuses_pit_typed_and_leaks_nothing() {
    let queries: Vec<(u64, String)> = vec![
        (1, "michael jordan".to_string()),
        (2, "jordan psa 10".to_string()),
        (3, "topps chrome".to_string()),
    ];
    let norm = Arc::new(vocab());
    let dict = {
        let mut d = Dict::new();
        let mut lc = String::new();
        for (_id, text) in &queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let _ = extract(&ast, &norm, &mut d, &mut lc);
            }
        }
        d.finalize_mask();
        Arc::new(d)
    };

    let k = 2usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                EngineConfig::default(),
            );
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest over gRPC");

    // Open refuses typed with the operator-facing alternative, and the
    // fail-closed unwind leaves no registry entry behind.
    let err = cluster
        .open_pit(None, &PitConfig::default(), Instant::now())
        .expect_err("remote assembly must refuse PIT");
    match err {
        ClusterPitError::Unsupported(detail) => {
            assert!(
                detail.contains("later increment"),
                "refusal names the deferral: {detail}"
            );
        }
        other @ ClusterPitError::Admission(_) => panic!("expected Unsupported, got {other:?}"),
    }
    assert_eq!(cluster.open_pit_count(), 0, "refused open leaks nothing");

    // A search_after boundary can never reach a remote shard (the wire cannot
    // carry it) — refused before any fan.
    let program = cluster
        .compile_rank_program(&RankProgramSpec::default())
        .expect("program");
    let err = cluster
        .try_percolate_filtered_top_k(
            "michael jordan",
            &[],
            TopKOptions {
                search_after: Some((0, 0)),
                ..TopKOptions::default()
            },
            &program,
            None,
        )
        .expect_err("boundary without pit on a remote assembly");
    assert!(matches!(
        err,
        ClusterRankedError::Shard(ShardError::PitUnsupported(_))
    ));

    // Normal current-view reads are untouched by the refusals.
    let live = cluster
        .percolate("michael jordan psa 10")
        .expect("current view");
    assert!(live.contains(&1) && live.contains(&2));
}
