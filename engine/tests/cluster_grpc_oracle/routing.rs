//! ADR-086 load-bearing redirect: prove the committed shard→node assignments DRIVE routing (the
//! feature is not inert). A coordinator that resolves its topology from the committed document routes
//! to the assigned endpoint; re-committing the assignment to a DIFFERENT server makes a fresh
//! coordinator route there.
//!
//! The redirect is proven WITHOUT killing a server (aborting a tonic serve task stops its accept loop
//! but not already-established connections): a sentinel query added through the first coordinator
//! lands only on server A, so the first coordinator sees it and the reassigned second coordinator —
//! reading from B per the committed map — does NOT. Both still match the brute oracle, so the
//! reassignment redirected routing without changing the answer. Data placement is arranged out-of-band
//! per the Cut-A scope (both servers ingest the corpus); data-moving reassignment via live handoff is
//! the deferred follow-on.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{
    resolve_topology, seed_position_preserving, ClusterConfig, ClusterEngine, InMemoryControlPlane,
    ShardEndpoints, ShardError, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// Resolve the (K=1) topology from the committed document and connect a coordinator to the resolved
/// endpoint — exactly what the coordinator-mode binary's `remote_connect` does (resolve → connect).
fn connect_resolved(
    control: &InMemoryControlPlane,
    norm: &Arc<Normalizer>,
    dict: &Arc<Dict>,
    cfg: &ClusterConfig,
    rt: &tokio::runtime::Runtime,
) -> Result<ClusterEngine, ShardError> {
    let resolved = resolve_topology(control, cfg.num_shards as u32)?;
    let endpoints: Vec<String> = resolved.into_iter().map(|(p, _)| p).collect();
    ClusterEngine::connect_remote(
        Arc::clone(norm),
        Arc::clone(dict),
        empty_tag_dict(),
        cfg,
        &endpoints,
        rt.handle(),
    )
}

#[test]
fn committed_assignment_drives_routing_to_the_assigned_endpoint() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);
    assert!(
        oracle.iter().map(HashSet::len).sum::<usize>() > 0,
        "degenerate corpus: no matches at all"
    );

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };

    // Two real shard servers A and B over the SAME frozen dict/norm (so either can serve position 0).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(2);
    {
        let _enter = rt.enter();
        for _ in 0..2 {
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
    for &a in &addrs {
        wait_until_listening(a);
    }
    let ep_a = format!("http://{}", addrs[0]);
    let ep_b = format!("http://{}", addrs[1]);

    // Seed the committed document: position 0 → A. A coordinator resolving from it routes to A and
    // matches the oracle over the wire.
    let control = InMemoryControlPlane::single_node(1, 128, dict.fingerprint());
    seed_position_preserving(&control, &[(ep_a.clone(), Vec::new())]).expect("seed → A");
    let coord_a = connect_resolved(&control, &norm, &dict, &cfg, &rt).expect("coordinator on A");
    coord_a.ingest(&queries).expect("ingest into A");
    for (i, t) in titles.iter().enumerate() {
        let got: HashSet<u64> = coord_a
            .percolate(t)
            .expect("percolate A")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "coord-A (routed to A) vs oracle on {t:?}");
    }

    // Re-commit the assignment: position 0 → B. A FRESH coordinator resolving from the document now
    // routes to B (the committed map, not the old endpoint, is authoritative) and matches identically.
    seed_position_preserving(&control, &[(ep_b.clone(), Vec::new())]).expect("reassign → B");
    assert_eq!(
        resolve_topology(&control, 1).unwrap(),
        vec![(ep_b.clone(), Vec::new()) as ShardEndpoints],
        "the committed document now points position 0 at B"
    );
    let coord_b = connect_resolved(&control, &norm, &dict, &cfg, &rt).expect("coordinator on B");
    coord_b.ingest(&queries).expect("ingest into B");
    for (i, t) in titles.iter().enumerate() {
        let got: HashSet<u64> = coord_b
            .percolate(t)
            .expect("percolate B")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "coord-B (routed to B) vs oracle on {t:?}");
    }

    // Load-bearing proof: a sentinel query added through coord-A lands ONLY on server A. coord-A
    // (routed to A) sees it; coord-B (routed to B by the committed reassignment) does NOT — so the
    // two coordinators are demonstrably reading from DIFFERENT servers, driven solely by the committed
    // shard→node map. Without routing-by-assignments the resolution path would be inert and both would
    // hit the same endpoint.
    let sentinel = 9_000_001u64;
    let sentinel_title = "1994 upper deck rareplayer0 psa 10";
    coord_a
        .add_query(sentinel, "1994 upper deck rareplayer0")
        .expect("add sentinel query to A");
    assert!(
        coord_a
            .percolate(sentinel_title)
            .expect("percolate A sentinel")
            .contains(&sentinel),
        "coord-A (on A) must see a query added to A"
    );
    assert!(
        !coord_b
            .percolate(sentinel_title)
            .expect("percolate B sentinel")
            .contains(&sentinel),
        "coord-B (on B, per the committed reassignment) must NOT see a query that exists only on A"
    );
}
