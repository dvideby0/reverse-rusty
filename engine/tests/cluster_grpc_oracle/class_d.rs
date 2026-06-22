//! Class-D (negation-only / always-candidate) queries over the gRPC transport (ADR-080).
//!
//! The load-bearing contract this locks: a remote `ShardServer` is COORDINATOR-GATED storage.
//! `LocalShard` forces `accept_class_d = true` on every shard it builds (in-memory, durable, and
//! adopt/recovery paths alike), so the operator's ONLY class-D knob is the COORDINATOR's
//! `per_shard.accept_class_d`; a shard never re-gates what the coordinator placed. Here the servers
//! are built with `EngineConfig::default()` (`accept_class_d = false`), yet a class-D query the
//! coordinator accepts is stored on every shard and served over the wire — proving the shard's own
//! config cannot drop it. (This is why `shardserver` needs no `--accept-class-d` flag: such a flag
//! would be silently overridden by `LocalShard`, dead/misleading config.)

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::config::EngineConfig;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// Logical id for the negation-only query, far above any regular id so it is recognizable.
const CLASS_D_ID: u64 = 1_000_000;

#[test]
fn grpc_class_d_is_coordinator_gated_despite_default_shard_config() {
    // A tiny corpus: class-A (anchored), class-C (broad), and ONE negation-only class-D query.
    let queries: Vec<(u64, String)> = vec![
        (1, "1990 topps chrome".to_string()),
        (2, "1986 fleer".to_string()),
        (3, "psa 10".to_string()),
        (CLASS_D_ID, "-reprint".to_string()),
    ];
    // Two probe titles: one free of the forbidden term (class-D MUST match — an always-candidate)
    // and one that contains it (class-D MUST be excluded — the forbidden feature is enforced in
    // exact verification, over the wire).
    let title_match = "1990 topps chrome psa 10";
    let title_reject = "1990 topps chrome reprint";

    // ONE authoritative frozen feature space, shared into every server AND the coordinator. Built
    // over the queries (incl. `-reprint`), so "reprint" has a real feature id on both sides.
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let k = 3usize;
    // The coordinator is the SOLE class-D gate (ADR-080).
    let mut cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    cfg.per_shard.accept_class_d = true;

    // Stand up K real gRPC shard servers built with the DEFAULT engine config
    // (accept_class_d = false) — the crux of the test: `LocalShard` forces acceptance, so the
    // shard's own knob is irrelevant. Each binds its ephemeral port once (no rebind window).
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
                EngineConfig::default(), // accept_class_d = false — deliberately
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
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // The class-D query is stored on EVERY shard (replicate-to-all, ADR-080), despite the servers
    // being built with accept_class_d = false — the coordinator's gate, not the shard's, decides.
    let cc = cluster.class_counts().expect("class_counts over gRPC");
    assert_eq!(
        cc[3], k as u64,
        "class-D must be stored (replicated to all {k} shards) despite default shard config: {cc:?}"
    );

    // Served over the wire: matches a title free of the forbidden term...
    let got_match: HashSet<u64> = cluster
        .percolate(title_match)
        .expect("percolate match title")
        .into_iter()
        .collect();
    assert!(
        got_match.contains(&CLASS_D_ID),
        "class-D query must match a title free of its forbidden term, over gRPC"
    );
    // ...without breaking the normal path (the regular queries still match).
    assert!(
        got_match.contains(&1) && got_match.contains(&3),
        "regular class-A/class-C queries must still match: {got_match:?}"
    );

    // ...and is EXCLUDED when the title carries the forbidden term (verification works over gRPC).
    let got_reject: HashSet<u64> = cluster
        .percolate(title_reject)
        .expect("percolate reject title")
        .into_iter()
        .collect();
    assert!(
        !got_reject.contains(&CLASS_D_ID),
        "class-D query must be excluded when the title contains its forbidden term"
    );

    // Broad off quarantines the always-candidate (it rides the broad lane), over gRPC too.
    let got_sel: HashSet<u64> = cluster
        .percolate_with_broad(title_match, false)
        .expect("percolate broad off")
        .into_iter()
        .collect();
    assert!(
        !got_sel.contains(&CLASS_D_ID),
        "class-D must be quarantined with the broad lane off"
    );
}
