//! ADR-101 per-shard broad-lane cost counters over REAL gRPC: the shard-side
//! `reverse_rusty_broad_*_total{shard}` counters move only when broad work actually happens, and
//! on the happy path they equal the client-summed per-call `MatchStats` exactly (the two-sided
//! consistency check, ADR-100 precedent). K = 1 so every percolate is exactly one RPC against the
//! one slot — the rendered totals and the client sums count the same calls.

use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::config::EngineConfig;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

#[test]
fn grpc_broad_cost_counters_move_only_on_broad_work_and_match_client_stats() {
    const N: usize = 60;
    let (queries, titles) = build_corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    // Cluster default broad ON — phase B percolates ride the default; phase A overrides it off.
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };

    let server_rt = tokio::runtime::Runtime::new().expect("server runtime");
    let client_rt = tokio::runtime::Runtime::new().expect("client runtime");

    let (addr, metrics_source) = {
        let _enter = server_rt.enter();
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
        let addr: SocketAddr = incoming.local_addr().expect("local_addr");
        let server = ShardServer::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        // Captured BEFORE `serve_with_incoming` consumes the server — the server-side counter
        // view of the same workload the client stats record.
        let metrics_source = server.metrics_source();
        server_rt.spawn(server.serve_with_incoming(incoming));
        (addr, metrics_source)
    };
    wait_until_listening(addr);
    let endpoints = vec![format!("http://{addr}")];

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        client_rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // ---- phase A: broad OFF ⇒ every broad counter stays exactly 0 -----------------------------
    let mut any_match = false;
    for title in titles.iter().take(N) {
        let ids = cluster
            .percolate_with_broad(title, false)
            .expect("percolate (broad off) over gRPC");
        any_match |= !ids.is_empty();
    }
    assert!(any_match, "degenerate corpus: no matches with broad off");
    let body = metrics_source.render();
    for family in [
        "reverse_rusty_broad_candidates_total",
        "reverse_rusty_broad_postings_scanned_total",
        "reverse_rusty_broad_queries_evaluated_total",
        "reverse_rusty_broad_batches_total",
    ] {
        assert!(
            body.contains(&format!("{family}{{shard=\"0\"}} 0")),
            "{family} must be 0 after a broad-off workload; got:\n{body}"
        );
    }

    // ---- phase B: broad ON ⇒ the counters equal the client-summed MatchStats exactly ----------
    // The cluster default is broad on, so `percolate_with_stats` exercises the plain branch; a
    // ranked leg (trivial spec — scores 0, reorders nothing) exercises the `req.rank` branch, so
    // BOTH recording points in the percolate handler are covered.
    let mut client_broad_candidates = 0u64;
    let mut client_broad_postings = 0u64;
    for title in titles.iter().take(N) {
        let (_ids, stats) = cluster
            .percolate_with_stats(title)
            .expect("percolate (broad on) over gRPC");
        client_broad_candidates += u64::from(stats.broad_candidates);
        client_broad_postings += u64::from(stats.broad_postings_scanned);
    }
    let empty_spec = reverse_rusty::RankSpec {
        priority_key: None,
        boosts: Vec::new(),
    };
    for title in titles.iter().take(8) {
        let (_scored, stats) = cluster
            .percolate_filtered_ranked(title, &[], true, &empty_spec)
            .expect("ranked percolate over gRPC");
        client_broad_candidates += u64::from(stats.broad_candidates);
        client_broad_postings += u64::from(stats.broad_postings_scanned);
    }
    assert!(
        client_broad_candidates > 0,
        "premise: the broad-on workload must retrieve broad candidates (broad_query_frac 0.06)"
    );

    // Two-sided precondition (ADR-100 precedent): a happy-path localhost workload has no
    // errors/timeouts/retries, so every client-counted call recorded server-side exactly once.
    let snap = cluster.transport_metrics();
    assert_eq!(snap.total_timeouts(), 0, "happy path has no timeouts");
    for m in &snap.methods {
        assert_eq!(m.errors, 0, "happy path has no {} errors", m.method);
        assert_eq!(m.retries, 0, "happy path has no {} retries", m.method);
    }

    let body = metrics_source.render();
    assert!(
        body.contains(&format!(
            "reverse_rusty_broad_candidates_total{{shard=\"0\"}} {client_broad_candidates}"
        )),
        "server-side broad candidates must equal the client sum ({client_broad_candidates}); got:\n{body}"
    );
    assert!(
        body.contains(&format!(
            "reverse_rusty_broad_postings_scanned_total{{shard=\"0\"}} {client_broad_postings}"
        )),
        "server-side broad postings must equal the client sum ({client_broad_postings}); got:\n{body}"
    );
    // The columnar-only families stay 0 on the per-title Percolate wire (rendered for name
    // symmetry — the ADR-101 caveat, asserted so a future batch RPC updates this test knowingly).
    assert!(body.contains("reverse_rusty_broad_queries_evaluated_total{shard=\"0\"} 0"));
    assert!(body.contains("reverse_rusty_broad_batches_total{shard=\"0\"} 0"));
}
