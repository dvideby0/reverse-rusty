//! Per-shard replication + peer recovery over gRPC (ADR-035/036): the replicated cluster ≡
//! brute oracle, stopping a primary still serves correct reads via its replica (FAILOVER),
//! and a fresh node PEER-RECOVERS a position's sealed segments from a live peer over the wire
//! and then serves that position correctly inside a cluster.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardGroup, ShardServer};
use reverse_rusty::compile::extract;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::{QueryScope, RankProgramSpec, TopKOptions};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// Per-shard replication + peer recovery over gRPC (ADR-035/036). Stands up K positions × RF=2
/// **durable** shard servers, builds the cluster via `connect_replicated`, and proves three
/// things end-to-end across the wire:
///   1. the replicated gRPC cluster ≡ the independent brute oracle;
///   2. stopping a primary still serves correct reads via its replica (FAILOVER — and, since
///      ingest fanned out to the replica, this also proves the write fan-out reached it);
///   3. a fresh node PEER-RECOVERS a position's sealed segments from a live peer (FetchSegments
///      over the wire) and then serves that position correctly inside a cluster.
#[test]
fn grpc_replicated_failover_and_peer_recovery() {
    let (queries, titles) = build_corpus();

    // Independent ground truth (brute oracle), broad on.
    let brute = Brute::build(&queries);
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut oracle: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    for t in &titles {
        oracle.push(brute.matches(t, &mut blc, &mut bfeats));
    }

    // ONE authoritative frozen dict at the coordinator; the servers start dict-less.
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

    let k = 3usize;
    let rf = 2usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    // Per position: a primary + (rf-1) replica `pending_durable` servers, each with its own dir.
    let mut groups: Vec<ShardGroup> = Vec::with_capacity(k);
    let mut primary_handles = Vec::with_capacity(k);
    let mut all_addrs: Vec<Vec<SocketAddr>> = Vec::with_capacity(k);
    let mut dirs: Vec<PathBuf> = Vec::new();
    {
        let _enter = rt.enter();
        for p in 0..k {
            let mut pos_addrs: Vec<SocketAddr> = Vec::with_capacity(rf);
            let mut replica_eps: Vec<String> = Vec::new();
            let mut primary_jh = None;
            for c in 0..rf {
                let dir = server_dir(&format!("{p}_{c}"));
                let incoming =
                    TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
                let addr = incoming.local_addr().expect("local_addr");
                let server = ShardServer::pending_durable(
                    Arc::clone(&norm),
                    EngineConfig::default(),
                    dir.clone(),
                );
                let jh = rt.spawn(server.serve_with_incoming(incoming));
                if c == 0 {
                    primary_jh = Some(jh); // keep the primary handle so we can stop it (failover)
                }
                // Replica handles are dropped — dropping a JoinHandle does NOT stop the task.
                pos_addrs.push(addr);
                if c > 0 {
                    replica_eps.push(format!("http://{addr}"));
                }
                dirs.push(dir);
            }
            groups.push(ShardGroup {
                primary: format!("http://{}", pos_addrs[0]),
                replicas: replica_eps,
            });
            primary_handles.push(primary_jh.expect("primary spawned"));
            all_addrs.push(pos_addrs);
        }
    }
    for addrs in &all_addrs {
        for &a in addrs {
            wait_until_listening(a);
        }
    }

    // Assemble the replicated gRPC cluster and load the corpus over the wire (the coordinator
    // ships the dict to every endpoint; ingest fans each bucket to the primary AND its replica).
    let cluster = ClusterEngine::connect_replicated(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &groups,
        rt.handle(),
    )
    .expect("connect replicated cluster");
    cluster
        .ingest(&queries)
        .expect("ingest over gRPC (fans to primary + replica)");
    let rank_program = cluster
        .compile_rank_program(&RankProgramSpec {
            priority_field: None,
            boosts: Vec::new(),
        })
        .expect("rank program");
    let rank_options = TopKOptions {
        search_after: None,
        size: 10,
        track_total_hits_up_to: 10_000,
        query_scope: QueryScope::WithBroad,
    };
    let ranked_before_failover = cluster
        .try_percolate_filtered_top_k(&titles[0], &[], rank_options, &rank_program, None)
        .expect("ranked read before failover");
    let sources_before_failover = cluster
        .fetch_ranked_sources(&ranked_before_failover, None)
        .expect("winner fetch before failover");

    let cc = cluster.class_counts().expect("class_counts over gRPC");
    assert!(
        cc[0] > 0 && cc[1] > 0 && cc[2] > 0,
        "every placement class must be exercised: {cc:?}"
    );

    // (1) The replicated gRPC cluster ≡ the brute oracle.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "replicated gRPC cluster vs brute on {title:?}"
        );
    }

    // (2) FAILOVER: stop position 0's primary. Every title probes position 0 (the replicated
    // broad lane), so every read must now fail over to position 0's replica and still match.
    primary_handles[0].abort();
    wait_until_not_listening(all_addrs[0][0]);
    let ranked_after_failover = cluster
        .try_percolate_filtered_top_k(&titles[0], &[], rank_options, &rank_program, None)
        .expect("bounded ranked read fails over to replica");
    assert_eq!(
        ranked_after_failover.hits, ranked_before_failover.hits,
        "RF>1 failover preserves global winners"
    );
    assert_eq!(
        ranked_after_failover.total_hits,
        ranked_before_failover.total_hits
    );
    assert_eq!(
        cluster
            .fetch_ranked_sources(&ranked_after_failover, None)
            .expect("winner fetch fails over to replica"),
        sources_before_failover
    );
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after primary stop")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "failover read vs brute on {title:?}");
    }

    // (3) PEER RECOVERY: a fresh durable node pulls position 1's sealed segments from its live
    // primary, then serves position 1 correctly inside a verify cluster.
    let fresh_dir = server_dir("fresh");
    let fresh_addr = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("local_addr");
        let server = ShardServer::pending_durable(
            Arc::clone(&norm),
            EngineConfig::default(),
            fresh_dir.clone(),
        );
        rt.spawn(server.serve_with_incoming(incoming));
        addr
    };
    dirs.push(fresh_dir);
    wait_until_listening(fresh_addr);

    let src_ep = format!("http://{}", all_addrs[1][0]); // position 1's live primary (durable)
    let tgt_ep = format!("http://{fresh_addr}");
    let (recovered_n, _hwm) = cluster
        // Position 1's primary hosts slot 1; recover it into the fresh node's slot 1 (ADR-093), so
        // the verify cluster below can address the recovered node as shard-id 1.
        .peer_recover_replica(1, &src_ep, &tgt_ep, rt.handle())
        .expect("peer recovery over gRPC");

    // Parity: the recovered node holds exactly position 1's query count.
    let pos1_count = cluster.shard_query_counts().expect("counts")[1];
    assert_eq!(
        recovered_n as usize, pos1_count,
        "recovered node query count {recovered_n} != source position's {pos1_count}"
    );

    // The recovered node serves position 1 correctly *inside a cluster*: a connect_remote (RF=1)
    // cluster with position 0 served by its still-live replica (its primary was stopped),
    // position 1 by the RECOVERED node, position 2 by its primary — must equal the brute oracle.
    let verify_eps = vec![
        format!("http://{}", all_addrs[0][1]), // pos 0 replica (primary was stopped)
        tgt_ep,                                // pos 1 recovered node
        format!("http://{}", all_addrs[2][0]), // pos 2 primary
    ];
    let verify = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &verify_eps,
        rt.handle(),
    )
    .expect("verify cluster over the recovered node");
    let verify_program = verify
        .compile_rank_program(&RankProgramSpec {
            priority_field: None,
            boosts: Vec::new(),
        })
        .expect("verify rank program");
    let recovered_ranked = verify
        .try_percolate_filtered_top_k(&titles[0], &[], rank_options, &verify_program, None)
        .expect("recovered node bounded read");
    assert_eq!(recovered_ranked.hits, ranked_before_failover.hits);
    verify
        .fetch_ranked_sources(&recovered_ranked, None)
        .expect("recovered node winner fetch");
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = verify
            .percolate(title)
            .expect("verify percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "recovered-node cluster vs brute on {title:?}"
        );
    }

    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
