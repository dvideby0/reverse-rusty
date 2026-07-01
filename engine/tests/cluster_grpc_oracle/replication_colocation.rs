//! RF>1 co-location + failover across MULTI-SHARD nodes (ADR-093 Stage 3, the piece deferred from
//! Stage 2). Classic cross-replication: two nodes, each hosting TWO co-located slots — one position's
//! primary and another position's replica. This exercises the `connect_replicated` co-location dedup
//! (each node adopts the dict once and gains its second slot via `AddShard`), then proves that killing
//! a whole node still serves every position with zero false negatives (read failover to the surviving
//! node's copy), and that a position peer-recovers onto a fresh node from the survivor.
//!
//!   node A = { pos0 PRIMARY, pos1 replica }      node B = { pos1 PRIMARY, pos0 replica }
//!
//! Kill A ⇒ pos0 fails over to B's replica, pos1 stays on B's primary — both still correct.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardGroup, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// A durable server + the handle to its serve task (kept so we can abort it to simulate a node loss)
/// + its data dir.
struct Server {
    addr: SocketAddr,
    ep: String,
    jh: JoinHandle<Result<(), tonic::transport::Error>>,
    dir: PathBuf,
}

fn spin_durable(rt: &tokio::runtime::Runtime, norm: &Arc<Normalizer>, tag: &str) -> Server {
    let dir = server_dir(tag);
    let (addr, jh) = {
        let _enter = rt.enter();
        let inc = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
        let addr = inc.local_addr().expect("local_addr");
        let srv =
            ShardServer::pending_durable(Arc::clone(norm), EngineConfig::default(), dir.clone());
        let jh = rt.spawn(srv.serve_with_incoming(inc));
        (addr, jh)
    };
    wait_until_listening(addr);
    Server {
        addr,
        ep: format!("http://{addr}"),
        jh,
        dir,
    }
}

#[test]
fn grpc_rf2_colocation_failover_across_multishard_nodes() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let k = 2usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // Two nodes, cross-replicated: A hosts pos0-primary + pos1-replica; B hosts pos1-primary +
    // pos0-replica. Each node therefore ends up with TWO co-located slots {0,1}.
    let a = spin_durable(&rt, &norm, "rf2_colo_a");
    let b = spin_durable(&rt, &norm, "rf2_colo_b");
    let groups = vec![
        ShardGroup {
            primary: a.ep.clone(),
            replicas: vec![b.ep.clone()],
        },
        ShardGroup {
            primary: b.ep.clone(),
            replicas: vec![a.ep.clone()],
        },
    ];

    // `connect_replicated` exercises the Stage-3 dedup: A adopts once (pos0 primary) then AddShards
    // slot 1 (pos1 replica); B likewise. A broken dedup (or a node that could not host two slots)
    // would fail HERE.
    let cluster = ClusterEngine::connect_replicated(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &groups,
        rt.handle(),
    )
    .expect("connect cross-replicated co-located cluster");
    cluster
        .ingest(&queries)
        .expect("ingest over gRPC (fans each position to its primary + co-located replica)");

    // Both positions independently populated (2 slots per node, K positions).
    let counts = cluster.shard_query_counts().expect("per-shard counts");
    assert_eq!(counts.len(), k, "one count per position: {counts:?}");
    assert!(
        counts.iter().all(|&c| c > 0),
        "every co-located slot must hold queries: {counts:?}"
    );

    // (1) Parity: the cross-replicated co-located cluster ≡ the independent brute oracle.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "cross-replicated co-located cluster vs brute on {title:?}"
        );
    }

    // (2) FAILOVER: kill node A ENTIRELY (both its co-located slots go down at once). Position 0's
    // primary was on A ⇒ reads fail over to B's replica; position 1's primary is on B ⇒ still served.
    // Every read must still equal the oracle — zero false negatives despite a whole node lost.
    a.jh.abort();
    wait_until_not_listening(a.addr);
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after node A down")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "failover across a downed multi-shard node vs brute on {title:?}"
        );
    }

    // (3) PEER RECOVERY across multi-shard nodes: rebuild position 0 onto a fresh node from B's
    // surviving replica (the only live copy of position 0 now A is gone), then verify a cluster over
    // the recovered node still ≡ brute.
    let fresh = spin_durable(&rt, &norm, "rf2_colo_fresh");
    let (recovered_n, _hwm) = cluster
        .peer_recover_replica(0, &b.ep, &fresh.ep, rt.handle())
        .expect("peer-recover position 0 from B's replica onto the fresh node");
    assert!(
        recovered_n > 0,
        "position 0 recovered a non-empty segment set"
    );

    // Verify cluster: position 0 on the recovered fresh node, position 1 on B's (still-primary) slot.
    let verify = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &[fresh.ep.clone(), b.ep.clone()],
        rt.handle(),
    )
    .expect("verify cluster over the recovered node");
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

    for dir in [&a.dir, &b.dir, &fresh.dir] {
        let _ = std::fs::remove_dir_all(dir);
    }
}
