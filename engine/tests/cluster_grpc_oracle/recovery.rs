//! No-quiesce peer recovery over gRPC (ADR-039/040): writes that land AFTER the snapshot
//! position `P` are replayed from the per-shard TRANSLOG TAIL (`FetchTranslog`) rather than
//! lost — so peer recovery need NOT pause writes. Covered both as an ordered snapshot →
//! write → catch-up sequence and under SUSTAINED, concurrent writes (a retention lease).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::config::EngineConfig;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// No-quiesce peer recovery (ADR-039, clustering step 5c) — the headline. A durable SOURCE node
/// is recovered onto a fresh TARGET by streaming its sealed segments at snapshot position `P`,
/// and the writes that land AFTER `P` are replayed from the per-shard TRANSLOG TAIL
/// (`FetchTranslog`) rather than lost — so peer recovery need NOT quiesce writes (closing
/// ADR-036's documented gap). Deterministic by ordering (snapshot → write → tail catch-up),
/// which exercises the exact path a concurrent recovery uses for writes during the copy window;
/// the pre-catch-up staleness assertion proves the writes truly post-date the snapshot. The
/// recovered node converges to BOTH the live source AND an independent brute oracle over the
/// final live set — zero false negatives across the wire.
#[test]
fn grpc_peer_recovery_without_quiescing() {
    let (queries, titles) = build_corpus();
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    // The snapshot ground truth, and the set of query ids that actually match ≥1 title (so the
    // post-snapshot mutations provably move title results — many corpus queries match nothing).
    let oracle_corpus = build_oracle(&queries, &titles);
    let matched: Vec<u64> = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        let mut v: Vec<u64> = s.into_iter().collect();
        v.sort_unstable();
        v
    };
    assert!(
        matched.len() >= 20,
        "corpus must match ≥20 distinct queries to mutate; got {}",
        matched.len()
    );

    // Mutations applied AFTER the snapshot: REMOVE 10 title-matching queries (their ids vanish
    // from results) and ADD 10 copies of OTHER title-matching queries (new ids appear in results).
    let removed_ids: Vec<u64> = matched.iter().take(10).copied().collect();
    let additions: Vec<(u64, String)> = matched
        .iter()
        .skip(10)
        .take(10)
        .map(|id| {
            let nid = next_id;
            next_id += 1;
            (nid, by_id[id].clone())
        })
        .collect();
    let removed_set: HashSet<u64> = removed_ids.iter().copied().collect();
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .filter(|(id, _)| !removed_set.contains(id))
        .cloned()
        .chain(additions.iter().cloned())
        .collect();

    // The final ground truth MUST differ from the snapshot, else the tail never mattered.
    let oracle_final = build_oracle(&final_live, &titles);
    assert!(
        oracle_corpus != oracle_final,
        "test setup: the post-snapshot mutations must change some title results"
    );

    // ONE authoritative frozen dict (over the corpus; the additions reuse corpus DSLs, so their
    // vocab is already present).
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // A single durable SOURCE node + a fresh durable TARGET node (both pending → adopt the dict).
    let src_dir = server_dir("nq_src");
    let tgt_dir = server_dir("nq_tgt");
    let (src_addr, tgt_addr) = {
        let _enter = rt.enter();
        let si = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind src");
        let sa = si.local_addr().expect("src addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                src_dir.clone(),
            )
            .serve_with_incoming(si),
        );
        let ti = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tgt");
        let ta = ti.local_addr().expect("tgt addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                tgt_dir.clone(),
            )
            .serve_with_incoming(ti),
        );
        (sa, ta)
    };
    wait_until_listening(src_addr);
    wait_until_listening(tgt_addr);
    let src_ep = format!("http://{src_addr}");
    let tgt_ep = format!("http://{tgt_addr}");

    // Coordinator over the source; load the corpus (→ source segments; the translog stays empty,
    // since bulk ingest writes a base segment directly).
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");

    // (1) SNAPSHOT: recover the fresh target from the source. The bulk copy is at position P; the
    // initial tail is empty (no post-snapshot writes yet), so hwm == P.
    let (_n, hwm) = cluster
        .peer_recover_replica(0, &src_ep, &tgt_ep, rt.handle())
        .expect("peer recovery");

    // A verify cluster over the recovered target alone, to read its state directly.
    let verify = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&tgt_ep),
        rt.handle(),
    )
    .expect("connect verify cluster");

    // Pre-catch-up the target reflects the SNAPSHOT (the corpus) — proving the subsequent writes
    // truly post-date it (else this would equal the final state and the test would be trivial).
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = verify
            .percolate(title)
            .expect("verify pre-catch-up")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_corpus[i],
            "target must equal the snapshot pre-catch-up on {title:?}"
        );
    }

    // (2) WRITES land on the source AFTER the snapshot (into its translog tail, > P).
    for id in &removed_ids {
        cluster.remove_query(*id).expect("remove on source");
    }
    for (id, dsl) in &additions {
        cluster.add_query(*id, dsl).expect("add on source");
    }

    // (3) TAIL CATCH-UP: replay the source's translog tail (> hwm) into the target — no segment
    // re-copy, no quiesce. Loop to a fixed point (writes are done, so it converges at once).
    let mut cursor = hwm;
    loop {
        let next = cluster
            .catch_up_recovered_replica(0, &src_ep, &tgt_ep, cursor, rt.handle())
            .expect("catch up tail");
        if next == cursor {
            break;
        }
        cursor = next;
    }

    // (4) The recovered target now equals the live source AND the independent brute oracle over
    // the FINAL live set, on every title — zero false negatives after a no-quiesce recovery.
    for (i, title) in titles.iter().enumerate() {
        let tgt: HashSet<u64> = verify
            .percolate(title)
            .expect("verify post-catch-up")
            .into_iter()
            .collect();
        let src: HashSet<u64> = cluster
            .percolate(title)
            .expect("source percolate")
            .into_iter()
            .collect();
        assert_eq!(
            tgt, oracle_final[i],
            "recovered target vs brute(final) on {title:?}"
        );
        assert_eq!(
            src, oracle_final[i],
            "live source vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&tgt_dir);
}

/// ADR-040 (retention + finalize): peer recovery under SUSTAINED, concurrent writes. Unlike
/// `grpc_peer_recovery_without_quiescing` (ordered snapshot → write → catch-up), here a writer
/// thread streams adds onto the source CONCURRENTLY with the recovery. The recovery holds a
/// translog RETENTION LEASE across its segment copy + convergence loop, so the racing writes are
/// neither trimmed by the copy's seal nor lost; its bounded loop drains what it can while writes
/// continue. After the writer finishes (no further seals), a final lease-free catch-up — now
/// race-free — converges the target to BOTH the live source AND the brute oracle over the final
/// live set: zero false negatives across the wire with writes never paused.
#[test]
fn grpc_peer_recovery_converges_under_sustained_writes() {
    let (queries, titles) = build_corpus();
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    let oracle_corpus = build_oracle(&queries, &titles);
    let matched: Vec<u64> = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        let mut v: Vec<u64> = s.into_iter().collect();
        v.sort_unstable();
        v
    };
    assert!(
        matched.len() >= 20,
        "need ≥20 matching queries; got {}",
        matched.len()
    );

    // The writer's known add sequence: 20 copies of matching DSLs with fresh ids → a deterministic
    // final live set. A pure stream of adds (no removes) keeps it a clean firehose racing the copy.
    let additions: Vec<(u64, String)> = matched
        .iter()
        .take(20)
        .map(|id| {
            let nid = next_id;
            next_id += 1;
            (nid, by_id[id].clone())
        })
        .collect();
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain(additions.iter().cloned())
        .collect();
    let oracle_final = build_oracle(&final_live, &titles);
    assert!(
        oracle_corpus != oracle_final,
        "test setup: the concurrent adds must change some title results"
    );

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let src_dir = server_dir("sw_src");
    let tgt_dir = server_dir("sw_tgt");
    let (src_addr, tgt_addr) = {
        let _enter = rt.enter();
        let si = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind src");
        let sa = si.local_addr().expect("src addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                src_dir.clone(),
            )
            .serve_with_incoming(si),
        );
        let ti = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tgt");
        let ta = ti.local_addr().expect("tgt addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                tgt_dir.clone(),
            )
            .serve_with_incoming(ti),
        );
        (sa, ta)
    };
    wait_until_listening(src_addr);
    wait_until_listening(tgt_addr);
    let src_ep = format!("http://{src_addr}");
    let tgt_ep = format!("http://{tgt_addr}");

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");

    // Recover the target CONCURRENTLY with a writer streaming the additions onto the source.
    let hwm = std::thread::scope(|s| {
        let cluster_ref = &cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                cluster_ref.add_query(*id, dsl).expect("writer add");
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        // The recovery runs while the writer is mid-stream — its retention lease keeps the racing
        // writes safe and its convergence loop drains what it can.
        let (_n, hwm) = cluster
            .peer_recover_replica(0, &src_ep, &tgt_ep, rt.handle())
            .expect("peer recovery under writes");
        writer.join().expect("writer thread");
        hwm
    });

    // Writer done + no further seals ⇒ a final lease-free catch-up is race-free; drain to a fixed
    // point (covers any residual the recovery's bounded loop did not reach while writes raced).
    let mut cursor = hwm;
    loop {
        let next = cluster
            .catch_up_recovered_replica(0, &src_ep, &tgt_ep, cursor, rt.handle())
            .expect("final catch up");
        if next == cursor {
            break;
        }
        cursor = next;
    }

    let verify = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&tgt_ep),
        rt.handle(),
    )
    .expect("connect verify cluster");

    for (i, title) in titles.iter().enumerate() {
        let tgt: HashSet<u64> = verify
            .percolate(title)
            .expect("verify")
            .into_iter()
            .collect();
        let src: HashSet<u64> = cluster
            .percolate(title)
            .expect("source")
            .into_iter()
            .collect();
        assert_eq!(
            tgt, oracle_final[i],
            "recovered target vs brute(final) on {title:?}"
        );
        assert_eq!(
            src, oracle_final[i],
            "live source vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&tgt_dir);
}
