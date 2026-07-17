//! Live handoff over gRPC (ADR-044/048): MOVE a shard between owners under concurrent writes
//! with no match dropped (`execute_handoff`); an aborted handoff must AUTO-UNFENCE the source
//! so it resumes serving (ADR-048); and the autoscaler's advisory `Handoff` driven by `tick`
//! must reach the resolution path and skip fail-safe without ever breaking matching.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    AutoscaleConfig, ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole, ScalingAction,
    ShardError, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::events::{DurabilityOp, EngineEvent};
use reverse_rusty::{QueryScope, RankProgramSpec, TopKOptions};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// ADR-044 (live handoff): MOVE a shard from one owner to another while a writer streams adds —
/// no match dropped, reads never paused. Where `grpc_peer_recovery_converges_under_sustained_writes`
/// recovers a *replica* and verifies it out-of-band, this drives the full handoff through
/// `execute_handoff` (peer-recover → FENCE the source → drain to convergence → FLIP routing), then
/// asserts the SAME cluster — its position now re-pointed at the new owner — matches the brute
/// oracle over the final live set. The writer's adds (drained from the source, or landing on the
/// target after the flip, or briefly rejected mid-fence and retried) all converge onto the new owner.
#[test]
fn grpc_live_handoff_under_sustained_writes() {
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

    // 20 adds of matching DSLs with fresh ids → a deterministic final live set (a clean firehose).
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

    let src_dir = server_dir("ho_src");
    let tgt_dir = server_dir("ho_tgt");
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
    let rank_program = cluster
        .compile_rank_program(&RankProgramSpec {
            priority_field: None,
            boosts: Vec::new(),
        })
        .expect("rank program");
    let rank_options = TopKOptions {
        size: 10,
        track_total_hits_up_to: 10_000,
        query_scope: QueryScope::WithBroad,
    };
    let ranked_before = cluster
        .try_percolate_filtered_top_k(&titles[0], &[], rank_options, &rank_program, None)
        .expect("top k before handoff");
    cluster
        .fetch_ranked_sources(&ranked_before, None)
        .expect("winner fetch before handoff");
    assert_eq!(
        cluster.handoff_generations(),
        vec![0],
        "position 0 starts at generation 0 (the source owner)"
    );

    // Run the handoff CONCURRENTLY with a writer streaming the additions through the cluster. The
    // add routes to position 0's CURRENT backing — the source pre-flip, the target post-flip — and is
    // briefly REJECTED in the fence→flip window (the source is fenced, routing not yet flipped). The
    // write is durably logged BEFORE apply, so no add is lost regardless of HOW the fence surfaces.
    std::thread::scope(|s| {
        let cluster_ref = &cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                loop {
                    match cluster_ref.add_query(*id, dsl) {
                        // This id is durably accounted for, so stop retrying it (re-`add_query`
                        // would double-log):
                        //  - Ok: it landed on the position's current owner.
                        //  - PartiallyApplied: the brief fence→flip window — the fenced source
                        //    rejected it with `failed_precondition`, which the broad lane's fan-out
                        //    funnel reports as PartiallyApplied (ADR-080 routes it through the same
                        //    fail-collect path as the selective lane). The add is ALREADY durably
                        //    logged and queued for repair (ADR-047); the post-handoff `resync`
                        //    re-drives it onto the new owner.
                        Ok(_) | Err(ShardError::PartiallyApplied { .. }) => break,
                        // A clean pre-apply failure (nothing logged or applied) — safe to retry until
                        // the flip lands it on the new owner.
                        Err(ShardError::Remote(_)) => std::thread::sleep(Duration::from_millis(2)),
                        Err(e) => panic!("unexpected writer error: {e}"),
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        cluster
            .execute_handoff(0, &src_ep, &tgt_ep, rt.handle())
            .expect("execute handoff");
        writer.join().expect("writer thread");
    });

    // The flip happened: position 0's generation bumped, and the cluster now routes to the new owner.
    assert_eq!(
        cluster.handoff_generations(),
        vec![1],
        "the handoff bumped position 0's generation"
    );

    // Converge any fence-window write that was durably logged but queued for repair (ADR-047). Its
    // failed position is 0, whose backing the flip swapped to the (healthy) new owner, so `resync`
    // re-drives the add there in a single pass. This is exactly what an operator, the autoscaler
    // `tick`, or a reopen-replay would do. A no-op when the fence window stayed empty (it is a race —
    // some runs land every add cleanly), so the loop converges immediately in that case too.
    for _ in 0..50 {
        if cluster.pending_repairs() == 0 {
            break;
        }
        let _ = cluster.resync();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        cluster.pending_repairs(),
        0,
        "every fence-window write must converge via resync after the flip"
    );

    let ranked_after = cluster
        .try_percolate_filtered_top_k(&titles[0], &[], rank_options, &rank_program, None)
        .expect("top k after handoff");
    assert_eq!(
        ranked_after.hits, ranked_before.hits,
        "zero-score top winners remain deterministic across the backing swap"
    );
    cluster
        .fetch_ranked_sources(&ranked_after, None)
        .expect("winner fetch after handoff");

    // Over EVERY title the cluster (now serving from the new owner) matches the brute oracle over the
    // final live set — zero false negatives across a live data move under concurrent writes.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after handoff")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_final[i],
            "post-handoff cluster vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&tgt_dir);
}

/// ADR-048 (auto-unfence-on-abort): a handoff that aborts AFTER fencing must LIFT the source's
/// fence so it resumes serving writes — otherwise the source is left permanently write-quiesced
/// and needs a manual restart (the gap ADR-044 left). We force the abort deterministically with
/// `handoff_final_drain_cap = 0`: the post-fence drain runs zero passes, so it never "converges"
/// and `execute_handoff` aborts fail-closed (after fencing). The source must then accept a write
/// again, and the cluster — routing unchanged (no flip) — must still match the brute oracle over
/// the live set: zero false negatives despite the failed move.
#[test]
fn grpc_handoff_abort_unfences_source() {
    let (queries, titles) = build_corpus();
    let next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    // Pick one matching query to re-add under a fresh id AFTER the abort — the write that proves
    // the source unfenced (a still-fenced source would reject it with failed_precondition).
    let oracle_corpus = build_oracle(&queries, &titles);
    let a_match = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        *s.iter()
            .min()
            .expect("need ≥1 matching query in the corpus")
    };
    let addition = (next_id, by_id[&a_match].clone());
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain(std::iter::once(addition.clone()))
        .collect();
    let oracle_final = build_oracle(&final_live, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    // Force the post-fence drain to abort immediately: 0 passes ⇒ never converges ⇒ fail-closed.
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        handoff_final_drain_cap: 0,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let src_dir = server_dir("ho_abort_src");
    let tgt_dir = server_dir("ho_abort_tgt");
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

    // The handoff fences the source, then the 0-pass post-fence drain forces a fail-closed abort.
    let err = cluster
        .execute_handoff(0, &src_ep, &tgt_ep, rt.handle())
        .expect_err("handoff must abort with final_drain_cap = 0");
    assert!(
        matches!(err, ShardError::Remote(_)),
        "the abort surfaces as a remote error, got {err:?}"
    );
    // No flip happened: routing is unchanged, position 0 still at generation 0.
    assert_eq!(
        cluster.handoff_generations(),
        vec![0],
        "an aborted handoff must NOT flip routing"
    );

    // The crux (ADR-048): the source AUTO-UNFENCED, so a write lands again. A still-fenced source
    // would reject it with failed_precondition → ShardError::Remote — so `Ok` here IS the proof.
    cluster
        .add_query(addition.0, &addition.1)
        .expect("source must accept writes after the aborted handoff unfenced it");

    // And the cluster still matches the brute oracle over the final live set — the failed move
    // dropped nothing (zero false negatives).
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after aborted handoff")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_final[i],
            "post-abort cluster vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&tgt_dir);
}

/// ADR-048 (autoscaler-driven handoff): the policy's advisory `Handoff` (ADR-045) is wired to
/// `execute_handoff` and DRIVEN by `tick`. Over a REAL gRPC cluster this exercises the driver's
/// resolution path end-to-end and proves it never breaks matching. The registered nodes carry no
/// endpoint (`addr = None`), so a recommended move can't be physically performed: the driver
/// surfaces a "missing endpoint" event and SKIPS fail-safe — routing is untouched (no flip) and
/// `percolate` stays equal to brute force (zero false negatives). (The happy-path move itself is
/// proven by `grpc_live_handoff_under_sustained_writes`, a direct `execute_handoff`; driving it
/// cleanly from `tick` additionally needs the control-plane node→endpoint map to match the shard
/// endpoints — deployment-model maturity that is Tier-3 residue, see ADR-048.)
#[test]
fn grpc_autoscaler_tick_drives_handoff_resolution_and_preserves_matching() {
    // Several shards so the corpus spreads across nodes; the exact count only needs to be enough
    // that a rebalance lands shards on ≥2 nodes (asserted below).
    const NS: usize = 6;

    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: NS,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // Spin up NS endpoint-less shard servers (the shards' data lives here; the control-plane node
    // addresses are deliberately `None`, see below).
    let dirs: Vec<PathBuf> = (0..NS).map(|i| server_dir(&format!("as_ho_{i}"))).collect();
    let eps: Vec<String> = {
        let _enter = rt.enter();
        let mut eps = Vec::with_capacity(NS);
        for dir in &dirs {
            let inc = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
            let addr = inc.local_addr().expect("addr");
            rt.spawn(
                ShardServer::pending_durable(
                    Arc::clone(&norm),
                    EngineConfig::default(),
                    dir.clone(),
                )
                .serve_with_incoming(inc),
            );
            eps.push(format!("http://{addr}"));
        }
        eps
    };
    for ep in &eps {
        wait_until_listening(ep.trim_start_matches("http://").parse().unwrap());
    }

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &eps,
        rt.handle(),
    )
    .expect("connect cluster");
    cluster.ingest(&queries).expect("ingest");

    // Capture the driver's "missing endpoint" skip event — proof the wiring was reached.
    let saw_skip = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&saw_skip);
    cluster.set_observer(Arc::new(move |ev: &EngineEvent| {
        if let EngineEvent::DurabilityFailure {
            op: DurabilityOp::ReplicaDesync,
            detail,
            ..
        } = ev
        {
            if detail.contains("autoscaler") {
                flag.store(true, std::sync::atomic::Ordering::Release);
            }
        }
    }));

    // Register endpoint-less data nodes and spread the shards across them.
    for id in 1..=3 {
        cluster
            .register_node(NodeDescriptor {
                id: NodeId(id),
                addr: None,
                role: NodeRole::Data,
            })
            .expect("register");
    }
    cluster.rebalance(1).expect("rebalance");

    // Reconcile membership to the ACTUAL assigned set: deregister every member that owns no
    // primary (incl. the genesis node 0). With members == the set of owning nodes, the skew tick
    // fires NO membership rebalance — so the ordering guard does not suppress the handoff, and the
    // driver is genuinely exercised. (HRW need not place every registered node, so this reconcile
    // is what makes the test robust rather than depending on a particular placement.)
    let assigned: std::collections::BTreeSet<u64> = cluster
        .control_state()
        .expect("state")
        .assignments
        .iter()
        .map(|a| a.primary.0)
        .collect();
    for n in cluster.control_state().expect("state").nodes {
        if !assigned.contains(&n.id.0) {
            cluster
                .deregister_node(n.id)
                .expect("deregister unassigned");
        }
    }

    // Derive a skew threshold just under the observed max/mean so node-skew fires deterministically.
    let state = cluster.control_state().expect("state");
    let counts = cluster.shard_query_counts().expect("counts");
    let mut node_load: std::collections::BTreeMap<u64, usize> = std::collections::BTreeMap::new();
    for a in &state.assignments {
        if let Some(&c) = counts.get(a.position as usize) {
            *node_load.entry(a.primary.0).or_default() += c;
        }
    }
    assert!(
        node_load.len() >= 2,
        "the shards must land on ≥2 nodes for node-skew to apply: {node_load:?}"
    );
    let total: usize = node_load.values().sum();
    let mean = total as f64 / node_load.len() as f64;
    let max = *node_load.values().max().unwrap() as f64;
    assert!(
        max > mean,
        "shards must distribute unevenly to skew: {node_load:?}"
    );
    let skew = (max / mean - 0.01).max(1.0 + f64::EPSILON);
    let acfg = AutoscaleConfig {
        enabled: true,
        target_replication_factor: 1,
        max_node_load_skew: skew,
        split_corpus_threshold: 0,
    };

    let decision = cluster.tick(&acfg).expect("tick");
    assert!(
        decision
            .actions
            .iter()
            .any(|a| matches!(a, ScalingAction::Handoff { .. })),
        "skew over threshold recommends a handoff: {decision:?}"
    );
    // The driver reached the resolution path and skipped fail-safe (the target has no endpoint).
    assert!(
        saw_skip.load(std::sync::atomic::Ordering::Acquire),
        "the driver must surface a missing-endpoint event when it can't resolve the move"
    );
    // No flip: a skipped move never re-points a position.
    assert_eq!(
        cluster.handoff_generations(),
        vec![0; NS],
        "a skipped handoff must not flip any position's routing"
    );

    // Zero false negatives: matching is unchanged across the autoscaler tick.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "cluster vs brute on {title:?} after an autoscaler tick"
        );
    }

    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
