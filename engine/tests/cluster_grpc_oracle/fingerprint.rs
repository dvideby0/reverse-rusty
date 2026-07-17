//! Content-fingerprint skip over gRPC (ADR-097): a RETAINED group-move member whose
//! order-independent live-set `ContentFingerprint` equals the frozen source's is provably
//! complete and SKIPS its `O(corpus)` re-copy — while a silently-desynced member fingerprint-
//! mismatches and still heals through the proven ADR-094 re-copy. The observable is the
//! coordinator's per-RPC transport metrics (ADR-085): the `recover_from` call delta is 0 on the
//! skip path and ≥ 1 on the heal path.
//!
//! Two proofs:
//!  - `grpc_pure_promotion_skips_complete_retained_member_zero_fn` — the ADR-094 cost case: a
//!    pure promotion (every member retained, F = ∅) runs ZERO `RecoverFrom` — the fence window
//!    collapses to freeze-probe + fingerprint RPCs + swap — and stays ≡ brute live + across a
//!    resolve-only coordinator restart.
//!  - `grpc_desynced_retained_member_fingerprint_mismatches_and_heals` — the guard-rail: a
//!    replica desynced OUT-OF-BAND (a rogue write through a second coordinator pointed only at
//!    it) mismatches, is re-copied (`recover_from` ≥ 1), and the rogue entry is HEALED AWAY —
//!    the promoted primary serves exactly the source's live set, ≡ brute (the skip never fires
//!    on divergent content).

use std::sync::Arc;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ReassignOutcome, ShardAssignment, ShardGroup,
};

use crate::harness::*;
use crate::reconcile_replicated::fixture::{
    assert_matches_oracle, converge_repairs, resolved_groups, seed_group_map, spin_n_durable,
    teardown,
};

/// The coordinator's cumulative call count for one transport-metrics method label.
fn rpc_calls(cluster: &ClusterEngine, label: &str) -> u64 {
    cluster
        .transport_metrics()
        .methods
        .iter()
        .find(|m| m.method == label)
        .map_or(0, |m| m.calls)
}

/// A K=1 RF=2 cluster over two durable servers with the committed group {primary: node 1 (A),
/// replica: node 2 (B)} and the corpus ingested + flushed — the pure-promotion fixture.
#[allow(clippy::type_complexity)]
fn build_rf2(
    tag: &str,
) -> (
    ClusterEngine,
    Vec<crate::reconcile_replicated::fixture::Server>,
    tokio::runtime::Runtime,
    Arc<reverse_rusty::normalize::Normalizer>,
    Arc<reverse_rusty::dict::Dict>,
    ClusterConfig,
    Vec<(u64, String)>,
    Vec<String>,
) {
    let (queries, titles) = build_corpus();
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        replication_factor: 2,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let servers = spin_n_durable(&rt, &norm, tag, 2);
    let groups = vec![ShardGroup {
        primary: servers[0].ep.clone(),
        replicas: vec![servers[1].ep.clone()],
    }];
    let cluster = ClusterEngine::connect_replicated(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &groups,
        rt.handle(),
    )
    .expect("connect RF=2 cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");
    cluster.flush().expect("flush both copies to segments");
    seed_group_map(&cluster, &servers, &[(0, vec![1])]); // committed {0: A primary, B replica}
    (cluster, servers, rt, norm, dict, cfg, queries, titles)
}

/// The promotion target: swap primary and replica — every member RETAINED, F = ∅.
fn promotion() -> ShardAssignment {
    ShardAssignment {
        position: 0,
        primary: reverse_rusty::cluster::NodeId(2),
        replicas: vec![reverse_rusty::cluster::NodeId(1)],
    }
}

#[test]
fn grpc_pure_promotion_skips_complete_retained_member_zero_fn() {
    let (cluster, servers, rt, norm, dict, cfg, queries, titles) = build_rf2("fp_skip");
    let oracle = build_oracle(&queries, &titles);

    // The observable: RecoverFrom calls before the move.
    let recover_before = rpc_calls(&cluster, "recover_from");
    let fp_before = rpc_calls(&cluster, "content_fingerprint");

    let outcome = cluster
        .reassign_group_and_move(0, &promotion(), rt.handle())
        .expect("pure promotion");
    assert!(
        matches!(outcome, ReassignOutcome::Moved { .. }),
        "{outcome:?}"
    );

    // THE SKIP: the retained member (the demoted A) was provably complete — ZERO re-copy ran.
    // The fingerprints were consulted (source + the one retained member = 2 calls).
    assert_eq!(
        rpc_calls(&cluster, "recover_from") - recover_before,
        0,
        "a provably-complete retained member skips its O(corpus) RecoverFrom"
    );
    assert!(
        rpc_calls(&cluster, "content_fingerprint") - fp_before >= 2,
        "the skip decision consulted both sides' fingerprints"
    );

    converge_repairs(&cluster);
    assert_matches_oracle(&cluster, &titles, &oracle, "after the skipped promotion");

    // The promoted group serves across a resolve-only coordinator restart, zero-FN.
    let state = cluster.control_state().expect("state");
    let coord2 = ClusterEngine::connect_replicated(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &resolved_groups(&state),
        rt.handle(),
    )
    .expect("fresh coordinator over the resolved committed groups");
    assert_matches_oracle(&coord2, &titles, &oracle, "restart coordinator");

    teardown(&servers);
}

#[test]
fn grpc_desynced_retained_member_fingerprint_mismatches_and_heals() {
    let (cluster, servers, rt, norm, dict, _cfg, queries, titles) = build_rf2("fp_heal");
    let oracle = build_oracle(&queries, &titles);

    // DESYNC the replica (B) out-of-band: a rogue write through a SECOND coordinator pointed
    // ONLY at B — its slot is unfenced, so the write lands on B and nowhere else. The rogue
    // reuses an existing matching DSL under a fresh id, so if it survived the move it would
    // surface as a phantom id in percolate results (an FP vs brute).
    let rogue_id = 999_999_u64;
    let rogue_dsl = queries
        .iter()
        .map(|(_, dsl)| dsl.clone())
        .find(|dsl| {
            let (q, t) = (vec![(rogue_id, dsl.clone())], titles.clone());
            build_oracle(&q, &t).iter().any(|s| !s.is_empty())
        })
        .expect("a corpus DSL that matches some title");
    {
        let side_cfg = ClusterConfig {
            num_shards: 1,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let side = ClusterEngine::connect_remote(
            Arc::clone(&norm),
            Arc::clone(&dict),
            empty_tag_dict(),
            &side_cfg,
            std::slice::from_ref(&servers[1].ep),
            rt.handle(),
        )
        .expect("side coordinator over B only");
        // Upsert, not add: a coordinator attached to an already-populated remote
        // has an unseeded logical-id directory, so insert-only `add_query` fails
        // closed there (ADR-109 unique-id admission); the replacement path lands
        // the same out-of-band rogue row.
        side.upsert_query(rogue_id, &rogue_dsl, 1)
            .expect("rogue write to B");
    }

    let recover_before = rpc_calls(&cluster, "recover_from");
    let outcome = cluster
        .reassign_group_and_move(0, &promotion(), rt.handle())
        .expect("promotion over the desynced replica");
    assert!(
        matches!(outcome, ReassignOutcome::Moved { .. }),
        "{outcome:?}"
    );

    // THE GUARD-RAIL: the fingerprints mismatched, so the member was HEALED by the proven
    // re-copy (never skipped on divergent content).
    assert!(
        rpc_calls(&cluster, "recover_from") - recover_before >= 1,
        "a desynced retained member is re-copied, not skipped"
    );

    converge_repairs(&cluster);
    // The rogue entry is gone: the promoted primary (B) serves EXACTLY the source's live set —
    // every title ≡ brute, no phantom rogue id anywhere.
    for (i, title) in titles.iter().enumerate() {
        let got: std::collections::HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert!(
            !got.contains(&rogue_id),
            "the rogue write was healed away on {title:?}"
        );
        assert_eq!(got, oracle[i], "healed cluster vs brute on {title:?}");
    }

    teardown(&servers);
}
