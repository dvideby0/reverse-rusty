//! ADR-035: per-shard replication (replication_factor > 1) — answer-invariance vs the single-node
//! engine + brute oracle, count non-inflation (the primary's view, never summed across copies), and
//! the replicated live write path.

use crate::harness::*;
use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// Replication is answer-invariant: a cluster with replicas returns EXACTLY the single-node
/// engine's set and the independent brute oracle's set, across shard counts × broad on/off.
/// (RF = 1 is covered by `cluster_matches_single_node_and_oracle`.)
#[test]
fn cluster_with_replicas_matches_single_node_and_oracle() {
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut ref_broad: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut ref_selective: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut oracle: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    for title in &titles {
        reference.match_title(title, &mut s, &mut out, true);
        ref_broad.push(out.iter().copied().collect());
        reference.match_title(title, &mut s, &mut out, false);
        ref_selective.push(out.iter().copied().collect());
        oracle.push(brute.matches(title, &mut blc, &mut bfeats));
    }

    for &rf in &[2usize, 3] {
        for &k in &[1usize, 3, 8] {
            let cfg = ClusterConfig {
                num_shards: k,
                replication_factor: rf,
                include_broad: true,
                ..ClusterConfig::default()
            };
            let cluster =
                ClusterEngine::build(vocab(), &cfg, &queries).expect("build replicated cluster");
            // Replicas must not inflate the per-class tally (it is the primary's view).
            let cc = cluster.class_counts().unwrap();
            assert!(
                cc[0] > 0 && cc[1] > 0 && cc[2] > 0,
                "rf={rf} k={k}: classes {cc:?}"
            );
            for (i, title) in titles.iter().enumerate() {
                let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
                assert_eq!(
                    got, oracle[i],
                    "rf={rf} k={k} broad=on: cluster vs oracle on {title:?}"
                );
                assert_eq!(
                    got, ref_broad[i],
                    "rf={rf} k={k} broad=on: cluster vs single-node on {title:?}"
                );
                let got_sel: HashSet<u64> = cluster
                    .percolate_with_broad(title, false)
                    .unwrap()
                    .into_iter()
                    .collect();
                assert_eq!(
                    got_sel, ref_selective[i],
                    "rf={rf} k={k} broad=off: cluster vs single-node selective on {title:?}"
                );
            }
        }
    }
}

/// Replicas are HA copies, not new logical data: `num_queries` / `class_counts` /
/// per-position counts must equal the RF = 1 cluster's. The composite reports the PRIMARY's
/// view, never a sum across copies — else the coordinator's cross-position sums would
/// multiply by the replication factor.
#[test]
fn replication_does_not_inflate_counts() {
    let (queries, _titles) = build_corpus();
    let mk = |rf: usize| {
        let cfg = ClusterConfig {
            num_shards: 3,
            replication_factor: rf,
            ..ClusterConfig::default()
        };
        ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster")
    };
    let base = mk(1);
    let replicated = mk(3);
    assert_eq!(
        base.num_queries().unwrap(),
        replicated.num_queries().unwrap(),
        "num_queries must not be inflated by replicas"
    );
    assert_eq!(
        base.class_counts().unwrap(),
        replicated.class_counts().unwrap(),
        "class_counts must not be inflated by replicas"
    );
    assert_eq!(
        base.shard_query_counts().unwrap(),
        replicated.shard_query_counts().unwrap(),
        "per-position counts must be the primary's, not summed across copies"
    );
}

/// The live write path at RF > 1: `add_query` fans to the replicas (so the query is findable
/// and copies stay set-equal), and `remove_query` returns the PRIMARY's tombstone count
/// (rf-independent), not a sum across copies.
#[test]
fn replicated_live_add_and_remove() {
    let (queries, _titles) = build_corpus();
    let mk = |rf: usize| {
        let cfg = ClusterConfig {
            num_shards: 8,
            replication_factor: rf,
            include_broad: true,
            ..ClusterConfig::default()
        };
        ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster")
    };
    let c1 = mk(1);
    let c2 = mk(2);
    let qid = 7_777_778u64;
    let dsl = "1994 upper deck rareplayer0";

    assert!(
        matches!(c2.add_query(qid, dsl).unwrap(), AddOutcome::Placed { .. }),
        "class-A live add should be Placed"
    );
    let title = "1994 upper deck rareplayer0 psa 10";
    assert!(
        c2.percolate(title).unwrap().contains(&qid),
        "a live-added query must be findable at rf=2 (write fanned to the replica)"
    );

    // The remove count is the primary's (same as rf=1), not multiplied by replicas.
    c1.add_query(qid, dsl).unwrap();
    let r1 = c1.remove_query(qid).unwrap();
    let r2 = c2.remove_query(qid).unwrap();
    assert_eq!(
        r1, r2,
        "remove count must be primary-only (rf-independent): rf1={r1} rf2={r2}"
    );
    assert!(
        !c2.percolate(title).unwrap().contains(&qid),
        "a removed query must no longer match"
    );
}
