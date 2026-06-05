//! Core three-way differential across shard counts {1, 3, 8, 16} × broad on/off, plus the
//! K=1 reduction to the single-node engine.

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

#[test]
fn cluster_matches_single_node_and_oracle() {
    let (queries, titles) = build_corpus();

    // Reference (single-node) and oracle are K-independent — build once. The
    // reference uses build_from_queries over the WHOLE corpus in one pass, so its
    // dict mask is finalized over the same feature distribution as the cluster's
    // authoritative dict (otherwise hot-mask divergence could legitimately differ
    // classifications).
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
    let mut total_truth = 0usize;
    for title in &titles {
        reference.match_title(title, &mut s, &mut out, true);
        ref_broad.push(out.iter().copied().collect());
        reference.match_title(title, &mut s, &mut out, false);
        ref_selective.push(out.iter().copied().collect());
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        oracle.push(truth);
    }
    assert!(total_truth > 0, "degenerate corpus: no matches at all");
    // The single-node engine itself must satisfy the contract with broad on.
    for (i, _) in titles.iter().enumerate() {
        assert_eq!(ref_broad[i], oracle[i], "single-node disagrees with oracle");
    }

    for &k in &[1usize, 3, 8, 16] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
        assert_eq!(cluster.num_shards(), k);

        // Every placement branch is exercised (A, B, C all present).
        let cc = cluster.class_counts().unwrap();
        assert!(cc[0] > 0, "K={k}: no class-A queries");
        assert!(
            cc[1] > 0,
            "K={k}: no class-B queries (any-of/arity-2 injection)"
        );
        assert!(cc[2] > 0, "K={k}: no class-C (broad) queries");

        for (i, title) in titles.iter().enumerate() {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(
                got, oracle[i],
                "K={k} broad=on: cluster vs brute-force oracle on {title:?}"
            );
            assert_eq!(
                got, ref_broad[i],
                "K={k} broad=on: cluster vs single-node on {title:?}"
            );

            // broad off: cluster must equal the single-node selective path (both
            // exclude class-C broad matches; class-B-arity-2 stays in the main lane).
            let got_sel: HashSet<u64> = cluster
                .percolate_with_broad(title, false)
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(
                got_sel, ref_selective[i],
                "K={k} broad=off: cluster vs single-node selective on {title:?}"
            );
        }
    }
}

#[test]
fn single_shard_cluster_equals_single_node_engine() {
    let (queries, titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    for title in &titles {
        let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
        reference.match_title(title, &mut s, &mut out, true);
        let want: HashSet<u64> = out.iter().copied().collect();
        assert_eq!(got, want, "K=1 must reduce to the single-node engine");
    }
}
