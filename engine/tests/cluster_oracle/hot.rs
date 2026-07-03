//! The cluster hot tier (class H, ADR-105) — the multi-shard differential for the
//! θ-hot-anchored always-visible tier. A class-H query places **selectively**
//! (ring-hashed on its non-top-64 anchor, exactly like class A — no replication,
//! no broad-eval-shard gating), so the cluster must match the single-node θ-on
//! engine and the independent brute across K ∈ {1, 3, 8, 16} × broad on/off —
//! including broad OFF, where the always-visible contract bites. This file also
//! pins the placement distinction: class H storage does NOT scale with K
//! (ring-placed once) while class C's does (replicated, ADR-080) — the proof the
//! hot tier avoids the replicated-B-arity-2 fan-out-multiply shape.

use crate::harness::*;
use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// The single-node oracle's θ at this scale (a genuine A/H mix — asserted).
const THETA: u32 = 64;

fn theta_cfg() -> EngineConfig {
    EngineConfig {
        hot_anchor_threshold: THETA,
        ..EngineConfig::default()
    }
}

fn corpus(seed: u64) -> (Vec<(u64, String)>, Vec<String>) {
    let data = generate(&GenConfig {
        num_queries: 20_000,
        num_titles: 1_500,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: 2_000,
        num_sets: 1_000,
    });
    (data.queries, data.titles)
}

/// The load-bearing differential: cluster (θ-on) ≡ single-node (θ-on) ≡ brute,
/// across K ∈ {1, 3, 8, 16} × include_broad ∈ {off, on}. The broad-off sweep is
/// what catches the one natural-looking wrong edit — routing by `is_hot_anchor`
/// instead of the frozen top-64 `is_hot` would make every class-H query
/// unreachable (its anchor's shard never probed).
#[test]
fn cluster_hot_matches_single_node_and_oracle() {
    let (queries, titles) = corpus(0x0C1A_0407);

    let mut reference = Engine::with_config(vocab(), theta_cfg());
    reference.build_from_queries(&queries);
    let ref_cc = reference.class_counts();
    assert!(ref_cc[4] > 0, "degenerate: single-node stored no class H");
    assert!(ref_cc[0] > 0, "degenerate: no class A mix");
    assert!(ref_cc[2] > 0, "degenerate: no class C for the ×K contrast");
    let brute = Brute::build(&queries);

    // K-independent references, both visibility modes.
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut ref_broad: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut ref_sel: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    for title in &titles {
        reference.match_title(title, &mut s, &mut out, true);
        let rb: HashSet<u64> = out.iter().copied().collect();
        assert_eq!(
            rb,
            brute.matches(title, &mut blc, &mut bfeats),
            "single-node(θ-on) disagrees with brute on {title:?}"
        );
        ref_broad.push(rb);
        reference.match_title(title, &mut s, &mut out, false);
        ref_sel.push(out.iter().copied().collect());
    }

    for &k in &[1usize, 3, 8, 16] {
        let mut cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        cfg.per_shard.hot_anchor_threshold = THETA;
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

        // Placement distinction: class H is RING-PLACED (stored once cluster-wide,
        // NOT a multiple of K) while class C replicates to every shard (ADR-080).
        let cc = cluster.class_counts().unwrap();
        assert_eq!(
            cc[4], ref_cc[4],
            "K={k}: class H must be ring-placed exactly once cluster-wide"
        );
        assert_eq!(
            cc[2],
            ref_cc[2] * k as u64,
            "K={k}: class C replicates to every shard (the contrast)"
        );

        for (i, title) in titles.iter().enumerate() {
            let broad: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(broad, ref_broad[i], "K={k}: broad-on vs single-node");
            let sel: HashSet<u64> = cluster
                .percolate_with_broad(title, false)
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(
                sel, ref_sel[i],
                "K={k}: broad-OFF vs single-node — a class-H query went \
                 unreachable or invisible on {title:?}"
            );
        }
    }
}

/// A live single-anchor class-H add places on exactly ONE shard (ring placement,
/// `Target::Selective`) — the structural proof it avoids the replicated lane —
/// and is immediately retrievable through a broad-OFF percolate (always-visible).
#[test]
fn live_hot_add_is_ring_placed_and_always_visible() {
    let (queries, _titles) = corpus(0x0C1A_11FE);
    let mut cfg = ClusterConfig {
        num_shards: 8,
        include_broad: false, // the sharp mode: H must be visible WITHOUT broad
        ..ClusterConfig::default()
    };
    cfg.per_shard.hot_anchor_threshold = THETA;
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    let h_before = cluster.class_counts().unwrap()[4];
    assert!(h_before > 0, "degenerate: no class H stored");

    // Find a θ-hot single-token anchor by re-adding an existing single-token
    // H-class query text under a fresh id: its anchor frequency is already ≥ θ
    // in the shared frozen dict, so the add classifies H deterministically.
    // The generator's broad shape 0 emits single-token player queries — pick a
    // text whose add lands in class H by probing the count delta.
    let mut placed_one = false;
    for (base_id, text) in queries.iter().take(4_000) {
        if text.split_whitespace().count() != 1 {
            continue;
        }
        let id = 9_000_000 + base_id;
        let outcome = cluster.add_query(id, text).unwrap();
        let AddOutcome::Placed { shards } = outcome else {
            continue;
        };
        let h_now = cluster.class_counts().unwrap()[4];
        if h_now == h_before + 1 {
            // This add classified H: ring placement = exactly one shard.
            assert_eq!(
                shards.len(),
                1,
                "a single-anchor class-H add must place on exactly one shard"
            );
            // …and it is visible on a broad-OFF percolate (the cluster default here).
            let got: HashSet<u64> = cluster
                .percolate(&format!("{text} listing extra"))
                .unwrap()
                .into_iter()
                .collect();
            assert!(
                got.contains(&id),
                "a live class-H add went invisible on the broad-off path"
            );
            placed_one = true;
            break;
        }
        // Not H (a rare anchor) — fine, keep looking.
    }
    assert!(placed_one, "never found a θ-hot single-token add to probe");
}

/// θ divergence between builds is cost-only: the SAME corpus built θ-on and θ-off
/// percolates byte-identically on the cluster for BOTH visibility modes (the
/// cluster analogue of the single-node visibility-invariance oracle).
#[test]
fn cluster_theta_is_visibility_invariant() {
    let (queries, titles) = corpus(0x0C1A_1417);
    let build = |theta: u32| {
        let mut cfg = ClusterConfig {
            num_shards: 4,
            include_broad: true,
            ..ClusterConfig::default()
        };
        cfg.per_shard.hot_anchor_threshold = theta;
        ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster")
    };
    let hot = build(THETA);
    let off = build(0);
    assert!(hot.class_counts().unwrap()[4] > 0);
    assert_eq!(off.class_counts().unwrap()[4], 0);
    for title in &titles {
        assert_eq!(
            hot.percolate(title).unwrap(),
            off.percolate(title).unwrap(),
            "θ changed cluster broad-on results on {title:?}"
        );
        assert_eq!(
            hot.percolate_with_broad(title, false).unwrap(),
            off.percolate_with_broad(title, false).unwrap(),
            "θ changed cluster broad-off results on {title:?}"
        );
    }
}
