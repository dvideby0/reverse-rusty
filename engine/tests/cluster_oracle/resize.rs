//! Resize differential (ADR-078, ADR-065 criterion 7): a cluster resized K→K′ returns EXACTLY
//! the brute-force oracle's set AND the single-node engine's set — zero false negatives across
//! grow, shrink, and shrink-to-1, broad on/off, tagged + untagged. The resize is a blue/green
//! rebuild under a fresh ring (`placement_of(&dict, &new_ring, &ex)` for every live query), so
//! this is the cover-preservation proof that re-placing a corpus under a different shard count
//! cannot drop a match.

use crate::harness::*;
use reverse_rusty::cluster::{
    recommended_shard_count, AutoscaleConfig, ClusterConfig, ClusterEngine,
};
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// Assert a cluster ≡ the independent brute oracle ≡ the single-node engine for every title,
/// broad on AND off — the full zero-FN contract. `reference` is the K-independent single-node
/// engine (reused across calls).
fn assert_matches_refs(
    cluster: &ClusterEngine,
    titles: &[String],
    reference: &mut Engine,
    brute: &Brute,
    ctx: &str,
) {
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    for title in titles {
        let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        assert_eq!(got, truth, "{ctx}: cluster vs brute oracle on {title:?}");
        reference.match_title(title, &mut s, &mut out, true);
        let ref_broad: HashSet<u64> = out.iter().copied().collect();
        assert_eq!(got, ref_broad, "{ctx}: cluster vs single-node on {title:?}");

        // broad off: the cluster's selective path must equal the single-node selective path.
        let got_sel: HashSet<u64> = cluster
            .percolate_with_broad(title, false)
            .unwrap()
            .into_iter()
            .collect();
        reference.match_title(title, &mut s, &mut out, false);
        let ref_sel: HashSet<u64> = out.iter().copied().collect();
        assert_eq!(
            got_sel, ref_sel,
            "{ctx}: cluster broad=off vs single-node selective on {title:?}"
        );
    }
}

#[test]
fn resize_grow_matches_oracle_and_single_node() {
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);

    for &(k0, k1) in &[(1usize, 8usize), (3, 8), (3, 16), (8, 16)] {
        let cfg = ClusterConfig {
            num_shards: k0,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
        let rebuilt = cluster.resize(k1).expect("resize grow");
        assert_eq!(cluster.num_shards(), k1, "{k0}->{k1}: num_shards updated");
        assert!(
            rebuilt > 0,
            "{k0}->{k1}: the rebuild covered the live corpus"
        );

        // Every placement class still present after the resize (broad/shard-0 lane survives).
        let cc = cluster.class_counts().unwrap();
        assert!(
            cc[0] > 0 && cc[1] > 0 && cc[2] > 0,
            "{k0}->{k1}: a placement class vanished after resize: {cc:?}"
        );
        assert_matches_refs(
            &cluster,
            &titles,
            &mut reference,
            &brute,
            &format!("grow {k0}->{k1}"),
        );
    }
}

#[test]
fn resize_shrink_matches_oracle_and_single_node() {
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);

    // Includes shrink-to-1 (everything collapses onto shard 0 — selective + the broad lane).
    for &(k0, k1) in &[(8usize, 3usize), (16, 3), (8, 1), (3, 1)] {
        let cfg = ClusterConfig {
            num_shards: k0,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
        cluster.resize(k1).expect("resize shrink");
        assert_eq!(cluster.num_shards(), k1, "{k0}->{k1}: num_shards updated");
        let cc = cluster.class_counts().unwrap();
        assert!(
            cc[0] > 0 && cc[1] > 0 && cc[2] > 0,
            "{k0}->{k1}: a placement class vanished after shrink: {cc:?}"
        );
        assert_matches_refs(
            &cluster,
            &titles,
            &mut reference,
            &brute,
            &format!("shrink {k0}->{k1}"),
        );
    }
}

#[test]
fn repeated_resize_round_trips_preserve_recall() {
    // Grow → shrink → re-grow → shrink-to-1 → grow, all in one process: exercises new-position
    // creation, merge-down, and re-grow back through previously-dropped positions, each step
    // still ≡ the oracle (zero FN). The in-memory analogue of the durable shrink→regrow test.
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);
    let check = &titles[..titles.len().min(250)];

    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    let mut generation = cluster.placement_generation();
    for &k in &[8usize, 2, 8, 1, 5] {
        cluster.resize(k).expect("resize");
        generation = generation.next().expect("generation capacity");
        assert_eq!(
            cluster.placement_generation(),
            generation,
            "each completed blue/green resize bumps placement exactly once"
        );
        assert_eq!(cluster.num_shards(), k);
        assert_matches_refs(
            &cluster,
            check,
            &mut reference,
            &brute,
            &format!("round-trip ->{k}"),
        );
    }
}

#[test]
fn resize_preserves_tags() {
    // A tagged cluster resized in either direction keeps every query's tags on whichever shard
    // now holds it: filtered percolation ≡ the brute oracle filtered by the same tags (ADR-074
    // carry-through, here driven by a ring change rather than a vocabulary change).
    let (queries, titles) = build_corpus();
    let tags = tags_parallel(&queries);
    let brute = Brute::build(&queries);

    for &(k0, k1) in &[(3usize, 8usize), (8, 3)] {
        let cfg = ClusterConfig {
            num_shards: k0,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
            .expect("build tagged cluster");
        cluster.resize(k1).expect("resize");

        let mut blc = String::new();
        let mut bfeats = Vec::new();
        for (ti, title) in titles.iter().take(120).enumerate() {
            let truth = brute.matches(title, &mut blc, &mut bfeats);
            let unfiltered: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(
                unfiltered, truth,
                "{k0}->{k1}: unfiltered ≠ oracle on {title:?}"
            );
            for filter in filters_for(ti) {
                let got: HashSet<u64> = cluster
                    .percolate_filtered(title, &filter)
                    .unwrap()
                    .into_iter()
                    .collect();
                let want: HashSet<u64> = truth
                    .iter()
                    .copied()
                    .filter(|l| passes_filter(&tags_for(*l), &filter))
                    .collect();
                assert_eq!(
                    got, want,
                    "{k0}->{k1}: filtered ≠ oracle (title {ti}, filter {filter:?})"
                );
                assert!(
                    got.is_subset(&unfiltered),
                    "{k0}->{k1}: filter added a match (title {ti})"
                );
            }
        }
    }
}

#[test]
fn resize_noop_and_invalid_are_safe() {
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
    let generation = cluster.placement_generation();
    assert_eq!(
        cluster.resize(4).expect("no-op"),
        0,
        "resize to the current count is a no-op (0 rebuilt)"
    );
    assert_eq!(cluster.num_shards(), 4);
    assert_eq!(cluster.placement_generation(), generation);
    assert!(cluster.resize(0).is_err(), "resize to 0 must error");
    assert_eq!(
        cluster.num_shards(),
        4,
        "a rejected resize leaves the cluster unchanged"
    );
    assert_eq!(
        cluster.placement_generation(),
        generation,
        "no-op and rejected resizes never bump placement generation"
    );
}

#[test]
fn resize_to_recommended_grows_and_preserves_recall() {
    // The autoscaler's `recommended_shard_count` over an over-threshold cluster, applied via
    // `resize_to_recommended`: it grows the ring and recall is preserved (zero FN).
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);
    let cfg = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");

    // A low threshold so each ~6k-query shard is over it ⇒ recommend K + (#over) > K.
    let ac = AutoscaleConfig {
        enabled: true,
        target_replication_factor: 1,
        max_node_load_skew: 0.0,
        split_corpus_threshold: 1000,
    };
    let snap = cluster.collect_load(&ac).expect("collect_load");
    let rec = recommended_shard_count(&snap, &ac).expect("a recommendation");
    assert!(
        rec > 2,
        "an over-threshold cluster recommends growing: {rec}"
    );

    let applied = cluster
        .resize_to_recommended(&ac)
        .expect("resize_to_recommended")
        .expect("a resize happened");
    assert_eq!(applied, rec, "applied the recommended count");
    assert_eq!(cluster.num_shards(), rec);

    let mut blc = String::new();
    let mut bfeats = Vec::new();
    for title in titles.iter().take(120) {
        let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        assert_eq!(
            got, truth,
            "recall preserved after auto-resize on {title:?}"
        );
    }
}
