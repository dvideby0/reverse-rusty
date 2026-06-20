//! The cluster class-D always-candidate lane (ADR-080) — graduating ADR-068's
//! single-node lane onto the multi-shard core via replicate-broad-to-all. A
//! negation-only query, accepted under `per_shard.accept_class_d`, is replicated to
//! every shard under the universal signature and evaluated on each title's broad-eval
//! shard — so the cluster matches EXACTLY the single-node engine (lane on) and the
//! independent brute oracle, across shard counts {1, 3, 8, 16} × broad on/off. Lane off
//! (the default) keeps the loud reject. This file also pins the two halves of the
//! replicate-to-all distinction: storage fan-out = N (broad on every shard) while read
//! fan-out stays bounded (broad evaluated on one shard, the shard-0 hotspot gone).

use crate::harness::*;
use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{gen_class_d_queries, generate, GenConfig};
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// Logical-id offset for the negation-only queries, so they are recognizable in result
/// sets (every generated regular id is far below this).
const CLASS_D_ID_BASE: u64 = 1_000_000;

fn lane_on() -> EngineConfig {
    EngineConfig {
        accept_class_d: true,
        ..EngineConfig::default()
    }
}

/// A generated regular corpus (class A + class C broad) with negation-only queries
/// interleaved every `step` slots — a direct port of the single-node `mixed_corpus`
/// (`tests/oracle/class_d.rs`), so the same generator + `gen_class_d_queries` guarantee
/// the corpus exercises BOTH vacuous outcomes (a class-D query matching a title, and a
/// forbidden feature rejecting one).
fn mixed_corpus(
    seed: u64,
    n_regular: usize,
    n_class_d: usize,
) -> (Vec<(u64, String)>, Vec<String>) {
    let data = generate(&GenConfig {
        num_queries: n_regular,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: 2_000,
        num_sets: 1_000,
    });
    let class_d = gen_class_d_queries(seed ^ 0xD00D, n_class_d);
    let step = (n_regular / n_class_d.max(1)).max(1);
    let mut queries: Vec<(u64, String)> = Vec::with_capacity(n_regular + n_class_d);
    let mut di = 0usize;
    for (i, (id, text)) in data.queries.into_iter().enumerate() {
        if i % step == 0 && di < class_d.len() {
            queries.push((CLASS_D_ID_BASE + di as u64, class_d[di].clone()));
            di += 1;
        }
        queries.push((id, text));
    }
    while di < class_d.len() {
        queries.push((CLASS_D_ID_BASE + di as u64, class_d[di].clone()));
        di += 1;
    }
    (queries, data.titles)
}

/// The load-bearing differential: cluster (lane on) ≡ single-node (lane on) ≡ brute
/// (keeping class-D), across K ∈ {1, 3, 8, 16}. Proves the always-candidate is visible to
/// every percolate on whichever shard each title evaluates broad — zero false negatives.
#[test]
fn cluster_class_d_matches_single_node_and_oracle() {
    let (queries, titles) = mixed_corpus(0x00C1_A55D, 12_000, 300);

    // K-independent references: the single-node engine with the lane on, and the
    // independent brute that keeps class-D. They must already agree (the engine's own
    // contract) before the cluster is held to them.
    let mut reference = Engine::with_config(vocab(), lane_on());
    reference.build_from_queries(&queries);
    let brute = Brute::build_accepting_class_d(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut ref_broad: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut oracle: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let (mut d_matched, mut d_rejected) = (false, false);
    for title in &titles {
        reference.match_title(title, &mut s, &mut out, true);
        let rb: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        assert_eq!(
            rb, truth,
            "single-node(lane on) disagrees with brute on {title:?}"
        );
        d_matched |= rb.iter().any(|&id| id >= CLASS_D_ID_BASE);
        d_rejected |= (0..300u64).any(|d| !rb.contains(&(CLASS_D_ID_BASE + d)));
        ref_broad.push(rb);
        oracle.push(truth);
    }
    assert!(
        d_matched,
        "no class-D query ever matched — degenerate corpus"
    );
    assert!(
        d_rejected,
        "no forbidden feature ever rejected a class-D query — degenerate corpus"
    );
    let ref_d = reference.class_counts()[3];
    assert!(ref_d > 0, "single-node stored no class-D");

    for &k in &[1usize, 3, 8, 16] {
        let mut cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        cfg.per_shard.accept_class_d = true;
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

        // Class D is now stored on EVERY shard (replicate-to-all) — counted once per shard.
        let cc = cluster.class_counts().unwrap();
        assert_eq!(
            cc[3],
            ref_d * k as u64,
            "K={k}: class-D must be replicated to every shard"
        );

        for (i, title) in titles.iter().enumerate() {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(got, oracle[i], "K={k}: cluster vs brute on {title:?}");
            assert_eq!(
                got, ref_broad[i],
                "K={k}: cluster vs single-node(lane on) on {title:?}"
            );
        }
    }
}

/// Lane off (the default): class-D is rejected at placement, stored nowhere, never
/// matched — and the cluster still satisfies the class-D-rejecting brute over the same
/// mixed input. Also pins the live `add_query` reject.
#[test]
fn cluster_class_d_rejected_when_lane_off() {
    let (queries, titles) = mixed_corpus(0x0FF0_C1D5, 6_000, 200);
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    assert_eq!(
        cluster.class_counts().unwrap()[3],
        0,
        "no class-D stored with the lane off"
    );
    assert_eq!(
        cluster.add_query(9_999_999, "-auto -signed").unwrap(),
        AddOutcome::RejectedClassD,
        "a live negation-only add must reject with the lane off"
    );

    let brute = Brute::build(&queries); // the class-D-rejecting reference
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    for title in &titles {
        let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
        assert!(
            got.iter().all(|&id| id < CLASS_D_ID_BASE),
            "a class-D query matched with the lane off on {title:?}"
        );
        assert_eq!(
            got,
            brute.matches(title, &mut blc, &mut bfeats),
            "lane-off cluster vs class-D-rejecting brute on {title:?}"
        );
    }
}

/// An effectively-empty query (no positives AND no forbidden) is rejected even with the lane on
/// — storing it would be a match-all (ADR-068). Crucially, an UPSERT to such a query must NOT
/// tombstone the prior live version: the coordinator rejects at placement BEFORE the two-pass
/// tombstone (a failed replace never deletes), so the prior query survives. The bug this guards:
/// placement fanned the empty plan to every shard, each stored nothing, and the upsert had
/// already deleted the prior copies — silent data loss.
#[test]
fn empty_class_d_is_rejected_and_a_failed_upsert_never_deletes() {
    let mut cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    cfg.per_shard.accept_class_d = true;
    let cluster =
        ClusterEngine::build(vocab(), &cfg, &[(1, "1996 skybox".to_string())]).expect("build");

    // An empty / whitespace-only add is rejected (class D, nothing to forbid), stores nothing.
    for empty in ["", "   "] {
        assert_eq!(
            cluster.add_query(42, empty).unwrap(),
            AddOutcome::RejectedClassD,
            "an effectively-empty query must reject"
        );
    }
    assert_eq!(cluster.class_counts().unwrap()[3], 0, "no class-D stored");

    // Query 1 matches its title before the bad upsert.
    let before: HashSet<u64> = cluster
        .percolate("1996 skybox premium")
        .unwrap()
        .into_iter()
        .collect();
    assert!(
        before.contains(&1),
        "query 1 should match before the upsert"
    );

    // Upserting query 1 to an EMPTY DSL is rejected WITHOUT tombstoning the live version.
    let (removed, outcome) = cluster.upsert_query(1, "   ", 1).unwrap();
    assert_eq!(outcome, AddOutcome::RejectedClassD);
    assert_eq!(
        removed, 0,
        "a rejected empty upsert must not tombstone the prior version"
    );
    let after: HashSet<u64> = cluster
        .percolate("1996 skybox premium")
        .unwrap()
        .into_iter()
        .collect();
    assert!(
        after.contains(&1),
        "query 1 was lost by a rejected empty upsert — silent data loss"
    );
}

/// With the broad lane excluded an always-candidate is invisible — the same documented
/// quarantine semantics as class C (the lane it rides), now across the cluster.
#[test]
fn cluster_class_d_quarantined_with_broad_off() {
    let (queries, titles) = mixed_corpus(0xD0FF_C1D5, 6_000, 200);
    let mut cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    cfg.per_shard.accept_class_d = true;
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    for title in &titles {
        let got = cluster.percolate_with_broad(title, false).unwrap();
        assert!(
            got.iter().all(|&id| id < CLASS_D_ID_BASE),
            "a class-D query matched with broad off on {title:?}"
        );
    }
}

/// The replicate-to-all distinction (ADR-080), encoded as a test: the broad lane's
/// STORAGE fan-out is N (class C + class D on every shard, so their summed counts are
/// multiples of K), while the per-title READ fan-out stays bounded — and the shard-0
/// always-probe hotspot is gone (some title's fan-out no longer includes shard 0).
#[test]
fn broad_lane_replicated_to_all_shards_but_read_fanout_bounded() {
    let (queries, titles) = mixed_corpus(0xB20A_DC1D, 8_000, 100);
    let k = 16usize;
    let mut cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    cfg.per_shard.accept_class_d = true;
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    // Storage fan-out = N: every class-C broad and every class-D query is on all K shards,
    // so their summed per-shard counts are positive multiples of K.
    let cc = cluster.class_counts().unwrap();
    assert!(
        cc[3] > 0 && cc[3].is_multiple_of(k as u64),
        "class-D on every shard: {}",
        cc[3]
    );
    assert!(
        cc[2] > 0 && cc[2].is_multiple_of(k as u64),
        "class-C broad on every shard: {}",
        cc[2]
    );
    assert_eq!(cluster.shard_query_counts().unwrap().len(), k);

    // Read fan-out stays bounded — broad evaluates on ONE shard, never all N.
    let max_fanout = titles
        .iter()
        .map(|t| cluster.shard_fanout(t).len())
        .max()
        .unwrap();
    assert!(
        max_fanout <= 8,
        "read fan-out blew up: {max_fanout} of {k} shards"
    );

    // The shard-0 hotspot is gone: pre-ADR-080 every title unconditionally probed shard 0;
    // now the broad-eval shard rotates, so at least one title's fan-out omits shard 0.
    assert!(
        titles.iter().any(|t| !cluster.shard_fanout(t).contains(&0)),
        "every title still probes shard 0 — the broad hotspot was not removed"
    );
}
