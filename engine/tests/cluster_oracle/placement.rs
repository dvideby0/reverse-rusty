//! Placement by cost class, multi-shard any-of placement, content-routed fan-out (not scatter),
//! and the `ingest()`-on-a-populated-cluster guard.

use crate::harness::*;
use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::gen::{generate, GenConfig};

#[test]
fn placement_by_cost_class() {
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    let mut id = 9_000_000u64;
    let mut next = || {
        id += 1;
        id
    };

    // class A: a rare anchor -> exactly one selective shard.
    match cluster
        .add_query(next(), "1994 upper deck rareplayer0")
        .unwrap()
    {
        AddOutcome::Placed { shards } => {
            assert_eq!(shards.len(), 1, "class A should hit exactly one shard");
            assert!(shards[0] < 8);
        }
        other => panic!("class A expected Placed, got {other:?}"),
    }

    // class B arity-2: all-hot required, no rare anchor -> replicated lane.
    assert_eq!(
        cluster.add_query(next(), "1994 upper deck").unwrap(),
        AddOutcome::Replicated,
        "all-hot {{year}} {{brand}} should be class-B arity-2 -> replicated lane"
    );

    // class C: a single hot anchor (broad) -> replicated lane.
    assert_eq!(
        cluster.add_query(next(), "rookie").unwrap(),
        AddOutcome::Replicated,
        "broad single-hot anchor should be replicated"
    );

    // class B any-of: pure any-of of two rare players -> selective (1..=2 shards).
    match cluster
        .add_query(next(), "(rareplayer0,rareplayer1000)")
        .unwrap()
    {
        AddOutcome::Placed { shards } => {
            assert!(
                (1..=2).contains(&shards.len()),
                "any-of of two members places on 1..=2 shards, got {shards:?}"
            );
        }
        other => panic!("any-of expected Placed, got {other:?}"),
    }

    // a malformed query is surfaced, not silently dropped.
    assert!(
        matches!(
            cluster.add_query(next(), "(((").unwrap(),
            AddOutcome::RejectedParse(_)
        ),
        "malformed DSL should be RejectedParse"
    );
}

#[test]
fn anyof_query_can_place_on_multiple_shards() {
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 16,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    // Over many distinct rare-player pairs on a 16-shard ring, at least one
    // any-of query must straddle two shards — the multi-shard placement case.
    let mut id = 8_000_000u64;
    let mut saw_two = false;
    for i in 0..150u64 {
        id += 1;
        if let AddOutcome::Placed { shards } = cluster
            .add_query(id, &format!("(rareplayer{i},rareplayer{})", i + 1000))
            .unwrap()
        {
            if shards.len() == 2 {
                saw_two = true;
                break;
            }
        }
    }
    assert!(
        saw_two,
        "expected at least one any-of query to place on two distinct shards"
    );
}

#[test]
fn fan_out_is_content_routed_not_scatter() {
    let (queries, titles) = build_corpus();
    let k = 16usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let mut max_fanout = 0usize;
    let mut saw_multi = false;
    for title in &titles {
        let f = cluster.shard_fanout(title).len();
        max_fanout = max_fanout.max(f);
        if f >= 2 {
            saw_multi = true;
        }
    }
    // Content routing, not scatter-gather: even on 16 shards a title touches only
    // a handful (its rare features + the replicated lane), never all N.
    assert!(saw_multi, "expected some title to fan out to >1 shard");
    assert!(
        max_fanout <= 8,
        "fan-out {max_fanout} on {k} shards is too high — routing is not content-routed"
    );
}

/// `ingest()` must refuse a non-empty cluster: it re-indexes from scratch, so calling it
/// on an already-populated cluster would silently duplicate entries (the ADR-029 footgun).
/// It returns `ShardError::Config` instead. (The happy path — ingest into a freshly
/// connected empty cluster — is covered by `cluster_grpc_oracle.rs`.)
#[test]
fn ingest_on_a_populated_cluster_is_rejected() {
    let data = generate(&GenConfig {
        num_queries: 500,
        num_titles: 1,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x1234_5678,
        num_players: 200,
        num_sets: 100,
    });
    let cfg = ClusterConfig {
        num_shards: 3,
        ..ClusterConfig::default()
    };
    // build() loads the corpus, so the cluster is already populated.
    let cluster = ClusterEngine::build(vocab(), &cfg, &data.queries).expect("build cluster");
    assert!(
        cluster.num_queries().unwrap() > 0,
        "corpus should populate the cluster"
    );
    assert!(
        matches!(
            cluster.ingest(&data.queries),
            Err(reverse_rusty::cluster::ShardError::Config(_))
        ),
        "ingest() on a populated cluster must error, not silently duplicate"
    );
}
