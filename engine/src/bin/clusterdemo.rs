//! Cluster demo: build an in-process multi-shard `ClusterEngine` over a realistic
//! synthetic corpus, show how each cost class is PLACED (consistent-hash ring vs
//! the replicated lane), then percolate a few titles showing the content-routed
//! fan-out (~a handful of shards, not all N) and the merged match set.
//!
//! Run: cargo run --release --bin clusterdemo

use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;

fn main() {
    let norm = Normalizer::default_vocab().expect("default vocabulary");

    // A realistic corpus so hotness is meaningful: years/brands/variants recur
    // (hot, never sole anchors), entities/collections are rare (the features
    // we shard on).
    let cfg = GenConfig {
        num_queries: 3_000,
        num_titles: 0,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x00C0_FFEE,
        num_entities: 800,
        num_collections: 400,
    };
    let mut queries = generate(&cfg).queries;

    // Seed three rare "demo" entities so the illustrative calls below resolve
    // against the frozen dict (any real rare entity would work too).
    let mut id = 9_000_000u64;
    for s in [
        "1994 north star demoentity1",
        "2003 acme demoentity2 new",
        "1995 vertex demoentity3 limited",
    ] {
        queries.push((id, s.to_string()));
        id += 1;
    }

    let ccfg = ClusterConfig {
        num_shards: 4,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(norm, &ccfg, &queries).expect("build cluster");

    let cc = cluster.class_counts().expect("class_counts (in-process)");
    let total = cluster.num_queries().expect("num_queries (in-process)");
    let per_shard = cluster
        .shard_query_counts()
        .expect("shard_query_counts (in-process)");
    println!("===== CLUSTER: {} shards =====", cluster.num_shards());
    println!(
        "indexed {total} physical entries (A={} B={} C={}); per-shard counts {per_shard:?}",
        cc[0], cc[1], cc[2],
    );
    println!("(class C + class-B-arity-2 concentrate on shard 0 — the replicated lane)");

    // ---- placement by cost class ----
    println!("\n===== PLACEMENT (add_query → where it lands) =====");
    let examples = [
        ("class A  (rare anchor)     ", "1994 north star demoentity1"),
        ("class B  (any-of, rare)    ", "(demoentity2,demoentity3)"),
        ("class B  (arity-2, all hot)", "1994 north star"),
        ("class C  (broad, hot only) ", "standard"),
    ];
    for (label, dsl) in examples {
        id += 1;
        let outcome = cluster.add_query(id, dsl).expect("add_query (in-process)");
        let where_ = match &outcome {
            AddOutcome::Placed { shards } => format!("selective shard(s) {shards:?}"),
            AddOutcome::Replicated => "replicated lane (shard 0)".to_string(),
            AddOutcome::RejectedClassD => "rejected (class D)".to_string(),
            AddOutcome::RejectedParse(e) => format!("parse error: {e}"),
        };
        println!("  {label}  {dsl:<32?} -> {where_}");
    }

    // ---- routing + merge ----
    println!("\n===== PERCOLATE (route → probe shards → union) =====");
    let titles = [
        "1994 north star demoentity1 pro",
        "2003 acme demoentity2 new pro",
        "1994 north star demoentity2 demoentity3 limited", // multi-entity → wider fan-out
    ];
    for t in titles {
        let fanout = cluster.shard_fanout(t);
        let (ids, stats) = cluster
            .percolate_with_stats(t)
            .expect("percolate (in-process)");
        println!("  title {t:?}");
        println!(
            "    routed to shards {:?}  (fan-out {}/{}),  matched {} queries,  candidates examined {}",
            fanout,
            fanout.len(),
            cluster.num_shards(),
            ids.len(),
            stats.unique_candidates,
        );
    }
}
