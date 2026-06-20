//! Cluster fan-out benchmark.
//!
//! Usage: clusterbench [num_queries] [num_titles] [num_shards] [broad_frac] [seed]
//!
//! Builds an in-process multi-shard `ClusterEngine` over a seeded corpus and reports the
//! *structural* cluster metrics that are fixed by the data + the ring (NOT the CPU, so they
//! reproduce on any machine): shards-probed-per-title (avg/p50/p95/p99/max), candidates per
//! title and the broad-lane share, and a fan-out-vs-K sweep showing content routing stays at
//! a bounded ~2–5 shard fan-out instead of broadcasting to all N. The single throughput line
//! is the one machine-DEPENDENT number and is labelled as such.
//!
//! Companion to `clusterdemo` (which illustrates placement + routing on a few titles) and to
//! `bench` (the single-node throughput/structure harness). Invariants + capture log live in
//! `docs/performance/benchmark-results.txt` (the CLUSTER section).

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::Normalizer;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_queries = arg_usize(&args, 1, 100_000);
    let num_titles = arg_usize(&args, 2, 5_000);
    let num_shards = arg_usize(&args, 3, 8);
    let broad_frac = arg_f64(&args, 4, 0.05);
    let seed = arg_u64(&args, 5, 0x00C0_FFEE);

    let cfg = GenConfig {
        num_queries,
        num_titles,
        broad_query_frac: broad_frac,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        // Scale the entity space with the corpus so selectivity stays realistic (mirrors
        // `bench`): a fixed tiny space would artificially saturate fan-out at high K.
        num_players: (num_queries / 40).max(2_000),
        num_sets: (num_queries / 100).max(1_000),
    };

    eprintln!(
        "[gen] queries={num_queries} titles={num_titles} shards={num_shards} broad_frac={broad_frac}"
    );
    let t0 = Instant::now();
    let data = generate(&cfg);
    eprintln!("[gen] done in {:.2}s", t0.elapsed().as_secs_f64());

    // ---- build the headline cluster (at the requested K) ----
    let ccfg = ClusterConfig {
        num_shards,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let tb = Instant::now();
    let cluster = ClusterEngine::build(vocab(), &ccfg, &data.queries).expect("build cluster");
    let build_s = tb.elapsed().as_secs_f64();

    let cc = cluster.class_counts().expect("class_counts");
    let total = cluster.num_queries().expect("num_queries");
    let per_shard = cluster.shard_query_counts().expect("shard_query_counts");
    let (min_s, max_s) = per_shard
        .iter()
        .fold((usize::MAX, 0usize), |(lo, hi), &c| (lo.min(c), hi.max(c)));
    println!("================ CLUSTER BUILD (K={num_shards}) ================");
    println!(
        "indexed entries     : {total}  (A={} B={} C={})",
        cc[0], cc[1], cc[2]
    );
    println!(
        "per-shard counts    : min={min_s} max={max_s}  (broad lane replicated to every shard, \
         ADR-080; eval-shard rotates per title — counts even, no shard-0 hotspot)"
    );
    println!(
        "build time          : {build_s:.2}s  ({:.0} queries/sec)",
        total as f64 / build_s.max(1e-9)
    );

    // ---- fan-out + candidate structure at the headline K ----
    let mut fanout: Vec<u32> = Vec::with_capacity(data.titles.len());
    let mut cand: Vec<u32> = Vec::with_capacity(data.titles.len());
    let mut sum_fanout: u64 = 0;
    let mut sum_cand: u64 = 0;
    let mut sum_broad: u64 = 0;
    let mut sum_matches: u64 = 0;
    for t in &data.titles {
        let f = cluster.shard_fanout(t).len() as u32;
        let (_ids, st) = cluster.percolate_with_stats(t).expect("percolate");
        fanout.push(f);
        cand.push(st.unique_candidates);
        sum_fanout += u64::from(f);
        sum_cand += u64::from(st.unique_candidates);
        sum_broad += u64::from(st.broad_candidates);
        sum_matches += u64::from(st.matches);
    }
    let n = data.titles.len().max(1) as f64;

    println!("================ FAN-OUT (shards probed / title) ================");
    println!(
        "avg {:.2}  p50 {}  p95 {}  p99 {}  max {}   (of {num_shards} shards)",
        sum_fanout as f64 / n,
        pct(&mut fanout.clone(), 0.50),
        pct(&mut fanout.clone(), 0.95),
        pct(&mut fanout.clone(), 0.99),
        fanout.iter().copied().max().unwrap_or(0),
    );
    println!("(content routing touches a handful of shards — one broad-eval shard + each rare anchor's owner — never all N)");

    println!("================ CANDIDATES / TITLE ================");
    println!(
        "avg unique cand/title : {:.2}   (p95={}, p99={}, max={})",
        sum_cand as f64 / n,
        pct(&mut cand.clone(), 0.95),
        pct(&mut cand.clone(), 0.99),
        cand.iter().copied().max().unwrap_or(0),
    );
    println!(
        "  of which broad lane : {:.2} avg   ({:.1}% of candidates)",
        sum_broad as f64 / n,
        if sum_cand > 0 {
            sum_broad as f64 / sum_cand as f64 * 100.0
        } else {
            0.0
        }
    );
    println!("avg matches/title     : {:.3}", sum_matches as f64 / n);

    // ---- fan-out scaling: same corpus, varying K (the machine-independent invariant) ----
    // Fan-out is bounded by a title's distinct rare-anchor count (+ its one broad-eval shard), so it saturates
    // at ~2–5 regardless of K once anchors land on distinct shards — it does NOT grow toward N.
    println!("================ FAN-OUT SCALING (same corpus, varying K) ================");
    println!(
        "   {:>4}  {:>11}  {:>11}  {:>10}  {:>12}",
        "K", "avg_fanout", "p99_fanout", "avg_cand", "broad_share"
    );
    for &k in &[1usize, 2, 4, 8, 16, 32] {
        if k > num_queries {
            break;
        }
        let kcfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let kc =
            ClusterEngine::build(vocab(), &kcfg, &data.queries).expect("build cluster (sweep)");
        let mut kfan: Vec<u32> = Vec::with_capacity(data.titles.len());
        let mut ksum_fan: u64 = 0;
        let mut ksum_cand: u64 = 0;
        let mut ksum_broad: u64 = 0;
        for t in &data.titles {
            let f = kc.shard_fanout(t).len() as u32;
            let (_ids, st) = kc.percolate_with_stats(t).expect("percolate (sweep)");
            kfan.push(f);
            ksum_fan += u64::from(f);
            ksum_cand += u64::from(st.unique_candidates);
            ksum_broad += u64::from(st.broad_candidates);
        }
        println!(
            "   {:>4}  {:>11.2}  {:>11}  {:>10.2}  {:>11.1}%",
            k,
            ksum_fan as f64 / n,
            pct(&mut kfan, 0.99),
            ksum_cand as f64 / n,
            if ksum_cand > 0 {
                ksum_broad as f64 / ksum_cand as f64 * 100.0
            } else {
                0.0
            }
        );
    }

    // ---- end-to-end percolate throughput (the one MACHINE-DEPENDENT number) ----
    for t in data.titles.iter().take(500) {
        let _ = cluster.percolate(t); // warmup
    }
    let tp = Instant::now();
    for t in &data.titles {
        let _ = cluster
            .percolate_with_broad(t, false)
            .expect("percolate (sel)");
    }
    let sel_s = tp.elapsed().as_secs_f64();
    let tpb = Instant::now();
    for t in &data.titles {
        let _ = cluster.percolate(t).expect("percolate (broad)");
    }
    let broad_s = tpb.elapsed().as_secs_f64();
    println!("================ THROUGHPUT (machine-DEPENDENT; not an invariant) ================");
    println!(
        "selective : {:.0} titles/sec   |   with broad : {:.0} titles/sec   (K={num_shards}, single driver thread; per-title fan-out parallelised internally)",
        data.titles.len() as f64 / sel_s.max(1e-9),
        data.titles.len() as f64 / broad_s.max(1e-9),
    );
}

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

fn pct(v: &mut [u32], q: f64) -> u32 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    let idx = ((v.len() as f64 - 1.0) * q).round() as usize;
    v[idx]
}

fn arg_usize(a: &[String], i: usize, d: usize) -> usize {
    a.get(i).and_then(|x| x.parse().ok()).unwrap_or(d)
}
fn arg_f64(a: &[String], i: usize, d: f64) -> f64 {
    a.get(i).and_then(|x| x.parse().ok()).unwrap_or(d)
}
fn arg_u64(a: &[String], i: usize, d: u64) -> u64 {
    a.get(i).and_then(|x| x.parse().ok()).unwrap_or(d)
}
