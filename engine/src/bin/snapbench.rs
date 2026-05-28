//! Snapshot-publish microbenchmark (P1-16).
//!
//! Measures the cost of `Engine::snapshot()` — the operation the server runs
//! after every write to publish a new lock-free read view (ADR-016). The audit
//! (P1-16) flagged that this deep-clones the entire engine on every write,
//! making writes O(total engine size) rather than O(delta).
//!
//! Usage: snapbench [num_queries] [iters]
//!
//! Reports:
//!   - time per bare `snapshot()` call (the publish cost)
//!   - time per PUT + publish cycle (insert_live + snapshot)
//!   - time per DELETE + publish cycle
//!   - time per bulk(1k) + publish cycle
//!
//! Build a large sealed engine first (build_from_queries seals into a base
//! segment), so `snapshot()` must reckon with the full corpus — exactly the
//! server's steady state.

use percolator::gen::{generate, GenConfig};
use percolator::segment::Engine;
use percolator::Normalizer;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_queries = arg_usize(&args, 1, 1_000_000);
    let iters = arg_usize(&args, 2, 200);

    let cfg = GenConfig {
        num_queries,
        num_titles: 1_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xC0FFEE,
        num_players: (num_queries / 40).max(2_000),
        num_sets: (num_queries / 100).max(1_000),
    };

    eprintln!("[gen] queries={}", num_queries);
    let data = generate(&cfg);

    let norm = Normalizer::default_vocab().expect("built-in vocab");
    let mut eng = Engine::new(norm);
    let tb = Instant::now();
    eng.build_from_queries(&data.queries);
    eprintln!(
        "[build] {} queries sealed into base segment(s) in {:.2}s",
        eng.num_queries(),
        tb.elapsed().as_secs_f64()
    );

    println!("================ SNAPSHOT PUBLISH COST ================");
    println!("corpus              : {} queries", eng.num_queries());
    println!("base segments       : {}", eng.num_segments() - 1);
    println!("dict features       : {}", eng.dict_len());

    // ---- bare snapshot() (the publish) ----
    // warmup
    for _ in 0..5 {
        std::hint::black_box(eng.snapshot());
    }
    let t = Instant::now();
    for _ in 0..iters {
        std::hint::black_box(eng.snapshot());
    }
    let per_snap = t.elapsed().as_secs_f64() / iters as f64;
    println!(
        "snapshot()          : {:.3} ms/call   ({:.0} publishes/sec)",
        per_snap * 1e3,
        1.0 / per_snap
    );

    // ---- PUT + publish (the audit's "single PUT copies the whole engine") ----
    let t = Instant::now();
    for i in 0..iters {
        let logical = 10_000_000 + i as u64;
        eng.insert_live("1994 upper deck michael jordan sp psa 10 -auto", logical, 1);
        std::hint::black_box(eng.snapshot());
    }
    let per_put = t.elapsed().as_secs_f64() / iters as f64;
    println!(
        "PUT + publish       : {:.3} ms/op    ({:.0} writes/sec)",
        per_put * 1e3,
        1.0 / per_put
    );

    // ---- DELETE + publish ----
    let t = Instant::now();
    for i in 0..iters {
        let logical = 10_000_000 + i as u64; // delete the ones we just inserted
        let _ = eng.delete_by_logical_id(logical);
        std::hint::black_box(eng.snapshot());
    }
    let per_del = t.elapsed().as_secs_f64() / iters as f64;
    println!(
        "DELETE + publish    : {:.3} ms/op    ({:.0} writes/sec)",
        per_del * 1e3,
        1.0 / per_del
    );

    // ---- bulk(1k) + publish ----
    let bulk_n = 1_000usize;
    let t = Instant::now();
    let bulk_iters = iters.min(20).max(1);
    for b in 0..bulk_iters {
        let batch: Vec<(u64, String)> = (0..bulk_n)
            .map(|j| {
                let logical = 20_000_000 + (b * bulk_n + j) as u64;
                (logical, "1994 upper deck michael jordan sp psa 10".to_string())
            })
            .collect();
        eng.bulk_ingest(&batch);
        std::hint::black_box(eng.snapshot());
    }
    let per_bulk = t.elapsed().as_secs_f64() / bulk_iters as f64;
    println!(
        "bulk(1k) + publish  : {:.3} ms/op    ({:.0} queries/sec)",
        per_bulk * 1e3,
        bulk_n as f64 / per_bulk
    );

    println!("======================================================");
    println!(
        "ideal: snapshot()/PUT/DELETE publish should be ~independent of corpus size\n\
         (O(delta), not O(total)). Re-run at multiple --num_queries to see scaling."
    );
}

fn arg_usize(a: &[String], i: usize, d: usize) -> usize {
    a.get(i).and_then(|x| x.parse().ok()).unwrap_or(d)
}
