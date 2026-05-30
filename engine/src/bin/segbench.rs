//! Read-amplification benchmark for the LSM multi-segment layout.
//!
//! Builds the SAME corpus as K = 1, 2, 4, 8 segments (the query set is split
//! into K equal bulk-ingested base segments) and measures, per K:
//!   * avg candidates examined / title  (distinct queries exact-checked)
//!   * avg postings scanned / title      (posting entries unioned)
//!   * titles/sec/core                   (single-thread match throughput)
//!
//! Because a title must probe every segment, read work scales ~linearly with
//! segment count — this is the read-amplification cost that compaction repays.
//!
//! Usage: segbench [num_queries] [num_titles] [broad_frac] [seed]
//! Defaults: 300k queries, 3k titles, broad_frac 0.0 (isolates the selective
//! path), seed 0xC0FFEE. Bounded to run in well under 40s.

use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::Normalizer;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_queries = arg_usize(&args, 1, 300_000);
    let num_titles = arg_usize(&args, 2, 3_000);
    let broad_frac = arg_f64(&args, 3, 0.0);
    let seed = arg_u64(&args, 4, 0x00C0_FFEE);

    let cfg = GenConfig {
        num_queries,
        num_titles,
        broad_query_frac: broad_frac,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: (num_queries / 40).max(2_000),
        num_sets: (num_queries / 100).max(1_000),
    };

    eprintln!("[gen] queries={num_queries} titles={num_titles} broad_frac={broad_frac}");
    let data = generate(&cfg);
    eprintln!(
        "[gen] done; {} queries, {} titles",
        data.queries.len(),
        data.titles.len()
    );

    // include_broad = false to isolate the selective path (broad_frac is 0.0).
    let include_broad = broad_frac > 0.0;

    println!("========================================= READ AMPLIFICATION ==========================================");
    println!(
        "{:>9} | {:>14} | {:>14} | {:>14} | {:>10} | {:>14}",
        "segments", "cands/title", "posts/title", "titles/sec", "skip %", "filter MB"
    );
    println!("{}", "-".repeat(102));

    for &k in &[1usize, 2, 4, 8] {
        let eng = build_k_segments(&data.queries, k);
        let stats = measure(&eng, &data.titles, include_broad);
        let filter_mb = eng.filter_bytes() as f64 / 1e6;
        println!(
            "{:>9} | {:>14.2} | {:>14.2} | {:>14.0} | {:>9.1}% | {:>14.2}",
            k, stats.cand_per, stats.post_per, stats.tps, stats.skip_pct, filter_mb
        );
    }
    println!("{}", "-".repeat(102));
    println!("(anchor filters skip probes that would miss; skip% rises with segment count)");
}

/// Build the corpus as exactly K immutable base segments via bulk_ingest.
fn build_k_segments(queries: &[(u64, String)], k: usize) -> Engine {
    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let total = queries.len();
    let per = total.div_ceil(k);

    // First chunk goes through build_from_queries (finalizes the mask); the rest
    // are bulk_ingested into their own base segments. This yields exactly K base
    // segments while keeping the dictionary/mask shared and finalized once.
    let mut start = 0usize;
    let mut built_first = false;
    while start < total {
        let end = (start + per).min(total);
        let chunk = &queries[start..end];
        if built_first {
            eng.bulk_ingest(chunk);
        } else {
            eng.build_from_queries(chunk);
            built_first = true;
        }
        start = end;
    }
    eng
}

struct MeasureResult {
    cand_per: f64,
    post_per: f64,
    tps: f64,
    skip_pct: f64,
}

/// Measure avg candidates/title, avg postings/title, titles/sec/core, and
/// filter skip percentage.
fn measure(eng: &Engine, titles: &[String], include_broad: bool) -> MeasureResult {
    let mut scratch = MatchScratch::new();
    let mut out: Vec<u64> = Vec::new();

    // warmup
    for t in titles.iter().take(500) {
        eng.match_title(t, &mut scratch, &mut out, include_broad);
    }

    // stats pass
    let mut sum_cand: u64 = 0;
    let mut sum_post: u64 = 0;
    let mut sum_probes: u64 = 0;
    let mut sum_skipped: u64 = 0;
    for t in titles {
        let st = eng.match_title(t, &mut scratch, &mut out, include_broad);
        sum_cand += u64::from(st.unique_candidates);
        sum_post += u64::from(st.postings_scanned);
        sum_probes += u64::from(st.probes_attempted);
        sum_skipped += u64::from(st.probes_skipped);
    }
    let n = titles.len() as f64;
    let skip_pct = if sum_probes > 0 {
        sum_skipped as f64 / sum_probes as f64 * 100.0
    } else {
        0.0
    };

    // throughput pass (whole-batch timer). Repeat to ~500k title-matches.
    let reps = (500_000 / titles.len().max(1)).max(1);
    let t0 = Instant::now();
    for _ in 0..reps {
        for t in titles {
            eng.match_title(t, &mut scratch, &mut out, include_broad);
        }
    }
    let secs = t0.elapsed().as_secs_f64();
    let tps = (reps * titles.len()) as f64 / secs;

    MeasureResult {
        cand_per: sum_cand as f64 / n,
        post_per: sum_post as f64 / n,
        tps,
        skip_pct,
    }
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
