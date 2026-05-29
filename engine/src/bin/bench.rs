//! Benchmark harness.
//!
//! Usage: bench [num_queries] [num_titles] [broad_frac] [skew] [seed]
//!
//! Reports build throughput, match throughput (titles/sec/core), candidate
//! counts (avg/p95/p99), exact-check counts, memory, cost-class distribution,
//! and a live-update micro-benchmark. Designed to push scale and fall back
//! gracefully (it just uses whatever N you give it).

use percolator::gen::{generate, GenConfig};
use percolator::segment::{Engine, MatchScratch};
use percolator::Normalizer;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_queries = arg_usize(&args, 1, 1_000_000);
    let num_titles = arg_usize(&args, 2, 20_000);
    let broad_frac = arg_f64(&args, 3, 0.05);
    let skew = arg_f64(&args, 4, 2.0);
    let seed = arg_u64(&args, 5, 0x00C0_FFEE);

    let cfg = GenConfig {
        num_queries,
        num_titles,
        broad_query_frac: broad_frac,
        hot_skew: skew,
        family_size: 8,
        seed,
        // scale the entity space with the query population so selectivity stays
        // realistic (real marketplaces add entities as listings/queries grow);
        // a fixed tiny space would artificially saturate at high query counts.
        num_players: (num_queries / 40).max(2_000),
        num_sets: (num_queries / 100).max(1_000),
    };

    eprintln!(
        "[gen] queries={num_queries} titles={num_titles} broad_frac={broad_frac} skew={skew}"
    );
    let t0 = Instant::now();
    let data = generate(&cfg);
    eprintln!("[gen] done in {:.2}s", t0.elapsed().as_secs_f64());

    // ---- build ----
    let norm = Normalizer::default_vocab().expect("built-in vocab");
    let mut eng = Engine::new(norm);
    let tb = Instant::now();
    eng.build_from_queries(&data.queries);
    let build_s = tb.elapsed().as_secs_f64();

    let cc = eng.class_counts();
    println!("================ BUILD ================");
    println!("indexed queries     : {}", eng.num_queries());
    println!(
        "cost classes        : A(selective)={}  B(arity-2/anyof)={}  C(broad)={}  D(rejected)={}",
        cc[0], cc[1], cc[2], cc[3]
    );
    println!("dict features       : {}", eng.dict_len());
    println!(
        "main signatures     : {}",
        eng.main_index().num_signatures()
    );
    println!(
        "main max posting len: {}   (#postings>1024: {})",
        eng.main_index().max_posting_len(),
        eng.main_index().count_over(1024)
    );
    println!(
        "build time          : {:.2}s  ({:.0} queries/sec)",
        build_s,
        eng.num_queries() as f64 / build_s
    );

    // ---- memory ----
    let exact_mb = eng.exact_bytes() as f64 / 1e6;
    let main_mb = eng.main_bytes() as f64 / 1e6;
    let broad_mb = eng.broad_bytes() as f64 / 1e6;
    let filter_mb = eng.filter_bytes() as f64 / 1e6;
    let rss_mb = read_rss_mb();
    println!("================ MEMORY ===============");
    println!("exact SoA heap      : {exact_mb:.1} MB");
    println!("main index postings : {main_mb:.1} MB");
    println!("broad index postings: {broad_mb:.1} MB");
    println!("anchor filters      : {filter_mb:.1} MB");
    println!("process RSS         : {rss_mb:.1} MB");
    if eng.num_queries() > 0 {
        println!(
            "RSS per query       : {:.1} bytes",
            rss_mb * 1e6 / eng.num_queries() as f64
        );
    }

    // ---- match throughput (warm), single thread ----
    let mut scratch = MatchScratch::new();
    let mut out: Vec<u64> = Vec::new();

    // warmup
    for t in data.titles.iter().take(1000) {
        eng.match_title(t, &mut scratch, &mut out, true);
    }

    // throughput pass (whole-batch timer = accurate). Run twice:
    //  (1) selective main lane only (broad queries quarantined -> batched offline)
    //  (2) with the broad lane evaluated inline per title (naive; shows its cost)
    let reps = arg_usize(&args, 6, (500_000 / num_titles.max(1)).max(1)); // ~total title-matches

    let tm1 = Instant::now();
    for _ in 0..reps {
        for t in &data.titles {
            eng.match_title(t, &mut scratch, &mut out, false);
        }
    }
    let sel_s = tm1.elapsed().as_secs_f64();
    let total_titles = reps * data.titles.len();
    let per_core = total_titles as f64 / sel_s; // headline = selective lane

    let mut total_matches: u64 = 0;
    let tm = Instant::now();
    for _ in 0..reps {
        for t in &data.titles {
            let st = eng.match_title(t, &mut scratch, &mut out, true);
            total_matches += u64::from(st.matches);
        }
    }
    let match_s = tm.elapsed().as_secs_f64();
    let per_core_broad = total_titles as f64 / match_s; // with broad lane inline

    // per-title stats + latency percentiles (one measured pass)
    let mut cand = Vec::with_capacity(data.titles.len());
    let mut broadc = Vec::with_capacity(data.titles.len());
    let mut posts = Vec::with_capacity(data.titles.len());
    let mut lat_ns = Vec::with_capacity(data.titles.len());
    let mut sum_unique: u64 = 0;
    let mut sum_broad: u64 = 0;
    let mut sum_posts: u64 = 0;
    let mut sum_matches: u64 = 0;
    for t in &data.titles {
        let s0 = Instant::now();
        let st = eng.match_title(t, &mut scratch, &mut out, true);
        let dt = s0.elapsed().as_nanos() as u64;
        lat_ns.push(dt);
        cand.push(st.unique_candidates);
        broadc.push(st.broad_candidates);
        posts.push(st.postings_scanned);
        sum_unique += u64::from(st.unique_candidates);
        sum_broad += u64::from(st.broad_candidates);
        sum_posts += u64::from(st.postings_scanned);
        sum_matches += u64::from(st.matches);
    }
    let n = data.titles.len() as f64;

    println!("================ MATCH ================");
    println!(
        "SELECTIVE lane only : {:.0} titles/sec/core   ({:.1}x the 2,778/s target on 1 core)",
        per_core,
        per_core / 2778.0
    );
    println!(
        "with broad inline   : {per_core_broad:.0} titles/sec/core   (naive; design batches the broad lane)"
    );
    println!(
        "est. 4-core (sel.)  : {:.0} titles/sec  ({:.2}B titles/hour)",
        per_core * 4.0,
        per_core * 4.0 * 3600.0 / 1e9
    );
    println!(
        "avg unique cand/title : {:.2}   (p95={}, p99={}, max={})",
        sum_unique as f64 / n,
        pct(&mut cand.clone(), 0.95),
        pct(&mut cand.clone(), 0.99),
        cand.iter().copied().max().unwrap_or(0)
    );
    println!(
        "  of which broad lane : {:.2} avg  (p99={})",
        sum_broad as f64 / n,
        pct(&mut broadc.clone(), 0.99)
    );
    println!(
        "avg postings scanned  : {:.2}   (p99={})",
        sum_posts as f64 / n,
        pct(&mut posts.clone(), 0.99)
    );
    println!("avg matches/title     : {:.3}", sum_matches as f64 / n);
    println!(
        "latency               : p50={}ns p95={}ns p99={}ns (incl. timer overhead)",
        pct_u64(&mut lat_ns.clone(), 0.50),
        pct_u64(&mut lat_ns.clone(), 0.95),
        pct_u64(&mut lat_ns.clone(), 0.99)
    );
    println!("(sanity) total matches over throughput pass: {total_matches}");

    // ---- parallel match throughput ----
    let ncpu = rayon::current_num_threads();
    // warmup
    let _ = eng.match_titles_par_stats(&data.titles, true);
    let tp = Instant::now();
    for _ in 0..reps {
        let _ = eng.match_titles_par_stats(&data.titles, false);
    }
    let par_sel_s = tp.elapsed().as_secs_f64();
    let par_sel = total_titles as f64 / par_sel_s;

    let tp2 = Instant::now();
    for _ in 0..reps {
        let _ = eng.match_titles_par_stats(&data.titles, true);
    }
    let par_broad_s = tp2.elapsed().as_secs_f64();
    let par_broad = total_titles as f64 / par_broad_s;

    println!("================ PARALLEL MATCH ({ncpu} threads) ====");
    println!(
        "SELECTIVE lane only : {:.0} titles/sec total   ({:.0}/thread, {:.1}x single-thread)",
        par_sel,
        par_sel / ncpu as f64,
        par_sel / per_core
    );
    println!(
        "with broad inline   : {:.0} titles/sec total   ({:.1}x single-thread)",
        par_broad,
        par_broad / per_core_broad
    );

    // ---- live update micro-bench ----
    println!("================ UPDATES ==============");
    let n_upd = 50_000.min(num_queries / 4 + 1);
    let tu = Instant::now();
    let mut ver = 2u32;
    for i in 0..n_upd {
        // update = insert new version of an existing logical id + tombstone old
        let logical = (i as u64) % (eng.num_queries() as u64).max(1);
        if let Some(old) = eng.insert_live(
            "1994 upper deck michael jordan sp psa 10 -auto",
            logical,
            ver,
        ) {
            // tombstone the freshly inserted? No: tombstone the *previous* local id.
            // Here we just tombstone an arbitrary earlier id to exercise the path.
            let _ = eng.tombstone(old.saturating_sub(1));
        }
        ver = ver.wrapping_add(1);
    }
    let upd_s = tu.elapsed().as_secs_f64();
    println!(
        "live updates        : {} in {:.3}s  ({:.0} updates/sec)  visibility: immediate (epoch swap)",
        n_upd,
        upd_s,
        n_upd as f64 / upd_s
    );
    println!("queries after updates: {}", eng.num_queries());
}

fn pct(v: &mut [u32], q: f64) -> u32 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    let idx = ((v.len() as f64 - 1.0) * q).round() as usize;
    v[idx]
}
fn pct_u64(v: &mut [u64], q: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    let idx = ((v.len() as f64 - 1.0) * q).round() as usize;
    v[idx]
}

fn read_rss_mb() -> f64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb: f64 = rest
                    .split_whitespace()
                    .next()
                    .and_then(|x| x.parse().ok())
                    .unwrap_or(0.0);
                return kb / 1024.0;
            }
        }
    }
    0.0
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
