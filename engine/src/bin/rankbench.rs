//! Deterministic ranked-delivery baseline + bounded local capture (ADR-107/108).
//!
//! Measures the current collect-all/rank-after-match path and records stable
//! semantic checksums alongside informational timings. The generated corpus is
//! deliberately split into ordinary, broad-heavy, canonical-body-duplicate,
//! and multi-shard duplicate-placement workloads.

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::gen::{generate, Dataset, GenConfig};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::{Normalizer, QueryScope, RankProgramSpec, RankSpec, TopKOptions};
use serde::Serialize;
use std::time::{Duration, Instant};

const DEFAULT_QUERIES: usize = 20_000;
const DEFAULT_TITLES: usize = 500;
const DEFAULT_SHARDS: usize = 8;
const DEFAULT_SEED: u64 = 0x1070_0001;
const KS: [usize; 4] = [10, 100, 1_000, 10_000];

#[derive(Serialize)]
struct ScoredSource {
    id: u64,
    score: i64,
    source: Option<String>,
}

struct Capture {
    match_counts: Vec<usize>,
    logical_emissions: u64,
    duplicate_emissions: u64,
    id_bytes: usize,
    score_bytes: usize,
    source_bytes: usize,
    rank_time: Duration,
    checksum: u64,
}

struct ClusterCapture {
    logical_emissions: u64,
    duplicate_emissions: u64,
    fanouts: Vec<usize>,
    checksum: u64,
}

struct BoundedCapture {
    k: usize,
    retained: usize,
    encoded_bytes: usize,
    match_rank_time: Duration,
    evaluations: u64,
    replacements: u64,
    collector_bound_entries: usize,
    collector_payload_bytes: usize,
    shard_rows_received: usize,
    routed_shards: usize,
    shard_result_bytes: u64,
    collect_merge_time: Duration,
    fetch_bytes: usize,
    fetch_time: Duration,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_queries = arg_usize(&args, 1, DEFAULT_QUERIES);
    let num_titles = arg_usize(&args, 2, DEFAULT_TITLES);
    let shards = arg_usize(&args, 3, DEFAULT_SHARDS).max(1);
    let seed = arg_u64(&args, 4, DEFAULT_SEED);

    println!("Reverse Rusty ranked-delivery synthetic baseline (ADR-107/108/110)");
    println!(
        "host: os={} arch={} profile={} crate={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        env!("CARGO_PKG_VERSION")
    );
    println!(
        "config: queries={num_queries} titles={num_titles} shards={shards} seed=0x{seed:016x} K={KS:?}"
    );

    let workloads = [
        workload("ordinary", num_queries, num_titles, 0.0, seed, false, false),
        workload(
            "broad-heavy",
            num_queries,
            num_titles,
            0.25,
            seed ^ 0xB0AD,
            false,
            false,
        ),
        workload(
            "body-duplicate",
            num_queries,
            num_titles,
            0.10,
            seed ^ 0xD0D0,
            true,
            false,
        ),
        workload(
            "placement-duplicate",
            num_queries,
            num_titles,
            0.0,
            seed ^ 0xA11F,
            false,
            true,
        ),
    ];

    for (name, data) in &workloads {
        run_workload(name, data, shards);
    }
}

fn workload(
    name: &'static str,
    num_queries: usize,
    num_titles: usize,
    broad_frac: f64,
    seed: u64,
    duplicate_bodies: bool,
    duplicate_placement: bool,
) -> (&'static str, Dataset) {
    let base_queries = if duplicate_bodies {
        num_queries.saturating_mul(2) / 3
    } else if duplicate_placement {
        num_queries.saturating_sub((num_queries / 20).max(1))
    } else {
        num_queries
    };
    let mut data = generate(&GenConfig {
        num_queries: base_queries,
        num_titles,
        broad_query_frac: broad_frac,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: (num_queries / 40).max(2_000),
        num_sets: (num_queries / 100).max(1_000),
    });

    // The low-volume ordinary corpus must not become a degenerate all-zero
    // workload merely because independently generated queries/titles miss each
    // other at this small scale. Replace a tail of the corpus with exact-title
    // sentinels: still selective, but guaranteed to exercise ranked delivery.
    if name == "ordinary" {
        let planted = data.titles.len().min(data.queries.len());
        let start = data.queries.len() - planted;
        for (entry, title) in data.queries[start..].iter_mut().zip(&data.titles) {
            entry.1.clone_from(title);
        }
    }

    let mut next_id = data
        .queries
        .last()
        .map_or(0, |(id, _)| id.saturating_add(1));
    if duplicate_bodies && !data.queries.is_empty() {
        let bodies: Vec<String> = data
            .queries
            .iter()
            .take((num_queries - data.queries.len()).max(1))
            .map(|(_, q)| q.clone())
            .collect();
        while data.queries.len() < num_queries {
            let body = &bodies[(data.queries.len() - base_queries) % bodies.len()];
            data.queries.push((next_id, body.clone()));
            next_id = next_id.saturating_add(1);
        }
    }

    if duplicate_placement {
        let pairs = (num_queries / 20).max(1);
        for i in 0..pairs {
            data.queries
                .push((next_id, format!("(zzownerleft{i},zzownerright{i})")));
            if i < num_titles.min(pairs) {
                data.titles
                    .push(format!("zzownerleft{i} zzownerright{i} showcase"));
            }
            next_id = next_id.saturating_add(1);
        }
    }

    (name, data)
}

fn run_workload(name: &str, data: &Dataset, shards: usize) {
    let tags: Vec<Vec<(String, String)>> = data
        .queries
        .iter()
        .map(|(id, _)| {
            vec![
                ("priority".to_string(), (id % 10_000).to_string()),
                (
                    "tier".to_string(),
                    if id % 7 == 0 { "gold" } else { "standard" }.to_string(),
                ),
            ]
        })
        .collect();
    let rank = RankSpec {
        priority_key: Some("priority".to_string()),
        boosts: vec![("tier".to_string(), "gold".to_string(), 25_000)],
    };

    let build_started = Instant::now();
    let mut engine = Engine::new(Normalizer::default_vocab().expect("built-in normalizer"));
    engine
        .try_build_from_queries_with_tags(&data.queries, &tags)
        .expect("synthetic tagged build");
    let snap = engine.snapshot();
    let compiled = snap.compile_rank_spec(&rank);
    let capture = capture_local(&snap, &data.titles, &compiled);
    let program_spec = RankProgramSpec {
        priority_field: Some("priority".to_string()),
        boosts: rank.boosts.clone(),
    };
    let bounded_program = snap
        .compile_rank_program(&program_spec)
        .expect("fixed priority rank program");

    let cluster = ClusterEngine::build_with_tags(
        Normalizer::default_vocab().expect("built-in normalizer"),
        &ClusterConfig {
            num_shards: shards,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &data.queries,
        &tags,
    )
    .expect("synthetic tagged cluster build");
    let cluster_program = cluster
        .compile_rank_program(&program_spec)
        .expect("cluster priority rank program");
    let bounded: Vec<BoundedCapture> = KS
        .into_iter()
        .map(|k| {
            capture_bounded(
                &snap,
                &cluster,
                &data.titles,
                &compiled,
                &bounded_program,
                &cluster_program,
                k,
            )
        })
        .collect();
    let batch: Vec<BatchCapture> = [16usize, 64, 256]
        .into_iter()
        .flat_map(|batch_size| [10usize, 100].into_iter().map(move |k| (batch_size, k)))
        .map(|(batch_size, k)| {
            capture_batch(
                &snap,
                &cluster,
                &data.titles,
                &bounded_program,
                &cluster_program,
                k,
                batch_size,
            )
        })
        .collect();
    let cluster_capture = capture_cluster(&cluster, &data.titles);

    let mut counts = capture.match_counts.clone();
    counts.sort_unstable();
    let total_matches: usize = counts.iter().sum();
    println!("\n[{name}]");
    println!(
        "corpus: queries={} titles={} build_ms={:.3}",
        data.queries.len(),
        data.titles.len(),
        build_started.elapsed().as_secs_f64() * 1_000.0
    );
    println!(
        "matches/title: p50={} p95={} p99={} total={}",
        percentile(&counts, 50),
        percentile(&counts, 95),
        percentile(&counts, 99),
        total_matches
    );
    println!(
        "local delivery: emissions={} unique={} duplicates={} dedup_ratio={:.6}",
        capture.logical_emissions,
        total_matches,
        capture.duplicate_emissions,
        ratio(capture.duplicate_emissions, capture.logical_emissions)
    );
    println!(
        "encoded bytes: ids={} scores={} sources={} rank_ms={:.3}",
        capture.id_bytes,
        capture.score_bytes,
        capture.source_bytes,
        capture.rank_time.as_secs_f64() * 1_000.0
    );
    println!(
        "cluster delivery: emissions={} duplicates={} dedup_ratio={:.6} fanout_p50={} fanout_p95={} fanout_p99={}",
        cluster_capture.logical_emissions,
        cluster_capture.duplicate_emissions,
        ratio(
            cluster_capture.duplicate_emissions,
            cluster_capture.logical_emissions
        ),
        percentile(&cluster_capture.fanouts, 50),
        percentile(&cluster_capture.fanouts, 95),
        percentile(&cluster_capture.fanouts, 99)
    );
    for capture in bounded {
        let k = capture.k;
        let shard_bound = k.saturating_mul(shards);
        println!(
            "bounded K={k}: retained={} match_rank_ms={:.3} encoded_bytes={} evaluations={} replacements={} collector_bound_entries={} collector_payload_bytes={} shard_rows={} routed_shards={} shard_result_bytes={} collect_merge_ms={:.3} fetch_bytes={} fetch_ms={:.3} max_cluster_rows/title={shard_bound}",
            capture.retained,
            capture.match_rank_time.as_secs_f64() * 1_000.0,
            capture.encoded_bytes,
            capture.evaluations,
            capture.replacements,
            capture.collector_bound_entries,
            capture.collector_payload_bytes,
            capture.shard_rows_received,
            capture.routed_shards,
            capture.shard_result_bytes,
            capture.collect_merge_time.as_secs_f64() * 1_000.0,
            capture.fetch_bytes,
            capture.fetch_time.as_secs_f64() * 1_000.0,
        );
    }
    for capture in batch {
        println!(
            "batch bs={} K={}: titles={} local_batch_ms={:.3} cluster_batch_ms={:.3} shard_calls={} shard_rows={} shard_result_bytes={} fetch_bytes={} fetch_ms={:.3}",
            capture.batch_size,
            capture.k,
            capture.titles,
            capture.local_time.as_secs_f64() * 1_000.0,
            capture.cluster_time.as_secs_f64() * 1_000.0,
            capture.fanned_shard_calls,
            capture.shard_rows_received,
            capture.shard_result_bytes,
            capture.fetch_bytes,
            capture.fetch_time.as_secs_f64() * 1_000.0,
        );
    }
    println!(
        "semantic checksum: local={:016x} cluster={:016x}",
        capture.checksum, cluster_capture.checksum
    );
}

/// ADR-112 bounded ranked batch capture: the local columnar batch entry and
/// the one-call-per-shard cluster batch, each asserted per-title identical to
/// the scalar bounded path (equivalence is the hard gate; timings are
/// informational).
struct BatchCapture {
    batch_size: usize,
    k: usize,
    titles: usize,
    local_time: Duration,
    cluster_time: Duration,
    fanned_shard_calls: usize,
    shard_rows_received: usize,
    shard_result_bytes: u64,
    fetch_bytes: usize,
    fetch_time: Duration,
}

#[allow(clippy::too_many_arguments)]
fn capture_batch(
    snap: &reverse_rusty::EngineSnapshot,
    cluster: &ClusterEngine,
    titles: &[String],
    program: &reverse_rusty::CompiledRankProgram,
    cluster_program: &reverse_rusty::CompiledRankProgram,
    k: usize,
    batch_size: usize,
) -> BatchCapture {
    const THRESHOLD: u64 = 10_000;
    let slice_len = titles.len().min(batch_size);
    let batch_titles = &titles[..slice_len];
    let options = TopKOptions {
        size: k,
        track_total_hits_up_to: THRESHOLD,
        query_scope: QueryScope::WithBroad,
    };
    let pred = reverse_rusty::exact::TagPredicate::empty();
    let mut scratch = MatchScratch::new();

    let started = Instant::now();
    let local = snap
        .try_match_titles_batch_top_k(
            batch_titles,
            reverse_rusty::segment::BatchMatchOptions {
                include_broad: true,
                ..reverse_rusty::segment::BatchMatchOptions::default()
            },
            options,
            program,
            &pred,
            None,
        )
        .expect("local batch top k");
    let local_time = started.elapsed();

    let started = Instant::now();
    let distributed = cluster
        .try_percolate_filtered_top_k_batch(batch_titles, &[], options, cluster_program, None)
        .expect("cluster batch top k");
    let cluster_time = started.elapsed();

    for (i, title) in batch_titles.iter().enumerate() {
        let scalar = snap
            .try_match_title_top_k(title, options, program, &pred, &mut scratch, None)
            .expect("scalar bounded reference");
        let expected: Vec<(u64, i64)> = scalar
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        let local_rows: Vec<(u64, i64)> = local.titles[i]
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        assert_eq!(
            local_rows, expected,
            "local batch diverged at K={k} title={i}"
        );
        let cluster_rows: Vec<(u64, i64)> = distributed.titles[i]
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        assert_eq!(
            cluster_rows, expected,
            "cluster batch diverged at K={k} title={i}"
        );
        assert!(
            distributed.titles[i].hits.len() <= k,
            "per-title rows exceed K"
        );
    }

    let fetch_started = Instant::now();
    let sources = cluster
        .fetch_ranked_sources_batch_bounded(&distributed, 16 * 1024 * 1024, None)
        .expect("batch winner fetch");
    let fetch_time = fetch_started.elapsed();
    let fetch_bytes = sources
        .iter()
        .flatten()
        .map(String::len)
        .fold(0usize, usize::saturating_add);

    BatchCapture {
        batch_size,
        k,
        titles: slice_len,
        local_time,
        cluster_time,
        fanned_shard_calls: distributed.fanned_shard_calls,
        shard_rows_received: distributed.shard_rows_received,
        shard_result_bytes: distributed.shard_result_bytes,
        fetch_bytes,
        fetch_time,
    }
}

fn capture_bounded(
    snap: &reverse_rusty::EngineSnapshot,
    cluster: &ClusterEngine,
    titles: &[String],
    compatibility_rank: &reverse_rusty::CompiledRankSpec,
    program: &reverse_rusty::CompiledRankProgram,
    cluster_program: &reverse_rusty::CompiledRankProgram,
    k: usize,
) -> BoundedCapture {
    const THRESHOLD: usize = 10_000;
    let mut scratch = MatchScratch::new();
    let mut oracle_scratch = MatchScratch::new();
    let mut oracle_ids = Vec::new();
    let mut retained = 0usize;
    let mut encoded_bytes = 0usize;
    let mut match_rank_time = Duration::ZERO;
    let mut evaluations = 0u64;
    let mut replacements = 0u64;
    let mut shard_rows_received = 0usize;
    let mut routed_shards = 0usize;
    let mut shard_result_bytes = 0u64;
    let mut collect_merge_time = Duration::ZERO;
    let mut fetch_bytes = 0usize;
    let mut fetch_time = Duration::ZERO;
    for title in titles {
        snap.match_title(title, &mut oracle_scratch, &mut oracle_ids, true);
        let mut expected = snap.rank(&oracle_ids, compatibility_rank);
        expected.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        expected.truncate(k);

        let started = Instant::now();
        let actual = snap
            .try_match_title_top_k(
                title,
                TopKOptions {
                    size: k,
                    track_total_hits_up_to: THRESHOLD as u64,
                    query_scope: QueryScope::WithBroad,
                },
                program,
                &reverse_rusty::exact::TagPredicate::empty(),
                &mut scratch,
                None,
            )
            .expect("bounded ranked match");
        match_rank_time += started.elapsed();
        let rows: Vec<(u64, i64)> = actual
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        assert_eq!(rows, expected, "bounded result diverged at K={k}");
        retained = retained.saturating_add(rows.len());
        encoded_bytes = encoded_bytes.saturating_add(
            serde_json::to_vec(&rows)
                .expect("serialize bounded rows")
                .len(),
        );
        evaluations = evaluations.saturating_add(actual.rank_stats.evaluations);
        replacements = replacements.saturating_add(actual.rank_stats.heap_replacements);

        let cluster_started = Instant::now();
        let distributed = cluster
            .try_percolate_filtered_top_k(
                title,
                &[],
                TopKOptions {
                    size: k,
                    track_total_hits_up_to: THRESHOLD as u64,
                    query_scope: QueryScope::WithBroad,
                },
                cluster_program,
                None,
            )
            .expect("distributed bounded ranked match");
        collect_merge_time += cluster_started.elapsed();
        let distributed_rows: Vec<(u64, i64)> = distributed
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        assert_eq!(
            distributed_rows, expected,
            "distributed result diverged at K={k}"
        );
        assert_eq!(distributed.total_hits, actual.total_hits);
        assert!(
            distributed.shard_rows_received <= k.saturating_mul(distributed.routed_shards),
            "rows_received exceeded K × routed_shards at K={k}"
        );
        shard_rows_received = shard_rows_received.saturating_add(distributed.shard_rows_received);
        routed_shards = routed_shards.saturating_add(distributed.routed_shards);
        shard_result_bytes = shard_result_bytes.saturating_add(distributed.shard_result_bytes);
        let fetch_started = Instant::now();
        let sources = cluster
            .fetch_ranked_sources(&distributed, None)
            .expect("winner fetch");
        fetch_time += fetch_started.elapsed();
        fetch_bytes = fetch_bytes.saturating_add(sources.iter().map(String::len).sum::<usize>());
    }

    // Structural payload bound: K heap rows + K heap-id entries + threshold+1
    // total-id entries. Hash-table bucket/control overhead is allocator-specific,
    // so report the portable payload bytes separately from the entry bound.
    let collector_bound_entries = k
        .saturating_mul(2)
        .saturating_add(THRESHOLD.saturating_add(1));
    let collector_payload_bytes = k
        .saturating_mul(std::mem::size_of::<(u64, i64)>())
        .saturating_add(k.saturating_mul(std::mem::size_of::<u64>()))
        .saturating_add(
            THRESHOLD
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<u64>()),
        );
    BoundedCapture {
        k,
        retained,
        encoded_bytes,
        match_rank_time,
        evaluations,
        replacements,
        collector_bound_entries,
        collector_payload_bytes,
        shard_rows_received,
        routed_shards,
        shard_result_bytes,
        collect_merge_time,
        fetch_bytes,
        fetch_time,
    }
}

fn capture_local(
    snap: &reverse_rusty::EngineSnapshot,
    titles: &[String],
    rank: &reverse_rusty::CompiledRankSpec,
) -> Capture {
    let mut scratch = MatchScratch::new();
    let mut ids = Vec::new();
    let mut capture = Capture {
        match_counts: Vec::with_capacity(titles.len()),
        logical_emissions: 0,
        duplicate_emissions: 0,
        id_bytes: 0,
        score_bytes: 0,
        source_bytes: 0,
        rank_time: Duration::ZERO,
        checksum: fnv_offset(),
    };
    for title in titles {
        let stats = snap.match_title(title, &mut scratch, &mut ids, true);
        capture.match_counts.push(ids.len());
        capture.logical_emissions = capture
            .logical_emissions
            .saturating_add(stats.logical_emissions);
        capture.duplicate_emissions = capture
            .duplicate_emissions
            .saturating_add(stats.duplicate_emissions);
        let started = Instant::now();
        let scored = snap.rank(&ids, rank);
        capture.rank_time += started.elapsed();
        let rows: Vec<ScoredSource> = scored
            .iter()
            .map(|&(id, score)| ScoredSource {
                id,
                score,
                source: snap.get_query_source(id),
            })
            .collect();
        let id_json = serde_json::to_vec(&ids).expect("serialize ids");
        let score_json = serde_json::to_vec(&scored).expect("serialize scores");
        let source_json = serde_json::to_vec(&rows).expect("serialize sources");
        capture.id_bytes += id_json.len();
        capture.score_bytes += score_json.len();
        capture.source_bytes += source_json.len();
        capture.checksum = fnv_extend(capture.checksum, &id_json);
        capture.checksum = fnv_extend(capture.checksum, &score_json);
        capture.checksum = fnv_extend(capture.checksum, &source_json);
    }
    capture
}

fn capture_cluster(cluster: &ClusterEngine, titles: &[String]) -> ClusterCapture {
    let mut fanouts = Vec::with_capacity(titles.len());
    let mut logical_emissions = 0u64;
    let mut duplicate_emissions = 0u64;
    let mut checksum = fnv_offset();
    for title in titles {
        let (ids, stats) = cluster
            .percolate_with_stats(title)
            .expect("cluster percolate");
        logical_emissions = logical_emissions.saturating_add(stats.logical_emissions);
        duplicate_emissions = duplicate_emissions.saturating_add(stats.duplicate_emissions);
        fanouts.push(cluster.shard_fanout(title).len());
        checksum = fnv_extend(
            checksum,
            &serde_json::to_vec(&ids).expect("serialize cluster ids"),
        );
    }
    fanouts.sort_unstable();
    ClusterCapture {
        logical_emissions,
        duplicate_emissions,
        fanouts,
        checksum,
    }
}

fn percentile(sorted: &[usize], p: usize) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    sorted[(sorted.len() - 1).saturating_mul(p.min(100)) / 100]
}

fn ratio(n: u64, d: u64) -> f64 {
    if d == 0 {
        0.0
    } else {
        n as f64 / d as f64
    }
}

const fn fnv_offset() -> u64 {
    0xcbf2_9ce4_8422_2325
}

fn fnv_extend(mut hash: u64, bytes: &[u8]) -> u64 {
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

fn arg_usize(args: &[String], index: usize, default: usize) -> usize {
    args.get(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn arg_u64(args: &[String], index: usize, default: u64) -> u64 {
    args.get(index)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
