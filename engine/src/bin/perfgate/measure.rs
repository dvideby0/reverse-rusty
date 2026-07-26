use super::{
    nearest_rank, runner_contract, workload_contract, Distribution, Report, ResourceMetrics,
    StructuralMetrics, TimingAttempt, BROAD_BATCH_SIZE, LATENCY_ROUNDS, THROUGHPUT_REPS,
    THROUGHPUT_SAMPLES, WARMUP_TITLES,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
use reverse_rusty::Normalizer;
use std::error::Error;
use std::fs;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> io::Result<Self> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "reverse-rusty-perfgate-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(super) fn measure_report() -> Result<Report, Box<dyn Error>> {
    let (mut report, engine, titles) = measure_static_report()?;
    report
        .timing_attempts
        .push(measure_timing(&engine, &titles)?);
    Ok(report)
}

pub(super) fn measure_static_report() -> Result<(Report, Engine, Vec<String>), Box<dyn Error>> {
    let workload = workload_contract();
    eprintln!(
        "[perfgate] generating {} queries / {} titles (seed={:#x})",
        workload.num_queries, workload.num_titles, workload.seed
    );
    let data = generate(&GenConfig {
        num_queries: workload.num_queries,
        num_titles: workload.num_titles,
        broad_query_frac: f64::from(super::BROAD_FRACTION_MILLIONTHS) / 1_000_000.0,
        hot_skew: f64::from(super::HOT_SKEW_MILLIONTHS) / 1_000_000.0,
        family_size: workload.family_size,
        seed: workload.seed,
        num_players: (workload.num_queries / 40).max(2_000),
        num_sets: (workload.num_queries / 100).max(1_000),
    });

    eprintln!("[perfgate] building in-memory reference");
    let mut engine = Engine::new(Normalizer::default_vocab()?);
    engine.build_from_queries(&data.queries);
    if engine.num_queries() != workload.num_queries {
        return Err(io::Error::other(format!(
            "stored {} of {} generated queries",
            engine.num_queries(),
            workload.num_queries
        ))
        .into());
    }

    let structure = measure_structure(&engine, &data.titles);
    let resources = measure_resources(&data.queries)?;
    let report = Report {
        schema_version: super::SCHEMA_VERSION,
        github_run_id: std::env::var("GITHUB_RUN_ID").ok(),
        runner: runner_contract(),
        workload,
        structure,
        resources,
        timing_attempts: Vec::with_capacity(2),
    };
    Ok((report, engine, data.titles))
}

fn measure_structure(engine: &Engine, titles: &[String]) -> StructuralMetrics {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut candidates = Vec::with_capacity(titles.len());
    let mut unique_candidate_sum = 0u64;
    let mut match_sum = 0u64;

    for title in titles {
        let stats = engine.match_title(title, &mut scratch, &mut out, true);
        candidates.push(stats.unique_candidates);
        unique_candidate_sum += u64::from(stats.unique_candidates);
        match_sum += u64::from(stats.matches);
    }
    candidates.sort_unstable();

    StructuralMetrics {
        stored_queries: engine.num_queries(),
        class_counts: engine.class_counts(),
        dict_features: engine.dict_len(),
        main_max_posting: engine.main_index().max_posting_len(),
        main_postings_over_1024: engine.main_index().count_over(1_024),
        unique_candidate_sum,
        unique_candidate_p95: nearest_rank(&candidates, 95),
        unique_candidate_p99: nearest_rank(&candidates, 99),
        unique_candidate_max: candidates.last().copied().unwrap_or(0),
        match_sum,
    }
}

fn measure_resources(queries: &[(u64, String)]) -> Result<ResourceMetrics, Box<dyn Error>> {
    eprintln!("[perfgate] measuring persistent resident memory and logical footprint");
    let dir = TempDir::new()?;
    {
        let mut engine = Engine::with_config(
            Normalizer::default_vocab()?,
            EngineConfig {
                data_dir: Some(dir.0.clone()),
                retain_source: false,
                ..EngineConfig::default()
            },
        );
        engine.build_from_queries(queries);
        if !engine.persistence_healthy() {
            return Err(io::Error::other("persistent benchmark build became unhealthy").into());
        }
    }

    let engine = Engine::open(
        Normalizer::default_vocab()?,
        EngineConfig {
            data_dir: Some(dir.0.clone()),
            retain_source: false,
            ..EngineConfig::default()
        },
    )?;
    let metrics = engine.metrics();
    if metrics.total_queries != queries.len() {
        return Err(io::Error::other(format!(
            "persistent reopen has {} queries, expected {}",
            metrics.total_queries,
            queries.len()
        ))
        .into());
    }
    let resident = metrics
        .dict_bytes
        .saturating_add(metrics.query_store_bytes)
        .saturating_add(metrics.logical_index_bytes)
        .saturating_add(metrics.alive_bytes);
    let (durable_bytes, durable_files) = logical_tree_size(&dir.0)?;
    Ok(ResourceMetrics {
        resident_bytes: u64::try_from(resident)?,
        durable_bytes,
        durable_files,
    })
}

fn logical_tree_size(path: &Path) -> io::Result<(u64, usize)> {
    let mut bytes = 0u64;
    let mut files = 0usize;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            let (child_bytes, child_files) = logical_tree_size(&entry.path())?;
            bytes = bytes.saturating_add(child_bytes);
            files = files.saturating_add(child_files);
        } else if file_type.is_file() {
            bytes = bytes.saturating_add(entry.metadata()?.len());
            files = files.saturating_add(1);
        }
    }
    Ok((bytes, files))
}

pub(super) fn measure_timing(
    engine: &Engine,
    titles: &[String],
) -> Result<TimingAttempt, Box<dyn Error>> {
    eprintln!(
        "[perfgate] timing {LATENCY_ROUNDS} latency rounds and {THROUGHPUT_SAMPLES} throughput samples"
    );
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    for title in titles.iter().take(WARMUP_TITLES) {
        black_box(engine.match_title(title, &mut scratch, &mut out, true));
    }

    let mut p50 = Vec::with_capacity(LATENCY_ROUNDS);
    let mut p95 = Vec::with_capacity(LATENCY_ROUNDS);
    let mut p99 = Vec::with_capacity(LATENCY_ROUNDS);
    for _ in 0..LATENCY_ROUNDS {
        let mut samples = Vec::with_capacity(titles.len());
        for title in titles {
            let start = Instant::now();
            black_box(engine.match_title(title, &mut scratch, &mut out, true));
            samples.push(u64::try_from(start.elapsed().as_nanos())?);
        }
        samples.sort_unstable();
        p50.push(nearest_rank(&samples, 50));
        p95.push(nearest_rank(&samples, 95));
        p99.push(nearest_rank(&samples, 99));
    }

    let mut selective = Vec::with_capacity(THROUGHPUT_SAMPLES);
    for _ in 0..THROUGHPUT_SAMPLES {
        let start = Instant::now();
        let mut checksum = 0u64;
        for _ in 0..THROUGHPUT_REPS {
            for title in titles {
                let stats = engine.match_title(title, &mut scratch, &mut out, false);
                checksum = checksum.wrapping_add(u64::from(stats.matches));
            }
        }
        black_box(checksum);
        selective.push(rate_per_second(
            titles.len().saturating_mul(THROUGHPUT_REPS),
            start.elapsed().as_nanos(),
        )?);
    }

    let options = BatchMatchOptions {
        include_broad: true,
        broad_batch_size: BROAD_BATCH_SIZE,
        broad_strategy: BroadStrategy::Columnar,
        broad_materialize: true,
        broad_prefilter: true,
    };
    black_box(engine.match_titles_batch_stats(titles, options));
    let mut columnar = Vec::with_capacity(THROUGHPUT_SAMPLES);
    for _ in 0..THROUGHPUT_SAMPLES {
        let start = Instant::now();
        let mut checksum = 0u64;
        for _ in 0..THROUGHPUT_REPS {
            let stats = engine.match_titles_batch_stats(titles, options);
            checksum = checksum.wrapping_add(u64::from(stats.matches));
        }
        black_box(checksum);
        columnar.push(rate_per_second(
            titles.len().saturating_mul(THROUGHPUT_REPS),
            start.elapsed().as_nanos(),
        )?);
    }

    Ok(TimingAttempt {
        latency_p50_ns: Distribution::from_samples(p50)?,
        latency_p95_ns: Distribution::from_samples(p95)?,
        latency_p99_ns: Distribution::from_samples(p99)?,
        selective_titles_per_sec: Distribution::from_samples(selective)?,
        columnar_titles_per_sec: Distribution::from_samples(columnar)?,
    })
}

fn rate_per_second(items: usize, elapsed_ns: u128) -> Result<u64, Box<dyn Error>> {
    let numerator = u128::try_from(items)?.saturating_mul(1_000_000_000);
    let rate = numerator / elapsed_ns.max(1);
    Ok(u64::try_from(rate)?)
}
