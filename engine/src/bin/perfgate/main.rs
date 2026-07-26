//! Deterministic, variance-aware CI performance gate (ADR-124).
//!
//! The legacy benchmark binaries remain exploratory. This binary owns the small,
//! reviewed contract that is safe to make merge-blocking:
//!
//! * one fixed generated corpus and runner shape;
//! * exact structural invariants;
//! * bounded resident-memory and durable-footprint growth;
//! * repeated p50/p95/p99 latency and throughput samples compared with a
//!   historical median/MAD band;
//! * one retry for timing-only failures (never for structure or resources).
//!
//! CLI usage:
//!
//! ```text
//! perfgate capture <report.json>
//! perfgate check <baseline.json> <report.json>
//! RR_PERF_ACCEPT_REBASELINE=1 perfgate rebaseline <baseline.json> <reason> <report.json>...
//! ```

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;

mod gate;
mod measure;

use gate::{check, rebaseline};
use measure::measure_report;

const SCHEMA_VERSION: u32 = 1;
const RUNNER_CONTRACT_ID: &str = "github-hosted-ubuntu-24.04-x64-public-standard";
const NUM_QUERIES: usize = 1_000_000;
const NUM_TITLES: usize = 20_000;
const BROAD_FRACTION_MILLIONTHS: u32 = 50_000;
const HOT_SKEW_MILLIONTHS: u32 = 2_000_000;
const SEED: u64 = 0x00C0_FFEE;
const FAMILY_SIZE: usize = 8;
const RAYON_THREADS: usize = 4;
const WARMUP_TITLES: usize = 1_000;
const LATENCY_ROUNDS: usize = 7;
const THROUGHPUT_SAMPLES: usize = 9;
const THROUGHPUT_REPS: usize = 5;
const BROAD_BATCH_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RunnerContract {
    id: String,
    os: String,
    arch: String,
    available_parallelism: usize,
    rayon_threads: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkloadContract {
    profile: String,
    num_queries: usize,
    num_titles: usize,
    broad_fraction_millionths: u32,
    hot_skew_millionths: u32,
    seed: u64,
    family_size: usize,
    warmup_titles: usize,
    latency_rounds: usize,
    throughput_samples: usize,
    throughput_reps: usize,
    broad_batch_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StructuralMetrics {
    stored_queries: usize,
    class_counts: [u64; 5],
    dict_features: usize,
    main_max_posting: usize,
    main_postings_over_1024: usize,
    unique_candidate_sum: u64,
    unique_candidate_p95: u32,
    unique_candidate_p99: u32,
    unique_candidate_max: u32,
    match_sum: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ResourceMetrics {
    resident_bytes: u64,
    durable_bytes: u64,
    durable_files: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Distribution {
    samples: Vec<u64>,
    median: u64,
    mad: u64,
}

impl Distribution {
    fn from_samples(samples: Vec<u64>) -> Result<Self, Box<dyn Error>> {
        if samples.is_empty() {
            return Err(io::Error::other("performance distribution has no samples").into());
        }
        let median_value = median(&samples);
        let deviations = samples
            .iter()
            .map(|sample| sample.abs_diff(median_value))
            .collect::<Vec<_>>();
        let mad = median(&deviations);
        Ok(Self {
            samples,
            median: median_value,
            mad,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimingAttempt {
    latency_p50_ns: Distribution,
    latency_p95_ns: Distribution,
    latency_p99_ns: Distribution,
    selective_titles_per_sec: Distribution,
    columnar_titles_per_sec: Distribution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimingHistory {
    latency_p50_ns: Vec<u64>,
    latency_p95_ns: Vec<u64>,
    latency_p99_ns: Vec<u64>,
    selective_titles_per_sec: Vec<u64>,
    columnar_titles_per_sec: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct GatePolicy {
    timing_material_regression_basis_points: u32,
    timing_mad_multiplier: u32,
    resource_material_regression_basis_points: u32,
    retry_timing_failures_once: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BaselineReference {
    source_run_ids: Vec<String>,
    structure: StructuralMetrics,
    resources: ResourceMetrics,
    timing: TimingHistory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Baseline {
    schema_version: u32,
    runner: RunnerContract,
    workload: WorkloadContract,
    policy: GatePolicy,
    reference: BaselineReference,
    rebaseline_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Report {
    schema_version: u32,
    github_run_id: Option<String>,
    runner: RunnerContract,
    workload: WorkloadContract,
    structure: StructuralMetrics,
    resources: ResourceMetrics,
    timing_attempts: Vec<TimingAttempt>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        Some("capture") => {
            let report_path = required_arg(&args, 2, "report path")?;
            let report = measure_report()?;
            write_json(Path::new(report_path), &report)?;
            print_report_summary(&report);
            Ok(())
        }
        Some("check") => {
            let baseline_path = required_arg(&args, 2, "baseline path")?;
            let report_path = required_arg(&args, 3, "report path")?;
            check(Path::new(baseline_path), Path::new(report_path))
        }
        Some("rebaseline") => {
            let baseline_path = required_arg(&args, 2, "baseline path")?;
            let reason = required_arg(&args, 3, "rebaseline reason")?;
            let report_paths = args
                .get(4..)
                .ok_or_else(|| io::Error::other("missing rebaseline reports"))?;
            rebaseline(Path::new(baseline_path), reason, report_paths)
        }
        _ => Err(io::Error::other(
            "usage: perfgate capture <report.json> | perfgate check <baseline.json> \
             <report.json> | perfgate rebaseline <baseline.json> <reason> <report.json>...",
        )
        .into()),
    }
}

fn required_arg<'a>(
    args: &'a [String],
    index: usize,
    label: &str,
) -> Result<&'a str, Box<dyn Error>> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| io::Error::other(format!("missing {label}")).into())
}

fn timing_histories(history: &TimingHistory) -> [(&'static str, &[u64]); 5] {
    [
        ("latency_p50_ns", &history.latency_p50_ns),
        ("latency_p95_ns", &history.latency_p95_ns),
        ("latency_p99_ns", &history.latency_p99_ns),
        (
            "selective_titles_per_sec",
            &history.selective_titles_per_sec,
        ),
        ("columnar_titles_per_sec", &history.columnar_titles_per_sec),
    ]
}

fn nearest_rank<T: Copy + Default>(sorted: &[T], percentile: usize) -> T {
    if sorted.is_empty() {
        return T::default();
    }
    let rank = percentile.saturating_mul(sorted.len()).div_ceil(100);
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn median(samples: &[u64]) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[middle]
    } else {
        u64::try_from(u128::midpoint(
            u128::from(sorted[middle - 1]),
            u128::from(sorted[middle]),
        ))
        .unwrap_or(u64::MAX)
    }
}

fn runner_contract() -> RunnerContract {
    RunnerContract {
        id: std::env::var("RR_PERF_RUNNER_CONTRACT")
            .unwrap_or_else(|_| "local-unpinned".to_string()),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        available_parallelism: std::thread::available_parallelism()
            .map_or(0, std::num::NonZero::get),
        rayon_threads: rayon::current_num_threads(),
    }
}

fn workload_contract() -> WorkloadContract {
    WorkloadContract {
        profile: "release-lto".to_string(),
        num_queries: NUM_QUERIES,
        num_titles: NUM_TITLES,
        broad_fraction_millionths: BROAD_FRACTION_MILLIONTHS,
        hot_skew_millionths: HOT_SKEW_MILLIONTHS,
        seed: SEED,
        family_size: FAMILY_SIZE,
        warmup_titles: WARMUP_TITLES,
        latency_rounds: LATENCY_ROUNDS,
        throughput_samples: THROUGHPUT_SAMPLES,
        throughput_reps: THROUGHPUT_REPS,
        broad_batch_size: BROAD_BATCH_SIZE,
    }
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut json = serde_json::to_string_pretty(value)?;
    json.push('\n');
    fs::write(path, json)?;
    Ok(())
}

fn print_report_summary(report: &Report) {
    println!(
        "structure: queries={} classes={:?} dict={} candidates/title={}/{}",
        report.structure.stored_queries,
        report.structure.class_counts,
        report.structure.dict_features,
        report.structure.unique_candidate_sum,
        report.workload.num_titles
    );
    println!(
        "resources: resident={} bytes ({:.2} B/query), durable={} bytes ({:.2} B/query), files={}",
        report.resources.resident_bytes,
        bytes_per_query(report.resources.resident_bytes, report.workload.num_queries),
        report.resources.durable_bytes,
        bytes_per_query(report.resources.durable_bytes, report.workload.num_queries),
        report.resources.durable_files
    );
    for (index, timing) in report.timing_attempts.iter().enumerate() {
        println!(
            "timing attempt {}: p50/p95/p99={}/{}/{} ns, selective={} t/s, columnar={} t/s",
            index + 1,
            timing.latency_p50_ns.median,
            timing.latency_p95_ns.median,
            timing.latency_p99_ns.median,
            timing.selective_titles_per_sec.median,
            timing.columnar_titles_per_sec.median
        );
    }
}

fn bytes_per_query(bytes: u64, queries: usize) -> f64 {
    let queries_u32 = u32::try_from(queries).unwrap_or(u32::MAX).max(1);
    bytes as f64 / f64::from(queries_u32)
}

fn print_failures(scope: &str, failures: &[String]) {
    eprintln!("performance gate {scope} failure(s):");
    for failure in failures {
        eprintln!("  - {failure}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_uses_median_and_mad() {
        let distribution =
            Distribution::from_samples(vec![100, 101, 102, 103, 10_000]).expect("samples");
        assert_eq!(distribution.median, 102);
        assert_eq!(distribution.mad, 1);
    }

    #[test]
    fn nearest_rank_matches_the_benchmark_convention() {
        let values = (1u32..=100).collect::<Vec<_>>();
        assert_eq!(nearest_rank(&values, 50), 50);
        assert_eq!(nearest_rank(&values, 95), 95);
        assert_eq!(nearest_rank(&values, 99), 99);
    }
}
