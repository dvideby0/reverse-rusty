use super::measure::{measure_static_report, measure_timing};
use super::{
    median, print_failures, runner_contract, timing_histories, workload_contract, write_json,
    Baseline, BaselineReference, Distribution, GatePolicy, Report, ResourceMetrics, TimingAttempt,
    TimingHistory, TimingSafetyLimits, RAYON_THREADS, RUNNER_CONTRACT_ID, SCHEMA_VERSION,
};
use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::io;
use std::path::Path;

pub(super) fn check(baseline_path: &Path, report_path: &Path) -> Result<(), Box<dyn Error>> {
    let baseline: Baseline = serde_json::from_slice(&fs::read(baseline_path)?)?;
    validate_baseline_contract(&baseline)?;

    let (mut report, engine, titles) = measure_static_report()?;
    let static_failures = compare_static(&report, &baseline);
    if !static_failures.is_empty() {
        write_json(report_path, &report)?;
        print_failures("deterministic/resource", &static_failures);
        return Err(io::Error::other("performance gate failed without a timing retry").into());
    }
    let history_pending = baseline.reference.source_run_ids.is_empty();
    let first = measure_timing(&engine, &titles)?;
    let first_failures = compare_timing(&first, &baseline);
    report.timing_attempts.push(first);

    if first_failures.is_empty() {
        write_json(report_path, &report)?;
        super::print_report_summary(&report);
        if history_pending {
            println!(
                "performance gate: PASS structure/resources + timing safety limits; \
                 variance-band comparison pending five reviewed CI reports"
            );
        } else {
            println!("performance gate: PASS");
        }
        return Ok(());
    }

    print_failures("timing attempt 1", &first_failures);
    if !baseline.policy.retry_timing_failures_once {
        write_json(report_path, &report)?;
        return Err(io::Error::other("performance timing gate failed").into());
    }

    println!("timing-only failure: repeating the complete timing window once");
    let second = measure_timing(&engine, &titles)?;
    let second_failures = compare_timing(&second, &baseline);
    report.timing_attempts.push(second);
    write_json(report_path, &report)?;

    if second_failures.is_empty() {
        super::print_report_summary(&report);
        if history_pending {
            println!(
                "performance gate: PASS timing safety limits on retry; \
                 variance-band comparison pending five reviewed CI reports"
            );
        } else {
            println!("performance gate: PASS on timing retry");
        }
        Ok(())
    } else {
        print_failures("timing attempt 2", &second_failures);
        Err(io::Error::other("performance timing gate failed twice").into())
    }
}

fn validate_baseline_contract(baseline: &Baseline) -> Result<(), Box<dyn Error>> {
    validate_baseline_definition(baseline)?;
    let runner = runner_contract();
    if baseline.runner != runner {
        return Err(io::Error::other(format!(
            "runner contract mismatch: expected {:?}, observed {:?}",
            baseline.runner, runner
        ))
        .into());
    }
    Ok(())
}

fn validate_baseline_definition(baseline: &Baseline) -> Result<(), Box<dyn Error>> {
    if baseline.schema_version != SCHEMA_VERSION {
        return Err(io::Error::other(format!(
            "baseline schema {} != supported schema {SCHEMA_VERSION}",
            baseline.schema_version
        ))
        .into());
    }
    if baseline.runner.id != RUNNER_CONTRACT_ID || baseline.runner.rayon_threads != RAYON_THREADS {
        return Err(io::Error::other(format!(
            "baseline runner must be {RUNNER_CONTRACT_ID} with {RAYON_THREADS} Rayon threads"
        ))
        .into());
    }
    let workload = workload_contract();
    if baseline.workload != workload {
        return Err(io::Error::other(format!(
            "workload contract mismatch: expected {:?}, binary has {:?}",
            baseline.workload, workload
        ))
        .into());
    }
    validate_timing_safety_limits(&baseline.policy.timing_safety_limits)?;
    validate_timing_reference(
        &baseline.reference.source_run_ids,
        &baseline.reference.timing,
    )
}

fn validate_timing_safety_limits(limits: &TimingSafetyLimits) -> Result<(), Box<dyn Error>> {
    if limits.latency_p50_max_ns == 0
        || limits.latency_p95_max_ns == 0
        || limits.latency_p99_max_ns == 0
        || limits.selective_titles_per_sec_min == 0
        || limits.columnar_titles_per_sec_min == 0
    {
        return Err(io::Error::other("timing safety limits must all be non-zero").into());
    }
    if limits.latency_p50_max_ns > limits.latency_p95_max_ns
        || limits.latency_p95_max_ns > limits.latency_p99_max_ns
    {
        return Err(io::Error::other(
            "timing latency safety limits must be ordered p50 <= p95 <= p99",
        )
        .into());
    }
    Ok(())
}

fn validate_timing_reference(
    source_run_ids: &[String],
    timing: &TimingHistory,
) -> Result<(), Box<dyn Error>> {
    let source_runs = source_run_ids.len();
    let histories = timing_histories(timing);
    if source_runs == 0 {
        if histories.iter().any(|(_, samples)| !samples.is_empty()) {
            return Err(io::Error::other(
                "a pending timing baseline must have no source runs and no timing samples",
            )
            .into());
        }
    } else {
        if source_runs < 5 {
            return Err(io::Error::other(
                "baseline must retain at least five reviewed CI source runs",
            )
            .into());
        }
        for (name, samples) in histories {
            if samples.len() != source_runs {
                return Err(io::Error::other(format!(
                    "{name} has {} samples for {source_runs} source runs",
                    samples.len()
                ))
                .into());
            }
        }
    }
    Ok(())
}

pub(super) fn rebaseline(
    baseline_path: &Path,
    reason: &str,
    report_paths: &[String],
) -> Result<(), Box<dyn Error>> {
    if std::env::var("RR_PERF_ACCEPT_REBASELINE").as_deref() != Ok("1") {
        return Err(io::Error::other(
            "refusing to rewrite the reviewed baseline without RR_PERF_ACCEPT_REBASELINE=1",
        )
        .into());
    }
    if reason.trim().is_empty() {
        return Err(io::Error::other("rebaseline reason must not be empty").into());
    }
    if report_paths.len() < 5 {
        return Err(io::Error::other(
            "an intentional rebaseline requires at least five independent CI reports",
        )
        .into());
    }

    let mut baseline: Baseline = serde_json::from_slice(&fs::read(baseline_path)?)?;
    validate_baseline_definition(&baseline)?;
    let reports = report_paths
        .iter()
        .map(|path| {
            serde_json::from_slice::<Report>(&fs::read(path)?)
                .map_err(|error| -> Box<dyn Error> { Box::new(error) })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let first = reports
        .first()
        .ok_or_else(|| io::Error::other("missing rebaseline report"))?;
    let mut source_runs = Vec::with_capacity(reports.len());
    let mut unique_runs = BTreeSet::new();
    let mut resident = Vec::with_capacity(reports.len());
    let mut durable = Vec::with_capacity(reports.len());
    let mut timing = TimingHistory {
        latency_p50_ns: Vec::with_capacity(reports.len()),
        latency_p95_ns: Vec::with_capacity(reports.len()),
        latency_p99_ns: Vec::with_capacity(reports.len()),
        selective_titles_per_sec: Vec::with_capacity(reports.len()),
        columnar_titles_per_sec: Vec::with_capacity(reports.len()),
    };

    for report in &reports {
        if report.schema_version != SCHEMA_VERSION
            || report.runner != baseline.runner
            || report.workload != baseline.workload
        {
            return Err(io::Error::other(format!(
                "report contract differs from the reviewed baseline: {:?}",
                report.github_run_id
            ))
            .into());
        }
        if report.structure != first.structure
            || report.resources.durable_files != first.resources.durable_files
        {
            return Err(io::Error::other(format!(
                "deterministic report fields disagree across source runs: {:?}",
                report.github_run_id
            ))
            .into());
        }
        let run_id = report
            .github_run_id
            .as_ref()
            .ok_or_else(|| io::Error::other("rebaseline reports must come from GitHub Actions"))?;
        if !unique_runs.insert(run_id.clone()) {
            return Err(io::Error::other(format!("duplicate source run {run_id}")).into());
        }
        let attempt = report.timing_attempts.first().ok_or_else(|| {
            io::Error::other(format!("source run {run_id} has no timing attempt"))
        })?;

        source_runs.push(run_id.clone());
        resident.push(report.resources.resident_bytes);
        durable.push(report.resources.durable_bytes);
        timing.latency_p50_ns.push(attempt.latency_p50_ns.median);
        timing.latency_p95_ns.push(attempt.latency_p95_ns.median);
        timing.latency_p99_ns.push(attempt.latency_p99_ns.median);
        timing
            .selective_titles_per_sec
            .push(attempt.selective_titles_per_sec.median);
        timing
            .columnar_titles_per_sec
            .push(attempt.columnar_titles_per_sec.median);
    }

    baseline.reference = BaselineReference {
        source_run_ids: source_runs,
        structure: first.structure.clone(),
        resources: ResourceMetrics {
            resident_bytes: median(&resident),
            durable_bytes: median(&durable),
            durable_files: first.resources.durable_files,
        },
        timing,
    };
    baseline.rebaseline_reason = reason.trim().to_string();
    validate_baseline_definition(&baseline)?;
    write_json(baseline_path, &baseline)?;
    println!(
        "rewrote {} from {} independent CI reports; review and commit the JSON diff",
        baseline_path.display(),
        reports.len()
    );
    Ok(())
}

fn compare_static(report: &Report, baseline: &Baseline) -> Vec<String> {
    let mut failures = Vec::new();
    if report.structure != baseline.reference.structure {
        failures.push(format!(
            "seed-fixed structure changed\n  expected: {:?}\n  observed: {:?}",
            baseline.reference.structure, report.structure
        ));
    }
    let resource_basis_points = baseline.policy.resource_material_regression_basis_points;
    compare_upper_resource(
        "resident bytes",
        report.resources.resident_bytes,
        baseline.reference.resources.resident_bytes,
        resource_basis_points,
        &mut failures,
    );
    compare_upper_resource(
        "durable logical bytes",
        report.resources.durable_bytes,
        baseline.reference.resources.durable_bytes,
        resource_basis_points,
        &mut failures,
    );
    if report.resources.durable_files != baseline.reference.resources.durable_files {
        failures.push(format!(
            "durable file count changed: expected {}, observed {}",
            baseline.reference.resources.durable_files, report.resources.durable_files
        ));
    }
    failures
}

fn compare_upper_resource(
    name: &str,
    current: u64,
    reference: u64,
    basis_points: u32,
    failures: &mut Vec<String>,
) {
    let allowance = basis_point_allowance(reference, basis_points);
    let limit = reference.saturating_add(allowance);
    println!("{name}: current={current} reference={reference} upper_limit={limit}");
    if current > limit {
        failures.push(format!(
            "{name} regressed materially: {current} > {limit} (reference {reference})"
        ));
    }
}

fn compare_timing(attempt: &TimingAttempt, baseline: &Baseline) -> Vec<String> {
    let mut failures = compare_timing_safety_limits(attempt, &baseline.policy.timing_safety_limits);
    if baseline.reference.source_run_ids.is_empty() {
        return failures;
    }
    compare_latency(
        "latency p50",
        &attempt.latency_p50_ns,
        &baseline.reference.timing.latency_p50_ns,
        &baseline.policy,
        &mut failures,
    );
    compare_latency(
        "latency p95",
        &attempt.latency_p95_ns,
        &baseline.reference.timing.latency_p95_ns,
        &baseline.policy,
        &mut failures,
    );
    compare_latency(
        "latency p99",
        &attempt.latency_p99_ns,
        &baseline.reference.timing.latency_p99_ns,
        &baseline.policy,
        &mut failures,
    );
    compare_throughput(
        "selective throughput",
        &attempt.selective_titles_per_sec,
        &baseline.reference.timing.selective_titles_per_sec,
        &baseline.policy,
        &mut failures,
    );
    compare_throughput(
        "columnar throughput",
        &attempt.columnar_titles_per_sec,
        &baseline.reference.timing.columnar_titles_per_sec,
        &baseline.policy,
        &mut failures,
    );
    failures
}

fn compare_timing_safety_limits(
    attempt: &TimingAttempt,
    limits: &TimingSafetyLimits,
) -> Vec<String> {
    let mut failures = Vec::new();
    for (name, current, limit) in [
        (
            "latency p50",
            attempt.latency_p50_ns.median,
            limits.latency_p50_max_ns,
        ),
        (
            "latency p95",
            attempt.latency_p95_ns.median,
            limits.latency_p95_max_ns,
        ),
        (
            "latency p99",
            attempt.latency_p99_ns.median,
            limits.latency_p99_max_ns,
        ),
    ] {
        println!("{name} safety floor: current_median={current} upper_limit={limit}");
        if current > limit {
            failures.push(format!(
                "{name} exceeded the absolute safety limit: {current} > {limit}"
            ));
        }
    }
    for (name, current, limit) in [
        (
            "selective throughput",
            attempt.selective_titles_per_sec.median,
            limits.selective_titles_per_sec_min,
        ),
        (
            "columnar throughput",
            attempt.columnar_titles_per_sec.median,
            limits.columnar_titles_per_sec_min,
        ),
    ] {
        println!("{name} safety floor: current_median={current} lower_limit={limit}");
        if current < limit {
            failures.push(format!(
                "{name} fell below the absolute safety limit: {current} < {limit}"
            ));
        }
    }
    failures
}

fn compare_latency(
    name: &str,
    current: &Distribution,
    history: &[u64],
    policy: &GatePolicy,
    failures: &mut Vec<String>,
) {
    let reference = distribution_unchecked(history);
    let allowance = timing_allowance(&reference, policy);
    let limit = reference.median.saturating_add(allowance);
    println!(
        "{name}: current_median={} current_mad={} reference_median={} reference_mad={} upper_limit={}",
        current.median, current.mad, reference.median, reference.mad, limit
    );
    if current.median > limit {
        failures.push(format!(
            "{name} regressed materially: {} > {limit}",
            current.median
        ));
    }
}

fn compare_throughput(
    name: &str,
    current: &Distribution,
    history: &[u64],
    policy: &GatePolicy,
    failures: &mut Vec<String>,
) {
    let reference = distribution_unchecked(history);
    let allowance = timing_allowance(&reference, policy);
    let limit = reference.median.saturating_sub(allowance);
    println!(
        "{name}: current_median={} current_mad={} reference_median={} reference_mad={} lower_limit={}",
        current.median, current.mad, reference.median, reference.mad, limit
    );
    if current.median < limit {
        failures.push(format!(
            "{name} regressed materially: {} < {limit}",
            current.median
        ));
    }
}

fn timing_allowance(reference: &Distribution, policy: &GatePolicy) -> u64 {
    basis_point_allowance(
        reference.median,
        policy.timing_material_regression_basis_points,
    )
    .max(
        reference
            .mad
            .saturating_mul(u64::from(policy.timing_mad_multiplier)),
    )
}

fn basis_point_allowance(value: u64, basis_points: u32) -> u64 {
    let numerator = u128::from(value)
        .saturating_mul(u128::from(basis_points))
        .saturating_add(9_999);
    u64::try_from(numerator / 10_000).unwrap_or(u64::MAX)
}

fn distribution_unchecked(samples: &[u64]) -> Distribution {
    Distribution::from_samples(samples.to_vec())
        .expect("baseline validation rejects empty timing histories")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timing_safety_limits() -> TimingSafetyLimits {
        TimingSafetyLimits {
            latency_p50_max_ns: 7_500,
            latency_p95_max_ns: 75_000,
            latency_p99_max_ns: 100_000,
            selective_titles_per_sec_min: 120_000,
            columnar_titles_per_sec_min: 180_000,
        }
    }

    #[test]
    fn timing_allowance_takes_material_or_noise_band_whichever_is_larger() {
        let policy = GatePolicy {
            timing_material_regression_basis_points: 3_000,
            timing_mad_multiplier: 3,
            resource_material_regression_basis_points: 500,
            retry_timing_failures_once: true,
            timing_safety_limits: timing_safety_limits(),
        };
        let quiet = Distribution::from_samples(vec![100; 5]).expect("quiet");
        assert_eq!(timing_allowance(&quiet, &policy), 30);

        let noisy = Distribution::from_samples(vec![50, 75, 100, 125, 150]).expect("noisy");
        assert_eq!(timing_allowance(&noisy, &policy), 75);
    }

    #[test]
    fn resource_limit_rounds_up() {
        assert_eq!(basis_point_allowance(101, 500), 6);
    }

    fn timing_history(samples: usize) -> TimingHistory {
        TimingHistory {
            latency_p50_ns: vec![1; samples],
            latency_p95_ns: vec![1; samples],
            latency_p99_ns: vec![1; samples],
            selective_titles_per_sec: vec![1; samples],
            columnar_titles_per_sec: vec![1; samples],
        }
    }

    #[test]
    fn pending_timing_reference_must_be_completely_empty() {
        validate_timing_reference(&[], &timing_history(0)).expect("empty pending state");
        assert!(validate_timing_reference(&[], &timing_history(1)).is_err());
    }

    #[test]
    fn timing_safety_limits_must_be_nonzero_and_ordered() {
        validate_timing_safety_limits(&timing_safety_limits()).expect("valid safety limits");

        let mut zero = timing_safety_limits();
        zero.selective_titles_per_sec_min = 0;
        assert!(validate_timing_safety_limits(&zero).is_err());

        let mut unordered = timing_safety_limits();
        unordered.latency_p50_max_ns = unordered.latency_p95_max_ns + 1;
        assert!(validate_timing_safety_limits(&unordered).is_err());
    }

    #[test]
    fn timing_safety_limits_block_regressions_without_history() {
        let dist = |value| Distribution::from_samples(vec![value; 3]).expect("distribution");
        let mut attempt = TimingAttempt {
            latency_p50_ns: dist(7_500),
            latency_p95_ns: dist(75_000),
            latency_p99_ns: dist(100_000),
            selective_titles_per_sec: dist(120_000),
            columnar_titles_per_sec: dist(180_000),
        };
        assert!(
            compare_timing_safety_limits(&attempt, &timing_safety_limits()).is_empty(),
            "limits are inclusive"
        );

        attempt.latency_p99_ns = dist(100_001);
        attempt.selective_titles_per_sec = dist(119_999);
        let failures = compare_timing_safety_limits(&attempt, &timing_safety_limits());
        assert_eq!(failures.len(), 2);
        assert!(failures.iter().any(|failure| failure.contains("p99")));
        assert!(failures.iter().any(|failure| failure.contains("selective")));
    }

    #[test]
    fn populated_timing_reference_requires_five_aligned_ci_runs() {
        let four = (0..4).map(|i| i.to_string()).collect::<Vec<_>>();
        assert!(validate_timing_reference(&four, &timing_history(4)).is_err());

        let five = (0..5).map(|i| i.to_string()).collect::<Vec<_>>();
        validate_timing_reference(&five, &timing_history(5)).expect("five aligned histories");
        assert!(validate_timing_reference(&five, &timing_history(4)).is_err());
    }
}
