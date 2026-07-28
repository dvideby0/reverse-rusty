# ADR-124: Variance-tolerant performance regression gate and scheduled soak

> [Distributed v1 — the ADR-065 graduation program decisions](areas/distributed-v1-graduation.md) · [Decision hub](../DECISIONS.md) · **Status:** Done (2026-07-25)

## Context

ADR-024 deliberately made benchmark output advisory because an absolute wall-clock threshold on a
floating hosted runner would be flaky. That protected developer trust in CI, but it also meant a
material latency, throughput, resident-memory, or on-disk-footprint regression could merge. The
10M mixed-operations soak was reproducible but manual, so long-horizon regressions depended on a
person remembering to run it.

GitHub now exposes a sufficiently explicit standard public-runner contract for a narrow gate:
`ubuntu-24.04` is an x64 VM with 4 vCPU and 16 GiB. The label still does not promise one CPU model,
so pinning the label is necessary but not sufficient; timing must use repeated samples and
variance-aware bands. GitHub also documents that scheduled workflows run from the default branch,
may be delayed at high-load times, and are less likely to be delayed away from the top of the hour.
Artifacts can declare their own retention period (within the repository limit).

Primary references:

- [GitHub-hosted runner specifications](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)
- [GitHub Actions `schedule` semantics](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
- [Workflow artifact retention](https://docs.github.com/en/actions/tutorials/store-and-share-data#configuring-a-custom-retention-period)

## Decision

### 1. One small merge-blocking contract

Add `perfgate`, separate from the exploratory `bench`, `segbench`, `snapbench`, and
`clusterbench` binaries. The required `gate + benchmarks` job now runs it after `check.sh` and the
deploy smoke. It is deliberately narrow:

- pinned `ubuntu-24.04`, x64, 4-way standard public runner;
- pinned release/LTO build and `RAYON_NUM_THREADS=4`;
- exactly 1,000,000 queries and 20,000 titles, seed `0x00C0FFEE`, broad fraction 5%, skew 2.0,
  family size 8;
- seven complete per-title latency rounds and nine 100,000-title throughput windows;
- selective scalar throughput plus production-shaped columnar broad throughput;
- a persistent `retain_source=false` reopen for resident-memory and logical-file-size measurement.

The binary fails before measuring if the observed OS, architecture, available parallelism, Rayon
width, workload, or baseline schema differs. Changing the runner or workload is therefore an
explicit contract migration, never an accidental comparison across unlike machines.

### 2. Separate deterministic invariants from noisy timings

The stored baseline is [`../performance/perf-baseline.json`](../performance/perf-baseline.json).
The following are merge-blocking without a retry:

- exact cost-class counts, dictionary size, posting shape, candidate sum/p95/p99/max, match sum;
- exact durable file count;
- resident bytes/query and logical durable bytes/query, each with a 5% material-growth ceiling.

Timing is summarized twice: each run records the median and median absolute deviation (MAD) across
its repeated windows, and the reference is the median/MAD across at least five reviewed CI runs.
For latency, the upper allowance is `max(30% of reference median, 3 × reference MAD)`; throughput
uses the symmetric lower allowance. A timing-only breach repeats the entire timing window once and
fails only if the retry also breaches. Structural or resource failures never retry. This makes
large regressions blocking without promoting transient host contention to a red build.

The bootstrap timing history uses six successful post-compiler-semantics CI artifacts. The
seed-fixed structure and resource values were captured from the same current workload and on-disk
format. Every PR uploads `perf-current.json` beside the legacy benchmark logs for diagnosis.

### 3. Rebaseline is an explicit reviewed operation

`perfgate rebaseline`:

- requires `RR_PERF_ACCEPT_REBASELINE=1`;
- requires a non-empty reason;
- requires at least five distinct GitHub Actions reports;
- rejects mixed runner/workload/schema reports and any disagreement in deterministic fields;
- uses the first timing attempt from every run (never cherry-picks a retry);
- preserves the gate policy while replacing the references and reason.

The resulting JSON diff must be reviewed in the PR that intentionally accepts the new performance
shape. A failing PR is never allowed to rewrite its own baseline automatically.

### 4. Schedule the existing large soak

`.github/workflows/soak.yml` runs the exact seeded 10M mixed-operations target every Monday at
03:37 UTC (off the top of the hour) and also supports `workflow_dispatch`. It records runner
metadata, full test output, and `/usr/bin/time -v` resources; artifacts are retained for 90 days.
The old `CI → run_soak` manual trigger remains compatible. The 20M durable multi-shard soak remains
manual because its recorded ~16 GiB peak consumes the entire standard public runner.

## Consequences

- A material regression in the agreed latency, throughput, memory, footprint, or seed-fixed work
  shape now blocks the already-required CI job.
- The broad exploratory sweeps remain advisory and continue to expose signals too noisy or
  expensive for every PR.
- Hosted-runner image or hardware-contract changes fail loud and require a five-run rebaseline.
- The weekly soak supplies retained longitudinal evidence without adding 10M-query cost to every
  pull request.

## Current outcome

The domain-neutral fixture migration changed workload semantics, invalidating the original six-run
timing history. Rather than compare unlike workloads, the baseline now uses the ADR's explicit
all-empty pending state: deterministic structure and resource checks remain active, while timing
is still captured in each CI report but comparison is visibly suspended until five fresh reviewed
reports are passed through the same `perfgate rebaseline` workflow. Any partial baseline history
fails validation.
