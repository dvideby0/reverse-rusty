# Performance

Measured results for Reverse Rusty (single core, aarch64 4-core / 3.8 GiB sandbox, std-only).

## Headline numbers

- Selective realtime path: **710k titles/sec/core @ 1M queries**, **437k @ 5M** — 158–255× the
  2,778 titles/sec spec target. **Candidates/title flat at ~54** regardless of query count.
- **Zero false negatives and zero false positives** vs a brute-force oracle over 109k matches.
- Updates: **~750k/sec/core**, immediate (epoch) visibility. Memory: **~256 B/query**.
- Build: **~650k queries/sec/core**. Broad queries inline cost ~9× throughput → quarantined.
- LSM read amplification: throughput falls ~2× from 1→8 segments while candidates/title stay flat.

Full analysis, tables, bottlenecks, and the 100M extrapolation are in [`results.md`](results.md).
The **benchmark runbook** — how to run each harness, the machine-independent **invariants** to
verify, and the dated **capture log** — is in [`benchmark-results.txt`](benchmark-results.txt).
The merge-blocking ADR-124 subset and its reviewed runner history live in
[`perf-baseline.json`](perf-baseline.json): exact seed-fixed work shape, 5% persistent
resident/durable ceilings, and variance-banded p50/p95/p99 plus selective/columnar throughput.
The ADR-107 pre-collector baseline and ADR-108 bounded K=10/100/1,000/10,000 post-integration
latency, structural-memory, result-byte, and checksum capture are in
[`ranked-percolation-baseline.txt`](ranked-percolation-baseline.txt). It also records the
post-ADR-114 rerun and ADR-115's evidence-based decision not to add exact competitive pruning:
the source plan's “verification dominates delivery” prerequisite was not established.

## Reproduce

```bash
cd engine
export CARGO_TARGET_DIR=/tmp/reverse-rusty-target                      # build off the synced folder

cargo test --release                                          # correctness oracle (zero false negatives)
cargo run --release --bin bench -- 1000000 5000 0.0 2.0 60    # selective path benchmark
cargo run --release --bin bench -- 1000000 5000 0.05 2.0 60   # with broad lane (shows its cost)
cargo run --release --bin rankbench -- 20000 500 8 275775489  # ADR-107/108/110 local + distributed bounded top-K/fetch
cargo run --release --bin learn -- 500000 50 0.30            # corpus feature learner
cargo run --release --bin segbench -- 300000 3000 0.0        # read-amplification vs segment count

# Diagnostic capture on any machine (only pinned CI may make a gate verdict):
RAYON_NUM_THREADS=4 cargo run --release --bin perfgate -- \
  capture /tmp/perf-current.json
```

`bench` args:
`<num_queries> <num_titles> <broad_frac> <skew> [seed] [reps] [hot_theta] [dedup]`.

## Automated regression policy

The required `gate + benchmarks` CI job pins the public standard `ubuntu-24.04` x64 runner
(4 vCPU / 16 GiB), release+LTO, four Rayon threads, 1M queries, 20k titles, 5% broad intent, and
seed `0x00C0FFEE`. `perfgate` collects seven complete latency distributions and nine throughput
windows. Each timing bound is the more tolerant of a 30% material-change band and three historical
MADs; a timing-only breach gets one complete retry. Deterministic structure and the 5% resource
ceilings never retry.

The broader capture log remains valuable but advisory. Full policy, rebaseline procedure, and
scheduled-soak contract → [`../testing.md`](../testing.md) and
[ADR-124](../decisions/adr-124-variance-tolerant-performance-gate.md).
