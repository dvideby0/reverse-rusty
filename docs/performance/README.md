# Performance

Canonical entry point for measured behavior. Throughput and latency are hardware-dependent; compare
them only within a dated same-machine capture. Candidate shape, result equality, fan-out, class
counts, and serialized/accounted sizes are deterministic for the pinned workloads.

## Current headline

- **Correctness:** zero candidate false negatives and zero final-set mismatches in the shared-front-end
  and independent differential suites; the durable 20M K=8 soak also reports no sentinel misses,
  ghosts, or reopen drift.
- **Selective structure:** the retired fixture's broad-off capture was scale-flat through 20M
  queries. The current generic 1M/20k pin is **53.90 candidates/title** (p95 95, p99 111);
  generic 3M/5M/20M reruns are still pending.
- **Broad/hot cost:** class-H scheduling is visibility-neutral. In the latest 20M broad-bearing
  historical in-memory capture, canonical-body sharing reduced repeated body candidates from
  6,616.65 to 53.75 per title while leaving the emitted match set unchanged. Flush still expands
  members into the current mmap format, so the durable cluster does not retain that Stage-A saving.
- **Memory and disk:** the current generic 1M `retain_source=false` baseline accounts for **6.01
  B/query resident** and **244.59 B/query durable**. Historical 20M accounting was about 5.2 B/query
  without retained source and 109 B/query with it. These are engine-accounted values, not host RSS
  or page-cache guarantees.
- **Regression policy:** deterministic work shape, resource ceilings, and absolute timing safety
  limits are merge-blocking through ADR-124; variance bands add sensitivity when reviewed history
  is available. The 10M mixed-operations soak runs weekly and on demand.

Full analysis, tables, bottlenecks, and the 100M extrapolation are in [`results.md`](results.md).
The **benchmark runbook** — how to run each harness, the machine-independent **invariants** to
verify, and the dated **capture log** — is in [`benchmark-results.txt`](benchmark-results.txt).
The merge-blocking ADR-124 subset and its reviewed runner history live in
[`perf-baseline.json`](perf-baseline.json): exact seed-fixed work shape, 5% persistent
resident/durable ceilings, permanent timing safety limits, and reviewed variance histories for
p50/p95/p99 plus selective/columnar throughput.
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

The current domain-neutral workload migration has an explicit pending timing history: source-run
IDs and all timing histories are empty together. Structure, resources, and conservative absolute
timing limits remain merge-blocking; only the more sensitive variance-band comparison waits for
five fresh reviewed CI reports. Supplying those reports to `perfgate rebaseline` repopulates the
histories without removing the permanent safety limits.

The broader capture log remains valuable but advisory. Full policy, rebaseline procedure, and
scheduled-soak contract → [`../testing.md`](../testing.md) and
[ADR-124](../decisions/adr-124-variance-tolerant-performance-gate.md).
