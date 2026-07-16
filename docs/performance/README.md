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
The ADR-107 pre-collector ranked-delivery capture is in
[`ranked-percolation-baseline.txt`](ranked-percolation-baseline.txt).

## Reproduce

```bash
cd engine
export CARGO_TARGET_DIR=/tmp/reverse-rusty-target                      # build off the synced folder

cargo test --release                                          # correctness oracle (zero false negatives)
cargo run --release --bin bench -- 1000000 5000 0.0 2.0 60    # selective path benchmark
cargo run --release --bin bench -- 1000000 5000 0.05 2.0 60   # with broad lane (shows its cost)
cargo run --release --bin rankbench -- 20000 500 8 275775489  # ADR-107 ranked-delivery baseline
cargo run --release --bin learn -- 500000 50 0.30            # corpus feature learner
cargo run --release --bin segbench -- 300000 3000 0.0        # read-amplification vs segment count
```

`bench` args: `<num_queries> <num_titles> <broad_frac> <skew> [reps]`.
