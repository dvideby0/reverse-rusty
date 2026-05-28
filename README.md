# Percolator — high-performance reverse product-query matcher

Percolator matches large numbers of stored product-intent queries against incoming eBay-style listing
titles ("percolation"). Specialized for short product titles, it gates candidates on **semantic
signatures** (not raw terms), verifies with **integer-only match plans**, **quarantines broad
queries**, and supports **frequent updates** — with a hard guarantee of **zero false negatives** for
supported semantics. There is a working, tested Rust engine in `engine/` plus the design and research
docs below.

## Status snapshot

Working, tested Rust engine (all tests pass, zero false negatives & zero false positives vs
a brute-force oracle). Production dependencies: **daachorse** (double-array Aho-Corasick for
multiword alias matching), **memmap2** (memory-mapped segment files), **roaring** (compressed
bitmaps for large postings), **rayon** (parallel matching). Selective path:
**710k titles/sec/core @ 1M queries** (158–255× the 2,778/s spec target),
**~3.8× parallel speedup on 4 threads**, **flat ~54 candidates/title** at any corpus size,
**~750k updates/sec/core**, **~256 B/query**. Multi-segment LSM write path (flush + bulk-ingest +
tombstones + compaction), per-segment anchor filters (cache-line blocked bloom), mmap'd segment
files with frozen hash tables, and write-ahead log for crash recovery are all implemented.
Clustering, aspects-first ingestion, and feature-model versioning are **design-complete but not yet
coded** — see [`docs/STATUS.md`](docs/STATUS.md) for the full implemented-vs-design-only breakdown.

## Quickstart

```bash
cd engine
export CARGO_TARGET_DIR=/tmp/perc-target                      # build off the synced folder

cargo run --release --bin demo                                # worked example end-to-end + explain
cargo test --release                                          # correctness oracle (zero false negatives)
cargo run --release --bin bench -- 1000000 5000 0.0 2.0 60    # benchmark <queries> <titles> <broad> <skew> <reps>
cargo run --release --bin learn -- 500000 50 0.30            # corpus feature learner
cargo run --release --bin norm -- <titles.txt>               # title introspection
cargo run --release --bin segbench -- 300000 3000 0.0        # read-amplification vs segment count
```

## Documentation map

| Doc | What's in it |
|---|---|
| [`docs/research/`](docs/research/README.md) | Prior art and peer studies: [`prior-art.md`](docs/research/prior-art.md) (Lucene Monitor, ES/OS percolator, Tantivy/Quickwit, roaring, Aho-Corasick/daachorse, set-containment), [`mokaccino.md`](docs/research/mokaccino.md) (closest Rust peer, build-vs-buy), [`corpus-feature-learning.md`](docs/research/corpus-feature-learning.md) (NPMI feature learning), [`real-data-findings.md`](docs/research/real-data-findings.md) (real eBay titles). |
| [`docs/design/`](docs/design/README.md) | Architecture overview + correctness contract + module map, then topic files: [`normalization.md`](docs/design/normalization.md) (DSL, normalizer, dictionary), [`matching.md`](docs/design/matching.md) (signatures, candidate index, exact matcher, broad lane, families, explain), [`ingestion-and-updates.md`](docs/design/ingestion-and-updates.md) (segments, LSM write path), [`clustering-and-scaling.md`](docs/design/clustering-and-scaling.md) (sharding, autoscaling). |
| [`docs/performance/`](docs/performance/README.md) | Measured results: [`results.md`](docs/performance/results.md) (full analysis, bottlenecks, 100M extrapolation, LSM read-amp) and [`benchmark-results.txt`](docs/performance/benchmark-results.txt) (raw captures). |
| [`docs/STATUS.md`](docs/STATUS.md) | What's implemented vs design-only, and the PoC simplifications. |

## Contribution conventions — where does new info go?

- **How another system solves this / new prior art** → `docs/research/` (extend
  [`prior-art.md`](docs/research/prior-art.md) or add a file; competitor deep-dives like mokaccino get
  their own file).
- **New or changed architecture / how a component works** → the matching `docs/design/<topic>.md`
  (`normalization`, `matching`, `ingestion-and-updates`, `clustering-and-scaling`).
- **New benchmark numbers / measurements** → [`docs/performance/results.md`](docs/performance/results.md)
  (and append raw captures to [`docs/performance/benchmark-results.txt`](docs/performance/benchmark-results.txt)).
- **Implementation progress / what's built vs planned** → [`docs/STATUS.md`](docs/STATUS.md).
- **Plans / next steps** → the **Roadmap** below.
- Keep each design doc starting with a one-line scope and cross-linking its siblings.

## Roadmap

Synthesized from the design docs' "next steps" and the design-only items in
[`docs/STATUS.md`](docs/STATUS.md).

**Near-term**
- Wire the corpus learner in as the runtime normalizer (daachorse automaton replacing the hand vocab).
- Aspects-first ingestion of eBay structured item-specifics (title normalizer becomes the fallback).
- Compaction-that-improves (re-anchoring drift during merge — the basic merge mechanic is done).

**Longer-term**
- Clustering & autoscaling: consistent-hash entity-anchor sharding, content routing, quorum
  cluster-manager, broad-lane replication, scale-to-zero.
- Blue/green feature-model versioning + re-materialize-from-log; explicit family DAG; roaring large
  postings + SIMD intersection.

Dependencies: `daachorse` (multiword alias automaton), `memmap2` (memory-mapped segment files),
`roaring` (compressed large postings), `rayon` (parallel matching). All four are the production
swap-ins documented in the design docs.
