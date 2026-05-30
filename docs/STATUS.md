# Status — what's built and what's next

Current state of the Rust engine (`engine/`) and the prioritized roadmap — **the canonical home for
what's implemented vs design-only**. Component detail lives in the [design docs](design/README.md) and
the [ADRs](DECISIONS.md); the per-file index is the module map in [`../CLAUDE.md`](../CLAUDE.md), and
dependency versions in [`../engine/Cargo.toml`](../engine/Cargo.toml). The full suite passes —
differential oracle, unit, server, coverage-gap, error-path, hardening, and persistence suites (the
last now covering durability-failure events + recovery-event buffering), plus doc-tests and the
pressure/soak suite (`tests/stress.rs` — now committed and run by `cargo test`; its 10M soak is
`#[ignore]`d). Run `cargo test --release` for the current count. GitHub Actions runs the full
`check.sh` gate + benchmarks on every PR — see [`testing.md`](testing.md) and [`DECISIONS.md`](DECISIONS.md) ADR-024.

---

## Implemented (working, tested)

- **Core pipeline** — DSL parser (`dsl.rs`), shared query/title normalizer with daachorse +
  `NormalizerBuilder` (`normalize.rs`), feature dictionary + 64-hot common mask (`dict.rs`),
  signature-cover optimizer + cost classes A/B/C/D (`compile.rs`), adaptive candidate index
  (inline → Vec → roaring, `index.rs`), integer-only SoA exact matcher (`exact.rs`).
- **LSM engine** (`segment.rs`) — immutable base segments + mutable memtable, `flush()`,
  `bulk_ingest()`, tombstone update/delete, ClickHouse-inspired score-based compaction
  (`compact` / `compact_all` / `compact_range`) with auto-triggers (`maybe_compact` / `maybe_flush`).
- **Persistence** — mmap'd `.seg` segment format with frozen hash tables (ADR-012), write-ahead log
  with CRC framing + crash recovery and configurable fsync policy (ADR-013), durable all-or-nothing
  bulk ingest (ADR-017), `Engine::open()` manifest + WAL recovery, query source store + `sources.dat`
  (ADR-014).
- **Read concurrency** — snapshot reads via `ArcSwap<EngineSnapshot>` + `parking_lot::Mutex` writer
  (ADR-016): lock-free reads, zero reader/writer contention.
- **Skip filter** — per-segment cache-line blocked bloom over signature keys (ADR-011), checked before
  each probe; `MatchStats` reports probe skip rate.
- **Runtime config** (`config.rs`) — `EngineConfig` knobs (segment cap, flush/holes thresholds,
  compaction cost, query complexity limits, WAL fsync policy) with startup validation. Dynamic knobs
  are runtime-tunable via the ES-style `GET/PUT /_settings` API (ADR-022); the config rides in the
  lock-free snapshot as `Arc<EngineConfig>`.
- **Observability** (`events.rs`) — `EngineEvent` / `EngineMetrics` / `CompactionTrigger` via a
  zero-dependency observer; wired to `tracing` structured logs + `prometheus` export. Durability
  degradation (WAL/manifest/segment/source-store write failures, corrupt-segment-skip on recovery)
  is routed through `EngineEvent::DurabilityFailure { op, detail, error }` instead of stderr, so the
  server logs it (`error!`/`warn!` by severity) and increments an alertable `durability_failures_total{op}`
  counter; recovery-time failures (pre-observer) are buffered and replayed on `set_observer` (ADR-021).
- **Vocabulary** (`vocab.rs`) — `Vocab` learn-from-any-of-groups + JSON persistence (ADR-015), runtime
  swap with vocab-epoch staleness tracking, per-segment reverse index for O(segments) delete.
- **HTTP server** (`bin/server.rs`) — ES-style REST (`/_doc`, `/_search` with explain/profile,
  `/_bulk` per-item status ADR-018, `/_stats`, `/_cat/stats`, `/_cat/segments` per-segment detail
  (text table + `?format=json`, ADR-023), `/_health`, `/_metrics`, `/_vocab*`,
  `/_settings` GET/PUT with dynamic-vs-static enforcement + `include_defaults` — ADR-022),
  graceful shutdown, production hardening (body/concurrency limits, request IDs, slow-query log,
  segment CRC, complexity limits).
- **Error handling** — typed `ParseError` / `NormalizerError`, fallible deserialization, zero
  panicking `unwrap()` in library code.
- **Tooling** — explain (`explain.rs`), seeded data generator (`gen.rs`), NPMI corpus learner
  (`bin/learn.rs`), title introspection (`bin/norm.rs`), benchmark + read-amplification harnesses
  (`bin/bench.rs`, `bin/segbench.rs`), CSV/JSONL loader (`loader.rs`).
- **Correctness** — randomized differential oracle (brute force vs engine): zero false negatives &
  zero false positives over 100k+ matches, across single-build / multi-segment / compaction configs.
- **Resident-memory reduction (ADR-020)** — per-component resident accounting
  (`dict`/`query_store`/`logical_index`/`alive` in `EngineMetrics`); lazy on-disk source store
  (`SourceStore`, `sources.dat` v2 sorted index+blob+CRC, `EngineConfig::retain_source`); flat mmap'd
  logical-index columns (`.seg` v2, binary-searched, v1-reconstruct back-compat). Resident drops from
  ~148 → ~4.5 B/query (`retain_source=false`). Both formats keep v1 read paths; oracle unchanged.

## Measured

Headline figures only. Full tables, p99s, and the 100M extrapolation are the canonical record in
[`performance/results.md`](performance/results.md); the machine-independent regression invariants live
in [`performance/benchmark-results.txt`](performance/benchmark-results.txt).

- Selective path **~158k–710k titles/sec/core** (1M–5M queries; ~256 B/query), **~3.8× on 4 threads**.
- Flat **~54 candidates/title**, independent of corpus size.
- **~750k updates/sec/core** with immediate (epoch) visibility; build **~650k queries/sec/core**.
- LSM read-amplification stays bounded as segments grow (1→8): candidates/title flat, throughput ~2×
  off, filter skip rate climbing toward ~87% — table in [`performance/results.md`](performance/results.md) §7.
- **Resident memory (mmap profile, ADR-020):** ~148 → **~4.5 B/query** with `retain_source=false`
  (source store + reverse index both off-heap) — ~33× (~14.5 GB → ~0.45 GB extrapolated to 100M).

---

## Roadmap (design-only, prioritized)

Priority follows the bottleneck analysis ([`performance/results.md`](performance/results.md) §9): the
selective match path is already ~255× the spec target with a flat ~54 candidates/title, so the leverage
is in the **broad lane**, **memory/footprint**, and the **durability + scale** story — not in shaving
the selective candidate count further.

### Tier 1 — highest leverage (the measured bottlenecks)

- **Broad-lane batch / columnar evaluation.** Class-C queries are classified and isolated today but
  still evaluated per-title (~9× slower than selective). Specified design: batch/columnar scans over a
  title batch + precomputed/materialized subscriptions for the broadest, metered to a higher cost
  class. The single biggest matching-performance lever. ([`design/matching.md`](design/matching.md) §4.)
- ~~**Memory: resident-footprint reduction.**~~ **✅ Shipped (ADR-020).** Phase-0 measurement showed
  resident RAM (once the SoA/index are mmap'd) is dominated by the **source store** (91 B/q) and the
  **reverse index** (53 B/q), *not* the dict. Both are now off-heap — lazy on-disk source store +
  flat mmap'd logical-index columns — dropping resident from **~148 → ~4.5 B/query** (~33×; ~14.5 GB →
  ~0.45 GB at 100M). Deferred as not worth it *for memory*: dict arena/mmap (bounded, ~3.5 B/q — its
  separate un-versioned-manifest correctness hazard is future work) and tighter SoA packing (paged —
  helps disk/throughput, not resident RAM).

### Tier 2 — feature-model quality & self-tuning

- **Compaction-that-improves.** The merge mechanic is done; add the "improve" phase — recompute stats
  and re-anchor queries whose anchor drifted hot, repacking covers during a merge that's already
  happening. ([`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) §7.)
- **Wire the NPMI learner as the runtime vocab source.** The `learn.rs` corpus learner and the `Vocab`
  runtime plumbing both exist but aren't connected; wiring them lets the feature model self-derive from
  the corpus. ([`research/corpus-feature-learning.md`](research/corpus-feature-learning.md).)
- **Alias / equivalence learning** (e.g. `UD` ≡ `Upper Deck`) with the precision-first safety rail
  (expansion-not-collapse, feedback-validated, reversible) — the one feature-learning sub-problem that
  can affect correctness, so it stays confidence-gated.

### Tier 3 — scale & production maturity (larger builds)

- **Feature-model versioning + blue/green re-materialize.** Frozen common-mask across minor versions;
  a major model change is replayed from the log into a parallel index, then an atomic alias/epoch swap.
- **Clustering.** Consistent-hash entity-anchor sharding, content routing, quorum cluster-manager,
  autoscaling, broad-lane replication — the 100M-query horizontal-scale story.
  ([`design/clustering-and-scaling.md`](design/clustering-and-scaling.md).)
- **Aspects-first ingestion.** Use eBay structured item-specifics as features instead of relying only
  on title parsing — higher feature quality, but a larger domain integration.

### Tier 4 — ES/OS percolator parity (not fully verified — based on initial gap analysis)

These items would close the remaining gaps between Reverse Rusty's DSL/normalizer and what
production ES/OS percolator deployments typically rely on. They are based on a preliminary
comparison with a real-world percolator workload; the scope of each may shrink or grow once
implementation begins.

- **Byte-cleaning: punctuation-equivalence rules.** `clean_into` currently maps all
  non-alphanumeric, non-marker characters to a space. Production title corpora treat
  mid-word hyphens (`-`), apostrophes (`'`, `'`), slashes (`/`), and periods differently
  — e.g. `O'Brien`, `O-Brien`, and `OBrien` should all normalize to the same token. Add a
  configurable punctuation-folding table to the byte-cleaning pass so callers can declare
  which characters collapse vs. become word boundaries.
  ([`normalization.md`](design/normalization.md) §2.)
- **`NormalizerBuilder`: bulk synonym / alias registration API.** The builder already
  supports phrases and single-token synonyms, but real deployments need to register
  hundreds of equivalences (abbreviation → canonical, variant spellings, term expansions
  like `auto` ≡ `{autograph, autographed, signature, signed}`). Add a batch registration
  method and/or a file-based vocabulary loader so large synonym tables are easy to maintain
  outside of code.
- **Metadata-aware result filtering.** ES/OS percolator queries are typically stored alongside
  structured metadata (entity type, category, status) and filtered at search time via bool
  clauses. Reverse Rusty today returns raw query-ID sets with no metadata awareness. Options:
  per-query tag storage with post-match filtering, or partitioned indices. Design TBD —
  the goal is to support the common pattern of "percolate title, then narrow by category"
  without requiring a separate metadata lookup.
- **Match scoring / ranking hooks.** ES/OS percolator returns `_score` from the stored
  query's relevance model; production consumers use `function_score` wrappers to boost
  results by metadata (e.g. status priority). Reverse Rusty currently returns binary
  match/no-match. Add an optional scoring callback or rank-annotation layer so callers
  can order results without a separate pass.

### Polish / niche

- **SIMD intersection** for medium/large (mostly broad-lane) roaring postings — a micro-optimization
  best folded into the broad-lane work above.

### Evaluated & declined

- **Query-family / shared-prefix DAG** (subtree pruning). Implicit anchor-sharing already captures the
  near-duplicate-clustering benefit, the selective path isn't the bottleneck, and the
  mmap-serialization + compaction-rebuild cost wasn't justified. See [`DECISIONS.md`](DECISIONS.md)
  ADR-019.

---

## Nice-to-have / operational polish backlog

Low-priority polish, ergonomics, and micro-optimizations — none are production blockers (moved here
from the audit's former P3 list). Roughly grouped:

**API / ops ergonomics**
- **No CORS headers** — browser-based tools can't hit the API. Add `tower-http::CorsLayer`.
- **No `--version` flag** in the CLI.
- **No Dockerfile or k8s manifests.**
- ~~**No segment detail endpoint** (`/_cat/segments`).~~ **✅ Shipped (ADR-023).** `GET /_cat/segments`
  returns per-segment detail — kind (memory/mmap/memtable), entries/alive/deleted, holes ratio, vocab
  epoch + stale flag, and a resident-vs-overhead byte split — as a text table or `?format=json`, read
  lock-free from the snapshot. Two follow-ups it deliberately deferred are tracked as their own items
  below (per-segment filter FP rate; `_cat` verbose/column-selection flags).
- **No thread-pool introspection** (`/_cat/thread_pool` equivalent).
- **No per-segment filter FP rate in `/_cat/segments`** (deferred from ADR-023). The anchor filter doesn't
  retain its inserted key count, and the mmap arm doesn't expose the filter's block count through the
  `BaseSegment` wrapper — so an honest, *symmetric* false-positive-rate column (real for both memory and
  mmap segments) needs a small change first: have `SegmentFilter` retain `n` at build time and expose
  block count on `MmapSegment`. Then add a `filter_fp_pct` column to the endpoint.
- **`_cat` endpoints lack ES `?v` / `?h` / `?help` flags** (noted in ADR-023). `/_cat/*` returns a fixed
  text table (always with a header) or `?format=json`; ES also supports a verbose toggle, column
  selection, and a help listing. Low-value polish, listed for completeness.
- **`took_ms` uses raw f64** — yields values like `0.003284000000000001`. Use integer ms or round to 2 dp.
- **No pre-warming** for mmap'd segments on cold start.

**Memory / hot-path micro-optimizations**
- **`alive: Vec<bool>`** uses 8× the memory of a bitvec (1 byte vs 1 bit per entry).
- **`seg_lens` Vec allocated on the match hot path** — could be a fixed-size array.
- **WAL `append_insert` allocates a Vec per write** — production WALs use pre-allocated write buffers.
- **Byte-at-a-time CRC-32** for manifest writes — table-based would be ~10× faster.

**Robustness / build hygiene**
- **Durable-ingest segment-write failures surface only as `ingest_rollback`, not `segment_write`.** ADR-021
  routes the *flush* path's segment write through a precise `DurabilityOp::SegmentWrite`, but the durable
  build/bulk path (`build_durable_base`) returns the `io::Error` up to the infallible wrapper, which emits
  `IngestRollback` with the OS error in the `error` field — so the operator sees the cause but not the
  precise op label (unlike a manifest failure, which emits both `manifest_write` + `ingest_rollback`).
  Optional refinement: emit `SegmentWrite`/`SegmentMmap` from inside `build_durable_base` for symmetric
  labeling. Low priority — the underlying error is already visible.
- **Dict format not versioned** — adding a new `FeatureKind` variant would silently corrupt deserialization.
- ~~**`GET /_vocab` acquires the write mutex.**~~ **✅ Fixed.** `EngineSnapshot` now carries the vocab as
  an `Arc<Vocab>` (the `Engine` holds `Option<Arc<Vocab>>`, `Arc::clone`d into each snapshot — O(1) per
  publish), and `get_vocab` reads `state.snapshot.load().vocab()` instead of locking the engine. Vocab
  reads are now lock-free like every other read endpoint, closing the last ADR-016 violation. (No new
  ADR — this completes ADR-016's stated design.)
- **Server/observability deps are not feature-gated** — the library crate unconditionally compiles
  `axum`/`tokio`/`tower`/`uuid`/`prometheus` even for pure-engine embeddings. Add an optional `server`
  feature to keep the embeddable core lean (compile time, binary size, supply-chain surface).
- ~~**Durability/persistence failures log to stderr, not the observability stack.**~~ **✅ Shipped
  (ADR-021).** All 14 durability/persistence failure sites in
  `src/segment/{lifecycle,ingest,persistence}.rs` (WAL init/append/checkpoint/reset, manifest write,
  segment write/mmap fallback, source-store write/re-map/load, corrupt-segment-skip and torn-WAL-tail
  on recovery) now emit `EngineEvent::DurabilityFailure { op: DurabilityOp, detail, error }` instead of
  `eprintln!`. The server's observer logs each through `tracing` (`error!` for data-at-risk ops, `warn!`
  for display-only/benign ones — `DurabilityOp::is_data_at_risk`) and increments
  `durability_failures_total{op}` for alerting. Construction/recovery failures predate the observer, so
  they are buffered and replayed when `set_observer` is called.

---

## Current limitations

- **Single-node.** Horizontal scale (sharding / routing / autoscaling) is designed but not built — see
  Tier 3 above.
- **Empty default vocabulary.** `default_vocab()` ships no domain terms; vocabulary is supplied at
  runtime via the `Vocab` system or `NormalizerBuilder`. Auto-deriving it from the corpus is the
  NPMI-wiring item in Tier 2.

The former production-hardening audit's medium-priority items — metrics gaps (P2-2), response-envelope
consistency (P2-8), and bulk-ingest lock scope (P2-14) — are now resolved (2026-05-29): P2-2 and P2-8
were implemented and P2-14 was closed as stale/by-design (reads are lock-free since ADR-016). The
audit no longer exists as a separate document; its surviving lower-priority items live in the
Nice-to-have backlog above and in the relevant ADRs.
