# Architecture Decision Records

Lightweight record of the key design decisions in Reverse Rusty. Each entry captures the context,
the decision, and the consequences — so that future contributors (human or agent) understand
*why* things are the way they are, not just *what* they are. Add new entries at the bottom;
ADRs are **append-only and never renumbered** — a superseded or reversed decision is marked, not
deleted (the record of *why something was not done* is as load-bearing as the rest).

## Index

Find an ADR by its number in the records below. (Implementation status of each decision is tracked in
[`STATUS.md`](STATUS.md), not here.)

| ADR | Decision | Status |
|---|---|---|
| 001 | Semantic signatures over term-level gating | Accepted |
| 002 | Integer-only exact verification (no strings on the hot path) | Accepted |
| 003 | Broad-query quarantine via cost classes | Accepted |
| 004 | LSM write path over full rebuild | Accepted |
| 005 | Typed errors over stringly-typed Results | Accepted |
| 006 | Forbidden features never gate (structural enforcement) | Accepted |
| 007 | Three production dependencies (daachorse, roaring, rayon) | Accepted |
| 008 | Deterministic data generation (seeded PRNG) | Accepted |
| 009 | ClickHouse-inspired score-based compaction over RocksDB-style leveled compaction | Accepted |
| 010 | NormalizerBuilder + fallible construction | Accepted |
| 011 | Cache-line blocked bloom over binary fuse / u64-blocked bloom | Accepted |
| 012 | mmap'd segment file format with frozen hash tables | Accepted |
| 013 | Write-ahead log (WAL) for crash recovery | Accepted |
| 014 | Engine-level query source store (not in segment files) | Accepted |
| 015 | Runtime vocabulary learning from query any-of groups | Accepted |
| 016 | Snapshot-based read path (ArcSwap) over global RwLock | Accepted |
| 017 | Durable bulk ingest — segment file is the artifact, manifest is the commit point | Accepted |
| 018 | Bulk ingest reports per-item outcomes (ES-style) | Accepted |
| 019 | Query-family factoring evaluated and declined | **Declined** |
| 020 | Production-scale resident-memory reduction (lazy source store + flat logical-index columns) | Accepted |
| 021 | Durability failures are observable events, not stderr writes | Accepted |
| 022 | ES-style runtime settings API (`GET/PUT /_settings`) | Accepted |
| 023 | Per-segment introspection endpoint (`GET /_cat/segments`) | Accepted |
| 024 | CI via GitHub Actions mirroring `check.sh`; commit pressure tests + benchmark baseline | Accepted |
| 025 | Wire query-complexity limits into the parser (the config knobs were cosmetic) | Accepted |
| 026 | Broad-lane batch / columnar evaluation (`match_titles_batch`, `POST /_mpercolate`) | Accepted |
| 027 | In-process multi-shard core — shared frozen dict, feature-anchor ring, designated replicated lane | Accepted |
| 028 | Feature-gate the server/observability stack behind a default-on `server` feature (lean core) | Accepted |
| 029 | gRPC `ShardServer` + the local↔remote `trait Shard` seam (clustering step 1, networking) | Accepted |
| 030 | Dict-fingerprint handshake + fallible cluster construction (ADR-029 sharp-edge closure) | Accepted |

---

### ADR-001: Semantic signatures over term-level gating

- **Context:** Generic percolators (Lucene Monitor, ES/OS) gate on raw terms extracted from
  queries. This works for full-text search, but product queries have structure — the same word
  means different things in different positions ("jordan" = player, brand, or year subset).
  Term-level gating retrieves too many false-positive candidates.
- **Decision:** Gate on 2–3 *semantic* feature combinations (e.g., `player:jordan +
  year:1994 + grader_grade:psa10`) produced by a domain-aware normalizer, rather than raw
  terms.
- **Consequence:** Flat ~54 candidates/title regardless of corpus size (measured 1M–5M).
  Requires a shared normalizer that maps both queries and titles into the same feature space.
  Makes the system domain-specific rather than generic.

### ADR-002: Integer-only exact verification (no strings on the hot path)

- **Context:** Most percolators re-run a scorer or mini-query-engine on each candidate. This
  pulls in string comparison, regex, allocation, and virtual dispatch — expensive per
  candidate.
- **Decision:** Push all parsing, normalization, and AST interpretation into compile time.
  The match-time exact check uses only `u64` mask operations and sorted `u32` slice
  galloping. No strings, regex, allocation, or generic AST interpretation on the hot path.
- **Consequence:** ~710k titles/sec/core. The common-mask gate (two `u64` reads) rejects
  most candidates before any further memory traffic. Trade-off: any change to query semantics
  requires recompilation of the affected query.

### ADR-003: Broad-query quarantine via cost classes

- **Context:** Some queries are inherently non-selective (e.g., a bare "jordan" with no
  year/grade). In a flat index these poison the hot path — one broad posting list can
  dominate match time.
- **Decision:** Classify queries at compile time into cost classes A/B/C/D. Class C (too
  common to be selective) is routed to a separate batch/columnar lane. Class D (effectively
  unconstrained) is rejected with rewrite suggestions.
- **Consequence:** The selective (A/B) path stays fast and predictable. Broad lane is ~9×
  slower but isolated. Class D rejection forces query authors to add specificity.

### ADR-004: LSM write path over full rebuild

- **Context:** The naive approach is to rebuild the entire index when queries change. At
  100M queries this is unacceptable (minutes of unavailability or double-buffering cost).
- **Decision:** Log-structured (LSM) write path with immutable segments + a mutable memtable
  (hot delta) + tombstones. Writes append to the memtable and become visible immediately via
  an atomic epoch swap. Segments are never mutated once sealed.
- **Consequence:** ~750k updates/sec/core with immediate visibility. Full rebuild is reserved
  for the initial seed and major feature-model changes (blue/green from the log, not
  stop-the-world). Read amplification grows with segment count — compaction caps it (ADR-009).
- **See also:** [ingestion-and-updates.md](design/ingestion-and-updates.md)

### ADR-005: Typed errors over stringly-typed Results

- **Context:** Early on, Reverse Rusty used `Result<_, String>` for parse failures and silently dropped
  rejections during ingest. This made debugging and accounting difficult.
- **Decision:** Introduce `ParseError { kind, pos }` with a `#[non_exhaustive]`
  `ParseErrorKind` enum implementing `Display` + `std::error::Error`. Ingest paths return
  `IngestReport` with separate counts for parse rejections vs class-D rejections. Added
  `try_insert_live` that surfaces typed errors.
- **Consequence:** Callers get inspectable, composable errors. No `unwrap()` in library code.
  Rejection accounting is accurate. Back-compat preserved via `insert_live` wrapper.

### ADR-006: Forbidden features never gate (structural enforcement)

- **Context:** Gating on MUST_NOT features is tempting (they look selective) but lethal for
  correctness — a title that *lacks* a forbidden feature would not be retrieved, causing a
  false negative.
- **Decision:** The signature optimizer literally cannot see forbidden features. They exist
  only in the exact-match plan. This is enforced structurally (code path), not by convention.
- **Consequence:** Zero false negatives for the MUST_NOT case, by construction. The
  differential oracle verifies this over millions of (title, query) pairs.
- **See also:** The correctness contract in [design/README.md](design/README.md) §2

### ADR-007: Three production dependencies (daachorse, roaring, rayon)

- **Context:** Reverse Rusty started std-only with hand-rolled alternatives (token-trie for alias
  matching, Vec-only postings, single-threaded matching).
- **Decision:** Replace each hand-rolled component with the production-grade crate once the
  design was validated: daachorse v3 for O(n) multiword alias matching, roaring v0.10
  for compressed bitmaps on large postings, rayon v1 for data-parallel matching.
- **Consequence:** Identical semantics with better performance characteristics. daachorse
  gives O(n) scan time regardless of vocab size. Roaring compresses large postings (>256
  entries) and enables future SIMD intersection. Rayon delivers ~3.8× speedup on 4 threads.
  Zero other external dependencies.

### ADR-008: Deterministic data generation (seeded PRNG)

- **Context:** Benchmarks and correctness tests need synthetic data that models adversarial
  cases (hot-entity skew, broad queries, near-duplicate families).
- **Decision:** Use a deterministic SplitMix64 PRNG with no external crates. All data
  generation in `gen.rs` is seeded and reproducible.
- **Consequence:** Benchmark numbers are reproducible across runs. The oracle test is
  deterministic. Adversarial patterns (skew, families) are configurable parameters, not
  random noise.

### ADR-009: ClickHouse-inspired score-based compaction over RocksDB-style leveled compaction

- **Context:** Compaction needs to bound the base segment count because percolation (unlike a
  KV point read) must probe **every** segment for every incoming title — read amplification
  is proportional to segment count. The initial design (ADR-004, ingestion-and-updates.md)
  specified RocksDB-style tiered L0 + leveled below. However, RocksDB's level/tier structure
  is optimized for *point reads* (stop at the first SSTable containing the key + Bloom skip).
  We can't stop early — our reads look like ClickHouse `SELECT * FROM table`, not `GET key`.
- **Decision:** Use a ClickHouse SimpleMergeSelector-inspired score-based greedy algorithm.
  Evaluate every contiguous range of ≥2 base segments with the scoring function
  `(sum_size + FIXED_COST * count) / (count - 1.9)`. Pick the lowest-scoring range and
  merge it. The `FIXED_COST` biases toward merging small segments first (cheap wins). This
  directly minimizes the time-integrated average segment count — the exact metric that drives
  our read performance. No level metadata, no L0/L1/L2 distinction. Also provide `compact_all()`
  and `compact_range(lo, hi)` for direct control.
- **Consequence:** Simpler than leveled compaction (no levels to track), directly optimizes for
  our performance objective (minimum segment count over time), and produces O(log N) merge tree
  depth with O(N log N) total work. The `compact(max_segments)` trigger is the only policy knob.
  Verified by two oracle tests (compact-all, compact-range): zero false negatives, zero false
  positives, identical match results pre- vs post-compaction.
- **See also:** [ingestion-and-updates.md](design/ingestion-and-updates.md) §5–6

### ADR-010: NormalizerBuilder + fallible construction

- **Context:** The `Normalizer::default_vocab()` constructor was the only way to build a
  normalizer, hardcoding the trading-card vocabulary. It also used `.expect()` on the daachorse
  automaton build — the sole panicking call in library code, violating the no-`unwrap()` invariant
  (ADR-005). Core types lacked `Debug` impls and `Send`/`Sync` was not verified at compile time.
- **Decision:** Four changes: (1) Convert `default_vocab()` to return `Result<Self,
  NormalizerError>`, introducing `NormalizerError` in `error.rs`. (2) Add `NormalizerBuilder` with
  a fluent API for assembling custom vocabularies — phrases, synonyms, graders, grade words — so
  the engine is domain-agnostic. `default_vocab()` now builds an empty normalizer (no hardcoded
  vocabulary); domain vocabulary is supplied at runtime via the `Vocab` system (ADR-015) or
  directly via `NormalizerBuilder`. (3) Add
  `Debug` impls (derive or manual) to all public types. (4) Add compile-time `Send`/`Sync`
  assertions on all key types in `lib.rs`.
- **Consequence:** Zero panicking calls in library code. Downstream callers can build normalizers
  for any product domain, not just trading cards. `Debug` + `Send`/`Sync` make the engine safe for
  production server use (behind `Arc<Mutex<Engine>>`, in `dbg!()` traces, etc.). `NormalizerError`
  wraps the daachorse error as a string to avoid leaking the dependency into the public API.

### ADR-011: Cache-line blocked bloom over binary fuse / u64-blocked bloom

- **Context:** The multi-segment LSM layout requires each incoming title to probe every segment.
  Read amplification is proportional to segment count. Per-segment anchor filters can skip
  probes that would definitely miss. The comparison point for each probe is a hash-map miss
  with an identity hasher — approximately one memory access (~1 cache line). Three approaches
  were evaluated:
  (A) **Binary fuse filters** (xorf crate): 3 parallel memory accesses, ~9 bits/key, ~0.4%
      FPR. Best space efficiency. Adds a 4th dependency, requires key deduplication, and
      construction can fail (needs retry with a different hash seed).
  (B) **Cache-line blocked bloom** (RocksDB Full Filter, Putze et al. 2007): 1 cache-line
      access (512-bit block = 64 bytes), ~10 bits/key, ~1% FPR. No new dependency. Proven
      over 10+ years in RocksDB production.
  (C) **u64-blocked bloom** (our initial attempt): 1 memory access, ~16 bits/key, ~0.5–2%
      FPR. Non-standard 64-bit block size is too narrow — only 5 fingerprint bits fit,
      leading to variable FPR under load.
  An earlier attempt with a classic scattered-probe bloom (7 random probes) made throughput
  **worse** than no filter (620k→167k at 8 segments vs baseline 779k→461k), confirming that
  multiple cache-line accesses per check exceed the hash-map miss budget. RocksDB abandoned
  scattered-probe blooms for the same reason circa 2014.
- **Decision:** Cache-line blocked bloom (Option B). 512-bit blocks with 6 probes via
  Kirsch-Mitzenmacher double hashing. No external dependency. Construction never fails.
  One cache-line access per check.
- **Consequence:** Filter skip rates scale properly with segment count (47%→87% at K=1→8).
  Memory is compact (~0.07 MB for 300k queries across 8 segments). Zero false negatives
  (verified by oracle). In-memory, net throughput improvement is modest because the filter
  check cost (~1 cache-line) approximately equals the hash-map miss it replaces. The real ROI
  comes when segments are mmap'd from disk — a hash-map miss against an mmap'd segment is a
  potential page fault (microseconds), while the in-memory filter check stays at nanoseconds.
  No percolation system in the literature (Lucene Monitor, ES Percolator, Luwak) implements
  segment-skip filters — this is novel to Reverse Rusty.
- **See also:** [ingestion-and-updates.md](design/ingestion-and-updates.md) §6

### ADR-012: mmap'd segment file format with frozen hash tables

- **Context:** Sealed segments are immutable — perfect for memory-mapped I/O. The in-memory
  representation uses `HashMap<u64, Posting>` for the candidate index, which can't be mmap'd
  (pointers, allocator metadata). The ExactStore is already struct-of-arrays (flat `Vec`s),
  so it maps naturally to flat byte regions. The key design question is what replaces HashMap
  on disk.
- **Decision:** Custom binary segment file format (`.seg`) with five sections:
  (1) **ExactStore** — each SoA array written sequentially with a count prefix and 8-byte
      alignment padding; directly castable to typed slices via mmap.
  (2) **CandidateIndex** (main + broad) — **frozen open-addressing hash table** with linear
      probing. Each slot is `(key: u64, offset: u32, len: u32)` = 16 bytes. Postings are
      flattened into a contiguous `[u32]` blob; slots point into it. Capacity is next-power-of-2
      above 2× entry count (~50% load factor). Probe uses the sig_key directly (already
      well-mixed via FNV-1a + avalanche).
  (3) **SegmentFilter** — raw u64 array + metadata (num_blocks, mask).
  (4) **Metadata** — cost classes as `[u8]`, alive flags as `[u8]`.
  File header (80 bytes) carries magic, version, query count, and byte offsets to each section.
  Written atomically via tmp-file + rename. No new dependencies (uses `memmap2` only for
  reading).
- **Consequence:** Zero-copy reads — `MmapSegment::open()` mmaps the file and casts pointers
  directly to typed slices. The frozen hash table trades ~2 probes/lookup (at 50% load) for
  zero allocation and OS-managed paging. Cold segments stay on disk; hot segments live in the
  page cache. The per-segment anchor filter (ADR-011) now delivers its full ROI: a bloom check
  in L1 cache skips a frozen-table probe that might page-fault. Backward compatible: engines
  without `data_dir` remain fully in-memory; no existing test changes.
- **Alternatives considered:** (A) Minimal perfect hashing (CHD/MPHF) — optimal space but
  requires an external crate or complex implementation; construction can fail. (B) Sorted array
  with binary search — O(log n) per probe vs O(1) average for open addressing; too slow for
  the hot path. (C) Serializing the Rust HashMap directly — non-portable, not mmap-safe.

### ADR-013: Write-ahead log (WAL) for crash recovery

- **Context:** With mmap'd segments, sealed data is durable on disk. But the mutable memtable
  (hot delta) lives in memory and is lost on crash. Without a WAL, all un-flushed inserts and
  tombstones are lost. The design doc specifies a durable mutation log as the source of truth
  (§3 of ingestion-and-updates.md).
- **Decision:** Simple append-only WAL (`wal.log`) with framed entries. Each entry:
  `[body_len: u32, crc32: u32, seq: u64, op: u8, payload...]`. Three operations: Insert
  (stores logical_id + version + query text), Tombstone (seg_idx + local_id),
  FlushCheckpoint (segment filename). CRC-32 per entry detects torn writes from crashes.
  Recovery: scan forward, skip entries with bad CRC, replay only entries after the last
  FlushCheckpoint (earlier entries are already materialized in sealed segments).
  WAL is written before memtable mutation (WAL-first = durability before visibility), and
  **a WAL append failure rejects the mutation**: the in-memory state is left untouched and the
  error is surfaced to the caller (`WriteError::Wal` from `try_insert_live`, `io::Result` from
  `tombstone`/`delete_by_logical_id`) — never swallowed (P1-17). Query parsing happens *before*
  the WAL append, so a malformed query (`WriteError::Parse`) never reaches the log. Two fsync
  policies via `EngineConfig::wal_sync_on_write` (P1-17): the default (`false`) `write(2)`s each
  append to the OS page cache and fsyncs only at flush checkpoints — an acknowledged write
  survives a process crash but not power loss until the next checkpoint (RocksDB `sync=false` /
  SQLite `NORMAL`); `true` fsyncs every append before acknowledging, so an acknowledged write
  survives power loss (SQLite `FULL`), at a large per-write latency cost. Checkpoints are always
  fsync'd.
- **Consequence:** Crash recovery is correct: replaying the WAL after the last checkpoint
  reproduces the exact memtable state. CRC-32 detects partial writes. The WAL is reset after
  compaction + manifest write (all data is in segments). No new dependencies (CRC-32 is
  hand-rolled, ~15 lines). Trade-off (measured): the default checkpoint-only policy costs
  ~3 µs/append (page-cache `write(2)`); enabling `wal_sync_on_write` raises that to ~4 ms/append
  — one device flush per mutation, ~1300x slower — in exchange for power-loss durability. A
  failed WAL append rejects the write rather than degrading durability silently, so callers can
  retry (the server maps `WriteError::Wal` to HTTP 503).
- **See also:** [ingestion-and-updates.md](design/ingestion-and-updates.md) §3

### ADR-014: Engine-level query source store (not in segment files)

- **Context:** The audit (P1-7) identified that search results returning bare integer IDs with
  no query text made the API effectively write-only. Implementing `GET /_doc/{id}` and rich
  search hits requires storing original query text somewhere. Three options: (A) add a source
  section to `.seg` files, (B) per-segment `.src` sidecar files, (C) engine-level
  `FastMap<u64, String>` persisted as a single `sources.dat` file.
- **Decision:** Option C — engine-level `FastMap` with a separate `sources.dat` binary file
  (atomic tmp+rename). Query text is populated on every ingest path and removed on delete.
  Persisted on flush alongside the manifest. Loaded on `Engine::open()`, with WAL replay
  adding memtable entries on top. Uses `FastMap` (identity hasher) consistent with all other
  u64-keyed maps in the codebase.
- **Consequence:** Query text is never on the match hot path (ADR-002 preserved). Segment
  files stay optimized for matching — no bloat from string data in mmap'd regions. Source
  lookups happen only in response formatting (after matching completes) or `GET /_doc/{id}`.
  Memory overhead is proportional to total query text (~20 bytes/query average). The
  `include_source: false` option lets clients skip source lookup entirely when they only need
  IDs. Trade-off: `sources.dat` is a full rewrite on every flush; at very large scale, a
  per-segment approach (option B) would amortize writes.
- **Alternatives rejected:** (A) Adding to `.seg` format would bloat mmap'd memory with data
  never accessed during matching, violating the hot-path budget. (B) Per-segment sidecars add
  complexity for compaction merging and are premature at current scale.

### ADR-015: Runtime vocabulary learning from query any-of groups

- **Context:** ADR-010 made the normalizer domain-agnostic via `NormalizerBuilder`, but
  vocabulary still had to be supplied manually. For a new domain the operator has no good way
  to bootstrap a vocabulary. Query any-of groups (e.g., `(rc, rookie)`) are an organic source
  of synonym relationships — the query author is asserting that the members are interchangeable
  in their intent. Mining these at runtime avoids the need for an external corpus pipeline.
- **Decision:** Add `Vocab` struct (`src/vocab.rs`) that holds learned synonyms, phrases, and
  graders. `Vocab::learn_from_queries()` extracts synonyms from stored query any-of groups
  using frequency and co-occurrence thresholds. The engine exposes `set_vocab()` to replace
  the normalizer vocabulary at runtime, plus REST endpoints (`GET/PUT /_vocab`,
  `POST /_vocab/learn`). Vocabulary is persisted as JSON via `--vocab-file`.
- **Consequence:** Bootstrapping a new domain requires only ingesting queries — the system
  can learn its own vocabulary. **Hazard:** `set_vocab()` replaces the normalizer without
  recompiling existing queries. Until queries are reingested, the "same normalizer for queries
  and titles" invariant (ADR-001) is violated in practice. **Enforcement:** A monotonic
  `vocab_epoch` counter on the engine is incremented on each `set_vocab()` call. Every
  segment (base and memtable) is stamped with the epoch at which its queries were compiled.
  `Engine::stale_segment_count()` / `has_stale_segments()` reports how many segments are
  out-of-date; `set_vocab()` returns this count. `EngineMetrics::stale_segments` and the
  `/_health` endpoint (yellow status when stale) make staleness visible to operators.
  Compaction preserves the minimum epoch of merged segments (still stale if any source was).
  A production system would additionally need blue/green rematerialization (see design-only:
  feature-model versioning). `serde` becomes a library dependency (via `Vocab` serialization),
  previously it was server-only.
- **See also:** ADR-010 (NormalizerBuilder), [normalization.md](design/normalization.md),
  [corpus-feature-learning.md](research/corpus-feature-learning.md)

### ADR-016: Snapshot-based read path (ArcSwap) over global RwLock

- **Context:** The HTTP server held the engine behind a single `RwLock<Engine>`. Every write
  operation (put_doc, bulk_ingest, flush, compact) took a write lock, blocking all concurrent
  reads. Flush and compaction involve disk I/O (seal + write + fsync) that can take hundreds of
  milliseconds, during which search, stats, health, and metrics were all blocked. ES uses a
  refresh model (readers see last-refreshed snapshots). RocksDB uses version sets (readers hold
  immutable snapshot refs with generation-number fast path). ClickHouse merges against immutable
  parts with atomic pointer swaps.
- **Decision:** Replace `RwLock<Engine>` with `Mutex<Engine>` (write serialization only) plus
  `ArcSwap<EngineSnapshot>` (lock-free reads). `EngineSnapshot` is a frozen, read-only view of
  the engine state built entirely from `Arc` handles: `Arc<Normalizer>`, `Arc<Dict>`,
  `Vec<Arc<BaseSegment>>`, `Arc<Segment>` (memtable), and `Arc<RwLock<QueryStore>>`. Publishing a
  snapshot is a handful of `Arc::clone`s — no deep copy of any engine structure (see the
  structural-sharing refinement below). Read endpoints (`/_search`,
  `/_stats`, `/_health`, `GET /_doc/{id}`, `/_metrics`) load the snapshot via `ArcSwap::load()`
  — zero contention, zero blocking. Write endpoints acquire the `Mutex`, mutate the engine,
  then call `publish_snapshot()` which atomically stores a new `Arc<EngineSnapshot>`.
  `EngineSnapshot` implements all read operations (`match_title`, `match_titles_par`, `metrics`,
  `explain_hit`, etc.) directly. Added `arc-swap = "1"` as a dependency (~200 lines, no
  transitive deps). Changed `Engine.norm` to `Arc<Normalizer>` and `MmapSegment.mmap` to
  `Arc<Mmap>` so snapshots share large immutable data without cloning.
- **Consequence:** Reads are fully non-blocking — a compaction that takes seconds no longer
  stalls search traffic. Write-to-read visibility is immediate (publish after every mutation).
  Snapshot creation is O(1) in the corpus size — a fixed number of `Arc::clone`s plus a
  `Vec<Arc>` clone whose length is the segment count (tens, not millions). `EngineSnapshot` is
  `Send + Sync` (verified by compile-time assertion). Benchmark: 894k titles/sec/core selective
  — no regression from snapshot indirection. The `Mutex` still serializes writes, which is
  correct (concurrent writes to the LSM engine would violate internal invariants).

- **Refinement (structural sharing):** The original implementation deep-cloned the entire engine
  on every publish (`Arc::new(self.dict.clone())`, deep-copied segments/memtable/query store),
  making writes O(total engine size) — a single PUT on a 1M-query engine cost ~82 ms and the cost
  grew linearly with the corpus. The engine now holds `dict: Arc<Dict>`,
  `segments: Vec<Arc<BaseSegment>>`, `memtable: Arc<Segment>`, and
  `query_store: Arc<RwLock<QueryStore>>`; mutations use copy-on-write (`Arc::make_mut` for the
  dict and memtable, which are bounded by vocab/memtable size) and shared interior mutability
  (the `RwLock` query store is mutated in place, so it is never copied on publish). Sealed base
  segments are immutable and shared by `Arc::clone`. Result: PUT + publish dropped from 82 ms to
  ~2 µs at 1M queries (~40,000×), and snapshot/PUT/DELETE publish cost is now flat across corpus
  size (verified by `src/bin/snapbench.rs`). `std::sync::RwLock` is used (not `parking_lot`) to
  keep the core std-only; the poison case is recovered with `.unwrap_or_else(|e| e.into_inner())`
  (release builds use `panic = "abort"`, so poisoning cannot occur there).
- **Dependency:** `arc-swap v1` — used by TiKV, crossbeam, and other high-concurrency Rust
  infrastructure. Lock-free atomic `Arc` swaps with epoch-based reclamation.
- **See also:** [ingestion-and-updates.md](design/ingestion-and-updates.md)

### ADR-017: Durable bulk ingest — segment file is the artifact, manifest is the commit point

- **Context:** `bulk_ingest` / `build_from_queries` compile a batch directly into a fresh base
  segment, deliberately bypassing the WAL (ADR-013). The audit (P1-15) flagged that this path
  could lose acknowledged data silently: on a segment-write or mmap failure, `make_base_segment`
  printed to stderr and *fell back to an in-memory segment*, so the engine reported success
  (`IngestReport { ingested: N }`) for data that was never persisted and would vanish on the next
  restart. The manifest-write result was likewise ignored before returning the report. Source
  text was never persisted by these paths at all (only `flush` wrote `sources.dat`), so even a
  clean restart lost the source text of bulk-ingested queries.
- **Research:** RocksDB's `IngestExternalFile` is the canonical model. It does *not* WAL the
  ingested data — that would be a redundant double-write of data already in a durable file.
  Instead the SST file *is* the durable artifact (fsync'd), and the atomic MANIFEST update that
  references it is the linearization/commit point; ingestion is all-or-nothing ("if the status is
  non-OK, none of the files are ingested"). Our segment file already fsyncs via tmp-write +
  `sync_all` + atomic `durable_rename` (P2-13), and `Engine::open` ignores any segment file not
  listed in the manifest — so we already had RocksDB's two ingredients. The bug was purely in
  error *handling*, not durability mechanics. So the fix is to surface failures and make the
  commit atomic, **not** to WAL bulk entries.
- **Decision:** Add fallible `try_bulk_ingest` / `try_build_from_queries` returning
  `io::Result<IngestReport>`; the existing infallible `bulk_ingest` / `build_from_queries` become
  thin wrappers that log + set `persistence_healthy = false` + return an empty report on `Err`
  (preserving the in-memory-mode and test/demo call sites). A shared `commit_base_segment` makes
  the batch all-or-nothing: (1) `build_durable_base` writes the segment file and mmaps it,
  propagating any I/O error instead of falling back to memory; (2) the segment is appended and the
  manifest written — the atomic commit point, which both references the new file and embeds the
  updated dict; (3) on manifest failure the in-memory segment is popped and the orphan file
  deleted, so nothing is committed. Accepted source text is collected locally and applied to the
  (display-only) query store + persisted to `sources.dat` *after* the commit point — bulk has no
  WAL backstop, so this is the sole point at which bulk source text becomes durable. The server
  maps a bulk persistence failure to HTTP 503 (`/_bulk`) and aborts startup on initial-load
  failure rather than serving a non-durable engine.
- **Consequence:** A bulk batch is now durable-or-rejected: callers never get a success report for
  data that isn't on disk. Match data (segment + manifest) is strictly all-or-nothing; a
  `sources.dat` failure *after* the commit point is surfaced via `persistence_healthy` but does
  not un-commit the already-durable match data (source text is display-only, never on the match
  path). Auxiliary in-memory state interned during a rejected batch (new dict features, rejection
  counters) is not rolled back — like RocksDB, internal stats reflect the attempt; only the
  committed data set is transactional. Cost: each bulk call now also rewrites `sources.dat`
  (O(corpus)); the manifest write (full dict serialization) was already O(corpus) per call, so
  this adds a constant factor, not a new asymptotic class (measured ~63 ms → 340 ms for a
  200-query bulk as the base corpus grows 10k → 100k). Shrinking that per-call cost (chunking,
  incremental sources, I/O outside the write lock) is tracked separately as audit P2-14. Also
  fixed in passing: bulk-ingested segments now carry the current `vocab_epoch` (they were
  defaulting to 0 and so were counted permanently stale once any vocab was set).
- **See also:** ADR-013 (WAL — the live-insert path this deliberately contrasts with), ADR-012
  (segment file format + manifest), ADR-014 (query source store), P2-13 (`durable_rename`),
  [ingestion-and-updates.md](design/ingestion-and-updates.md)

---

### ADR-018: Bulk ingest reports per-item outcomes (ES-style)

- **Context:** `POST /_bulk` reported only an aggregate `IngestReport` (counts of ingested / parse-
  rejected / class-D-rejected). Every item that parsed as NDJSON was stamped status 201 — even when
  the engine subsequently dropped its query (a DSL parse error inside the query, or a cost-class-D
  quarantine, ADR-003). The caller saw the batch-level `errors: true` flag but had no way to learn
  *which* items were dropped or why. This diverged from the single-doc `PUT /_doc` path, which
  already returns a per-item 400 with a reason. (audit P1-8)
- **Research:** Elasticsearch's `_bulk` is the reference contract: the batch returns HTTP 200 with
  an `items[]` array in which each item carries its *own* `status` and, on failure, an `error`
  object; a top-level `errors` boolean flags that at least one item failed. The audit suggested two
  options: (a) insert one-by-one via `try_insert_live` for natural per-item results, or (b) have the
  bulk path return per-item outcomes. Option (a) was rejected — it would route bulk through the
  memtable + WAL live-insert path, destroying the all-or-nothing durable-segment commit and the
  single-segment build efficiency (ADR-017) and WAL-ing every entry (the redundant double-write
  ADR-017 explicitly avoids). The two-pass bulk compiler already decides each query's fate per item;
  only the mapping back to input position was being thrown away.
- **Decision:** Option (b). Add a public `IngestItemStatus { Ingested, RejectedParse(ParseError),
  RejectedClassD }` and `try_bulk_ingest_detailed`, returning `(IngestReport, Vec<IngestItemStatus>)`
  with one entry per input query in submission order (`items[i]` describes `queries[i]`).
  `try_bulk_ingest` stays as a thin wrapper that discards the per-item vec, so its other callers
  (infallible wrappers, bench, persistence tests) are untouched. The `/_bulk` handler tracks each
  pair's response slot and maps the engine outcome back onto it: parse and class-D rejections become
  per-item 400s mirroring `PUT /_doc` — parse echoes the typed `ParseError` detail (position + kind),
  class-D uses "query has no anchorable feature (cost class D)". Durability is unchanged
  (all-or-nothing, ADR-017); per-item statuses are reported only once the batch has durably committed.
- **Consequence:** Bulk callers get ES-parity per-item visibility — a partially-bad batch durably
  commits its good items *and* reports exactly which were dropped and why, instead of a silent 201.
  `IngestItemStatus` carries the typed `ParseError` (not a stringified message), keeping the
  diagnostic inspectable end-to-end (ADR-005). The aggregate `IngestReport` is retained and stays
  consistent with the per-item tallies. Cost is one `Vec<IngestItemStatus>` allocation on the cold
  bulk write path (never the match hot path).
- **See also:** ADR-017 (durable bulk ingest), ADR-005 (typed errors), ADR-003 (cost-class-D
  quarantine), the single-doc `PUT /_doc` path it now matches.

### ADR-019: Query-family factoring evaluated and declined

- **Context:** The design carried an explicit **query-family / shared-prefix DAG** as a roadmap item
  (formerly `matching.md` §5, listed in `STATUS.md` as "the next optimization to push selective
  candidates below ~54"). The idea: near-duplicate product queries share a required-feature prefix
  (`1994 upper_deck series0001 michael_jordan` + per-leaf card term / grade / negatives); store the
  shared prefix once and, at match time, check it once — if the title lacks a shared feature, prune the
  whole subtree in one test instead of rejecting each leaf. This ADR records the decision **not** to
  build it, so the rationale is durable and the item is not silently re-added later.
- **Research:** The academic basis is **PRETTI** (prefix-tree set-containment join); its successor
  **LIMIT+** exists *specifically because PRETTI's full prefix tree grows too large*, and bounds the
  depth with a cost model. The same "evaluate a shared predicate once for many rules/subscriptions"
  pattern appears in **RETE** rule engines (alpha-node sharing) and the **Fabret et al.** content-based
  pub/sub *counting algorithm* (examine common predicates first, recursively eliminate groups that
  cannot match). A spectrum was considered: **L1** a posting-prefix gate (one shared-prefix mask+tail
  per anchor posting; gate the whole posting before iterating), **L2** explicit family grouping (a
  two-level prefix→leaf-residual store + family-level dedup), **L3** a full multi-level DAG. The
  lossless-cover contract is preserved by construction in every variant — the shared prefix is a subset
  of each leaf's required features, and forbidden features are never shared or gated — so it would be a
  pure performance optimization (results must be *bit-identical* to today, the strongest possible test).
- **Decision:** **Do not build it.** Keep the *implicit* clustering the candidate index already
  provides: near-duplicates share signature anchors, so a single failed anchor probe drops the whole
  cluster's candidates. Reasons: **(1) it optimizes a non-bottleneck** — the selective path is already
  ~255× the spec target with a flat ~54 candidates/title and a common-mask gate (two `u64` ops,
  ADR-002) that rejects most candidates almost for free; the measured bottlenecks are the **broad lane**
  and **memory bandwidth** (`performance/results.md` §9), which family factoring barely touches (broad
  queries are short and don't share prefixes). **(2) The cost is concentrated in the wrong place** — not
  the algorithm but the mmap `.seg` format (version bump + back-compat read path), the `compact_from`
  rebuild, and a two-level SoA: the bug-prone surfaces, for a speculative and probably-modest win on a
  number that is already excellent. **(3) The literature already walked back** from the unbounded tree
  (LIMIT+). The synthetic generator's clean `family_size=8` clusters also flatter the feature versus
  messy real titles.
- **Consequence:** No `src/family.rs`; `matching.md` §5 is removed; the "four moves vs generic
  percolators" thesis becomes **three** (semantic signatures, integer verification, broad-query
  quarantine). The roadmap redirects that energy to the actual bottlenecks — **broad-lane batch/columnar
  evaluation** and **dictionary interning / tighter SoA** (`STATUS.md`). The decision is **reversible**:
  implicit anchor-sharing is unchanged, so nothing precludes a future *bounded* L1 posting-prefix gate
  if real-data measurement ever justifies it — the entry point would be a measurement spike + L1, gated
  by an on/off differential and the existing oracle, never the full DAG.
- **See also:** ADR-002 (integer verification / common-mask gate — why the verifier is already cheap),
  ADR-003 (broad-lane quarantine — the actual #1 opportunity), `research/prior-art.md` §6 (PRETTI /
  LIMIT+ / FreshJoin), `performance/results.md` §9 (bottleneck analysis).

### ADR-020: Production-scale resident-memory reduction (lazy source store + flat logical-index columns)

- **Context:** Once the exact-match SoA and candidate index are mmap'd (ADR-012), they are paged from
  disk and no longer dominate *resident* RAM. The structures that stay fully in RAM are auxiliary and
  none is on the match hot path — yet the engine's memory accounting counted **none** of them
  (`exact/index/filter_bytes` only), so the reported "~256 B/query" undercounted. We needed to know the
  real resident footprint before optimizing, then shrink it so far more queries fit per node ahead of
  sharding.
- **Research / measurement (Phase 0, the gate):** Added per-component resident accounting
  (`dict_bytes`, `query_store_bytes`, `logical_index_bytes`, `alive_bytes` on `EngineMetrics`, populated
  in both `Engine::metrics` and `EngineSnapshot::metrics`; `BaseSegment` dispatch returns *real* bytes
  for mmap segments) and a persistent (mmap) benchmark mode (the prior `bench` built only an in-memory
  engine and measured the wrong profile). Measured on a reopened (mmap) engine: **~148 B/query
  resident = query_store 91 (61%) + logical_index 53 (36%) + dict 3.5 (bounded) + alive 1**, i.e.
  **~14.5 GB at 100M**, almost entirely two structures that are off the match path. This *reprioritized*
  the original four-item plan: Items 1+2 capture ~97% of the win; Items 3 (alive, 1 B/q) and 4 (dict,
  3.5 B/q and bounded) are negligible for resident RAM. Prior art: Lucene/Tantivy stored fields are
  on-disk/lazy; Lucene/Tantivy term dicts are mmap'd; Arrow/Parquet dictionary encoding.
- **Decision:**
  1. **Item 1 — lazy on-disk source store (Lucene stored-fields model).** `SourceStore` is `Resident`
     (the historical in-RAM map) or `Lazy` (an in-memory overlay of post-flush mutations over an mmap'd,
     binary-searchable file). `sources.dat` **v2** = sorted `(logical_id, blob_off, len)` index + text
     blob + CRC trailer, read with safe `&[u8]` slicing (no raw pointers). `EngineConfig::retain_source`
     selects the mode — **default `true`**, so existing behavior is byte-for-byte unchanged and the
     memory win is opt-in. v1 files still read; lazy open migrates v1→v2. `_source`/explain in lazy mode
     is a cold binary-search + possible page fault, never the match path.
  2. **Item 2 — flat logical-index columns.** Replace the `MmapSegment`'s rebuilt
     `FastMap<u64, Vec<u32>>` reverse index with two sorted parallel columns (`logical: [u64]`,
     `local: [u32]`) serialized into the `.seg` (**FORMAT_VERSION 1→2**, offset in a reserved header
     slot). `MmapSegment` *borrows* them from the mmap (`Mapped` — ~zero resident); a v1 file
     *reconstructs* owned columns from `logical_arr` (`Owned` — flat, far below the old per-`Vec` map,
     reclaimed on recompaction). `locals_for_logical` binary-searches the contiguous run and returns the
     same `&[u32]` sub-slice, so all delete call sites are unchanged. The in-memory `Segment` (memtable +
     fallback) keeps its `FastMap` (bounded, mutable) — untouched.
  3. **Descoped: Items 3 (alive→bitset) and 4 (dict arena+mmap).** Measured contribution is ~0.7% and
     ~2.4% (and dict saturates), not worth the format churn *for memory*. The dict's separate
     un-versioned-manifest correctness hazard (adding a `FeatureKind` corrupts deserialization) remains
     open as future work, justified by correctness rather than bytes.
- **Consequence:** Resident memory drops from **~148 → ~96 B/query** (Item 2 alone, default config) and
  to **~4.5 B/query** with both items + `retain_source=false` (just dict 3.5 + alive 1) — a **~33×**
  reduction, ~14.5 GB → ~0.45 GB at 100M (measured at 200k, `bench` prints both profiles). No match
  semantics change, so the differential oracle is unchanged and green; new formats (`sources.dat` v2,
  `.seg` v2) both retain v1 read paths, tested incl. a v1-reconstruct round-trip. `_source` in lazy mode
  trades instant reads for a cold disk fetch. Cost is contained to `storage.rs` + `config.rs` + a thin
  `segment.rs` wiring layer; the in-memory `Segment` and compaction are untouched.
- **See also:** ADR-012 (mmap segments — the precondition), ADR-014 (extracting `sources.dat` from the
  segment — the precedent), ADR-016 (snapshot publish stays an `Arc::clone`), ADR-017 (durable bulk —
  source becomes durable only at the commit point), ADR-019 (the family work this redirected energy
  from), `STATUS.md` (roadmap), `performance/results.md` §9.

### ADR-021: Durability failures are observable events, not stderr writes

- **Context:** The engine emits structured lifecycle events through an optional observer
  ([`EngineEvent`] + `emit()`), which the server translates into `tracing` logs and Prometheus
  counters — the library stays observability-stack-agnostic (no `tracing`/`log` dependency). But the
  *durability failure* paths predated that discipline: ~14 sites across
  `src/segment/{lifecycle,ingest,persistence}.rs` wrote to **stderr** via `eprintln!` (segment
  write/mmap fell back to in-memory; WAL init/append/checkpoint/reset failed; manifest write failed;
  `sources.dat` write/re-map/load failed; a corrupt segment or torn WAL tail was skipped on recovery).
  Each already set the right health flag (`wal_healthy`/`persistence_healthy`, surfaced via `/_health`)
  and took the consistency-preserving action (reject the write, roll the batch back, or fall back to
  memory) — but the *failure signal itself* never reached `--log-format json` structured logs or
  Prometheus. An operator running the server could not **alert** on degraded durability: stderr is not
  scraped, and `/_health` is a coarse liveness gate, not a per-failure counter. The working precedent
  was already in the tree: `EngineEvent::SegmentCleanupFailed` routes a best-effort cleanup miss through
  the observer. Durability failures are strictly *more* important than a leaked file, yet were *less*
  observable.
- **Decision:** Add one structured event, `EngineEvent::DurabilityFailure { op: DurabilityOp, detail:
  String, error: String }`, and route all 14 sites through `emit()`.
  - **`DurabilityOp`** is a `Copy` discriminator (`WalInit`, `WalAppend`, `WalCheckpoint`, `WalReset`,
    `SegmentWrite`, `SegmentMmap`, `SegmentRecovery`, `ManifestWrite`, `SourceStoreWrite`,
    `SourceStoreRemap`, `SourceStoreLoad`, `WalTornTail`, `IngestRollback`) with a stable snake_case
    `as_str()` for metric labels and an `is_data_at_risk()` predicate. Folding the kind into one
    enum-carrying variant (rather than 14 top-level `EngineEvent` variants) keeps the server's match
    arms — and every other observer — small, while still giving operators a precise, matchable label.
    This mirrors the existing `CompactionTrigger`/`FeatureKind`/`ParseErrorKind` enum-as-discriminator
    pattern.
  - **Severity is derived, not stored:** `is_data_at_risk()` returns true for failures that mean match
    data may be lost or was never durably committed (segment/manifest/WAL-append/init, ingest rollback,
    recovery skip) and false for display-only (`_source`) failures and benign WAL housekeeping
    (checkpoint/reset/torn-tail). The server logs the former at `error!` and the latter at `warn!`, and
    increments `durability_failures_total{op}` for both — so alerting rules can page on
    `op=~"segment_write|manifest_write|wal_append|wal_init|ingest_rollback|segment_recovery"` and merely
    record the rest.
  - **Recovery-time failures are buffered.** `with_config`/`open` run *before* an observer can be
    attached (`set_observer` is called after construction), so emitting there would be a no-op. Those
    sites push onto a bounded `pending_events: Vec<EngineEvent>`; `set_observer` drains and delivers them
    synchronously on attach, then clears. The runtime `emit()` path is unchanged (drops events when no
    observer is set, exactly as before) — only construction buffers, so there is no unbounded-growth
    path.
  - The per-site `*_healthy` flags and rollback/​fallback control flow are **untouched** — this change
    only adds an observable signal; it does not alter what the engine *does* on failure.
- **Consequence:** An operator can now alert on degraded durability from metrics alone, and every
  failure (including silent recovery skips) appears in structured logs with a kind, a human-readable
  consequence, and the underlying error. The compiler enforces completeness: `EngineEvent`'s observer
  matches have no wildcard arm, so any future event variant forces both the Prometheus and `tracing`
  paths to handle it. No match semantics change → the differential oracle is unchanged and green; two
  new persistence tests cover a runtime failure (read-only `segments/` → `SegmentWrite` event) and the
  buffer-and-replay (corrupt segment on reopen → `SegmentRecovery` delivered on `set_observer`). Cost is
  contained to `events.rs` (the type), a thin `segment/` wiring layer, and the server observer.
- **See also:** ADR-013 (WAL — the durability mechanism whose failures this surfaces), ADR-016
  (`/_health` exposes `wal_healthy`/`persistence_healthy` from the snapshot — the coarse gate this
  complements), ADR-017 (durable bulk ingest — the all-or-nothing rollback now emits `IngestRollback`),
  ADR-020 (the resident-memory work that introduced the lazy source store whose write/remap/load
  failures are among the routed sites), `STATUS.md` (operational-polish backlog).

### ADR-022: ES-style runtime settings API (`GET/PUT /_settings`)

- **Context:** Every engine tuning knob (`EngineConfig`) was fixed at process start from CLI flags.
  Changing compaction/flush cadence or query-complexity limits meant a restart — and there was no way to
  *introspect* the live config at all. Operators expect a settings surface like Elasticsearch's
  `GET/PUT /_cluster/settings` / `GET /<index>/_settings`: read the effective config, and update the
  *dynamic* subset at runtime, with the *static* (node/index-creation) settings rejected. The user asked
  for "flexible configuration with a familiar (ES-style) interface."
- **Decision:** Add `GET /_settings` and `PUT /_settings`, borrowing ES *concepts* (dynamic-vs-static,
  `include_defaults`, `acknowledged`) while keeping our existing `ApiError` envelope rather than copying
  ES's verbose error body.
  - **`GET /_settings`** returns the live config as JSON (the `EngineConfig` field names *are* the
    setting keys, so GET output round-trips into PUT input). `?include_defaults=true` also returns
    `EngineConfig::default()`. It reads the **lock-free snapshot**, not the engine mutex — so the config
    now rides in `EngineSnapshot` as `Arc<EngineConfig>` (the `Engine` holds `Arc<EngineConfig>`,
    `Arc::clone`d into each snapshot — O(1) per publish, copy-on-write via `set_config`). This is the
    same pattern as the vocab snapshot fix and keeps *all* read endpoints off the write lock (ADR-016).
  - **`PUT /_settings`** takes a **flat JSON patch** (`{"max_segments": 16}`). A pure
    `apply_settings_patch(cfg, patch)` enforces, per key: dynamic (applied), static (rejected: "setting
    [X] is not dynamically updateable"), unknown (rejected: "unknown setting [X]"), wrong JSON type
    (rejected), then runs `EngineConfig::validate()` for range checks. **All-or-nothing**: every key is
    checked and *any* problem rejects the whole request with all reasons, so a bad key never half-applies
    (matches ES). On success it `set_config`s the validated clone and republishes the snapshot.
  - **Dynamic** (re-read on the next maintenance/compile decision): `max_segments`,
    `holes_ratio_threshold`, `memtable_flush_threshold`, `auto_compact_on_flush`,
    `auto_compact_on_ingest`, `max_query_length`, `max_query_clauses`, `max_anyof_group_size`,
    `compaction_fixed_cost`. **Static** (bound at construction — the data dirs, WAL fsync policy, and
    source-store mode are already established; changing them at runtime is unsafe or meaningless):
    `data_dir`, `wal_sync_on_write`, `retain_source`.
  - **Transient semantics:** updates are **in-memory only** — the startup CLI flags remain the durable
    source, so a restart reverts them. The PUT response says `"persistent": false` so clients aren't
    surprised. (ES historically had transient settings too.) Persisting overrides to a
    `data_dir/settings.json` is deferred — see below.
- **Consequence:** Operators can tune the live engine and read its effective config without a restart,
  through a familiar interface, with precise per-key errors. The pure patch function is unit-tested
  directly (dynamic apply, static/unknown/type/range rejection, all-or-nothing) without the HTTP layer;
  an integration test covers snapshot-carries-config + copy-on-write immutability; the change was also
  verified end-to-end (GET, `include_defaults`, valid PUT round-trip, and the three rejection paths). No
  match semantics change, so the oracle is unchanged. `EngineConfig` gains `Serialize` (its fields are
  serde-friendly); the library does **not** gain `Deserialize` — PUT uses the flat-patch path so the
  dynamic/static policy lives in the server, not the type.
- **Deferred:** (1) **persistent settings** — write dynamic overrides to `data_dir/settings.json` and
  re-apply on `open`, so a tuned node survives restart (the ES "persistent" tier); (2) **server-level
  settings** — `slow_query_threshold_ms` and `include_broad` live in `AppState`, not `EngineConfig`;
  exposing them via `/_settings` needs an atomic/lock around those fields (they're currently set once at
  startup); (3) a config **file** loader (elasticsearch.yml-style) layered under the CLI flags.
- **See also:** ADR-016 (lock-free snapshot reads — this puts the config there too), the vocab snapshot
  fix (same `Arc`-in-snapshot pattern), `config.rs` (`EngineConfig` + `validate`), `STATUS.md` (the
  feature-gating and ops-ergonomics backlog this sits alongside), ADR-025 (the follow-up that actually
  wired the three query-complexity limits into the parser — they were classified *dynamic* here before
  they were enforced anywhere).

### ADR-023: Per-segment introspection endpoint (`GET /_cat/segments`)

- **Context:** The only window into the index was the *aggregate* `/_stats` (+ a bare size/holes table in
  `/_cat/stats`). For an LSM/segment engine that is exactly the wrong altitude: the questions operators
  actually ask are per-segment — *which* segment is driving a compaction (its holes ratio), *where* memory
  sits (which segments are resident vs mmap'd/off-heap), and *which* segments are stale against the current
  vocab epoch (need reingest). Elasticsearch answers this with `_cat/segments`, and the ops-ergonomics
  backlog called for the same. `EngineMetrics` already flattened the per-segment data into parallel
  `segment_sizes`/`segment_holes` vectors, losing kind, staleness, and the memory split.
- **Decision:** Add a dependency-free introspection record `SegmentInfo` (+ `SegmentKind`:
  `Memory`/`Mmap`/`Memtable`) in `events.rs`, alongside `EngineMetrics` and following the same no-serde
  convention (the server builds its own `Serialize` row type from it). One collector,
  `collect_segment_infos(segments, memtable, current_epoch)`, is shared by both `Engine::segment_infos()`
  and `EngineSnapshot::segment_infos()`, so the server reads it **lock-free from the snapshot** like every
  other read endpoint (ADR-016).
  - **Rows.** Base segments first (`ordinal 0..n`, oldest first), then the **memtable as the final row**
    (`kind = memtable`) — always present, even when empty, so the hot delta is visible. Each row carries
    `entries` (total), `alive`, `deleted`, `holes_ratio`, `vocab_epoch`, `stale`, and a deliberate
    **two-way memory split**: `resident_bytes` (exact SoA + indexes + filter — **0 for `mmap`**, matching
    the `EngineMetrics` accounting, which honestly signals "this segment is off-heap") and
    `overhead_bytes` (reverse index + liveness overlay — resident for *both* kinds). `stale` reuses the
    engine's own rule (`epoch < current`, and the empty memtable is never stale).
  - **`GET /_cat/segments`** returns a human-readable text table by default (consistent with `/_cat/stats`),
    and a JSON **array** of row objects on `?format=json` (the ES `_cat?format=json` convention). The text
    table humanizes bytes (binary units, 2 dp); JSON keeps raw integers for machine consumption. The
    rendering + the `SegmentInfo → SegmentRow` projection are pure functions, unit-tested without the HTTP
    layer (mirroring `apply_settings_patch`).
- **Consequence:** Operators get segment-level visibility into compaction pressure, memory distribution,
  and staleness through a familiar interface — additively, with no change to the existing `/_stats`
  response shape (no client breakage) and **no change to match semantics** (oracle untouched). Covered by
  server-inline tests (table shape, stale yes/no, bytes humanizer, JSON projection) and an engine-level
  test asserting the layout invariants (dense ordinals, `alive + deleted == entries`, memtable-last,
  engine/snapshot agreement, and a deletion surfacing as a hole); verified end-to-end over HTTP in both
  formats.
- **Deferred:** per-segment **filter FP rate / bit count** — the anchor filter doesn't retain its inserted
  key count, and the mmap arm doesn't expose the filter's block count through the `BaseSegment` wrapper, so
  an honest, symmetric FP-rate column needs a small `filter.rs`/`MmapSegment` change first. Left out rather
  than reported asymmetrically. (Other `_cat` endpoints — `_cat/thread_pool`, a `?v`/`?h` column selector —
  remain in the ops-ergonomics backlog.)
- **See also:** ADR-016 (lock-free snapshot reads), ADR-020 (the resident-vs-off-heap byte accounting this
  surfaces per segment), ADR-022 (the sibling ES-style endpoint), `events.rs` (`SegmentInfo`/`SegmentKind`),
  `STATUS.md` (ops-ergonomics backlog).

---

### ADR-024: CI via GitHub Actions mirroring `check.sh`; commit pressure tests + benchmark baseline

- **Context:** The quality gate was `engine/check.sh` (fmt + clippy + test + audit + deny), run by hand
  before pushing — CLAUDE.md called it "the local CI substitute." Three gaps had opened up: (1) nothing
  *enforced* the gate, so an unrun `check.sh` could merge; (2) the pressure suite (`tests/stress.rs`) and
  the benchmark regression baseline (`docs/performance/benchmark-results.txt`) were gitignored — the
  latter silently, via a blanket `*.txt` rule — so both were invisible to any automated runner and easy
  to let rot; (3) the "CI is a non-goal" framing no longer matched the intent to check every PR.
- **Decision:** Add GitHub Actions CI (`.github/workflows/ci.yml`) that **runs `check.sh` itself** rather
  than re-listing the checks — one source of truth, so "green locally" and "green in CI" cannot diverge.
  - **Commit what CI must see.** Un-gitignore `tests/stress.rs` (15 pressure tests + one `#[ignore]`d 10M
    soak) and `benchmark-results.txt`; tighten `.gitignore` so only genuine runtime data (`data/`, loose
    `*.csv`/`*.jsonl`/`*.txt`) stays ignored. The stress suite is now part of `cargo test --release` and
    runs on every PR; the 10M soak stays `#[ignore]`d and runs only on demand (`workflow_dispatch` →
    `run_soak`), as it needs ~minutes and multi-GiB RAM.
  - **Benchmarks run-and-print, never gate.** CI runs the seeded, deterministic `bench`/`segbench`/
    `snapbench` and uploads their console output as an artifact, but `continue-on-error` keeps them from
    failing the build. Throughput is hardware-dependent (the runner is not the reference machine), and the
    machine-independent *structural* invariants stay a **manual** comparison against `benchmark-results.txt`
    — a deliberate choice over a brittle numeric assert that would false-alarm on runner variance.
  - **Reproducibility + local fast-fail.** Pin the toolchain in `engine/rust-toolchain.toml`; cache builds
    with `Swatinem/rust-cache`; install `cargo-audit`/`cargo-deny` as prebuilt binaries. Locally, committed
    git hooks (activated once via `./setup-hooks.sh`) run the fast gate (fmt + clippy, `check.sh --fast`)
    on commit and the full gate on push.
- **Consequence:** Every PR is gated by the same checks a developer runs locally; the pressure tests and
  benchmark baseline are now version-controlled and exercised rather than drifting out-of-tree; and
  benchmark numbers are captured per-PR for review without producing false regressions from runner
  variance. This **supersedes the "local CI substitute" framing**: `check.sh` remains the gate and the
  local entry point, but it is now also the script CI runs — not a stand-in for the absence of CI. Cost:
  PR runs pay the release+LTO compile (mitigated by caching) and the full suite including stress (a few
  minutes); accepted in exchange for the coverage.
- **See also:** [`testing.md`](testing.md) (the how-we-test guide), ADR-008 (seeded determinism — why the
  benchmarks reproduce), `engine/check.sh`, `.github/workflows/ci.yml`, `engine/rust-toolchain.toml`.

### ADR-025: Wire query-complexity limits into the parser (the config knobs were cosmetic)

- **Context:** `EngineConfig` exposed three query-complexity limits — `max_query_length`,
  `max_query_clauses`, `max_anyof_group_size` — surfaced as CLI flags and as *dynamic* settings in
  ADR-022, and `config.rs` documented them as "rejected at parse time." But the parser
  (`dsl::parse`) only ever enforced its own compiled-in constants (`MAX_QUERY_LENGTH = 10_240`,
  `MAX_CLAUSES = 256`, `MAX_ANY_OF_SIZE = 64`); `parse()` took only `&str` and no ingest path ever read
  the `EngineConfig` fields. So the flags and `PUT /_settings` for these limits were **cosmetic** —
  setting them had no effect. There was also a latent default drift: the config field defaulted to
  `10_000` while the parser actually enforced `10_240`, so the documented and effective defaults
  disagreed. A repo review surfaced the wiring gap.
- **Decision:** Thread the configured limits into the parser, off the match hot path (parsing is
  compile-time, so this respects the no-work-on-the-hot-path invariant).
  - Add `dsl::ParseLimits { max_query_length, max_clauses, max_any_of_size }`, whose `Default` is the
    compiled-in constants, and `dsl::parse_with_limits(input, &limits)`. `dsl::parse` becomes the
    thin default-limits wrapper (used by the explain / read-only path and callers without a config).
  - `EngineConfig::parse_limits()` derives a `ParseLimits` from the three fields. The three **front-door**
    ingest paths — `try_build_from_queries`, `try_insert_live`, `try_bulk_ingest_detailed` — call
    `parse_with_limits` with the live config's limits. The config defaults now *reference* the `dsl`
    constants (single source of truth), so default behavior is unchanged and the `10_000`/`10_240` drift
    is gone. CLI `default_value_t` and the `api.md` example were aligned to the constants too.
  - **WAL replay keeps the compiled-in ceiling** (the non-obvious bit): `replay_insert` deliberately
    calls `dsl::parse` (default limits), *not* the configured limits. A WAL entry was already accepted at
    its front-door write; re-applying a since-tightened limit on recovery could silently drop an
    already-acknowledged write and diverge recovered state from the durable log. Durability beats policy
    on the replay path; the compiled-in ceiling still bounds replay resource use.
- **Consequence:** The `--max-*` flags and `PUT /_settings` now actually govern parsing on every ingest
  path — a tightened limit takes effect on the next ingest and is usable as a real abuse/resource guard —
  making ADR-022's *dynamic* classification and the `config.rs` / `api.md` docs true rather than
  aspirational. No match semantics change, so the oracle is unchanged. Regression-tested by a `dsl` unit
  test (`parse_with_limits_enforces_custom_bounds`, both tighter and looser than the defaults) and an
  integration test (`configured_query_limits_are_enforced_at_ingest_and_are_dynamic`) that also exercises
  the dynamic `set_config` path. The compiled-in constants are retained as the defaults and as the
  replay ceiling.
- **See also:** ADR-022 (the settings API that listed these as dynamic before they were enforced),
  ADR-013 (WAL — why replay must not re-litigate limit policy), ADR-002 (no work on the match hot path —
  why threading limits through compile-time parsing is fine), `dsl.rs` (`ParseLimits` /
  `parse_with_limits`), `config.rs` (`parse_limits`).

### ADR-026: Broad-lane batch / columnar evaluation (`match_titles_batch`, `POST /_mpercolate`)

- **Context:** Class-C ("broad") queries are quarantined out of the selective path (ADR-003) because
  their best signature is still a *hot* feature (one of the 64 most frequent), so their postings are
  huge. But they were still evaluated **inline, per title**: `Segment::match_into(include_broad=true)`
  walks the huge posting and runs the scalar `ExactStore::verify` once *per title*. The same posting is
  re-scanned for every title containing that hot feature, so candidates/title jump 54 → 684 and
  throughput collapses ~9× (710k → 78k titles/sec/core), p99 ~28× ([results.md](performance/results.md)
  §1). Broad queries are only ~0.2% of the corpus but dominate match cost — the single biggest
  remaining matching-performance lever ([STATUS](STATUS.md) Tier 1). The resident-memory prerequisite
  (ADR-020) had already shipped.
- **Decision:** Evaluate the broad lane **once per title-batch, columnar**, while the selective lane
  stays per-title (it is already fast and scale-flat). New module `segment/broad_batch.rs`, exposed as
  `match_titles_batch[_with_stats]` on `Engine`/`EngineSnapshot` (sharing the `MatchView` body so the
  two read paths can't drift) and as a new HTTP endpoint. Mechanics for a batch of titles:
  1. **Per-batch inverted index.** Normalize each title (the same `match_features` call the per-title
     path makes), compute its `tmask`, and build `feature → bitmap-of-titles` + `tmask_batch[t]`.
  2. **Collect reachable broad queries (per segment).** For each *distinct* feature in the batch, form
     `sig_key([f])`, check the segment anchor filter (ADR-011), and probe the segment's `broad` index
     **once** — *this is the amortization*: each huge posting is read once per batch per segment, not
     once per title. Union locals via the existing epoch-stamp dedup.
  3. **Verify by bitmap algebra.** For each reachable broad query, `exact::eval_batch` reproduces
     `verify` clause-for-clause as the bitwise **transpose** over batch-sized title sets (mask gate →
     per-title gate bitmap; required tail → AND of feature bitmaps; forbidden tail → AND-NOT; any-of →
     AND of OR-over-members). Per-query cost is O(#tail + #forbidden + Σgroup) word-ops, *independent of
     how many titles match*, and auto-vectorizes.
  4. **Pure-anchor fast path.** A broad query whose *entire* semantics is its hot anchor (no required
     tail, no forbidden, no any-of, `req_mask` is the single anchor bit) matches exactly the titles
     containing the anchor — emit straight from the anchor's title bitmap with **zero** verification.
  - **Parallelism:** a per-rayon-chunk broad pass (chunk = `broad_batch_size`). Each worker owns its
     scratch (cleared, not freed, between batches — no hot-path allocation); no cross-thread shared
     mutable state, so `par == seq` holds trivially. A posting is walked ~`ncpu` times per batch rather
     than `num_titles` times — same order-of-magnitude win, far simpler than a global scatter/merge.
  - **Reuse `ExactStore` verbatim** on the query side (no parallel broad store); pure-anchor is derived
     from the existing SoA columns at probe time — **no `.seg` format change**. The mmap and in-memory
     segments drive one body via a `BroadBackend` trait.
- **Why correct:** each bitmap clause is the exact transpose of the corresponding scalar test over the
  *same* `ExactStore` columns; retrieval is the same lossless-cover superset narrowed by exactly those
  clauses (signatures are untouched). Forbidden features enter **only** as AND-NOT in verification,
  never in retrieval — the "never gate on MUST_NOT" invariant (ADR-006) holds structurally. The result
  set is therefore **byte-identical** to the per-title `match_title(include_broad=true)` for every
  title and every setting — a pure performance change. Guarded by `tests/broad_batch.rs` (batch≡scalar
  across single/multi-segment, memtable, tombstones, any-of/forbidden, a `broad_batch_size` sweep incl.
  word/chunk boundaries, all three posting variants, `Inline`≡`Columnar`, and `materialize` on≡off),
  an additive brute-force batch oracle in `tests/oracle.rs`, and a batch≡per-title-under-churn test in
  `tests/stress.rs`.
- **HTTP ergonomics — new `/_mpercolate`, `/_search` unchanged.** The plan originally proposed routing
  `/_search`'s `documents:[...]` arm through the batch path "transparently." It is **not** transparent:
  `/_search` returns documented per-slot `stats` (per-title candidate/posting counts), and the columnar
  broad lane amortizes work *per batch*, so per-title broad stats structurally cannot exist there.
  Mirroring Elasticsearch's `_search`-vs-`_msearch` split, the batch path is exposed as a **new** `POST
  /_mpercolate` (ES `_msearch`-shaped `responses[]` envelope, one entry per document, per-request
  `include_broad`, optional top-level broad summary), and `/_search` stays the rich/observable path
  (per-slot stats, `explain`, `profile`, paging) on the per-title matcher. Users pick fast-vs-rich;
  broad-heavy batch workloads go to `/_mpercolate`.
- **Materialization, reinterpreted.** The original spec floated "precomputed/materialized subscriptions
  refreshed periodically" for the broadest queries. Literal periodic-refresh materialization does not
  map to *streaming* percolation (titles arrive continuously; there is no batch to refresh against).
  Its benefit — skipping per-evaluation work for pure-anchor broad queries — is captured instead by the
  pure-anchor fast path, which is exact, always-fresh, and needs no background refresh or extra state.
- **Kill-switches + knobs.** Four **dynamic** config knobs (ADR-022): `broad_batch_size` (256),
  `broad_columnar` (true; false ⇒ provable inline fallback, byte-identical), `broad_materialize` (true;
  false ⇒ pure-anchor queries go through full verification instead), `max_percolate_batch` (10_000;
  bounds per-request work). Plus four cumulative `broad_*` Prometheus counters and a `broad_candidates`
  field on `StatsResponse` — the "metered to a higher cost class" intent from ADR-003.
- **Alternatives considered:**
  - *Switch `/_search` to the batch path* — rejected; silently regresses documented per-slot stats (see
    HTTP ergonomics above).
  - *A global scatter/merge across all titles* — rejected; the per-rayon-chunk pass gets the same
    order-of-magnitude amortization with worker-local scratch and trivial `par == seq`.
  - *A separate broad exact store / `.seg` format change* — rejected; `ExactStore` columns already carry
    everything `eval_batch` and the pure-anchor predicate need.
  - *Roaring/SIMD posting intersection for the very broadest postings* — deferred as a micro-optimization
    on top of this work (plain `Vec<u64>` bitmaps already auto-vectorize and beat roaring at batch-dense
    title sets).
- **Consequence:** The broad lane is no longer the bottleneck. Broad postings scanned amortize
  ~1/`broad_batch_size` (29× at 256, 115× at 1024 — structural, machine-independent, in
  `benchmark-results.txt`); end-to-end the columnar batch runs ~2.4× the inline path and within ~37% of
  the selective ceiling at the same chunking (dev box). Dark by default for the per-title API
  (`include_broad` still opt-in); the batch entry points are additive. **Out of scope (follow-ups):**
  class-C ingest warnings / rewrite-suggestion generation (its own feature; the new broad meters satisfy
  the "metered" intent), and SIMD/roaring broad-posting intersection.
- **See also:** ADR-003 (broad-query quarantine — this is how the quarantined lane is finally
  evaluated), ADR-002 (integer-only hot path — `eval_batch` is allocation-free bitmap integer work),
  ADR-006 (forbidden never gates — preserved structurally in the transpose), ADR-022 (the dynamic
  settings the four knobs plug into), ADR-016 (the lock-free snapshot the batch matchers read),
  ADR-020 (the resident-memory prerequisite), [matching.md](design/matching.md) §4,
  [api.md](reference/api.md) (`/_mpercolate`), `segment/broad_batch.rs`, `exact.rs`
  (`eval_batch_slices` / `is_pure_anchor`).

### ADR-027: In-process multi-shard core — shared frozen dict, feature-anchor ring, designated replicated lane

- **Context:** Clustering was entirely design-only ([clustering-and-scaling.md](design/clustering-and-scaling.md),
  STATUS Tier 3). The design's own build path (§10) is explicitly incremental and front-loads the
  correctness-critical heart *before* any networking: step 1 (wrap the engine as a shard) + step 2
  (a coordinator with a consistent-hash ring + content routing over K shards **in one process**),
  validated by extending the differential oracle to a multi-shard harness. The novel, no-false-negative
  part of the design is the entity-anchor sharding + content routing; gRPC, the durable externalized
  log, Raft, and object storage are "borrowed plumbing." So we build steps 1–2 first.
- **Decision:** A `cluster` module (`ClusterEngine` over K `Shard`s, each a `Shard`-wrapped `Engine` +
  `ArcSwap<EngineSnapshot>`) with **zero new dependencies** (`rayon`/`arc-swap` already present).
  Four load-bearing sub-decisions:
  1. **One authoritative, frozen `Arc<Dict>` shared read-only into every shard.** Each `Engine` interns
     features and finalizes its 64-bit hot mask *per build* (`Arc::make_mut` on the write path), so two
     independent engines disagree on both `FeatureId`s and which features are "hot." Either divergence
     flips a query's cost class / anchor across shards → a title routes to one shard while the query was
     indexed under a different key on another → **false negative**. The coordinator therefore builds the
     dict over the whole corpus once (pass A), `finalize_mask`, then freezes it and shares it; shards
     index via the new non-mutating `Engine::ingest_extracted` / `insert_extracted` (which call
     `Segment::add_compiled`, read-only over the dict), so the `Arc` is never forked. This is the
     in-process model of the design's "feature-model version in cluster state" (§4.3/§8.7).
  2. **Consistent-hash ring keyed on `FeatureId`** (not on `sig_key`). Safe *because* of the shared dict
     (ids are globally stable), and it gives the design's true ~2–5 fan-out: a title routes on its few
     rare features, not on the combinatorial set of probe-signatures it generates. (A `sig_key`-keyed
     ring would be correct but blow fan-out up to ~all shards for titles with several hot features.)
     `ring_hash` = FNV-1a + a murmur3 finalizer; FNV alone clusters sequential ids and skews shard load.
  3. **`compile::anchor_plan` is the single source of truth for placement.** `build_signatures` was
     refactored to compute the pre-hash anchor feature *groups* and then hash them (byte-identical
     output — the existing oracle is the guard), so the coordinator places by anchor *identity* without
     re-deriving the optimizer's per-class selection. Forbidden features can't leak in: `anchor_plan`
     reads only `required`/`anyof`, never `forbidden` (ADR-006 holds structurally).
  4. **Placement by cost class; queries with no rare anchor go to a designated replicated lane (shard 0).**
     Class A (one rare anchor) → one shard; class-B any-of (rare members) → one shard per member; **class-B
     arity-2** (rarest required is hot ⇒ *all* required hot ⇒ no rare anchor to hash on) and **class C**
     (broad) → the replicated lane. In-process that lane is materialized on shard 0 and evaluated **only**
     there (always probed, with `include_broad`), so there is no double-counting; selective shards run
     `include_broad=false`. This is the in-process stand-in for the design's "replicate the broad lane to
     every node" (§7). Routing a title = shard 0 ∪ `{ring.lookup(f) : f ∈ title, !is_hot(f)}`; results
     are unioned + deduped. Deletes fan out to all shards (idempotent), sidestepping any placement journal.
- **Why correct (no false negatives):** for any query `Q` a title `T` matches — if `Q` is class A /
  B-any-of, its anchor (resp. a matched member) is a *required*, non-hot feature, present in `T`, so `T`
  routes to `ring.lookup(anchor) =` `Q`'s shard; if `Q` is class-B-arity-2 / C it lives on shard 0,
  which `T` always probes. Each shard is a verbatim single-node engine, so its lossless cover + integer
  exact-verify finish the job; no shard boundary can drop a match. No false positives: every emitted id
  passed `exact.verify` (title-content-only) on some shard, and the union dedups. Guarded by
  `tests/cluster_oracle.rs`: cluster ≡ single-node ≡ independent brute-force oracle, as sets, across
  K ∈ {1,3,8,16} × broad on/off, with every placement branch asserted present (`class_counts`) and
  fan-out asserted ≪ K.
- **Alternatives considered:**
  - *Per-shard independent dicts* — rejected; hot-mask/`FeatureId` divergence is a false-negative trap (1).
  - *`sig_key`-keyed ring* — rejected; correct but defeats the ~2–5 fan-out win for hot-heavy titles (2).
  - *Place class-B-arity-2 on its rarer feature's shard* — rejected; that feature is *hot*, and titles
    route only on non-hot features, so the query would be unreachable → false negative. The replicated
    lane is the correct home (4).
  - *Generator knobs for class coverage* — rejected; adding required `GenConfig` fields breaks ~30
    literal sites. The oracle hand-injects pure-any-of, all-hot, and multi-entity cases instead.
- **Consequence:** The central claim of the clustering design — content-routed percolation by anchor
  entity with a clean no-false-negative proof — is now built and proven in one process, dependency-free,
  and is the foundation later steps wrap. Surface: `cluster::ClusterEngine` (library) + `clusterdemo`
  bin + the oracle; no HTTP yet. **Out of scope (later build-path steps):** gRPC `ShardServer` + a
  local↔remote `Shard` trait (step 1 networking), the durable externalized mutation log / read-your-writes
  quorum (§4.1/§6), Raft cluster-manager quorum (§4.3), object storage / attach-and-mmap replicas (§4.2),
  auto shard count / auto-split / rebalance (§8), autoscaling (§8.5), epoch fencing / self-heal (§9),
  replicate-broad-to-*all*-nodes (§7; in-process uses one designated evaluator), and incremental
  **new-vocabulary** adds (the dict is frozen post-build; `add_query` compiles read-only against it).
- **See also:** the clustering design ([clustering-and-scaling.md](design/clustering-and-scaling.md) §3,
  §7, §10), ADR-001 (semantic signatures — the anchor the ring hashes), ADR-003 (broad-query quarantine —
  the lane that gets replicated), ADR-006 (forbidden never gates — preserved in placement + routing),
  ADR-016 (the lock-free snapshot each shard reads), the lossless-cover contract
  ([design/README.md](design/README.md) §2), `src/cluster/{ring,shard,coordinator}.rs`,
  `src/compile.rs` (`anchor_plan`), `tests/cluster_oracle.rs`.

### ADR-028: Feature-gate the server/observability stack behind a default-on `server` feature (lean core)

- **Context:** The library crate unconditionally compiled the full HTTP/observability stack
  (`axum`, `tokio`, `clap`, `parking_lot`, `tower`, `uuid`, `tracing`, `tracing-subscriber`,
  `prometheus`) even for pure-engine embeddings and the engine-only CLI bins — STATUS flagged this as a
  build-hygiene gap (compile time, binary size, supply-chain surface). It also became *timely*: the next
  increment (gRPC `ShardServer`, ADR-029) adds `tonic`/`prost` — a heavy, network-only dependency that
  needs a clean home behind a feature, not bolted onto the always-on surface. A usage audit confirmed
  all nine crates are imported **only** in `src/bin/server.rs`; none leak into the library.
- **Decision:** Mark the nine crates `optional = true` and gather them under a **`server` feature**, with
  **`default = ["server"]`** so every documented command (`cargo build --release`,
  `cargo run --release --bin server`, `cargo test --release`) behaves exactly as before. The server bin
  carries `required-features = ["server"]`, so under `--no-default-features` Cargo skips it and its
  `use axum::…` never compiles — meaning **zero `#[cfg]` attributes are needed in code**; the gating is
  entirely at the Cargo-manifest level. `serde`/`serde_json` stay **core** (Vocab JSON persistence,
  `EngineConfig` Serialize, `ExplainDetail`, and the JSONL loader are all library code). A new
  `check.sh` lane — `cargo clippy --no-default-features --release -- -D warnings` — enforces that no
  server-only crate ever creeps back into library code (it would fail the lean lint).
- **Why default-on, not lean-by-default:** preserving the documented commands and the green gate is the
  win; the dependency-hygiene guarantee comes from the enforcement lane, not from which feature set is
  the default. The lean core is one flag away (`--no-default-features`) and is continuously verified.
- **Alternatives considered:**
  - *Lean-by-default (`default = []`)* — rejected; forces `--features server` onto every server
    build/run/test and churns every build command in CLAUDE.md + docs, for no benefit the enforcement
    lane doesn't already provide.
  - *Gate the engine-only bins too (bench/demo/clusterdemo/learn/segbench/snapbench/norm behind a `cli`
    feature)* — unnecessary; they use only core deps, so they add nothing to the dependency tree.
  - *Make `serde`/`serde_json` optional as well* — rejected; they are genuine library dependencies.
- **Consequence:** `cargo build --no-default-features` yields the lean embeddable core (daachorse,
  memmap2, rayon, roaring, arc-swap, serde, serde_json + transitives); the full server remains the
  default build. No runtime or behavior change. This is the clean seam beside which ADR-029's
  `distributed` (gRPC) feature slots — tonic/prost land off-by-default without touching the core surface.
- **See also:** ADR-007 (the original three-production-deps philosophy this extends), ADR-029 (gRPC
  `ShardServer` — the `distributed` feature that reuses this seam), [`STATUS.md`](STATUS.md),
  [`engine/Cargo.toml`](../engine/Cargo.toml) (authoritative pins + feature defs),
  `engine/check.sh` (the lean-core lane).

### ADR-029: gRPC `ShardServer` + the local↔remote `trait Shard` seam (clustering step 1, networking)

- **Context:** ADR-027 built clustering steps 1–2 in-process and explicitly deferred step 1's *networking*
  half — "lift the shard behind a `ShardServer` (gRPC) so a shard can be remote (the local↔remote
  `trait Shard` seam)." This ADR builds exactly that: the seam plus a `tonic` transport, behind an
  off-by-default `distributed` feature, proven by a gRPC differential oracle. It builds on ADR-028's
  lean-core feature split (the `distributed` deps slot beside the `server` ones).
- **Decision** — six load-bearing sub-decisions:
  1. **`trait Shard` abstracts the OPERATION, not the data.** A remote shard has no in-process
     `EngineSnapshot`, so the seam exposes `percolate(title, include_broad) -> (ids, MatchStats)` (the
     body of the old `query_shard`), never `snapshot()`. The in-process struct was renamed
     `Shard → LocalShard`; `RemoteShard` (a gRPC client) is the second impl. The coordinator holds
     `Vec<Box<dyn Shard>>` — **dynamic dispatch**, because a cluster of mixed local + remote shards is
     the whole point; one vtable hop is negligible against a match or an RPC. (`assert_send_sync` in
     `lib.rs` still guards `ClusterEngine: Send + Sync`.)
  2. **The seam is fallible** (`Result<_, ShardError>` on every method). A `LocalShard` never errs; a
     `RemoteShard` errs on transport failure. Surfacing that — instead of swallowing it into an empty
     result — is load-bearing for **zero false negatives**: a dropped shard probe would silently shrink
     the union. The coordinator's runtime methods propagate it; `build` stays **infallible** (it only
     ever makes `LocalShard`s and ingests via the inherent infallible `LocalShard::ingest_local`). The
     distributed load path is the new `ClusterEngine::ingest` (the analog of `build`'s pass B, over the
     seam).
  3. **Sync trait + `block_on` bridge.** The coordinator fans probes out via rayon (sync), so the trait
     stays sync; `RemoteShard` holds a `tokio::runtime::Handle` and blocks on its async tonic client
     internally, confining all async to that type and leaving the coordinator + `LocalShard` + the
     in-process oracle untouched. Safe because rayon workers are not tokio workers (no nested-runtime
     panic). Trade-off: a parked rayon worker per in-flight RPC — an async fan-out is the documented
     later optimization.
  4. **The write path ships raw DSL, not pre-extracted `FeatureId`s.** Raw ids are valid only if both
     sides' dicts are byte-identical; sending DSL keeps the wire dict-agnostic and lets the server
     re-compile read-only against ITS frozen dict (exactly what `add_query` does in-process), so a dict
     mismatch fails **loud** rather than corrupting matches. **Key constraint:** every `ShardServer` must
     be built over a byte-identical frozen dict — the ADR-027 shared-dict invariant, extended across the
     wire (in-test the `Arc<Dict>` is literally shared). Placement stays coordinator-only; the server is
     a dumb executor.
  5. **Codegen is isolated in a workspace sub-crate** (`reverse-rusty-shard-proto`, `engine/grpc/`),
     compiled with the **pure-Rust `protox`** compiler so neither dev nor CI needs a system `protoc`. The
     engine depends on it + `tonic` only under `distributed`; the `RemoteShard`/`ShardServer` glue stays
     in `src/cluster/{remote,server}.rs` behind cfg. *Why a sub-crate, not an in-crate `build.rs`:* a
     build script cannot see `#[cfg(feature = "distributed")]` (Cargo passes features to build scripts
     only as runtime env vars), so optional codegen deps can't be conditionally invoked in-crate without
     making them non-optional — which would drag `protox`/`tonic-prost-build` into the lean core (ADR-028).
     No generated code is checked in; it is regenerated from `proto/shard.proto` on every build.
  6. **No TLS** (plaintext localhost) this increment — avoids pulling `rustls`/`ring`/`openssl` into the
     `cargo deny` license surface; transport security + auth are a later step.
- **Why correct:** `tests/cluster_grpc_oracle.rs` stands up K = 3 real `ShardServer`s on localhost,
  assembles a `ClusterEngine` of `RemoteShard`s, loads the corpus over the `IngestExtracted` RPC, and
  asserts the gRPC-backed cluster returns EXACTLY the independent brute-force oracle's set AND the
  single-node engine's set, broad on and off — plus a live add → percolate → remove over the
  Insert/Delete RPCs. The seam refactor is otherwise behavior-preserving: the in-process
  `cluster_oracle.rs` stays green, dependency-free, on the default build.
- **Alternatives considered:**
  - *Infallible seam (swallow or panic on RPC error)* — rejected; swallowing is a false negative,
    panicking violates the no-panic-in-library rule (and `panic = "abort"` would fail-stop the process).
  - *Async trait + async fan-out now* — deferred; large blast radius across the coordinator's public API
    and the synchronous oracle. Sync + `block_on` is correct and contained; revisit for remote-fan-out
    throughput.
  - *In-crate `build.rs` codegen* — rejected; can't gate codegen deps without polluting the lean core.
  - *Commit the generated code* — rejected; checked-in generated noise + manual regen drift. The
    sub-crate auto-regenerates via `protox`.
  - *Send pre-extracted `Extracted` over the wire* — rejected; only valid under byte-identical dicts and
    fails silently if they diverge (4).
- **Consequence:** Clustering build-path **step 1 is complete** (in-process core + gRPC transport).
  Surface (behind `distributed`): `cluster::{ShardServer, RemoteShard}`,
  `ClusterEngine::{connect_remote, ingest}`, the `shardserver` bin, and the gRPC oracle. **Out of scope
  (later steps):** durable externalized log / read-your-writes quorum, Raft cluster-manager, object-store
  segments, multi-process dict shipping (the connect-time dict-hash handshake itself landed — ADR-030), autoscaling, auto-split,
  TLS/auth, async remote fan-out, and production panic-isolation at the RPC boundary.
- **Known sharp edges (live in the shipped surface, distinct from the unbuilt work above):**
  - *Unchecked cross-process dict identity → silent false negatives.* `ShardServer::new` and
    `connect_remote` both take the frozen dict from the caller with NO verification that the coordinator's
    and the servers' dicts match. In-process and the localhost oracle share one `Arc<Dict>`, so it holds;
    across a real process boundary a diverged dict drops matches **silently** — the one false-negative
    path the fallible seam does not catch. The `shardserver` bin builds its own dict and exposes no way to
    ship it, so it is **not yet correctly consumable by a separate coordinator**. *Cheap mitigation before
    full dict-shipping: exchange a dict fingerprint at connect / first RPC and error on mismatch — turns a
    silent FN into a loud failure.* **→ DONE (ADR-030): the handshake landed; a divergent dict now fails
    the connect with `ShardError::DictMismatch`. Full dict-shipping is still deferred, so cross-process use
    still requires matching dicts — but it no longer fails *silently*.**
  - *The `MatchStats` wire map is unverified.* `cluster_grpc_oracle.rs` asserts matched-ID sets, not the
    11 round-tripped stats fields, so a transposition in `cluster/proto.rs` would go undetected. *Cheap
    fix: assert a stats round-trip.* **→ DONE (ADR-030): a `proto.rs` round-trip unit test (by field name,
    both directions) + a gRPC-vs-in-process stats equality check in the oracle.**
  - *No transport auth + plaintext:* any client can call `Delete`/`Flush`/`IngestExtracted`. Localhost-only.
  - *`panic = "abort"`* fail-stops a shard process on a handler panic.
  - *Grown audit surface:* the workspace now locks the full tonic tree, so `cargo audit`/`deny` cover
    crates a non-`distributed` build never compiles (the compiled lean core is unchanged).
- **See also:** ADR-027 (the in-process core this extends), ADR-028 (the feature-gating seam `distributed`
  reuses), [`clustering-and-scaling.md`](design/clustering-and-scaling.md) §10 (step 1),
  `engine/grpc/` (the proto sub-crate), `src/cluster/{shard,remote,server,proto}.rs`,
  `src/bin/shardserver.rs`, `tests/cluster_grpc_oracle.rs`.

---

### ADR-030: Dict-fingerprint handshake + fallible cluster construction (ADR-029 sharp-edge closure)

- **Status:** Accepted.
- **Context:** ADR-029 shipped the gRPC transport with five documented "known sharp edges." Two were
  correctness gaps with cheap, already-flagged mitigations: (1) unchecked cross-process dict identity could
  drop matches *silently* — the one false-negative path the fallible seam cannot catch — and (2) the
  11-field `MatchStats` wire map in `cluster/proto.rs` was untested, so a field transposition would go
  undetected. Two smaller issues sat alongside them: cluster *construction* still used `assert!`/`panic!`
  (against the no-panic-in-library rule, ADR-005), and `ClusterEngine::ingest` silently re-indexed
  (duplicated) entries if called on an already-populated cluster. This ADR records closing all four, plus a
  test-only flake fix.
- **Decision:**
  - **Dict-fingerprint handshake.** `Dict::fingerprint()` is a stable `fnv1a64` over the
    *correctness-relevant* content only — the `name→id` mapping (names in id order), each feature's kind and
    common-mask bit, and the `finalized` flag. `freq` is excluded: its sole match-relevant effect (which
    features get a mask bit) is already captured by `mask_bit`, so hashing it would flag false mismatches.
    A new `DictFingerprint` RPC lets `RemoteShard::connect` fetch the server's fingerprint and compare it to
    the coordinator's; a mismatch returns the new `ShardError::DictMismatch` instead of connecting. This
    turns the silent-FN path into a loud connect-time failure. It does **not** ship the dict (servers must
    still be built over the same feature space) — full dict-shipping stays deferred (ADR-029 out-of-scope).
  - **Fully-fallible construction.** `HashRing::new`, `ClusterEngine::from_parts`, `build`, and
    `connect_remote` now return `Result<_, ShardError>`, replacing the four construction `assert!`s with the
    new `ShardError::Config`. Chosen over a boundary-only conversion (which would leave `build` infallible):
    the no-panic rule applies to all library construction, and the caller ripple is tests/bins only.
  - **`ingest` re-entry guard.** `ClusterEngine::ingest` errors with `ShardError::Config` on a non-empty
    cluster rather than silently duplicating; its documented contract was always "a freshly assembled
    (empty) cluster" (use `add_query` for incremental adds).
  - **`MatchStats` wire test.** A `proto.rs` unit test asserts the map by field name in *both* directions
    with 11 distinct values (catching a symmetric transposition a pure round-trip would miss); the gRPC
    oracle additionally asserts the gRPC cluster's merged stats equal an in-process cluster's, per title.
  - **gRPC-test port-race fix.** The oracle binds each shard's ephemeral port exactly once via tonic
    `TcpIncoming` + a new `ShardServer::serve_with_incoming`, removing the bind→drop→rebind window that
    could flake CI.
- **Consequence:** ADR-029 sharp edges (1) and (2) are closed; a negative oracle test
  (`grpc_connect_rejects_divergent_dict`) proves the handshake fires on divergence. Cross-process gRPC is
  now correctness-*safe* (a divergent dict fails loud) though still not a full deployment — TLS/auth (edge
  3) and dict-shipping remain open. No hot-path or lean-core change: the fingerprint is connect-time only,
  and every edit lives in the cluster module / `distributed` lane.
- **See also:** ADR-029 (the edges this closes), ADR-005 (typed errors / no panics in library code),
  ADR-027 (the in-process core), `src/dict.rs` (`fingerprint`),
  `src/cluster/{shard,ring,coordinator,remote,server,proto}.rs`, `engine/grpc/proto/shard.proto`,
  `tests/cluster_grpc_oracle.rs`, `tests/cluster_oracle.rs`.
