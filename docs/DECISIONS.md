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
| 031 | Externalized single-node coordinator mutation log behind `trait ClusterLog` (clustering step 3a) | Accepted |
| 032 | Per-shard durable compiled segments — attach-and-mmap on open, not re-ingest (clustering step 3b) | Accepted |
| 033 | Shared-nothing cluster storage — supersede the Aurora-disaggregated / object-store framing | Accepted |
| 034 | Cross-process dict shipping over gRPC (the first shared-nothing multi-node step) | Accepted |
| 035 | Per-shard replication + peer recovery — the `ReplicatedShard` composite (clustering step 4a) | Accepted |
| 036 | gRPC multi-node per-shard replication + peer recovery (clustering step 4b) | Accepted |
| 037 | Cluster-state control-plane seam behind `trait ControlPlane` (clustering step 5a) | Accepted |
| 038 | openraft backend behind the `ControlPlane` seam + gRPC `ControlService` (clustering step 5b) | Accepted |
| 039 | Durable + replicated per-shard query log (the translog) + no-quiesce peer recovery (clustering step 5c) | Accepted |
| 040 | Translog retention leases + finalize under sustained writes (clustering step 5d) | Accepted |
| 041 | Durable Raft log + control-plane restart recovery (clustering step 5e) | Accepted |
| 042 | Shard→node allocator (rendezvous hashing) — committing the placement map (clustering step 5f) | Accepted |
| 043 | Swappable shard backing — the live-handoff routing-flip mechanism (clustering step 6a) | Accepted |
| 044 | Live data-moving handoff — the cross-node move that drives the swap (clustering step 6b) | Accepted |
| 045 | Autoscaler — the policy/trigger layer over rebalance + advisories (clustering step 6c) | Accepted |
| 046 | Dynamic vocabulary (Cluster v1) — feature-hashing for new tokens + runtime normalizer learning for aliases | Accepted |

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
  2. **Consistent-hash ring (virtual nodes) keyed on `FeatureId`** (not on `sig_key`). Safe *because* of
     the shared dict (ids are globally stable), and it gives the design's true ~2–5 fan-out: a title routes
     on its few rare features, not on the combinatorial set of probe-signatures it generates. (A `sig_key`-keyed
     ring would be correct but blow fan-out up to ~all shards for titles with several hot features.)
     `ring_hash` = FNV-1a + a murmur3 finalizer over the id (FNV alone clusters sequential ids and skews
     shard load); virtual nodes balance shard load at small K. The prior-art survey
     ([research/clustering-prior-art.md](research/clustering-prior-art.md) §1) compares ring+vnodes against
     jump-hash / rendezvous / Maglev and the *feature-token* (`fnv1a64(feature_name)`) keying a per-shard-dict
     design would require; the shared dict (sub-decision 1) lets us key on the integer id directly — simpler,
     faster (no name re-hash on the routing path), and the shared dict is mandatory anyway.
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
  §7, §10) and the prior-art survey ([research/clustering-prior-art.md](research/clustering-prior-art.md) —
  the hashing-variant comparison + the formal cross-shard correctness argument behind this ADR), ADR-001
  (semantic signatures — the anchor the ring hashes), ADR-003 (broad-query quarantine —
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
    still requires matching dicts — but it no longer fails *silently*.** **→ Dict-shipping LANDED (ADR-034):
    `connect_remote` now ships the frozen dict to each server, so a data node need not rebuild it from the
    corpus out-of-band.**
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
    still be built over the same feature space) — full dict-shipping stays deferred (ADR-029 out-of-scope;
    **shipped in ADR-034**).
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

---

### ADR-031: Externalized single-node coordinator mutation log behind `trait ClusterLog` (clustering step 3a)

- **Status:** Accepted.
- **Context:** ADR-027/029/030 built the in-process multi-shard core and its gRPC transport, but the
  coordinator had *no durability of its own*: live `add_query`/`remove_query` mutations existed only in
  shard memtables, so a coordinator restart lost every post-build write and there was no single ordered
  source of truth the whole cluster could be rebuilt from. `clustering-and-scaling.md` §10 step 3 is
  "externalize the mutation log (start with a single-node WAL, then Raft) and make segments loadable from a
  shared path." This ADR records the **first sub-step (3a) only**: a durable, ordered, append-only log of
  cluster mutations so the entire cluster is rebuildable from the log alone. The shared-path/object-store
  half of step 3 and all of Raft (step 4) stay design-only.
- **Decision:**
  - **A `trait ClusterLog` seam, not a concrete type** — mirroring the proven `trait Shard` local↔remote
    idiom. Two impls ship now: `FileClusterLog` (durable, CRC-framed) and `NullClusterLog` (in-memory: the
    no-`data_dir` path *and* a fast test backend). Both are exercised today, so the trait earns its keep on
    present need, not speculation, and they yield a differential test — `NullClusterLog ≡ FileClusterLog`
    proves coordinator behavior is log-impl-independent. The Raft-backed log drops in behind the same seam
    later (`append`→quorum-commit, `replay`→committed prefix, `checkpoint`→snapshot-install, epoch→term).
  - **Logical-id + raw DSL granularity.** Each `Add` logs `(logical_id, version, dsl)`, each `Remove` logs
    `logical_id`. Raw DSL — never compiled form — is the source of truth (the ADR-029 DSL-on-wire
    invariant), so replay recompiles against the manifest's frozen dict and re-derives placement through the
    existing `anchor_plan` path. Dict (fingerprint-checked) + ring (deterministic) ⇒ recovery reproduces the
    original placement exactly, so no shard boundary drops a match across a restart — the lossless-cover
    argument extended over a crash.
  - **A single `apply(mutation)` funnel.** Live writes and replay flow through one private apply path (the
    Raft state-machine `apply` in disguise), so replay reproduces live application by construction — they
    cannot drift. Write paths are **log-first / fail-closed**: `add_query`/`remove_query` append to the log
    *before* touching any shard; on append failure they emit `DurabilityFailure{WalAppend}` and return the
    error with shards untouched (the engine's WAL-first contract, ADR-017, lifted to the coordinator).
  - **Coordinator-level snapshot, not per-shard-Engine durability.** The base snapshot is the coordinator's
    distinct live set `logical → (version, dsl)` (reusing the `sources.dat` v2 shape + a version column),
    with the frozen dict stored *once* in a `ClusterManifest`. This is the "log is the database" shape (§4.1):
    `ClusterEngine::open` reads the manifest → fingerprint-checks the dict → re-derives the ring →
    bulk-rebuilds shards from the snapshot → replays the log tail through `apply`. (Per-shard segment
    durability — "segments loadable from a shared path" — is the *other* half of step 3, deferred to 3b.)
  - **New `clog.rs` framing, not the existing `Wal`.** `Wal`'s `OP_TOMBSTONE` is per-shard
    `(seg_idx, local_id)` and `Wal::parse_entries` treats unknown ops as a torn tail, so reusing the `Wal`
    *type* for logical-id ops is subtly broken. `clog.rs` copies the proven framing/CRC/torn-tail/fsync
    pattern (ADR-013) into a separate file with logical-level ops — a cluster log and an engine WAL can never
    be confused. `checkpoint()` writes a fresh base snapshot + new manifest (the atomic commit point) *before*
    truncating the log, so a crash mid-checkpoint just replays an already-applied (idempotent) tail.
- **Consequence:** An in-process cluster created with a `data_dir` survives a crash: `ClusterEngine::open`
  reconstructs byte-identical placement (zero false negatives) from manifest + base snapshot + replayed log,
  proven by `tests/cluster_durability_oracle.rs` (rebuild ≡ pre-crash ≡ brute across K∈{1,3,8} × broad
  on/off, plus checkpoint-compaction, torn-tail recovery, append-fails-closed, the two-backend differential,
  fsync parity, and fail-loud guards on a missing/corrupt manifest). Dependency-free (lean core, **not**
  behind `distributed`); the `NullClusterLog` path is byte-identical to pre-ADR-031, so `tests/cluster_oracle.rs`
  is unchanged. `LogPos` and `epoch` are **plumbed but not enforced** — both are needed now (replay cursor;
  checkpoint generation) and merely *shaped* like their Raft counterparts. **Deliberately deferred** (dead
  surface without Raft): per-entry epoch fencing, quorum / read-your-writes append modes, per-shard logs,
  object-store snapshots, and cross-process coordinator durability (`connect_remote` uses an in-memory log
  this increment). *(Amended by **ADR-033**: the "object-store" framing is dropped — the cluster is
  **shared-nothing** (local segments + per-node/coordinator WAL); object storage, if ever added, is only an
  optional pluggable backup target, never the serving path.)*
- **See also:** ADR-027 (the in-process core this extends), ADR-029 (the DSL-on-wire invariant replay relies
  on), ADR-030 (the dict-fingerprint check reused on `open`), ADR-013 (the engine WAL whose framing this
  copies), ADR-017 (the durable all-or-nothing ingest contract), ADR-021 (the `DurabilityFailure` event
  reused), `clustering-and-scaling.md` §10 step 3, `src/cluster/clog.rs`, `src/cluster/coordinator.rs`,
  `src/storage.rs` (cluster manifest + snapshot), `tests/cluster_durability_oracle.rs`.

### ADR-032: Per-shard durable compiled segments — attach-and-mmap on open, not re-ingest (clustering step 3b)

- **Status:** Accepted.
- **Context:** ADR-031 (step 3a) gave the coordinator a durable mutation log + a *coordinator-level* base
  snapshot of raw DSL, but `ClusterEngine::open` rebuilt every shard by **re-ingesting** — re-parsing,
  re-compiling, and re-indexing every query from that snapshot — before replaying the log tail. At the 100M-query
  target that recompile-on-every-restart is the dominant reopen cost. `clustering-and-scaling.md` §10 step 3's
  other half (3b) is "make segments loadable from a shared path so a replica attaches-and-mmaps instead of
  re-ingesting" — the Aurora "segments are materialized views of the log in shared storage" shape (§4.2). This
  ADR records the **local-dir** version (object store is a later step). The seam/`apply`-funnel/epoch from
  ADR-031 were shaped for exactly this; the log itself is unchanged.
- **Decision:**
  - **REPLACE the raw-DSL base snapshot with per-shard COMPILED durable segments.** Each shard is a segments-only
    durable `Engine` under `shard_<i>/` (`segments/seg_*.seg` + `sources.dat`), built over the coordinator's one
    shared frozen dict. On `open` a shard **attaches-and-mmaps** its committed segments and the log tail strictly
    after `snapshot_pos` is replayed through the same `apply` funnel as live writes — no re-ingest. The coordinator
    `live: Mutex<FastMap>` set and the `cluster_snapshot_<epoch>.dat` file are **removed** (the live set existed
    only to source that snapshot). *(Rejected: ADDITIVE — keep the snapshot AND add segments as a cache. It
    double-materializes the base, a second correctness surface, and keeps the dead live set. The lost "recompile-
    from-DSL is an independent recovery path" property is bought back by the differential brute oracle, which
    already cross-checks every reopen against a from-scratch ground truth.)*
  - **The coordinator manifest (v2) is the single atomic commit point**, exactly as in 3a (tmp + CRC + rename). It
    now records, per shard, the live segment-file registry `Vec<Vec<String>>` + per-shard `next_seg_id` (so a flush
    after reopen never clobbers a committed filename) alongside the dict + ring + log cursor + epoch. `build` and
    `checkpoint` commit it; `open` reads it as the authority for which `.seg` each shard attaches (NOT the shard's
    own manifest — shards write none).
  - **Checkpoint re-seals tombstoned base segments** (the load-bearing correctness fix). A `Remove` against a
    *base* segment only mutates its in-RAM alive overlay (`MmapSegment::tombstone`); the `.seg` keeps the old
    alive bits. So `checkpoint` = seal each shard's memtable into a segment **and** re-seal any base segment with
    tombstones (drop the dead entries into a fresh `.seg`, O(tombstoned data) not O(corpus)). Without this, a
    checkpoint that truncated a base-segment `Remove` from the log would let the deleted query RESURRECT on reopen
    — a false positive. This makes the invariant *the committed segment set reflects every applied mutation ≤
    snapshot_pos, including tombstones* a theorem, and matches the design's "segments are materialized views
    produced by the compaction job" (§4.1).
  - **Crash-safety mirrors 3a.** A crash *before* the manifest commit leaves the old (registry, cursor)
    authoritative — the freshly written `.seg` are orphans (not in the old registry, ignored + GC'd) and their
    entries are recovered via log replay, so there is no double-apply and no loss. A crash *after* the commit
    loads the new segments and replays only the (now shorter) tail.
  - **Fail loud on a missing / CRC-corrupt committed segment** (`open_shared_segments` returns `Err`), deliberately
    diverging from `Engine::open`'s skip-and-degrade: a skipped shard segment is a silent shard-sized false
    negative, which the zero-false-negative contract forbids. `segment_filenames()` likewise errors if a segment
    write fell back to in-memory, so the coordinator refuses to commit a registry that would lose it (all-or-nothing,
    ADR-017 lifted to the cluster).
  - **Engine surface is minimal and the flags are internal**, not `EngineConfig` (which is `Serialize`d into every
    snapshot + exposed via `/_settings`): a private `owns_manifest` bool, a `with_shared_segments_only` constructor
    (segment dir, no WAL, no own manifest), `open_shared_segments`, `segment_filenames`, `reseal_tombstoned_segments`.
    A pre-existing gap surfaced and fixed in passing: `class_counts` now tallies mmap segments too (it previously
    counted only in-memory/memtable segments, returning 0 for a reopened durable cluster's attached base).
- **Consequence:** A durable in-process cluster reopens by attach-and-mmap — no recompilation of the corpus — and
  still rebuilds byte-identical placement (zero false negatives). Proven by an extended
  `tests/cluster_durability_oracle.rs`: the existing rebuild ≡ pre-crash ≡ brute (K∈{1,3,8} × broad) plus new
  tests for attach-with-the-log-deleted, the **checkpoint-after-removing-a-build-time-query** bug-catcher (verified
  to fail without the re-seal), orphan-segment-ignored-and-GC'd, and corrupt-segment-fails-loud. Dependency-free
  (lean core, **not** behind `distributed`); `tests/cluster_oracle.rs` (in-memory) and the gRPC oracle are
  unchanged. **Deliberately deferred:** ~~object-store segments (S3 behind a path abstraction)~~ *(this
  "multi-node half" framing is superseded by **ADR-033** — the cluster is **shared-nothing**: per-shard local
  segments stay the durable base, no object store)*, a Raft-backed `ClusterLog`, cross-process / remote-shard
  durability (`RemoteShard::segment_filenames` returns `Err`), incremental (non-full) re-seal, and retaining
  build-time raw DSL on disk *before the first checkpoint* (the compiled segments are the base; sources.dat is
  written at the first flush/checkpoint, sufficient for a future feature-model re-materialize).
- **See also:** ADR-031 (step 3a, the coordinator log this builds the base on), ADR-027 (the in-process core),
  ADR-030 (the dict-fingerprint check reused on `open`), ADR-017 (all-or-nothing durable ingest), ADR-012 (the mmap
  segment format attached here), ADR-021 (the `DurabilityFailure` event), `clustering-and-scaling.md` §4.2 + §10
  step 3b, `src/cluster/coordinator.rs`, `src/cluster/shard.rs`, `src/segment/{lifecycle,compaction,persistence}.rs`,
  `src/storage.rs` (cluster manifest v2 + `MmapSegment::class_counts`), `tests/cluster_durability_oracle.rs`.

### ADR-033: Shared-nothing cluster storage — supersede the Aurora-disaggregated / object-store framing

- **Status:** Accepted.
- **Context:** The clustering design (`clustering-and-scaling.md` §4) modeled the durable layer on **Aurora's
  disaggregated storage**: a quorum mutation log + immutable compiled segments living in **shared object
  storage** (S3-shaped), with replicas/failover "attaching" to that shared storage. ADR-032's stated
  "multi-node half" was therefore *object-store segments* (swap the local `MmapSegment::open` for an S3 fetch).
  On review that is the wrong fit: (1) it implies an **external storage service** in the serving path, which
  clashes with this project's lean, self-contained, dependency-light ethos (16 deps, std-only core); (2) the
  payoff it buys — "cheap replicas / fast failover from shared storage" — only materializes once a multi-node
  control plane exists, which it does **not** yet; (3) it nudges the design toward a cloud-storage coupling we
  do not want. Crucially, **the systems we actually take cues from do not work this way.**
- **Decision:** Adopt the **shared-nothing** model that Elasticsearch/OpenSearch, Cassandra, and Kafka use,
  and which our building blocks already match:
  - **Local storage per node.** Each shard keeps its compiled segments on **local disk** (already true —
    ADR-032's per-shard `shard_<i>/segments/*.seg`). No shared storage in the serving path.
  - **Durability = a per-node/coordinator WAL** (already true — ADR-031's `ClusterLog`), the analogue of
    ES's per-shard translog.
  - **HA = primary/replica with peer recovery** (future): a new owner streams segments from a peer + replays
    the log tail, *not* from object storage — the ES/Cassandra recipe.
  - **Membership/routing = a quorum/Raft control plane** (future): holds the ring + shard→node map +
    feature-model version + log epoch.
  - **Object storage is NOT a dependency.** If it ever returns, it is only an **optional, pluggable
    snapshot/backup** target with a **local-filesystem default** (the shape of ES's `fs` snapshot repository,
    which is a plain shared directory — no cloud), never in the serving path and never AWS-coupled.
  *(Rejected: keep the Aurora-disaggregated model. It is a legitimate school — Aurora/Neon — but it trades
  self-containment for an external storage service to make replicas cheap; we get the same "cheap replicas"
  property from warm replicas + peer recovery without taking on that dependency, and the shared-nothing
  primitives are already built.)*
- **Consequence:** The clustering critical path is re-pointed: **dict shipping (ADR-034) → per-shard
  replication + peer recovery → Raft/quorum control plane → auto-split + autoscale.** Object-store
  segments leave the roadmap. ADR-031's and ADR-032's "deferred: object-store" notes are amended in place
  (their *local-disk* durability decisions stand unchanged — only the "object-store next" framing is dropped;
  ADRs are never renumbered/rewritten). `clustering-and-scaling.md` §4/§5/§8/§10 are reworked to the
  shared-nothing model; §2/§3 (the title-fan-out asymmetry + the anchor-routing no-false-negative argument)
  are **model-independent and unchanged**. No engine code changes in this ADR — it is a design realignment;
  the code increment that accompanies it is ADR-034.
- **See also:** ADR-031 (the coordinator WAL = the shared-nothing durable log), ADR-032 (per-shard local
  segments = the shared-nothing local base), ADR-027 (the in-process core), ADR-034 (dict shipping, the first
  shared-nothing multi-node step), `clustering-and-scaling.md` §4/§5/§8/§10,
  `research/clustering-prior-art.md` (the ES/Cassandra/Kafka vs Aurora/Neon comparison).

### ADR-034: Cross-process dict shipping over gRPC (the first shared-nothing multi-node step)

- **Status:** Accepted.
- **Context:** ADR-029/030 built the gRPC `ShardServer`/`RemoteShard` transport and a connect-time
  dict-fingerprint *handshake* (a divergent dict fails loud, not silently). But the handshake only *verifies*;
  it never *ships*. So a shard server had to obtain a **byte-identical frozen dict out-of-band** — in practice
  by rebuilding it from the **entire corpus** (`shardserver.rs` ran a full extract pass over the queries just
  to construct the dict). That is the opposite of a data node you can stand up empty, and it was the headline
  caveat on the cross-process transport. Under the shared-nothing realignment (ADR-033), making the existing
  transport actually deployable cross-process is the first concrete multi-node step.
- **Decision:** The coordinator **ships its authoritative frozen dict to each server at connect.**
  - **A new `AdoptDict` RPC.** Payload = the dict serialized by the existing core
    `crate::storage::serialize_dict` + the coordinator's `Dict::fingerprint` of it (an integrity check; the
    server recomputes and rejects a mismatch as `invalid_argument`). Reuses the *exact* bytes the cluster
    manifest already persists — no new serialization surface.
  - **Servers can start *pending* (dict-less).** New `ShardServer::pending(norm, config)` holds its
    `(dict, shard)` behind an `ArcSwapOption` (the codebase's `ArcSwap` snapshot idiom); reads against a
    pending server return `failed_precondition`. `ShardServer::new(norm, dict, config)` (pre-built) is kept,
    signature unchanged.
  - **Adoption contract (the load-bearing part).** On `AdoptDict`: **empty** shard (pending, or zero
    queries) → adopt (build a fresh `LocalShard` over the shipped dict); **same** fingerprint already held →
    idempotent no-op; **non-empty** shard whose dict **differs** → refuse with `failed_precondition`, because
    re-basing already-loaded data onto a different feature space would silently corrupt matches. The client
    (`RemoteShard::connect_and_adopt`) maps that refusal to `ShardError::DictMismatch` (reading back the
    server's actual fingerprint), so the silent-FN guard from ADR-030 is *preserved* — just relocated to where
    it is a real risk (a *committed* server), since adopting onto an empty server is correct, not an error.
  - **`connect_remote` ships by default.** It serializes the dict once and adopts per endpoint. Shipping an
    identical dict to a pre-built server is an idempotent no-op (the fingerprint matches), so existing callers
    (and the gRPC oracle's pre-built-server test) are behavior-preserved; the returned fingerprint *is* the
    handshake. No `ClusterConfig` change.
  - **Scope — dict only.** The fingerprint (and thus shipping) covers the **dict**. The **normalizer** must
    still match on both sides; everything uses `Normalizer::default_vocab()` today, which is corpus-independent
    and reproduced identically on any node, so the default case works end-to-end after shipping. Shipping +
    fingerprinting the vocab→normalizer is the explicit next hardening, **deferred** here.
- **Consequence:** A data node starts **empty** and is handed the frozen dict by the coordinator — no corpus,
  no out-of-band dict coordination. Proven by `tests/cluster_grpc_oracle.rs`: a new
  `grpc_cluster_with_dict_shipping` stands up K **pending** servers, ships the dict via `connect_remote`, and
  asserts the cluster ≡ single-node ≡ brute (broad on/off); the divergence test is updated to load data first
  (an empty server correctly *adopts*, so the guard now fires on a populated server holding a divergent dict);
  a `server.rs` unit test exercises every arm of the adoption contract. All behind the off-by-default
  `distributed` feature (lean core untouched). **Deferred:** normalizer/vocab shipping + fingerprint, TLS/auth
  on the transport, and the per-shard replication / Raft control-plane steps (ADR-033 roadmap).
- **See also:** ADR-029 (the transport + the DSL-on-wire invariant this completes), ADR-030 (the
  dict-fingerprint handshake this turns from verify-only into ship-then-verify), ADR-033 (the shared-nothing
  realignment this is the first step of), ADR-027 (the one-frozen-dict invariant), `src/cluster/server.rs`
  (`pending` + `AdoptDict`), `src/cluster/remote.rs` (`connect_and_adopt`), `src/cluster/coordinator.rs`
  (`connect_remote`), `engine/grpc/proto/shard.proto`, `tests/cluster_grpc_oracle.rs`.

### ADR-035: Per-shard replication + peer recovery — the `ReplicatedShard` composite (clustering step 4, in-process)

- **Status:** Accepted.
- **Context:** Under the shared-nothing realignment (ADR-033), the next clustering step after dict shipping
  (ADR-034) is **per-shard replication + peer recovery** — the Elasticsearch/Cassandra HA primitive: a shard
  position becomes a **primary + N replicas**, a write fans out to the replicas, a read **fails over** to a
  replica if the primary is down, and a fresh/recovering replica is brought up by **streaming the primary's
  local segments from a peer**. The building blocks already exist: ADR-031's coordinator log is the WAL and
  ADR-032's durable per-shard `.seg` files are the streamable segments. Following the rhythm of ADR-027
  (in-process sharding) → ADR-029 (gRPC transport), this ADR builds the **in-process** mechanism first,
  dependency-free and oracle-proven; the gRPC multi-node lift is ADR-036.
- **Decision:** A **`ReplicatedShard` composite** (`src/cluster/replica.rs`) that implements the existing
  `pub(crate) trait Shard` and wraps **one shard position's** copies — a primary `Box<dyn Shard>` + N replica
  `Box<dyn Shard>`. It slots into the coordinator's `Vec<Box<dyn Shard>>` via the existing `from_parts` seam
  with **zero coordinator changes** (the coordinator still sees one shard per position; the RF copies live
  inside the box), and composes over `LocalShard` (in-process) or — in ADR-036 — `RemoteShard`.
  - **Set-equality is the correctness basis.** Matching emits **logical** ids (local ids are segment-internal
    and append-only), so a replica fed the **same ordered op stream** holds the **same set of live logical
    queries** — byte-identical local ids are not required. Replication thus reduces to "apply the same op to
    every copy."
  - **The four guards (zero false negatives).** (1) **Reads** serve the primary and fail over **only on
    `ShardError::Remote`** (transport) and **only to an in-sync replica**; a `DictMismatch`/`Config`/`Log`
    error propagates (failing over would mask a real bug), and if every reachable copy fails the error
    propagates — never an empty/partial set. A replica that missed a write (out of sync) is never read. (2)
    **Aggregation presents the PRIMARY's view** — `num_queries`/`class_counts` reflect one copy and
    `delete_by_logical_id` returns the primary's count — because the coordinator *sums* these across shard
    *positions*; summing replicas would multiply totals by RF. (3) **Writes are primary-authoritative**: apply
    to the primary first (its return is the composite's; a primary error fails the op), then fan the same op to
    the in-sync replicas. (4) **Checkpoint/durability delegate to the primary** (`seal_for_checkpoint`/
    `segment_filenames`/`next_seg_id`), the manifest-recorded copy.
  - **Replica failures are tolerated (the Elasticsearch model).** A replica that errors on a replicated write
    is dropped from the in-sync set and a `DurabilityOp::ReplicaDesync` event is surfaced (redundancy reduced,
    flagged for re-recovery); the write still succeeds on the authoritative primary. A
    `wait_for_active_shards`-style write *precondition* would create a false-failure with the log-first
    coordinator (the primary + log already hold an acked write), so it is **deferred** to the control plane —
    there is no post-write min-in-sync rollback.
  - **Replicas are HA copies, not catalogued data.** `ClusterConfig::replication_factor` (default **1** —
    byte-identical to pre-ADR-035: RF=1 boxes a bare `LocalShard`, no composite). The **primary** is the
    durable copy at `shard_<i>/` recorded in the manifest (**`ClusterManifest` v2 unchanged**); replicas are
    extra copies (durable `shard_<i>/replica_<r>/` for a durable cluster, in-RAM for an in-memory one) seeded
    at `build` by the same op stream and **rebuilt on `open` by peer recovery** from the just-attached primary
    — then the log-tail replay feeds primary AND replicas through the composite. This matches ES ("replicas are
    allocated, not catalogued; the primary + log are the durable truth") and keeps the durable format stable.
  - **Peer recovery primitive** (`replica::peer_recover`): seal the primary (flush + reseal base tombstones) →
    copy its `.seg` files (and `sources.dat` if present — display-only, tolerated absent) into a clean replica
    dir → `LocalShard::open_segments` (fail-loud on a missing/corrupt segment). The in-process stand-in for ES
    "stream segments from a peer," and the basis for the gRPC streaming RPC in ADR-036. Durable-primary only
    (an in-memory primary has no files; in-memory clusters seed replicas by op-stream replay).
- **Consequence:** Dependency-free (lean core untouched). Proven by the extended `tests/cluster_oracle.rs`
  (RF ∈ {2,3} × K ∈ {1,3,8} × broad ≡ single-node ≡ brute; counts not inflated by replicas; live add/remove
  with primary-only remove counts) and `tests/cluster_durability_oracle.rs` (durable RF=2 reopen ≡ pre-crash ≡
  brute; checkpoint seals primaries only), plus `replica.rs` unit tests (in-sync failover, no-failover on
  `DictMismatch`, primary-write-failure propagation, replica-failure tolerance + `ReplicaDesync` event,
  set-equality through an op stream, peer recovery reproducing the primary set incl. a baked tombstone). One
  new trait method `Shard::set_event_sink` (default no-op) lets the coordinator fan its observer into the
  composites. **Deferred:** the gRPC multi-node lift (ADR-036 — replicas as `RemoteShard`s + a streaming
  segment-fetch RPC for cross-node peer recovery), and (control-plane, ADR-033 roadmap) automatic
  failure-detection/promotion, an allocator for shard→node placement, and `wait_for_active_shards`-style write
  preconditions.
- **See also:** ADR-027 (the in-process core + the one-frozen-dict invariant + the `from_parts` seam this
  reuses), ADR-031 (the coordinator log = the WAL replicas replay), ADR-032 (per-shard durable segments = what
  peer recovery streams), ADR-033 (the shared-nothing model this implements step 4 of), ADR-036 (the gRPC lift),
  `src/cluster/replica.rs`, `src/cluster/coordinator.rs` (`replication_factor`, `build`/`open` wiring),
  `src/cluster/shard.rs` (`set_event_sink`), `tests/cluster_oracle.rs`, `tests/cluster_durability_oracle.rs`.

### ADR-036: gRPC multi-node per-shard replication + peer recovery (clustering step 4b)

- **Status:** Accepted.
- **Context:** ADR-035 built per-shard replication + peer recovery **in-process** (the `ReplicatedShard`
  composite + the `peer_recover` primitive). This lifts it onto the gRPC transport — replicas on different
  nodes, with cross-node peer recovery — completing build-path step 4, following the ADR-027 (in-process) →
  ADR-029 (gRPC) rhythm. Behind the off-by-default `distributed` feature.
- **Decision:**
  - **Replicas are remote shards.** A new `ClusterEngine::connect_replicated(groups: &[ShardGroup], …)`
    connects + dict-ships (ADR-034) to every endpoint and wraps each position's primary + replica
    `RemoteShard`s in one `ReplicatedShard` — so the coordinator's placement / routing / merge is identical
    to a non-replicated remote cluster, while reads fail over and writes fan out (ADR-035). `connect_remote`
    (RF=1) is unchanged; a `ShardGroup` with no replicas degenerates to a bare `RemoteShard`.
  - **Servers become durable.** `ShardServer` gains a `data_dir` + `pending_durable`/`new_durable` ctors, and
    `AdoptDict` now builds a **segments-only durable** `LocalShard` when a `data_dir` is set — so the node's
    writes persist `.seg` files, the prerequisite for streaming or attaching segments. In-memory servers
    (today's default, and the dict-shipping oracle) are byte-for-byte unchanged.
  - **`FetchSegments` (server-streaming).** The source seals a consistent snapshot (`seal_for_checkpoint` —
    flush + reseal base tombstones), then streams a **manifest frame first** (the complete `.seg` file set +
    `next_seg_id` + dict fingerprint) followed by a chunked run per file (≤256 KiB `FileChunk`s; `sources.dat`
    last if present). The receiver pre-validates the manifest and **rejects a truncated stream rather than
    attaching a subset** (a subset is a silent shard-sized false negative); files land via tmp+rename. The
    request carries the dict fingerprint and the source refuses a mismatch (never ships segments compiled
    against a divergent feature space).
  - **`RecoverFrom` (target-driven — the Elasticsearch model: the recovering node pulls).** Coordinator
    `peer_recover_replica(source, target, handle)` ships the dict to the fresh node (adopt), then drives its
    `RecoverFrom`, which connects to the source peer, drains `FetchSegments`, attaches the segments
    (`open_segments`, fail-loud on missing/corrupt), and swaps in the recovered shard.
  - **One new dependency, distributed-only:** `tokio-stream` (the `ReceiverStream` wrapper for the
    server-streaming response). The lean core and the default server build are untouched.
- **Honest scope (the load-bearing boundary).** Peer recovery **quiesces writes to the position for the copy
  window.** The full ES "stream segments **+ replay the log tail**" needs a *durable / replicated coordinator
  log* for a remote cluster — but a remote cluster uses `NullClusterLog` (ADR-031's durable log is the
  in-process story), so there is no tail to replay. That snapshot-then-delta replay couples to the Raft
  control plane (step 5) and is deferred. Also deferred: an allocator deciding shard→node placement (the
  caller supplies `ShardGroup`s + recovery endpoints by hand — no membership / failure detector yet), TLS/auth
  (plaintext localhost), and true bounded-memory file streaming (the source reads one segment file into memory
  at a time today).
- **Consequence:** A coordinator can run replicas on separate gRPC nodes that fail over, and bring a fresh
  node up by streaming a peer's segments. Proven by `tests/cluster_grpc_oracle.rs`'s new
  `grpc_replicated_failover_and_peer_recovery`: K=3 × RF=2 durable servers, `connect_replicated` ≡ brute;
  stopping a primary still serves correct reads via its replica (failover — which also proves ingest fanned
  out to the replica); and a fresh node peer-recovers a position's segments from a live peer and then serves
  that position correctly inside a verify cluster. Full `check.sh` green (incl. the `clippy (distributed)` +
  `tests (distributed)` lanes).
- **See also:** ADR-035 (the in-process composite + `peer_recover` this lifts onto gRPC), ADR-029 (the
  transport + the DSL-on-wire invariant), ADR-034 (dict shipping — reused per endpoint), ADR-031/032 (the
  coordinator log + per-shard durable segments peer recovery streams), ADR-033 (the shared-nothing model),
  `engine/grpc/proto/shard.proto`, `src/cluster/{server,remote,coordinator}.rs`, `src/bin/shardserver.rs`,
  `tests/cluster_grpc_oracle.rs`.

### ADR-037: Cluster-state control-plane seam behind `trait ControlPlane` (clustering step 5a)

- **Status:** Accepted (increment 5a — the dependency-free seam; the openraft backend is 5b, the quiesce-gap
  fix is 5c, both roadmap).
- **Context:** Build-path step 5 is the **quorum/Raft control plane**: a small, quorum-replicated
  cluster-state document (consistent-hash ring params + the **shard→node map** + membership + feature-model
  version + an epoch) — the Elasticsearch cluster-manager model the shared-nothing design (ADR-033 §4.3)
  commits to. It is also what unblocks the two honest-scope gaps ADR-036 left (shard→node placement /
  membership, and eventually the recovery-quiesce window). Pulling a consensus library is a heavy-dependency
  decision, so two forks were settled with the maintainer first: **(1) seam-first** — build a dependency-free
  `ControlPlane` seam + an in-memory backend now, proven by an oracle, exactly as `trait ClusterLog` +
  `NullClusterLog` (ADR-031) preceded any real durability engine, so the consensus engine drops in behind a
  stable firewall; **(2) control-plane state only** — consensus holds the small, low-rate cluster-state doc,
  **never** the ~750k/sec query mutations (those stay on `ClusterLog` + the per-shard primary→replica path,
  ADR-031/035/036) nor the per-shard segment registry (that stays in the local `ClusterManifest`, ADR-032);
  **(3) target engine = openraft** (step 5b) — it owns the dangerous parts (joint-consensus membership +
  snapshots) a zero-false-negatives project should not hand-roll, is actively maintained (unlike tikv/raft-rs,
  now in maintenance mode), and is async/`distributed`-gated so the **lean core never sees it**. The
  lean-dependency philosophy is real but not absolute — battle-tested consensus that owns the perilous parts
  is worth the (feature-gated) weight.
- **Decision (5a, this increment — `engine/src/cluster/control.rs`, lean core, no new dependency):**
  - **The seam.** `trait ControlPlane: Send + Sync` — sync, fallible (`Result<_, ControlError>`), the
    document-mutation + linearizable-read sibling of `ClusterLog`. Methods: `cluster_state()` (a cheap
    `Arc<ClusterState>` snapshot read), `version()`, `propose(ClusterStateChange)`, `change_membership(voters)`,
    `leader()`. **Not** a log-append seam — a consensus library owns its own log, so the seam abstracts
    *committed state* + *proposals*, not framed bytes.
  - **The document.** `ClusterState { epoch, nodes, voters, assignments, num_shards, vnodes, dict_fingerprint,
    model_version }` (`serde`, self-contained — the future Raft snapshot payload); `ClusterStateChange`
    (`AddNode`/`RemoveNode`/`AssignShard`/`BumpModelVersion`) is the future log-entry payload; `NodeId`
    (newtype), `NodeRole`, `NodeDescriptor`, `ShardAssignment`, `StateVersion`, and a typed `ControlError`.
  - **The backend.** `InMemoryControlPlane` applies every proposal immediately and is always `Ok` (a single
    node trivially has a quorum) — the `NullClusterLog` analogue + the fast differential-test backend.
    `single_node(num_shards, vnodes, dict_fingerprint)` is the default the coordinator builds: one
    `NodeId(0)` owning every position, so the RF=1 / in-process path is **byte-identical** to pre-ADR-037.
  - **Coordinator wiring.** `ClusterEngine` gains `control: Box<dyn ControlPlane>` threaded through the
    existing `ClusterDurable` bundle (no `from_parts` signature change); `build`/`open`/`connect_*` default it
    to `single_node`. New introspection: `control_state()`, `assignment_for(position)` (errors loudly on an
    unassigned live position — never a silent default, the fail-closed stance), `reassign_shard()`. The
    placement/route/apply/percolate hot path is **untouched** — the control plane is read at
    assembly/introspection time only.
  - **Shape choices baked in for openraft (so 5b changes no call site).** `ControlError::ForwardToLeader`
    exists from day one (a follower's `client_write` returns it); `change_membership` is **distinct** from
    `propose` (joint consensus is special in Raft — folding it in would force a re-cut); reads are a snapshot
    *pull*, not a watch (openraft has no watch of an application document); `ClusterState.epoch` (an app
    counter) is kept distinct from the future Raft term **and** from `ClusterManifest.epoch` (the local
    checkpoint generation) — three distinct notions, deliberately not unified.
- **Honest scope (the correction to carry forward).** The roadmap shorthand "the Raft step unblocks the
  quiesce-during-recovery gap" is **imprecise**. The control-plane Raft holds the *cluster-state doc*, which is
  explicitly **not** the query mutations; building it provides membership + epoch fencing (necessary) but does
  **not by itself** lift the ADR-036 quiesce window. Lifting it requires the **per-shard query log** to become
  durable + replicated (the ES translog) so a recovering replica streams segments from a peer **and then
  replays the tail after `snapshot_pos`** — a *distinct* mechanism from the control-plane doc, scheduled as
  **5c**. Also still design-only: the openraft backend itself (5b — a `RaftControlPlane` over `Raft<C>`, a new
  gRPC `ControlService` carrying an opaque-bytes envelope, manager role/bin, multi-node elections); an
  allocator that *acts* on the shard→node map (5a commits a reassignment as a **map-only** change — no physical
  data movement); TLS/auth. **Increment plan: 5a** = the seam (here); **5b** = the openraft backend; **5c** =
  the durable/replicated per-shard query log that closes the quiesce gap.
- **Consequence:** The coordinator now carries a quorum-shaped cluster-state seam with node identity + a
  shard→node map, dependency-free and byte-identical by default. Proven by `tests/cluster_control_plane_oracle.rs`
  (the default control plane ≡ the independent brute oracle across K×RF; the committed document is well-formed;
  a shard reassignment advances the epoch + changes the map while every match set is unchanged; every backend
  driven by one script converges to the identical document — the two-backend differential, openraft-ready) +
  nine `control.rs` unit tests (apply determinism, idempotency, fail-closed). The existing
  `cluster_oracle`/`cluster_grpc_oracle`/`cluster_durability_oracle` are unchanged and stay green — itself the
  byte-identical acceptance signal. Full `check.sh` green (fmt + clippy ×3 incl. lean-core + tests ×2 incl.
  distributed + audit + deny).
- **See also:** ADR-031 (the `ClusterLog` seam + one-`apply`-funnel + manifest-epoch this mirrors and was
  shaped for), ADR-033 (the shared-nothing control-plane model §4.3), ADR-027 (the in-process core + the
  one-frozen-dict invariant), ADR-035/036 (per-shard replication — the data-path HA the control plane sits
  *above*, and the quiesce gap 5c closes), `src/cluster/control.rs`, `src/cluster/coordinator.rs`
  (`control` field, `control_state`/`assignment_for`/`reassign_shard`), `src/cluster/shard.rs`
  (`ShardError::ControlPlane`), `tests/cluster_control_plane_oracle.rs`.

### ADR-038: openraft backend behind the `ControlPlane` seam + gRPC `ControlService` (clustering step 5b)

- **Status:** Accepted (increment 5b — the real consensus backend; the durable-query-log quiesce fix is 5c,
  roadmap).
- **Context:** ADR-037 (step 5a) shipped the dependency-free `trait ControlPlane` seam + an in-memory backend
  and froze the seam's shape *for openraft* (membership distinct from `propose`, a `ForwardToLeader` error,
  snapshot-read, app-epoch ≠ Raft term). Step 5b drops the real consensus engine in behind that **unchanged**
  seam, plus the cross-process transport for the managers' own consensus. The engine choice (`openraft`, not
  tikv/raft-rs) was settled with the maintainer in ADR-037: it owns the dangerous parts (joint-consensus
  membership + snapshots) a zero-false-negatives project should not hand-roll, is actively maintained, and is
  `distributed`-gated so the lean core never sees it.
- **Decision (`engine/src/cluster/control_raft.rs` + `control_server.rs`, `distributed`-gated):**
  - **Dependency.** `openraft = "=0.9.24"` (latest STABLE — the 0.10 line is alpha), `optional`, in the
    `distributed` feature only. Features: `serde` (the Raft messages cross the wire), `storage-v2` (the
    non-deprecated split `RaftLogStorage` + `RaftStateMachine` traits — the legacy `RaftStorage` + `Adaptor`
    would trip our `-D warnings` clippy lane), `generic-snapshot-data` (ship the tiny cluster-state snapshot
    whole via `full_snapshot`, no chunked streaming). `cargo deny` accepts the tree (openraft is Apache-2.0;
    its transitives are MIT/Apache/BSD — no allowlist change needed).
  - **Type config.** `declare_raft_types!(TypeConfig: D = ClusterStateChange, R = ClusterStateResponse)` —
    `ClusterStateChange` (ADR-037's "future log-entry payload") IS the Raft log entry; `NodeId = u64`,
    `Node = BasicNode` (its `addr` is the gRPC endpoint the transport already passes).
  - **State machine reuses the ONE apply funnel.** `RaftStateMachine::apply` routes a committed
    `Normal(ClusterStateChange)` through `control::apply` — the SAME function `InMemoryControlPlane` uses
    (made `pub(super)`) — so the two backends are live ≡ replay by construction. A `Membership` entry derives
    `ClusterState::voters` from the Raft voter set (the faithful `change_membership` mapping); a `Blank`
    leader-marker is a no-op. The state machine + an in-memory log store complete the openraft storage traits.
  - **`RaftControlPlane` is the seam impl.** `cluster_state` → `ensure_linearizable` then read the SM;
    `propose` → `client_write`; `change_membership` → `Raft::change_membership`; `leader` →
    `current_leader`. openraft's `ForwardToLeader` maps 1:1 onto `ControlError::ForwardToLeader`, so the
    coordinator changes **no call site**. The sync seam bridges onto async Raft with `handle.block_on` (off
    the runtime's worker threads — exactly the `RemoteShard` bridge; the control plane is never on the
    per-title hot path).
  - **Cross-process transport.** A new `ControlService` (3 RPCs: AppendEntries / Vote / Snapshot) added to the
    **existing** `engine/grpc/proto/shard.proto` (one FDS, no `build.rs` change). The wire is an **opaque
    `bytes` envelope** carrying the serde-encoded Raft message — the proto need not mirror openraft's intricate,
    version-coupled message types; the handler's `Result<_, RaftError>` is encoded *inside* the reply, only
    transport failures surface as a gRPC status. A tonic-backed `RaftNetwork`/`RaftNetworkFactory` (lazy
    per-target clients) is the client; `ControlServer` is the server (relays each RPC to the local Raft
    handler), served via the same port-race-safe `serve_with_incoming` as `ShardServer`. New bin
    `controlserver` (a manager node; `--bootstrap` forms the initial cluster).
  - **No coordinator change.** The seam already accepts any `Box<dyn ControlPlane>` (ADR-037); the default
    backend stays `InMemoryControlPlane`, so every existing oracle is byte-identical and green. The backend is
    exercised through the public `trait ControlPlane` — the exact surface the coordinator depends on.
- **The load-bearing design subtlety.** A faithful Raft proof is inherently **multi-node**: a lone node cannot
  satisfy a voter-set change, and openraft commits its own `Blank`/`Membership` log entries, so the semantic
  `ClusterState::epoch` is **not** comparable to the in-memory backend's under the same script. So the openraft
  backend gets its OWN multi-node differential (3 real nodes converge to the same voters/nodes/assignments/model
  the in-memory backend reaches — NOT epoch), rather than slotting into ADR-037's single-handle
  `control_plane_backends_agree` test. `ClusterState::voters` is openraft-membership-derived in this backend (it
  ends at the same set the in-memory backend reaches via `change_membership`).
- **Honest scope (carried forward from ADR-037).** This does **not** close ADR-036's recovery-quiesce window —
  that needs a durable/replicated *per-shard query log* (the ES translog), a distinct mechanism scheduled as
  **5c**. Also deferred: a durable `RaftLogStorage` (CRC-framed, reusing `storage::crc32` + `durable_rename`) +
  restart-recovery (the in-memory log proves convergence, not crash recovery); TLS/auth on the control
  transport; an allocator that *acts* on the shard→node map (5a/5b commit map-only changes).
- **Consequence:** The cluster-state control plane is now backed by a real, battle-tested consensus engine
  behind the same seam — multi-process elections, leader failover, and committed-state durability across a
  leader death — with the lean core untouched (openraft is absent from the `--no-default-features` dependency
  graph). Proven by `tests/cluster_control_raft_oracle.rs` (a 3-node in-process cluster converges to the
  in-memory document; a follower `propose` returns `ForwardToLeader`; `change_membership` routes to Raft; and —
  over real gRPC `ControlService` servers on localhost — the cluster elects a leader, survives that leader being
  killed, re-elects from quorum, preserves the committed document, and accepts a fresh write). Full `check.sh`
  green (fmt + clippy ×3 incl. lean-core + tests ×2 incl. distributed + audit + deny).
- **See also:** ADR-037 (the seam this fills + the shape choices that made it drop-in), ADR-031 (the
  `ClusterLog` seam + one-`apply`-funnel this mirrors), ADR-029/034 (the gRPC transport + dict shipping the
  `ControlService` sits alongside), ADR-036 (the data-path HA + the 5c quiesce gap), ADR-033 (shared-nothing —
  consensus holds the cluster-state doc only, never query mutations), `src/cluster/control_raft.rs`,
  `src/cluster/control_server.rs`, `src/bin/controlserver.rs`, `engine/grpc/proto/shard.proto`
  (`ControlService`), `tests/cluster_control_raft_oracle.rs`.

### ADR-039: Durable + replicated per-shard query log (the translog) + no-quiesce peer recovery (clustering step 5c)

- **Status:** Accepted.
- **Context:** ADR-036's gRPC peer recovery copies a *point-in-time snapshot* of a shard's `.seg` files, so
  writes to the position had to be **quiesced** for the whole copy window (documented in `server.rs`,
  `coordinator.rs`, `replica.rs`). The reason was structural: a remote/gRPC cluster uses `NullClusterLog` (the
  ADR-031 coordinator log is the *in-process* story), so there was no durable tail to replay the writes that
  land during the copy. This is the Elasticsearch **translog** gap — the last data-plane hole before a real
  multi-node deployment. A correction carried from ADR-037/038: the control plane does **not** close this gap
  (it holds the cluster-state *doc*, never query mutations); closing it needs the per-shard *query* log.
- **Decision (`src/cluster/translog.rs` + `shard.rs` + `replica.rs` + the gRPC surface):**
  - **Reuse, don't reinvent.** The translog reuses ADR-031's proven log machinery verbatim: the
    `ClusterMutation { Add{logical,version,dsl}, Remove{logical} }` op (logical-id + raw DSL — the ADR-029
    DSL-on-wire invariant, re-compilable against the frozen dict → byte-identical placement), the opaque
    `LogPos`, and the CRC-framed `FileClusterLog` / in-memory `NullClusterLog` backends (torn-tail forward-scan
    recovery, atomic tmp+rename checkpoint, `fsync` knob). `translog.rs` is the thin per-shard wiring. **Not**
    the engine WAL: its tombstone is a per-shard *physical* `(seg_idx, local_id)`, un-replayable on a peer whose
    local ids differ (replicas are set-equal, not byte-identical) — the same reason ADR-031 declined it.
  - **Owned by the durable `LocalShard`.** Each durable shard (an in-process replica *or* a gRPC data node)
    keeps its own dense, monotonic translog rooted in its data dir; in-memory shards keep a `NullClusterLog` →
    byte-identical to pre-ADR-039. Writes are **log-first / fail-closed**: `insert_extracted` /
    `delete_by_logical_id` append the mutation under the engine lock (so log order == apply order) BEFORE
    applying, rejecting the write on an append failure (the per-shard analogue of `add_query`). Bulk
    `ingest_extracted` goes straight to a durable base segment (no translog). **Replication rides the existing
    primary→replica fan-out** — each in-sync replica appends to its own translog (the ES model; no new transport).
  - **The position boundary is the zero-false-negative lynchpin.** `seal_for_checkpoint` (flush memtable →
    reseal base tombstones) captures `P = last_pos` under the write lock and trims the translog to `P`, so the
    segments hold exactly ops ≤ `P` and the tail exactly ops > `P`. Recovery streams segments (≤ `P`) then
    replays the tail (> `P`): no overlap, no double-apply — the property `ClusterEngine::open` already relies on,
    pushed to the shard. (`add_compiled` is append-only, so correctness rests on the position bound, never on add
    idempotency.)
  - **No-quiesce recovery, both paths.** In-process `peer_recover` seals the primary at `P`, copies its
    segments, attaches, then replays the primary's translog tail (> `P`) into the new replica — the writes that
    landed during the copy, recovered rather than lost; a re-runnable `catch_up_replica` drains any further tail.
    Over gRPC: `FetchManifest.up_to_seqno` carries `P`, a new server-streaming `FetchTranslog(after_seqno)` RPC
    serves the un-sealed tail (read-only — no seal, so the source keeps accepting writes), and the coordinator's
    `peer_recover_replica` recovers segments then replays the tail through the SAME apply funnel (re-derived from
    DSL). The documented quiesce notes are deleted. The wire `TranslogEntry { seqno, oneof{ AddItem add; uint64
    remove_logical } }` reuses `AddItem` (typed, not opaque — keeps the wire DSL-bearing + oracle-assertable);
    all additive, zero `build.rs` change.
  - **Data-node self-restart (§6).** A durable shard records a per-shard checkpoint **sidecar** (`shard.ckpt`:
    `next_seg_id` + `local_checkpoint P` + segment list + dict fingerprint, CRC + atomic tmp+rename) at each
    seal — AFTER the segments are durable, BEFORE the translog is trimmed, so a crash in between just replays an
    already-captured, position-filtered prefix. `new_durable` finds the sidecar on restart and attaches the
    committed segments + replays the translog tail (engine-only, since the ops are already in the log), so a
    `shardserver --data-dir` survives its own crash with no coordinator manifest (the remote coordinator is
    non-durable). The sidecar's dict-fingerprint guard refuses attaching segments built for a divergent space.
- **Honest scope.** Recovery is deterministic-by-ordering in the oracles (snapshot → write → tail catch-up),
  which exercises the exact path concurrent writes take during the copy; under *sustained* writes, full
  convergence still needs a brief finalize (the quiesce window shrinks from the whole copy to the residual
  delta — `catch_up_replica` is the loop). Translog **retention/GC** for a slow recovering replica (keep the
  tail back to the slowest follower) is a policy not yet set — 5c seals the source fresh, so the copy's `P` is
  current. Deferred (unchanged from prior steps): TLS/auth (plaintext localhost); bounded-memory streaming of a
  very large tail; an allocator acting on the shard→node map; add-as-upsert / version-LWW (replay preserves op
  order). For the in-process *durable cluster*, the coordinator `ClusterLog` remains the authoritative
  crash-rebuild source (`open` resets the per-shard translog) — the per-shard translog is the recovery tail +
  the data-node durability; unifying the two logs is a future cleanup.
- **Consequence:** A coordinator can bring a fresh node up from a live peer WITHOUT quiescing the source's
  writes, and a durable data node self-recovers after its own crash. The default in-memory / RF=1 / in-process
  paths are byte-identical, so every prior oracle is unchanged and green — the acceptance signal. Proven by:
  `tests/cluster_grpc_oracle.rs::grpc_peer_recovery_without_quiescing` (a fresh node recovers segments at `P`,
  writes land after `P`, the translog tail catches them up, recovered ≡ live source ≡ brute oracle over the
  final live set — zero false negatives across the wire); `replica.rs::peer_recover_replays_tail_without_quiescing`
  (the in-process analogue) and `::durable_shard_self_restarts_from_translog` (§6); plus `translog.rs` unit tests
  (fresh/reset, torn-tail via the reused `clog` backend, the sidecar round-trip). Full `check.sh` green (fmt +
  clippy ×3 incl. lean-core + tests ×2 incl. distributed + audit + deny). `translog.rs` is std-only (lean core);
  the gRPC pieces are `distributed`-gated.
- **See also:** ADR-031 (the `ClusterLog` seam + CRC framing + one-`apply`-funnel this reuses and re-homes per
  shard), ADR-036 (the gRPC peer recovery whose quiesce gap this closes), ADR-035 (the in-process `ReplicatedShard`
  + `peer_recover` this extends), ADR-032 (the per-shard durable segments the translog tails), ADR-029/034 (the
  transport + DSL-on-wire + dict shipping the `FetchTranslog` wire reuses), ADR-037/038 (the control plane — the
  cluster-state doc, explicitly NOT the query mutations this log carries), ADR-033 (shared-nothing — local
  segments + per-node durable log, no object store), `src/cluster/{translog,shard,replica,server,remote,coordinator}.rs`,
  `engine/grpc/proto/shard.proto` (`FetchTranslog`/`TranslogEntry`), `tests/cluster_grpc_oracle.rs`.

### ADR-040: Translog retention leases + finalize under sustained writes (clustering step 5d)

- **Status:** Accepted.
- **Context:** ADR-039 made peer recovery no-quiesce by streaming a peer's segments at position `P` then
  replaying the translog tail (> `P`). Its own *Honest scope* flagged two coupled gaps that 5d closes:
  1. **A latent false negative under a concurrent seal.** ADR-039's `seal_for_checkpoint` trims the translog
     unconditionally to its checkpoint `P`. If a recovery has snapshotted segments at `P_snap` and is still
     replaying the tail, a *concurrent* seal (another recovery's `FetchSegments`, a checkpoint) trims the source
     past `P_snap`, moving those ops into NEW segments the recovering node never copied — so its
     `translog_tail(P_snap)` silently loses them. The no-quiesce oracle didn't hit this only because it ordered
     snapshot → write → catch-up with no second seal; concurrent recoveries from one source are a real
     deployment shape.
  2. **No bounded finalize under sustained writes.** A single seal→copy→catch-up leaves the replica caught up
     to a high-water, but writes that landed during the catch-up are still behind. Promoting the replica
     into the in-sync set without a final reconciliation would silently miss those writes.
- **Decision — retention leases (the Elasticsearch *peer-recovery retention lease*), `src/cluster/{shard,replica,server,remote,coordinator}.rs`:**
  - **A lease registry on the recovery source.** `LocalShard` holds `Mutex<RetentionLeases>` (`lease_id →
    retained_pos`). `seal_for_checkpoint` now trims to **`min(P, leases.floor())`** instead of `P`; with no
    lease the floor is absent and it trims to `P` — **byte-identical to ADR-039**. The sidecar's
    `local_checkpoint` stays `P` (segments still capture ≤ `P`); any retained ops in `(trim_to, P]` are
    redundant with the segments and position-filtered out on replay (`replay(P)` ⇒ ops > `P`), so the
    self-restart path is unchanged. Three new `Shard` methods (defaults: a no-op lease at `LogPos(0)`, so
    in-memory / remote-less shards and every non-recovery caller are untouched): `acquire_retention_lease()
    -> (id, pos)` pins at the current high-water; `renew_retention_lease(id, to)` advances it (monotonic) as
    a consumer catches up so the prefix can GC; `release_retention_lease(id)` drops it. `ReplicatedShard`
    delegates all three to its primary (the recovery source).
  - **Why a lease and not "don't trim during recovery."** A single global "recovering" flag breaks under
    concurrent recoveries; leaving the translog untrimmed unbounds it. The lease set takes the MIN across
    holders (correct for N concurrent recoveries) and trims freely the instant the last lease drops (bounded
    GC). The acquire's read-then-register is benign under a racing seal: a seal that trims to `L' > at` before
    the lease registers also *sealed* `(at, L']` into segments, so a recovery copying segments at `P ≥ L'`
    still has them; once registered, no later seal trims past `at`.
  - **The finalize (bounded quiesce), `ReplicatedShard::add_recovered_replica` + `ClusterEngine::add_replica`
    / `peer_recover_replica`.** Hold ONE lease across the whole flow: peer-recover a snapshot + initial tail,
    then **loop** `catch_up_replica` (renewing the lease each pass) until the tail stops advancing — shrinking
    the residual a final quiesce must cover toward zero. In-process, the promotion drains the last residual
    and inserts the replica into the in-sync set **under the composite `write_lock`** (so no write slips
    between the final drain and the in-sync insertion — an atomic promotion); `replicas` became
    `Mutex<Vec<Arc<ReplicaSlot>>>` to allow this runtime growth, with reads/fan-out snapshot-cloning the `Arc`
    handles so a slow probe never holds the lock. **Correctness never depends on the loop converging** — the
    lease keeps the tail safe regardless; only the residual *window size* does (`max_passes` bounds it).
  - **Over gRPC.** A `RetentionLease(op, lease_id, pos, dict_fingerprint)` RPC (op 0/1/2 = acquire/renew/
    release; dict-fingerprint-guarded like `FetchTranslog`) plumbs the three methods to the server's shard;
    `peer_recover_replica` acquires the lease before the segment copy, holds it across the convergence loop,
    and releases on completion (a release failure on an otherwise-good recovery is surfaced as a
    `ReplicaDesync` event, never conflated with the recovery outcome). Additive proto, zero `build.rs` change.
- **Honest scope.** The in-process finalize promotes atomically under `write_lock` (fully lease-protected
  end-to-end). The gRPC `catch_up_recovered_replica` is a *lease-free* manual pass for callers that have
  externally quiesced — a concurrent seal during it could still strand it, so the retention-safe gRPC entry is
  `peer_recover_replica` (which holds the lease across its own convergence loop); a true cross-node in-sync
  *promotion* of a remote replica (vs. the test's separate verify cluster) routes through the allocator
  (ADR-042) and is not yet wired. Retention is keyed by lease only — there is no time/size cap on a stuck
  lease yet (a crashed recovering node leaves its lease until the source restarts or a future lease-expiry
  policy lands). Deferred unchanged: TLS/auth, bounded-memory streaming of a very large tail.
- **Consequence:** A concurrent seal can no longer trim away an in-flight recovery's tail (the latent FN is
  closed), the translog GCs the moment no recovery needs it (no unbounded growth), and a replica can be grown
  into a live position at runtime without pausing writes — the quiesce window is the residual delta, not the
  whole copy. Default in-memory / RF=1 paths are byte-identical (no lease ⇒ trim to `P`), so every prior
  oracle is unchanged and green. Proven by: `replica.rs::seal_honors_retention_lease_so_concurrent_seal_keeps_the_recovery_tail`
  (a second seal during a held lease keeps the tail; releasing it lets the source GC) and
  `::add_recovered_replica_promotes_an_in_sync_set_equal_replica` (runtime growth → an in-sync, set-equal
  replica that receives post-promotion writes); `tests/cluster_grpc_oracle.rs::grpc_peer_recovery_converges_under_sustained_writes`
  (a writer thread streams adds CONCURRENTLY with the recovery; the lease keeps the racing writes safe and the
  target converges to live source ≡ brute over the final set). Full `check.sh` green. The lease registry +
  finalize are std-only (lean core); the `RetentionLease` RPC is `distributed`-gated.
- **See also:** ADR-039 (the no-quiesce translog whose two scope gaps this closes), ADR-036/035 (the gRPC +
  in-process peer recovery the lease protects), ADR-031 (the `LogPos`/`ClusterLog` machinery), ADR-042 (the
  allocator that will drive cross-node promotion), ADR-033 (shared-nothing),
  `src/cluster/{shard,replica,coordinator,server,remote}.rs`, `engine/grpc/proto/shard.proto` (`RetentionLease`).

### ADR-041: Durable Raft log + control-plane restart recovery (clustering step 5e)

- **Status:** Accepted.
- **Context:** ADR-038 shipped the openraft control-plane backend with an **in-memory** `RaftLogStorage`
  + `RaftStateMachine` — enough to prove consensus convergence (the 3-node oracle), but a manager node
  lost its entire Raft state on restart, so it could not actually rejoin a quorum after a crash. ADR-038's
  own scope note flagged "a durable Raft log (CRC-framed, reusing `storage::crc32`)" as the deferred
  follow-on. This is that follow-on: the byte-level durable substrate that makes a `controlserver` node
  survive a restart.
- **Decision (`src/cluster/control_store.rs` + the durable mode in `control_raft.rs`):**
  - **What openraft actually requires durable (0.9.24 storage FAQ), and only that.** The vote (election
    safety — two leaders in one term if lost), the log entries (so committed-but-un-snapshotted entries can
    replay), the **committed** log id (`save_committed`, so a restart re-applies `(snapshot.last, committed]`),
    and the state-machine **snapshot** (so the log can be `purge`d and the SM rebuilt). The state machine
    itself is **NOT** persisted per-apply — openraft rebuilds it on restart from the latest snapshot + the
    durable log replayed up to `committed`. So `apply` stays a pure in-memory `control::apply` (unchanged
    from ADR-038 ⇒ live ≡ replay preserved), and the durable cost is one fsync per low-rate control op, never
    per query.
  - **Two on-disk shapes, reusing proven patterns** (`control_store.rs`): a **CRC-framed append-only record
    log** (`append_record`/`read_records`/`rewrite_records`) for the Raft entries — the same forward-scan /
    torn-tail recovery shape as `clog`/`wal.rs` (a crash mid-append drops the last partial frame, never
    corrupts an acknowledged prefix), `truncate`/`purge` are an atomic rewrite + reopen; and **atomic
    single-value files** (`write_value`/`read_value`, tmp + fsync + rename + parent-fsync) for the
    vote / committed / last-purged / snapshot. Serialization is `serde_json` (the SAME codec the gRPC
    `RaftNetwork` already uses — every persisted type, incl. `Entry<TypeConfig>` and `SnapshotMeta`, is
    already serde for the wire); CRC via the core `storage::crc32`.
  - **One backend, two modes, selected by a dir.** `LogStore`/`StateMachine` gained `in_memory()` (the
    ADR-038 path — the in-process oracle, **byte-identical**) and `open(dir, fsync)` (durable). `build_node`
    takes `Option<&Path>`; `in_process_cluster` passes `None` (oracle stays in-RAM); `start_grpc_node` (and
    the `controlserver --data-dir` flag) pass a dir → durable, fsync on. The seam (`trait ControlPlane`) and
    every coordinator call site are unchanged.
  - **Restart is idempotent.** A durable node, rebuilt over the same dir, loads vote+log+committed+snapshot,
    re-elects from its persisted vote, and openraft replays the committed tail into the SM. A new
    `RaftControlPlane::shutdown()` cleanly joins the core so the files are released before a restart from the
    same dir; `initialize` returns `NotAllowed` on an already-formed cluster (ignored), so the same builder
    serves first-boot and restart.
- **Honest scope.** A genuine multi-process *rolling* restart (kill one of three live gRPC managers, restart
  it, watch it rejoin) is exercised in spirit by ADR-038's `grpc_three_node_survives_leader_failure` plus
  this step's single-node durable restart; an end-to-end durable-3-node-rolling-restart harness is a deferred
  test (the durability mechanism is proven, the multi-process orchestration is heavier). No log-size/age
  compaction *policy* beyond openraft's own snapshot+purge cadence. TLS/auth on the control transport remains
  deferred (plaintext localhost), as does an allocator acting on the shard→node map (ADR-042).
- **Consequence:** A `controlserver --data-dir` is now a real durable cluster-manager: it survives a crash,
  resumes its committed cluster-state document, and rejoins the quorum. The default in-memory path (oracle +
  any embedding that passes no dir) is byte-identical to ADR-038, so every prior control-plane oracle is
  unchanged and green. Proven by `tests/cluster_control_raft_oracle.rs::durable_node_recovers_committed_document_after_restart`
  (commit a document → `shutdown` → rebuild from the same dir → the committed membership/assignments/model
  survive AND a fresh write still commits) + `control_store.rs` unit tests (log round-trip, torn-tail drop,
  prefix/suffix rewrite, value round-trip). Full `check.sh` green. All `distributed`-gated — the lean core
  never compiles openraft or this store. No new dependency (reuses `serde_json` + `storage::crc32`).
- **See also:** ADR-038 (the openraft backend whose in-memory store this makes durable), ADR-037 (the seam,
  unchanged), ADR-031/039 (the CRC-framed-log + torn-tail pattern this mirrors), ADR-013 (the engine WAL whose
  framing lineage this shares), ADR-033 (shared-nothing — local durable state, no object store),
  `src/cluster/{control_store,control_raft}.rs`, `src/bin/controlserver.rs` (`--data-dir`).

### ADR-042: Shard→node allocator (rendezvous hashing) — committing the placement map (clustering step 5f)

- **Status:** Accepted.
- **Context:** ADR-037/038 gave the control plane a **shard→node map** (`ClusterState.assignments`) but
  nothing computed it — the default was a single logical node owning every position, and §4.3's own note
  flagged "the allocator that *acts* on the shard→node map ... is the next increment." Without it, a node
  joining or leaving the cluster never changes placement: no balance, no rebalance. This is the decision
  layer that fills the map.
- **Decision (`src/cluster/allocator.rs` + `ClusterEngine::{register_node, deregister_node, rebalance}`):**
  - **Rendezvous (HRW) hashing, not `position % N`.** For each shard position, rank the member nodes by a
    stable `hash(position, node)` (the project's `util::fnv1a64`, identical across runs + nodes) and take the
    top RF — highest weight = primary, the rest = replicas. HRW is balanced + deterministic like a modulus,
    but **minimal-movement**: adding a node only wins the ≈1/N positions where it now out-weighs the prior
    top; removing a node hands off only *its* positions to each one's next-best node — the
    Elasticsearch/Cassandra rebalance property (§8), and the same hashing family as the entity-anchor
    `HashRing` (keyed on `(position, node)` instead of a feature id). `rf` is clamped to `[1, node_count]`
    (a position can't have more distinct copies than nodes); replicas are distinct from the primary by
    construction. Pure computation over `NodeId`/`ShardAssignment` — **lean core**, dependency-free.
  - **The coordinator drives it.** `register_node`/`deregister_node` propose `AddNode`/`RemoveNode` through
    the control plane (the membership half of the inputs); `rebalance(rf)` reads the committed membership,
    plans the desired map, and commits **only the changed positions** (`allocator::changed_assignments`)
    via `AssignShard` proposals — minimal proposals, returning the count moved. It is **idempotent** (no
    membership change ⇒ 0 reassignments) and **fail-closed** (a rejected proposal leaves the prior map
    intact). On the single-node default it is a no-op (the one node already owns everything), so existing
    behavior is unchanged.
- **Scope — decision, not (yet) data movement.** `rebalance` commits the *desired* map; physically
  relocating a shard's segments to a new owner on a reassignment reuses the existing **peer-recovery** path
  (`peer_recover_replica`, ADR-036/039) and is the deployment wiring on top — an in-process cluster holds
  every shard locally, so the map is **advisory** there and matching is unaffected (the local shards do not
  move). Deferred: serve-then-drop handoff + epoch fencing during a live move (§9), an autoscaler that
  *calls* `rebalance`/`register_node` on membership events (step 6), and `recommended_shard_count` /
  auto-split (step 6). The allocator is the building block those will use.
- **Consequence:** A cluster can now compute and commit a balanced shard→node map and rebalance it as nodes
  join/leave, with bounded churn — the missing decision layer for multi-node placement, and the foundation
  for autoscale/auto-split. The single-node default is a no-op, so every prior oracle is unchanged. Proven by
  `allocator.rs` unit tests (distinct primary+replicas, RF clamping, determinism, ≈1/N movement on a node
  add, balanced primaries, the changed-only diff) + `tests/cluster_allocator_oracle.rs` over a real
  `ClusterEngine` (register → rebalance ⇒ a balanced fully-assigned map; idempotent; a deregistered node
  drops out of every position; and — load-bearing — `percolate` is **byte-identical** before and after every
  rebalance, so the allocator cannot introduce a false negative). Full `check.sh` green; no new dependency.
- **See also:** ADR-037 (the `ClusterState` map + `AssignShard` this computes), ADR-038/041 (the durable
  control plane that commits + persists it), ADR-027 (the entity-anchor `HashRing` — the sibling consistent
  hash), ADR-036/039 (the peer recovery a data-moving rebalance will drive), ADR-033 (shared-nothing —
  rebalance = peer recovery, no shared storage), `src/cluster/{allocator,coordinator}.rs`,
  `tests/cluster_allocator_oracle.rs`.

### ADR-043: Swappable shard backing — the live-handoff routing-flip mechanism (clustering step 6a)

- **Status:** Accepted.
- **Context:** ADR-042's allocator commits the *desired* shard→node map but explicitly does **not** move
  data; §9 calls for **serve-then-drop + epoch fencing** on a live move, and §4.3 names the data-moving
  handoff as the next step. The coordinator routes by ring **position index** into `shards: Vec<Box<dyn
  Shard>>` and never reads the shard→node map on the hot path (`route`/`percolate_inner`), so in-process a
  reassignment is a no-op for matching — the handoff is meaningful only over gRPC, where a position's
  `RemoteShard` must be **re-pointed at a new owner** at runtime. That re-point needs a position's backing to
  be atomically swappable. This increment ships exactly that mechanism (the routing flip + the fence stamp);
  the cross-node move that *drives* a swap is ADR-044 (step 6b).
- **Decision (`src/cluster/handoff.rs`, `distributed`-gated):**
  - **A `HandoffShard` wrapper, mirroring `ReplicatedShard`.** A `Shard` whose backing is one boxed shard in
    an `ArcSwap<Box<dyn Shard>>` plus an `AtomicU64` generation. `swap_backing(new, gen)` re-points the slot
    atomically — **backing stored first, generation published with `Release` after**, so a reader/fencer that
    `Acquire`-observes the new generation also observes the new backing (no "demoted but still serving"
    window). **Serve-then-drop falls out of `arc_swap` for free:** an in-flight probe holds its loaded `Guard`
    (the old backing) and completes correctly; the old backing drops only when the last `Guard` releases — no
    read-path lock, safe under the coordinator's rayon probe fan-out. The same `ArcSwap`-for-lock-free-reads
    pattern `LocalShard`/`ShardServer` already use (ADR-016).
  - **`impl Shard for Arc<HandoffShard>` (not the bare type)** so the SAME `Arc` clones into both `shards[i]`
    (boxed) and the coordinator's typed `handoffs: Vec<Arc<HandoffShard>>` side-table; the `wrap_handoff`
    helper builds both views from one allocation, so they share one object **by construction** (a swap through
    the handle is instantly visible to reads through `shards[i]`). Step 6b reaches the typed handle to flip a
    position with **no downcast** and **no `Shard`-trait change** (every method forwards to the live backing,
    including the *defaulted* ones — omitting one would silently inherit the wrong default for a wrapped
    `ReplicatedShard`; a unit test guards this).
  - **Representation:** `ArcSwap<Box<dyn Shard>>`, not `Arc<dyn Shard>` — `arc_swap`'s `RefCnt` is implemented
    only for `Arc<T: Sized>` and `dyn Shard` is unsized, but a `Box<dyn Shard>` is a Sized fat pointer, so
    `Arc<Box<dyn Shard>>` qualifies; auto-deref still reaches `dyn Shard` for the forwards, so the extra hop is
    invisible.
  - **Gated + opt-in.** The whole module and the coordinator's `handoffs` field are behind `distributed`, so
    the lean core and the in-process/RF=1 **default path never compile it and stay byte-identical** (and there
    is no lean dead-code lane to satisfy). The gRPC builders (`connect_remote`/`connect_replicated`) wrap each
    position via `wrap_handoff`; `ClusterEngine::handoff_generations()` exposes the per-position fence stamps
    (read-only introspection).
  - **The generation** is the committed control-plane epoch (`ClusterState::epoch`) the backing was installed
    under. **Inert in 6a** (nothing compares it); it is the fence token ADR-044 reads to tell a demoted owner
    "you are fenced at generation N" before dropping it.
- **Scope — mechanism, not (yet) the move.** 6a ships the swappable backing + the fence stamp + serve-then-drop
  and proves them with unit tests. The cross-node move (`execute_handoff` = `peer_recover_replica` → a final
  catch-up under a brief write quiesce so the new owner ≡ the source at the flip instant → `swap_backing` →
  fence the old owner via a new `Fence` RPC → drop it) is **ADR-044 (step 6b)**, proven over the gRPC oracle.
  **Honest scope:** epoch fencing is load-bearing for the *multi-coordinator* future; with today's single
  coordinator the flip is serialized, so correctness rests on serve-then-drop + (6b's) quiesce, and the fence
  is defense-in-depth. The in-flight-probe-vs-fence race (a probe that loaded the old owner can hit its fence
  and surface `ShardError::Remote`) is ADR-044's to handle (swap → drain → fence → drop).
- **Consequence:** a position's backing can be re-pointed at runtime without a read-path lock and without
  touching the default path — the missing routing-flip half of the live handoff (the byte mover, peer
  recovery, already exists). Proven by six `handoff.rs` unit tests: a swap to a set-equal backing is
  byte-identical (ids + stats); an in-flight read serves the old backing while a fresh read sees the new one;
  the generation tracks swaps and is co-visible with the backing; concurrent readers survive repeated swaps;
  and both writes and the defaulted `set_event_sink` forward to the backing. Full `check.sh` green; no new
  dependency (reuses `arc-swap`, already lean-core).
- **See also:** ADR-042 (the shard→node map a reassignment acts on), **ADR-044** (the cross-node move that
  drives `swap_backing` — the next increment), ADR-035/036/039 (peer recovery — the byte mover a handoff
  re-points to), ADR-016 (the `ArcSwap` lock-free-snapshot pattern reused), ADR-027 (the position routing this
  flips), `src/cluster/{handoff,coordinator}.rs`, clustering-and-scaling.md §9/§4.3/§10.

### ADR-044: Live data-moving handoff — the cross-node move that drives the swap (clustering step 6b)

- **Status:** Accepted.
- **Context:** ADR-043 made a shard position's backing **runtime-swappable** (the routing flip + a generation
  fence stamp), but nothing *drove* a swap. The allocator (ADR-042) **decides** the shard→node map; peer
  recovery (ADR-036/039) **moves** the bytes; ADR-043 **flips** routing — this increment wires
  decide→move→flip into one **live** move: a position's owner changes while the cluster keeps serving reads
  and (almost all) writes. This is §9's serve-then-drop + epoch-fencing step.
- **Decision (`ClusterEngine::execute_handoff` + a new `Fence` RPC):**
  - **A write fence on the old owner.** A new `Fence(generation)` RPC sets a monotonic
    `fenced_at_generation` on the `ShardServer`; once fenced, the data-mutating writes
    (`insert`/`delete`/`ingest`) return `failed_precondition`, while **reads + the recovery RPCs**
    (`FetchSegments`/`FetchTranslog`/`RetentionLease`) stay served. **Write-only by design**, so an in-flight
    READ never hits the fence — which dissolves the ADR-043 in-flight-probe caveat (a demoted owner keeps
    serving reads until the coordinator stops routing to it = serve-then-drop). Dict-fingerprint-guarded and
    monotonic (a stale, lower-generation fence never un-fences). `RemoteShard::fence` is inherent (not a
    `Shard` method) — only the handoff orchestrator fences a specific old owner, by endpoint.
  - **`execute_handoff(position, source_ep, target_ep)` under one retention lease** held on the source for
    the whole move (ADR-040 — so the segment-copy seal, or any concurrent seal, can't strand the tail): (1)
    **no-quiesce bulk recover** the target from the source (segments at `P` + drain the translog tail — the
    source keeps serving + accepting writes); (2) **fence** the source (the position's brief write-quiesce
    begins); (3) **drain to CONVERGENCE** — loop `catch_up` until the source's high-water stops advancing;
    (4) **flip** the `HandoffShard` backing source→target (serve-then-drop) and release the lease.
  - **Why fence-late, not fence-first.** Fencing before the copy would write-quiesce the source for the
    *whole* segment copy — exactly the ADR-036 whole-copy quiesce that 5c/5d eliminated. Fencing *after* the
    no-quiesce bulk copy keeps that property; only the brief converge-then-flip is write-quiesced.
  - **Why convergence, not a single final catch-up.** A write that passed the source's fence check *just
    before* the fence took effect can still append *after* a single catch-up reads the tail (a TOCTOU). But a
    fenced source accepts no new writes, so its tail is finite and frozen: looping `catch_up` until the
    high-water stops advancing captures every op it ever accepted. Convergence — not a fixed pass count — is
    what makes the flip lossless; a generous cap guards only a misbehaving (still-accepting) source.
- **Honest scope.** Single-coordinator: the flip is serialized, so the fence is the *cross-node / future
  multi-coordinator* guard (defense-in-depth today). A write rejected in the fence→flip window is **fail-closed**
  (rejected + retryable — the caller retries onto the new owner; it never silently vanishes). On non-convergence
  (a misbehaving source) `execute_handoff` **aborts the flip fail-closed** and leaves the source fenced — a
  *stuck position* needing operator attention, never a lost write (auto-unfence-on-abort is a refinement).
  "Drop the old owner" = drop it from **routing**, not teardown — its server keeps running (tearing it down is a
  separate ops step). RF > 1 *group* relocation (moving a whole primary+replica set) reuses the same swap (the
  backing can be a `ReplicatedShard`), but the oracle covers the single-owner move; the **autoscaler** that
  *triggers* a handoff on a membership/rebalance event is step 6c (design-only).
- **Consequence:** a shard can be moved between owners **live**, under concurrent writes, with **zero false
  negatives** and **uninterrupted reads** — the missing decide→move→flip wiring (the allocator decides, peer
  recovery moves, the 6a `HandoffShard` flips, the fence guards). Proven by
  `tests/cluster_grpc_oracle.rs::grpc_live_handoff_under_sustained_writes` (reassign a position source→target
  under a concurrent writer that retries the brief fence-window rejections; the SAME cluster — its position
  re-pointed to the new owner — ≡ the brute oracle over the final live set; the handoff generation bumps; every
  add converges onto the new owner) + `src/cluster/server.rs::fence_rejects_writes_but_serves_reads` (writes
  rejected, reads served, monotonic, fingerprint-guarded). Full `check.sh` green; no new dependency (the `Fence`
  RPC reuses tonic; no `proto.rs` mapper needed for its scalar messages).
- **See also:** ADR-043 (the swappable `HandoffShard` backing this drives), ADR-042 (the shard→node map a
  reassignment acts on), ADR-036/039/040 (peer recovery + the per-shard translog + retention leases — the byte
  mover, the no-quiesce tail, and the lease this holds), ADR-033 (shared-nothing — the move is peer recovery,
  no shared storage), `src/cluster/{coordinator,server,remote,handoff}.rs`, `grpc/proto/shard.proto`,
  `tests/cluster_grpc_oracle.rs`.

### ADR-045: Autoscaler — the policy/trigger layer over rebalance + advisories (clustering step 6c)

- **Status:** Accepted.
- **Context:** The scaling *mechanisms* are built — `register_node`/`deregister_node`/`rebalance` (the HRW
  allocator, ADR-042) and the live data-moving handoff (`execute_handoff`, ADR-043/044) — but nothing
  *decided when* to drive them: they fired only from tests. §8's "auto-rebalance"/"auto-split" goals and the
  §6c build step flagged the autoscaler as the missing policy. This increment adds it.
- **Decision (a pure policy + a thin driver):**
  - **`cluster::autoscale::evaluate(snapshot, config) -> AutoscaleDecision`** — a *pure, deterministic*
    policy over a `LoadSnapshot` (membership + the shard→node map + per-shard corpus). Three rules: (1)
    **membership drift → `Rebalance` (executable)** when the registered node set differs from the node set the
    assignments reference (a join leaves a node unplaced; a leave leaves a stale id still owning a position —
    the dangerous case, routing to a dead owner); (2) **per-node skew → `Handoff` (advisory)** when a node's
    primary-corpus exceeds `max_node_load_skew ×` the mean; (3) **per-shard corpus over a threshold →
    `RecommendSplit` (advisory)**.
  - **The driver on `ClusterEngine`** (`coordinator::autoscale`): `tick(config)` collects the snapshot
    (`control_state` + `shard_query_counts` — the only load signal that crosses the `Shard` seam, so the
    policy behaves identically in-process and across nodes), runs `evaluate`, **executes the executable
    subset** (each `Rebalance` → the idempotent `rebalance(rf)`), and returns the full decision incl.
    advisories; `on_node_joined`/`on_node_left` are the event-driven `register`+`tick` / `deregister`+`tick`
    convenience entries.
  - **Coarse trigger, idempotent truth.** The membership rule never recomputes HRW (that keeps `evaluate` a
    pure function of the snapshot, with no allocator coupling); the idempotent `rebalance` computes the exact
    minimal diff. **No clock / hysteresis:** `rebalance` is idempotent and `evaluate` is pure, so back-to-back
    ticks on unchanged membership cannot thrash — the idempotence *is* the hysteresis.
  - **Opt-in / disabled default.** `AutoscaleConfig::default()` is disabled ⇒ `tick` is a no-op ⇒ every
    pre-existing oracle stays byte-identical. Lean core, no new dependency, no `distributed`-gated code.
- **Honest scope / deferred.** **Auto-split is advisory only** — there is no split mechanism (the ring's
  `num_shards` is fixed at construction; splitting needs ring re-keying + a `recommended_shard_count` signal
  from compaction telemetry — a separate future increment). **Load-driven handoff is advisory only** — the
  policy emits a `Handoff`, but `execute_handoff` (gRPC-gated, ADR-044) is not driven this increment. QPS /
  compute-replica autoscaling (HPA-style, §8) is out of the engine's scope (a deployment-orchestrator concern).
- **Consequence:** membership/skew-driven rebalance is now automatic behind one opt-in policy, with split /
  handoff surfaced as advisories for an operator or a later increment. Proven by `src/cluster/autoscale.rs`
  unit tests (the deterministic policy decisions) + `tests/cluster_autoscale_oracle.rs` (over a real
  in-process cluster: `tick` commits the same map a manual `rebalance` does; **`percolate` is byte-identical
  before/after a tick** — the zero-false-negative property; a second tick commits nothing; a disabled config
  is a true no-op; a corpus-over-threshold advisory mutates nothing). Full `check.sh` green.
- **See also:** ADR-042 (the allocator `rebalance` it drives), ADR-043/044 (the handoff it will later
  *trigger* once load-driven moves are wired), ADR-027 (the content routing its rebalances must preserve),
  `src/cluster/autoscale.rs` + `src/cluster/coordinator/autoscale.rs`, `tests/cluster_autoscale_oracle.rs`.

### ADR-046: Dynamic vocabulary (Cluster v1) — feature-hashing for new tokens + runtime normalizer learning for aliases

- **Status:** Accepted + **implemented** (approach chosen by the dynamic-vocabulary research spike).
  **Both mechanisms are built and oracle-proven.** (1) feature-hashing for new tokens
  (`dict::synthetic_id`/`get_or_synthetic` + both readonly paths hash; additive — prior oracles
  byte-identical). (2) runtime normalizer learning for aliases: a synchronous **recompile pass**
  (`Engine::recompile_stale_segments` — recompile every live query under the new normalizer, clearing
  the vocab-epoch staleness) for the single engine + a cluster **blue/green rebuild**
  (`ClusterEngine::set_vocab` — re-mint the dict, re-place every query, atomic swap; durable via a
  manifest `vocab_data` blob, manifest **v3**) + **auto-learning** (`learn_and_apply` wires the ADR-015
  any-of learner as a runtime vocab source). Proven by `tests/cluster_oracle.rs`
  (absorb-without-broadening, satisfiable all-unknown any-of, **declared alias makes both surface forms
  match**, auto-learn) + `tests/cluster_durability_oracle.rs` (alias survives reopen + rebind) +
  `tests/hardening_fixes.rs` (single-engine recompile + learn). **In-process only:** `set_vocab` refuses a
  non-local cluster (an alias is normalizer-only and is not shipped to a `RemoteShard` — the cross-process
  shipping below is beyond v1). Prior-art survey: [`research/dynamic-vocabulary.md`](research/dynamic-vocabulary.md).
- **Context:** The cluster freezes one shared dictionary so every shard agrees on each term's integer
  `FeatureId` (ADR-027 — globally-consistent ids are what make the cross-shard signature cover lossless).
  The cost: a live write whose query introduces a term absent from the frozen dict was **silently dropped**
  in the read-only compile (`cluster/coordinator/ingest.rs:140`, `cluster/server.rs:211` →
  `compile::extract_readonly`) — the query broadened, and an all-unknown any-of group risked a **false
  negative**. A production percolator over eBay-style listings must instead **absorb** new vocabulary. The
  hard part: our **content-routed** sharding (a title routes by its anchor `FeatureId`) needs *cross-shard
  agreement* on a new term's id — unlike a scatter-gather engine, which never needs two nodes to agree.
- **Prior art (two camps, neither a direct fit — survey in the research doc):** *growable local
  dictionaries* (Vespa attributes — dynamic + real-time, but **node-local** ids, works only because Vespa
  scatter-gathers; Lucene/ES per-segment term dicts) and *rebuild-based globalization* (ES/OS **global
  ordinals** — exact, but **per-shard** and a **rebuild** on refresh). Cross-shard-consistent ids *without*
  a rebuild or coordination point to **feature hashing** (Weinberger et al. 2009 — the hashing trick).
- **Decision — two complementary mechanisms:**
  1. **New tokens → deterministic feature-hashing.** A term absent from the frozen dict gets
     `FeatureId = RESERVED_BASE | fold_u32(fnv1a64(name))`, in a reserved high-`u32` range disjoint from the
     dense interned ids (`dict.rs:13` — ids are `u32`, interned densely, so a high range is free). Every
     shard + the coordinator compute the **same** id independently (no coordination; in-process ≡
     cross-process — `fnv1a64` is already our stable cross-process hash, `util.rs:13`). Synthetic ids
     **bypass the immutable `Arc<Dict>`** (never interned, never serialized, don't change the fingerprint —
     `storage.rs:1370`/`dict.rs:176`), so the ADR-034 handshake is untouched. The exact matcher compares
     ids by `binary_search` (`exact.rs:54`), so synthetic ids work unchanged — and a collision is a
     **bounded false *positive* that survives verification, never a false negative** (a term always hashes
     the same, so query-requires-`t` and title-contains-`t` always agree). This *fixes* both original bugs
     (broadening + any-of collapse).
  2. **New alias / synonym rules → runtime normalizer learning.** Aliases (`Upper Deck` ≡ `UD`) are a
     *normalizer* operation — only the normalizer sees raw text and can canonicalize two surface forms to
     one feature name *before* id assignment, so hashing cannot express them. Reuse the ADR-015 `Vocab`
     machinery (it already learns synonyms from query any-of groups) to rebuild the `Normalizer` and swap
     its `Arc`. **In-process (the Cluster v1 core) there is one shared `Arc<Normalizer>`, so the swap is
     atomic — no propagation window.** A change bumps the vocab epoch; queries compiled under the old epoch
     are recompiled (the existing vocab-epoch staleness machinery) so the "same normalizer for queries and
     titles" invariant holds and zero-FN is preserved. **As built:** the recompile must be **synchronous**
     (a stale segment carries old-normalizer ids, so a lazy window would drop matches), and at the cluster
     level it is a full **blue/green rebuild from the live corpus** — re-mint the dict + **re-place** every
     query, because an alias can change a query's anchor feature (hence its shard), so an in-shard recompile
     would strand it on the wrong shard. Durability lives in the manifest (a serialized `Vocab`), not the
     log — `set_vocab` rebuilds then checkpoints, so a runtime alias survives reopen; no `SetVocab` log op
     (which would mis-replay alias rebind/removal, since `Vocab::add_synonym` is first-write-wins).
- **Correctness (load-bearing):** (a) **both sides hash** — the title path (`normalize::match_features`)
  *and* the query path (`compile_features_readonly`) must hash unknown tokens; dropping a title token would
  re-introduce an FN. (b) **collisions are bounded, tunable, never FN** — a ~31-bit range gives birthday
  collisions around tens of thousands of *distinct* unknown terms, and a raw id-collision becomes a
  *visible* wrong match only when a query is anchored on the collided term *and* a title carries the other
  colliding term, so the effective rate is far lower; tunable by range size, with an optional **v2**
  hot-term promotion into exact interned ids at compaction. (c) **guard by-id dict lookups**
  (`mask_bit`/`kind`/`name`) — a synthetic id is out of the interned Vecs' range → treat as
  non-hot/non-mask/unknown-name (synthetic ids are rare by construction → never in the 64-hot common mask,
  always the exact verifier's non-mask required tail).
- **Scope for v1 (tokens + aliases, in-process):** hashed tokens need **no shipping** cross-process (every
  node computes them), so the token half works in-process *and* cross-process for free. The alias half's
  **cross-process normalizer *shipping*** (a versioned normalizer + the propagation-window consistency
  design, analogous to dict shipping ADR-034) rides with the experimental distributed layers and is
  **beyond v1**.
- **Alternatives declined:** *coordinator-assigned exact ids* (ES global-ordinals style via the control
  plane) — exact, but adds a coordination step + a propagation window (transient FN) + Raft coupling, a
  hazard hashing avoids; *post-freeze dict mutation* — the dict is an immutable shared `Arc`, and breaking
  it would fork the feature space across shards.
- **Consequence:** new vocabulary is absorbed with matching correct (**zero false negatives**), no
  coordination for tokens, and reuse of existing `Vocab`/epoch machinery for aliases. The only cost is a
  bounded, tunable false-positive rate from token-hash collisions (plus, for a durable cluster, a benign
  per-shard `sources.dat` accumulation across repeated `set_vocab` calls — matching uses segments and
  `live_sources` de-dups, so correctness is unaffected; a future sources rewrite reclaims it). Both
  mechanisms + the absorb-correctly oracle assertions are **built** — the Cluster-v1 Tier-0 deliverable
  (STATUS.md).
- **See also:** ADR-027 (the shared frozen dict + content routing this preserves), ADR-015 (the `Vocab`
  synonym-learning the alias half reuses), ADR-034 (dict shipping — the template for the deferred
  cross-process normalizer shipping), [`research/dynamic-vocabulary.md`](research/dynamic-vocabulary.md).
  Code sites: `src/dict.rs`, `src/normalize.rs` (`match_features`, `compile_features_readonly`),
  `src/compile.rs` (`extract_readonly`), `src/vocab.rs`, `src/cluster/coordinator/ingest.rs`,
  `src/cluster/server.rs`.

### ADR-047: Remote live-write partial-apply — observe, fail-closed, repair (`resync`) + the `block_on` thread-context contract

- **Status:** Accepted + **implemented** (distributed-layer hardening from an external review). The
  in-process / RF=1 default path is **byte-identical** (its `LocalShard` writes are infallible, so no
  partial apply is ever recorded) — `tests/cluster_oracle.rs` + `tests/cluster_durability_oracle.rs` stay
  green unchanged. Proven by `partial_apply_is_detected_then_resync_converges` +
  `resync_requeues_when_shard_still_failing` (`cluster/coordinator/tests.rs`, deterministic, lean core) and
  `grpc_partial_apply_is_detected_and_queued` + `remote_single_target_percolate_safe_from_tokio_worker`
  (`tests/cluster_grpc_oracle.rs`, real wire).
- **Context:** A selective (class-A / class-B-any-of) query is placed on **2+ shards**; the coordinator's
  `apply_add` fanned the inserts out in a loop with `?`, and `apply_remove` summed a `Result` iterator. With
  **remote** shards (the experimental `distributed` layer), shard A's insert can succeed and shard B's RPC
  then fail — leaving the method returning `Err` with shard A already mutated, **no signal and no repair**:
  a silent partial mutation. Because writes are **log-first** (ADR-031), the mutation is durably committed,
  so `ClusterEngine::open`'s replay re-drives every target shard and the divergence **self-heals on reopen**
  — but a *live* cluster stays divergent until then (a transient **false-negative window** on the
  un-applied shard). Separately, the `RemoteShard` sync→async bridge used `Handle::block_on` directly; on
  the single-target read path (`targets.len() <= 1`, the sequential branch) that runs on the *caller's*
  thread, so a future async coordinator probing `percolate` from a tokio worker would hit the
  nested-runtime **panic** the rayon fan-out path happens to avoid. (Surfaced by an external review; the
  in-process core, durable reopen, fan-out bench, and status honesty were verified accurate and need no
  change.)
- **Decision:**
  1. **Detect, don't bail.** `apply_add`'s `Selective` branch and `apply_remove` now **try every target
     shard and collect per-shard failures** instead of bailing on the first error. An empty failure set is
     byte-identical to the old loop (the default path).
  2. **Observe + fail-closed.** A non-empty failure set queues the failed shards for repair (keyed by
     logical id, so a later mutation supersedes an earlier pending one), emits a
     `DurabilityFailure { op: ClusterPartialApply }` event (`is_data_at_risk = true` — a missed match is
     this system's worst outcome), and returns the honest `ShardError::PartiallyApplied { logical, applied,
     failed, detail }`. The error is distinct from a clean `Remote`/`Log` failure so a higher layer can act,
     and documents that the mutation is **committed** (re-`add_query` would double-log; recover via repair).
  3. **Repair (`resync`).** `ClusterEngine::resync()` drains the queue and re-drives each mutation against
     **only its still-failed shards** via the existing `apply_mutation` seam — converging without a full
     reopen. Re-driving touches only failed shards: an Add there is a clean first insert (they never
     received it), a Remove is idempotent — so converged shards are untouched. Idempotent + re-queues a
     still-unreachable shard. The autoscaler `tick` calls it opportunistically (a cheap no-op when empty).
  4. **`block_on` thread-context contract.** All `RemoteShard` RPCs route through `block_on_in_context`,
     which dispatches on the caller's context: off any runtime → plain `block_on` (the rayon-fan-out / build
     path, unchanged); on a **multi-thread** runtime worker → `task::block_in_place(|| block_on)` (the
     documented re-entry; `Runtime::new()` / tonic / axum are all multi-thread); on a current-thread runtime
     → offload to a scoped non-runtime thread.
- **Correctness (load-bearing):** the **durable cluster log stays authoritative** — a reopen replays it in
  order, so `resync` is a *liveness* optimization, not the correctness backstop. `resync` can only **add**
  matches on a lagging shard (closing the FN window), never remove a true match, so it cannot introduce a
  false negative. The in-process / RF=1 path never records a partial apply (infallible writes) ⇒ zero
  behavior change ⇒ the v1 zero-false-negative contract is untouched.
- **Scope / remaining gap (this is the experimental distributed layer, not Cluster v1):** a **single-shard**
  failure (the replicated lane, or a 1-shard selective whose write totally fails) is a clean `Err` that
  converges on **reopen**, not live `resync`. There is still **no cross-write fencing / quorum** — two
  concurrent writers to overlapping shards, or a `resync` racing a same-id write, resolve last-writer-wins
  in memory and authoritatively by the log on reopen; production multi-machine use needs a real fence +
  durable-multi-node rolling-restart harness. The current-thread-runtime `block_on` offload is a fallback,
  not the shipped servers' path (they are multi-thread).
- **Alternatives declined:** *compensating rollback* (delete from already-applied shards on partial
  failure) — fights the log-first model (the mutation is committed; rollback then a reopen-replay would
  resurrect it, an inconsistency between the returned `Err` and the post-reopen state); *two-phase commit /
  quorum write* — the right end state for production, but heavyweight for an experimental layer and a larger
  design (control-plane coupling); *silent self-heal on reopen only* (the pre-ADR behavior) — leaves a live
  FN window with no signal and no live remedy.
- **Consequence:** a mid-fan-out remote write failure is now **visible** (typed error + event + a
  `pending_repairs()` gauge) and **repairable live** (`resync`, plus `tick` auto-heal), instead of a silent
  partial mutation healed only by a full restart; and the `block_on` bridge is safe from any caller thread.
  The honesty the review asked for, now backed by code. Cost: a per-write uncontended mutex touch on the
  (empty) repair queue — negligible, and off the match hot path.
- **See also:** ADR-031 (log-first cluster writes — why a partial apply is *committed*, not lost), ADR-027
  (placement: which queries are multi-shard selective), ADR-029 (the `RemoteShard` gRPC bridge this hardens),
  ADR-044 (the handoff fence this reuses as the test's deterministic write-failure injector), ADR-021 (the
  `EngineEvent`/`DurabilityOp` observability this extends). Code sites:
  `src/cluster/coordinator/ingest.rs` (`apply_add`/`apply_remove`/`note_partial`/`clear_pending`/`resync`/
  `pending_repairs`), `src/cluster/coordinator.rs` (`PendingRepair`/`ResyncReport`/the queue field),
  `src/cluster/coordinator/autoscale.rs` (`tick`), `src/cluster/shard.rs` (`ShardError::PartiallyApplied`),
  `src/events.rs` (`DurabilityOp::ClusterPartialApply`), `src/cluster/remote.rs` (`block_on_in_context`).
