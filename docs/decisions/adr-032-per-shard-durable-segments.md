# ADR-032: Per-shard durable compiled segments — attach-and-mmap on open, not re-ingest (clustering step 3b)

> [Back to the decisions index](../DECISIONS.md)


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

