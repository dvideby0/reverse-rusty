# ADR-020: Production-scale resident-memory reduction (lazy source store + flat logical-index columns)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


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

