# ADR-051: Fail-closed flush, compaction & reseal — never destroy durable state before the replacement is committed

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** ADR-017 made the *bulk-ingest* path durable-or-rejected by routing it through
  `commit_base_segment` (segment file = artifact, manifest = atomic commit point, all-or-nothing on
  failure). But three other paths that seal a segment were left on the old fail-*open*
  `make_base_segment`, which on a segment-write or mmap failure falls back to an **in-memory**
  segment and sets `persistence_healthy = false` (ADR-021 made that fallback *observable*, but
  deliberately did not change the behavior). An external review (2026-06) found that this fallback
  is a silent data-loss / on-disk-corruption path in those three callers:
  - **`flush`** sealed the memtable, then unconditionally wrote a WAL `FlushCheckpoint` and reset
    the WAL gated **only** on the manifest write succeeding — never on whether the *segment* had
    persisted. The manifest's `segment_files` list is built from `Mmap` segments only, so an
    in-memory fallback is silently excluded, yet the manifest write itself *succeeds*. Net: the
    checkpoint marker was written and the WAL truncated while the just-flushed queries lived only
    in RAM and were referenced by nothing on disk → **acknowledged writes vanished on restart.**
    (A second, narrower bug: the checkpoint was written *before* the manifest, so even a
    durably-written segment could be lost if the manifest write then failed — the marker made
    recovery skip entries the manifest never came to reference.)
  - **`compact_all` / `compact_range` / auto-`maybe_compact`** drained the source segments, built
    the merged segment through the fallback path, **deleted the old `.seg` files, *then* wrote the
    manifest.** On a merged-write failure the merge lived only in RAM with the sources already
    deleted; even on success, a crash between the delete and the manifest write left a manifest
    referencing deleted files. Either way reopen was broken / lossy.
  - **`reseal_tombstoned_segments`** (the cluster checkpoint's tombstone-baking step, ADR-032/039)
    had the same delete-then-manifest shape; a failed reseal that still trimmed the translog would
    **resurrect a deleted query** (false positive) on reopen.
  - **`recompile_stale_segments`** (the vocab-change rebuild, ADR-046) was the worst case: it
    `clear()`s *every* segment, seals the one recompiled segment via the fallback, then resets the
    WAL + deletes the old files — a failed write there could erase the entire corpus from disk.
- **Research:** Same canonical model as ADR-017 (RocksDB `IngestExternalFile`) plus the universal
  LSM/WAL discipline: **build the replacement durable *before* you destroy what it replaces, and
  treat the manifest write as the single linearization point.** RocksDB never deletes an obsolete
  SST until the new MANIFEST that omits it is fsync'd; a WAL is only released after a memtable
  flush is durably installed (PostgreSQL/ARIES "write durable, then advance the log"). Our segment
  files already fsync via tmp-write + `sync_all` + atomic `durable_rename`, and `Engine::open`
  ignores any `.seg` not listed in the manifest — so, as in ADR-017, the durability *mechanics*
  were already present; the bug was purely in error *handling* and *ordering*. The fix is to reuse
  ADR-017's `build_durable_base` (propagates I/O errors instead of falling back) in these paths and
  to reorder every destructive step after the commit point — **not** to add new durability
  machinery.
- **Decision:** Make all four paths fail closed, reusing the ADR-017 primitives:
  - `make_base_segment` / `seal_and_push` now **return whether the seal actually persisted** (`true`
    for a disk-backed `Mmap` or for in-memory mode where there is nothing to persist; `false` for a
    persistent-mode fallback). The data still falls back to an in-memory segment so reads keep
    working — there is never a live false negative — but callers can now gate their destructive
    follow-up on durability.
  - **`flush`** advances the WAL (checkpoint **then** reset) only when `persisted && manifest_ok`,
    and writes the checkpoint *after* the manifest. On failure the WAL is left intact, so a restart
    replays the un-sealed memtable. There is no data loss — only degraded durability
    (`persistence_healthy = false`).
  - The three compaction entry points are unified into one **`do_compact_range`** that builds the
    merged segment with `build_durable_base` *before* touching the vec, keeps the source `Arc`s in
    hand, makes the manifest write the commit point, and **only then** deletes the old files. A
    build failure aborts before any mutation; a manifest failure rolls the range back to its
    (still-durable) source segments and deletes the orphan merged file. It returns `None` on a
    rolled-back commit.
  - **`reseal_tombstoned_segments`** builds each resealed segment durably before retiring the
    original (a failed reseal keeps the original, un-baked segment and its file) and deletes retired
    files only after the manifest commit. The cluster checkpoint (`seal_for_checkpoint_at`) now
    **bails before trimming the translog** if `persistence_healthy` is false, so an un-baked
    tombstone is never stranded — its translog entry replays on recovery.
  - **`recompile_stale_segments`** gates the WAL reset + old-file deletion on the same
    `persisted && manifest_ok`.
  - A new `DurabilityOp::Compaction` (warn-level / *not* data-at-risk, because the operation rolled
    back to a durable state) distinguishes "couldn't optimize, data safe" from `SegmentWrite`
    ("fell back to RAM, data only in memory"). The server maps a degraded `/_flush` or `/_compact`
    to **HTTP 503** with `acknowledged: false`, mirroring the `/_bulk` contract, so a client never
    reads `acknowledged: true` for a write that isn't on disk.
- **Consequence:** A flush/compaction/reseal/recompile is now **durable-or-degraded, never lossy**:
  on a disk failure the engine keeps serving from RAM, the pre-operation durable state (WAL +
  source `.seg` files) is preserved intact, and a restart recovers everything. `persistence_healthy`
  is a sticky latch: once any durable write fails it stays false until the engine is reopened, so a
  subsequent `/_flush` or `/_compact` reports 503 even if that specific call did nothing wrong —
  the conservative, honest signal for "this engine's durability is compromised; restart it." A
  rolled-back compaction advances `next_seg_id` by one (a harmless gap) and, when a published
  snapshot still pins the source segments, materializes the merge inputs by cloning rather than
  unwrapping (the snapshot already forces that clone in the server, so it is not a new cost on the
  hot path). Verified by two regression tests (`failed_flush_retains_data_in_wal_and_recovers_on_reopen`,
  `failed_compaction_rolls_back_and_keeps_segments_on_disk`) that inject a read-only `segments/`
  dir and assert recovery + rollback; both were confirmed to fail on the pre-fix code. The cluster
  durability oracle (durable reopen + checkpoint + torn-tail) is unchanged and still green.
- **See also:** ADR-017 (durable bulk ingest — the `commit_base_segment` / `build_durable_base`
  primitives this reuses), ADR-021 (durability failures observable — the `DurabilityFailure` event
  this extends with `Compaction`), ADR-013 (WAL — the flush backstop), ADR-012 (segment format +
  manifest commit point), ADR-032/039 (cluster reseal + translog), ADR-046 (vocab recompile),
  [ingestion-and-updates.md](../design/ingestion-and-updates.md)
