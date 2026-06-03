# ADR-023: Per-segment introspection endpoint (`GET /_cat/segments`)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


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


