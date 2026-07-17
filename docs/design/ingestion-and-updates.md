# Ingestion & Update Lifecycle — immutable segments, LSM write path, compaction

*Scope: how stored queries get in, get updated, and get re-optimized — the write path and storage
model. Covers immutable segments + hot delta + tombstones, the LSM write path, deltas-with-merge,
bulk-ingest vs rebuild rules, compaction-that-improves, and feature-model versioning. Siblings:
[`matching.md`](matching.md) (what's stored), [`clustering-and-scaling.md`](clustering-and-scaling.md)
(the durable mutation log in a cluster), [`normalization.md`](normalization.md). See the
[overview](README.md) for the correctness contract. **Answer in one line:** a log-structured (LSM)
write path with immutable segments and read-optimized compaction; never rebuild by default;
rebuild-from-scratch is reserved for the initial seed and major feature-model changes (and even then
it's blue/green from the log, not stop-the-world).*

> **Implementation status:** Core LSM engine implemented and tested (segments, memtable, flush, bulk_ingest, tombstones, **compaction**, **mmap'd segments**, **WAL**). Compaction uses a ClickHouse-inspired score-based merge selector (§5–6 below) that directly optimizes for minimum time-integrated segment count — the right objective for a percolator where reads probe every segment. Supports `compact(max_segments)`, `compact_all()`, and `compact_range(lo, hi)`. Verified by oracle tests. Mmap'd segment file format with frozen hash tables (ADR-012) and write-ahead log for crash recovery (ADR-013) are implemented. Re-anchoring drifted queries during compaction ("compaction-that-improves") is built (opt-in via `compaction_reanchor`, ADR-056, oracle-proven); feature-model versioning is design-only.

**TL;DR (for agents)**
- **Owns:** LSM engine (`segment.rs`), the write path and storage model
- **Key invariant:** Segments are immutable once sealed; writes go to the memtable only; never rebuild existing segments by default
- **Write path:** `insert_live` → memtable → `flush()` seals to base segment; `bulk_ingest()` compiles a batch directly into a new segment
- **Update model:** tombstones + re-insert (new PhysicalVersionId); epoch-based atomic visibility
- **Measured:** ~750k updates/sec/core, ~650k bulk compiles/sec/core (full numbers: [performance/results.md](../performance/results.md))
- **Design-only:** feature-model versioning, stat-driven self-tuning (telemetry-driven cover refresh + `recommended_shard_count`/`recommended_arity`)
- **Recently implemented:** durable mutation log (WAL — ADR-013), mmap'd segments (ADR-012), per-segment anchor filters (cache-line blocked bloom — ADR-011), score-based compaction (ADR-009), compaction re-anchoring (ADR-056), per-query metadata storage (ADR-049)

Builds on [`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md) (feature
learner) and [`clustering-and-scaling.md`](clustering-and-scaling.md) (cluster mutation log). Grounded
in RocksDB/LSM, Lucene segment merging, and Aurora's log-is-the-database.

---

## 1. Immutable segments + hot delta + tombstones (the core model)

```
Index = [ Segment_0, Segment_1, ..., Segment_n ]   (immutable, mmap-able)
        + HotDelta                                  (small, in-memory, mutable; the "memtable")
        + Tombstones                                (set of dead PhysicalVersionId)
        + epoch (atomic snapshot pointer)
```

- **Add query:** compile → assign new `PhysicalVersionId` → insert into HotDelta (its own little
  candidate index + exact-match arrays) → publish new epoch (atomic pointer swap). Visible immediately;
  no segment rebuild.
- **Update query:** compile new version into HotDelta; **tombstone** the old `PhysicalVersionId`. The
  matcher skips tombstoned IDs at the resolve step.
- **Delete query:** tombstone.
- **Match** probes all segments + HotDelta under one epoch snapshot; readers never block writers (the
  epoch pointer is swapped atomically; old snapshots are reclaimed once no reader holds them).

---

## 2. The one fact that makes our write path different from a KV store

In RocksDB/Cassandra a **point read stops at the first SSTable that has the key**, and Bloom filters
let it skip the rest — so a few extra delta segments barely cost reads. **Percolation can't stop
early.** A title's matching queries could live in *any* segment, so the matcher must probe the title's
anchor postings in **every segment and union the results**. Therefore:

> **Read amplification ≈ number of segments per shard.**

This flips the usual LSM tuning. We are **read-amplification-sensitive on segment *count***, so the
write path must keep the number of live segments **small and bounded**, and should use the LSM analog
of Bloom filters — **per-segment anchor membership filters** — to skip segments that provably hold no
query for a given anchor (see §6). This is measured: see the segment-count read-amplification result in
[`../performance/results.md`](../performance/results.md). Everything below follows from this.

---

## 3. The write path (LSM, log-structured) — never mutate in place

```
add/update/remove ─► (1) append to durable MUTATION LOG  [Aurora "log is the database", see clustering doc]
                     (2) apply to in-memory MEMTABLE (the hot delta)  ── visible at next epoch
                                   │ flush on size/time
                                   ▼
                     (3) immutable L0 SEGMENT (compiled candidate index + exact SoA)
                                   │ background compaction (merge + IMPROVE)
                                   ▼
                     (4) larger base segments  ── bounded total segment count
```

- The **log is the source of truth and durability** (quorum-replicated in a cluster). Segments are
  *materialized views* of the log and can always be rebuilt from it.
- **Segments are immutable** (Lucene/LSM): the write path is append-only; complexity is pushed to
  the merge, which is the right place for it.
- **Updates/deletes are tombstones**, not in-place edits: update = compile new version into the
  memtable + tombstone the old physical id; delete = tombstone. The matcher skips tombstoned ids at
  the resolve step; space is reclaimed at merge (Lucene marks deleted docs and drops them when it
  rewrites a segment — same model).

This is exactly the §1 model, now named and tuned.

---

## 4. Five write scenarios → decision rules (this answers the questions directly)

| Scenario | Best-in-class approach | Cost | Touches existing data? |
|---|---|---|---|
| **Single add / update / remove** | append to log + memtable; tombstone old on update | O(1 query) — **~750k upserts/s/core measured**, visible at next epoch | no |
| **Bulk add (e.g. 1M new queries)** | compile the batch in parallel → build **new L0 segment(s) directly** → atomic publish | O(batch); ~**650k compiles/s/core** → ~1.5s/1M; the existing 100M is untouched | no |
| **Routine churn / accumulated tombstones** | background compaction triggered by size & `holes_ratio` | amortized; off the hot path | merges a few segments |
| **Anchor drift / poor covers** | repaired *during* compaction of the affected segments (re-anchor, repack) | amortized into a merge already happening | only the segments being merged |
| **Initial seed (100M from scratch)** | parallel **base-level build**, skip the memtable, publish once | one-time; embarrassingly parallel per shard | n/a (creating) |
| **Major feature-model change** (new tokenizer / feature-ID generation) | **blue/green re-materialize from the log**, alias/epoch swap | background, zero-downtime | builds a parallel index, then swaps |

**So: should we always build from scratch? No.** From-scratch is two narrow cases — the *initial seed*
and a *major feature-model version bump*. Everything else is incremental delta + merge. The measured
contrast makes it stark: bulk-adding 1M queries to a 100M index is **~1.5s** as an L0 segment vs
**~150s** to rebuild 100M — and the rebuild also churns memory and invalidates caches for no benefit.

**Bulk add = build a segment directly, don't funnel through the memtable.** This is the analog of
RocksDB *ingest-external-SST* / Lucene *addIndexes*: compile the batch in parallel, sort by anchor,
pack postings + exact SoA, append a "segment created" record to the log, and publish via epoch swap.
The batch never contends with the live memtable and never rewrites existing segments.

---

## 5. Deltas with eventual merge — tuned for our read profile

Yes, deltas-with-eventual-merge is exactly right; the tuning is the interesting part.

**Bound the segment count (because reads amplify over it).** Cap live segments per shard at a small
constant (e.g. ≤ ~8–10): memtable + a few L0 deltas + a couple of base segments. RocksDB's default is
a sensible template — **L0 uses tiering** (absorbs ingest/flush bursts cheaply), **deeper levels use
leveling** (few, large, low read/space amplification). We adopt that hybrid but with a *hard cap on
total segments* rather than optimizing purely for write amplification, because each extra segment is a
per-title probe.

**Compaction-strategy choice for us:**
- *Tiered* minimizes write amplification but **raises read amplification** (more sorted runs) — bad
  for us beyond L0.
- *Leveled* minimizes read/space amplification at higher write amplification — **good for us**, since
  reads touch every segment and update volume, while high, is small per item.
- **Verdict (original design hypothesis):** tiered L0 (burst absorption) + leveled below + a
  segment-count ceiling — the read-optimized corner of the LSM design space.
- **Update — what shipped (ADR-009):** the engine does **not** implement explicit tiered/leveled
  levels. It keeps a single pool of base segments and selects merges with a **ClickHouse-inspired
  score-based greedy selector** (`(sum_size + FIXED_COST·count) / (count − 1.9)`), which directly
  minimizes the time-integrated average segment count — the correct objective when reads probe every
  segment. The segment-count ceiling (`max_segments`) and `holes_ratio` triggers are retained. See
  [`../DECISIONS.md`](../DECISIONS.md) ADR-009 and §10 below.

---

## 6. Per-segment anchor filters = our "Bloom filters"

Attach to each segment a compact static membership filter (xor / binary-fuse — already scouted in
[`../research/prior-art.md`](../research/prior-art.md) §7) over the set of **anchor features present in
that segment**. Before probing a segment for a title's anchor `f`, test the filter; skip the segment on
a miss. This restores the "skip segments you don't need" property that KV stores get from Bloom filters,
cutting effective read amplification back toward 1–2 segments even when several exist. Immutable filters
fit the immutable-segment model perfectly.

**Update — what shipped (ADR-011):** the filter is **not** xor/binary-fuse. Research into RocksDB's
filter history showed a **cache-line blocked Bloom filter** (512-bit blocks, 6 probes via
Kirsch-Mitzenmacher double hashing, ~10 bits/key, ~1% FPR — in `src/filter.rs`) is the better fit for
our one-cache-line-access budget. It is built at every seal point and integrated into the match path
(each signature probe tests the filter first). See [`../DECISIONS.md`](../DECISIONS.md) ADR-011 and §10 below.

**Merge triggers (Lucene-style):** segment count over cap, segment size tiers, and **`holes_ratio`**
(tombstoned/total) over a threshold. TieredMergePolicy's idea of scoring a merge by size balance *and
deletion percentage* is the right scoring function; pick the lowest-cost merge that most reduces
segment count and reclaims the most tombstones.

---

## 7. Compaction that *improves*, not just merges

Compaction (background) does more than concatenate-minus-tombstones — it *improves* the index, all
amortized into a merge that's happening anyway. This is the Tantivy/Lucene lifecycle with a
domain-specific "improve, don't just merge" twist; the swap is atomic (a new immutable segment + a new
epoch):

1. **Drop tombstones**, reclaim space, renumber to dense `SegmentLocalQueryId`s for cache locality.
2. **Recompute statistics** (feature df, per-signature posting length, candidate-survival rate from
   runtime telemetry) for the merged range.
3. **Re-anchor drifted queries** (built, opt-in via `compaction_reanchor` — ADR-056) — a query whose
   anchor went hot (a player got popular) gets a fresh, more-selective signature cover. This is how
   frequency drift is repaired **lazily and locally**, never by a global rebuild.
4. **Rewrite poor covers / split hot signatures** ([`matching.md`](matching.md) §1, §4), **repack
   postings** into the optimal adaptive representation, re-rank feature IDs for locality, and **rebuild
   per-segment anchor filters**.
5. **Refresh the feature model** for the range (re-run the corpus learner,
   [`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md)) and emit
   `recommended_shard_count` / `recommended_arity` telemetry (stat-driven self-tuning).

---

## 8. The feature-model version problem (the genuinely hard, percolator-specific part)

A query's anchor choice and the 64-bit common mask depend on **global frequencies and the learned
feature set**, which drift as queries are added. Two regimes:

- **Minor drift (the common case):** new queries compile against the *current* model; their new
  features land in the memtable's overlay dictionary; frequencies update incrementally. Existing
  queries keep their compiled form until a compaction re-anchors them (§7.3). **The global 64-hot
  common-mask is frozen across minor versions** so the exact-match masks in all live segments remain
  comparable; a title computes one mask word and it's valid everywhere. No rebuild.
- **Major change (rare):** a new tokenizer generation or a re-ranked common-mask changes feature
  *identity/encoding* across the whole corpus. Handle as **blue/green re-materialization from the
  mutation log**: replay the log into a new index version built with the new model, in the background,
  then **atomic alias/epoch swap** (OpenSearch reindex-and-alias + Aurora log-replay). Zero downtime,
  and the old version keeps serving until the swap. This is the *only* true from-scratch rebuild, and
  it's driven off the log, not a re-ingest of source data.

Versioning rule: every segment records the **feature-model version** it was built against; the cluster
state ([`clustering-and-scaling.md`](clustering-and-scaling.md)) holds the active version; minor
versions interoperate (frozen mask), majors are isolated behind a blue/green swap.

---

## 9. Visibility & consistency

- **Near-real-time by default:** a write is durable once it's in the log; it becomes *matchable* at the
  next **epoch swap** (memtable applied) — sub-second, no segment-refresh stall (the cost we avoided vs
  Lucene/percolator refresh).
- **Optional read-your-writes:** `add_query` can block until a quorum of the owning shard's replicas
  has applied the log entry (Aurora quorum-commit). Off by default.
- **Atomic epoch swap** gives each title a consistent MVCC snapshot across memtable + all segments;
  readers never block writers.

---

## 10. How this maps onto / extends the current engine

- **Today (implemented):** a multi-segment LSM-shaped engine — `Vec<base Segment>` + a mutable
  `memtable` Segment; matching unions across all segments with per-segment epoch-dedup; `flush()`
  seals the memtable into a base segment; `bulk_ingest()` compiles a batch directly into a new base
  segment without rebuilding existing ones; tombstones handle update/delete. That already realizes the
  memtable + delta + tombstone core *and* read-amp = segment count (measured by `segbench`).
- **Implemented since initial design:**
  1. ~~Leveled/tiered compaction~~ → ClickHouse-inspired score-based compaction (ADR-009).
  2. ~~Per-segment anchor filters~~ → cache-line blocked bloom filter (ADR-011).
  3. ~~Durable mutation log + mmap'd segments~~ → WAL (ADR-013) + mmap'd segment file format with
     frozen hash tables (ADR-012). `Engine::open()` for manifest + WAL recovery.
- **Next steps (see [`../STATUS.md`](../STATUS.md)):**
  1. **Stat-driven self-tuning** — re-anchoring on compaction is built (ADR-056, §7.3); the
     telemetry-driven cover refresh + `recommended_shard_count`/`recommended_arity` remain design-only.
  2. **Feature-model versioning** + a blue/green re-materialize path from the log (design-only).

---

## 11. Per-query metadata storage

> **Status: built (single-node) + oracle-proven** (2026-06-03, ADR-049). The metadata *model*, filtered
> percolation, and ranking live in [`matching.md`](matching.md) §5; this section is the **storage /
> persistence** half — how per-query tags are written, sealed, and recovered. Tags are an SoA column in the
> `.seg` **v3** format and ride the **v2** WAL (so a tagged insert survives crash recovery); both read older
> files back as untagged. Decided in [`../DECISIONS.md`](../DECISIONS.md) ADR-049, tracked in
> [`../STATUS.md`](../STATUS.md) Tier 4.

The reference workload ([`../research/percolator-workload.md`](../research/percolator-workload.md))
attaches structured tags (a category, a status, secondary keys) to every stored query. Storing them
follows the existing query-storage model with no new moving parts:

- **What's stored.** Each query's tags are interned to `TagId`s (matching.md §5.1) and held as one more
  **SoA column** alongside the exact-match arrays — `tag_off/tag_len` into a sorted `tag_blob` — indexed
  by `SegmentLocalQueryId` like every other per-query column (§1; matching.md §3). Tag *strings* live in
  the engine-level dictionary, never in the hot path or per segment.
- **Write path (unchanged shape).** Tags ride the same routes as the query itself: `insert_live` carries
  them into the memtable; `flush()` seals them into the base segment's tag column; `bulk_ingest()` packs
  them as it compiles the batch (§4). An **update** re-inserts the new version (with its tags) and
  tombstones the old physical id — tags are versioned exactly like the expression. Because the dominant
  *update* in the workload is a **metadata/status-only change**, a future optimization may rewrite only
  the tag column for a query whose expression is unchanged; the baseline simply re-compiles the query.
- **Persistence + reopen.** The tag column is part of the immutable `.seg` payload (ADR-012), so it
  mmaps back on `Engine::open()` / attach-and-mmap (ADR-032) with no rebuild — the same durability story
  as the required/forbidden columns. The segment format gains one versioned section; older segments
  without it read back as "no tags" (an empty column), so the change is backward-compatible.
- **What does *not* change.** The candidate index (matching.md §2), the signature optimizer, and the
  common-mask gate are untouched — tags are verify-stage data only (matching.md §5.3). The
  lossless-cover contract and the segment / compaction lifecycle are unaffected; a compaction (§7)
  simply carries the tag column through the merge like any other SoA column.

---

## 12. Distributed ownership persistence (ADR-109)

Cluster rows add placement identity beside the exact SoA, downstream of matching semantics. Segment
v7 appends parallel generation (`u64`), shard-count (`u32`), mode (`u8`), position offset/length, and
sorted `u32` position-blob columns. Open validates every column count, mode, range, ordering, and local
membership before publishing the segment. Flush and both compaction paths carry the columns; canonical-
body members retain independent placement rows. Placement is included in content fingerprints so peer
recovery cannot skip a copy solely because semantic query bodies agree.

Standalone segments v1–v6 remain readable and continue to use `EmitAll`; v7 is written only when a
segment contains distributed placement. Cluster persistence uses a deliberately stricter migration:
cluster manifest v6 records the current placement generation, coordinator log and per-shard translog
v4 persist the write-time placement on Add/Upsert, and adopted feature-space v2 records generation +
shard count. Older clustered formats are rejected with rebuild/wipe guidance because reconstructing
their placement under the current ring could change the unique emitter.

## 13. Bottom line (best-in-class, tailored to us)

- **Log-structured, append-only, immutable segments** — the proven Lucene/LSM/Aurora write path.
- **Deltas + eventual merge, but *read-optimized*:** bound the segment count and add per-segment anchor
  filters, because percolation read-amplifies over segments (the one place we diverge from RocksDB
  tuning).
- **Never rebuild by default.** Single writes → memtable+log (~750k/s/core, NRT). Bulk adds → build an
  L0 segment directly (~650k compiles/s/core), existing data untouched. Churn → background
  compaction-that-improves (re-anchor drift, reclaim tombstones, refresh the model).
- **From-scratch is reserved** for the initial seed and major feature-model changes — and the latter is
  a zero-downtime **blue/green replay from the log**, not a re-ingest.
