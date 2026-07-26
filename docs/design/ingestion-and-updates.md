# Ingestion & Update Lifecycle — immutable segments, LSM write path, compaction

*Scope: how stored queries get in, get updated, and get re-optimized — the write path and storage
model. Covers immutable segments + hot delta + tombstones, the LSM write path, deltas-with-merge,
bulk-ingest vs rebuild rules, compaction, vocabulary rebuilds, and compatibility fences. Siblings:
[`matching.md`](matching.md) (what's stored), [`clustering-and-scaling.md`](clustering-and-scaling.md)
(the durable mutation log in a cluster), [`normalization.md`](normalization.md). See the
[overview](README.md) for the correctness contract. **Answer in one line:** a log-structured (LSM)
write path with immutable segments and read-optimized compaction; ordinary changes are incremental,
while explicit vocabulary changes rebuild from retained canonical query source before publication.*

> **Implementation status:** The core LSM engine is implemented and tested: segments, memtable,
> flush, bulk ingest, tombstones, compaction, mmap'd segments, and WAL. Compaction uses a
> ClickHouse-inspired score selector (§5–6) that minimizes time-integrated segment count; the API
> supports `compact(max_segments)`, `compact_all()`, and `compact_range(lo, hi)`. Frozen mmap hash
> tables and crash recovery shipped under ADR-012/013. Compaction re-anchoring is opt-in and
> oracle-proven (ADR-056); [feature-model versioning](../roadmap.md#versioned-feature-models-with-bluegreen-re-materialization)
> remains proposed.

**TL;DR (for agents)**
- **Owns:** LSM engine (`segment.rs`), the write path and storage model
- **Key invariant:** Segments are immutable once sealed; writes go to the memtable only; never rebuild existing segments by default
- **Write path:** `insert_live` → memtable → `flush()` seals to base segment; `bulk_ingest()` compiles a batch directly into a new segment
- **Update model:** tombstone old local rows + insert a new version; snapshot publication makes the result visible atomically
- **Measurements:** current ingest captures live in [performance/results.md](../performance/results.md)
- **Built controls:** manual `recommended_shard_count` / resize helpers; optional deterministic compaction re-anchoring
- **Design-only:** versioned feature-model generations, telemetry-driven cover refresh, and automatic arity/placement recommendations
- **Recently implemented:** durable mutation log (WAL — ADR-013), mmap'd segments (ADR-012), per-segment anchor filters (cache-line blocked bloom — ADR-011), score-based compaction (ADR-009), compaction re-anchoring (ADR-056), per-query metadata storage (ADR-049)

Builds on [`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md) (feature
learner) and [`clustering-and-scaling.md`](clustering-and-scaling.md) (cluster persistence). Grounded
in RocksDB/LSM and Lucene segment merging; the shipped cluster is shared-nothing and does not treat a
remote object-store log as the database.

---

## 1. Immutable segments + hot delta + tombstones (the core model)

```
Index = [ Segment_0, Segment_1, ..., Segment_n ]   (immutable, mmap-able)
        + HotDelta                                  (small, in-memory, mutable; the "memtable")
        + Tombstones                                (dead segment-local rows)
        + published immutable snapshot
```

- **Add query:** compile → assign a new local row/version → insert into HotDelta (its own little
  candidate index + exact-match arrays) → publish a new snapshot. Visible immediately;
  no segment rebuild.
- **Update query:** compile new version into HotDelta; **tombstone** the old local row. The
  matcher skips tombstoned IDs at the resolve step.
- **Delete query:** tombstone.
- **Match** probes all segments + HotDelta under one immutable snapshot; server readers use
  `ArcSwap`, so publication does not block them and old snapshots live until their readers release.

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
add/update/remove ─► (1) append to the mode's durable tail (WAL or coordinator log)
                     (2) apply to in-memory MEMTABLE (the hot delta)  ── publish snapshot
                                   │ flush on size/time
                                   ▼
                     (3) immutable L0 SEGMENT (compiled candidate index + exact SoA)
                                   │ manifest/checkpoint commit + background compaction
                                   ▼
                     (4) larger base segments  ── bounded total segment count
```

- Durable state is a **checkpoint plus a mutation tail**, not an indefinitely retained full log.
  Standalone checkpoints atomically select immutable segments, dictionaries/vocabulary, source
  metadata, and watermarks in the manifest; the WAL replays only work after that checkpoint. Cluster
  checkpoints similarly select per-shard segments in the cluster manifest, with coordinator and
  per-shard log tails for post-checkpoint recovery. Logs may be truncated after commit, so segments
  and retained source are part of the authoritative corpus.
- **Segments are immutable** (Lucene/LSM): the write path is append-only; complexity is pushed to
  the merge, which is the right place for it.
- **Updates/deletes are tombstones**, not in-place edits: update = compile new version into the
  memtable + tombstone the old physical id; delete = tombstone. The matcher skips tombstoned ids at
  the resolve step; space is reclaimed at merge (Lucene marks deleted docs and drops them when it
  rewrites a segment — same model).

This is exactly the §1 model, now named and tuned.

---

## 4. Six write scenarios → decision rules

| Scenario | Best-in-class approach | Cost | Touches existing data? |
|---|---|---|---|
| **Single add / update / remove** | append to durable tail + memtable; tombstone old on update | O(1 query), visible when the successful operation publishes | no |
| **Bulk add** | compile the batch → build a **new base segment directly** → durable commit + publish | O(batch); existing segments are untouched | no |
| **Routine churn / accumulated tombstones** | background compaction triggered by size & `holes_ratio` | amortized; off the hot path | merges a few segments |
| **Anchor drift / poor covers** | repaired *during* compaction of the affected segments (re-anchor, repack) | amortized into a merge already happening | only the segments being merged |
| **Initial seed** | bulk-build base segments, skip the memtable, publish after durable commit | one-time; shard builds are independent | n/a (creating) |
| **Vocabulary/normalizer change supported by the current API** | preflight retained source → rebuild live queries under the proposed vocabulary → publish only after success | O(live corpus) | replaces compiled state |

**So: should we always build from scratch? No.** From-scratch is two narrow cases — the *initial seed*
and an explicit vocabulary/normalizer rebuild. Everything else is incremental delta + merge.

**Bulk add = build a segment directly, don't funnel through the memtable.** This is the analog of
RocksDB *ingest-external-SST* / Lucene *addIndexes*: the current implementation parses/compiles the
batch in deterministic passes, packs postings + exact SoA into a new segment, commits its segment and
source metadata atomically, then publishes. It bypasses the live memtable and does not rewrite
existing segments.

---

## 5. Deltas with eventual merge — tuned for our read profile

The shipped engine does **not** model explicit tiered/leveled levels. It keeps one pool of sealed base
segments and selects a contiguous merge range with a ClickHouse-inspired score:

```
(sum_size + compaction_fixed_cost × count) / (count − 1.9)
```

The fixed per-segment cost biases the selector toward reducing time-integrated segment count, the
right objective when a read may probe every segment. `max_segments` (default 8) triggers count-based
compaction; `holes_ratio_threshold` triggers reclaim when any base segment has too many dead rows.
Both are runtime settings in standalone mode. ADR-009 records why this replaced the original
tiered-L0/leveled-base hypothesis.

---

## 6. Per-segment anchor filters = our "Bloom filters"

Every sealed segment carries a **cache-line blocked Bloom filter** over its signature keys: 512-bit
blocks, six probes via double hashing, roughly 10 bits/key, and a target false-positive rate near 1%
(`src/filter.rs`, ADR-011). Each signature probe checks the filter before reading the frozen index. A
definite miss skips that segment; a false positive only performs extra work and cannot drop a match.
The filter is rebuilt at every seal/merge point. ADR-011 records why this replaced the original
xor/binary-fuse proposal.

---

## 7. Compaction that *improves*, not just merges

Compaction (background) does more than concatenate-minus-tombstones — it *improves* the index, all
amortized into a merge that's happening anyway. The replacement is published atomically:

1. **Drop tombstones**, reclaim space, renumber to dense `SegmentLocalQueryId`s for cache locality.
2. **Optionally re-anchor** reconstructed positive predicates with the current frozen dictionary
   frequencies (`compaction_reanchor`, ADR-056). Visibility guards forbid a cost-only move into the
   opt-in C lane; `hot_migration_max_moves` bounds A↔H work.
3. **Regroup canonical duplicate bodies** on the destination side (ADR-106), while preserving each
   logical member's own identity, tags, rank, source generation, and placement metadata.
4. **Rebuild postings and anchor filters** from the surviving destination rows. The adaptive
   in-memory posting representation is selected naturally as IDs are appended; durable output expands
   body groups and writes the current frozen segment format.

Compaction does **not** consume candidate-survival telemetry, re-rank `FeatureId`s/common-mask bits,
learn a new vocabulary, or choose a recommended signature arity. Those are separate roadmap ideas,
not current merge behavior.

---

## 8. Vocabulary changes and semantic compatibility

The current system has two concrete mechanisms:

- The engine dictionary's name→ID mapping and top-64 common mask are frozen and persisted. New names
  on frozen/read-only paths use deterministic synthetic IDs. Existing compiled rows therefore remain
  comparable, and compaction never changes their mask interpretation.
- A vocabulary/normalizer change requires canonical query source. `Engine::set_vocab` preflights the
  proposed normalizer against every live source and marks the compiled corpus stale; the serving path
  follows it with `recompile_stale_segments` before publishing. The library exposes that split
  explicitly. The in-process cluster builds replacement shards from live source and swaps the rebuilt
  state only after success. Remote shard servers run the stock normalizer; the current transport does
  not ship normalizers, so custom vocabulary and live vocabulary changes are refused there.

`vocab_epoch` is process-local stale-state bookkeeping, not a persisted universal feature-model
version. Durable format versions and compiler semantic fences make incompatible binaries fail loud,
but a general multi-generation feature model with blue/green serving is still proposed:
[`Versioned feature models`](../roadmap.md#versioned-feature-models-with-bluegreen-re-materialization).

---

## 9. Visibility & consistency

- **Immediate after success:** the standalone durable path appends its WAL record before applying;
  cluster writes append the coordinator log before shard apply. A successful public operation
  publishes the updated snapshot before returning, so a later read through that process sees it.
- **Primary-authoritative replication:** `ReplicatedShard` applies to the primary, then fans the
  mutation to replicas. Replica failures mark those copies out of sync but do not turn the successful
  primary write into a quorum failure. There is no optional quorum-ack read-your-writes mode.
- **Snapshot consistency:** each match sees one immutable snapshot of a shard's memtable + segments.
  Cluster reads additionally carry placement-generation/ownership fences where the result contract
  requires them.

---

## 10. How this maps onto / extends the current engine

- **Today (implemented):** a multi-segment LSM-shaped engine — `Vec<base Segment>` + a mutable
  `memtable` Segment; matching unions across all segments with per-segment candidate dedup; `flush()`
  seals the memtable into a base segment; `bulk_ingest()` compiles a batch directly into a new base
  segment without rebuilding existing ones; tombstones handle update/delete. That already realizes the
  memtable + delta + tombstone core *and* read-amp = segment count (measured by `segbench`).
- **Implemented since initial design:**
  1. ~~Leveled/tiered compaction~~ → ClickHouse-inspired score-based compaction (ADR-009).
  2. ~~Per-segment anchor filters~~ → cache-line blocked bloom filter (ADR-011).
  3. ~~Durable mutation log + mmap'd segments~~ → WAL (ADR-013) + mmap'd segment file format with
     frozen hash tables (ADR-012). `Engine::open()` for manifest + WAL recovery.
- **Open follow-ups:** the roadmap owns the full proposals and completion tests for
  [`Self-tuning cost and placement recommendations`](../roadmap.md#self-tuning-cost-and-placement-recommendations)
  and [`Versioned feature models`](../roadmap.md#versioned-feature-models-with-bluegreen-re-materialization).

---

## 11. Per-query metadata storage

> **Status: built in standalone and cluster paths, with differential coverage** (ADR-049/055). The metadata *model*, filtered
> percolation, and ranking live in [`matching.md`](matching.md) §5; this section is the **storage /
> persistence** half — how per-query tags are written, sealed, and recovered. Tags were introduced in
> segment v3 and WAL v2 and remain present in the current segment v10 / WAL v7 formats; segment v1/v2
> reopen as untagged. The complete version matrix is in
> [`rolling-upgrade.md`](../operations/rolling-upgrade.md). Decided in
> [ADR-049](../decisions/adr-049-percolator-parity-tags.md); the cluster extension is ADR-055.

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
v7 introduced parallel generation (`u64`), shard-count (`u32`), mode (`u8`), position offset/length, and
sorted `u32` position-blob columns. Open validates every column count, mode, range, ordering, and local
membership before publishing the segment. Flush and both compaction paths carry the columns; canonical-
body members retain independent placement rows. Placement is included in content fingerprints so peer
recovery cannot skip a copy solely because semantic query bodies agree.

The current segment writer is v10; readers accept v1–v10 subject to the documented feature fences, and
rows without placement continue to use `EmitAll`. Cluster persistence uses a deliberately stricter
migration: cluster manifest v7 is current (v6 remains readable), coordinator log and per-shard
translog v4 persist write-time placement, and adopted feature-space v2 records generation + shard
count. Cluster manifests v1–v5 require rebuild/wipe because reconstructing placement under the
current ring could change the unique emitter.

## 13. Bottom line (best-in-class, tailored to us)

- **Log-structured, append-only, immutable segments** — the proven Lucene/LSM shape, with manifests
  and segments as part of durable truth rather than a never-truncated remote log.
- **Deltas + eventual merge, but *read-optimized*:** bound the segment count and add per-segment anchor
  filters, because percolation read-amplifies over segments (the one place we diverge from RocksDB
  tuning).
- **Never rebuild by default.** Single writes use the durable tail + memtable; bulk adds build a base
  segment directly; churn uses background compaction to reclaim rows and optionally re-anchor.
- **Rebuild explicitly** for the initial seed or a vocabulary/normalizer change, and rebuild from
  retained canonical source. General versioned blue/green feature-model generations remain roadmap
  work.
