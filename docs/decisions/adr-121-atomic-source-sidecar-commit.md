# ADR-121: Atomic manifest-selected source-sidecar commits

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** Canonical query source is deliberately outside the match-optimized `.seg` files
  (ADR-014). Standalone bulk ingest therefore had two durable publications: it wrote and selected the
  new segment in `manifest.bin`, then updated `sources.dat`. A failure or crash after the manifest
  rename left acknowledged match data without the corresponding source document. Health degradation
  detected a live-process write failure, and source-generation checks prevented stale GET/explain
  results, but restart had no durable information from which to complete the missing update. The same
  ordering hazard existed on a WAL-backed flush if the source rewrite failed but the manifest and WAL
  reset still advanced. Detection without recovery is insufficient because source drives GET,
  explain, compiler/vocabulary rebuilds, checkpoint reopen, and backup.

- **Research.** The smallest established design is an immutable-file set selected by one commit
  document. Lucene records the active segment set in the latest
  [`segments_N`](https://lucene.apache.org/core/9_12_3/core/org/apache/lucene/index/SegmentInfos.html);
  files are prepared and synced before that commit becomes active. RocksDB recovery likewise derives
  live SST/blob files from the
  [MANIFEST](https://github.com/facebook/rocksdb/wiki/Track-WAL-in-MANIFEST), and external-file ingest
  promises all-or-nothing selection of already-built SSTs
  ([official RocksDB documentation](https://github.com/facebook/rocksdb/wiki/creating-and-ingesting-sst-files)).
  Reverse Rusty's cluster compiler migration already used the same shape: generation-named source
  sidecars selected with the shard segment registry in cluster-manifest v7 (ADR-118). Extending that
  proven mechanism to the standalone manifest is smaller and easier to reason about than introducing
  a second repair journal, its own commit marker, replay rules, and garbage-collection state machine.

- **Decision — source is part of the manifest file set.** Standalone manifest **v7** appends one
  validated source-sidecar basename. Pre-v7 manifests continue to select `sources.dat`. A source
  mutation now writes a complete immutable `sources_gNNNN….dat` candidate using tmp + fsync + durable
  rename. Only after that succeeds does `manifest.bin` atomically select both the segment registry and
  the candidate source corpus. Generation filenames are never trusted as paths: the manifest codec
  accepts one safe basename only. A failed attempt may leave an unselected generation file, which is
  an inert orphan and can be replaced on retry.

- **Decision — ordering by write path.**
  - Bulk ingest first writes/mmaps the new segment, then writes a candidate source snapshot with the
    accepted documents overlaid **without publishing them in memory**, then commits the manifest.
    Source preparation or manifest failure deletes the artifacts and rejects the batch. After the
    manifest rename, the source documents are published in RAM and lazy mode remaps the selected
    file. A crash in that final window simply reopens the manifest-selected file, completing
    publication automatically.
  - WAL-backed flush writes the segment, prepares the source candidate, commits the joint manifest,
    and only then checkpoints/resets the WAL. A source failure therefore leaves the prior manifest
    and complete WAL recovery authority intact.
  - Compaction, tombstone reseal, and source-only publication use the same joint commit helper.
    Source-driven recompile prepares its candidate first but selects it only after the replacement
    segment is durable. The preparation step must not write a source-only manifest: that would
    advance the WAL watermark before the new segment captures a memtable insert followed by its
    logical delete, allowing recovery to replay the insert while skipping the delete.
  - Cluster shards retain their coordinator-owned protocol. They do not write a local manifest;
    `cluster_manifest.bin`/the shard checkpoint already owns source-sidecar selection and refuses to
    trim its log when source persistence is unhealthy.

- **Recovery, availability, and diagnostics.** Matching still opens from committed segments when a
  selected source file is externally missing or corrupt. The engine marks persistence unhealthy and
  emits `SourceStoreLoad`; GET/explain/source-driven rebuilds fail loud rather than returning stale
  content or committing a strict subset. An explicit in-memory commit fence also refuses bulk,
  flush, compaction, and source-only publication from that incomplete recovery baseline; otherwise a
  valid empty replacement could hide the damage and make a later backup certify missing sources.
  The fence clears only by restarting after the selected corpus is repaired. In the ordinary crash
  case no repair action is necessary: the selected immutable file was durable before the commit
  point. Lazy remap failure after commit closes the same fence, while restart remains self-healing
  because the manifest already names the valid file.

- **Backup and compatibility.** Single-node backup copies and verifies exactly the source basename
  selected by `manifest.bin`; an unselected generation is skipped, and a missing selected v7 file
  fails backup. Restore uses the unchanged `Engine::open` path. Manifest v1–v6 remain readable and
  retain optional legacy `sources.dat` behavior. Manifest v7 is a rollback fence: an older binary
  refuses it instead of silently reopening the fixed legacy sidecar. The source binary format itself
  is unchanged.

- **Consequences.** The source store remains outside the match hot path, and its full-rewrite cost is
  unchanged asymptotically. Standalone commits can transiently hold the old and new source files at
  once; the old selected file is removed only after the new manifest commits and lazy readers remap.
  Crash-left orphans are ignored by open and backup. No AST, strings, allocation, or new branch enters
  matching.

- **Proof.** A deterministic test arms a crash injection immediately after the joint manifest rename
  and before live source publication during WAL-free bulk ingest. Reopen proves matching, GET
  document, explain, source-driven rebuild, a subsequent checkpoint/reopen, and backup/restore all
  recover the same query. Persistence tests cover resident and lazy stores, stale/missing selected
  files, refusal to replace an incomplete recovery baseline, same-version generation mismatch,
  source-write failure preserving the prior manifest, recompile failure preserving a later logical
  delete in the WAL, and later live/bulk source ordering. Manifest tests pin v7 round-trip, rollback
  version, and basename validation; backup tests require and copy only the selected generation.
