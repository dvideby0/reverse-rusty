# ADR-066: Tombstone durability at the commit point (manifest liveness bitmaps + address-free delete log)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** Found while building the ADR-064 atomic upsert (whose replace-by-id semantics tombstone a
  query's prior copies): **two pre-existing single-node durability bugs around base-segment tombstones**,
  both reproduced by failing tests before the fix, one of them a zero-false-negative violation — the one
  class of bug this project's correctness contract forbids outright.
  - **Bug 1 — acknowledged deletes resurrect across flush + reopen.** A delete against a BASE segment
    mutates only the in-RAM mmap **alive-overlay** (`storage/segment/mmap.rs` — the on-disk `.seg` alive
    flags are frozen at write time); its WAL frame was the *only* durable record. `flush()` then
    checkpoints **and resets** the WAL on the claim "all prior entries are materialized into sealed
    segments" — true for memtable mutations (the flushed segment carries them), **false for base-segment
    tombstones**, which are in no durable artifact at all. Crash after the flush ⇒ the deleted query
    **resurrects** on reopen. Reproducible under the **default config** whenever the deleted entries sit
    below the holes-ratio compaction trigger (e.g. 1 delete in a 20-entry segment = 5% < 30%) — i.e. the
    common case for a large segment with scattered deletes. The cluster recognized and fixed exactly this
    hazard at its checkpoint (`reseal_tombstoned_segments`, ADR-032: *"the deleted query would resurrect
    on reopen"*); the single-node flush path had the same hazard unaddressed.
  - **Bug 2 — compaction + crash replays positional tombstones into the wrong query (a false negative).**
    `delete_by_logical_id` logged one **positional** `(seg_idx, local_id)` WAL frame per copy. A
    compaction commit **splices the segments vec and renumbers local ids**, while leaving the WAL
    untouched — so an un-checkpointed positional frame (e.g. delete → explicit `/_compact` → crash)
    replays its stale address against the *post-compaction* segment list and **tombstones an unrelated
    query**: a silently-deleted innocent stored query (the sacred FN case), plus the intended delete may
    be lost. Proven by a test that deletes q3, compacts, crashes — and reopens with q4 missing.

- **Decision.** Make tombstone state durable **at the manifest commit point**, and make the production
  delete **address-free in the log** — three coordinated pieces:
  1. **Manifest v3: per-segment dead-locals bitmaps (the Lucene `.liv` analogue).**
     `save_manifest_if_persistent` records, for every dirty mmap segment, a roaring bitmap of its DEAD
     local ids (`segment_tombstones: Vec<(file_name, bitmap_bytes)>`; `roaring` is already a core
     dependency). `Engine::open` applies each segment's bitmap right after attaching it, **before** the
     WAL tail replays. Every site that resets the WAL already gates on a successful manifest write
     (flush, vocab-recompile), so the reset is now safe by construction: the commit it gates on carries
     the overlay. Lucene solves this identically — per-commit `.liv` live-docs files for segments with
     deletions. The dead set is maintained **incrementally** on the `MmapSegment` (seeded from the
     on-disk flags, one bitmap insert per tombstone), so the commit serializes it in **O(deletes)** —
     never a full-segment rescan (a codex-review fix; the first cut rescanned every dirty segment per
     commit), and not O(segment bytes) like reseal-per-flush would be.
  2. **WAL v3: one address-free `DeleteByLogical { logical }` frame** (op 3) per
     `delete_by_logical_id` call, replacing the N positional frames. Replay re-derives the affected
     copies from the recovered state — live path and replay share one funnel
     (`apply_delete_by_logical`: tombstone every live copy across segments + memtable, then drop the
     source text). The frame is immune to compaction's address renumbering. Bonus: the delete is now
     **all-or-nothing under a WAL failure** (one frame up front) where the old loop could fail midway
     with earlier tombstones already applied.
  3. **A `wal_seq_watermark` in the manifest, governing replay of BOTH delete-frame kinds.** The
     manifest records the last WAL seq whose effects the commit captured; on recovery, a frame with
     `seq <= watermark` is **skipped**, and each kind needs the skip for its own reason:
     - A **positional** frame (`tombstone`/`tombstone_in` — niche per-address APIs, today used only by
       tests/bench) targeting a **base** segment addresses positions that may have been **renumbered**
       since (the Bug-2 mechanism); at/below the watermark its effect is already in the bitmaps (or the
       entry was dropped by the merge).
     - A **logical** `DeleteByLogical` frame must not replay over state committed after it: **bulk
       ingest bypasses the WAL** (its segment + manifest commit IS its durability, ADR-017), so a
       same-id query bulk-ingested *after* the delete exists only in the attached segments — an
       unconditional replay would erase the newer query (a codex-review catch; "in-order replay" is not
       sufficient precisely because bulk mutations don't appear in the log).
     Frames **above** the watermark replay normally, and that is safe by this invariant: *every
     segments-vec mutation (flush append, bulk append, compaction splice, reseal) commits a manifest*,
     so a frame newer than the last commit was appended against exactly the committed segment list +
     baked state that `open` attaches. Memtable frames (the `u32::MAX` sentinel) always replay — the
     memtable is rebuilt purely from the WAL tail and is never in the manifest. Two properties keep the
     watermark comparison sound: `Wal::last_seq` stays monotonic across `reset()` within a process, and
     on reopen `Engine::open` **re-pins the sequence past the recovered watermark**
     (`Wal::ensure_seq_after`) — a reset (header-only) WAL file would otherwise rescan to seq 1 below
     the manifest's watermark, and deletes issued after the reopen would be wrongly skipped by the
     *next* recovery (the second codex-review catch).

- **Why this is safe (and conservative in the right direction).**
  - The bitmaps record state the engine already applied in memory; applying them on open is idempotent
    with the on-disk alive flags and with replayed frames (`tombstone` no-ops on a dead or out-of-range
    local). A corrupt bitmap is **not applied** and surfaces as a `DurabilityFailure` — a resurrected
    delete is a bounded false positive (the exact verifier's output is still correct for live queries),
    whereas guessing could tombstone the wrong query, a false negative. Same direction as ADR-008.
  - Nothing touches signature gating, the candidate index, or the verifier — match-time behavior for
    live engines is byte-identical; only crash-recovery state changes (to match what was acknowledged).
  - In-memory mode (no `data_dir`) has no WAL and no manifest — untouched. Cluster shards run
    `owns_manifest = false` (their durability is the per-shard translog + reseal-at-checkpoint,
    ADR-032/039) — untouched.

- **Format compatibility.** Manifest `PMAN` v2 → **v3** (appends the watermark + bitmaps after the
  tag-dict blob, the same append-a-section pattern as v1→v2); v1/v2 manifests read back with watermark 0
  and no bitmaps — their era never persisted this state, so there is nothing to restore (the historical
  hazard for a pre-upgrade WAL tail is accepted and documented, not retro-fixable). WAL `PWAL` v2 → **v3**
  (adds op 3; informational per the v1→v2 precedent — old entries are unchanged and both generations
  coexist in one file). A **downgrade** (old binary, new files) fails loud on the manifest version check;
  an old binary reading a v3 WAL stops at the first op-3 frame and reports skipped bytes (the torn-tail
  path) — consistent with the existing unknown-op behavior.

- **Alternatives.** (1) *Reseal dirty segments at every flush* (extend ADR-032's cluster mechanism to
  single-node) — rejected: correct but O(dirty-segment bytes) per flush; one delete in a 1M-entry segment
  would rewrite the whole segment on the next flush. The bitmap is O(deletes) and rides a write that
  already happens; compaction still reclaims the space on its own schedule. (2) *Stop resetting the WAL
  while any base segment is dirty* — rejected: unbounded WAL growth under sustained delete load, and it
  leaves Bug 2's stale-address replay unsolved. (3) *Stable segment-id addressing in tombstone frames*
  (resolve-or-skip by filename id) — workable, but it re-encodes per-copy physical addresses the log
  doesn't need; the logical frame is smaller, simpler, and structurally immune rather than
  resolution-dependent. The watermark covers the residual positional APIs without a format change to
  their frames. (4) *Replay `DeleteByLogical` frames unconditionally, relying on in-order idempotence* —
  the first cut did, and the codex review showed it wrong: bulk ingest's mutations never enter the WAL,
  so "in order" over the log is not in order over *time* — an older delete frame would replay over a
  newer bulk-ingested same-id query and erase it. The watermark skip is load-bearing, not an
  optimization.

- **Testing.** New `tests/persistence/tombstone_durability.rs` — every pre-fix failure mode pinned:
  Bug 1 isolated (compaction disabled) **and** under the default config (the masked path); the
  delete → compaction → crash differential over a 20-query corpus (per-query want/got — catches both the
  misfire FN and the resurrection); the positional `tombstone_in` → compaction → crash watermark skip;
  delete recovery from a bare WAL tail (no flush); delete → manifest-commit → re-insert-same-id → crash
  (WAL'd re-insert); **delete → bulk re-insert of the same id → crash** (the codex P1 pin: the
  WAL-less bulk copy must survive the older delete frame); and **deletes issued after a reopen with a
  reset WAL surviving a second crash** (the codex P2 pin for the sequence re-pinning, covering both
  frame kinds). Unit tests: `wal.rs` round-trips the new frame alongside legacy ops and pins `last_seq`
  monotonicity across reset; `storage/manifest.rs` round-trips v3 and reads a hand-rolled v2 byte image
  (watermark 0, no bitmaps). The existing WAL-failure suite (delete-rejected-not-acknowledged) holds —
  strengthened, since the single frame makes the rejection all-or-nothing. Full suite (oracle +
  persistence + cluster oracles) green; `check.sh` green.

- **Consequences.** Acknowledged deletes are now durable across every flush/compaction/crash
  interleaving, and crash recovery can never tombstone a query the user didn't delete — closing a
  standing zero-FN violation. The manifest is now the single commit point for *liveness* as well as
  membership, which is exactly the substrate the ADR-064 **atomic upsert** needs: its replace-by-id
  tombstones survive restart the same way a delete does. The `/_doc` write path is unchanged at the API
  level; `deleted_count` semantics are identical. Follow-on (deliberately out of scope): the degraded
  Memory-fallback segment path (persistence already unhealthy + WAL retained) keeps its historical
  positional behavior; the ADR-064 items continue on top of this fix.
