# ADR-079: Backup/restore — the engine-driven consistent snapshot

**Status:** Accepted (2026-06-19)

**Context.** ADR-065 criterion 11 — a *documented + tested* backup/restore for both the
single-node engine and the cluster (coordinator manifest + per-shard segments + logs). The
durable artifacts to capture already exist and `Engine::open` / `ClusterEngine::open` already
reconstruct from them (oracle-proven ≡ brute), so "restore" is a solved problem. The open part
was producing a *safe* backup: a naive `cp -r` of a live `data_dir` is **not** safe (ADR-064 item
7) — a concurrent flush/compaction commits a new manifest and then deletes the superseded `.seg`
files (`cleanup_segment_files` at the end of `do_compact_range`), so an external copier that reads
the manifest and then copies segments can race that deletion and capture a manifest referencing a
file it missed.

**Key insight.** That hazard exists *only* because the copier is external to the engine. The
engine is single-writer (ADR-017): compaction is synchronous on the write path, there is no
background LSM thread. So if the **engine itself** performs a manifest-driven copy while holding
its own write-path exclusion, no compaction can run during the copy and it is race-free **by
construction**. The "write-quiescing / FS-snapshot / file-pinning" trilemma from ADR-064 item 7
collapses to: *the engine names the pinned set (the just-committed manifest's segment list) and
holds the writer for the copy window.*

**Decision.** Backup is a first-class engine operation that produces a consistent on-disk
snapshot; restore is the existing `open()` pointed at the (relocated) copy.

- **Single-node** — `Engine::backup_to(&mut self, dest)`. `&mut self` is deliberate: it encodes
  the write-path exclusion at the type level and matches `flush`/`compact`; the server only ever
  calls it under its `Mutex<Engine>`, so reads keep flowing off the lock-free `ArcSwap` snapshot.
  Copies the manifest-referenced segments + `sources.dat` + `wal.log`; restore replays the WAL
  tail, so no flush is forced and the manifest's `wal_seq_watermark` ↔ WAL pair is consistent
  (frozen under the lock).
- **Cluster** — `ClusterEngine::backup_to(&self, dest)` (matches `checkpoint`). The server holds
  `write_serial` + the cluster READ lock across the whole call. It `checkpoint()`s **first** — this
  is load-bearing: `seal_for_checkpoint` persists every shard's `sources.dat` even when its
  memtable is empty (the ADR-074 seam), so the source dir is fully consistent; a raw hot-copy of a
  clean shard could miss `sources.dat` and silently lose its corpus on the next vocab rebuild —
  then copies the coordinator manifest + `cluster.log` + each shard's manifest-referenced segments
  + `sources.dat`. **Replica dirs are not copied** — `open` rebuilds them from the primaries via
  peer recovery (ADR-035).
- **Atomic backup, manifest-last.** The copy is staged into a sibling `<dest>.backup.tmp` and
  `durable_rename`d into place (a single top-dir rename moves the whole nested tree atomically); a
  crash mid-backup leaves only the staging dir, never a half-populated `dest`. Within the staging
  dir the manifest is copied **last** (the engine's own "build durable, then commit" discipline).
  A pre-existing `dest` is refused.
- **Copy, never regenerate, the manifests.** A byte-for-byte file copy preserves the manifest
  version word — including the v4 class-D rollback fence (ADR-068) — and carries the embedded
  dict / vocab / tag-dict blobs verbatim, so the restore sees identical placement, the same loud
  fence, and the same tag space with no special-casing. Orphan `.seg` files (left by an earlier
  crashed compaction) are skipped — only manifest-referenced files are copied.
- **Verify before acknowledging.** `verify_backup` / `verify_cluster_backup` re-open every
  referenced segment via `MmapSegment::open` (which CRC-checks), so a corrupt or missing file is a
  loud error, not a backup that fails only at restore time. Also exposed for an operator to vet a
  backup before trusting it.
- **REST.** `POST /_backup {"dest": "<server-side path>"}` in both single-node and cluster mode,
  behind the ADR-062 bearer-token gate (default-deny on non-GET/HEAD ⇒ covered with no auth-table
  edit). Restore stays operator-driven (point a fresh server/coordinator at the copy via
  `--data-dir`); there is no `POST /_restore` in v1.

**Strategy trade-off (recorded, not hidden).** The built-in path is a **write-exclusion snapshot**:
correct and simple, but it pauses *writes* (not reads) for the copy duration — fine for the common
case, a multi-second write-stall on a very large corpus. For a zero-write-stall production backup
the runbook documents the **FS-snapshot-of-a-checkpoint'd-dir** procedure: `POST /_checkpoint` (or
`POST /_cluster/checkpoint`), take an atomic COW filesystem / cloud-volume snapshot (instant), then
copy the snapshot offline — the engine's only job there is the consistent on-disk state `checkpoint`
already produces. A true **online backup that allows concurrent writes** (reusing the retention-lease
+ translog-tail machinery peer recovery already uses, ADR-039/040) is the documented follow-on.

**Why this is safe.** The backup captures exactly the committed live set (manifest-referenced
segments + their baked tombstone bitmaps inside the manifest + the WAL tail for single-node), and
restore is the *same* `open()` the durability oracle already proves ≡ brute. Zero false negatives
is preserved because (a) the lock is held across the whole copy, (b) the cluster path checkpoints
first, (c) manifests are copied not regenerated, and (d) the single-node path copies the WAL — all
four enforced in code. A persistence-degraded engine is refused (its on-disk state is
known-incomplete); an in-memory engine/cluster is refused (nothing to back up). The percolate hot
path is untouched.

**Proven.** `tests/persistence/backup.rs` — backup → relocate → `open` ≡ source (multi-segment +
base tombstone + WAL tail), point-in-time isolation (post-backup churn does not leak into the
snapshot), a never-flushed WAL-only engine round-trips, refuse-existing-dest / refuse-in-memory,
no-partial-dest-on-failure, and `verify_backup` catches a corrupted segment.
`tests/cluster_durability_oracle/backup.rs` — backup → `open` ≡ pre-backup ≡ brute across
K∈{1,3,8} × broad on/off, post-backup-churn isolation, a tagged corpus (filtered ≡ tag-aware brute
— the embedded tag space survives), backup → open → checkpoint → reopen (a restored backup is a
legitimate durable root), refuse-in-memory, and `verify_cluster_backup` catches a corrupted shard
segment. Plus `storage::backup` unit tests for the copy/verify primitives.

**See also:** ADR-031/032 (the durable manifest + reattach that `open` restores from), ADR-017
(single-writer — why the in-engine copy is race-free), ADR-066 (durable tombstones carried in the
manifest), ADR-074 (the `sources.dat`-at-checkpoint seam the cluster path depends on), ADR-068 (the
class-D v4 fence preserved by a verbatim manifest copy), ADR-062 (the auth gate), ADR-064 item 7 +
ADR-065 criterion 11 (the requirement). Deferred: online no-quiesce backup via retention leases;
incremental/differential backups; streaming to a remote/object-store dest; a `POST /_restore`
endpoint.
