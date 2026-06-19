# Backup & restore

Operational procedure for backing up and restoring a Reverse Rusty deployment, single-node or
cluster. Design rationale + the safety argument: [ADR-079](../decisions/adr-079-backup-restore.md).

> **TL;DR** — `POST /_backup {"dest": "<server-side path>"}` writes a consistent, self-contained
> snapshot to `dest`. Restore by pointing a fresh server/coordinator at the copy with `--data-dir`.
> Reads keep serving during a backup; writes pause for the copy. `dest` must not already exist.

## What a backup contains

A backup is a relocatable copy of the durable `data_dir` — exactly the files the committed manifest
references, nothing else:

| Mode | Files copied |
|---|---|
| **Single-node** | `manifest.bin` + the manifest's `segments/*.seg` + `sources.dat` + `wal.log` |
| **Cluster** | `cluster_manifest.bin` + `cluster.log` + per-shard `shard_<i>/segments/*.seg` + `shard_<i>/sources.dat` |

The frozen dict, vocabulary, and tag space are embedded **inside** the manifests, so they travel
with the copy automatically. **Replica directories are not copied** — a cluster rebuilds replicas
from the primaries on open. Orphan segment files (left by an earlier crashed compaction) are
skipped.

## Why not just `cp -r` the data directory?

A live `cp -r` is **unsafe**. A concurrent flush/compaction commits a new manifest and then deletes
the now-superseded segment files; a copier that reads the manifest and then copies segments can race
that deletion and capture a manifest that references a file the copy missed — a corrupt backup.

`POST /_backup` avoids this by doing the copy **inside the engine, under its write lock**: no
compaction can run during the snapshot, so the manifest and the files it names are always
consistent. The whole backup is staged in a sibling temp dir and atomically renamed into place, so
a crash mid-backup never leaves a half-written `dest`.

## Taking a backup (REST)

```sh
# single-node or cluster coordinator — same call
curl -fsS -XPOST http://<host>:9200/_backup \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $RR_AUTH_TOKEN" \   # if auth is enabled (ADR-062)
  -d '{"dest": "/backups/rr-2026-06-19"}'
# → {"acknowledged": true, "dest": "/backups/rr-2026-06-19", ...}
```

Notes:
- `dest` is a path **on the server's filesystem**, not the client's. Mount your backup volume into
  the container and point `dest` there.
- `dest` must **not already exist** (a 400 otherwise) — never overwrite a prior backup in place.
- An in-memory engine/cluster (no `--data-dir`) returns 400; a persistence-degraded engine returns
  503 (its on-disk state is known-incomplete — investigate before backing up).
- The cluster call checkpoints first, so it doubles as a durability commit point.

## Restoring

Restore is just opening the engine on the backup directory — there is no separate restore command.

```sh
# copy the backup to where the new instance will read it, then:
server --data-dir /restore/rr-2026-06-19 --port 9200                  # single-node
server --cluster --data-dir /restore/rr-2026-06-19 --port 9200 ...    # cluster coordinator
```

The instance reconstructs from the manifest, attaches the segments, and replays the log/WAL tail —
the same crash-recovery path the durability oracle proves equivalent to the pre-backup state. A
restored cluster rebuilds its replicas from the primaries on open.

**Validate a backup before trusting it.** The library exposes `storage::verify_backup(dir)` /
`storage::verify_cluster_backup(dir)`, which re-open every referenced segment and check its CRC. A
fresh `POST /_backup` already runs this before acknowledging; re-run it on archived copies to detect
bit-rot before a real restore is needed.

## Zero-write-stall backups (large deployments)

The built-in `POST /_backup` pauses **writes** (not reads) for the duration of the file copy — a
multi-second stall on a very large corpus. For a backup that never pauses writes, snapshot a
checkpoint'd directory at the filesystem layer:

1. `POST /_checkpoint` (cluster) or `POST /_flush` (single-node) to commit a consistent on-disk state.
2. Take an atomic copy-on-write snapshot of the `data_dir` volume (ZFS/LVM snapshot, AWS EBS
   snapshot, GCP disk snapshot, etc.) — instantaneous, no engine involvement.
3. Copy the snapshot to backup storage at your leisure (the snapshot is frozen, immune to the live
   engine's later compactions).
4. Restore = mount/copy the snapshot's contents into a `data_dir` and start an instance on it.

This is the recommended production path where a write stall is unacceptable and CoW storage is
available.

## Scheduling

`POST /_backup` is a single idempotent-per-`dest` call — drive it from cron/k8s-CronJob with a
date-stamped `dest`, then prune old copies with your normal retention tooling. Each backup is
fully self-contained (no dependency on prior backups), so pruning is just `rm -rf` of old dirs.

## Not covered in v1 (see ADR-079)

- **Online (no-quiesce) backup** that allows concurrent writes during the copy — the
  retention-lease + translog-tail machinery peer recovery uses is the documented follow-on.
- **Incremental/differential** backups — every backup is a full copy.
- **Streaming to an object store** (S3/GCS) directly — `dest` is a local filesystem path; pair with
  an FS-snapshot + your own uploader, or copy the backup dir up afterward.
- A **`POST /_restore`** endpoint — restore is operator-driven (`--data-dir`), by design.
