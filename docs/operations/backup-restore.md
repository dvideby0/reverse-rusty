# Backup & restore

Operational procedure for backing up and restoring a Reverse Rusty deployment, single-node or
cluster. Design rationale + the safety argument: [ADR-079](../decisions/adr-079-backup-restore.md).

> **TL;DR** — on a durable single engine or in-process cluster,
> `POST /_backup {"dest": "<server-side path>"}` writes a consistent, self-contained snapshot to
> `dest`. Restore by pointing a fresh server/coordinator at the copy with `--data-dir`. Reads keep
> serving during a backup; writes pause for the copy. `dest` must not already exist. A remote
> cluster has a stateless coordinator, so back up its shard/control volumes as a quiesced set
> instead.

## What a backup contains

A backup is a relocatable copy of the durable `data_dir` — exactly the files the committed manifest
references, nothing else:

| Mode | Files copied |
|---|---|
| **Single-node** | `manifest.bin` + the manifest's `segments/*.seg` + selected `sources_g*.dat` (legacy: `sources.dat`) + `wal.log` |
| **In-process cluster** | `cluster_manifest.bin` + `cluster.log` + per-shard `shard_<i>/segments/*.seg` + each manifest-selected source sidecar |

The frozen dict, vocabulary, and tag space are embedded **inside** the manifests, so they travel
with the copy automatically. **Replica directories are not copied** — a cluster rebuilds replicas
from the primaries on open. Orphan segment or source-generation files left by an interrupted
pre-commit attempt are skipped.

## Why not just `cp -r` the data directory?

A live `cp -r` is **unsafe**. A concurrent flush/compaction commits a new manifest and then deletes
the now-superseded segment files; a copier that reads the manifest and then copies segments can race
that deletion and capture a manifest that references a file the copy missed — a corrupt backup.

`POST /_backup` avoids this by doing the copy **inside the engine, under its write lock**: no
compaction can run during the snapshot, so the manifest and the files it names are always
consistent. The filesystem work runs on a blocking worker rather than an async HTTP runtime
thread. One backup is admitted per server; another call waits asynchronously without occupying a
blocking worker. Once admitted, its permit lives with the blocking work, so a client disconnect
cannot admit a queue of detached backups. A separate completion supervisor records the eventual
success or failure even after that disconnect. The whole backup is staged in a uniquely-owned sibling
`<dest>.backup.tmp.<pid>.<sequence>` dir, verified, and atomically promoted without replacing an
entry that already occupies `dest`. A crash mid-backup never leaves a half-written `dest`; it can
leave that operation's uniquely named staging dir, which may be pruned once no backup is running.
If the target platform or filesystem cannot perform atomic no-replace rename, promotion fails
closed; it never falls back to a racy check-then-rename.

## Taking a backup (REST)

```sh
# durable single-node or in-process-cluster coordinator — same call
curl -fsS -XPOST http://<host>:9200/_backup \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $RR_AUTH_TOKEN" \   # if auth is enabled (ADR-062)
  -d '{"dest": "/backups/rr-2026-06-19"}'
# single-node:
# → {"took":12,"took_ms":12.7,"acknowledged":true,"dest":"/backups/rr-2026-06-19"}
# in-process cluster also returns the checkpoint generation:
# → {"took":18,"took_ms":18.4,"acknowledged":true,"dest":"...","epoch":7}
```

Notes:
- The route is strict and synchronous: POST only, no query parameters, exactly one JSON `dest`
  field, and a 64 KiB body limit. Missing/wrong JSON content type is 415; malformed, duplicate,
  unknown, empty/whitespace, or NUL-containing input is 400; oversize input is 413. A successful
  response means copy, verification, and atomic promotion all finished.
- Each server admits one backup at a time. Excess calls wait asynchronously for that slot and can
  be cancelled safely while waiting; an admitted backup finishes and its outcome is logged and
  counted even if its client disconnects.
- `dest` is a path **on the server's filesystem**, not the client's. Mount your backup volume into
  the container and point `dest` there.
- `dest` must **not already contain any filesystem entry** (a 400 otherwise, including a dangling
  symlink) — never overwrite a prior backup in place.
- An in-memory engine/cluster (no `--data-dir`) returns 400; a persistence-degraded engine returns
  503 (its on-disk state is known-incomplete — investigate before backing up).
- The in-process-cluster call checkpoints first, so it doubles as a durability commit point.
- A remote cluster's coordinator is stateless and returns 400. Use the per-volume procedure below;
  the coordinator cannot create a cross-shard snapshot.

### Why there is no `/_snapshot` alias

Elasticsearch and OpenSearch create `/_snapshot/{repository}/{snapshot}` only after a repository
has been registered, and their default request starts asynchronous work that is inspected through
snapshot/task status APIs
([Elasticsearch create snapshot](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create),
[OpenSearch create snapshot](https://docs.opensearch.org/latest/api-reference/snapshots/create-snapshot/)).
Reverse Rusty has no repository registry or snapshot-task lifecycle: it accepts one fresh
server-side path and waits for a verified full copy. Mapping repository and snapshot path segments
onto an arbitrary filesystem path, or acknowledging background work without a status surface,
would be false compatibility. `/_backup` therefore remains an explicitly native operation.

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

## Filesystem snapshots and remote deployments

The built-in `POST /_backup` pauses **writes** (not reads) for the duration of the file copy — a
multi-second stall on a very large corpus. For a backup that never pauses writes, snapshot a
checkpointed **local-mode** directory at the filesystem layer:

1. `POST /_checkpoint` (in-process cluster) or `POST /_flush` (single-node) to commit a consistent
   on-disk state.
2. Take an atomic copy-on-write snapshot of the `data_dir` volume (ZFS/LVM snapshot, AWS EBS
   snapshot, GCP disk snapshot, etc.) — instantaneous, no engine involvement.
3. Copy the snapshot to backup storage at your leisure (the snapshot is frozen, immune to the live
   engine's later compactions).
4. Restore = mount/copy the snapshot's contents into a `data_dir` and start an instance on it.

This is the recommended operational path where a write stall is unacceptable and CoW storage is
available.

For a **remote** cluster, the coordinator has no `data_dir` and no cross-shard snapshot barrier.
Pause ingest, wait for in-flight writes to finish, snapshot every shard and control-plane volume as
one named set, then resume ingest. Each volume is individually crash-consistent; quiescing is what
makes the set globally consistent. `POST /_checkpoint` on the stateless coordinator does not flush
remote shards, and `POST /_backup` returns 400. Restore the complete volume set into the same logical
topology; see [`disaster-recovery.md`](disaster-recovery.md).

## Scheduling

For local durable modes, `POST /_backup` is a one-shot call to a fresh `dest`; retry with a new
date-stamped destination because an existing path is deliberately refused. Drive it from
cron/k8s-CronJob, then prune old copies with your normal retention tooling. Each backup is fully
self-contained (no dependency on prior backups). Remote deployments schedule storage-provider
snapshots of the quiesced volume set instead.

## Rehearsal — prove you can restore

A backup you have never restored is a hope, not a plan. The engine's restore path is
CI-proven (`local-smoke.sh` restores a backup on every PR; the durability oracles diff restored
vs source), so what a rehearsal actually tests is **your** side: the snapshots exist, they are
complete, your runbook works, and you know how long a restore takes. Quarterly, or after any
storage/topology change:

1. **Pick the latest real backup** — the `POST /_backup` dir (local modes) or the newest
   quiesced per-volume snapshot set (remote — see the zero-write-stall procedure above). Use the
   real artifact, not a fresh one taken for the drill.
2. **Verify integrity first:** run `storage::verify_backup(dir)` / `verify_cluster_backup(dir)`
   on the copy (a tiny Rust snippet, or keep a copy of the backup around and rely on the verify
   that ran at `POST /_backup` time + checksums from your archiver). Bit-rot found *now* is a
   snooze; found during a real recovery it is the incident.
3. **Restore into a sandbox** — a fresh dir + port on any machine with the released image:
   `server --data-dir <copy> --port 9201` (single-node) or the full topology bring-up against
   restored volumes (remote; [`disaster-recovery.md` §3.3](disaster-recovery.md)). **Start the
   clock here.**
4. **Verify content, not just liveness:**
   - `GET :9201/_stats` — the query count equals the count you recorded when the backup was
     taken (record it next to the backup; the smoke does exactly this).
   - **Golden-titles probe:** keep a small file of representative titles WITH their expected
     matched ids (regenerate it whenever the corpus changes materially); percolate each against
     the restored instance and diff:

     ```sh
     # golden-titles.tsv: <raw title>\t<expected sorted id array>, e.g.
     #   1994 fleer jordan psa 10\t[12,845]
     while IFS=$'\t' read -r title expected; do
       body=$(jq -Rn --arg t "$title" '{query:{percolate:{document:{title:$t}}}}')
       got=$(curl -fsS -XPOST http://localhost:9201/_search \
             -H 'content-type: application/json' -d "$body" \
             | jq -c '[.hits.hits[]._id|tonumber]|sort')
       [ "$got" = "$expected" ] || echo "MISMATCH: $title got=$got want=$expected"
     done < golden-titles.tsv
     ```
5. **Stop the clock and record it.** Copy time + reopen time + verification time = your measured
   **RTO evidence** — the number [`disaster-recovery.md` §1](disaster-recovery.md) tells you to
   plug into its table. Record the backup's age at restore time too: that is your demonstrated
   RPO.
6. **Tear the sandbox down** — and fix whatever snagged (a missing mount, a stale golden file, a
   runbook step that assumed a host that no longer exists). The snags are the yield.

## Not covered in v1 (see ADR-079)

- **Online (no-quiesce) backup** that allows concurrent writes during the copy — the
  retention-lease + translog-tail machinery peer recovery uses is the documented follow-on.
- **Incremental/differential** backups — every backup is a full copy.
- **Streaming to an object store** (S3/GCS) directly — `dest` is a local filesystem path; pair with
  an FS-snapshot + your own uploader, or copy the backup dir up afterward.
- A **`POST /_restore`** endpoint — restore is operator-driven (`--data-dir`), by design.
