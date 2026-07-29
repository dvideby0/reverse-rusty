# `POST /_backup` — Create a durable snapshot

> [Ingest & lifecycle APIs](../ingest.md) · [REST API hub](../../api.md)

This strict native endpoint synchronously snapshots a durable single engine or in-process cluster
to a fresh server-side directory:

```json
{"dest": "/srv/reverse-rusty/backups/2026-07-29"}
```

The destination must not already exist. One backup is admitted per server; excess calls wait
asynchronously for that admission rather than blocking the HTTP runtime. A stateless remote
coordinator returns 400 because durability belongs to the shard-node volumes.

For an in-process cluster, backup first checkpoints the cluster, then copies and verifies the
coordinator manifest, per-shard segments, source stores, and coordinator log. The response includes
the committed checkpoint `epoch`. A single durable engine snapshots its committed local state
under the same fresh-directory rule.

This is a privileged operator API: `dest` is an arbitrary server-side path written with the
server process's filesystem permissions. It belongs to the default-deny bearer-auth set and must
never be exposed unauthenticated on a non-loopback bind.

The full safety guarantee, filesystem-snapshot alternative, restore procedure, and rehearsal drill
are canonical in the [backup and restore runbook](../../../operations/backup-restore.md). Remote
clusters use quiesced node-volume snapshots. See ADR-079 and ADR-139 for rationale and the strict
REST contract.
