# `POST /_checkpoint` — Cluster durability commit

> [Ingest & lifecycle APIs](../ingest.md) · [REST API hub](../../api.md)

`/_checkpoint` is the strict native commit point for a durable in-process cluster (ADR-161):

```bash
curl -X POST localhost:9200/_checkpoint
```

```json
{
  "took": 4,
  "took_ms": 4.37,
  "acknowledged": true,
  "durable": true,
  "epoch": 7,
  "shards_checkpointed": 3
}
```

The commit seals or reseals every logical shard position, atomically publishes the coordinator
manifest and its source/segment registry, advances `epoch`, and only then allows the committed
mutation-log prefix and orphaned segment files to be reclaimed. A shard persistence or manifest
failure returns a typed error (a durability failure is 503 `durability_unavailable`) without
advancing or acknowledging a new epoch.

An in-memory cluster or stateless remote coordinator has no coordinator `data_dir`. The request is
still an acknowledged process-local maintenance boundary, but the response makes the absence of a
durability commit explicit:

```json
{
  "took": 0,
  "took_ms": 0.08,
  "acknowledged": true,
  "durable": false,
  "epoch": 0,
  "shards_checkpointed": 0,
  "message": "no durable checkpoint was created because the coordinator has no data directory"
}
```

In particular, `acknowledged: true` means the requested operation completed; clients must inspect
`durable` before treating it as a recovery point. The stateless coordinator does not flush remote
shard nodes or create a cross-shard snapshot barrier. Quiesce writes and snapshot every shard and
control-plane volume as one set for a consistent remote backup.

The request accepts only `POST`, no query parameters, and an empty body. Route bodies are capped at
64 KiB and must complete within 250 ms; other methods return 405 with `Allow: POST`. Responses are
`Cache-Control: no-store`. Checkpoint and backup share one durability-work slot and the cluster
writer boundary, so either can wait for the other while existing immutable read snapshots continue
serving. The disk work and lock wait run off the async runtime. Once admitted, a client disconnect
does not cancel the checkpoint; completion is still logged and counted.

This endpoint is not an Elasticsearch/OpenSearch flush alias. Their familiar flush contract is
already implemented by `GET`/`POST /_flush`; it seals memtables but does not commit the coordinator
manifest or advance the cluster mutation-log checkpoint.
