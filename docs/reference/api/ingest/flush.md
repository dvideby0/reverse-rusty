# `GET|POST /_flush` — Flush memtables

> [Ingest & lifecycle APIs](../ingest.md) · [REST API hub](../../api.md)

`/_flush` implements a strict ES/OpenSearch-familiar, indexless flush boundary for Reverse Rusty's
one implicit `queries` index (ADR-137). It seals the standalone memtable into an immutable base
segment, or asks every cluster shard position to do the same:

```bash
curl -X POST 'localhost:9200/_flush?wait_if_ongoing=true'
```

Standalone success retains the native corpus/segment totals and adds familiar timing and shard
results:

```json
{
  "took": 1,
  "took_ms": 1.24,
  "acknowledged": true,
  "_shards": {
    "total": 1,
    "successful": 1,
    "failed": 0
  },
  "total_queries": 3,
  "base_segments": 1
}
```

Both GET and POST have the same behavior. Because GET is still mutating maintenance, it requires the
configured bearer token just like POST. The request body must be empty. The supported query
parameters are:

- `force=false|true`: accepted. Reverse Rusty always runs its synchronous native memtable-seal
  boundary; forcing a clean memtable is an acknowledged no-op because there is no segment payload
  to materialize.
- `wait_if_ongoing=false|true`: defaults to `true`, which waits behind an earlier explicit flush.
  `false` returns 409 `flush_in_progress_exception` when another flush is active. Its admission lock
  is separate from the ordinary writer lock, so an unrelated document write is not mislabeled as a
  flush.

Unknown, duplicate, or malformed query values and a nonempty body return a structured 400 before
maintenance starts. Other methods return 405 with `Allow: GET, POST`; oversized bodies retain the
server's 413 response. Named-index paths, wildcards, aliases, unavailable-index controls, and
closed-index controls are not implemented.

If a standalone durable segment cannot be written, the engine publishes the readable in-memory
fallback but does not claim durability. The response is **`503 Service Unavailable`** with
`"acknowledged":false`, `_shards.successful:0`, and `_shards.failed:1`;
`persistence_healthy` flips false (see `GET /_health`). The WAL remains authoritative and recovers
the data on restart—`acknowledged:true` is never returned for a write that did not reach disk
(ADR-051).

Cluster success uses the same envelope and reports logical shard positions in `_shards`; it omits
the standalone-only `total_queries` and `base_segments`. A local or remote shard persistence/
transport failure fails the coordinator request loudly rather than returning a clean shard result.
A bare cluster flush seals shard memtables, but it does not truncate coordinator or per-shard
mutation tails. For a durable in-process cluster, use `POST /_checkpoint` for the full durability
commit that reseals tombstones, commits the coordinator manifest, and advances those tails. A
stateless remote coordinator has no durable coordinator checkpoint, so a whole-cluster recovery
point still requires a quiesced snapshot of every shard-node volume.
