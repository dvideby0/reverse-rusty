# Ingest & lifecycle — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `POST /_bulk` — Bulk ingest

`/_bulk` implements a strict, ES/OpenSearch-familiar subset of the bulk API for Reverse Rusty's one
implicit `queries` index (ADR-136). Send alternating action and source records as NDJSON. The body
must be nonempty, contain no blank lines, and end with a newline:

```bash
curl -X POST localhost:9200/_bulk \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- <<'EOF'
{"index":{"_index":"queries","_id":"1"}}
{"query":"(laptop,notebook) 16gb -refurbished","version":7,"category":"electronics"}
{"create":{"_id":2}}
{"query":"vintage leather jacket -(replica,faux)","rank_fields":{"priority":25}}
EOF
```

```json
{
  "took": 1,
  "took_ms": 1.23,
  "errors": false,
  "items": [
    {
      "index": {
        "_index": "queries",
        "_id": 1,
        "_version": 7,
        "result": "created",
        "status": 201
      }
    },
    {
      "create": {
        "_index": "queries",
        "_id": 2,
        "_version": 1,
        "result": "created",
        "status": 201
      }
    }
  ]
}
```

The supported actions are:

- `index`: create a missing ID or atomically replace its currently live query. It returns 201
  `created` or 200 `updated`.
- `create`: insert only when the ID is absent. An existing ID returns a per-item 409
  `version_conflict_engine_exception`.

Actions execute in request order. Each action object must contain exactly one operation. Its
metadata accepts required `_id`, optional `_index: "queries"`, and false `require_alias` or
`_require_alias`. `_id` may be an unsigned 64-bit JSON integer or decimal string; responses use the
numeric value. Omitting `_index` selects the implicit `queries` index.

Every source line requires string `query`. It may also carry the same unsigned application
`version` (default 1), [metadata tags](documents.md#per-query-metadata-tags-adr-049), ES-style scalar
tag siblings, and `rank_fields.priority` accepted by `PUT /_doc`. `version` is stored display
metadata; it is not ES/OpenSearch internal versioning or an optimistic-concurrency control.

The only query parameters are:

- `refresh=false|true|wait_for`: all are accepted. Reverse Rusty publishes every successful write
  before responding, so each receives immediate visibility.
- `require_alias=false`: accepted for generic clients. `true` is rejected because Reverse Rusty has
  no index aliases.

`application/x-ndjson` is preferred. `application/json` is retained as a compatibility allowance;
other or missing media types return 415. The server-wide 100 MiB body limit applies.

The entire action structure is checked before mutation. Malformed framing, an unknown operation or
action field, a foreign index, a required alias, a missing final newline, and an unknown query
parameter return one structured 400 response. Once action/source pairing is valid, malformed source
JSON, missing source fields, invalid DSL, class-D rejection, create conflict, and persistence
failure are reported in their original item slot. A valid batch returns HTTP 200 and sets `errors`
when any item failed:

```json
{
  "took": 0,
  "took_ms": 0.41,
  "errors": true,
  "items": [
    {
      "create": {
        "_index": "queries",
        "_id": 1,
        "status": 409,
        "error": {
          "type": "version_conflict_engine_exception",
          "reason": "document 1 already exists; `create` requires a missing id"
        }
      }
    }
  ]
}
```

Standalone fresh, unique, default-version IDs retain the direct immutable-segment bulk-build path:
valid entries compile into one segment and commit atomically. A repeated or existing ID, or a source
version other than 1, uses ordered WAL-backed live writes so `index` remains a true replacement and
`create` can conflict. A direct segment commit failure rejects the whole request with 503; an
ordered WAL failure is a per-item 503 after any earlier successful items. Cluster mode uses its
ordered coordinator-log upsert/create path and reports a durably logged partial write as an
error-bearing `partial` item pending repair.

`update`, `delete`, automatic IDs, arbitrary indices, routing, pipelines, scripts, aliases,
sequence-number/primary-term controls, ES/OpenSearch version controls, and shard-wait controls are
not implemented. Supplying them fails rather than silently approximating their semantics. Use
`DELETE /_doc/{id}` for deletion.

## `GET|POST /_flush` — Flush memtables

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

## `POST /_compact` — Force compaction

Trigger segment compaction to merge segments and reclaim tombstones:

```bash
curl -X POST localhost:9200/_compact
```

When compaction runs:

```json
{
  "acknowledged": true,
  "segments_merged": 2,
  "entries_before": 150,
  "entries_after": 142,
  "tombstones_reclaimed": 8
}
```

When no compaction is needed:

```json
{
  "acknowledged": true,
  "message": "no compaction needed"
}
```

If the engine's persistence is degraded — a compaction that couldn't durably commit was rolled
back, or an earlier durable write failed — `/_compact` returns **`503 Service Unavailable`** with
`"acknowledged": false` and `"message": "persistence degraded; compaction not durably acknowledged"`.
A failed compaction always rolls back to its source segments, so it never loses data (ADR-051).

`/_compact` is single-node only. Cluster mode returns 501; each shard engine runs its own configured
flush/compaction policy.
