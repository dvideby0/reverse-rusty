# Ingest & lifecycle — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `POST /_bulk` — Bulk ingest

NDJSON format, compatible with Elasticsearch's `_bulk` API:

```bash
curl -X POST localhost:9200/_bulk \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- <<'EOF'
{"index": {"_id": 1}}
{"query": "(laptop,notebook) 16gb -refurbished"}
{"index": {"_id": 2}}
{"query": "vintage leather jacket -(replica,faux)"}
{"index": {"_id": 3}}
{"query": "\"running shoes\" (nike,adidas) -used"}
EOF
```

```json
{
  "took_ms": 1.23,
  "errors": false,
  "items": [
    {"index": {"_id": 1, "status": 201, "error": null}},
    {"index": {"_id": 2, "status": 201, "error": null}},
    {"index": {"_id": 3, "status": 201, "error": null}}
  ]
}
```

If any query fails, `errors` is `true` and that item gets a `400` status with the parse error message;
successfully ingested queries in the same batch are unaffected (per-item status — ADR-018).

Each source line may also carry [metadata tags](documents.md#per-query-metadata-tags-adr-049) — a `tags` object or
ES-style sibling fields — exactly as `PUT /_doc` does, e.g. `{"query": "...", "category": "electronics"}`.

## `POST /_flush` — Flush memtable

Flush the in-memory memtable to an immutable on-disk segment:

```bash
curl -X POST localhost:9200/_flush
```

```json
{
  "acknowledged": true,
  "total_queries": 3,
  "base_segments": 1
}
```

If the segment can't be durably written (disk failure), the flush falls back to an in-memory
segment so reads keep matching, but it is **not** durable: the response is
**`503 Service Unavailable`** with `"acknowledged": false`, and `persistence_healthy` flips false
(see `GET /_health`). The data is retained in the WAL and recovers on restart — `acknowledged: true`
is never returned for a write that isn't on disk (ADR-051).

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

