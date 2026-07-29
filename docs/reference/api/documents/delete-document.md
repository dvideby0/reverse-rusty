# `DELETE /_doc/{id}` — Remove a query

> [Documents APIs](../documents.md) · [REST API hub](../../api.md)

Reference shapes: [Elasticsearch delete document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete)
and [OpenSearch delete document](https://docs.opensearch.org/latest/api-reference/document-apis/delete-document/).

```bash
curl -X DELETE localhost:9200/_doc/1
```

```json
{"_index": "queries", "_id": 1, "result": "deleted", "deleted_count": 1}
```

A successful delete returns **200** and is visible to every later match and point read before the
response is sent. `deleted_count` is a Reverse Rusty extension and counts the one logical document,
not historical segment rows, placement copies, or replicas; it is therefore always `1` when
`result` is `deleted`.

If the query ID doesn't exist (or was already deleted), the operation is state-idempotent and
returns **404**:

```json
{"_index": "queries", "_id": 1, "result": "not_found"}
```

The common refresh control is strict and identical in single-node and coordinator modes:

| Query parameter | Behavior |
|---|---|
| `refresh=false`, `true`, or `wait_for` | Accepted. Reverse Rusty publishes every completed delete before replying, so all three receive the stronger immediate-search-visibility guarantee. |

Any other query parameter—or any other `refresh` value—returns a structured **400**
`illegal_argument_exception` before mutation. In particular, `routing`, `timeout`,
`wait_for_active_shards`, `if_seq_no`, `if_primary_term`, `version`, and `version_type` are rejected
instead of silently pretending that their routing, availability, or concurrency guarantees were
honored.

The applicable ES/OpenSearch identity fields are `_index: "queries"` and Reverse Rusty's numeric
`_id`. `_version` is deliberately absent: ES/OpenSearch allocate a new internal version for the
delete tombstone, while Reverse Rusty's caller-supplied application version is removed with the
source and is neither incremented nor retained as a concurrency token. `_shards`, `_seq_no`, and
`_primary_term` are also absent because Reverse Rusty has no equivalent REST-visible acknowledgement
or optimistic-concurrency state.

Deletes are log-first at each durable data owner. A standalone WAL failure rejects with **503**
`durability_unavailable` and does not apply the tombstone. Coordinator errors use the typed shard
error status. A remote multi-shard delete that only partly applies returns **503** with
`"result": "partial"` and an error string naming the applied and pending shards. Retry the
idempotent DELETE to drive every position again. While the same coordinator remains running,
`POST /_cluster/resync` can instead converge its in-memory repair queue; a stateless remote
coordinator restart does not preserve that queue.
