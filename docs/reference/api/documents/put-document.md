# `PUT /_doc/{id}` — Register or replace a query

> [Documents APIs](../documents.md) · [REST API hub](../../api.md)

Reference shapes: [Elasticsearch index document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-index)
and [OpenSearch index document](https://docs.opensearch.org/latest/api-reference/document-apis/index-document/).

```bash
curl -X PUT localhost:9200/_doc/1 \
  -H 'Content-Type: application/json' \
  -d '{"query": "(laptop,notebook) 16gb -refurbished"}'
```

```json
{"_index": "queries", "_id": 1, "_version": 1, "result": "created"}
```

**Replace-by-id (ES `index` semantics, ADR-067).** A re-PUT of an existing id is an **atomic
upsert**: the new version is inserted and every prior live copy is tombstoned in one critical section
and one snapshot publish — the old semantics stop matching exactly when the new ones start (no window
where the id matches under both, and no no-match window like the old DELETE-then-PUT recipe). A fresh
id answers **201** with `"result": "created"`; a replacement answers **200** with
`"result": "updated"`:

```json
{"_index": "queries", "_id": 1, "_version": 1, "result": "updated"}
```

Successful responses carry the applicable ES/OpenSearch fields: the implicit index is always
`"queries"`, `_id` remains Reverse Rusty's numeric `u64`, and `_version` is the display version
stored with this write. Reverse Rusty cannot honestly report Elasticsearch/OpenSearch replication
counts, sequence numbers, or primary terms, so it omits `_shards`, `_seq_no`, and `_primary_term`
instead of inventing values.

### Index-operation parameters

The two compatible operation controls are strict and identical in single-node and coordinator
modes:

| Query parameter | Behavior |
|---|---|
| `op_type=index` | Default. Atomically create or replace the id. |
| `op_type=create` | Atomically create only when the id is absent; an existing id returns **409** `version_conflict_engine_exception` and remains unchanged. |
| `refresh=false`, `true`, or `wait_for` | Accepted. Reverse Rusty publishes every fully applied write before replying, so all three receive the stronger immediate-search-visibility guarantee. |

Any other query parameter—or any other value for these parameters—returns a structured **400**
`illegal_argument_exception` before mutation. This is deliberate: silently ignoring `routing`,
`pipeline`, `if_seq_no`, `if_primary_term`, `version`, or `version_type` could make an ES/OpenSearch
client believe a write constraint was honored when it was not. A remote coordinator whose
pre-existing shards cannot be enumerated also fails `op_type=create` closed rather than guessing
that an id is absent; ordinary `op_type=index` remains available.

The JSON-body `version` field is Reverse Rusty application metadata (an unsigned 32-bit value,
default `1`) and is preserved verbatim in the successful response, `GET /_doc/{id}`, persistence,
and recovery. It is **not** Elasticsearch/OpenSearch internal versioning or optimistic concurrency:
it does not auto-increment, and repeated or lower values are legal. The ES/OS query-parameter
version and sequence-number controls are therefore rejected, not partially emulated.

If the query fails to parse or has no anchorable features (cost class D), the response includes the
error — and the **prior version stays live and matchable** (a failed replace never deletes):

```json
{"_index": "queries", "_id": 1, "result": "rejected", "error": "query has no anchorable feature (cost class D); negation-only queries are stored as always-candidates when the accept_class_d setting is enabled"}
```

With the [`accept_class_d` setting](../settings.md) on (ADR-068), a **negation-only** query (only `-...`
clauses) is accepted instead and stored as a broad-lane **always-candidate**: it matches every title
bearing none of its forbidden terms, and — like every broad-lane query — only on requests that include
the broad lane. A query with no positive *and* no forbidden terms (effectively empty) is rejected
regardless.

### Per-query metadata tags (ADR-049)

A stored query may carry **structured tags** — `(key, value)` metadata used to *narrow* percolated
results later (see
[filtered percolation](../percolate/search.md#filtered-percolation-adr-049)). Provide them either as
a canonical `tags` object or, Elasticsearch-style, as sibling fields of `query` (anything that isn't
`query`/`version`/`tags`/`rank_fields`). The two forms are merged.

A value may be a **string, number, bool, or an array of those** (ADR-073). Numbers and bools coerce
to their canonical JSON text — `7` → `"7"`, `true` → `"true"`, the ES keyword behavior — and the
filter side coerces with the **same rule**, so a category ingested as `7` is matched by a filter
sending `7` *or* `"7"` (note `7.0` coerces to `"7.0"`, a *different* tag, exactly as in ES). An
explicit `null` — top-level or as an array element — is the ES "no value" and contributes no tag.
Anything else (an object, a nested array, or a non-object `tags` field) is a loud **400**; in
`/_bulk` the rejection is per-item. Before ADR-073 such values were dropped *silently*, leaving the
query unreachable by any filter on that key. An **empty tag key** is also a loud 400: an empty
`priority_key` means "no priority term" (the gRPC wire cannot express it), so an empty-key tag
would be reachable by some ranking paths and not others.

```bash
# ES-style siblings:
curl -X PUT localhost:9200/_doc/1 -H 'Content-Type: application/json' \
  -d '{"query": "dell laptop", "category": "electronics", "status": "active"}'

# or the canonical `tags` object (equivalent):
curl -X PUT localhost:9200/_doc/1 -H 'Content-Type: application/json' \
  -d '{"query": "dell laptop", "tags": {"category": "electronics", "status": "active"}}'
```

Tags are interned to integers, stored as a hot-path SoA column, and persisted (they survive reopen and
crash recovery). They **never** affect *which* queries a title matches — only the optional
[percolation filter](../percolate/search.md#filtered-percolation-adr-049) can narrow an
already-correct result set, so they cannot introduce a false negative.

### Typed priority (ADR-108)

Local bounded ranking has one fixed signed `i64` field. Supply it separately from permissive tags:

```bash
curl -X PUT localhost:9200/_doc/1 -H 'Content-Type: application/json' \
  -d '{"query":"acme labs chrome","rank_fields":{"priority":50}}'
```

`rank_fields.priority` accepts an integer JSON value or a signed decimal string fitting `i64`.
Floats, booleans, nulls, arrays/objects, overflow, and unknown rank fields return a structured 400
(`invalid_rank_value` or `unsupported_rank_field`). The server mirrors the typed value into the
canonical `priority` tag for compatibility ranking and rollback. If `tags.priority` is also supplied,
there must be exactly one numerically-equal value; a conflict is rejected.

Without `rank_fields`, existing `tags.priority` behavior is unchanged: a numeric legacy value lowers
into the typed column, while a malformed value remains legal and scores zero. The same rules apply per
item in `POST /_bulk`.
