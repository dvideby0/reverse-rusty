# Documents — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `PUT /_doc/{id}` — Register or replace a query

```bash
curl -X PUT localhost:9200/_doc/1 \
  -H 'Content-Type: application/json' \
  -d '{"query": "(laptop,notebook) 16gb -refurbished"}'
```

```json
{"_id": 1, "result": "created", "error": null}
```

**Replace-by-id (ES `index` semantics, ADR-067).** A re-PUT of an existing id is an **atomic
upsert**: the new version is inserted and every prior live copy is tombstoned in one critical section
and one snapshot publish — the old semantics stop matching exactly when the new ones start (no window
where the id matches under both, and no no-match window like the old DELETE-then-PUT recipe). A fresh
id answers **201** with `"result": "created"`; a replacement answers **200** with
`"result": "updated"`:

```json
{"_id": 1, "result": "updated", "error": null}
```

If the query fails to parse or has no anchorable features (cost class D), the response includes the
error — and the **prior version stays live and matchable** (a failed replace never deletes):

```json
{"_id": 1, "result": "rejected", "error": "query has no anchorable feature (cost class D); negation-only queries are stored as always-candidates when the accept_class_d setting is enabled"}
```

With the [`accept_class_d` setting](settings.md) on (ADR-068), a **negation-only** query (only `-...`
clauses) is accepted instead and stored as a broad-lane **always-candidate**: it matches every title
bearing none of its forbidden terms, and — like every broad-lane query — only on requests that include
the broad lane. A query with no positive *and* no forbidden terms (effectively empty) is rejected
regardless.

### Per-query metadata tags (ADR-049)

A stored query may carry **structured tags** — `(key, value)` metadata used to *narrow* percolated
results later (see [filtered percolation](percolate.md#filtered-percolation-adr-049) below). Provide them either as
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
crash recovery). They **never** affect *which* queries a title matches — only the optional filter below
can narrow an already-correct result set, so they cannot introduce a false negative.

### Typed priority (ADR-108)

Local bounded ranking has one fixed signed `i64` field. Supply it separately from permissive tags:

```bash
curl -X PUT localhost:9200/_doc/1 -H 'Content-Type: application/json' \
  -d '{"query":"topps chrome","rank_fields":{"priority":50}}'
```

`rank_fields.priority` accepts an integer JSON value or a signed decimal string fitting `i64`.
Floats, booleans, nulls, arrays/objects, overflow, and unknown rank fields return a structured 400
(`invalid_rank_value` or `unsupported_rank_field`). The server mirrors the typed value into the
canonical `priority` tag for compatibility ranking and rollback. If `tags.priority` is also supplied,
there must be exactly one numerically-equal value; a conflict is rejected.

Without `rank_fields`, existing `tags.priority` behavior is unchanged: a numeric legacy value lowers
into the typed column, while a malformed value remains legal and scores zero. The same rules apply per
item in `POST /_bulk`.

## `GET /_doc/{id}` — Retrieve a query

Reference shapes: [Elasticsearch get document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get)
and [OpenSearch get document](https://docs.opensearch.org/latest/api-reference/document-apis/get-documents/).

```bash
curl localhost:9200/_doc/1
```

```json
{
  "_index": "queries",
  "_id": 1,
  "_version": 7,
  "found": true,
  "_source": {
    "query": "dell laptop",
    "tags": {
      "category": "electronics",
      "status": ["active", "reviewed"]
    }
  }
}
```

The response follows the Elasticsearch/OpenSearch get-document envelope while retaining Reverse
Rusty's numeric `u64` identity: `_index` is the implicit `"queries"` index, `_version` is the
version supplied on the successful write (default `1`), and `_source.query` is the original DSL.
Tags are read back in a canonical `tags` object: scalar-coerced values are strings, one value is a
string, and multiple values are a sorted array. This canonical form intentionally does not preserve
whether a tag arrived under `tags` or as an ES-style sibling field.

Reads are real-time: every acknowledged write publishes a new snapshot before returning. Reverse
Rusty has no REST-visible equivalent of Elasticsearch's `_seq_no` or `_primary_term`, so it omits
those fields instead of inventing concurrency tokens.

The common ES/OS source projection parameters are supported (comma-separated values and `*`/`?`
wildcards):

| Query parameter | Behavior |
|---|---|
| `_source=false` | Omit `_source` while retaining identity, version, and `found` |
| `_source_includes=query,tags.category` | Return only matching source fields |
| `_source_excludes=tags.internal_*` | Remove matching source fields |

The singular aliases `_source_include` and `_source_exclude` are accepted too. Unsupported query
parameters fail with **400** rather than being silently ignored.

Use `HEAD /_doc/{id}` for a bodyless existence check. It returns **200** when the query exists and
**404** otherwise.

If the query ID doesn't exist, `GET` returns **404** with:

```json
{"_index": "queries", "_id": 1, "found": false}
```

Both single-node and in-process coordinator modes provide this contract. A coordinator backed by
remote shards returns **501** because the v1 gRPC transport does not expose source lookup; it never
misreports an unavailable lookup as `found: false`.

If the match index has a live row but its source sidecar is missing, the request fails with
`source_unavailable` (**500** in single-node mode, **502** through a local coordinator) rather than
misreporting the live document as a 404.

`sources.dat` v2 now appends a backward-readable metadata footer while leaving the original query
index/blob intact, so query-only hit enrichment remains lazy and old binaries still read query text.
v1 and original-v2 files open automatically. Dense legacy tags are reconstructed from the persisted
tag dictionary; the rare pre-footer document carrying only post-freeze synthetic tags cannot be
reversed, so its response includes
`"_source_metadata":{"complete":false,...}` until the document is re-PUT. This is explicit rather
than silently presenting incomplete metadata as the original source.

## `DELETE /_doc/{id}` — Remove a query

```bash
curl -X DELETE localhost:9200/_doc/1
```

```json
{"_id": 1, "result": "deleted", "deleted_count": 1}
```

If the query ID doesn't exist (or was already deleted):

```json
{"_id": 1, "result": "not_found"}
```
