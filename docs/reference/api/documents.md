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
{"_id": 1, "result": "rejected", "error": "query has no anchorable feature (cost class D)"}
```

### Per-query metadata tags (ADR-049)

A stored query may carry **structured tags** — `(key, value)` metadata used to *narrow* percolated
results later (see [filtered percolation](percolate.md#filtered-percolation-adr-049) below). Provide them either as
a canonical `tags` object or, Elasticsearch-style, as sibling fields of `query` (anything that isn't
`query`/`version`/`tags`); a value may be a string or an array of strings. The two forms are merged.

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

## `GET /_doc/{id}` — Retrieve a query

```bash
curl localhost:9200/_doc/1
```

```json
{"_id": 1, "found": true, "_source": {"query": "dell laptop"}}
```

If the query ID doesn't exist:

```json
{"_id": 1, "found": false}
```

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

