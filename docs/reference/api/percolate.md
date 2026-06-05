# Percolate — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `POST /_search` — Percolate titles

Match a single title against all stored queries:

```bash
curl -X POST localhost:9200/_search \
  -H 'Content-Type: application/json' \
  -d '{"document": {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"}}'
```

```json
{
  "took_ms": 0.42,
  "hits": {
    "total": 1,
    "hits": [{"_id": 1, "_source": {"query": "dell laptop"}}]
  }
}
```

Optional request fields:

| Field | Default | Description |
|---|---|---|
| `timeout_ms` | 30000 | Per-request **response** timeout in ms; returns 408 on expiry. In-flight matching is not cancelled — see note. |
| `size` | 1000 | Maximum number of hits to return (per slot in multi-doc mode) |
| `from` | 0 | Offset into the result set for pagination |
| `rank` | – | Optional ranking block (ADR-059) — order hits by a priority tag and/or request boosts before `from`/`size`. See [Ranking](#ranking-adr-059). |
| `include_source` | true | Include original query text in each hit |

`total` always reflects the full match count; `hits` is the paginated window. Set
`include_source: false` to skip query text lookup for faster responses.

> **`timeout_ms` is a response deadline, not a compute budget.** On expiry the request
> returns `408`, but the matching work already dispatched to the blocking/Rayon pool
> runs to completion in the background — it is not interrupted (there is no
> cooperative cancellation on the match path, which is kept branch-predictable and
> allocation-free by design). So `timeout_ms` bounds *when the client gets a
> response*, not how long the server spends. Under a flood of slow titles with a short
> timeout, abandoned work can still occupy worker threads; bound load with a modest
> request-concurrency limit rather than relying on `timeout_ms` to shed CPU. The same
> applies to `/_mpercolate`.

Match multiple titles in a single request:

```bash
curl -X POST localhost:9200/_search \
  -H 'Content-Type: application/json' \
  -d '{
    "documents": [
      {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"},
      {"title": "Vintage Brown Leather Bomber Jacket Size L"}
    ],
    "timeout_ms": 5000
  }'
```

```json
{
  "took_ms": 0.87,
  "hits": {
    "total": 2,
    "hits": [
      {"_id": 1, "_source": {"query": "dell laptop"}},
      {"_id": 2, "_source": {"query": "leather jacket"}}
    ]
  },
  "slots": [
    {
      "slot": 0,
      "total": 1,
      "hits": [{"_id": 1, "_source": {"query": "dell laptop"}}],
      "stats": {
        "unique_candidates": 15,
        "postings_scanned": 47,
        "matches": 1,
        "probes_attempted": 28,
        "probes_skipped": 12
      }
    },
    {
      "slot": 1,
      "total": 1,
      "hits": [{"_id": 2, "_source": {"query": "leather jacket"}}],
      "stats": {
        "unique_candidates": 9,
        "postings_scanned": 22,
        "matches": 1,
        "probes_attempted": 18,
        "probes_skipped": 8
      }
    }
  ]
}
```

The `stats` object per slot shows how much work the engine did: how many candidates were retrieved
from the index, how many posting lists were scanned, how many bloom-filter probes were skipped, and
how many candidates survived to become confirmed matches. The search body also accepts `explain` and
`profile` options for per-query match tracing (see [`../design/matching.md`](../../design/matching.md) §6).

### Filtered percolation (ADR-049)

The dominant production read pattern is *"percolate, then narrow to one category."* Attach a tag filter
to a percolate request to keep only the matches whose stored query carries the requested
[metadata tags](documents.md#per-query-metadata-tags-adr-049). The filter is a **conjunction across keys** (AND) of
**value sets** (OR within a key); it is applied in the hot-path verify stage and can only *remove*
matches, never add or drop a wanted one. A filter value never seen at ingest matches nothing (the safe
`terms` semantics). Two equivalent shapes are accepted:

**Native** — a `filter` block alongside `document`/`documents`:

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "document": {"title": "Dell XPS 15 Laptop 16GB RAM New"},
  "filter": {"category": ["electronics", "computers"], "status": "active"}
}'
```

**Elasticsearch `bool`/`terms` percolate envelope** — for drop-in compatibility with existing percolate
clients. The document(s) come from `query.bool.must.percolate` and the filter from `query.bool.filter`
(an array of `terms`/`term` clauses). A bare `query.percolate` (no `bool`) works for the unfiltered case.

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must": {"percolate": {"field": "query", "document": {"title": "Dell XPS 15 Laptop New"}}},
      "filter": [
        {"terms": {"category": ["electronics", "computers"]}},
        {"term":  {"status": "active"}}
      ]
    }
  }
}'
```

Only the `percolate` + `bool.filter(terms/term)` subset is supported; any other query clause (e.g.
`match`, `range`) returns **400** rather than silently widening the result set. `/_mpercolate` accepts the
same `filter` block and ES envelope (applied to every document in the batch).

### Ranking (ADR-059)

By default hits come back in the engine's order (a boolean candidate set — the engine is a recall-first
matcher, not a ranker). Attach an optional `rank` block to **order** the hits before pagination. Ranking
is a pure post-match step: it only reorders + paginates the already-final set — it never adds or drops a
match. A `rank` block has two optional parts:

- **`priority_key`** — the name of a [tag](documents.md#per-query-metadata-tags-adr-049) whose **numeric
  value** is the query's base priority (a query tagged `priority=50` scores 50; a non-numeric or absent
  value scores 0).
- **`boosts`** — a list of `{key, value, boost}` entries; a query scores `+boost` for each `(key, value)`
  tag it carries.

The score is **additive** — `score = Σ matched boosts + priority` — and hits are ordered by `score`
descending, ties broken by ascending `_id` (a stable, repeatable order for pagination). Each hit then
carries a `_score` field (present only when a `rank` block was supplied). Want a boost to always
outrank priority? Choose boost magnitudes above your priority range.

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "document": {"title": "2020 Topps Chrome Update"},
  "filter": {"category": "cards"},
  "size": 20,
  "rank": {
    "priority_key": "priority",
    "boosts": [{"key": "tier", "value": "gold", "boost": 100}]
  }
}'
```

```json
{
  "took_ms": 0.31,
  "hits": {
    "total": 3,
    "hits": [
      {"_id": 1, "_score": 110, "_source": {"query": "topps chrome"}},
      {"_id": 3, "_score": 100, "_source": {"query": "topps chrome auto"}},
      {"_id": 2, "_score": 50,  "_source": {"query": "topps chrome rookie"}}
    ]
  }
}
```

`rank` works on `/_search` (single + multi-document) and `/_mpercolate` (each document's hits ranked
independently), composes with `filter`, and is **opt-in**: with no `rank` block the response is
byte-identical to before — no `_score` field, engine order preserved. Cluster (multi-shard) ranking is
not yet available; the REST endpoints rank against the single-node engine.

## `POST /_mpercolate` — Batch percolate (high throughput)

The throughput counterpart to `/_search`. Percolates a **batch** of documents in one request and
evaluates the broad lane **once per batch, columnar** (ADR-026) instead of once per document — so a
hot broad anchor's huge posting is scanned once for the whole batch, not re-scanned per document.
Returns an Elasticsearch `_msearch`-style `responses[]` envelope: one entry per input document, in
submission order (`responses[i]` corresponds to `documents[i]`).

```bash
curl -X POST localhost:9200/_mpercolate \
  -H 'Content-Type: application/json' \
  -d '{
    "documents": [
      {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"},
      {"title": "Vintage Brown Leather Bomber Jacket Size L"},
      {"title": "Generic unmatched listing"}
    ],
    "include_broad": true,
    "profile": true
  }'
```

```json
{
  "took_ms": 0.91,
  "responses": [
    {"hits": {"total": 1, "hits": [{"_id": 1, "_source": {"query": "dell laptop"}}]}},
    {"hits": {"total": 1, "hits": [{"_id": 2, "_source": {"query": "leather jacket"}}]}},
    {"hits": {"total": 0, "hits": []}}
  ],
  "broad": {
    "strategy": "columnar",
    "batch_size": 256,
    "broad_batches": 1,
    "broad_postings_scanned": 0,
    "broad_queries_evaluated": 0,
    "broad_candidates": 0,
    "total_matches": 2
  }
}
```

Optional request fields:

| Field | Default | Description |
|---|---|---|
| `include_broad` | server default (`--include-broad`) | Per-request override: evaluate class-C (broad) queries for this batch |
| `include_source` | true | Include original query text in each hit |
| `size` | 1000 | Maximum hits per document |
| `from` | 0 | Per-document offset into each document's hits for pagination |
| `rank` | – | Optional ranking block (ADR-059), applied per document — see [Ranking](#ranking-adr-059) |
| `timeout_ms` | 30000 | Per-request **response** timeout in ms; returns 408 on expiry. In-flight matching is not cancelled — see note. |
| `profile` | false | Include the top-level `broad` summary |

Each per-document result is **byte-identical** to calling `/_search` with that single title (for the
same `size`/`from`/`rank`) — batching is a performance change only, never a semantic one (proven by
`tests/broad_batch.rs`). The optional top-level `broad` summary surfaces the columnar evaluator's
amortization: as the batch grows, `broad_postings_scanned` rises far slower than `broad_candidates`
(each huge posting is consulted once per batch). An empty `documents` array is a valid no-op (`200` with
`responses: []`); a missing `documents` field is a `400`.

**When to use which.** Reach for `/_mpercolate` for high-throughput batch/streaming percolation,
especially with broad queries enabled. Both endpoints support `size`/`from` pagination and the `rank`
block; reach for `/_search` when you want the rich, per-document observability it alone provides —
per-slot `stats`, `explain`, and `profile`. Because the broad lane is amortized per batch, `/_mpercolate`
deliberately does not produce per-document candidate/posting stats — only the batch-level `broad` summary.

