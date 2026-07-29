# `GET|POST /_search` — Percolate titles

> [Percolation & delivery APIs](../percolate.md) · [REST API hub](../../api.md)

Match a single title against all stored queries. Both methods accept the same JSON body; `POST` is
usually friendlier to proxies, while `GET` matches the Elasticsearch/OpenSearch search-method
surface.

```bash
curl -X POST localhost:9200/_search \
  -H 'Content-Type: application/json' \
  -d '{"document": {"title": "Dell XPS 15 Laptop 16GB RAM 512GB SSD New"}}'
```

```json
{
  "took": 0,
  "timed_out": false,
  "took_ms": 0.42,
  "hits": {
    "total": 1,
    "hits": [
      {"_index": "queries", "_id": 1, "_source": {"query": "dell laptop"}}
    ]
  }
}
```

The JSON body accepts these Reverse Rusty controls:

| Field | Default | Description |
|---|---|---|
| `timeout_ms` | 30000 | Native millisecond timeout alias; returns 408 on expiry. Mutually exclusive with `timeout`. |
| `timeout` | `30s` | ES/OS integer time value with `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d`; returns 408 on expiry. |
| `size` | 1000 | Maximum number of hits to return (per slot in multi-doc mode). |
| `from` | 0 | Offset into the result set for pagination. |
| `rank` | – | Optional ranking block (ADR-059) — order hits by a priority tag and/or request boosts before `from`/`size`. See [Ranking](#ranking-adr-059). |
| `include_broad` | server default (`--include-broad`) | Per-request override: evaluate class C and accepted class D for this request. Class H remains always visible. |
| `include_source` / `_source` | `true` single-node; `false` cluster | Include original query text in each hit. These are aliases and cannot both be present. An explicit `true` works for an in-process cluster; a remote/gRPC cluster returns 501 because its source-fetch wire is not implemented. |
| `explain` | `false` | Attach `_explanation` to each hit for a single native/ES `document`. Multi-document requests return 400 because one union hit can match several input titles. Cluster mode currently returns 400. |
| `profile` | `false` | Include top-level candidate/posting statistics. Multi-document profile statistics are the sum of the per-slot `stats`. |

The ES/OS controls `from`, `size`, `explain`, `profile`, `_source`, and `timeout` may instead be
placed in the query string. A control cannot appear in both locations, even with the same value.
Unknown body fields, query parameters, `rank` fields, boost fields, and document fields return a
structured 400 instead of being silently ignored. `rank`, `include_broad`, `include_source`, and
`timeout_ms` are body-only Reverse Rusty extensions.

`hits.total` is deliberately the legacy integer rather than the newer ES object; it always reflects
the full match count, while `hits.hits` is the paginated window. Every hit carries the stable
`_index: "queries"` identity. `took` is whole milliseconds, `took_ms` is a higher-precision Reverse
Rusty extension, and `timed_out` is always false on a successful response because expiry returns 408
without partial hits. `_shards` is omitted: the compatibility endpoint does not synthesize ES shard
accounting from content-routed positions.

Set `_source: false` (or `include_source: false`) to skip query text lookup. Compatibility cluster
endpoints default it to false so remote clusters remain usable without a source-fetch round trip.
If source or explanation enrichment was requested but is unavailable for a confirmed hit, the whole
request fails (`500 source_unavailable`/`explanation_unavailable` locally; a missing in-process
cluster source is `502`). Matching, ranking, source, and explanation use one exact snapshot
generation. A concurrent replacement can therefore make old enrichment unavailable, but can never
splice the replacement's source onto the older match. In coordinator mode, a request that asks for
sources takes the core mutation-frozen read view through matching and source cloning. Direct
`ClusterEngine` writes and REST writes both wait for that short view; source-free searches keep the
unfenced concurrent-read path.

> **An explicit `timeout` or `timeout_ms` is also a compute budget (ADR-099/123).** On expiry the
> request returns `408` as always, and — when the request set either timeout
> explicitly — the dispatched match work now **cancels itself cooperatively** at
> per-title/segment boundaries and at a fixed interval through dense posting,
> candidate, and canonical-body loops instead of burning the Rayon pool to completion.
> Results are never partial: a cancelled match returns nothing (the same
> 408), never a truncated union. Requests that omit both controls keep the implicit
> 30 s **response** deadline only (the unarmed sampler compiles away and the hot path
> carries zero deadline reads);
> the kill-switch is the dynamic `cooperative_cancel` setting. To bound *how many*
> searches occupy the pool at once, start the server with
> `--max-concurrent-searches N` (excess requests queue within their own timeout).
> Cancellations are counted in `match_cancellations_total{endpoint}`. The same
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
  "took": 0,
  "timed_out": false,
  "took_ms": 0.87,
  "hits": {
    "total": 2,
    "hits": [
      {"_index": "queries", "_id": 1, "_source": {"query": "dell laptop"}},
      {"_index": "queries", "_id": 2, "_source": {"query": "leather jacket"}}
    ]
  },
  "slots": [
    {
      "slot": 0,
      "total": 1,
      "hits": [
        {"_index": "queries", "_id": 1, "_source": {"query": "dell laptop"}}
      ],
      "stats": {
        "unique_candidates": 15,
        "broad_candidates": 0,
        "postings_scanned": 47,
        "matches": 1,
        "probes_attempted": 28,
        "probes_skipped": 12
      }
    },
    {
      "slot": 1,
      "total": 1,
      "hits": [
        {"_index": "queries", "_id": 2, "_source": {"query": "leather jacket"}}
      ],
      "stats": {
        "unique_candidates": 9,
        "broad_candidates": 0,
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
from the index (`broad_candidates` is the subset that came from the quarantined broad lane), how many
posting lists were scanned, how many bloom-filter probes were skipped, and how many candidates
survived to become confirmed matches. See [`../design/matching.md`](../../../design/matching.md) §6
for per-query match tracing.
For a stored quoted clause, `_explanation.required_phrases` or
`_explanation.forbidden_phrases` contains its analyzed `positions` and
`arcs: [{start, end, alternatives}]`. A separated required path reports
`required_phrase[N] not contiguous`; a present forbidden path reports
`forbidden_phrase[N] present` (ADR-120).

### Filtered percolation (ADR-049)

The dominant production read pattern is *"percolate, then narrow to one category."* Attach a tag filter
to a percolate request to keep only the matches whose stored query carries the requested
[metadata tags](../documents/put-document.md#per-query-metadata-tags-adr-049). The filter is a **conjunction across
keys** (AND) of **value sets** (OR within a key). It intentionally narrows the exact Boolean-match
set and is evaluated during verification; it never participates in semantic candidate retrieval.
A filter value never seen at ingest matches nothing (the safe `terms` semantics). Filter values
take the **same canonical scalar coercion as ingest** (ADR-073):
strings, numbers, and bools are accepted everywhere a value is (`{"category": 7}` matches a tag
ingested as `7` or `"7"`); a `null`, object, or nested array anywhere in a filter is a loud **400** —
an unanswerable predicate is never silently dropped (which would *widen* the result set). Two
equivalent shapes are accepted:

**Native** — a `filter` block alongside `document`/`documents`:

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "document": {"title": "Dell XPS 15 Laptop 16GB RAM New"},
  "filter": {"category": ["electronics", "computers"], "status": "active"}
}'
```

**Elasticsearch `bool`/`terms` percolate envelope** — for compatibility with existing percolate
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

Only the `percolate` + `bool.must`/`bool.filter(terms/term)` subset is supported. The percolate clause
must name `field: "query"` and exactly one of `document` or `documents`; each document contains only
a string `title`. A `term`/`terms` clause names exactly one tag field, and `terms` values are an array.
The native `document`/`documents`/`filter` shape cannot be mixed with `query`. Unsupported siblings,
options, or clauses (for example `should`, `must_not`, `match`, or `range`) return **400** rather than
silently widening or changing the request. `/_mpercolate` accepts the same `filter` block and ES
envelope (applied to every document in the batch).

### Ranking (ADR-059)

By default hits come back in the engine's order (a boolean candidate set — the engine is a recall-first
matcher, not a ranker). Attach an optional `rank` block to **order** the hits before pagination. Ranking
is a pure post-match step: it only reorders + paginates the already-final set — it never adds or drops a
match. This compatibility block is the static business-policy API; named title-dependent profiles
belong to the native v2 and exhaustive surfaces documented in the
[ranking reference](../../ranking.md#2-selecting-a-profile). A compatibility `rank` block has two
optional parts:

- **`priority_key`** — the name of a
  [tag](../documents/put-document.md#per-query-metadata-tags-adr-049) whose **numeric
  value** is the query's base priority (a query tagged `priority=50` scores 50; a non-numeric or absent
  value scores 0). An empty string means "no priority term" — identical to omitting the field — on every
  path (single-node, in-process cluster, and over gRPC, whose wire encodes the absent key as `""`).
- **`boosts`** — a list of `{key, value, boost}` entries; a query scores `+boost` for each `(key, value)`
  tag it carries.

The score is **additive** — `score = Σ matched boosts + priority` — and hits are ordered by `score`
descending, ties broken by ascending `_id` (a stable, repeatable order for pagination). Each hit then
carries a `_score` field (present only when a `rank` block was supplied). Want a boost to always
outrank priority? Choose boost magnitudes above your priority range.

```bash
curl -X POST localhost:9200/_search -H 'Content-Type: application/json' -d '{
  "document": {"title": "2020 Acme Labs Chrome Pro New"},
  "filter": {"category": "items"},
  "size": 20,
  "rank": {
    "priority_key": "priority",
    "boosts": [{"key": "tier", "value": "gold", "boost": 100}]
  }
}'
```

```json
{
  "took": 0,
  "timed_out": false,
  "took_ms": 0.31,
  "hits": {
    "total": 3,
    "hits": [
      {"_index": "queries", "_id": 1, "_score": 110, "_source": {"query": "acme labs chrome"}},
      {"_index": "queries", "_id": 3, "_score": 100, "_source": {"query": "acme labs chrome pro"}},
      {"_index": "queries", "_id": 2, "_score": 50,  "_source": {"query": "acme labs chrome new"}}
    ]
  }
}
```

`rank` works on `/_search` (single + multi-document) and `/_mpercolate` (each document's hits ranked
independently), composes with `filter`, and is **opt-in**: with no `rank` block the response is
identical to the unranked path — no `_score` field, engine order preserved. Compatibility cluster endpoints
use ADR-075 rank-at-shard/full-union merge; `/v2/_search` uses ADR-110's bounded exact merge.
