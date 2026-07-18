# Percolate — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `POST /v2/_search` — Exact bounded ranked percolation (ADR-107/108/110)

Single-node and cluster-coordinator modes serve exact bounded top-K ranking without first
materializing every matching ID. The route accepts exactly one `document`; batching, exhaustive
`all`, and approximate `terminated` delivery are later increments and reject loudly, as does
`from` (deep pagination is the PIT/cursor flow below, ADR-113). Existing `/_search` and
`/_mpercolate` behavior and response bytes are unchanged.

```json
{
  "document": {"title": "1996 Skybox Premium Michael Jordan PSA 10"},
  "query_scope": "standard",
  "result_mode": "top_k",
  "size": 100,
  "track_total_hits_up_to": 10000,
  "rank": {
    "priority_field": "priority",
    "boosts": [{"key": "tenant", "value": "acme", "boost": 1000}]
  },
  "include_source": true,
  "explain": false,
  "allow_partial_results": false,
  "timeout_ms": 5000
}
```

```json
{
  "took_ms": 0.31,
  "complete": true,
  "query_scope": "standard",
  "_shards": {"total": 1, "successful": 1, "failed": 0},
  "hits": {
    "total": {"value": 17, "relation": "eq"},
    "hits": [
      {
        "_id": 42,
        "_score": 1050,
        "_source": {"query": "michael jordan psa 10"}
      }
    ]
  }
}
```

`complete=true` means the exact best K was computed over the selected visibility scope; it does not
mean every true match appears in the page. Winner order is always `(score desc, _id asc)` and
integer addition saturates at the `i64` bounds. Totals are exact while unique matches do not exceed
`track_total_hits_up_to`; after the next distinct match the result is
`{"value": threshold, "relation": "gte"}`. `size=0` returns no hits but still computes the
thresholded total.

Defaults are `result_mode="top_k"`, `query_scope="standard"`, `size=100`, typed `priority` ranking,
`track_total_hits_up_to=10000`, `include_source=true`, `explain=false`,
`allow_partial_results=false`, and `timeout_ms=5000`. Hard limits are `size <= 10000` and
`track_total_hits_up_to <= 10000`. A native `filter` uses the same tag predicate as compatibility
percolation. Requested source or explanation lookup is fail-closed. The timeout is compute-armed and
includes waiting for the dedicated ranked-search permit; timeout returns 408 and cooperative matching
receives the same deadline.

In cluster mode, ADR-109 ownership is applied before each shard's heap. Every routed logical position
returns at most K sorted owned hits; the coordinator validates disjointness, performs the exact global
merge, and reports routed positions in `_shards` (physical replicas do not inflate the count). Exact
shard totals are summed; `eq` is returned only when every shard is exact and the global sum remains
within the threshold. The coordinator then fetches **current** source only for final winners, grouped
by owning position, and compiles explanations locally. A shard/fetch failure, missing source,
placement-generation drift, timeout, or malformed reply fails the whole response—partial hits never
escape. Enrichment is current-view even under a PIT (ADR-113): matching, scores, order, and totals
are snapshot-stable, but `_source` text is read from the live store — a winner deleted after the
PIT was opened fails its enriched page typed (`include_source: false` pages stay fully pinned).

Winner source text is charged once against `--max-ranked-enrichment-bytes` (default 16 MiB), even when
both `_source` and explanation use it. Exceeding the cap returns `413 rank_enrichment_limit` with no
partial response. Cluster transport/protocol failures return 502; stale placement or unavailable
cluster configuration returns 503. `allow_partial_results=true` remains a 400.

The optional rank program supports only `priority_field="priority"` plus additive integer tag boosts.
Unknown rank fields return `unsupported_rank_field`. `result_mode="all"` or `"terminated"`,
`allow_partial_results=true`, `from`, `documents`, and `query` return explicit 400s.

## `POST /v2/_pit`, `DELETE /v2/_pit` — Point-in-time cursor pagination (ADR-113)

Deep pagination over `/v2/_search` without deep `from`: open a PIT, page with `search_after`
cursors over ONE frozen view, never mixing generations.

```
POST /v2/_pit {"keep_alive_s": 60}      -> {"pit_id": "<opaque token>"}
POST /v2/_search {..., "pit": {"id": "<pit_id>"}}          -> page 1 + "next_cursor"
POST /v2/_search {..., "cursor": "<next_cursor>"}          -> page N (resend the same request)
DELETE /v2/_pit {"pit_id": "<pit_id>"}  -> {"closed": true|false}
```

A PIT pins the engine snapshot (single-node) or every shard position's snapshot (in-process
cluster) for a renew-on-use keep-alive: default `--pit-default-keep-alive-secs` (60), ceiling
`--pit-max-keep-alive-secs` (600, over-ask is a 400), at most `--max-open-pits` (64) concurrently
open — a breach is **429 `pit_limit_exceeded`**, never an eviction. Every use (open, page,
cursor) renews the deadline; abandoned PITs expire; `DELETE` frees immediately (`closed: false`
when already gone — the goal state either way). Open PITs retain memory (the pinned memtable
copy) and, after compaction, disk (unlinked-but-mapped segments) until released; the `open_pits`
gauge tracks them.

Cursor rules: a FULL page (`hits.length == size`, `size > 0`) returns `next_cursor`; a short page
ends the stream (no cursor). The client resends the **same** `document`/`query_scope`/`rank`/
`filter` with each cursor — they are fingerprinted into the token and a drifted resend is a 400
`cursor_mismatch`; `size`, `timeout_ms`, and `track_total_hits_up_to` may vary per page. Totals
are page-invariant (every page of one PIT reports the identical total). `pit` + `cursor` together
is a 400. Concatenating pages yields exactly the one-shot ranked result over the same PIT — no
duplicates, no gaps.

Fail-closed staleness — **409 `stale_cursor`** (the one deliberate read-surface 409; the pinned
generation is unrecoverable, so open a new PIT and restart rather than retrying): an expired or
closed PIT, a server restart (tokens are HMAC-signed with a per-process key), and — in cluster
mode — any placement change (`resize`, vocabulary rebuild) or a primary failover (PIT reads are
primary-only, never silently failed over). Structurally garbled tokens are 400s. A remote/gRPC
coordinator assembly refuses PIT entirely with **501 `pit_unsupported`** (wire PIT is a later
increment; page via an in-process cluster or single-node mode). Both endpoints ride the open
search auth allowlist.

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
| `timeout_ms` | 30000 | Per-request timeout in ms; returns 408 on expiry. Set explicitly, it also arms **cooperative cancellation** of the in-flight match work (ADR-099) — see note. |
| `size` | 1000 | Maximum number of hits to return (per slot in multi-doc mode) |
| `from` | 0 | Offset into the result set for pagination |
| `rank` | – | Optional ranking block (ADR-059) — order hits by a priority tag and/or request boosts before `from`/`size`. See [Ranking](#ranking-adr-059). |
| `include_broad` | server default (`--include-broad`) | Per-request override: evaluate class-C (broad) queries for this request (ADR-073 — previously `/_mpercolate`-only; on `/_search` the field was silently ignored) |
| `include_source` | true | Include original query text in each hit |

`total` always reflects the full match count; `hits` is the paginated window. Set
`include_source: false` to skip query text lookup for faster responses.

> **An explicit `timeout_ms` is also a compute budget (ADR-099).** On expiry the
> request returns `408` as always, and — when the request set `timeout_ms`
> explicitly — the dispatched match work now **cancels itself cooperatively** at
> coarse (per-segment / per-title) boundaries instead of burning the Rayon pool to
> completion. Results are never partial: a cancelled match returns nothing (the same
> 408), never a truncated union. Requests that omit `timeout_ms` keep the implicit
> 30 s **response** deadline only (the unarmed hot path carries zero deadline reads);
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
      "hits": [{"_id": 2, "_source": {"query": "leather jacket"}}],
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
survived to become confirmed matches. The search body also accepts `explain` and
`profile` options for per-query match tracing (see [`../design/matching.md`](../../design/matching.md) §6).

### Filtered percolation (ADR-049)

The dominant production read pattern is *"percolate, then narrow to one category."* Attach a tag filter
to a percolate request to keep only the matches whose stored query carries the requested
[metadata tags](documents.md#per-query-metadata-tags-adr-049). The filter is a **conjunction across keys** (AND) of
**value sets** (OR within a key); it is applied in the hot-path verify stage and can only *remove*
matches, never add or drop a wanted one. A filter value never seen at ingest matches nothing (the safe
`terms` semantics). Filter values take the **same canonical scalar coercion as ingest** (ADR-073):
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
byte-identical to before — no `_score` field, engine order preserved. Compatibility cluster endpoints
use ADR-075 rank-at-shard/full-union merge; `/v2/_search` uses ADR-110's bounded exact merge.

## `POST /v2/_mpercolate` — Exact bounded ranked batch (ADR-112)

The batch counterpart to `/v2/_search`: one shared parameter set + `documents[]`, one exact bounded
top-K result per document (`responses[i]` corresponds to `documents[i]`), evaluated through the
columnar batch kernel — in coordinator mode with ONE `PercolateTopKBatch` call per involved shard
instead of a per-document fan.

```bash
curl -X POST localhost:9200/v2/_mpercolate \
  -H 'Content-Type: application/json' \
  -d '{
    "documents": [{"title": "1996 skybox premium michael jordan psa 10"},
                  {"title": "generic unmatched listing"}],
    "query_scope": "standard",
    "size": 10,
    "rank": {"priority_field": "priority"},
    "include_source": true,
    "timeout_ms": 30000
  }'
```

Response: `{took_ms, complete, query_scope, responses: [{_shards, hits: {total, hits: [{_id,
_score, _source?}]}}]}` — per-slot `_shards.total` is that document's routed fan-out; totals carry
the same `eq`/`gte` honesty as `/v2/_search`. Empty `documents` is a 200 with empty `responses`.

Semantics and bounds:

- **Shared options.** `query_scope`, `size`, `track_total_hits_up_to`, `rank`, `filter`,
  `include_source`, and `timeout_ms` apply to every slot (per-document options are a named 400;
  heterogeneous-K callers split batches). Defaults match `/v2/_search`, except `timeout_ms`
  defaults to 30000 (the v1 batch default).
- **`explain` is not supported here** (a named 400) — per-(document, winner) explanation
  compilation is antithetical to the throughput path; use `/v2/_search` for one document.
- **`pit`/`cursor` are not supported here** (named 400s, ADR-113) — batch cursor pagination is a
  deferred increment; page per title via `/v2/_search`.
- **Admission**: batch length ≤ min(`max_percolate_batch`, 10 000) and `size × documents ≤ 2^20`
  (the aggregate collector heap budget), both rejected as `rank_admission_rejected` before any
  matching.
- **Winner `_source`** is fetched once per distinct winner across the whole batch and charged per
  DELIVERED occurrence against the same 16 MiB credit as `/v2/_search`
  (`--max-ranked-enrichment-bytes`); overflow is a whole-request 413.
- **No partial results**: one absolute deadline covers routing, matching, merge, and enrichment —
  expiry is a whole-batch 408; any shard/enrichment failure fails the whole request (the same
  status mapping as `/v2/_search`).

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
| `timeout_ms` | 30000 | Per-request timeout in ms; returns 408 on expiry. Set explicitly, it also arms **cooperative cancellation** of the in-flight match work (ADR-099) — see note. |
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
