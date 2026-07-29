# `POST /v2/_search` — Exact bounded ranked percolation (ADR-107/108/110/127)

> [Percolation & delivery APIs](../percolate.md) · [REST API hub](../../api.md)

Single-node and cluster-coordinator modes serve exact bounded top-K ranking without first
materializing every matching ID. The route accepts exactly one `document`; batching and
approximate `terminated` delivery reject loudly, as does `from` (deep pagination uses the
[PIT/cursor flow](pit.md), ADR-113). Exact exhaustive `all` is deliberately a separate
[background job/stream surface](exhaustive-jobs.md) (ADR-114), not a giant `/v2/_search` response.
Existing `/_search` and
`/_mpercolate` remain separate compatibility/full-result contracts rather than aliases for this
bounded API. The v2 document is strict and contains only `title`; unknown top-level, document, PIT,
rank, or boost fields are 400 errors rather than ignored input.

```json
{
  "document": {"title": "2024 North Star Wireless Mouse Pro New"},
  "query_scope": "standard",
  "result_mode": "top_k",
  "size": 100,
  "track_total_hits": 10000,
  "rank": {
    "priority_field": "priority",
    "boosts": [{"key": "tenant", "value": "acme", "boost": 1000}]
  },
  "_source": true,
  "explain": false,
  "allow_partial_results": false,
  "timeout": "5s"
}
```

```json
{
  "took": 0,
  "timed_out": false,
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
        "_source": {"query": "wireless mouse pro"}
      }
    ]
  }
}
```

The familiar controls `size`, numeric `track_total_hits`, `query_scope`, `explain`, boolean
`_source`, and `timeout` may be supplied in the JSON body or query string. The native aliases
`track_total_hits_up_to` and `timeout_ms` work in either location; `include_source` remains
body-only. Aliases are mutually exclusive, and a control in both body and query string is a 400
even when the values agree. `timeout` is a non-negative integer plus `nanos`, `micros`, `ms`, `s`,
`m`, `h`, or `d`. Boolean `track_total_hits` is deliberately rejected: `true` would promise an
uncapped exact count and `false` would suppress count work, neither of which matches this
endpoint's bounded threshold contract. Unknown query parameters, malformed values, or unsupported
ES/OS search controls are also 400s.

`complete=true` means the exact best K was computed over the selected visibility scope; it does not
mean every true match appears in the page. Winner order is always `(score desc, _id asc)` and
integer addition saturates at the `i64` bounds. Totals are exact while unique matches do not exceed
the selected total threshold; after the next distinct match the result is
`{"value": threshold, "relation": "gte"}`. `size=0` returns no hits but still computes the
thresholded total. `took` is whole milliseconds and `timed_out` is always false on a successful
response; a deadline returns a structured 408 instead of partial hits. `took_ms` is Reverse Rusty's
higher-precision extension. Native v2 keeps numeric `_id` and does not synthesize an `_index`,
because stored queries are logical IDs rather than resources in a caller-selected index.

Defaults are `result_mode="top_k"`, `query_scope="standard"`, `size=100`, `static_v1` ranking with
typed `priority`,
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
escape. A source/explanation request takes a request-scoped mutation-frozen cluster view across
matching, winner fetch, and explanation; a same-ID replacement cannot splice its source onto an
older hit. Source-free requests remain concurrent. Enrichment is current-view even under a PIT
(ADR-113): matching, scores, order, and totals are snapshot-stable, but `_source` text is read from
the live store as it exists when the request obtains that fence. A winner deleted before that point
fails its enriched page typed (`include_source: false` pages stay fully pinned).

Winner source text is charged once against `--max-ranked-enrichment-bytes` (default 16 MiB), even when
both `_source` and explanation use it. Exceeding the cap returns `413 rank_enrichment_limit` with no
partial response. Cluster transport/protocol failures return 502; stale placement or unavailable
cluster configuration returns 503. `allow_partial_results=true` remains a 400. Malformed JSON uses
the same structured 400 envelope; missing/wrong content type remains 415 and an oversized body
remains 413 rather than being flattened into a generic validation status.

The optional native rank program accepts `profile`, `priority_field="priority"`, and additive
integer tag boosts. `profile` defaults to `static_v1`; unknown profiles return
`unknown_rank_profile`, and unknown priority fields return `unsupported_rank_field`. The canonical
score formula, profile types, feature meanings, evaluation-cost boundary, and fail-closed
distributed contract are in the [ranking reference](../../ranking.md).
`result_mode="all"` or `"terminated"`,
`allow_partial_results=true`, `from`, `documents`, and `query` return explicit 400s.
