# `POST /v2/_mpercolate` — Exact bounded ranked batch (ADR-112/128)

> [Percolation & delivery APIs](../percolate.md) · [REST API hub](../../api.md)

The batch counterpart to `/v2/_search`: one shared parameter set + `documents[]`, one exact bounded
top-K result per document (`responses[i]` corresponds to `documents[i]`), evaluated through the
columnar batch kernel — in coordinator mode with ONE `PercolateTopKBatch` call per involved shard
instead of a per-document fan.

```bash
curl -X POST localhost:9200/v2/_mpercolate \
  -H 'Content-Type: application/json' \
  -d '{
    "documents": [{"title": "2024 north star wireless mouse pro new"},
                  {"title": "generic unmatched listing"}],
    "query_scope": "standard",
    "size": 10,
    "track_total_hits": 10000,
    "rank": {"priority_field": "priority"},
    "_source": true,
    "timeout": "30s",
    "allow_partial_search_results": false
  }'
```

Response: `{took, took_ms, complete, query_scope, responses: [{timed_out, status, _shards, hits:
{total, hits: [{_id, _score, _source?}]}}]}` — `took` is the whole-millisecond batch duration and
`took_ms` is the higher-precision extension. A successful slot reports `timed_out: false` and
`status: 200`; per-slot `_shards.total` is that document's routed fan-out, and totals carry the same
`eq`/`gte` honesty as `/v2/_search`. Empty `documents` is a 200 with empty `responses`.

Semantics and bounds:

- **Shared options.** `query_scope`, `size`, `track_total_hits_up_to`, `rank`, `filter`,
  `include_source`, and `timeout_ms` apply to every slot (per-document options are a named 400;
  heterogeneous-K callers split batches). Numeric `track_total_hits`, Boolean `_source`, and
  time-value `timeout` are mutually-exclusive ES/OS aliases for the corresponding native controls.
  `allow_partial_search_results: false` aliases native `allow_partial_results: false`; `true` is a
  named 400 because the endpoint never returns partial success. Defaults match `/v2/_search`, except
  timeout defaults to 30 seconds (the v1 batch default).
- **Strict boundary.** Unknown top-level, document, rank, or boost fields and every query-string
  parameter are structured 400s. Malformed/type-invalid JSON is a structured 400; body-size and
  content-type failures retain 413 and 415. Boolean `track_total_hits`, `_source` field filters, and
  duplicate alias pairs are rejected rather than approximated.
- **`explain: true` is not supported here** (a named 400; `false` is accepted) — per-(document,
  winner) explanation compilation is antithetical to the throughput path; use `/v2/_search` for
  one document.
- **`pit`/`cursor` are not supported here** (named 400s, ADR-113) — batch cursor pagination is a
  [roadmap item](../../../roadmap.md#api-and-operator-ergonomics); page per title via `/v2/_search`.
- **Admission**: batch length ≤ min(`max_percolate_batch`, 10 000) and `size × documents ≤ 2^20`
  (the aggregate collector heap budget), both rejected as `rank_admission_rejected` before any
  matching.
- **Winner `_source`** is fetched once per distinct winner across the whole batch and charged per
  DELIVERED occurrence against the same 16 MiB credit as `/v2/_search`
  (`--max-ranked-enrichment-bytes`); overflow is a whole-request 413.
- **No partial results**: one absolute deadline covers routing, matching, merge, and enrichment —
  expiry is a whole-batch 408; any shard/enrichment failure fails the whole request (the same
  status mapping as `/v2/_search`).
- **ES/OS boundary:** this native endpoint deliberately keeps a JSON `documents[]` envelope and one
  shared option set, not the alternating NDJSON metadata/search lines or independent per-search
  failures of Elasticsearch/OpenSearch `_msearch` (ADR-128). Source-enriched cluster batches hold
  one mutation-frozen read view across matching and union fetch; source-free batches stay
  concurrent.
- **Auth boundary:** when a bearer token is configured, this POST currently requires it even with
  `--auth-protect-reads=false`; unlike `/v2/_search`, it is not on the read-via-POST allowlist.
