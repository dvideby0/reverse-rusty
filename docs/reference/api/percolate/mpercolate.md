# `POST /_mpercolate` — Batch percolate (high throughput)

> [Percolation & delivery APIs](../percolate.md) · [REST API hub](../../api.md)

The full-result throughput counterpart to `/_search` (ADR-135). It accepts one strict JSON request
with a shared option set and returns one ordered `responses[i]` slot per input document.
In **standalone mode**, it evaluates the broad lane once per title batch with the columnar kernel
(ADR-026), so a hot anchor's large posting is scanned once for the batch rather than once per title.
Coordinator mode preserves the same exact per-slot semantics but fans out one per-title match; it
does not claim the standalone columnar amortization.

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
    "_source": true,
    "timeout": "2s",
    "allow_partial_search_results": false,
    "profile": true
  }'
```

```json
{
  "took": 0,
  "took_ms": 0.91,
  "responses": [
    {
      "timed_out": false,
      "status": 200,
      "hits": {
        "total": 1,
        "hits": [{"_index": "queries", "_id": 1, "_source": {"query": "dell laptop"}}]
      }
    },
    {
      "timed_out": false,
      "status": 200,
      "hits": {
        "total": 1,
        "hits": [{"_index": "queries", "_id": 2, "_source": {"query": "leather jacket"}}]
      }
    },
    {"timed_out": false, "status": 200, "hits": {"total": 0, "hits": []}}
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

The request must choose exactly one document shape:

- Native: `documents: [{"title":"..."}, ...]`, optionally with a top-level native `filter`.
- ES/OS-familiar: the strict `query.percolate` or `query.bool` subset documented for
  [`/_search`](search.md#getpost-_search--percolate-titles), using `field: "query"` and either `document`
  or `documents`.

The shapes cannot be mixed. The top-level body, each native document, the percolate query, the
ranking block, and the query string are strict: unsupported fields and every query parameter return
a structured `400` instead of being ignored. The media type is `application/json`; malformed JSON
returns structured `400`, an oversized payload preserves `413`, and a missing/wrong JSON content type
preserves `415`.

Shared request fields:

| Field | Default | Description |
|---|---|---|
| `include_broad` | server default (`--include-broad`) | Per-request override: evaluate class C and accepted class D for this batch. Class H remains always visible |
| `include_source` / `_source` | `true` standalone; `false` cluster | Boolean aliases controlling stored query text. Specify at most one. An explicit `true` works for an in-process cluster; a remote/gRPC cluster returns 501 |
| `size` | 1000 | Maximum hits per document |
| `from` | 0 | Per-document offset into each document's hits for pagination |
| `rank` | – | Optional ranking block (ADR-059), applied per document — see [Ranking](search.md#ranking-adr-059) |
| `timeout_ms` / `timeout` | 30000 ms | Native milliseconds or an ES/OS time value such as `250ms` or `2s`; specify at most one. Expiry returns whole-request 408 and an explicit value arms cooperative cancellation (ADR-099) |
| `profile` | false | Standalone only: include the top-level columnar `broad` summary. A coordinator returns `501 profile_unsupported` for `true`; `false` is accepted |
| `explain` | false | `false` is accepted; `true` returns 400 and directs the caller to `/_search` per document |
| `allow_partial_search_results` | false | `false` names the actual fail-closed contract; `true` returns 400 |

Every successful slot has `timed_out: false`, `status: 200`, and a `hits` object. Its exact matched
IDs, total, ranking, page, and source projection are the same as a corresponding per-title search.
Standalone source enrichment stays on the exact snapshot used for matching and fails with
`source_unavailable` rather than attaching text from a concurrent replacement. Cluster source
enrichment retains its mutation-fenced read view. An empty native `documents` array is a valid no-op
(`200` with `responses: []`); a missing document shape is a `400`.

This is **not** the Elasticsearch/OpenSearch multi-search wire format. Their current `_msearch`
endpoints use alternating NDJSON metadata/query lines and may return independent slot errors.
Current ES/OS multi-document percolation also returns union hits with
`_percolator_document_slot`; Reverse Rusty deliberately returns one independent response per input
so the standalone batch kernel can share work. NDJSON, per-document control sets, and partial slot
success are rejected rather than imitated incompletely.

**When to use which.** Use standalone `/_mpercolate` for high-throughput batch/streaming
percolation, especially with broad queries enabled. Both endpoints support `size`/`from` and
`rank`; use `/_search` for rich per-document `stats`, explanations, or profiles. The standalone
batch endpoint deliberately exposes only its aggregate broad summary, while the coordinator names
that unavailable columnar profile instead of returning misleading zeros.
