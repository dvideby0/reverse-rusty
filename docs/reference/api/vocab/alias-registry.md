# `GET` / `HEAD /_vocab/aliases` — Review governed aliases

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

## Registry model and activation policy

The registry governs equivalence expansion with provenance, a structural **kind**, confidence,
optional feedback evidence, and a lifecycle **status** (`candidate`, `active`, or `rejected`).
Candidates and rejected entries are metadata only. Active, expressible groups widen positive
matching through the false-negative-safe equivalence path.

| Source and kind | Default status |
|---|---|
| Operator-imported or manually edited single-token or multi-word group | `active` |
| Any-of-learned clear single-token spelling/abbreviation variant | `active` |
| Any-of-learned distinct-token or multi-word group | `candidate` |
| Distributionally discovered group, of any kind | `candidate` |
| Mixed-feature-kind or otherwise unexpressible group | `candidate`; it cannot affect matching |

Multi-word aliases are implemented, not deferred: ADR-061 supplies query-side collapse plus the
two title feature views, and ADR-076 makes cluster routing positive-view-aware. Import,
learn-and-apply, and an edited registry installed through `PUT /_vocab` work in single-node and
cluster modes. The lower-confidence discovery-record and match-feedback workflows remain
single-node as documented in their focused contracts.

## Read the registry

Returns the governed registry for review. GET and HEAD accept optional non-negative integer
`from` and `size` parameters, matching the familiar paging controls on Elasticsearch's
[get-synonym-set API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym).
`count` is the total number of stored entries before paging; `summary` likewise describes the whole
registry, not only the returned page. Omitting `size` preserves the historical full-registry
response. `size=0` or an offset at or beyond `count` returns an empty `entries` array.

```bash
curl 'localhost:9200/_vocab/aliases?from=0&size=100'
```

```json
{
  "count": 2,
  "aliases": {
    "entries": [
      { "forms": ["package", "packages"], "provenance": "learned_from_queries",
        "kind": "single_token_variant", "status": "active", "confidence": 0.6 },
      { "forms": ["new", "refurbished"], "provenance": "learned_from_queries",
        "kind": "single_token_distinct", "status": "candidate", "confidence": 0.5 }
    ]
  },
  "summary": { "active": 1, "candidate": 1, "rejected": 0 }
}
```

Entry order is the registry's stable stored order. Offset pages do not pin a snapshot across
requests: if another call replaces the registry between pages, the next page can reflect the new
registry. Fetch without `size`, or fetch `GET /_vocab` once, when one coherent full review is
required.

The transport is strict and bodyless. Unknown, duplicate, or malformed parameters and non-empty
bodies return structured 400 errors; stalled bodies return 408; bodies over the GET-specific
64 KiB limit return 413; unsupported methods return 405 with `Allow: GET, HEAD`. Every
route-reached response has `Cache-Control: no-store` and fixed `vocab_aliases_get` telemetry. HEAD
returns the corresponding paged GET headers and `Content-Length` with no body.

Registry capture, paging, and JSON serialization share the one administrative blocking-work slot.
Standalone mode captures one immutable snapshot without an engine lock. Coordinator mode clones
the registry under a brief cluster read lock inside the blocking worker and releases the lock
before serialization. Closed admission returns `503 aliases_unavailable`; worker or serialization
failure returns the same type with 500.

This remains a native governance API ([ADR-150](../../../decisions/adr-150-alias-registry-read-api-contract.md)).
Elasticsearch/OpenSearch
[aliases](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-alias)
route indices and data streams, while analyzer synonym rules do not carry this registry's
provenance, kind, confidence, evidence, and lifecycle status. Reverse Rusty therefore does not
expose this data through `/_alias`, `/_cat/aliases`, or a fabricated named synonym-set path.
