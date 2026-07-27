# ADR-128: V2 batch percolate API contract — strict shared controls and coherent enrichment

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `POST /v2/_mpercolate` already provided bounded exact top-K results for a
  `documents[]` batch, honest thresholded totals per slot, one batch deadline, one enrichment
  credit, and fail-loud local/cluster delivery ([ADR-112](adr-112-distributed-title-batching.md)).
  Its HTTP boundary still ignored unknown top-level fields and every query parameter, returned
  Axum's unstructured extractor failures, and exposed only native spellings for source, timeout,
  and total controls. The response omitted Elasticsearch/OpenSearch's top-level `took`. In cluster
  mode, batch matching and the later union winner-source fetch could observe different versions of
  one logical ID if a direct mutation ran between the phases.

- **Decision — strict native batch envelope.** The top-level request rejects unknown fields.
  `documents[i]` remains title-only and reports a stable indexed error for every discarded sibling;
  `rank` and boost objects retain their strict nested DTOs. Query-string parameters are not part of
  this endpoint's shared-options contract, so any supplied parameter is a structured 400 instead of
  an ignored no-op. JSON syntax/type failures use the standard 400 envelope, while real payload-size
  and content-type extractor failures preserve 413 and 415.

- **Decision — compatible controls only where semantics align.** The shared body accepts numeric
  `track_total_hits` as an alias for `track_total_hits_up_to`, Boolean `_source` as an alias for
  `include_source`, and an Elasticsearch/OpenSearch time-value `timeout` as an alias for
  `timeout_ms`. Alias pairs are mutually exclusive even when their values agree. Boolean
  `track_total_hits` and `_source` field patterns remain errors because the bounded collector and
  one-field stored source cannot implement them honestly. `explain: false` is accepted while
  `true` directs callers to `/v2/_search`. `allow_partial_search_results` aliases the native
  `allow_partial_results`; `false` names the existing fail-closed behavior and `true` is rejected.

- **Decision — deliberately not an NDJSON multi-search clone.** Elasticsearch and OpenSearch
  multi-search accept alternating metadata/search lines in NDJSON and may return independent
  per-search failures ([Elasticsearch multi-search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-msearch),
  [OpenSearch multi-search](https://docs.opensearch.org/latest/api-reference/search-apis/multi-search/)).
  Reverse Rusty's native endpoint keeps one JSON `documents[]` array and one shared control set so
  the columnar kernel can amortize the batch under one bounded admission product. Per-document
  options, independent partial failure, `from`, PIT/cursor state, query DSL, and NDJSON remain loud
  non-features. Successful responses add the truthful whole-batch integer `took`; each slot reports
  `timed_out: false` and `status: 200`, while `took_ms`, `complete`, and `query_scope` remain native
  extensions. A timeout or any slot failure still fails the whole HTTP request.

- **Decision — one cluster mutation view through union enrichment.** A cluster batch requesting
  `_source` acquires the HTTP write serial guard and the core exclusive `ClusterReadView` before it
  enters the coordinator Rayon pool. Batch top-K matching and deduplicated union source fetch both
  use that view. REST and direct-library same-ID mutations therefore cannot splice a replacement
  source onto an older match, and a waiter does not occupy a shared Rayon worker. Source-free
  batches retain the prior concurrent path. The bearer-token boundary is unchanged:
  `/v2/_mpercolate` remains protected when a token is configured and read protection is otherwise
  disabled; the broader policy decision remains tracked in the roadmap.

- **Why this is safe and proof.** All aliases lower to the existing `TopKOptions`, source flag, and
  cooperative deadline after validation; none can affect signature construction, candidate
  visibility, or exact verification. Local router tests pin compatible controls, every
  alias/unknown/type failure, structured malformed input, and preserved 413/415 responses.
  Coordinator router tests pin the same strict shape and exact ordered slots. The existing
  per-slot-equals-`/v2/_search` test protects hit semantics. A core concurrency test holds the
  batch read view across top-K and union fetch, proves a same-ID upsert blocks, reads only the old
  coherent source in every slot, then observes the replacement after the view drops.
