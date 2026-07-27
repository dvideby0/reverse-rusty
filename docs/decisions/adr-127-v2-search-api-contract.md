# ADR-127: V2 search API contract — strict controls and mutation-consistent enrichment

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `POST /v2/_search` already provided exact bounded top-K ranking, honest thresholded
  totals, PIT/cursor pages, winner-only source fetch, and fail-loud local/cluster delivery
  ([ADR-107](adr-107-ranked-percolation-result-contract.md),
  [ADR-108](adr-108-typed-priority-local-bounded-ranking.md),
  [ADR-110](adr-110-distributed-top-k-query-then-fetch.md), and
  [ADR-113](adr-113-pit-cursor-pagination.md)). Its HTTP boundary did not receive the same
  contract audit as compatibility search. Unknown top-level and nested fields were ignored, all
  query parameters were ignored, and the body exposed only Reverse Rusty spellings for source,
  timeout, and total controls. A malformed content type or oversized body also bypassed the
  standard structured error envelope. Successful responses omitted the familiar `took` and
  `timed_out` fields. In cluster mode, bounded matching and the later winner-source fetch were
  individually exact but a direct same-ID mutation could run between them, pairing an old hit with
  the replacement query's source.

- **Decision — strict input and unambiguous controls.** The request, `document`, `pit`, `rank`, and
  boost DTOs reject unknown fields. The document accepts only `title`, so product metadata cannot
  appear to influence a search when it is actually discarded. `size`, `query_scope`, `explain`,
  boolean `_source`, time-value `timeout`, and numeric `track_total_hits` work in the body or query
  string. Existing `timeout_ms` and `track_total_hits_up_to` work in both locations;
  `include_source` remains a body-only alias. An alias pair or one effective control in both
  locations is a structured 400, even when equal; unknown query parameters fail likewise. Time
  values use a non-negative integer plus `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d`.

- **Decision — compatible only where semantics align.** Elasticsearch and OpenSearch accept search
  controls such as [`size`, `_source`, `timeout`, and `track_total_hits`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-2);
  OpenSearch documents the same request family in its
  [Search API](https://docs.opensearch.org/latest/api-reference/search-apis/search/).
  This endpoint accepts only numeric `track_total_hits`: Boolean `true` would promise an uncapped
  exact count beyond Reverse Rusty's fixed 10,000 threshold, while `false` would request a
  count-suppression mode the collector does not implement. `_source` is Boolean only because the
  stored source has one query field; field-pattern filtering would be cosmetic. Unsupported
  controls remain loud errors rather than accepted no-ops.

- **Decision — response and extractor honesty.** A success adds whole-millisecond `took` and
  `timed_out: false`, retaining the higher-precision `took_ms` extension. Timeouts still fail the
  entire request with 408; the server never returns successful partial hits with
  `timed_out: true`. Native v2 keeps its established `{value, relation}` total, numeric `_id`, and
  per-hit shape so each `/v2/_mpercolate` slot remains byte-equivalent to a corresponding single
  search. It does not synthesize `_index`: the API addresses one logical stored-query corpus, not a
  caller-selected index. JSON syntax/type failures use structured 400 responses, while the
  extractor's real 413 and 415 statuses are preserved with the same envelope.

- **Decision — one cluster mutation view through enrichment.** A cluster request that needs a
  source for `_source` or explanation acquires the HTTP write serial guard and the core exclusive
  `ClusterReadView` before entering the coordinator Rayon pool. Bounded matching, current winner
  fetch, and explanation assembly all use that view. REST and direct-library mutations therefore
  cannot replace one logical ID between phases, and waiting for the fence cannot occupy a shared
  Rayon worker. Source-free top-K keeps the prior concurrent path. PIT still pins Boolean
  matching, scores, order, and totals rather than source history; enrichment reads the live source
  at the request fence and fails loud when a winner is unavailable.

- **Why this is safe.** Strict validation runs before matching and rejects only ambiguous,
  malformed, unsupported, or previously discarded input. Aliases lower to the existing bounded
  `TopKOptions`, rank program, source flag, and cooperative deadline; none changes signature
  construction, candidate visibility, or exact verification. The read view reuses the mutation
  barrier already required for exhaustive and compatibility enriched reads and is held only when
  response enrichment needs cross-phase consistency. No durable or wire format changes.

- **Proof.** Local router tests pin body/query aliases, threshold behavior, omitted source with
  retained explanation, every duplicate-alias/location error, unknown fields at each nesting
  level, Boolean-total rejection, structured malformed input, and preserved 413/415 statuses.
  Coordinator router tests pin the same strict controls and unchanged exact delivery.
  The shared v2 batch-equivalence test proves the per-hit response shape did not drift. A core
  concurrency test holds a ranked read view across top-K and bounded source fetch, proves a direct
  same-ID upsert blocks, observes only the old coherent source, then observes the replacement after
  drop. Existing local, distributed, PIT, ranking, enrichment-limit, timeout, oracle, and
  fail-loud suites remain the semantic backstop.

- **Deferred / deliberately unsupported.** Boolean `track_total_hits`, `_source` field patterns,
  `from`, query DSL, aggregations, sorting DSL, stored/doc-value fields, routing, preference,
  partial results, and successful partial timeout responses require semantics this bounded
  percolation API does not expose. They remain structured 400s. Index-scoped aliases and a string
  Elasticsearch-style `_id` would be broader identity/wire migrations and are not synthesized.
