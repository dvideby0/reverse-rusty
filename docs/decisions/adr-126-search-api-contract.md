# ADR-126: Search API contract — strict percolation and generation-consistent enrichment

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** Compatibility `POST /_search` already implemented exact single- and multi-document
  percolation, filtering, ranking, paging, source, explain, and profile. Its boundary was permissive:
  a native body could mix `document`, `documents`, and an ES `query`; unsupported `bool` and
  `percolate` siblings were ignored; arbitrary body fields and query parameters were ignored; and
  the ES `field` was neither required nor validated. This diverged from the
  [Elasticsearch search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-2),
  [Elasticsearch percolate query](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-percolate-query),
  [OpenSearch search API](https://docs.opensearch.org/latest/api-reference/search-apis/search/),
  and [OpenSearch percolator field](https://docs.opensearch.org/latest/field-types/supported-field-types/percolator/)
  in unsafe ways: a caller could receive a successful answer to a different request than the one
  sent. The handler also reloaded the published engine snapshot after matching, while the cheap
  snapshots shared one interior-mutable source store. A concurrent same-id replacement could pair
  old match IDs with new ranking/source/explain data. Multi-document `explain` and top-level
  `profile` were silently unfulfilled.

- **Decision — one strict request.** `GET` and `POST` accept the same JSON body. A request chooses
  exactly one native shape (`document` or `documents`, optionally `filter`) or one ES shape
  (`query.percolate` or `query.bool.must.percolate`, optionally `bool.filter`). Mixing shapes,
  supplying both singular and plural documents, or adding unknown body/rank/boost/document fields
  is a structured 400. The supported ES subset requires `field: "query"`, exactly one document
  selector, a title-only document, only `must` and `filter` in `bool`, exactly one percolate `must`,
  one tag field per `term`/`terms`, and an array for `terms`. Unsupported clauses, options, and
  siblings fail rather than disappear.

- **Decision — compatible controls without ambiguous precedence.** `from`, `size`, `explain`,
  `profile`, `_source`, and integer-unit `timeout` work in the body or query string. A control in
  both locations is a 400, even when values agree. Native `include_source` and `timeout_ms` remain
  body aliases, mutually exclusive with `_source` and `timeout`; `rank` and `include_broad` remain
  body-only extensions. Unknown query parameters and invalid/overflowing time values fail before
  matching. An explicit timeout arms the existing cooperative cancellation path; expiry remains a
  no-partial-results 408. Multi-document explain is rejected because one union hit may match
  several titles; cluster explain remains unsupported and loud.

- **Decision — one generation or no enrichment.** One `Arc<EngineSnapshot>` now owns matching,
  rank compilation/scoring, source fetch, and explanation. Because the `SourceStore` is shared
  across cheap snapshots, snapshot source reads atomically compare the store row's internal source
  generation with the exact row generation captured by the snapshot before cloning text. A
  concurrent replacement may make enrichment unavailable to the older request, in which case the
  whole request fails with a typed source/explanation error. It can never attach the replacement
  query to the older Boolean result. The same generation-attested primitive protects bounded v2
  winner source reads.
  Coordinator compatibility requests that explicitly include source take a core
  `ClusterReadView`: the exclusive side of the same mutation barrier held by every direct
  `ClusterEngine` mutation. The view spans matching and cloning only the paged sources, so both
  REST and direct-library writes wait; source-free cluster search retains its per-title concurrent
  read behavior.

- **Decision — honest response and profile.** Successful compatibility responses add ES/OS-style
  whole-millisecond `took`, `timed_out: false`, and `_index: "queries"` on hits while retaining the
  precise `took_ms` extension. `hits.total` remains the established integer; `_shards` is omitted
  rather than synthesized from content-routing positions. Multi-document `profile: true` now
  merges all per-title match statistics at the top level as cluster mode already did. Missing
  requested source/explanation fails loud rather than omitting a field. Duration histograms cover
  validation, timeout, enrichment, panic, and success exits.

- **Why this is safe.** Strict parsing can only reject requests whose effective meaning was
  previously ambiguous, unsupported, or partly discarded. Query/filter lowering still compiles
  tags only for post-candidate verification, so no negative or metadata predicate enters signature
  retrieval. GET and the added controls select, order, explain, or bound already-confirmed matches;
  they do not alter candidate cover. Source-generation checks are off the matching hot path and
  convert a cross-generation response into an explicit failure.

- **Proof.** Local and coordinator router tests pin GET/POST parity, ES controls, response identity,
  unknown/duplicate-control rejection, and cluster feature rejection. Parser regressions cover
  mixed shapes and every formerly ignored ES sibling while proving shared v2/batch document
  parsing stays unchanged. Handler tests pin multi-document admission/explain rejection, aggregated
  profile statistics, missing-source failure, overflow-safe deadlines, and a permit-barrier
  replacement race. Core tests prove an old exact row rejects a newer shared source generation and
  that a direct cluster upsert waits for a match-plus-source read view. Existing oracle, ranking,
  filter, cancellation, remote fail-loud, and durability suites remain the semantic backstop.

- **Deliberate deltas / deferred.** This is a strict compatible subset, not a claim of wire-level
  Elasticsearch/OpenSearch equivalence. `hits.total` is not the newer `{value, relation}` object;
  compatibility search has no `_shards`, search templates, aggregations, sorting DSL, custom
  routing, preferences, stored fields, or partial timeout results. Remote source fetch and cluster
  explain require transport support. Those controls remain loud 400/501 responses rather than
  cosmetic emulation.
