# ADR-135: Compatibility batch-percolate API contract

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `POST /_mpercolate` is Reverse Rusty's established full-result batch endpoint. In
  standalone mode it is also the only REST surface that drives the columnar broad-lane evaluator
  from ADR-026. Its boundary was permissive: unknown top-level, document, and query-string fields
  could be ignored; native and Elasticsearch-shaped inputs could be mixed; source and timeout
  aliases were absent; JSON extractor failures did not use the standard error envelope; and the
  response only borrowed `responses[]` from multi-search without its timing or per-slot status
  signals. Standalone matching then reloaded the published snapshot before ranking and source
  enrichment, so a concurrent same-ID replacement could pair an older match with newer source
  text. Coordinator mode silently ignored `profile`, even though it evaluates titles independently
  and cannot report the standalone columnar-batch profile.

- **Compatibility boundary.** Current Elasticsearch and OpenSearch multi-search use alternating
  metadata/search lines in NDJSON and may report independent per-search failures
  ([Elasticsearch multi-search](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-msearch),
  [OpenSearch multi-search](https://docs.opensearch.org/latest/api-reference/search-apis/multi-search/)).
  Their current percolators accept multiple documents inside one search query and identify matching
  input slots on union hits
  ([Elasticsearch percolate query](https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-percolate-query),
  [OpenSearch percolate query](https://docs.opensearch.org/latest/query-dsl/specialized/percolate/)).
  Reverse Rusty's endpoint deliberately remains a native JSON `documents[]` plus shared-options
  request that returns one independent `responses[i]` slot per input. This shape is what lets the
  standalone engine amortize broad evaluation across the batch. It does not accept NDJSON, claim
  wire compatibility with `_msearch`, synthesize union slot fields, or permit partial slot success.

- **Decision — strict shared request.** The top-level body and every native document reject unknown
  fields. A request chooses either native `documents` plus optional `filter`, or the same strict
  `query.percolate` / `query.bool` subset supported by compatibility search; mixed shapes, a missing
  or wrong percolator field, and unsupported query siblings are structured 400s. Query-string
  parameters are unsupported and rejected rather than ignored. An empty native `documents` array
  remains the established successful no-op. Malformed JSON is a standard JSON 400, while real body
  size and media-type failures preserve 413 and 415.

- **Decision — truthful familiar controls and response.** Boolean `_source` aliases
  `include_source`, and an Elasticsearch/OpenSearch time value `timeout` aliases `timeout_ms`.
  Alias pairs are mutually exclusive. `explain: false` and
  `allow_partial_search_results: false` let generic clients state the actual behavior; either true
  is a named 400 because explanations belong to per-document `/_search` calls and the batch is
  fail-closed. Success adds whole-millisecond `took` beside native `took_ms`; each ordered slot adds
  `timed_out: false` and `status: 200`. A timeout, shard failure, enrichment failure, or task failure
  still withholds the entire response.

- **Decision — one enrichment generation.** Standalone matching, compatibility ranking, and source
  reads use one captured `Arc<EngineSnapshot>`. Source reads verify the shared source-store row
  generation against that snapshot's exact live row and fail with `source_unavailable` on a
  mismatch or missing source; they never attach replacement text to an older Boolean result.
  Coordinator source requests retain their existing exclusive `ClusterReadView` through matching
  and paged source cloning. Standalone `profile: true` continues to expose the real columnar
  broad-lane summary. Coordinator mode now returns `501 profile_unsupported` instead of silently
  discarding that request, because its compatibility implementation deliberately fans out one
  per-title match rather than running the standalone columnar kernel.

- **Safety and proof.** Request aliases lower only to the existing batch options, post-match paging,
  source flag, and cooperative deadline. Strict parsing cannot weaken signature cover or exact
  verification. Local route tests pin strict native/ES shapes, aliases, response metadata,
  structured 400/413/415 errors, POST-only routing, empty-batch behavior, match equivalence,
  fail-loud missing source, and a deterministic same-ID replacement race. Coordinator route tests
  pin the same strict controls and response slots plus the explicit profile limit. Existing
  batch-vs-scalar, oracle, filtered-ranking, cancellation, and distributed fail-loud suites remain
  the semantic backstop.
