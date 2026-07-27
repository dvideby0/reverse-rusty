# ADR-129: V2 open-PIT API contract — strict controls and dual ES/OpenSearch response

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `POST /v2/_pit` already pinned one immutable engine snapshot locally or every
  in-process shard position under the coordinator mutation barrier, with bounded TTL and open-PIT
  admission from ADR-113. Its HTTP contract had not received the later API audit. It accepted only
  the Reverse Rusty-specific JSON field `keep_alive_s`; unknown JSON fields were ignored, every
  query parameter was ignored, and a non-empty body without JSON content type could be treated as
  no body. The response exposed only `pit_id`, omitting Elasticsearch's `id` and shard summary and
  OpenSearch's creation timestamp. Extractor failures did not use the standard JSON error envelope.

- **Decision — strict, location-independent controls.** The endpoint accepts an absent body and
  retains its configured default keep-alive. `keep_alive` is a non-negative integer time value
  followed by `nanos`, `micros`, `ms`, `s`, `m`, `h`, or `d`; the existing `keep_alive_s` seconds
  control remains an alias. Either spelling may appear in the JSON body or query string, but aliases
  and locations are mutually exclusive even when their values agree. Unknown, duplicate, malformed,
  and wrong-type controls are structured 400 errors.

- **Decision — partial creation remains fail-loud.** Native `allow_partial_results`, Elasticsearch
  `allow_partial_search_results`, and OpenSearch `allow_partial_pit_creation` are aliases accepted
  in either location. `false` is accepted; `true` is a named 400. Reverse Rusty's distributed exact
  read contract cannot truthfully open a PIT missing a required position. Routing, preference,
  wildcard, and index-filter controls remain unsupported rather than becoming accepted no-ops.

- **Decision — one truthful response superset.** A success returns the same signed token as both
  `id` (Elasticsearch) and `pit_id` (OpenSearch and the original native field), plus
  `creation_time` in Unix epoch milliseconds and `_shards {total, successful, skipped, failed}`.
  Local mode reports one successful logical shard; in-process cluster mode reports every logical
  shard position pinned by the all-or-nothing fan. There are never skipped or failed positions in a
  successful response. Elasticsearch opens PITs at `POST /{index}/_pit` and returns `id` plus
  `_shards`; OpenSearch uses `POST /{target}/_search/point_in_time` and returns `pit_id`,
  `creation_time`, and `_shards`
  ([Elasticsearch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-open-point-in-time),
  [OpenSearch](https://docs.opensearch.org/latest/api-reference/search-apis/point-in-time-api/)).
  Reverse Rusty retains `/v2/_pit` because it exposes one logical stored-query corpus rather than
  caller-selected indices.

- **Decision — optional body without silent discard.** A truly empty body is valid so an
  ES/OpenSearch-style query-only request needs no artificial `{}` or content type. A non-empty body
  must be `application/json` or an `application/*+json` media type. Malformed JSON uses structured
  400, a non-JSON media type uses structured 415, and the configured body limit retains structured
  413.

- **Correctness and compatibility.** This is an HTTP-boundary and response-only change. Registry
  admission, local `Arc<EngineSnapshot>` pinning, cluster mutation exclusion, all-position fan-out,
  rollback on shard refusal, token signing, TTL renewal, and 429/501 error classifications are
  unchanged. Validation completes before snapshot pinning. The auth allowlist is unchanged. Durable,
  wire, matching, candidate, and hot-path formats are untouched. `DELETE /v2/_pit` is explicitly
  outside this decision and receives its own API audit.

- **Proof.** Local router tests cover query-only Elasticsearch controls, OpenSearch controls and
  `+json`, the dual response identity, creation time, truthful shard counts, native alias
  preservation, every duplicate/location conflict, partial refusal, strict unknowns, malformed time,
  and structured 400/413/415 statuses. The coordinator HTTP test proves the same controls and a
  three-position success summary while retaining the existing page-concatenation and post-resize
  staleness proof. Existing PIT registry, cluster K×RF, mutation, rollback, expiry, cap, cursor,
  source-failure, restart, and remote-refusal suites remain the lifecycle backstop.
