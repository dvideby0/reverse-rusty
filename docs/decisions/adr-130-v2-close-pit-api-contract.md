# ADR-130: V2 close-PIT API contract — atomic batch close and dual-dialect results

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `DELETE /v2/_pit` already authenticated one process-local `pit_id`, removed its
  registry entry, and released the pinned local snapshot or every logical primary-shard pin. The
  HTTP contract had not received the later API audit. It ignored unknown JSON fields and every
  query parameter, accepted only the Reverse Rusty/OpenSearch token spelling, exposed only
  `{"closed": boolean}`, and let raw extractor failures escape the standard error envelope.
  Elasticsearch instead requires scalar `id` and reports `succeeded` plus `num_freed`; OpenSearch
  accepts scalar or array `pit_id` and reports an ordered per-PIT result
  ([Elasticsearch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-close-point-in-time),
  [OpenSearch](https://docs.opensearch.org/latest/api-reference/search-apis/point-in-time-api/)).

- **Decision — one strict identity envelope.** A required JSON body names exactly one of
  Elasticsearch `id` or OpenSearch/native `pit_id`. Either alias accepts a string or non-empty
  string array, making the OpenSearch batch shape available without splitting behavior by token
  spelling. Nulls, both aliases, missing identity, duplicate fields, wrong types, unknown fields,
  and all query parameters are structured 400 errors. Missing/wrong content type is structured
  415. A PIT-specific 64 KiB request limit rejects oversized bodies with structured 413 before
  JSON decoding or array materialization.

- **Decision — bounded, mutation-atomic validation.** A batch contains at most the configured
  `--max-open-pits` entries (with an effective minimum of one). The server authenticates and
  decodes every token before reaping or closing any registry entry. A malformed token therefore
  returns 400, and a token signed by another process returns 409 `stale_cursor`, without partially
  closing an earlier valid item. Structurally valid entries that are already expired, closed, or
  cleared by placement change are normal per-item misses rather than request-validation failures.
  Delete-all is not exposed: explicit IDs preserve bounded work and prevent one authenticated
  client from indiscriminately discarding other clients' pinned views.

- **Decision — one truthful response superset.** Every 200 response retains native `closed`, adds
  Elasticsearch `succeeded` and `num_freed`, and adds OpenSearch `pits` in request order.
  `closed` is true exactly when every requested registry entry was live and removed;
  `pits[].successful` reports that fact per token. `succeeded` is true when request processing
  completed and every still-existing context was released, including the idempotent
  already-gone case. `num_freed` counts actual pinned contexts released: one per live PIT locally,
  or one per logical primary-shard position per live PIT in coordinator mode. It never counts
  physical replicas. Thus an already-gone token returns 200 with `closed: false`,
  `succeeded: true`, `num_freed: 0`, and a false per-PIT result rather than pretending a live
  context was freed.

- **Correctness and compatibility.** PIT IDs, HMAC signing, TTL renewal, registry admission,
  snapshot ownership, cluster mutation exclusion, placement invalidation, auth, and remote 501
  refusal are unchanged. The original `pit_id` scalar request and `closed` response field remain
  valid. Local close still drops the pinned `Arc`; coordinator close still fans to every logical
  primary under the cluster read lock. The change is confined to the HTTP boundary and response
  accounting; durable, wire, matching, candidate, and hot-path formats are untouched.

- **Proof.** Local router tests cover Elasticsearch scalar, OpenSearch batch, and native
  already-gone shapes; ordered aggregate and per-item results; alias conflicts; missing, null,
  empty, oversized, wrong/unknown body and query controls; structured 400/413/415 failures; and
  all-token pre-validation for both malformed and foreign-token batches with unchanged registry
  and gauge. The production-router 413 regression uses a valid array larger than 64 KiB, proving
  the endpoint rejects it at the HTTP body boundary rather than allocating one `String` per item.
  The coordinator HTTP test closes two PITs over three logical shards, proves `num_freed: 6`,
  ordered per-PIT results, full registry/gauge release, and idempotent re-close. Existing registry
  expiry, cap, restart, placement-change, K×RF, and cursor-staleness suites remain the lifecycle
  backstop.
