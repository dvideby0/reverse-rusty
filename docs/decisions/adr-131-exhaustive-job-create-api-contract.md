# ADR-131: Exhaustive-job creation API contract — strict async ergonomics

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** ADR-114 established correct, bounded exhaustive execution and terminally attested
  single-consumer delivery. Its creation HTTP boundary had not received the later endpoint audit:
  it required a caller event id plus redundant `result_mode` and sink fields, ignored every query
  parameter and unknown JSON field, silently treated explicit null like omission on optional
  fields, exposed raw extractor errors, and returned only native job fields. Elasticsearch async
  search accepts time and partial-result controls and returns server-generated identity plus
  running/partial/timing state; OpenSearch asynchronous search has analogous wait, retention, and
  identity fields
  ([Elasticsearch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-async-search-submit),
  [OpenSearch](https://docs.opensearch.org/latest/search-plugins/async/index/)).

  ADR-132 later defines the strict retained-status response and polling controls; this decision
  continues to own creation only.

- **Decision — strict minimal creation.** One JSON `document` remains required. Because the route
  itself means exhaustive delivery, omitted `result_mode` defaults to `all`; every other value gets
  named 400 `unsupported_result_mode`. The HTTP NDJSON sink is the default, while explicit
  `ndjson_stream` and historic `grpc_stream` remain equivalent accepted spellings. Scope, filter,
  and rank retain their prior semantics and defaults. Typed body/nested objects and the query
  string reject unknowns; duplicate typed schema controls, explicit nulls, malformed/wrong types,
  and ambiguous aliases are structured 400 errors. The dynamic filter retains its existing
  metadata-map semantics. Missing/wrong JSON content type is structured 415. A route-local 1 MiB
  limit returns structured 413 before JSON deserialization, independent of the server-wide bulk
  allowance.

- **Decision — familiar controls only where semantics align.** Native `timeout_ms` and ES/OpenSearch
  `timeout` are aliases accepted in either body or query, never both aliases or locations.
  Time values use the shared integer `nanos|micros|ms|s|m|h|d` parser and remain bounded by the
  configured positive exhaustive-job maximum. Native `allow_partial_results` and Elasticsearch
  `allow_partial_search_results` are equivalent body/query aliases; false or omission is accepted
  and true fails because a partial set cannot satisfy the exact-delivery contract.
  `wait_for_completion_timeout`, `keep_alive`, and `keep_on_completion` are recognized only to
  produce an explanatory 400: this route always returns immediately, and in-memory status uses
  bounded count-based pruning rather than client-selected time retention. Silently accepting those
  controls would promise behavior the lifecycle does not implement.

- **Decision — optional retry identity and truthful response projection.** A supplied `event_id`
  retains ADR-114's retained-record idempotency and conflict rules. When omitted, the server
  generates and returns a UUID; this makes one-shot creation familiar, but a retry made before the
  client receives that value is deliberately a new job. Success preserves `job_id`, lowercase
  native `state`, URLs, generation, and `reused`, and adds `id == job_id`, `is_running`,
  `is_partial`, and `start_time_in_millis`. `is_partial` is true until and unless terminal
  completion is delivered and committed, so failed/cancelled records never masquerade as exact.
  No expiration timestamp is invented: execution deadline and retained-result expiry are different
  concepts.

- **Correctness and compatibility.** Preparation resolves all defaults and aliases before
  fingerprinting or capacity admission. Existing explicit native requests remain valid and
  idempotent; defaulted mode/sink have the single already-supported meaning. Job admission,
  fingerprint semantics, dedicated worker/registry bounds, snapshot capture, cluster mutation
  fencing, stream ownership, cancellation, timeout enforcement, provisional chunks, terminal
  checksum, remote transport, auth, and 429/503 classifications are unchanged. Durable, wire,
  matching, candidate, and hot-path formats are untouched.

- **Proof.** Standalone production-router tests cover minimal generated-identity creation,
  ES-style query controls, explicit native retry reuse, response projections, every alias/location
  conflict, partial-result refusal, unsupported wait/retention controls, non-all mode,
  strict top-level/nested/query shapes, explicit null, malformed JSON, media types, and the 1 MiB
  pre-deserialization limit under the outer 100 MiB server allowance. Coordinator routing proves
  the same minimal and ES-control contract. Existing lifecycle tests still prove exact completion,
  semantic fingerprinting, tag-dictionary stability, collision rejection, single-consumer HEAD
  safety, disconnect failure, and cancellation while waiting for the cluster write barrier.
