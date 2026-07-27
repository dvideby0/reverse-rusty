# ADR-137: Flush REST API contract — strict controls, shard results, and fail-loud durability

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** Reverse Rusty exposed only `POST /_flush`, accepted arbitrary query parameters and
  nonempty bodies without inspecting them, and returned deployment-specific envelopes. Standalone
  reported native query/segment totals; the coordinator reported only `acknowledged`. Neither mode
  returned the familiar shard summary. The coordinator path also omitted the normal HTTP
  request-count and duration metrics. More seriously, `LocalShard::flush` returned success after
  `Engine::flush` fell back to an in-memory segment because of a disk failure, so a durable
  in-process cluster or shard server could acknowledge a flush that had not reached disk.

- **Compatibility boundary.** Elasticsearch and OpenSearch accept both `GET` and `POST /_flush`,
  define `force` and `wait_if_ongoing` Boolean controls, reject a non-waiting request when another
  flush is active, and return a `_shards {total, successful, failed}` result
  ([Elasticsearch Flush API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-flush),
  [OpenSearch Flush API](https://docs.opensearch.org/latest/api-reference/index-apis/flush/)).
  Reverse Rusty adopts that indexless shape for its one implicit `queries` index. Named-index,
  alias, wildcard, unavailable-index, and closed-index controls remain unsupported rather than
  being silently approximated.

- **Decision — strict body-free transport.** `GET` and `POST` are the only admitted methods. The
  request body must be empty. Query parsing accepts only Boolean `force` and `wait_if_ongoing`; unknown,
  duplicate, or malformed values return one structured 400 before any flush. Body extraction keeps
  the configured 413 limit. Other methods return a structured 405 and `Allow: GET, POST`. Because
  GET is still a mutating maintenance operation, the auth middleware protects GET and derived HEAD
  requests exactly like POST instead of applying its normal unauthenticated-read allowance.

- **Decision — concurrency controls.** Each server state owns a small explicit-flush mutex separate
  from the ordinary writer lock. `wait_if_ongoing=true` (the preserved default) waits for an
  earlier explicit flush. `false` attempts that mutex once and returns 409
  `flush_in_progress_exception` if another flush owns it. Keeping this admission separate prevents
  an unrelated document write from being mislabeled as an ongoing flush. Both `force` values are
  accepted: Reverse Rusty always executes the same synchronous memtable-seal boundary, and forcing
  a clean memtable is an acknowledged no-op because there is no segment payload to materialize.

- **Decision — shared truthful response.** A successful response retains native `acknowledged` and
  precise `took_ms`, adds integer `took`, and adds `_shards`. Standalone reports one logical shard
  plus its existing `total_queries` and `base_segments`; cluster mode reports the number of logical
  shard positions and omits standalone-only totals. Query/body/method failures occur before
  admission. Coordinator successes and failures now increment the same `flush` HTTP counter and
  duration histogram as standalone.

- **Decision — fail loud at the shard seam.** A standalone durable write failure remains a 503 with
  `acknowledged:false`, `_shards.failed:1`, a published readable in-memory fallback, and the WAL
  left authoritative (ADR-051). `LocalShard::flush` now checks the engine's sticky persistence
  health after publishing its snapshot and returns `ShardError::Log` when the segment was not
  durably persisted. That error crosses both in-process and gRPC shard seams, so the coordinator
  cannot render a clean acknowledgement. A bare cluster flush still seals shard memtables only; it
  does not truncate the coordinator/per-shard mutation tails. `POST /_checkpoint` remains the
  durable in-process cluster commit that reseals tombstones, commits the coordinator manifest, and
  advances log checkpoints.

- **Safety and proof.** Flush changes storage representation, not query compilation, candidate
  retrieval, exact verification, placement, or visibility. The existing writer and cluster-write
  locks still fence mutation from the seal; the new mutex controls only explicit-flush admission.
  Standalone route tests pin GET/POST parity, response fields, idempotence, strict controls/body/
  methods, 413 preservation, non-waiting conflict, and disk-failure fail-closed behavior.
  Coordinator tests pin the shared boundary and logical shard counts. A durable-cluster oracle
  makes every shard segment directory read-only, proves `ClusterEngine::flush` returns a log error,
  and proves the in-memory fallback remains readable.
