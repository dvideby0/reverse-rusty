# ADR-138: Compaction REST API contract — force-all correctness and force-merge ergonomics

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `POST /_compact` was described by the server and API reference as “force
  compaction,” but it called `Engine::maybe_compact()`. With two or more base segments below the
  configured thresholds, the endpoint returned an acknowledged no-op instead of forcing a merge.
  It also silently ignored query parameters and request bodies, relied on Axum's generic method
  rejection, blocked an async runtime worker during corpus rewriting, and returned neither an
  integer timing projection nor a shard result. There was no familiar force-merge spelling.

- **Compatibility boundary.** Elasticsearch and OpenSearch expose indexless
  `POST /_forcemerge`, synchronously wait by default, use `max_num_segments=1` for a one-segment
  target, support `flush`, `only_expunge_deletes`, and `wait_for_completion`, and report `_shards`
  ([Elasticsearch Force Merge API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge-1),
  [OpenSearch Force Merge API](https://docs.opensearch.org/latest/api-reference/index-apis/force-merge/)).
  Reverse Rusty has one implicit `queries` index, no closed-index/alias/wildcard selection, no
  distinct Lucene expunge-only policy, and no task API. Compatibility is therefore an indexless,
  strict semantic subset rather than accepted no-ops.

- **Decision — native force-all.** `POST /_compact` now calls `Engine::compact_all()` regardless of
  the automatic segment-count or holes-ratio thresholds. It merges every sealed base segment into
  one and leaves the mutable memtable alone. Fewer than two sealed segments is an acknowledged
  `"nothing to compact"` result. The native route accepts no query parameters or body.

- **Decision — familiar force-merge alias.** `POST /_forcemerge` without
  `max_num_segments` runs one configured policy selection, matching the familiar “merge as
  necessary” default. `max_num_segments=1` selects force-all. `flush` defaults true and seals the
  memtable before selection under the same writer lock, with the normal post-flush policy pass
  suppressed so the requested target owns both the work and report; `flush=false` keeps the delta
  separate. `only_expunge_deletes=false` and `wait_for_completion=true` are accepted.
  `only_expunge_deletes=true`, non-1 segment targets, and asynchronous completion fail with
  `illegal_argument_exception` before mutation. Named-index paths and all other controls remain
  unsupported.

- **Decision — strict transport and truthful result.** Both routes are POST-only and body-free.
  Unknown, duplicate, malformed, or unsupported controls are structured 400s before writer
  admission. Other methods return a structured 405 and `Allow: POST`; extraction preserves the
  configured 413 limit. Success retains the native detailed report and `acknowledged`, adds integer
  `took` beside precise `took_ms`, and adds `_shards {total:1, successful:1, failed:0}`. A durable
  failure returns 503, `acknowledged:false`, and one failed shard. Cluster mode exposes both
  spellings as explicit 501 boundaries because compaction remains a per-shard policy; checkpoint is
  the distributed durability operation.

- **Decision — execution and durability.** The CPU/I/O-heavy operation runs through
  `spawn_blocking`, so it does not occupy a Tokio runtime worker. The call still waits
  synchronously; once admitted, a client disconnect does not cancel maintenance. The engine writer
  mutex serializes competing writes/maintenance while already-published read snapshots remain
  available. A sticky persistence failure is checked before new work. Flush or merged-segment
  failure is never acknowledged, and ADR-051's build-durable-then-retire commit point keeps every
  source segment available after rollback.

- **Safety and proof.** Compaction changes physical representation but not compiled predicates,
  candidate visibility, exact verification, identity, or ranking. Existing lifecycle, oracle,
  persistence, and crash suites continue to prove match-set and recovery equivalence. New route
  tests pin force-all below the policy threshold, force-merge policy/default and one-segment modes,
  pre-selection flush behavior, strict controls/body/method/413 handling, shard/timing/report
  fields, publication after a dropped admitted request, cluster 501 parity, and injected
  durable-write rollback with both source segments still readable.
