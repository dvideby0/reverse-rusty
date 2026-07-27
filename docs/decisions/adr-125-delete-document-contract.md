# ADR-125: DELETE document contract — strict controls, logical results, and partial repair

> [Distributed v1 — the ADR-065 graduation program decisions](areas/distributed-v1-graduation.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `DELETE /_doc/{id}` already used the correct log-first engine operations: a local
  WAL-backed tombstone or a coordinator-log-backed all-position remove. Its HTTP boundary did not
  represent the [Elasticsearch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete)
  and [OpenSearch](https://docs.opensearch.org/latest/api-reference/document-apis/delete-document/)
  contracts safely. The handler extracted no query parameters, so `refresh`,
  concurrency controls, routing, active-shard waits, misspellings, and arbitrary parameters were
  silently ignored while the document was deleted. Responses omitted the familiar `_index`.
  `deleted_count` exposed physical rows and could therefore vary with historical layouts or
  replicated placement even though the REST resource was one logical document. The coordinator
  also passed `PartiallyApplied` to the generic error mapper, producing a 200 error envelope while
  recording a 503 metric; the body omitted the required repair path and could invite a duplicate
  delete log frame.

- **Decision — strict compatible control.** `refresh=false|true|wait_for` are accepted. Reverse
  Rusty publishes each completed delete before replying, so every value receives the stronger
  immediate-visibility guarantee. A deny-unknown parameter DTO rejects malformed values and every
  unsupported control with 400 `illegal_argument_exception` before any WAL/coordinator-log append.
  This includes `routing`, `timeout`, `wait_for_active_shards`, `if_seq_no`, `if_primary_term`,
  `version`, and `version_type`; accepting any of them without its stated availability or
  concurrency semantics would be dishonest.

- **Decision — honest response metadata.** A successful delete returns 200 with
  `_index: "queries"`, numeric `_id`, `result: "deleted"`, and the existing Reverse Rusty
  `deleted_count` extension normalized to one logical document. Physical segment rows, placement
  copies, and replicas remain operational detail. A missing or already-deleted id returns 404 with
  `_index`, `_id`, and `result: "not_found"` and no count. `_version` is omitted because
  Elasticsearch/OpenSearch define it as the newly allocated internal delete-tombstone version;
  Reverse Rusty's caller-supplied application version is removed, not incremented or retained.
  `_shards`, `_seq_no`, and `_primary_term` are likewise omitted rather than synthesized.

- **Decision — failures and partial apply.** A standalone WAL append failure remains an unapplied
  503 but now uses the standard structured error envelope with `durability_unavailable`.
  Coordinator failures use their typed write status for both the response and Prometheus label,
  and every coordinator delete records latency. `PartiallyApplied` is handled before the generic
  mapper: it returns 200 `result: "partial"` with applied/pending shard lists and
  `POST /_cluster/resync` guidance. The coordinator log already owns that mutation, so a repeated
  DELETE is the wrong recovery action; resync or replay completes it.

- **Why this is safe.** Parameter validation happens before engine or coordinator access. The
  accepted path still calls the existing `delete_by_logical_id` / `remove_query` funnels, preserving
  log-before-apply durability, same-id serialization, idempotent tombstones, and queued remote
  repair. Response normalization changes no signature, candidate, verifier, placement, or durable
  format state. The lossless signature-cover contract is untouched.

- **Proof.** Local and in-process coordinator handler tests pin all three refresh values, immediate
  match/point-read invisibility after success, the 200/404 identity envelopes, logical count `1`,
  deliberate metadata omissions, and rejection-before-mutation for unsupported, malformed, and
  duplicate controls. A focused handler unit pins `PartiallyApplied` as an explicit 200 body with
  resync guidance; existing coordinator fault-injection tests prove that partial removes remain
  reserved and converge through resync. The existing WAL, crash-recovery, compaction, repeat-delete,
  and reinsert suites continue to prove core delete behavior.

- **Deferred / deliberately unsupported.** Index-scoped `queries/_doc/{id}` aliases, custom routing,
  availability waits/timeouts, internal or external delete versioning, and sequence-number/primary-
  term conditions require state models Reverse Rusty does not expose today. They remain loud 400s,
  not partial emulations.
