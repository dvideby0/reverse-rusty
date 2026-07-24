# ADR-117: PUT document index contract — create-only, strict controls, honest response metadata

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** `PUT /_doc/{id}` already had the most important Elasticsearch/OpenSearch behavior:
  an atomic replace-by-id with 201 `created` / 200 `updated` (ADR-067 locally, ADR-070 in a
  coordinator). Its HTTP boundary was still unsafe for drop-in clients. The handler extracted no
  query parameters, so every parameter was silently ignored. In particular,
  `?op_type=create` overwrote a live query even though both
  [Elasticsearch](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-index) and
  [OpenSearch](https://docs.opensearch.org/latest/api-reference/document-apis/index-document/)
  define it as create-if-absent with a conflict on an existing id. `refresh`, concurrency controls,
  routing, pipelines, and even misspelled parameters all received a success response despite doing
  nothing. Successful bodies also omitted the applicable `_index` and `_version` fields, while the
  reference examples claimed an `error: null` field that serde deliberately omitted.

- **Decision — strict compatible controls.**
  - `op_type=index` is the default and retains the proven atomic upsert.
  - `op_type=create` is a real atomic create-if-absent. The single-node handler holds the engine
    writer lock across exact-index liveness check and WAL-backed insert. The cluster path uses
    `ClusterEngine::create_query_with_tags`: the existing logical-id stripe covers absence check,
    reservation, coordinator-log append, and shard fan-out. One concurrent caller wins; every
    loser receives 409 `version_conflict_engine_exception`, writes no log frame, and cannot replace
    the winner. The versioned create method carries the request's display version instead of the
    legacy `add_query_with_tags` default of 1.
  - `refresh=false|true|wait_for` are accepted. Reverse Rusty publishes every fully applied write
    before replying, so each value receives a stronger immediate-visibility guarantee; there is no
    segment-refresh action to fake. A remote cluster's explicit `partial` outcome remains partial
    and queued for repair under ADR-047.
  - The parameter DTO denies everything else. Invalid values and unsupported controls such as
    `routing`, `pipeline`, `if_seq_no`, `if_primary_term`, query `version`, and `version_type`
    return a structured 400 `illegal_argument_exception` before mutation. A coordinator whose
    logical-id directory is unauthoritative also refuses create-only writes; guessing absence would
    violate distributed unique-id ownership.

- **Decision — honest response and version boundary.** Successful local and coordinator writes now
  return `_index: "queries"`, numeric `_id`, the stored `_version`, and `result`. `_shards`,
  `_seq_no`, and `_primary_term` remain absent: Reverse Rusty has no REST-visible equivalents and
  synthetic values would invite invalid retry/concurrency logic. The JSON-body `version` remains
  unsigned application metadata (default 1), preserved verbatim and allowed to repeat or decrease;
  it is not ES/OS auto-incrementing internal versioning or optimistic concurrency. Documentation
  says this at the write boundary, and unsupported ES/OS concurrency parameters fail rather than
  partially emulating it.

- **Why this is safe.** Both create paths reuse existing accepted-write funnels: local
  `try_insert_live_ranked` (WAL first) and cluster `Add` (coordinator log first). The only new
  condition is checked while holding the same lock/stripe that serializes the corresponding
  mutation, so there is no check-then-write race. Rejected parameters and conflicts mutate neither
  dictionaries nor query state. No signature construction, candidate gating, exact verification,
  hot-path work, or durable format changes; the lossless-cover contract is untouched.

- **Proof.** Handler tests pin the applicable success envelope, all three refresh values,
  `op_type=index`, strict invalid/unsupported parameters, and concurrent same-id create races in
  both server modes: exactly one 201, exactly one 409, exactly one body live, and a later conflict
  cannot replace it. Cluster-core tests pin version/tag preservation plus insert-only behavior and
  hold a provisional reservation across a fault-injected failing log append: a concurrent create
  waits for rollback, then succeeds rather than reporting a false conflict. The cluster handler
  also pins one shared status classifier for error responses and their Prometheus labels. Existing
  upsert, failed-replace, flush-threshold, typed-rank, WAL/reopen, cluster replay, and independent
  matching suites remain the regression proof.

- **Deferred / deliberately unsupported.** Automatic IDs (`POST .../_doc`), the index-scoped
  `queries/_doc/{id}` alias, `_create/{id}`, internal/external version conflict rules, sequence
  numbers/primary terms, custom routing, ingest pipelines, active-shard waits, and shard-level write
  acknowledgements each require their own honest state model or API audit. They are not silently
  accepted here.
