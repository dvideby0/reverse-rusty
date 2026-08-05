# ADR-167: Cluster-resize REST API contract

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-078 established the correct in-process resize mechanism: gather the unique live corpus, rebuild
it under a new ring while preserving the frozen model/tag spaces, atomically swap the serving
cluster, update control state, and checkpoint durable deployments. Its REST wrapper remained a thin
Axum JSON extractor. It inherited the server-wide 100 MiB body limit, ignored query controls,
accepted unknown fields and an effectively unbounded shard count, blocked a Tokio request worker
on topology/write/cluster locks for the full `O(corpus)` operation, returned unsanitized backend
errors, and exposed no admission, body deadline, cache, telemetry, or terminal control-version
contract. A remote cluster failed only after lock acquisition with a generic 400.

The underlying rebuild has strong oracle and durability coverage, but the transport could amplify
one small administrative request into unbounded ring allocation or stall the async runtime. It also
left client automation unable to distinguish a rejected request, a started rebuild, and an exact
terminal control state.

## Compatibility boundary

Elasticsearch and OpenSearch expose named-index `/{index}/_split/{target}` and
`/{index}/_shrink/{target}` operations. They create a separate target index, constrain primary
shard counts to a multiple or factor, require source-index preparation, copy/hard-link segments,
and may acknowledge target creation before recovery finishes
([Elasticsearch shrink](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink),
[OpenSearch split](https://docs.opensearch.org/latest/api-reference/index-apis/split/),
[OpenSearch shrink](https://docs.opensearch.org/latest/api-reference/index-apis/shrink-index/)).

Reverse Rusty owns one reverse-query corpus rather than named source/target indices. Resize replaces
the in-process serving ring in place, accepts an arbitrary bounded count, rebuilds from source, and
does not return until all target shards and required durable state are complete. Keep the native
path. Do not alias it to `_split`/`_shrink`, accept target index/settings/active-shard/task controls,
or fabricate an index identity. Adopt `cluster_manager_timeout` and its `master_timeout` alias for
the admission/start wait that maps exactly, plus `shards_acknowledged:true` on terminal success.
Reject the overall `timeout`: once the blue/green rebuild starts, cancelling it at an HTTP deadline
cannot safely promise that serving state, control state, and the durable manifest stayed unchanged.

## Decision

- Admit only `POST` with structured `Allow: POST`; require JSON or `+json`; reject unknown,
  duplicate, missing, null, and non-object forms; cap the body at 64 KiB and delivery at 250 ms.
- Require `num_shards` in `1..=1024`. The upper bound matches the familiar default maximum routing
  space while, more importantly, preventing a tiny request from allocating an unbounded 128-vnode
  ring plus shard set. Preserve arbitrary grow/shrink and same-count retry semantics within it.
- Reject every remote coordinator topology with actionable 501 before admission. A remote shard
  retaining old placement while the coordinator swaps rings would violate lossless routing. The
  supported alternative is a separate green cluster, full re-ingest/validation, and traffic cutover.
- Share the one expensive-administration admission slot with stats/vocabulary work. Move admission,
  exclusive topology/write/cluster lock waits, the complete rebuild, control update, durable
  checkpoint, and final version read onto one independently supervised OS thread. The worker hands
  its terminal result back for the non-blocking HTTP response construction. This keeps Tokio free
  and makes zero-timeout behavior independent of the shared blocking pool.
- Accept one manager-timeout spelling, default/max 30 seconds. Zero is a non-waiting permit and
  exclusive-lock probe. Positive values bound admission and all lock waits until an atomic start
  gate opens. Deadline/disconnect before start cancels queued work so it cannot mutate later.
- Once started, wait for the exact result rather than pretending to cancel. A disconnect drops only
  the response; the worker retains admission and every exclusive guard through rebuild, swap,
  control commit, checkpoint, and version attestation. The shutdown checkpoint waits behind the
  same REST-write guard, joining any active resize before process exit.
- Return `{acknowledged, shards_acknowledged, version, old_num_shards, num_shards, rebuilt}` only
  after terminal success. `version` is the final observed control application version;
  `shards_acknowledged` is exact because every local target shard is built and any required
  checkpoint has committed before 200. Same-count retries repair a stale control shard count before
  returning and retain ADR-078's healing durable checkpoint.
- Fail loud on rebuild/control/durability/version errors with typed status and a sanitized reason
  directing the operator to health and cluster state. Detailed paths/backend endpoints remain in
  logs. Return structured `Cache-Control: no-store` responses and fixed `cluster_resize`
  request/duration telemetry on every route-reached outcome.

## Consequences

Automation can make a bounded request, distinguish guaranteed-not-started timeouts from a started
operation, and correlate a 200 with the authoritative control version and completed target shard
set. The public 1024-shard ceiling is a REST resource-safety contract; direct library construction
retains its existing responsibility to size a cluster appropriately.

The endpoint remains synchronous and can outlive its manager-start timeout after it begins. That is
intentional: the alternative is an outcome-unknown request timeout or a new persistent task state
machine, neither of which the current in-process mechanism implements. A client disconnect does not
cancel or multiply work; inspect health/state before retrying. The shared administrative slot may
delay stats or vocabulary work while a resize runs, but it prevents an unbounded worker queue.

Remote resize remains unsupported rather than approximated. The existing separate-cluster
blue/green runbook is operationally heavier but preserves the lossless routing contract until the
roadmap's resumable remote state machine exists.

## Safety and proof

The matching and placement algorithm is unchanged. ADR-078's complete-source rebuild still swaps
the ring and shard set together, preserves the frozen dictionary/vocabulary/tags/ranking values,
and checkpoints before success. Rejecting remote topologies before admission preserves the rule
that routing and stored placement must use the same ring.

Focused handler tests prove grow success plus post-resize matching, same-count acknowledgement,
post-swap control-proposal failure repair, exact final version/shard fields, strict
method/query/media/object/field/count controls, body size and absolute deadlines,
zero/positive/closed admission, topology-lock deadline cancellation with no delayed mutation, early
remote refusal, fixed no-store telemetry, dedicated execution independent of the shared blocking
pool, post-start manager-timeout semantics, and disconnect-retained completion/admission. Existing
cluster oracle and durability resize suites continue to prove grow, shrink, repeated transitions,
broad and tagged queries, dictionary identity, reopen, directory cleanup, mutation replay, and
stale-data non-resurrection.
