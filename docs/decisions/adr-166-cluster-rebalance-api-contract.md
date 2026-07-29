# ADR-166: Cluster-rebalance REST API contract

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_cluster/rebalance` originally decoded an optional body directly as `Bytes`, inherited the
server-wide 100 MiB limit, and accepted serde's lenient struct forms, unknown fields, nulls, zero
parallelism, and `max_parallel` without data movement. Query parameters were ignored. The operation
had no fixed body deadline, admission bound, no-store/fixed-label telemetry, manager-start timeout,
or final control-state version. Backend error strings—including endpoints—were returned to clients.

More importantly, an empty body meant a map-only control-plane update in every topology. That is
safe for an in-process cluster, where all physical shards remain co-resident, but unsafe for a
populated remote cluster: committed routing could point at an owner that never received the data.
The reference warned operators not to use the endpoint's default in exactly the topology where
rebalance matters most.

## Compatibility boundary

Elasticsearch and OpenSearch expose `POST /_cluster/reroute` for explicit per-shard commands, not a
whole-cluster “recompute desired placement now” operation. Both accept manager and request timeout
controls; their reroute returns after the allocation decision rather than synchronously waiting for
every physical transfer
([Elasticsearch cluster reroute](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute),
[OpenSearch cluster reroute](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)).

Keep Reverse Rusty's native `/_cluster/rebalance` path and deterministic HRW planner. Do not alias it
to `/_cluster/reroute`, accept its `commands`, or fabricate allocation decisions. Adopt only
`cluster_manager_timeout` and its `master_timeout` alias because they map to waiting for admission
and authoritative topology access before this workflow starts. Do not accept the familiar overall
`timeout`: Reverse Rusty's remote operation waits for live handoff and cannot safely cancel a
transfer once it has fenced/drained a source.

## Decision

- Admit only `POST`, accept an empty body or one JSON object, require JSON media type for a
  non-empty body, reject unknown/duplicate/null fields and non-object forms, cap transport at
  64 KiB, and give body delivery 250 ms.
- Make topology choose the safe default. An in-process cluster uses the existing map-only HRW
  commit because every physical shard is already local. A remote cluster uses
  `rebalance_and_move_with` by default, recovering/fencing/draining/flipping each changed position
  before committing its assignment. `move:true` may request that remote behavior explicitly;
  `move:false` on a remote cluster fails with `409 unsafe_rebalance_mode` before mutation.
- Accept optional `max_parallel` only when the selected operation moves data. It must be a positive
  integer. The default is one; larger values retain ADR-095's conflict-free waves and move-ledger
  serialization for shared endpoints.
- Return one stable response with `acknowledged`, final observed control-state `version`,
  `moved_data`, `reassigned`, `moved`, `failed`, and `not_attempted`. Map-only success has empty move
  arrays. A data-moving pass that stops at one failed wave remains a resumable HTTP 200 with
  `acknowledged:false`; completed positions remain valid and a retry converges the rest.
- Read and attest the authoritative control version after the workflow. If planning, movement,
  commit, or final attestation fails, fail loud with the typed backend status and a sanitized
  reason directing the operator to `/_cluster/state`. Detailed endpoint/transport failures stay in
  server logs.
- Admit one operator-triggered whole-cluster rebalance per coordinator. The owned permit stays with
  the blocking worker through its final state read, including after HTTP disconnect. One request's
  internal `max_parallel` concurrency remains available.
- Accept exactly one manager-timeout spelling, default/max 30 seconds. Zero performs a non-waiting
  permit/topology-lock probe. Positive values bound admission, blocking-pool queueing, and
  topology/cluster-lock waiting until the workflow atomically starts. A queued worker is cancelled
  at the deadline and cannot mutate later. Once started, the request waits for the exact terminal
  report; manager timeout never pretends to cancel a live handoff.
- Run every control read, HRW plan/commit, and data-moving workflow off Tokio request workers under
  the existing shared topology barrier. Descriptor mutation remains exclusive with the workflow;
  ADR-095's internal disjoint move concurrency is unchanged.
- Return structured `Cache-Control: no-store` responses and fixed
  `cluster_rebalance` request/duration telemetry for every route-reached outcome.

## Consequences

A bodyless `POST /_cluster/rebalance` now means the operator intent its name implies in both
topologies. In-process mode cheaply updates its advisory map. Remote mode moves data before routing
authority changes; the API no longer offers the known unsafe map-only shortcut there. Existing
remote callers that deliberately depended on the old default incur physical movement, which is the
correctness-preserving behavior.

This endpoint is synchronous with respect to the whole selected workflow, unlike ES/OpenSearch
reroute. A remote pass may therefore outlive the manager-start timeout after it begins.
Disconnecting after that atomic start does not cancel or multiply it: the independently supervised
worker finishes while retaining the single admission slot, and the idempotent endpoint can be
inspected/retried afterward. Disconnecting before start cancels the queued gate, so a blocking-pool
worker cannot mutate later unless it atomically started before cancellation.

Map-only rebalance still commits changed assignments as the established sequence of control
proposals. That sequence is safe only in-process; if a proposal or the final state read fails, the
API does not claim success. Retrying recomputes the deterministic diff and converges it.

## Safety and proof

The matching/routing safety proof is unchanged. Remote positions use ADR-090/094's proven
move-then-commit path and ADR-095's conflict-aware waves; a failed move does not commit an empty
owner, while prior completed moves remain data-bearing and resumable. The REST topology resolver
prevents the only unsafe dispatch (`remote + map-only`) before the start gate opens.

Focused lean and distributed handler tests prove changed-placement/version success, topology-safe
mode selection, strict method/query/media/object/field controls, positive parallelism, body size
and absolute body deadlines, zero/positive/closed admission, topology-lock timeouts, blocking-pool
queue cancellation, off-runtime execution, post-start manager-timeout semantics, disconnect-retained
admission and completion, fixed telemetry/no-store headers, and sanitized fail-loud control errors.
Existing allocator, handoff, replicated-group rebalance, reconcile, topology-resolution, Raft, and
multi-machine suites continue to prove the underlying placement and movement mechanisms.
