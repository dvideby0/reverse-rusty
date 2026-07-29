# ADR-165: Node-deregistration REST API contract

> [Clustering — replication & control plane decisions](areas/clustering-replication-and-control-plane.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`DELETE /_cluster/nodes/{id}` used Axum's generic numeric path extractor and synchronously called
`ClusterEngine::deregister_node` on a Tokio request worker. It ignored query and body input,
accepted reserved bootstrap id zero, inherited the server-wide 100 MiB body ceiling, and had no
body or control-write deadline, explicit method/cache/telemetry contract, admission bound, or
committed version in the response. Extractor and backend failures did not share one sanitized
route contract.

The operation was also easy to overread. `RemoveNode` removes only a descriptor. It deliberately
does not change Raft voters, shard assignments, live routing, physical data, or process lifecycle.
Those separations are necessary, but the one-line API reference called it only an idempotent member
removal and did not state its safe-removal preconditions.

## Compatibility boundary

Elasticsearch removes a node operationally by relocating shards, stopping the process, and
handling master-eligible voting configuration as needed
([Elastic node removal](https://www.elastic.co/docs/deploy-manage/maintenance/add-and-remove-elasticsearch-nodes)).
OpenSearch likewise treats voting exclusions and node lifecycle as separate cluster procedures
([OpenSearch voting exclusions](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-voting-configuration-exclusions/)).
Neither product exposes a REST operation that simply deletes a member descriptor from cluster
state.

Keep the Reverse Rusty native path and descriptor-only semantics. Do not alias it to `/_nodes`,
the Elastic shutdown API, voting exclusions, or a data-moving operation. Adopt only
`cluster_manager_timeout` and its `master_timeout` alias because this endpoint really does wait for
one authoritative manager write.

## Decision

- Admit only `DELETE` with a structured 405 and `Allow: DELETE`. Require a positive canonical
  decimal `u64` path id, reserve bootstrap id zero, reject all query fields except one manager
  timeout alias, accept no body, cap body transport at 64 KiB, and bound delivery at 250 ms.
- Preserve `RemoveNode` as one descriptor-only committed transition. Repeating an absent id stays
  state-idempotent but commits another transition and advances the application version, provided
  the id has no dangling voter or assignment references.
- Before proposing, fail with `409 node_in_use` when the id remains a voter or is a primary/replica
  in any shard assignment. Hold the server's dedicated topology-operation lock across this check
  and proposal, serializing it against registration, assignment, manual movement, and unattended
  reconciliation on the supported single active coordinator without excluding serving reads.
- Return `{acknowledged, version, node_id}`. Change `ClusterEngine::deregister_node` to return the
  exact `StateVersion` produced by `ControlPlane::propose` instead of discarding it.
- State explicitly that the operation changes only `nodes`: it does not change `voters`,
  `assignments`, live routing, physical data, or process state. Drain a populated data node before
  deletion by upserting its existing id/address with role `manager`, then running data-moving
  reconcile or rebalance. That keeps the old endpoint resolvable while excluding it from desired
  data placement. A manager voter must leave through joint consensus; map-only rebalance is unsafe.
- Accept exactly one manager-timeout spelling, with a 30-second default and maximum. Zero performs
  non-queuing admission and, once admitted, runs one write to completion. A positive timeout covers
  shared administrative admission and proposal execution.
- Move the administrative permit, cluster-lock wait, and synchronous consensus proposal to a
  blocking worker. An atomic queued/start gate prevents blocking-pool delay from beginning a
  proposal after its deadline. A timeout that cancels queued work says no proposal began; a timeout
  after start reports an unknown outcome and directs callers to inspect cluster state. The
  non-cancellable detached proposal retains admission until completion.
- Return no-store structured responses and fixed-label request telemetry on every route-reached
  outcome. Log detailed control failures server-side, but return only a sanitized fail-loud
  `control_plane_error`.

## Consequences

Automation can distinguish an acknowledged descriptor commit from a request rejected before
proposal start and can correlate the response with the authoritative state version. It cannot
mistake the response for a completed data move, routing transition, process shutdown, or voter
change.

Drain-before-delete preserves the endpoint that live movement must resolve. Temporarily changing a
data node's descriptor role to `manager` makes it ineligible for desired data placement without
adding a Raft vote; reconcile can still resolve the current source by id and address. Only after
assignments converge—and any vote leaves through joint consensus—can descriptor removal commit.
The API cannot acknowledge the broken intermediate state where an assignment names a descriptor it
just removed.

The shared administrative slot means a slow manager write can temporarily delay state/stats
introspection. It cannot create an unbounded blocking-worker queue, and no blocking consensus or
topology/cluster-lock wait occupies a Tokio request worker. The topology barrier uses shared guards
for movement, so disjoint move concurrency remains governed by the existing move ledger rather than
being globally serialized.

## Safety and proof

The consensus transition remains the established deterministic `RemoveNode`; this change does not
alter assignment, routing, matching, query data, physical recovery, or Raft voter membership.
Protecting id zero prevents the public route from deleting the bootstrap descriptor while its
voter and every genesis assignment remain live. The voter/assignment precondition prevents loss of
the source endpoint needed by `reassign_and_move` and replicated group movement. Returning the
proposal's own version avoids a racy follow-up state read.

Focused handler tests prove exact version/id responses, unchanged voters and assignments,
state-idempotent repeated removal, no-proposal voter/assignment conflicts, strict
query/path/method/body handling, size and body deadlines, no-store telemetry,
zero/positive/closed admission behavior, off-runtime proposal execution, deadline-detached
completion with retained admission and unknown outcome, and sanitized control failure. Existing
control-plane differential, durable Raft restart, topology-resolution, data-moving
reassignment/reconcile, health, and allocator suites continue to prove the underlying state and
serving semantics.
