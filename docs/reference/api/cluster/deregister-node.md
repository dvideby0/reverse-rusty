# `DELETE /_cluster/nodes/{id}` — Deregister a member descriptor

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

This is a strict **native** control-plane mutation (ADR-037/165). It removes one logical member
descriptor from the authoritative `ClusterState` document. A successful response means the
configured control-plane backend committed `RemoveNode` (a quorum when the durable Raft backend is
attached). It does not mean a process stopped, data moved, routing changed, or a manager left the
Raft voter set. The request fails closed while the id remains a voter or appears in any shard
assignment.

```bash
curl -X DELETE \
  'localhost:9200/_cluster/nodes/7?cluster_manager_timeout=5s'
```

The path id must be a positive unsigned 64-bit integer. `0` is the reserved bootstrap in-process
manager and cannot be deregistered through REST. Encoded, signed, named, empty, and out-of-range
identities are rejected before consensus work starts.

Successful response:

```json
{
  "acknowledged": true,
  "version": 43,
  "node_id": 7
}
```

`version` is the exact committed `ClusterState` application version returned by the removal
proposal. It is the same identity exposed by `GET /_cluster/state`, not a Raft term or log index,
checkpoint epoch, feature-model version, or placement generation.

Deregistration is state-idempotent when the id is unreferenced: deleting an already-absent positive
id leaves the descriptor set unchanged. It is still a committed control transition and advances
`version`, so a repeated successful request returns a later version. A dangling voter or assignment
reference is rejected even when the descriptor is already absent.

Before proposing, the coordinator verifies that the id is absent from both `voters` and every
assignment's `primary` and `replicas`. An in-use id returns `409 node_in_use` without proposing or
advancing `version`. On the supported single active coordinator, that check and removal hold the
dedicated topology-operation lock, serializing them against registration, assignment, manual
reconcile/rebalance, and the unattended reconcile loop.

For a populated data node, drain it before deletion:

1. If the remote coordinator still has `--shard-endpoint` arguments, first switch its deployment to
   resolve-only (`--route-by-assignments`, the control endpoints, the committed `--shards` count,
   and no shard-endpoint arguments), restart it, and wait for green health.
2. Upsert the same id and address through `POST /_cluster/nodes` with role `manager`. This changes
   placement eligibility while preserving the source endpoint needed by live handoff; it does
   **not** add the node to the Raft voter set.
3. On that resolve-only coordinator, run `POST /_cluster/reconcile` or bodyless
   `POST /_cluster/rebalance` (an explicit `{"move":true}` is equivalent). The rebalance route
   rejects remote map-only, CLI-seeded, and static endpoint-order routing.
4. Verify through `GET /_cluster/state` that no assignment names the id. If it is a voter, remove
   it separately through the control-plane joint-consensus procedure.
5. Delete the descriptor, then stop the process or remove its physical data.

A successful operation changes only `nodes`:

- It does not update `voters`. Removing a manager descriptor does not remove a Raft vote; change
  manager membership separately through the control-plane joint-consensus procedure.
- It does not rewrite `assignments`, live routing, recover or remove physical shard data, or stop
  the node process.

This separation is deliberate: safely moving a populated node can partially progress and needs
the existing move/fence/reconcile protocol, while changing a voter uses Raft joint consensus.
Folding either into a descriptor delete would falsely acknowledge stronger semantics than one
atomic `RemoveNode` proposal can provide.

Supported query controls:

- `cluster_manager_timeout` is the OpenSearch-inclusive spelling.
- `master_timeout` is the Elasticsearch and legacy OpenSearch spelling.

They are aliases; specify at most one. Values use the shared time syntax (`nanos`, `micros`, `ms`,
`s`, `m`, `h`, or `d`), default to 30 seconds, and cannot exceed 30 seconds. Exact `0` performs a
non-queuing administrative-admission probe; when admitted, one proposal runs to completion.
Positive values cover administrative admission, topology/cluster-lock waiting, and the synchronous
control-plane proposal.

A timeout before proposal start returns `408 node_deregistration_timeout` and states that nothing
was started. At the deadline the server atomically cancels a still-queued blocking worker, so
blocking-pool delay cannot start the proposal afterward. Once a proposal starts, the synchronous
consensus client cannot be cancelled safely: an HTTP deadline may return 408 while the detached
worker later commits. That response says the outcome is unknown. Inspect `GET /_cluster/state`
before retrying. The detached worker retains the shared administrative permit until it finishes,
bounding slow-manager work.

The route accepts only `DELETE`, accepts no request body, caps body transport at 64 KiB, and gives
body delivery 250 ms. Every route-reached response is structured JSON, `Cache-Control: no-store`,
and observed under the fixed `cluster_node_deregister` metric label. In-use conflicts are
actionable but do not expose endpoints. Control-plane failures fail loud, while backend addresses
and transport details remain server-side.

Elasticsearch and OpenSearch do not expose an equivalent REST descriptor-removal operation. Their
node-removal procedures coordinate allocation, process lifecycle, and—when applicable—voting
configuration
([Elasticsearch add/remove nodes](https://www.elastic.co/docs/deploy-manage/maintenance/add-and-remove-elasticsearch-nodes),
[OpenSearch voting exclusions](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-voting-configuration-exclusions/)).
Reverse Rusty therefore does not alias this operation to `/_nodes` or claim safe-shutdown
semantics. Only the manager-timeout spellings are adopted because they map exactly to waiting for
this control-plane write.

---
