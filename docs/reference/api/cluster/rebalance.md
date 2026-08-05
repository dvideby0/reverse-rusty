# `POST /_cluster/rebalance` — Converge desired placement

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

This is a strict **native** whole-cluster topology workflow (ADR-042/090/095/166). It recomputes the
deterministic rendezvous-hash placement from current data-node membership and the configured
replication factor, then converges every changed logical position.

```bash
curl -X POST \
  'localhost:9200/_cluster/rebalance?cluster_manager_timeout=5s'
```

The empty-body default is safe for the assembled topology:

- An in-process cluster commits only the advisory shard→node map. Every physical shard is already
  co-resident, so no data copy is necessary.
- A resolve-only remote cluster peer-recovers each desired target, fences and drains the
  source, flips live routing, then commits the new assignment. It moves data before changing
  durable routing authority.
- A CLI-seeded assignment-routed coordinator returns `409 rebalance_resolve_only_required`. Its
  current live sources are authoritative, but changing the map would make the endpoint-list guard
  reject its next restart. Restart with the committed `--shards` count and no
  `--shard-endpoint` arguments before retrying.
- A static endpoint-order remote coordinator returns
  `409 rebalance_routing_not_authoritative` without planning or mutation. Restart it with
  `--route-by-assignments` and `--control-endpoint`; otherwise the committed map cannot safely name
  the live source for a handoff.

Optional JSON body:

| Field | Required | Contract |
|---|---|---|
| `move` | no | Omit for the topology-safe default. `true` explicitly selects resolve-only remote data movement. `false` is accepted only in-process; resolve-only remote mode returns `409 unsafe_rebalance_mode`, CLI-seeded mode returns `409 rebalance_resolve_only_required`, and static remote mode returns `409 rebalance_routing_not_authoritative`. |
| `max_parallel` | no | Positive integer conflict-free wave width for a resolve-only remote data-moving pass; default 1. It is rejected when the selected operation is map-only. |

```bash
curl -X POST localhost:9200/_cluster/rebalance \
  -H 'Content-Type: application/json' \
  -d '{"move":true,"max_parallel":4}'
```

`max_parallel` does not make conflicting moves overlap. ADR-095 partitions changed positions into
waves and the move ledger serializes any operations sharing a source or target node. A failure
finishes the current wave, stops later waves, and leaves every completed position in a valid
move-then-commit state.

Complete in-process response:

```json
{
  "acknowledged": true,
  "version": 47,
  "moved_data": false,
  "reassigned": 2,
  "moved": [],
  "failed": null,
  "not_attempted": []
}
```

Complete resolve-only remote response uses `moved_data:true`; `reassigned` is the number of
positions newly converged and `moved` lists their numeric positions. A listed RF=1 position may be a
commit-only reconciliation when an earlier uncommitted flip already placed its target live; no stale
source recopy occurs. `version` is a final linearizable observation of the committed `ClusterState`
application version after the complete or resumable workflow. It is not a Raft term/log index,
checkpoint epoch, feature-model version, or placement generation.

A per-position remote failure remains a resumable HTTP 200:

```json
{
  "acknowledged": false,
  "version": 49,
  "moved_data": true,
  "reassigned": 2,
  "moved": [0, 3],
  "failed": {
    "position": 5,
    "reason": "data movement stopped before this position reached an attested commit; inspect server logs and retry the idempotent rebalance"
  },
  "not_attempted": [7, 8]
}
```

Detailed endpoint and transport diagnostics stay in server logs. Retry the same request: placement
is deterministic, completed positions are already converged, and the next pass resumes the
remaining diff. A planning, control-plane, hard movement, or final-version-attestation error fails
loud with a structured non-200 response and directs the operator to inspect
`GET /_cluster/state`; it never returns a successful partial result without the report above.

Supported query controls:

- `cluster_manager_timeout` is the OpenSearch-inclusive spelling.
- `master_timeout` is the Elasticsearch and legacy OpenSearch spelling.

They are aliases; specify at most one. Values use the shared time syntax (`nanos`, `micros`, `ms`,
`s`, `m`, `h`, or `d`), default to 30 seconds, and cannot exceed 30 seconds. Exact `0` performs a
non-waiting rebalance-admission and topology-lock probe. A positive value covers admission,
dedicated-worker dispatch, and topology/cluster-lock waiting until the workflow starts.

The manager timeout does **not** cancel a started data move. Once the worker atomically starts, the
request waits for its terminal report because peer recovery/fence/drain/flip cannot be safely
cancelled at an arbitrary HTTP deadline. A disconnect after start drops only the response: the
independently supervised worker retains the single rebalance admission slot and completes. A
disconnect before start cancels its queued gate, so a delayed worker cannot mutate later. The
familiar ES/OS overall `timeout` control is rejected because their reroute
APIs return after an allocation decision, whereas this native endpoint synchronously waits for
physical movement.

Graceful coordinator shutdown stops accepting traffic, then waits to acquire and retain that same
rebalance slot before durability cleanup and process exit. An already-started handoff therefore
finishes across HTTP drain instead of being terminated after fencing or a live-routing flip.
The container orchestrator's termination budget is an outer hard limit: set Compose
`RR_COORDINATOR_STOP_GRACE_PERIOD` or Helm
`coordinator.terminationGracePeriodSeconds` to at least the 30-second drain plus the largest
expected `O(corpus)` handoff. A shorter grace can still SIGKILL the process.

The route accepts only `POST`, caps body transport at 64 KiB, and gives body delivery 250 ms. An
empty body needs no content type; a non-empty body requires `application/json` or
`application/*+json`. Unknown/duplicate/null fields, non-object JSON, zero parallelism, unsupported
query controls, and ignored field combinations are rejected before topology mutation. Every
route-reached response is structured JSON, `Cache-Control: no-store`, and observed under the fixed
`cluster_rebalance` metric label.

Elasticsearch and OpenSearch expose `POST /_cluster/reroute` for explicit shard allocation
commands, followed by their allocation engines' normal balancing. They do not expose Reverse
Rusty's whole-cluster HRW trigger
([Elasticsearch cluster reroute](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute),
[OpenSearch cluster reroute](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)).
Reverse Rusty therefore keeps its native path and does not accept `commands`, `dry_run`, `explain`,
`retry_failed`, or `metric`. Only the manager-timeout spellings are shared because their waiting
semantics align.

---

Back to the [REST API reference](../../api.md).
