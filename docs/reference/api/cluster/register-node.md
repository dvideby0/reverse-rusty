# `POST /_cluster/nodes` — Register or replace a member

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

This is a strict **native** control-plane mutation (ADR-037/164). It commits one node descriptor to
the authoritative `ClusterState` document. A successful response means the configured control-plane
backend committed the descriptor (a quorum when the durable Raft backend is attached); it does not
mean the endpoint was contacted, data moved, or a manager joined the Raft voter set.

```bash
curl -X POST \
  'localhost:9200/_cluster/nodes?cluster_manager_timeout=5s' \
  -H 'Content-Type: application/json' \
  -d '{"id":7,"addr":"https://shard-7.example.net:50051","role":"data"}'
```

Request fields:

| Field | Required | Contract |
|---|---|---|
| `id` | yes | Positive unsigned logical identity. `0` is reserved for the bootstrap in-process manager. |
| `addr` | yes | HTTP(S) mesh endpoint origin, at most 2 KiB, with a host, optional valid nonzero port, and no credentials, fragment, path, or query. Reachability is not probed. |
| `role` | no | `data` (default) or `manager`. `manager` means manager-eligible metadata only. |

Unknown or duplicate fields, nulls, malformed JSON, invalid roles, and any other address shape are
rejected before consensus work starts. The address is the gRPC endpoint recorded for the logical
node. Use `https` only when the coordinator has the corresponding mesh TLS client configuration.

Registration is an upsert by logical `id`. Repeating the same descriptor is state-idempotent but is
still a committed control transition and advances the application state version. Supplying a new
address or role replaces that descriptor. Replacement does not copy or verify data: an operator
changing an assigned data node's address must ensure the new endpoint is the same recovered logical
node, or first use the data-moving topology operations so assignment authority never points at an
empty slot.

Successful response:

```json
{
  "acknowledged": true,
  "version": 42,
  "node": {
    "id": 7,
    "addr": "https://shard-7.example.net:50051",
    "role": "data"
  }
}
```

`version` is the exact committed `ClusterState` application version returned by the proposal. It is
the same identity exposed as `version`/`epoch` by
[`GET /_cluster/state`](../observability/cluster-state.md), not a Raft term or log index,
checkpoint epoch,
feature-model version, or placement generation.

Registration changes only `nodes`:

- It does not update `voters`. A `manager` descriptor is eligible metadata; Raft membership changes
  use the separate control-plane joint-consensus mechanism.
- It does not change `assignments`, rebalance, recover, or move shard data. Use the documented
  rebalance/reassign/reconcile procedures after the physical node is ready.
- It does not prove the endpoint is reachable or holds compatible data. Cluster health and the
  data-moving operations remain the serving checks.

Supported query controls:

- `cluster_manager_timeout` is the OpenSearch-inclusive spelling.
- `master_timeout` is the Elasticsearch and legacy OpenSearch spelling.

They are aliases; specify at most one. Values use the shared time syntax (`nanos`, `micros`, `ms`,
`s`, `m`, `h`, or `d`), default to 30 seconds, and cannot exceed 30 seconds. Exact `0` performs a
non-queuing administrative-admission probe; when admitted, one proposal runs to completion.
Positive values cover administrative admission, topology/cluster-lock waiting, and the synchronous
control-plane proposal.

A timeout before proposal start returns `408 node_registration_timeout` and states that nothing was
started. At the deadline the server atomically cancels a still-queued blocking worker, so blocking
pool delay cannot start the proposal afterward. Once a proposal starts, the synchronous consensus
client cannot be cancelled safely: an HTTP deadline may return 408 while the detached worker later
commits. That response explicitly says the outcome is unknown. Inspect `GET /_cluster/state` for the
descriptor and version before retrying. The detached worker retains the shared administrative
permit until it finishes, bounding slow-manager work.

The route accepts only `POST`, requires `application/json` or `application/*+json`, caps the body at
64 KiB, and gives body delivery 250 ms. Every route-reached response is structured JSON,
`Cache-Control: no-store`, and observed under the fixed `cluster_node_register` metric label.
Control-plane failures fail loud, while backend addresses and transport details remain server-side.

Elasticsearch and OpenSearch expose observational node-info/stats APIs, not a REST membership
insertion operation
([Elasticsearch nodes info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-info),
[OpenSearch Nodes APIs](https://docs.opensearch.org/latest/api-reference/nodes-apis/index/)).
Reverse Rusty therefore does not alias this mutation to `/_nodes` or fabricate either product's
node response. Only the manager-timeout spellings are adopted because they map exactly to waiting
for this control-plane write.

---
