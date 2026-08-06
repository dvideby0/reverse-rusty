# `POST /_cluster/gc` — Reclaim orphaned shard slots

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

This strict **native** operation runs one idempotent orphan-slot garbage-collection sweep
(ADR-096/173). Data-moving reassignment deliberately leaves a fenced source slot behind until the
durable assignment and live routing agree. GC lists every registered data node and drops a hosted
slot only when its position is assigned elsewhere **and** the coordinator's live routing no longer
reaches that node.

```bash
curl -X POST \
  'localhost:9200/_cluster/gc?cluster_manager_timeout=5s'
```

The endpoint is available only in a `distributed` build on an **assignment-routed remote
coordinator** with `--route-by-assignments` and `--control-endpoint`. Both the initial CLI-seeded
form and a later resolve-only form are safe: GC never changes the assignment map, and its live-route
keep set preserves the CLI-seeded coordinator's active backings.

- A static endpoint-order coordinator returns `409 gc_assignment_routing_required`; its control
  document is not guaranteed to be a complete node inventory.
- An in-process cluster returns `400 gc_requires_remote_cluster`; it has no remote slots to reclaim.
- A build without `distributed` returns a structured 501 after request validation.

The request is bodyless. A complete response is HTTP 200:

```json
{
  "acknowledged": true,
  "completed": true,
  "version": 52,
  "took": 184,
  "took_ms": 184.73,
  "dropped": [
    {"node": 11, "shard": 3, "num_queries": 4200}
  ],
  "pending_disk_cleanup": [],
  "kept_live_routed": [],
  "skipped_unassigned": [],
  "failed": [],
  "skipped_nodes": []
}
```

`dropped` lists slots removed from the node's serving map and atomically renamed out of the
`shard_<id>/` namespace. `num_queries` is the listing-time live count and is observational. The
rename is transactional with map removal: if it fails, the node restores the fence, keeps the slot
hosted, and the drop appears in `failed`. A successful rename makes the slot impossible to reattach
on restart. The final trash-directory delete is best-effort; if it does not finish, the slot also
appears in `pending_disk_cleanup` and the response is incomplete.

Every later node inventory retries pending trash deletion and reports any survivors, so a second
sweep cannot acknowledge completion merely because the slot already left the serving map. The
node's next boot uses the same retry. A carried-over pending entry has `num_queries: 0` because no
hosted slot remains from which to recover the earlier observational count. GC wire protocol v2
provides this distinction and inventory; an older ambiguous node is placed in `skipped_nodes`.

`kept_live_routed` is an intentional terminal outcome. It identifies a slot not named for that node
by the durable map but still reached by current live routing, such as a raw handoff or an
uncommitted move. GC never drops it. A slot named as a committed primary or replica is also kept but
omitted from the response because it is ordinary placement, not an orphan candidate.

`acknowledged` and `completed` are identical. They are true only when every registered data node
was classified, every classifiable orphan was removed, no position lacked a committed assignment,
and no physical trash deletion remains pending. A partial sweep still returns HTTP 200 because its
successful drops are final and the operation is safely resumable:

```json
{
  "acknowledged": false,
  "completed": false,
  "version": 52,
  "took": 912,
  "took_ms": 912.41,
  "dropped": [],
  "pending_disk_cleanup": [
    {
      "node": 11,
      "shard": 3,
      "num_queries": 4200,
      "warning": "the slot left the serving namespace but physical trash deletion is pending; a later sweep or node restart will retry it"
    }
  ],
  "kept_live_routed": [],
  "skipped_unassigned": [
    {
      "node": 12,
      "shard": 7,
      "num_queries": 18,
      "warning": "the committed map has no assignment for this position, so the slot was kept fail-safe"
    }
  ],
  "failed": [
    {
      "node": 13,
      "shard": 2,
      "num_queries": 95,
      "reason": "this orphan slot was not reclaimed; inspect server logs and retry the idempotent GC sweep"
    }
  ],
  "skipped_nodes": [
    {
      "node": 14,
      "reason": "this node could not be classified; inspect server logs and retry the idempotent GC sweep"
    }
  ]
}
```

`skipped_unassigned` is fail-safe: without a committed assignment, the map cannot prove that a
copy exists elsewhere. `failed` covers a per-slot fence, lease, identity, or drop failure.
`skipped_nodes` covers an unreachable, incompatible, or feature-model-divergent node; nothing on
that node is touched. Detailed endpoint, mesh, fingerprint, and transport diagnostics remain in
server logs rather than the client response. Retry the same sweep after correcting the cause.

`version` is a final observation of the committed `ClusterState` application version. GC does not
normally change it; the value attests the map against which the terminal report was observed. It is
not a Raft term/log index, checkpoint epoch, feature-model version, or placement generation. `took`
is the integer millisecond compatibility field; `took_ms` preserves fractional milliseconds.

Supported query controls:

- `cluster_manager_timeout` is the OpenSearch-inclusive spelling.
- `master_timeout` is the Elasticsearch and legacy OpenSearch spelling.

They are aliases; specify at most one. Values use the shared time syntax (`nanos`, `micros`, `ms`,
`s`, `m`, `h`, or `d`), default to 30 seconds, and cannot exceed 30 seconds. Exact `0` performs a
non-waiting admission and topology-lock probe. A positive value covers the shared maintenance slot,
dedicated-worker dispatch, and topology/cluster-lock waiting until the sweep atomically starts.

The manager timeout does **not** cancel a started sweep. Once the first destructive classification
begins, the request waits for the terminal report. A disconnect after start drops only the response;
the independently supervised worker retains admission and completes. A disconnect before start
cancels the queued gate. The familiar ES/OS overall `timeout` parameter is rejected.

Manual GC, manual reconcile, and the opt-in `--reconcile-interval-secs` reconcile+GC loop share one
maintenance admission slot. Duplicate whole-cluster sweeps therefore cannot accumulate. Graceful
shutdown aborts the loop, then acquires and retains the shared slot before durability cleanup, so a
detached manual sweep or loop epilogue finishes first. The orchestrator termination budget remains
an outer hard limit and must cover the HTTP drain plus the largest expected sweep.

The route accepts only `POST`, caps body transport at 64 KiB, and gives body delivery 250 ms. Every
non-empty body is rejected. Unknown or duplicate query controls, duplicate timeout aliases,
`accept_data_loss`, `dry_run`, and overall `timeout` are rejected before topology access. Every
route-reached response is structured JSON, `Cache-Control: no-store`, and observed under the fixed
`cluster_gc` metric label. The cluster mutation auth gate applies.

Elasticsearch and OpenSearch expose `DELETE /_dangling/{index_uuid}` for a named dangling **index**
and require `accept_data_loss=true`. That operation does not prove and sweep redundant physical
copies outside a durable-plus-live routing keep set
([Elasticsearch delete dangling index](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-dangling-indices-delete-dangling-index),
[OpenSearch dangling indexes](https://docs.opensearch.org/latest/api-reference/index-apis/dangling-index/)).
Reverse Rusty therefore keeps `/_cluster/gc` native, does not expose a `/_dangling` alias, and does
not adopt `accept_data_loss`. Only the manager-timeout spellings are shared because their
admission/start meaning maps exactly.

---

Back to the [REST API reference](../../api.md).
