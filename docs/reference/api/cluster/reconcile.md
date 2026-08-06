# `POST /_cluster/reconcile` — Converge desired placement

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

This strict **native** operation runs one idempotent controller pass (ADR-092/094/095/172). It
computes deterministic rendezvous-hash placement from current data-node membership and the
configured replication factor, then attempts every divergent logical position. Unlike
`/_cluster/rebalance`, it continues after per-position failures so an unattended-style pass makes
all safe progress and returns a complete resumable report.

```bash
curl -X POST \
  'localhost:9200/_cluster/reconcile?cluster_manager_timeout=5s'
```

The endpoint is available only in a `distributed` build on a **resolve-only**, assignment-routed
remote coordinator: `--route-by-assignments`, `--control-endpoint`, the committed `--shards` count,
and no `--shard-endpoint`. This is the only assembly where the durable assignment map identifies
the live movement sources and remains the restart topology after the map changes.

- A static endpoint-order coordinator returns `409 reconcile_routing_not_authoritative`.
- A CLI-seeded assignment-routed coordinator returns `409 reconcile_resolve_only_required`; its
  next guarded restart would still compare against the stale endpoint order.
- An in-process cluster returns `400 reconcile_requires_remote_cluster`; use
  `POST /_cluster/rebalance` for its advisory map.
- A build without `distributed` returns a structured 501 after request validation.

The optional JSON body has one field:

| Field | Required | Contract |
|---|---|---|
| `max_parallel` | no | Positive integer conflict-free wave width; default 1. Each parallel move consumes one OS thread and recovery bandwidth, so size it to the mesh and disks. |

```bash
curl -X POST localhost:9200/_cluster/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"max_parallel":4}'
```

Parallelism never overlaps conflicting endpoint footprints. The wave planner separates moves that
share a source or target, and the move ledger independently serializes conflicts. The pass still
attempts later waves after an individual position fails.

A converged response is HTTP 200:

```json
{
  "acknowledged": true,
  "converged": true,
  "version": 52,
  "took": 184,
  "took_ms": 184.73,
  "reconciled": [0, 3],
  "skipped": [],
  "uncommitted": [],
  "failed": []
}
```

`reconciled` lists desired positions whose assignment committed during this pass. For RF=1 this
may include a commit-only recovery when an earlier uncommitted flip already made the desired target
the attested live authority; the retry does not copy stale source data over it. `skipped` lists
planned positions that another operation converged before their turn. A pass against an already
converged map returns all four lists empty and does not advance the control version.

`version` is a final observation of the committed `ClusterState` application version after the
complete or resumable workflow. It is not a Raft term/log index, checkpoint epoch, feature-model
version, or placement generation. `took` is the integer millisecond compatibility field;
`took_ms` preserves fractional milliseconds.

A pass that made only partial progress also returns HTTP 200 because each listed position is an
individually valid, resumable outcome:

```json
{
  "acknowledged": false,
  "converged": false,
  "version": 54,
  "took": 912,
  "took_ms": 912.41,
  "reconciled": [0, 3],
  "skipped": [4],
  "uncommitted": [
    {
      "position": 5,
      "from": 11,
      "to": 14,
      "warning": "live routing reached the target but the durable assignment did not; retry promptly before coordinator restart"
    }
  ],
  "failed": [
    {
      "position": 8,
      "reason": "this position did not converge; inspect server logs and retry the idempotent reconcile pass"
    }
  ]
}
```

An `uncommitted` position remains exact on the running coordinator because live routing already
reaches the target, but the durable map is stale. Restore control-plane writes and retry promptly
before coordinator restart; newer writes can make the old durable owner stale. Detailed endpoint,
mesh, and transport diagnostics remain in server logs. `failed` means that position did not reach
the desired terminal state; retrying the same deterministic pass is safe.

A planning, control-plane-read, worker, or final-version-attestation failure returns a structured
non-200 response and directs the operator to inspect `GET /_cluster/state`. It never returns a
successful response without a terminal version/report.

Supported query controls:

- `cluster_manager_timeout` is the OpenSearch-inclusive spelling.
- `master_timeout` is the Elasticsearch and legacy OpenSearch spelling.

They are aliases; specify at most one. Values use the shared time syntax (`nanos`, `micros`, `ms`,
`s`, `m`, `h`, or `d`), default to 30 seconds, and cannot exceed 30 seconds. Exact `0` performs a
non-waiting admission and topology-lock probe. A positive value covers the shared reconcile slot,
dedicated-worker dispatch, and topology/cluster-lock waiting until the pass atomically starts.

The manager timeout does **not** cancel a started pass. Once movement begins, the request waits for
the terminal report because recovery/fence/drain/flip cannot safely stop at an arbitrary HTTP
deadline. A disconnect after start drops only the response; the independently supervised worker
retains admission and completes. A disconnect before start cancels the queued gate. The familiar
ES/OS overall `timeout` parameter is rejected.

Manual reconcile, manual `POST /_cluster/gc`, and the opt-in `--reconcile-interval-secs` loop share
one maintenance admission slot, so duplicate whole-cluster passes cannot accumulate. The loop
itself is accepted only in the same resolve-only topology. Graceful shutdown first aborts the loop,
then acquires and retains the shared slot before durability cleanup; an already-running reconcile
pass or GC sweep therefore finishes.
The orchestrator termination budget remains an outer hard limit and must cover the HTTP drain plus
the largest expected `O(corpus)` pass.

The route accepts only `POST`, caps body transport at 64 KiB, and gives body delivery 250 ms. An
empty body needs no content type; a non-empty body requires `application/json` or
`application/*+json`. Unknown/duplicate/null fields, non-object JSON, zero parallelism, unsupported
query controls, and duplicate timeout aliases are rejected before topology access. Every
route-reached response is structured JSON, `Cache-Control: no-store`, and observed under the fixed
`cluster_reconcile` metric label.

Elasticsearch and OpenSearch expose `POST /_cluster/reroute` for explicit named-index allocation
commands and allocation-engine decisions. Their `retry_failed=true` performs one retry round for
shards blocked by prior allocation failures; it does not mean “recompute and physically converge
the entire deterministic HRW map”
([Elasticsearch cluster reroute](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute),
[OpenSearch cluster reroute](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-reroute/)).
Reverse Rusty therefore keeps `/_cluster/reconcile` native and rejects `commands`, `retry_failed`,
`dry_run`, `explain`, `metric`, and overall `timeout`. Only the manager-timeout spellings are shared
because their admission/start meaning maps exactly.

---

Back to the [REST API reference](../../api.md).
