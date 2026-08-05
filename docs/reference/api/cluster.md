# Cluster control APIs

> [REST API hub](../api.md) · [Clustering design](../../design/clustering-and-scaling.md)

Strict native membership and topology-changing operations. These routes do not fabricate
Elasticsearch/OpenSearch semantics where the underlying control model differs.

| API | What it does | Availability |
|---|---|---|
| [`POST /_cluster/nodes`](cluster/register-node.md) | Commit or replace a member descriptor without moving data or changing voters. | Coordinator with authoritative control plane |
| [`DELETE /_cluster/nodes/{id}`](cluster/deregister-node.md) | Remove an unassigned, non-voter descriptor after fail-closed guards. | Coordinator with authoritative control plane |
| [`POST /_cluster/rebalance`](cluster/rebalance.md) | Converge deterministic whole-cluster placement through the topology-safe local or remote workflow. | In-process or authoritative resolve-only coordinator |
| [`POST /_cluster/resize`](cluster/resize.md) | Strictly rebuild and atomically replace an in-process ring under a bounded new shard count. | In-process cluster only |
| [`POST /_cluster/resync`](cluster/resync.md) | Strictly re-drive queued partial-apply mutations against only their failed positions. | Coordinator mode |
| [`POST /_cluster/handoff`](cluster/handoff.md) | Explicitly uncommitted, raw-endpoint live-routing handoff for low-level deployment tests. | Distributed build |
| [`POST /_cluster/reassign`](cluster/reassign.md) | Attest the live owner, move one position when needed, then commit or reconcile the assignment. | Distributed resolve-only authoritative coordinator |
| [`POST /_cluster/reconcile`](cluster/reconcile.md) | Run one resumable desired-placement reconciliation pass. | Distributed authoritative coordinator |
| [`POST /_cluster/gc`](cluster/gc.md) | Reclaim fenced, unrouted orphan slots without dropping live or unassigned data. | Distributed authoritative coordinator |

Read-only control state is documented under
[`GET|HEAD /_cluster/state`](observability/cluster-state.md). Logical shard counts are documented
under [`GET /_cat/shards`](observability/cat-shards.md).
