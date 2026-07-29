# Observability APIs

> [REST API hub](../api.md)

Machine-readable state, human-readable CAT views, readiness, and metrics.

| API | What it does | Availability |
|---|---|---|
| [`GET /_stats`](observability/stats.md) | Strict no-store JSON engine or cluster metrics with truthful physical/live counts. | Single-node and coordinator modes |
| [`GET /_cat/stats`](observability/cat-stats.md) | Selectable and sortable human-readable stats table. | Single-node only |
| [`GET /_cat/segments`](observability/cat-segments.md) | Per-segment LSM detail with strict CAT controls. | Single-node only |
| [`GET /_cat/shards`](observability/cat-shards.md) | Logical shard counts and committed assignments. | Coordinator mode |
| [`GET\|HEAD /_cluster/state[/_all\|/version]`](observability/cluster-state.md) | Authoritative control-plane state or exact version projection. | Coordinator mode |
| [`GET\|HEAD /_health`](observability/health.md) | Waitable, fail-loud readiness status. | Single-node and coordinator modes |
| [`GET\|HEAD /_metrics`](observability/metrics.md) | Prometheus text-format metrics with complete coordinator collection. | Single-node and coordinator modes |
