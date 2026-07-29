# `POST /_cluster/reconcile` — Converge desired placement

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

Run one controller-style pass that moves data until the committed shard-to-node map matches the
deterministic desired placement. The pass is idempotent and continues past per-position failures.
An optional `{"max_parallel": N}` body runs up to `N` conflict-free moves concurrently; an empty
body runs sequentially.

The response contains
`{acknowledged, converged, reconciled[], skipped[], uncommitted[], failed[]}`.
`acknowledged` is true only when the cluster fully converged. This is the manual one-shot form of
the opt-in `--reconcile-interval-secs` loop. The endpoint requires a distributed build and
otherwise returns 501. See ADR-092 and ADR-095.

Cross-topology assembly rules and mesh configuration are documented in
[coordinator mode](../server/coordinator-mode.md). Cluster design and failure invariants are
canonical in [clustering and scaling](../../../design/clustering-and-scaling.md).
