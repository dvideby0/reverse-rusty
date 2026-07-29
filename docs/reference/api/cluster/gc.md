# `POST /_cluster/gc` — Reclaim orphaned shard slots

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

Run one orphan-slot garbage-collection sweep for fenced, unrouted slots left behind by
data-moving reassignment. The keep-set is the committed assignment map plus live routing, so a
source or target involved in a flip without a commit is never dropped. Unassigned positions are
skipped fail-safe, and a restarted unfenced orphan is fence-armed before removal.

The idempotent sweep records per-slot failures and continues. Its response contains
`{acknowledged, dropped[], kept_live_routed[], skipped_unassigned[], failed[], skipped_nodes[]}`.
This is the manual one-shot form of the opt-in `--reconcile-gc-orphans` loop epilogue. The endpoint
requires a distributed build and otherwise returns 501. See ADR-096.

Cross-topology assembly rules and mesh configuration are documented in
[coordinator mode](../server/coordinator-mode.md). Cluster design and failure invariants are
canonical in [clustering and scaling](../../../design/clustering-and-scaling.md).
