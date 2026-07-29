# `POST /_cluster/reassign` — Move and commit one assignment

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

Supply `{"position": N, "node": M}`. The coordinator resolves node `M` from membership,
live-handoffs the data, and only then commits the shard-to-node assignment. This move-then-commit
order keeps routing authoritative both live and after a resolve-only restart.

A successful response contains
`{acknowledged, moved, committed, position, node, generation}`. A 200 response with
`committed:false` and a warning means data moved but the durable map commit failed; repeat the
operation to reconcile. A failed move commits nothing and automatically unfences its source.
The endpoint requires a distributed build and otherwise returns 501. See ADR-090.

Cross-topology assembly rules and mesh configuration are documented in
[coordinator mode](../server/coordinator-mode.md). Cluster design and failure invariants are
canonical in [clustering and scaling](../../../design/clustering-and-scaling.md).
