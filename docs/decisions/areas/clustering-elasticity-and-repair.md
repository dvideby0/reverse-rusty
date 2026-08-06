# Clustering — elasticity & repair decisions

> [Architecture decision hub](../../DECISIONS.md)

Allocation, handoff, autoscaling, resize, reconciliation, and repair after partial distributed writes.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [042](../adr-042-shard-node-allocator.md) | Shard→node allocator | Uses rendezvous hashing for balanced placement, minimal movement, and deterministic rebalance plans. | Accepted |
| [043](../adr-043-swappable-shard-backing.md) | Swappable shard backing (`HandoffShard`) | Atomically swaps a shard position's backing under a generation fence so reads can serve through handoff. | Accepted |
| [044](../adr-044-live-data-moving-handoff.md) | Live data-moving handoff | Recovers, fences, drains, and flips a shard under concurrent writes without losing matches. | Accepted |
| [045](../adr-045-autoscaler.md) | Autoscaler policy and trigger | Separates pure scale recommendations from the driver that applies safe rebalance actions. | Accepted |
| [047](../adr-047-remote-partial-apply-resync.md) | Remote partial-apply repair | Detects partial distributed writes and repairs them through an explicit, fail-closed `resync` path. | Accepted |
| [048](../adr-048-reliability-hardening.md) | Reliability hardening | Adds abort-safe unfencing, lease expiry, and autoscaler-driven handoff execution. | Accepted |
| [065](../adr-065-distributed-v1-graduation.md) | Distributed v1 graduation criteria | Defines the release-candidate program; each graduation deliverable is captured in its own ADR. | Accepted |
| [090](../adr-090-data-moving-reassignment.md) | Data-moving live reassignment | Moves shard data before committing ownership, so routing always points to a data-bearing source. | Accepted |
| [166](../adr-166-cluster-rebalance-api-contract.md) | Cluster-rebalance REST API contract | Makes native whole-cluster rebalance strict, bounded, topology-safe by default, and explicit about its ES/OpenSearch boundary. | Accepted |
| [167](../adr-167-cluster-resize-api-contract.md) | Cluster-resize REST API contract | Makes native in-process ring replacement strict, bounded, off-runtime, terminally attested, and explicit about its ES/OpenSearch boundary. | Accepted |
| [169](../adr-169-cluster-resync-api-contract.md) | Cluster-resync REST API contract | Makes native partial-apply repair strict, admission-bounded, off-runtime, disconnect-safe, and explicit about its ES/OpenSearch boundary. | Accepted |
| [170](../adr-170-cluster-handoff-api-contract.md) | Cluster-handoff REST API contract | Attests the live source, requires explicit uncommitted intent, bounds start admission, and supervises terminal handoff completion. | Accepted |
| [171](../adr-171-cluster-reassign-api-contract.md) | Cluster-reassign REST API contract | Attests live authority, reconciles without stale recopy, bounds start admission, and reports durable state truthfully. | Accepted |
| [172](../adr-172-cluster-reconcile-api-contract.md) | Cluster-reconcile REST API contract | Makes desired-placement convergence strict, resolve-only, singly admitted, supervised, and terminally attested. | Accepted |
| [173](../adr-173-cluster-gc-api-contract.md) | Cluster-GC REST API contract | Makes orphan cleanup strict, assignment-routed, shared-admission, disconnect-safe, and truthful about incomplete work. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
