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

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
