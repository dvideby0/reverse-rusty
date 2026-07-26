# Clustering — replication & control plane decisions

> [Architecture decision hub](../../DECISIONS.md)

Replication, peer recovery, translogs, cluster state, consensus, and control-plane durability.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [035](../adr-035-per-shard-replication-peer-recovery.md) | Per-shard replication + peer recovery | Uses a primary plus in-sync replicas, with segment streaming to restore a replica before read failover. | Accepted |
| [036](../adr-036-grpc-replication-peer-recovery.md) | gRPC replication + peer recovery | Extends replication across processes with remote failover and segment-recovery RPCs. | Accepted |
| [037](../adr-037-control-plane-seam.md) | Control-plane seam (`trait ControlPlane`) | Defines a dependency-free cluster-state interface with an in-memory default backend. | Accepted |
| [038](../adr-038-openraft-control-service.md) | openraft backend + gRPC control service | Replicates cluster-state metadata through Raft without putting query mutations in consensus. | Accepted |
| [039](../adr-039-durable-translog-no-quiesce-recovery.md) | Durable translog + no-quiesce recovery | Streams a segment checkpoint and replays its translog tail so recovery need not pause writes. | Accepted |
| [040](../adr-040-translog-retention-leases.md) | Translog retention leases + finalize | Pins required log tails during recovery and finalizes replicas through bounded catch-up passes. | Accepted |
| [041](../adr-041-durable-raft-log-recovery.md) | Durable Raft restart recovery | Persists Raft log, vote, committed state, and snapshots so control nodes can restart into quorum. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
