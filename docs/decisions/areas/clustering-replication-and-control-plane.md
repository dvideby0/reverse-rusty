# Clustering — replication & control plane decisions

> [Architecture decision hub](../../DECISIONS.md)

Replication, peer recovery, translogs, cluster state, consensus, and control-plane durability.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [035](../adr-035-per-shard-replication-peer-recovery.md) | Per-shard replication + peer recovery (`ReplicatedShard`) | One primary + N replicas (in-process): primary-authoritative writes, in-sync-only read failover, peer recovery by streaming segments. Set-equality is the basis. | Accepted |
| [036](../adr-036-grpc-replication-peer-recovery.md) | gRPC multi-node replication + peer recovery | Lift replication + peer recovery onto gRPC — remote replicas that fail over + cross-node segment streaming (`FetchSegments`/`RecoverFrom`); servers become durable. | Accepted |
| [037](../adr-037-control-plane-seam.md) | Control-plane seam (`trait ControlPlane`) | Dependency-free seam + in-memory backend holding the cluster-state doc (ring params + shard→node map + membership + epoch); shaped for openraft, byte-identical by default. | Accepted |
| [038](../adr-038-openraft-control-service.md) | openraft backend + gRPC `ControlService` | A real openraft backend behind the seam; consensus holds only the cluster-state doc, never query mutations; survives leader death. | Accepted |
| [039](../adr-039-durable-translog-no-quiesce-recovery.md) | Durable translog + no-quiesce peer recovery | Per-shard durable+replicated query log lets recovery stream segments at P then replay the tail > P — recovery without quiescing writes; data nodes self-restart. | Accepted |
| [040](../adr-040-translog-retention-leases.md) | Translog retention leases + finalize | Leases pin the translog tail across a recovery (min over holders) so a concurrent seal can't strand it; a bounded finalize loop grows a replica in-sync without pausing writes. | Accepted |
| [041](../adr-041-durable-raft-log-recovery.md) | Durable Raft log + control-plane restart recovery | Make the openraft log/vote/committed/snapshot durable so a `controlserver --data-dir` survives a crash and rejoins quorum; `apply` stays pure in-memory. | Accepted |

---

Each summary links to the canonical ADR record. Implementation status belongs in
[STATUS.md](../../STATUS.md); documentation placement rules belong in
[the documentation hub](../../README.md).
