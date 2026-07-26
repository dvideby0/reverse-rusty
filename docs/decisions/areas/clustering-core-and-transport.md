# Clustering — core & transport decisions

> [Architecture decision hub](../../DECISIONS.md)

The multi-shard correctness core, remote shard seam, shared feature space, and durable shard topology.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [027](../adr-027-in-process-multi-shard-core.md) | In-process multi-shard core | K-shard coordinator: one shared frozen dict → globally-stable `FeatureId`s, a feature-anchor ring (~2–5 fan-out), broad lane on a replicated shard. The no-false-negative heart of clustering. | Accepted |
| [029](../adr-029-grpc-shardserver-shard-seam.md) | gRPC `ShardServer` + local↔remote `trait Shard` | Lift the shard behind a `trait Shard` + a tonic `ShardServer` (off-by-default `distributed`); ships DSL not feature-ids; the fallible seam preserves zero-FN. | Accepted |
| [030](../adr-030-dict-fingerprint-handshake.md) | Dict-fingerprint handshake + fallible construction | Connect-time `Dict::fingerprint` handshake turns a divergent cross-process dict from a silent false-negative into a loud `DictMismatch`; construction made fully fallible. | Accepted |
| [031](../adr-031-externalized-coordinator-log.md) | Externalized coordinator log (`trait ClusterLog`) | A durable CRC-framed (+ null) ordered, log-first mutation log so the whole cluster is rebuildable from the log alone. | Accepted |
| [032](../adr-032-per-shard-durable-segments.md) | Per-shard durable compiled segments | Reopen by attach-and-mmap per-shard `.seg` files (not re-ingest); coordinator manifest is the atomic commit point; checkpoint re-seals tombstoned base segments. | Accepted |
| [033](../adr-033-shared-nothing-storage.md) | Shared-nothing cluster storage | Supersede the Aurora/object-store framing — shared-nothing (local segments + per-node WAL + peer recovery + Raft control plane), like ES/Cassandra/Kafka. | Accepted |
| [034](../adr-034-cross-process-dict-shipping.md) | Cross-process dict shipping over gRPC | Ship the frozen dict to each server at connect (`AdoptDict`); a data node starts empty/pending instead of rebuilding the dict from the whole corpus. | Accepted |

---

Each summary links to the canonical ADR record. Implementation status belongs in
[STATUS.md](../../STATUS.md); documentation placement rules belong in
[the documentation hub](../../README.md).
