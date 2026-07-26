# Clustering — core & transport decisions

> [Architecture decision hub](../../DECISIONS.md)

The multi-shard correctness core, remote shard seam, shared feature space, and durable shard topology.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [027](../adr-027-in-process-multi-shard-core.md) | In-process multi-shard core | Uses one frozen dictionary, anchor sharding, and content routing to preserve lossless retrieval across K shards. | Accepted |
| [029](../adr-029-grpc-shardserver-shard-seam.md) | gRPC `ShardServer` + local↔remote `trait Shard` | Gives local and remote shards one fallible interface, with tonic transport behind the optional `distributed` feature. | Accepted |
| [030](../adr-030-dict-fingerprint-handshake.md) | Dict-fingerprint handshake + fallible construction | Rejects mismatched feature dictionaries at connect time and makes cluster construction fail loudly. | Accepted |
| [031](../adr-031-externalized-coordinator-log.md) | Externalized coordinator log (`trait ClusterLog`) | Records ordered mutations in a CRC-framed coordinator log so the cluster can rebuild after restart. | Accepted |
| [032](../adr-032-per-shard-durable-segments.md) | Per-shard durable compiled segments | Stores compiled segments per shard and reopens them by attach-and-mmap under one coordinator commit. | Accepted |
| [033](../adr-033-shared-nothing-storage.md) | Shared-nothing cluster storage | Chooses local segments, peer recovery, and Raft instead of a shared object store. | Accepted |
| [034](../adr-034-cross-process-dict-shipping.md) | Cross-process dict shipping over gRPC | Ships the frozen dictionary during adoption so a data node need not rebuild the corpus. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
