# ADR-033: Shared-nothing cluster storage — supersede the Aurora-disaggregated / object-store framing

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** The clustering design (`clustering-and-scaling.md` §4) modeled the durable layer on **Aurora's
  disaggregated storage**: a quorum mutation log + immutable compiled segments living in **shared object
  storage** (S3-shaped), with replicas/failover "attaching" to that shared storage. ADR-032's stated
  "multi-node half" was therefore *object-store segments* (swap the local `MmapSegment::open` for an S3 fetch).
  On review that is the wrong fit: (1) it implies an **external storage service** in the serving path, which
  clashes with this project's lean, self-contained, dependency-light ethos (16 deps, std-only core); (2) the
  payoff it buys — "cheap replicas / fast failover from shared storage" — only materializes once a multi-node
  control plane exists, which it does **not** yet; (3) it nudges the design toward a cloud-storage coupling we
  do not want. Crucially, **the systems we actually take cues from do not work this way.**
- **Decision:** Adopt the **shared-nothing** model that Elasticsearch/OpenSearch, Cassandra, and Kafka use,
  and which our building blocks already match:
  - **Local storage per node.** Each shard keeps its compiled segments on **local disk** (already true —
    ADR-032's per-shard `shard_<i>/segments/*.seg`). No shared storage in the serving path.
  - **Durability = a per-node/coordinator WAL** (already true — ADR-031's `ClusterLog`), the analogue of
    ES's per-shard translog.
  - **HA = primary/replica with peer recovery** (future): a new owner streams segments from a peer + replays
    the log tail, *not* from object storage — the ES/Cassandra recipe.
  - **Membership/routing = a quorum/Raft control plane** (future): holds the ring + shard→node map +
    feature-model version + log epoch.
  - **Object storage is NOT a dependency.** If it ever returns, it is only an **optional, pluggable
    snapshot/backup** target with a **local-filesystem default** (the shape of ES's `fs` snapshot repository,
    which is a plain shared directory — no cloud), never in the serving path and never AWS-coupled.
  *(Rejected: keep the Aurora-disaggregated model. It is a legitimate school — Aurora/Neon — but it trades
  self-containment for an external storage service to make replicas cheap; we get the same "cheap replicas"
  property from warm replicas + peer recovery without taking on that dependency, and the shared-nothing
  primitives are already built.)*
- **Consequence:** The clustering critical path is re-pointed: **dict shipping (ADR-034) → per-shard
  replication + peer recovery → Raft/quorum control plane → auto-split + autoscale.** Object-store
  segments leave the roadmap. ADR-031's and ADR-032's "deferred: object-store" notes are amended in place
  (their *local-disk* durability decisions stand unchanged — only the "object-store next" framing is dropped;
  ADRs are never renumbered/rewritten). `clustering-and-scaling.md` §4/§5/§8/§10 are reworked to the
  shared-nothing model; §2/§3 (the title-fan-out asymmetry + the anchor-routing no-false-negative argument)
  are **model-independent and unchanged**. No engine code changes in this ADR — it is a design realignment;
  the code increment that accompanies it is ADR-034.
- **See also:** ADR-031 (the coordinator WAL = the shared-nothing durable log), ADR-032 (per-shard local
  segments = the shared-nothing local base), ADR-027 (the in-process core), ADR-034 (dict shipping, the first
  shared-nothing multi-node step), `clustering-and-scaling.md` §4/§5/§8/§10,
  `research/clustering-prior-art.md` (the ES/Cassandra/Kafka vs Aurora/Neon comparison).

