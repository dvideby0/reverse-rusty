# ADR-035: Per-shard replication + peer recovery — the `ReplicatedShard` composite (clustering step 4, in-process)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** Under the shared-nothing realignment (ADR-033), the next clustering step after dict shipping
  (ADR-034) is **per-shard replication + peer recovery** — the Elasticsearch/Cassandra HA primitive: a shard
  position becomes a **primary + N replicas**, a write fans out to the replicas, a read **fails over** to a
  replica if the primary is down, and a fresh/recovering replica is brought up by **streaming the primary's
  local segments from a peer**. The building blocks already exist: ADR-031's coordinator log is the WAL and
  ADR-032's durable per-shard `.seg` files are the streamable segments. Following the rhythm of ADR-027
  (in-process sharding) → ADR-029 (gRPC transport), this ADR builds the **in-process** mechanism first,
  dependency-free and oracle-proven; the gRPC multi-node lift is ADR-036.
- **Decision:** A **`ReplicatedShard` composite** (`src/cluster/replica.rs`) that implements the existing
  `pub(crate) trait Shard` and wraps **one shard position's** copies — a primary `Box<dyn Shard>` + N replica
  `Box<dyn Shard>`. It slots into the coordinator's `Vec<Box<dyn Shard>>` via the existing `from_parts` seam
  with **zero coordinator changes** (the coordinator still sees one shard per position; the RF copies live
  inside the box), and composes over `LocalShard` (in-process) or — in ADR-036 — `RemoteShard`.
  - **Set-equality is the correctness basis.** Matching emits **logical** ids (local ids are segment-internal
    and append-only), so a replica fed the **same ordered op stream** holds the **same set of live logical
    queries** — byte-identical local ids are not required. Replication thus reduces to "apply the same op to
    every copy."
  - **The four guards (zero false negatives).** (1) **Reads** serve the primary and fail over **only on
    `ShardError::Remote`** (transport) and **only to an in-sync replica**; a `DictMismatch`/`Config`/`Log`
    error propagates (failing over would mask a real bug), and if every reachable copy fails the error
    propagates — never an empty/partial set. A replica that missed a write (out of sync) is never read. (2)
    **Aggregation presents the PRIMARY's view** — `num_queries`/`class_counts` reflect one copy and
    `delete_by_logical_id` returns the primary's count — because the coordinator *sums* these across shard
    *positions*; summing replicas would multiply totals by RF. (3) **Writes are primary-authoritative**: apply
    to the primary first (its return is the composite's; a primary error fails the op), then fan the same op to
    the in-sync replicas. (4) **Checkpoint/durability delegate to the primary** (`seal_for_checkpoint`/
    `segment_filenames`/`next_seg_id`), the manifest-recorded copy.
  - **Replica failures are tolerated (the Elasticsearch model).** A replica that errors on a replicated write
    is dropped from the in-sync set and a `DurabilityOp::ReplicaDesync` event is surfaced (redundancy reduced,
    flagged for re-recovery); the write still succeeds on the authoritative primary. A
    `wait_for_active_shards`-style write *precondition* would create a false-failure with the log-first
    coordinator (the primary + log already hold an acked write), so it is **deferred** to the control plane —
    there is no post-write min-in-sync rollback.
  - **Replicas are HA copies, not catalogued data.** `ClusterConfig::replication_factor` (default **1** —
    byte-identical to pre-ADR-035: RF=1 boxes a bare `LocalShard`, no composite). The **primary** is the
    durable copy at `shard_<i>/` recorded in the manifest (**`ClusterManifest` v2 unchanged**); replicas are
    extra copies (durable `shard_<i>/replica_<r>/` for a durable cluster, in-RAM for an in-memory one) seeded
    at `build` by the same op stream and **rebuilt on `open` by peer recovery** from the just-attached primary
    — then the log-tail replay feeds primary AND replicas through the composite. This matches ES ("replicas are
    allocated, not catalogued; the primary + log are the durable truth") and keeps the durable format stable.
  - **Peer recovery primitive** (`replica::peer_recover`): seal the primary (flush + reseal base tombstones) →
    copy its `.seg` files (and `sources.dat` if present — display-only, tolerated absent) into a clean replica
    dir → `LocalShard::open_segments` (fail-loud on a missing/corrupt segment). The in-process stand-in for ES
    "stream segments from a peer," and the basis for the gRPC streaming RPC in ADR-036. Durable-primary only
    (an in-memory primary has no files; in-memory clusters seed replicas by op-stream replay).
- **Consequence:** Dependency-free (lean core untouched). Proven by the extended `tests/cluster_oracle.rs`
  (RF ∈ {2,3} × K ∈ {1,3,8} × broad ≡ single-node ≡ brute; counts not inflated by replicas; live add/remove
  with primary-only remove counts) and `tests/cluster_durability_oracle.rs` (durable RF=2 reopen ≡ pre-crash ≡
  brute; checkpoint seals primaries only), plus `replica.rs` unit tests (in-sync failover, no-failover on
  `DictMismatch`, primary-write-failure propagation, replica-failure tolerance + `ReplicaDesync` event,
  set-equality through an op stream, peer recovery reproducing the primary set incl. a baked tombstone). One
  new trait method `Shard::set_event_sink` (default no-op) lets the coordinator fan its observer into the
  composites. **Deferred:** the gRPC multi-node lift (ADR-036 — replicas as `RemoteShard`s + a streaming
  segment-fetch RPC for cross-node peer recovery), and (control-plane, ADR-033 roadmap) automatic
  failure-detection/promotion, an allocator for shard→node placement, and `wait_for_active_shards`-style write
  preconditions.
- **See also:** ADR-027 (the in-process core + the one-frozen-dict invariant + the `from_parts` seam this
  reuses), ADR-031 (the coordinator log = the WAL replicas replay), ADR-032 (per-shard durable segments = what
  peer recovery streams), ADR-033 (the shared-nothing model this implements step 4 of), ADR-036 (the gRPC lift),
  `src/cluster/replica.rs`, `src/cluster/coordinator.rs` (`replication_factor`, `build`/`open` wiring),
  `src/cluster/shard.rs` (`set_event_sink`), `tests/cluster_oracle.rs`, `tests/cluster_durability_oracle.rs`.

