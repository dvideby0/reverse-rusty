# ADR-042: Shard→node allocator (rendezvous hashing) — committing the placement map (clustering step 5f)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-037/038 gave the control plane a **shard→node map** (`ClusterState.assignments`) but
  nothing computed it — the default was a single logical node owning every position, and §4.3's own note
  flagged "the allocator that *acts* on the shard→node map ... is the next increment." Without it, a node
  joining or leaving the cluster never changes placement: no balance, no rebalance. This is the decision
  layer that fills the map.
- **Decision (`src/cluster/allocator.rs` + `ClusterEngine::{register_node, deregister_node, rebalance}`):**
  - **Rendezvous (HRW) hashing, not `position % N`.** For each shard position, rank the member nodes by a
    stable `hash(position, node)` (the project's `util::fnv1a64`, identical across runs + nodes) and take the
    top RF — highest weight = primary, the rest = replicas. HRW is balanced + deterministic like a modulus,
    but **minimal-movement**: adding a node only wins the ≈1/N positions where it now out-weighs the prior
    top; removing a node hands off only *its* positions to each one's next-best node — the
    Elasticsearch/Cassandra rebalance property (§8), and the same hashing family as the entity-anchor
    `HashRing` (keyed on `(position, node)` instead of a feature id). `rf` is clamped to `[1, node_count]`
    (a position can't have more distinct copies than nodes); replicas are distinct from the primary by
    construction. Pure computation over `NodeId`/`ShardAssignment` — **lean core**, dependency-free.
  - **The coordinator drives it.** `register_node`/`deregister_node` propose `AddNode`/`RemoveNode` through
    the control plane (the membership half of the inputs); `rebalance(rf)` reads the committed membership,
    plans the desired map, and commits **only the changed positions** (`allocator::changed_assignments`)
    via `AssignShard` proposals — minimal proposals, returning the count moved. It is **idempotent** (no
    membership change ⇒ 0 reassignments) and **fail-closed** (a rejected proposal leaves the prior map
    intact). On the single-node default it is a no-op (the one node already owns everything), so existing
    behavior is unchanged.
- **Scope — decision, not (yet) data movement.** `rebalance` commits the *desired* map; physically
  relocating a shard's segments to a new owner on a reassignment reuses the existing **peer-recovery** path
  (`peer_recover_replica`, ADR-036/039) and is the deployment wiring on top — an in-process cluster holds
  every shard locally, so the map is **advisory** there and matching is unaffected (the local shards do not
  move). Deferred: serve-then-drop handoff + epoch fencing during a live move (§9), an autoscaler that
  *calls* `rebalance`/`register_node` on membership events (step 6), and `recommended_shard_count` /
  auto-split (step 6). The allocator is the building block those will use.
- **Consequence:** A cluster can now compute and commit a balanced shard→node map and rebalance it as nodes
  join/leave, with bounded churn — the missing decision layer for multi-node placement, and the foundation
  for autoscale/auto-split. The single-node default is a no-op, so every prior oracle is unchanged. Proven by
  `allocator.rs` unit tests (distinct primary+replicas, RF clamping, determinism, ≈1/N movement on a node
  add, balanced primaries, the changed-only diff) + `tests/cluster_allocator_oracle.rs` over a real
  `ClusterEngine` (register → rebalance ⇒ a balanced fully-assigned map; idempotent; a deregistered node
  drops out of every position; and — load-bearing — `percolate` is **byte-identical** before and after every
  rebalance, so the allocator cannot introduce a false negative). Full `check.sh` green; no new dependency.
- **See also:** ADR-037 (the `ClusterState` map + `AssignShard` this computes), ADR-038/041 (the durable
  control plane that commits + persists it), ADR-027 (the entity-anchor `HashRing` — the sibling consistent
  hash), ADR-036/039 (the peer recovery a data-moving rebalance will drive), ADR-033 (shared-nothing —
  rebalance = peer recovery, no shared storage), `src/cluster/{allocator,coordinator}.rs`,
  `tests/cluster_allocator_oracle.rs`.

