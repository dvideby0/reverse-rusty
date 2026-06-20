# ADR-027: In-process multi-shard core — shared frozen dict, feature-anchor ring, designated replicated lane

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Clustering was entirely design-only ([clustering-and-scaling.md](../design/clustering-and-scaling.md),
  STATUS Tier 3). The design's own build path (§10) is explicitly incremental and front-loads the
  correctness-critical heart *before* any networking: step 1 (wrap the engine as a shard) + step 2
  (a coordinator with a consistent-hash ring + content routing over K shards **in one process**),
  validated by extending the differential oracle to a multi-shard harness. The novel, no-false-negative
  part of the design is the entity-anchor sharding + content routing; gRPC, the durable externalized
  log, Raft, and object storage are "borrowed plumbing." So we build steps 1–2 first.
- **Decision:** A `cluster` module (`ClusterEngine` over K `Shard`s, each a `Shard`-wrapped `Engine` +
  `ArcSwap<EngineSnapshot>`) with **zero new dependencies** (`rayon`/`arc-swap` already present).
  Four load-bearing sub-decisions:
  1. **One authoritative, frozen `Arc<Dict>` shared read-only into every shard.** Each `Engine` interns
     features and finalizes its 64-bit hot mask *per build* (`Arc::make_mut` on the write path), so two
     independent engines disagree on both `FeatureId`s and which features are "hot." Either divergence
     flips a query's cost class / anchor across shards → a title routes to one shard while the query was
     indexed under a different key on another → **false negative**. The coordinator therefore builds the
     dict over the whole corpus once (pass A), `finalize_mask`, then freezes it and shares it; shards
     index via the new non-mutating `Engine::ingest_extracted` / `insert_extracted` (which call
     `Segment::add_compiled`, read-only over the dict), so the `Arc` is never forked. This is the
     in-process model of the design's "feature-model version in cluster state" (§4.3/§8.7).
  2. **Consistent-hash ring (virtual nodes) keyed on `FeatureId`** (not on `sig_key`). Safe *because* of
     the shared dict (ids are globally stable), and it gives the design's true ~2–5 fan-out: a title routes
     on its few rare features, not on the combinatorial set of probe-signatures it generates. (A `sig_key`-keyed
     ring would be correct but blow fan-out up to ~all shards for titles with several hot features.)
     `ring_hash` = FNV-1a + a murmur3 finalizer over the id (FNV alone clusters sequential ids and skews
     shard load); virtual nodes balance shard load at small K. The prior-art survey
     ([research/clustering-prior-art.md](../research/clustering-prior-art.md) §1) compares ring+vnodes against
     jump-hash / rendezvous / Maglev and the *feature-token* (`fnv1a64(feature_name)`) keying a per-shard-dict
     design would require; the shared dict (sub-decision 1) lets us key on the integer id directly — simpler,
     faster (no name re-hash on the routing path), and the shared dict is mandatory anyway.
  3. **`compile::anchor_plan` is the single source of truth for placement.** `build_signatures` was
     refactored to compute the pre-hash anchor feature *groups* and then hash them (byte-identical
     output — the existing oracle is the guard), so the coordinator places by anchor *identity* without
     re-deriving the optimizer's per-class selection. Forbidden features can't leak in: `anchor_plan`
     reads only `required`/`anyof`, never `forbidden` (ADR-006 holds structurally).
  4. **Placement by cost class; queries with no rare anchor go to a designated replicated lane (shard 0).**
     Class A (one rare anchor) → one shard; class-B any-of (rare members) → one shard per member; **class-B
     arity-2** (rarest required is hot ⇒ *all* required hot ⇒ no rare anchor to hash on) and **class C**
     (broad) → the replicated lane. In-process that lane is materialized on shard 0 and evaluated **only**
     there (always probed, with `include_broad`), so there is no double-counting; selective shards run
     `include_broad=false`. This is the in-process stand-in for the design's "replicate the broad lane to
     every node" (§7). Routing a title = shard 0 ∪ `{ring.lookup(f) : f ∈ title, !is_hot(f)}`; results
     are unioned + deduped. Deletes fan out to all shards (idempotent), sidestepping any placement journal.
- **Why correct (no false negatives):** for any query `Q` a title `T` matches — if `Q` is class A /
  B-any-of, its anchor (resp. a matched member) is a *required*, non-hot feature, present in `T`, so `T`
  routes to `ring.lookup(anchor) =` `Q`'s shard; if `Q` is class-B-arity-2 / C it lives on shard 0,
  which `T` always probes. Each shard is a verbatim single-node engine, so its lossless cover + integer
  exact-verify finish the job; no shard boundary can drop a match. No false positives: every emitted id
  passed `exact.verify` (title-content-only) on some shard, and the union dedups. Guarded by
  `tests/cluster_oracle.rs`: cluster ≡ single-node ≡ independent brute-force oracle, as sets, across
  K ∈ {1,3,8,16} × broad on/off, with every placement branch asserted present (`class_counts`) and
  fan-out asserted ≪ K.
- **Alternatives considered:**
  - *Per-shard independent dicts* — rejected; hot-mask/`FeatureId` divergence is a false-negative trap (1).
  - *`sig_key`-keyed ring* — rejected; correct but defeats the ~2–5 fan-out win for hot-heavy titles (2).
  - *Place class-B-arity-2 on its rarer feature's shard* — rejected; that feature is *hot*, and titles
    route only on non-hot features, so the query would be unreachable → false negative. The replicated
    lane is the correct home (4).
  - *Generator knobs for class coverage* — rejected; adding required `GenConfig` fields breaks ~30
    literal sites. The oracle hand-injects pure-any-of, all-hot, and multi-entity cases instead.
- **Consequence:** The central claim of the clustering design — content-routed percolation by anchor
  entity with a clean no-false-negative proof — is now built and proven in one process, dependency-free,
  and is the foundation later steps wrap. Surface: `cluster::ClusterEngine` (library) + `clusterdemo`
  bin + the oracle; no HTTP yet. **Out of scope (later build-path steps):** gRPC `ShardServer` + a
  local↔remote `Shard` trait (step 1 networking), the durable externalized mutation log / read-your-writes
  quorum (§4.1/§6), Raft cluster-manager quorum (§4.3), object storage / attach-and-mmap replicas (§4.2),
  auto shard count / auto-split / rebalance (§8), autoscaling (§8.5), epoch fencing / self-heal (§9),
  replicate-broad-to-*all*-nodes (§7; in-process uses one designated evaluator — now graduated by
  [ADR-080](adr-080-cluster-replicate-broad-to-all.md): the broad lane lives on every shard, evaluated
  on one broad-eval shard per title), and incremental **new-vocabulary** adds (the dict is frozen
  post-build; `add_query` compiles read-only against it).
- **See also:** the clustering design ([clustering-and-scaling.md](../design/clustering-and-scaling.md) §3,
  §7, §10) and the prior-art survey ([research/clustering-prior-art.md](../research/clustering-prior-art.md) —
  the hashing-variant comparison + the formal cross-shard correctness argument behind this ADR), ADR-001
  (semantic signatures — the anchor the ring hashes), ADR-003 (broad-query quarantine —
  the lane that gets replicated), ADR-006 (forbidden never gates — preserved in placement + routing),
  ADR-016 (the lock-free snapshot each shard reads), the lossless-cover contract
  ([design/README.md](../design/README.md) §2), `src/cluster/{ring,shard,coordinator}.rs`,
  `src/compile.rs` (`anchor_plan`), `tests/cluster_oracle.rs`.

