# ADR-045: Autoscaler — the policy/trigger layer over rebalance + advisories (clustering step 6c)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** The scaling *mechanisms* are built — `register_node`/`deregister_node`/`rebalance` (the HRW
  allocator, ADR-042) and the live data-moving handoff (`execute_handoff`, ADR-043/044) — but nothing
  *decided when* to drive them: they fired only from tests. §8's "auto-rebalance"/"auto-split" goals and the
  §6c build step flagged the autoscaler as the missing policy. This increment adds it.
- **Decision (a pure policy + a thin driver):**
  - **`cluster::autoscale::evaluate(snapshot, config) -> AutoscaleDecision`** — a *pure, deterministic*
    policy over a `LoadSnapshot` (membership + the shard→node map + per-shard corpus). Three rules: (1)
    **membership drift → `Rebalance` (executable)** when the registered node set differs from the node set the
    assignments reference (a join leaves a node unplaced; a leave leaves a stale id still owning a position —
    the dangerous case, routing to a dead owner); (2) **per-node skew → `Handoff` (advisory)** when a node's
    primary-corpus exceeds `max_node_load_skew ×` the mean; (3) **per-shard corpus over a threshold →
    `RecommendSplit` (advisory)**.
  - **The driver on `ClusterEngine`** (`coordinator::autoscale`): `tick(config)` collects the snapshot
    (`control_state` + `shard_query_counts` — the only load signal that crosses the `Shard` seam, so the
    policy behaves identically in-process and across nodes), runs `evaluate`, **executes the executable
    subset** (each `Rebalance` → the idempotent `rebalance(rf)`), and returns the full decision incl.
    advisories; `on_node_joined`/`on_node_left` are the event-driven `register`+`tick` / `deregister`+`tick`
    convenience entries.
  - **Coarse trigger, idempotent truth.** The membership rule never recomputes HRW (that keeps `evaluate` a
    pure function of the snapshot, with no allocator coupling); the idempotent `rebalance` computes the exact
    minimal diff. **No clock / hysteresis:** `rebalance` is idempotent and `evaluate` is pure, so back-to-back
    ticks on unchanged membership cannot thrash — the idempotence *is* the hysteresis.
  - **Opt-in / disabled default.** `AutoscaleConfig::default()` is disabled ⇒ `tick` is a no-op ⇒ every
    pre-existing oracle stays byte-identical. Lean core, no new dependency, no `distributed`-gated code.
- **Honest scope / deferred.** **Auto-split is advisory only** — there is no split mechanism (the ring's
  `num_shards` is fixed at construction; splitting needs ring re-keying + a `recommended_shard_count` signal
  from compaction telemetry — a separate future increment). **Load-driven handoff is advisory only** — the
  policy emits a `Handoff`, but `execute_handoff` (gRPC-gated, ADR-044) is not driven this increment. QPS /
  compute-replica autoscaling (HPA-style, §8) is out of the engine's scope (a deployment-orchestrator concern).
- **Consequence:** membership/skew-driven rebalance is now automatic behind one opt-in policy, with split /
  handoff surfaced as advisories for an operator or a later increment. Proven by `src/cluster/autoscale.rs`
  unit tests (the deterministic policy decisions) + `tests/cluster_autoscale_oracle.rs` (over a real
  in-process cluster: `tick` commits the same map a manual `rebalance` does; **`percolate` is byte-identical
  before/after a tick** — the zero-false-negative property; a second tick commits nothing; a disabled config
  is a true no-op; a corpus-over-threshold advisory mutates nothing). Full `check.sh` green.
- **See also:** ADR-042 (the allocator `rebalance` it drives), ADR-043/044 (the handoff it will later
  *trigger* once load-driven moves are wired), ADR-027 (the content routing its rebalances must preserve),
  `src/cluster/autoscale.rs` + `src/cluster/coordinator/autoscale.rs`, `tests/cluster_autoscale_oracle.rs`.

