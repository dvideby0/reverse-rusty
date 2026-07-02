# ADR-104: The 20M multi-shard scale soak (Distributed-v1 criterion 12, scale half)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Built + passing (2026-07-02).** A new `#[ignore]`d test target
  `engine/tests/cluster_soak/` — one soak (`twenty_million_multi_shard_soak`) that builds a
  **durable K=8 in-process cluster at 20M stored queries** on real hardware, proves it ≡ the
  single-node engine over 50k titles (zero mismatches), survives live mutations, and reopens from
  disk byte-identically. The canonical run's numbers are pinned in
  [`performance/benchmark-results.txt`](../performance/benchmark-results.txt) (INVARIANTS +
  capture log). This ships the **scale half** of ADR-065 criterion 12; the **real-corpus
  FN/throughput audit stays open** (blocked on a user-supplied corpus — the `RR_ORACLE_CORPUS`
  hook, ADR-087, is the intake).

- **Context:** Criterion 12 ([ADR-065](adr-065-distributed-v1-graduation.md)) demands "a
  multi-shard load test at ≥20M stored queries on real hardware" — the run that turns the headline
  numbers from design-target evidence into deployment evidence. Everything else in the graduation
  program had shipped (criteria 1–11 + follow-ons through ADR-097), but the largest soak on record
  was **10M, single-node** (`tests/stress/soak.rs`), and every cluster oracle runs at ≤100k
  queries. The cluster layers whose behaviour *changes with scale* — dict size, postings tiers
  (inline → Vec → roaring), ring placement distribution, per-shard segment counts, the coordinator
  manifest, cross-shard merge — had never been exercised past 100k. Separately, the 10M soak never
  verified its live inserts were *retrievable* (only that par==seq and deletes stayed dead), so the
  ADR-046 synthetic-ID add path had no at-scale retrievability check either.

- **Decision:** One `#[ignore]`d soak test, **in-process multi-shard, durable, differential
  against the single-node engine** — run once locally as the acceptance run, kept in-tree for
  reproducibility, wired into **no gate and no CI workflow**.

  1. **The zero-FN reference that scales is the single-node `Engine`, not brute force.** Brute
     force at 20M × 50k is ~10¹² predicate evaluations — infeasible. The single-node engine is
     itself proven ≡ brute (`tests/oracle`), ≡ the front-end-independent reference matcher
     (ADR-087), and soaked at 10M — and it runs **none** of the cluster code (placement, ring,
     content routing, broad replicate-to-all, cross-shard merge). Comparing full per-title match
     sets over all 50k titles therefore catches any false negative *the cluster layers* introduce
     at scale. A K=1 cluster was rejected as the reference: it still runs `placement_of`/`route`/
     `percolate_inner`, so a shared-cluster-code bug could cancel out on both sides.

  2. **Constructed sentinels are the absolute check.** ~2,000 planted pairs (query
     `sentineltok{i}`, title `"sentineltok{i} listing"` — single rare required term ⇒ class A,
     ring-placed) assert *containment* (title's result ⊇ its sentinel id) pre-mutation,
     post-mutation, and after reopen. This catches a systemic miss that would hit the reference
     engine identically (the one blind spot of a relative differential). The generator provides no
     per-title ground truth, so the sentinels construct it.

  3. **Mutations run through the cluster and are mirrored on the reference**, then the full
     differential re-runs: 100k `add_query` with never-seen tokens (the frozen-dict **synthetic-ID
     path**, ADR-046 — a 1k sample is asserted retrievable via constructed titles, the check the
     10M soak lacked), 20k `upsert_query` (ADR-070 single-frame replace), 200k `remove_query`
     (fan-out-all-shards deletes), plus a ghost sweep (no removed id in any result).

  4. **Durability rides the same build:** the cluster is built durable from the start
     (`data_dir`, per-shard segments + coordinator log, ADR-031/032), so the leg costs one
     `flush` → `checkpoint` → drop → `open` → re-verify (a recorded 2k-title subset + all
     sentinels byte-identical, removed ids still absent). The coordinator-manifest + mmap-attach
     reopen path had never run past ~100k.

  5. **Structural bands, not pins, gate the run — and only the scale-invariant structure**
     (first run at this scale; the measured exacts become the capture-log pin afterwards):
     fan-out avg ∈ [1, 5] and p99 ≤ K−1 (the never→K claim), per-shard count max ≤ 2×min
     (placement balance), class D = 0, and a loose pathological ceiling on candidates/title
     (≤ Q/1000 — trips only if the cover regresses toward candidates ∝ Q). Candidate volume is
     otherwise *captured, not banded*: with the broad lane ON it grows with corpus size **by
     design** (the recorded lineage — 85.64 @100k, 682 @1M — already showed this; that growth is
     exactly what the ADR-026 columnar batch lane amortizes). The engine's flatness claim
     (~54 candidates/title, corpus-size-independent) is a **broad-OFF selective-path** invariant
     and is re-verified at 20M by `bench 20000000 20000 0.0` in the same capture — its first run
     tripped a naive "selective ≤ 300" band precisely because banding broad-on volume encodes a
     false invariant.

  6. **Sizing + knobs.** Defaults 20M queries / 50k titles / K=8, seed `0x2000_0000`, generator
     pools per the clusterbench convention (`Q/40` players, `Q/100` sets) so fan-out numbers are
     comparable in convention to the pinned 100k invariants. Every dimension is env-overridable
     (`RR_CLUSTER_SOAK_QUERIES` / `_TITLES` / `_SHARDS` / `_DIR`) so the harness smokes at 200k in
     ~3s and reruns anywhere. Phase order is memory-deliberate: the cluster (with its clone-heavy
     pass-B build transient) is built **before** the reference engine, the query corpus is freed
     before the second differential, and the reference is dropped before the reopen leg —
     measured peak 16.07 GB on the 48 GiB dev box (the two-resident-engines differential phase
     dominates), total ~4 min wall-clock.

  7. **Deliberately NOT in any gate or CI workflow.** The soak is a **one-off acceptance run**
     (like the NPMI learner capture, an evidence run — not a recurring benchmark): `#[ignore]`d,
     in its own test target so the existing CI `run_soak` dispatch (which runs every ignored test
     in the `stress` target) is untouched, and given **no** dispatch input of its own. The gate
     only ever *compiles* it. Rerunning is a manual, by-name invocation; the tree keeps the
     harness so the ADR's claim is reproducible, the docs keep the numbers.

- **What this run does and does not prove.**
  - **Proves:** the sharded index's structural claims hold at 20M — bounded fan-out, balanced
    placement, selective flatness, cluster ≡ single-node (zero FN relative to the proven
    reference), sentinel containment (absolute), live mutation correctness incl. synthetic-ID
    retrievability, and durable reopen — on real hardware.
  - **Does not prove:** the wire. This is deliberate — the scale dimensions (dict, postings, SoA
    stores, placement distribution, manifest/segment counts, merge) are identical in-process and
    over gRPC, and the transport's own failure modes (framing, deadlines, dict shipping,
    recovery) are separately proven by `tests/cluster_grpc_oracle` + the containerized
    multi-machine harness (ADR-072, criterion 3) across real network boundaries. A 20M ingest
    over localhost tonic would measure protobuf throughput, not index scale. Multi-*machine* at
    scale remains future deployment evidence (Phase 0 item 4), and the **real-corpus audit
    remains criterion 12's open half**.
  - **Known ceiling:** peak residency is roughly Q-proportional (16 GB measured at 20M,
    dominated by holding the cluster + the single-node reference simultaneously for the
    differential), so ~40M is the practical limit of this harness on a 48 GiB box; past that
    needs a streaming build or a staged/partitioned reference (out of scope here).

- **Consequences.**
  - ADR-065 criterion 12 reduces to the real-corpus FN/throughput audit; the roadmap (the live
    criterion tracker) records the split.
  - Purely additive: no engine-source change, no new dependency, no CI change; the lean /
    server / distributed builds and every gate are byte-identical (the target compiles, never
    runs).
  - New at-scale pins land in `benchmark-results.txt` for future runs to reproduce against
    (fan-out distribution at 20M/K=8, selective candidates/title, per-shard balance).

- **Alternatives considered.**
  - **Brute-force reference at 20M** — infeasible (~10¹² evaluations); replaced by the
    proven-reference differential + constructed absolute sentinels.
  - **K-sweep at 20M** (K=1 vs K=8 identity) — redundant with the single-node differential
    (which is also *stronger*: the reference shares no cluster code) and already pinned
    machine-independently at 100k by clusterbench; would multiply build time + memory for no new
    information.
  - **A gRPC leg** — rejected as gold-plating (see "does not prove" above).
  - **A `run_cluster_soak` CI dispatch input** — rejected: the 20M run does not fit the 16 GB
    GitHub runner, a scaled-down CI variant would prove nothing the 200k smoke doesn't, and the
    user's direction is a one-off run with zero recurring footprint.
