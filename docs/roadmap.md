# Roadmap — what's next

The prioritized roadmap: **open work only.** When an item ships it is **deleted from this file** —
its ADR becomes the permanent record and [`STATUS.md`](STATUS.md) gets a one-line entry (the
editing rule lives there). Tier numbers are stable — ADRs and PRs reference "Tier N" — so a
completed tier keeps its heading and one line. Decision rationale → [`DECISIONS.md`](DECISIONS.md);
component design → [`design/`](design/README.md).

Priority follows the bottleneck analysis ([`performance/results.md`](performance/results.md) §9):
the selective path is far past the spec target with a flat ~54 candidates/title, so the leverage is
in the broad lane, memory/footprint, and the durability + scale story — not in shaving the
selective candidate count further.

### Tier 0 — Cluster v1 acceptance gate — ✅ complete

Dynamic vocabulary (ADR-046), the named cluster oracle gate ([`testing.md`](testing.md)),
`clusterbench` fan-out invariants, and the v1/experimental reframe — all shipped; see
[`STATUS.md`](STATUS.md).

### Tier 1 — measured bottlenecks — ✅ complete

Broad-lane batch/columnar evaluation (ADR-026) and resident-memory reduction (ADR-020) — shipped.

### Tier 2 — feature-model quality & self-tuning

Shipped: NPMI phrases (ADR-053), equivalence expansion (ADR-054), compaction re-anchoring
(ADR-056), alias governance + multi-word activation (ADR-060/061). Open:

- **Distributional alias discovery** — context-similarity candidates feeding the shipped
  ADR-054/060 seam. Noisy (conflates substitutes with co-hyponyms) ⇒ review-first, never
  auto-active.
- **Match-feedback alias validation** — the highest-precision *automated* alias signal; needs an
  operational title→query feedback loop.
- **The rest of the "improve" menu** ([`design/ingestion-and-updates.md`](design/ingestion-and-updates.md)
  §7): candidate-survival telemetry, `recommended_shard_count`/`recommended_arity`, feature-ID
  re-ranking for locality, re-running the corpus learner per range.
- **Vocab consolidation on compaction** — background re-materialize of hashed terms / learned
  synonyms into the dict (the ADR-046 deferral; distinct from ADR-056 re-anchoring).

### Tier 3 — scale & production maturity

- **Distributed v1 ([ADR-065](decisions/adr-065-distributed-v1-graduation.md)) — open criteria**
  (1–11 shipped: ADR-070/071/072/074/075/076/077/078/079/080/081, + follow-ons ADR-082/083/084; see
  [`STATUS.md`](STATUS.md)):
  - **Criterion 12 — scale proof at target:** a multi-shard load test at ≥20M stored queries on
    real hardware (largest soak to date: 10M single-node), plus the **real-corpus FN/throughput
    audit** owed in [`STATUS.md`](STATUS.md) "Current limitations".
  - *Criterion 7 follow-ons (deferred, [ADR-078](decisions/adr-078-cluster-resize.md)):* always-on
    autoscaler-driven resize (needs hysteresis to avoid thrash, since a resize is non-idempotent +
    `O(corpus)`) + a cross-process / online resize (ship the re-keyed data to remote shards over the
    live-handoff machinery; the v1 resize is in-process blue/green).
  - *Live-handoff follow-on (deferred, [ADR-086](decisions/adr-086-control-plane-routing-and-failover.md)):*
    **data-moving reassignment** — a committed assignment change *driving* `execute_handoff`
    (peer-recovery + `HandoffShard` swap) so a reassignment moves data and routing re-points LIVE while
    the coordinator runs. ADR-086 shipped the boot-time half — routing by the committed shard→node
    assignments (opt-in `--route-by-assignments`, position-preserving + a fail-loud guard) +
    multi-control-endpoint failover — so the deferred remainder is the runtime re-point loop with a
    zero-FN proof under concurrent writes (until then a non-data-moving HRW `rebalance` must not be
    used to re-point routing on a populated cluster). (k8s/Helm manifests + gRPC health/readiness
    probes shipped — [ADR-084](decisions/adr-084-kubernetes-helm-health.md); ADR-082 closed the
    advertise-URL; the `shardserver --accept-class-d` item was a phantom — remote shards force-accept
    class-D, the coordinator is the sole gate.)
- **Feature-model versioning + blue/green re-materialize** — frozen common-mask across minor
  versions; a major model change replays the log into a parallel index, then an atomic epoch swap.
- **Aspects-first ingestion** — use eBay structured item-specifics as features instead of relying
  only on title parsing; higher feature quality, larger domain integration.

### Tier 4 — ES/OS percolator parity — small residue

The parity program is **complete** — tags + filtered percolation (ADR-049/055), punctuation
folding (ADR-058), ranking (ADR-059/075), alias evolution (ADR-060/061), and the ADR-064 drop-in
work package (ADR-067/068/069/073); workload mapping →
[`research/percolator-workload.md`](research/percolator-workload.md). Open refinements, each
behind a shipped seam:

- **Component-conjunction alternative on alias activation** — keep the scattered-components
  reading when a multi-word alias activates, via CNF distributivity (recall-only widening,
  bounded; ADR-061 §semantics-of-activation).
- **Additive punctuation fold** — emit the joined form AND the split components (à la Lucene's
  `WordDelimiterGraphFilter`); pure recall gain behind the ADR-058 `PunctClass` seam.
- **Columnar two-view broad lane** — the broad lane drops to the inline path while multi-word
  aliases are active; a perf follow-on, not correctness (ADR-061).

### Evaluated & declined

- **Query-family / shared-prefix DAG** — optimizes a non-bottleneck at high format/rebuild cost;
  see [ADR-019](decisions/adr-019-query-family-factoring-declined.md). Reversible.

---

## Nice-to-have / operational polish backlog

Low-priority polish and micro-optimizations — none are production blockers.

**API / ops ergonomics**
- **CORS headers** — browser-based tools can't hit the API; add `tower-http::CorsLayer`.
- **Thread-pool introspection** (`/_cat/thread_pool` equivalent).
- **Per-segment filter FP rate in `/_cat/segments`** (deferred from ADR-023) — needs
  `SegmentFilter` to retain its inserted-key count and `MmapSegment` to expose block count, then a
  `filter_fp_pct` column.
- **`_cat` `?v`/`?h`/`?help` flags** — ES-style verbose/column-selection; listed for completeness.
- **`took_ms` uses raw f64** (`0.003284000000000001`) — integer ms or round to 2 dp.
- **No pre-warming** for mmap'd segments on cold start.
- **No measured restart/reopen time** at ≥1M queries (ADR-064 item 7) — the design implies
  sub-second-to-seconds; capture a number.
- **Tags are write-only over REST** — no endpoint returns a stored query's tags; small read-back
  addition for metadata audits (ADR-064 item 7).
- **Class-C ingest warnings / rewrite suggestions** — surface "this query landed in the broad
  lane" at ingest with a rewrite hint (the ADR-026 follow-up).

**Memory / hot-path micro-optimizations**
- **`alive: Vec<bool>`** — 8× the memory of a bitvec.
- **`seg_lens` Vec allocated on the match hot path** — could be a fixed-size array.
- **WAL `append_insert` allocates a Vec per write** — pre-allocated write buffers.
- **Byte-at-a-time CRC-32 for manifest writes** — table-based is ~10× faster.
- **SIMD intersection** for medium/large (mostly broad-lane) roaring postings (the ADR-026
  follow-up).

**Test-infrastructure follow-ons (ADR-063 audit)**
- **Extend the parse-union oracle's fuzz alphabet** with `#`/`/`/`pop` markers, 4-digit years, and
  fused graders *inside phrase patterns* (needs the reference `emit_parse` to learn the
  marker/year rules).
- **A cross-seam integration harness** (recovery×vocab, adopt-on-fresh vs adopt-on-recovered,
  `set_vocab` guard matrix) — ~22% of historical review-caught escapes were cross-seam; point
  regression tests exist, the *combinations* don't.
- **Occasional targeted `cargo-mutants` runs** on `normalize`/`compile`/`exact` after major
  matcher changes (declined as a per-PR gate in ADR-063 for wall-clock cost).
- **Messy variants of the cluster oracles** — thread `messify_dataset` through
  `tests/cluster_oracle` + the durability oracle (the harnesses already take a `Dataset`).

**Robustness**
- **Durable-ingest segment-write failures surface as `ingest_rollback`, not `segment_write`** —
  emit `SegmentWrite`/`SegmentMmap` from inside `build_durable_base` for symmetric labeling (the
  OS error is already visible; low priority).
- **Cooperative cancellation on the match path** (ADR-052 deferral) — `timeout_ms` is a response
  deadline only; timed-out work runs to completion. Weigh a coarse per-segment deadline check
  against simply bounding concurrency.
