# Roadmap — what's next

The prioritized roadmap for Reverse Rusty: the **open** future work, grouped by leverage. This doc
deliberately carries only what is **not yet done** — a glance shows the remaining surface. Completed work
is the canonical record of the sibling [`STATUS.md`](STATUS.md) ("what's built vs design-only") and the
[ADRs](DECISIONS.md); component design lives in the [design docs](design/README.md).

---

Priority follows the bottleneck analysis ([`performance/results.md`](performance/results.md) §9): the
selective match path is already ~255× the spec target with a flat ~54 candidates/title, so the leverage
is in the **durability + scale** story and **feature-model quality** — not in shaving the selective
candidate count further.

## Shipped milestones (detail → [`STATUS.md`](STATUS.md))

These tiers were active build work and are **complete**; they are summarized here only so the open tiers
below keep their context (the per-feature detail + status lives in [`STATUS.md`](STATUS.md)).

- **Tier 0 — Cluster v1 acceptance gate — complete.** The in-process multi-shard core + durable local
  reopen + dynamic vocabulary (ADR-046), oracle-proven zero-false-negative — the shippable milestone. The
  named gate is `tests/cluster_oracle.rs` + `tests/cluster_durability_oracle.rs` ([`testing.md`](testing.md)).
- **Tier 1 — the measured bottlenecks — complete.** Broad-lane batch / columnar evaluation (ADR-026) and
  resident-footprint reduction (ADR-020).
- **Tier 4 — ES/OS percolator parity — complete.** Per-query metadata + filtered percolation (ADR-049 /
  ADR-055), punctuation-equivalence folding (ADR-058), ranking + `/_mpercolate` pagination (ADR-059), and
  bulk synonym/alias file loading (ADR-060). **One deferred sub-item:** cluster (multi-shard) ranking —
  the cross-shard priority fetch at the coordinator merge, behind the same `RankSpec` seam (ADR-059).

## Tier 2 — feature-model quality & self-tuning (residue)

The mechanisms shipped (compaction-that-improves ADR-056, NPMI corpus phrases ADR-053, equivalence/alias
expansion ADR-054 + bulk loading ADR-060); what remains is the higher-precision **alias-discovery
sources** and the rest of the "improve" menu.

- **Alias discovery — the deferred sources.** The expansion *mechanism* + the declared / any-of-learned /
  file-loaded sources are shipped; still open, in precision order: **distributional discovery**
  (context-similarity candidates — noisy, conflates substitutes with co-hyponyms, so review-first) and
  **match-feedback validation** (the highest-precision *automated* signal — needs an operational
  title→query loop). Both feed the shipped expansion mechanism when built.
- **The rest of the §7 "improve" menu** ([`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) §7):
  candidate-survival telemetry; `recommended_shard_count` / `recommended_arity`; feature-ID re-ranking for
  locality; re-running the corpus learner per range; and the **vocab-consolidation re-materialize** that
  consolidates hashed terms / learned synonyms on compaction (distinct from the frequency-drift
  re-anchoring already shipped in ADR-056). Re-ranking the frozen 64-hot mask itself is a major-version
  blue/green concern (Tier 3).

## Tier 3 — scale & production maturity (larger builds)

- **Feature-model versioning + blue/green re-materialize.** Frozen common-mask across minor versions; a
  major model change is replayed from the log into a parallel index, then an atomic alias/epoch swap.
- **Harden the distributed multi-node layers for real machines.** The full shared-nothing stack is built
  and oracle-proven *in-process / on localhost* (ADR-027 / 029 / 031–048; per-ADR detail in
  [`STATUS.md`](STATUS.md)), but not yet deployed and hardened across real machines. The build path +
  cross-shard correctness argument are in
  [`design/clustering-and-scaling.md`](design/clustering-and-scaling.md) §10. Still **design-only** — the
  production multi-node residue (on the **shared-nothing** model — no object store / cloud dependency,
  ADR-033):
  - **Auto-split** + `recommended_shard_count` — the autoscaler's split recommendation needs a real split
    mechanism (ring re-keying; `num_shards` is fixed at construction today) and the clean node→endpoint
    move it implies.
  - **Cross-process dynamic vocabulary / normalizer shipping** — shipping learned aliases + the
    punctuation table to a remote shard's normalizer (the in-process piece is the now-complete Tier-0 v1
    item; [research spike](research/dynamic-vocabulary.md)).
  - **Replicate-broad-to-all** (in-process uses the shard-0 lane only).
  - **TLS / auth** on the (currently plaintext) gRPC + control transports.
  - An end-to-end **durable-multi-node rolling-restart harness**.
- **Aspects-first ingestion.** Use eBay structured item-specifics as features instead of relying only on
  title parsing — higher feature quality, but a larger domain integration.

## Validation (the open credibility step)

- **Real-corpus false-negative / throughput audit.** The oracle + benchmarks run against the seeded
  synthetic generator only ([`STATUS.md`](STATUS.md) Current limitations). Running a real saved-search
  corpus with messy listing titles through the normalizer — a FN/FP + throughput audit — is the
  highest-leverage step for external credibility and a prerequisite before quoting the headline numbers as
  production guarantees rather than design-target evidence.

## Polish / niche

- **SIMD intersection** for medium/large (mostly broad-lane) roaring postings — a micro-optimization best
  folded into the broad-lane work.

## Evaluated & declined

- **Query-family / shared-prefix DAG** (subtree pruning). Implicit anchor-sharing already captures the
  near-duplicate-clustering benefit, the selective path isn't the bottleneck, and the
  mmap-serialization + compaction-rebuild cost wasn't justified. See [`DECISIONS.md`](DECISIONS.md)
  ADR-019.

---

## Nice-to-have / operational polish backlog

Low-priority polish, ergonomics, and micro-optimizations — none are production blockers. Roughly grouped:

**API / ops ergonomics**
- **No CORS headers** — browser-based tools can't hit the API. Add `tower-http::CorsLayer`.
- **No `--version` flag** in the CLI.
- **No Dockerfile or k8s manifests.**
- **No thread-pool introspection** (`/_cat/thread_pool` equivalent).
- **No per-segment filter FP rate in `/_cat/segments`** (deferred from ADR-023). The anchor filter doesn't
  retain its inserted key count, and the mmap arm doesn't expose the filter's block count through the
  `BaseSegment` wrapper — so an honest, *symmetric* false-positive-rate column (real for both memory and
  mmap segments) needs a small change first: have `SegmentFilter` retain `n` at build time and expose
  block count on `MmapSegment`. Then add a `filter_fp_pct` column to the endpoint.
- **`_cat` endpoints lack ES `?v` / `?h` / `?help` flags** (noted in ADR-023). `/_cat/*` returns a fixed
  text table (always with a header) or `?format=json`; ES also supports a verbose toggle, column
  selection, and a help listing. Low-value polish, listed for completeness.
- **`took_ms` uses raw f64** — yields values like `0.003284000000000001`. Use integer ms or round to 2 dp.
- **No pre-warming** for mmap'd segments on cold start.

**Memory / hot-path micro-optimizations**
- **`alive: Vec<bool>`** uses 8× the memory of a bitvec (1 byte vs 1 bit per entry).
- **`seg_lens` Vec allocated on the match hot path** — could be a fixed-size array.
- **WAL `append_insert` allocates a Vec per write** — production WALs use pre-allocated write buffers.
- **Byte-at-a-time CRC-32** for manifest writes — table-based would be ~10× faster.

**Robustness / build hygiene**
- **Durable-ingest segment-write failures surface only as `ingest_rollback`, not `segment_write`.** ADR-021
  routes the *flush* path's segment write through a precise `DurabilityOp::SegmentWrite`, but the durable
  build/bulk path (`build_durable_base`) returns the `io::Error` up to the infallible wrapper, which emits
  `IngestRollback` with the OS error in the `error` field — so the operator sees the cause but not the
  precise op label. Optional refinement: emit `SegmentWrite`/`SegmentMmap` from inside `build_durable_base`
  for symmetric labeling. Low priority — the underlying error is already visible.
- **Deferred from the external-review hardening pass (ADR-052):**
  - **Optional bearer-token / API-key auth for mutating endpoints.** The HTTP server defaults to a
    loopback bind (`--host 127.0.0.1`), but has no built-in auth — exposing it requires a trusted network
    or an authenticating reverse proxy. An opt-in `RR_AUTH_TOKEN`-style gate on
    `_doc`/`_bulk`/`_flush`/`_compact`/`_vocab`/`_settings` would let it serve a wider network safely.
  - **Cooperative cancellation on the match path.** `timeout_ms` is a response deadline only — a timed-out
    `/_search`/`/_mpercolate` returns 408 but its `spawn_blocking`/Rayon work runs to completion. A coarse
    per-segment deadline check could shed abandoned CPU, at the cost of a branch on the (deliberately
    branch-predictable) hot path; weigh against simply bounding concurrency.
