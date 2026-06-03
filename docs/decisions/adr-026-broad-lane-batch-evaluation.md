# ADR-026: Broad-lane batch / columnar evaluation (`match_titles_batch`, `POST /_mpercolate`)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Class-C ("broad") queries are quarantined out of the selective path (ADR-003) because
  their best signature is still a *hot* feature (one of the 64 most frequent), so their postings are
  huge. But they were still evaluated **inline, per title**: `Segment::match_into(include_broad=true)`
  walks the huge posting and runs the scalar `ExactStore::verify` once *per title*. The same posting is
  re-scanned for every title containing that hot feature, so candidates/title jump 54 → 684 and
  throughput collapses ~9× (710k → 78k titles/sec/core), p99 ~28× ([results.md](../performance/results.md)
  §1). Broad queries are only ~0.2% of the corpus but dominate match cost — the single biggest
  remaining matching-performance lever ([STATUS](../STATUS.md) Tier 1). The resident-memory prerequisite
  (ADR-020) had already shipped.
- **Decision:** Evaluate the broad lane **once per title-batch, columnar**, while the selective lane
  stays per-title (it is already fast and scale-flat). New module `segment/broad_batch.rs`, exposed as
  `match_titles_batch[_with_stats]` on `Engine`/`EngineSnapshot` (sharing the `MatchView` body so the
  two read paths can't drift) and as a new HTTP endpoint. Mechanics for a batch of titles:
  1. **Per-batch inverted index.** Normalize each title (the same `match_features` call the per-title
     path makes), compute its `tmask`, and build `feature → bitmap-of-titles` + `tmask_batch[t]`.
  2. **Collect reachable broad queries (per segment).** For each *distinct* feature in the batch, form
     `sig_key([f])`, check the segment anchor filter (ADR-011), and probe the segment's `broad` index
     **once** — *this is the amortization*: each huge posting is read once per batch per segment, not
     once per title. Union locals via the existing epoch-stamp dedup.
  3. **Verify by bitmap algebra.** For each reachable broad query, `exact::eval_batch` reproduces
     `verify` clause-for-clause as the bitwise **transpose** over batch-sized title sets (mask gate →
     per-title gate bitmap; required tail → AND of feature bitmaps; forbidden tail → AND-NOT; any-of →
     AND of OR-over-members). Per-query cost is O(#tail + #forbidden + Σgroup) word-ops, *independent of
     how many titles match*, and auto-vectorizes.
  4. **Pure-anchor fast path.** A broad query whose *entire* semantics is its hot anchor (no required
     tail, no forbidden, no any-of, `req_mask` is the single anchor bit) matches exactly the titles
     containing the anchor — emit straight from the anchor's title bitmap with **zero** verification.
  - **Parallelism:** a per-rayon-chunk broad pass (chunk = `broad_batch_size`). Each worker owns its
     scratch (cleared, not freed, between batches — no hot-path allocation); no cross-thread shared
     mutable state, so `par == seq` holds trivially. A posting is walked ~`ncpu` times per batch rather
     than `num_titles` times — same order-of-magnitude win, far simpler than a global scatter/merge.
  - **Reuse `ExactStore` verbatim** on the query side (no parallel broad store); pure-anchor is derived
     from the existing SoA columns at probe time — **no `.seg` format change**. The mmap and in-memory
     segments drive one body via a `BroadBackend` trait.
- **Why correct:** each bitmap clause is the exact transpose of the corresponding scalar test over the
  *same* `ExactStore` columns; retrieval is the same lossless-cover superset narrowed by exactly those
  clauses (signatures are untouched). Forbidden features enter **only** as AND-NOT in verification,
  never in retrieval — the "never gate on MUST_NOT" invariant (ADR-006) holds structurally. The result
  set is therefore **byte-identical** to the per-title `match_title(include_broad=true)` for every
  title and every setting — a pure performance change. Guarded by `tests/broad_batch.rs` (batch≡scalar
  across single/multi-segment, memtable, tombstones, any-of/forbidden, a `broad_batch_size` sweep incl.
  word/chunk boundaries, all three posting variants, `Inline`≡`Columnar`, and `materialize` on≡off),
  an additive brute-force batch oracle in `tests/oracle.rs`, and a batch≡per-title-under-churn test in
  `tests/stress.rs`.
- **HTTP ergonomics — new `/_mpercolate`, `/_search` unchanged.** The plan originally proposed routing
  `/_search`'s `documents:[...]` arm through the batch path "transparently." It is **not** transparent:
  `/_search` returns documented per-slot `stats` (per-title candidate/posting counts), and the columnar
  broad lane amortizes work *per batch*, so per-title broad stats structurally cannot exist there.
  Mirroring Elasticsearch's `_search`-vs-`_msearch` split, the batch path is exposed as a **new** `POST
  /_mpercolate` (ES `_msearch`-shaped `responses[]` envelope, one entry per document, per-request
  `include_broad`, optional top-level broad summary), and `/_search` stays the rich/observable path
  (per-slot stats, `explain`, `profile`, paging) on the per-title matcher. Users pick fast-vs-rich;
  broad-heavy batch workloads go to `/_mpercolate`.
- **Materialization, reinterpreted.** The original spec floated "precomputed/materialized subscriptions
  refreshed periodically" for the broadest queries. Literal periodic-refresh materialization does not
  map to *streaming* percolation (titles arrive continuously; there is no batch to refresh against).
  Its benefit — skipping per-evaluation work for pure-anchor broad queries — is captured instead by the
  pure-anchor fast path, which is exact, always-fresh, and needs no background refresh or extra state.
- **Kill-switches + knobs.** Four **dynamic** config knobs (ADR-022): `broad_batch_size` (256),
  `broad_columnar` (true; false ⇒ provable inline fallback, byte-identical), `broad_materialize` (true;
  false ⇒ pure-anchor queries go through full verification instead), `max_percolate_batch` (10_000;
  bounds per-request work). Plus four cumulative `broad_*` Prometheus counters and a `broad_candidates`
  field on `StatsResponse` — the "metered to a higher cost class" intent from ADR-003.
- **Alternatives considered:**
  - *Switch `/_search` to the batch path* — rejected; silently regresses documented per-slot stats (see
    HTTP ergonomics above).
  - *A global scatter/merge across all titles* — rejected; the per-rayon-chunk pass gets the same
    order-of-magnitude amortization with worker-local scratch and trivial `par == seq`.
  - *A separate broad exact store / `.seg` format change* — rejected; `ExactStore` columns already carry
    everything `eval_batch` and the pure-anchor predicate need.
  - *Roaring/SIMD posting intersection for the very broadest postings* — deferred as a micro-optimization
    on top of this work (plain `Vec<u64>` bitmaps already auto-vectorize and beat roaring at batch-dense
    title sets).
- **Consequence:** The broad lane is no longer the bottleneck. Broad postings scanned amortize
  ~1/`broad_batch_size` (29× at 256, 115× at 1024 — structural, machine-independent, in
  `benchmark-results.txt`); end-to-end the columnar batch runs ~2.4× the inline path and within ~37% of
  the selective ceiling at the same chunking (dev box). Dark by default for the per-title API
  (`include_broad` still opt-in); the batch entry points are additive. **Out of scope (follow-ups):**
  class-C ingest warnings / rewrite-suggestion generation (its own feature; the new broad meters satisfy
  the "metered" intent), and SIMD/roaring broad-posting intersection.
- **See also:** ADR-003 (broad-query quarantine — this is how the quarantined lane is finally
  evaluated), ADR-002 (integer-only hot path — `eval_batch` is allocation-free bitmap integer work),
  ADR-006 (forbidden never gates — preserved structurally in the transpose), ADR-022 (the dynamic
  settings the four knobs plug into), ADR-016 (the lock-free snapshot the batch matchers read),
  ADR-020 (the resident-memory prerequisite), [matching.md](../design/matching.md) §4,
  [api.md](../reference/api.md) (`/_mpercolate`), `segment/broad_batch.rs`, `exact.rs`
  (`eval_batch_slices` / `is_pure_anchor`).

