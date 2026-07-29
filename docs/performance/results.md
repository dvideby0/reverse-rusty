# Performance Results — Reverse Rusty

All numbers below are **measured**, not modeled, on Reverse Rusty in `engine/`, but they come from
several dated captures. Early prototype tables are retained as historical algorithm evidence and are
labelled as such; the current generic 1M persisted-memory profile and historical 20M scale proof have
their own sections.
The benchmark runbook,
the machine-independent **invariants** to verify on any box, and the dated **capture log** are in
[`benchmark-results.txt`](benchmark-results.txt). Where the report extrapolates to the 100M-query
target, the assumptions are stated explicitly. See [`README.md`](README.md) for the headline numbers
and reproduction commands; [`../CHANGELOG.md`](../CHANGELOG.md) for the chronology of later shipped
changes.

Capture hardware and toolchain are recorded beside each dated entry in
[`benchmark-results.txt`](benchmark-results.txt). The early §1–§7 prototype capture used a small
4-core/3.8-GiB aarch64 sandbox and predates the daachorse, roaring, mmap, and rayon implementations;
do not use its process-RSS table as the current memory profile.

The workload target from the spec: **100M stored queries, 10M titles/hour (~2,778 titles/sec),
frequent updates, zero false negatives.**

---

## 1. Historical prototype throughput capture

| Config | Queries | Candidates/title (avg, p99) | Throughput (titles/s/core) | p99 latency | RSS/query |
|---|---:|---|---:|---:|---:|
| Selective realtime path | 1,000,000 | 54.6 / 112 | **709,763** (255× target) | 2.25 µs | 258 B |
| Selective realtime path | 3,000,000 | 54.4 / 112 | 518,213 (187×) | 3.42 µs | 256 B |
| Selective realtime path | 5,000,000 | 54.3 / 112 | 437,547 (158×) | 2.46 µs | 258 B |
| With broad lane inline (naive) | 1,000,000 | 684 / 2,311 | 78,269 (28×) | 63.6 µs | 258 B |
| Adversarial skew 3.5 + broad | 1,000,000 | 670 / 2,474 | 288,583 selective / 74,025 w/broad | 62.3 µs | — |

**The two findings that matter:**

1. **The selective path is fast and scale-flat.** Candidates/title stay **pinned at ~54** whether
   there are 1M, 3M, or 5M stored queries. The architecture's per-title cost is governed by the
   *entity space density*, not the *total number of queries* — which is the whole point. Even on
   one core, the selective path runs at **158–255× the 2,778 titles/sec target**.

2. **Broad queries were the entire risk — and are now batched.** Folding broad queries (5% of the
   population, concentrated on hot entities) into the realtime path *inline* collapses throughput by
   **~9×** (710k → 78k) and inflates p99 latency by **~28×** — the percolator "unsupported/un-gateable
   query becomes an always-candidate" failure mode, reproduced and measured (the "naive" row above).
   Reverse Rusty now evaluates the broad lane **once per title-batch, columnar** (ADR-026): each hot
   broad posting is scanned once per batch instead of once per title, and per-query verification is
   bitmap algebra. The broad work amortizes ~1/batch_size — broad postings scanned drop **29× at batch
   256, 115× at batch 1024** (machine-independent; the regression gate in
   [`benchmark-results.txt`](benchmark-results.txt)). On the dev box (M4 Max, 16-thread) the batched
   broad lane runs **~2.4× faster than the inline path** and adds only ~37% over the selective ceiling,
   while staying byte-identical to the per-title result (`tests/broad_batch.rs`). Pure-anchor broad
   queries (whole semantics == one hot term) skip verification entirely. See
   [`../design/matching.md`](../design/matching.md) §4.

---

## 2. Correctness (the hard requirement)

The shared-front-end differential suite (`tests/oracle/`) compares candidate retrieval + exact
verification with brute-force evaluation over the same compiled feature semantics. Its pinned
historical run covered 40,000 queries × 4,000 titles:

```
oracle: truth_matches=109024 engine_matches=109024 false_neg=0 false_pos=0
```

**Zero false negatives** (the contract) and **zero false positives** (the exact matcher is exact).
The lossless-cover invariant holds empirically over ~109k real matches. The spec's worked example
also passes its hand-written PASS/FAIL expectations (`spec_example_matches_expected`).

Independence from the production parser/normalizer is tested separately under
`tests/independent_oracle/`, which uses the zero-dependency `ref-matcher/` implementation and compares
canonical feature strings. Calling the in-tree `tests/oracle/` brute force “front-end independent”
would overstate what that suite proves.

---

## 3. Historical prototype build throughput

Building (parse → extract → finalize mask → choose signatures → SoA + index) runs at a steady
**~650,000 queries/sec/core** across all scales:

| Queries | Build time | Rate |
|---:|---:|---:|
| 1,000,000 | 1.48 s | 677k/s |
| 3,000,000 | 4.55 s | 659k/s |
| 5,000,000 | 7.70 s | 650k/s |

Build is linear in query count and trivially parallelizable per shard. The cost-class split at 5%
broad fraction is consistently **~99.5% class A (selective), ~0.2% class B, ~0.2% class C (broad)**,
0 class D — i.e. the compiler keeps almost everything on the fast realtime path.

---

## 4. Candidate generation — the core metric

The whole engine exists to make this number small. The historical 1M–5M selective capture measured:

```
historical avg unique candidates/title : 54.3   (p95 = 96, p99 = 112, max ≈ 130)
                                             — FLAT across 1M..5M queries
avg exact verifications     : ≈ candidates (each candidate gets one integer-only verify)
```

The retired fixture supplied those 1M–5M measurements. The current seeded generic generator has a
separately pinned 1M-query/20k-title structural baseline: 1,077,983 unique candidates in total,
**53.90/title**, p95 95, p99 111, and max 136. Generic 3M/5M captures and a reviewed timing history
have not yet been recorded, so the historical series—not the current 1M pin—is the evidence for
scale-flatness and throughput at those sizes.

The generic fixture's selective query families combine a year, brand, `collection####`,
`product#####` or named entity, and optional attribute/variant; its broad families use a skewed
popular entity, `wireless pro`, or `bundle`. Its ~**54** candidate count is evidence for signatures
over that exact workload, not a measured OpenSearch comparison. The count includes deliberately
conservative arity-2 hot-pair generation; the exact matcher then rejects candidates that do not
survive verification.

The generic pin records 5,234,864 confirmed row matches versus 6,610,573 in the retired fixture
(20.8% fewer). That is not a recall or coverage score. A small difference in the title-hit
probability of one repeated broad-query family is multiplied across thousands of identical stored
rows, while the candidate sum changes by only 0.4% and candidate p95/p99 remain exactly 95/111.
The new workload therefore retains retrieval and rejection pressure but asks exact verification to
reject more broad candidates. At this pool size, independently sampled
entity+year+brand+collection families contribute effectively no confirmed rows; `match_sum` is
therefore a broad-lane output-volume metric, while the selective timing lane primarily measures
candidate rejection. Correctness and successful selective matching remain owned by the
differential/self-match oracles, not by making this benchmark's `match_sum` resemble the retired
fixture.

The **common-mask gate** in the exact matcher (two `u64` ops over the 64 hottest features) is what
makes each of those 54 verifications cheap: most are rejected before any memory traffic beyond the
candidate's two mask words. The historical p99 latency of ~2–3 µs for the full normalize → generate
→ verify cycle reflects this. The generic fixture now has permanent absolute timing safety limits;
its more sensitive variance history is pending the reviewed CI rebaseline.

### CPU rank-profile cost

ADR-162's ranking remains post-match: `static_v1`, linear, and tree profiles see the same confirmed
members, and K reduces retained/delivered rows rather than evaluations. On the dated 20k-query
`rankbench` capture, the illustrative three-term linear profile and two-stump tree profile added
roughly 0.6–1.6 microseconds per title over static scoring across the four workload shapes. The
percentage delta looks larger because the complete static workloads take only about one to two
milliseconds for hundreds of titles. This is evidence that feature extraction plus tiny CPU models
is practical, not a production LambdaMART forecast: tree cost grows with the number and depth of
paths actually evaluated, and broad match counts multiply that work. Exact numbers, fingerprints,
commands, and caveats are in [`benchmark-results.txt`](benchmark-results.txt).

---

## 5. Current memory and durable-size profile

ADR-020's source-store work changed the profile enough that the old ~256 B/query process-RSS table
must not drive deployment sizing.

Current pinned captures:

| Workload/profile | Engine-accounted resident | Durable bytes | Interpretation |
|---|---:|---:|---|
| Current generic 1M persisted, `retain_source=false` | 6,005,244 B (6.01 B/query) | 244,585,986 B (244.59 B/query) | the CI regression baseline in `perf-baseline.json`; four committed files |
| Historical 20M selective, `retain_source=false` | ~5.2 B/query | — | compiled structures + dictionary, source excluded from resident accounting |
| Historical 20M selective, `retain_source=true` | ~109.0 B/query | — | resident source dominates |
| Historical 1M component capture, resident source | source ~113.5 B/query; dict ~4.9 B/query | — | illustrates why source policy is the first sizing choice |

These are engine-reported/accounted bytes for the named workloads, not a promise about process RSS.
Allocator overhead, mmap residency, filesystem page cache, source/explain access, tags, predicates,
and lane mix still consume real host memory. Durable bytes include source/file-format data and must be
measured independently from resident memory.

The earlier prototype capture is preserved below only to show that its process RSS scaled linearly:

| Queries | historical exact SoA | historical main postings | historical process RSS |
|---:|---:|---:|---:|
| 1,000,000 | 73 MB | 4.9 MB | 258 MB |
| 3,000,000 | 243 MB | 14.6 MB | 767 MB |
| 5,000,000 | 487 MB | 24.3 MB | 1,289 MB |

Do not turn either table into a fixed shard count. Selective A/B/H rows divide across positions,
while C/D and unroutable default-visible rows replicate to every position; RF multiplies physical
copies. The current sizing method, including page-cache and transient-copy headroom, is in
[`../operations/sizing.md`](../operations/sizing.md).

---

## 6. Updates

The hot-delta + tombstone path measured in the early capture gave:

```
live updates : 50,000 in ~0.065 s  ≈ 750,000 updates/sec/core   visibility: immediate
```

The shipped durable path is log-first, applies the new version/tombstone, and publishes a new
immutable snapshot before a successful operation returns. Background compaction folds the delta,
reclaims dead rows, rebuilds postings/filters, and can optionally re-anchor under deterministic
visibility guards; it does not run a learned cover optimizer. Treat the historical update rate as a
dated microbenchmark, not a durability or multi-node write-SLO guarantee.

---

## 7. LSM multi-segment read amplification (segbench)

The LSM-shaped engine probes **every** segment per title and unions the results, so per-title probe
work scales ~linearly with segment count (the read-amplification fact that
[`../design/ingestion-and-updates.md`](../design/ingestion-and-updates.md) §2 is built around).
Measured with `segbench` on a 300k-query / 3k-title corpus (`broad_frac=0.0`, seed `0xC0FFEE`) split
into K equal bulk-ingested base segments (`build_from_queries(chunk0) + bulk_ingest(chunk1..K-1)`,
empty memtable):

| Segments | Candidates/title | Postings/title | Throughput (titles/s/core) |
|---:|---:|---:|---:|
| 1 | 53.48 | 53.48 | 717,728 |
| 2 | 55.11 | 55.11 | 574,005 |
| 4 | 56.67 | 56.67 | 327,257 |
| 8 | 57.68 | 57.68 | 351,379 |

**Reading:** throughput falls ~with segment count (≈2× from 1→8 segments) — the read-amplification of
fanning every title across all segments (one signature-map lookup per probe per segment).
**Candidates/postings per title stay nearly flat** because, over a large synthetic entity space,
signatures are highly selective: most per-segment probes hit empty/tiny postings, so the dominant
added cost is the probe (hash-lookup) *count*, not extra exact-verified candidates. Compaction
(merging K segments back to 1) repays this read tax. Cache-line blocked per-segment Bloom filters are
now built and skip definite-miss signature probes; the historical table predates that implementation,
so it does not quantify the current filter benefit (see
[`../design/ingestion-and-updates.md`](../design/ingestion-and-updates.md) §6). Run time ~6.5s
(<40s budget). Reproduce: `cargo run --release --bin segbench -- 300000 3000 0.0`.

---

## 8. Behaviour under skew and adversarial inputs

- **Hot-entity skew (zipf, skew=3.5):** the selective path holds at **288k titles/sec/core** with
  flat candidate counts — popular entities don't poison the selective lane because class-A queries
  anchor on the *rarer* required feature (the model), not the hot entity.
- **Broad queries:** isolated by classification. Inline they cost ~9× throughput; the engine now
  batches them columnar (once per title-batch, ADR-026) so the broad work amortizes ~1/batch_size.
  The engine measures and reports the broad contribution separately every run (`of which broad lane`,
  plus the `BROAD LANE` section's inline-vs-columnar comparison and amortization sweep), so the cost
  — and the win — is always visible.
- **Near-duplicate query clusters:** generated at `family_size=8`; the signature index naturally
  shares anchors across a cluster, so a single failed anchor probe eliminates the whole cluster's
  candidates at once — realized *implicitly* at anchor granularity, with no explicit family structure
  (that structure was evaluated and declined; see [`../DECISIONS.md`](../DECISIONS.md) ADR-019).

---

## 9. Bottleneck analysis & where Reverse Rusty is honest about its limits

- **#1 bottleneck: repeated broad/hot work.** Quarantine + columnar batch evaluation (ADR-026)
  amortize broad postings across a title batch, and the always-visible hot tier (ADR-105) moves
  runtime-hot non-top-64 anchors off the scalar lane without changing visibility. Canonical-body
  sharing (ADR-106) addresses concentration: in the latest 20M broad-bearing in-memory capture it
  reduced body candidates/title from 6,616.65 to 53.75 and the largest main posting from 43,533 to
  103 while emitting the identical ~6.5k matches/title. That recovery was dedup-driven; only 782
  rows entered H at θ=1024 in this synthetic corpus. Current mmap segments expand body members at
  flush, so the persisted/durable counterpart remains measured roadmap work rather than a solved
  cost.
- **#2: memory bandwidth at scale.** Candidate counts are flat but absolute throughput drops as the
  index leaves cache. Mitigation: sharding for cache residency, tighter SoA packing, mmap segments.
- **Simplifications at the time of this capture (status updated inline):**
  - ~~The alias extractor is a token trie, not the daachorse double-array automaton.~~ **Resolved:**
    daachorse v3 double-array Aho-Corasick (leftmost-longest) is now the shipped alias matcher
    (`src/normalize.rs`).
  - ~~Large postings would use the `roaring` crate.~~ **Resolved:** three-tier adaptive postings —
    inline (≤8) → `Vec<u32>` (≤256) → `roaring` bitmap (>256) — are implemented in `src/index.rs`.
  - Near-duplicate clustering is realized only *implicitly* (near-duplicates share signature anchors),
    **not** as an explicit shared-prefix DAG with subtree pruning. The explicit structure was
    **evaluated and declined** (ADR-019): the implicit sharing already captures the benefit, the
    selective path is not the bottleneck (the broad lane and memory bandwidth are), and the DAG's
    mmap-serialization / compaction-rebuild cost was not justified against an already-flat ~54
    candidates/title.
  - The dictionary retains one string per interned feature, but current component captures put it at
    roughly 4–5 B/query on the synthetic 1M/20M workloads. Resident query source—not duplicate
    per-query dictionary strings—dominates the `retain_source=true` profile. Mmap/source-store
    residency must be measured separately.
  - ~~Matching is single-threaded here.~~ **Resolved:** rayon parallel matching (`match_titles_par`)
    delivers ~3.8× on 4 cores; the per-core numbers above remain the right unit for the algorithm's cost.

---

## 10. Verdict against the spec's objective

> *Produce a design and prototype … that can plausibly outperform Lucene/OpenSearch-style generic
> percolation by one or more orders of magnitude on marketplace-style product listing titles.*

In the historical selective capture Reverse Rusty sustained **158–255× the throughput target on a
single core** with **flat ~54 candidates/title** and zero false negatives; the 20M current capture
extends the candidate-flatness result. The engine reproduces and then controls—rather than
eliminates—the broad-query failure mode with opt-in visibility, columnar batching,
visibility-neutral hot scheduling, and in-memory body sharing. The
order-of-magnitude claim is supported by measurement for the selective majority of the synthetic
workload. Since the earliest
captures, daachorse, roaring, mmap'd segments, rayon parallel matching, broad-lane batching, and the
multi-shard core have shipped; see [`../CHANGELOG.md`](../CHANGELOG.md). The multi-shard leg is now
*measured*, not just specified, at 20M (§11); remaining evidence and memory work lives in the
[`roadmap`](../roadmap.md).

---

## 11. Scale proof: the 20M multi-shard soak (ADR-104)

The scale half of Distributed-v1 criterion 12 ([ADR-065](../decisions/adr-065-distributed-v1-graduation.md)):
one run (`tests/cluster_soak/`, [ADR-104](../decisions/adr-104-cluster-scale-soak.md)) builds a
**durable K=8 in-process cluster over 20,002,000 queries** (20M generated + 2k planted sentinels,
seed-deterministic; 20.61M stored entries across the shards — broad-lane queries replicate to all
K, ADR-080), and proves at that scale what the cluster oracles prove at ≤100k:

- **Zero false negatives relative to the proven reference:** the cluster's full match set equals
  the single-node engine's on **every one of 50k titles**, before *and* after live mutations
  (100k synthetic-ID adds + 20k upserts + 200k deletes, mirrored on both engines). The
  single-node engine is the reference that scales — brute force at 20M×50k is ~10¹² evaluations —
  and it runs none of the cluster code, so a cluster-layer FN cannot cancel out.
- **Absolute zero-FN sentinels:** 2,000 planted query/title pairs are retrieved by containment at
  every checkpoint (pre-mutation, post-mutation, post-reopen) — the check a relative differential
  structurally cannot make.
- **Bounded fan-out at 20M:** avg **3.18** shards probed/title, p50 3, p95 5, **p99 5, max 7 of
  8** — content routing still touches a handful of shards, never all K, at 200× the corpus the
  invariant was pinned on. Placement stays balanced (per-shard max ≤ 1.19× min).
- **Durable reopen at 20M:** `flush → checkpoint → drop → open` reattaches the coordinator
  manifest + mmap segments and re-serves a recorded 2k-title subset + all sentinels
  **byte-identically**, with no deleted id resurrected — the reopen path had never run past ~100k.

**Candidates/title at scale — the honest reading.** With the broad lane ON, candidates/title
*grows with corpus size by design* (the recorded lineage: 85.64 @100k → 682 @1M → **10,036
@20M** on this corpus shape) — broad-lane volume is exactly what the ADR-026 columnar batch path
amortizes, so the soak captures it rather than banding it. The engine's **flatness claim is the
broad-OFF selective path**, and it holds at 20M: `bench 20000000 20000 0.0` measures **54.56
candidates/title (p95 96, p99 112, max 152; max main posting 104)** — bit-compatible with the
54.5 pinned at 1M and 54.29 at 5M — at **~438k titles/sec/core**. Commands, pins, and the dated
capture: [`benchmark-results.txt`](benchmark-results.txt).

ADR-106 does not invalidate this durable baseline: its Stage-A body groups are expanded when an
in-memory segment flushes. The post-ADR-106 durable K=8 rerun therefore reproduced the same
10,035.55 candidates/title and stayed fully correct across reopen. The corresponding in-memory
capture retained the sharing and recovered the repeated-body scan described in §9.

**What this run deliberately does not prove:** the gRPC wire at scale (the scale dimensions —
dict, postings tiers, placement, manifest — are transport-identical; the wire's own failure modes
are owned by the gRPC oracles + the ADR-072 single-host container-network harness), and the **real-corpus
FN/throughput audit**, which remains criterion 12's open half (intake: the ADR-087
`RR_ORACLE_CORPUS` hook).
