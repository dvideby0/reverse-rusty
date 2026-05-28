# Design — a domain-aware reverse product-query matcher

*Architecture overview, the correctness contract, and the module map. The per-component detail lives
in the topic files linked below. Read [`../research/prior-art.md`](../research/prior-art.md) first for
the borrowed ideas. The PoC in `engine/` implements the core of this design at reduced scale to
validate the numbers (see [`../STATUS.md`](../STATUS.md) for what's built vs design-only).*

## Topic files

- [`normalization.md`](normalization.md) — query DSL, the shared query/title normalizer, the feature
  dictionary, and the normalizer hardening from real eBay data.
- [`matching.md`](matching.md) — the signature-cover optimizer, the candidate index, the integer-only
  exact matcher, broad-query cost classes (A/B/C/D), query-family factoring, and explain tooling.
- [`ingestion-and-updates.md`](ingestion-and-updates.md) — immutable segments + hot delta + tombstones,
  the LSM write path, deltas-with-merge, bulk-ingest vs rebuild rules, and feature-model versioning.
- [`clustering-and-scaling.md`](clustering-and-scaling.md) — sharding sketch and the full
  horizontal-scaling design (entity-anchor content routing, OpenSearch/Aurora cluster layers,
  self-tuning autoscaling).

---

## 1. Mental model

Two phases, sharply separated:

```
COMPILE TIME (per stored query, off the hot path)
  query DSL text
    → parse → AST
    → semantic normalization (same normalizer as titles)
    → CompiledQuery { required, forbidden, any_of_groups, entities }
    → signature-cover optimizer  → candidate_signatures (lossless cover)
    → cost classification (A/B/C/D)
    → append to segment build (postings + SoA exact-match plan)

MATCH TIME (per incoming title, the hot path — allocation-free)
  raw title bytes
    → normalize (in-place, into a reusable scratch buffer)
    → extract dense feature IDs + entity slots  → TitleFeatureSet
    → enumerate title signatures (small, bounded)
    → probe candidate index → union of candidate SegmentLocalQueryIds
    → exact integer verification (mask + sorted-slice checks)
    → map survivors to GlobalLogicalQueryId  → emit matches
```

The compile phase is allowed to be expensive and clever. The match phase is dumb, branch-predictable
integer work. **No parsing, no strings, no regex, no allocation, no generic AST interpretation on the
hot path** — those are all pushed into compile time.

The DSL, normalizer, and feature dictionary are detailed in [`normalization.md`](normalization.md);
the optimizer, candidate index, exact matcher, cost classes, family factoring, and explain in
[`matching.md`](matching.md).

---

## 2. The correctness contract (the thing that must never break)

> **Lossless signature cover:** if a title `T` *could* satisfy query `Q`'s positive semantics, then
> `T` must generate at least one signature that retrieves `Q` from the candidate index.

Formally, let `sig_cover(Q)` be the set of signatures the compiler indexed `Q` under, and
`sigs(T)` the signatures a title generates. We require:

```
positively_matches(T, Q)  ⇒  sig_cover(Q) ∩ sigs(T) ≠ ∅
```

This is the generalization of Lucene Monitor's decomposition invariant. It guarantees **zero false
negatives**. The *converse* is deliberately not required: a title may retrieve queries it does not
actually match (false-positive **candidates**), which the exact matcher then rejects. Candidate false
positives cost CPU; they never cost correctness.

**Forbidden (MUST_NOT) features are never used for gating** — gating on a negative would let an absent
feature drop a real match. Negatives are checked *only* in exact verification. This is the most common
source of correctness bugs in naive percolators and we forbid it structurally (the signature optimizer
literally cannot see forbidden features).

**Construction proof obligation.** Every signature is built only from a query's *required* features
and *required any-of groups* (see [`matching.md`](matching.md) §1). Because each signature is a subset
of features that must be present for the query to match, any matching title contains all of them, hence
generates that signature. Disjunctions are covered by emitting one signature family per branch
(mirroring the "extract from every OR branch" rule). The PoC includes a randomized **differential
oracle** test (brute-force matcher vs. engine) asserting zero false negatives across millions of
(title, query) pairs — this is how we *verify*, not just *assert*, the contract.

---

## 3. How this design answers the spec's mandatory questions

- **Avoid evaluating too many queries/title?** Semantic signatures (more selective than terms) +
  union-of-tiny-postings candidate retrieval + family subtree pruning + broad-query quarantine.
- **Guarantee no false negatives?** The lossless-cover contract (signatures built only from required
  features; every OR branch covered; negatives never gate), verified by a differential oracle test.
- **Handle 100M queries?** Immutable mmap segments + sharding by entity; only `u32` local IDs on the
  hot path; compact SoA + adaptive postings.
- **Frequent updates?** Hot delta + tombstones + atomic epoch swap; background "improving" compaction;
  no in-place postings mutation, no full rebuild.
- **Isolate broad queries?** Compile-time cost classes A/B/C/D; class C → broad lane (batch/columnar/
  subscriptions); class D → reject with rewrite suggestions.
- **Minimize memory bandwidth?** Common-mask gate rejects most candidates in two `u64` reads; SoA
  layout; segment-local `u32` IDs; resolve to `u64` global IDs only on confirmed match.
- **Degrade under skew?** Hot-signature splitting, broad lane, cost-class metering, and
  candidate-survival telemetry feeding compaction keep p99 bounded.
- **vs generic percolator?** Semantic (not term) gating, integer (not Scorer) verification, family
  factoring, and broad-query quarantine — each removes a class of work generic percolators still pay.

---

## 4. Module map (PoC ↔ design)

| Design topic | PoC module |
|---|---|
| DSL ([normalization](normalization.md)) | `src/dsl.rs` (parser + AST) |
| Normalizer ([normalization](normalization.md)) | `src/normalize.rs` |
| Feature dictionary ([normalization](normalization.md)) | `src/dict.rs` |
| Signature optimizer ([matching](matching.md)) | `src/compile.rs` |
| Candidate index ([matching](matching.md)) | `src/index.rs` |
| Exact matcher ([matching](matching.md)) | `src/exact.rs` |
| Broad lane / cost class ([matching](matching.md)) | `src/compile.rs` (`CostClass`) + `src/index.rs` |
| Family factoring ([matching](matching.md)) | `src/family.rs` |
| Segments / delta / tombstones ([ingestion](ingestion-and-updates.md)) | `src/segment.rs` |
| Explain ([matching](matching.md)) | `src/explain.rs` |
| data generator | `src/gen.rs` |
| corpus feature learner | `src/bin/learn.rs` |
| title introspection | `src/bin/norm.rs` |
| benchmarks / oracle | `src/bin/{bench,segbench}.rs`, `tests/oracle.rs` |
