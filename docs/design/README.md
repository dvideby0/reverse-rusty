# Design — a domain-aware reverse product-query matcher

*Architecture overview, the correctness contract, and the module map. The per-component detail lives
in the topic files linked below. Read [`../research/prior-art.md`](../research/prior-art.md) first for
the borrowed ideas. Reverse Rusty (in `engine/`) implements the single-node engine, the in-process
cluster, and the experimental distributed layers described here. These topic files own current
behavior; [`../CHANGELOG.md`](../CHANGELOG.md) records what shipped, and
[`../roadmap.md`](../roadmap.md) owns unfinished work.*

## Topic files

- [`normalization.md`](normalization.md) — query DSL, the shared query/title normalizer, the feature
  dictionary, and the normalizer hardening from real eBay data.
- [`matching.md`](matching.md) — the signature-cover optimizer, the candidate index, the integer-only
  exact matcher, two-axis placement classes (A/B/C/D/H), and explain tooling.
- [`ingestion-and-updates.md`](ingestion-and-updates.md) — immutable segments + hot delta + tombstones,
  the LSM write path, deltas-with-merge, bulk-ingest vs rebuild rules, vocabulary rebuilds, and format
  fences.
- [`clustering-and-scaling.md`](clustering-and-scaling.md) — the built in-process cluster and the
  experimental shared-nothing distributed stack: entity-anchor routing, local segments, mutation
  tails, replication, and a quorum control plane for topology (no object store; ADR-033). The same
  page distinguishes explicit operator controls from unfinished automation.

---

## 1. Mental model

Two phases, sharply separated:

```
COMPILE TIME (per stored query, off the hot path)
  query DSL text
    → parse → AST
    → semantic normalization (same normalizer as titles)
    → CompiledQuery { required, forbidden, any_of_groups, required_phrases, ... }
    → signature-cover optimizer  → candidate_signatures (lossless cover)
    → visibility + scheduling classification (A/B/C/D/H)
    → append to segment build (postings + SoA exact-match plan)

MATCH TIME (per incoming title, the hot path — allocation-free)
  raw title bytes
    → normalize (in-place, into a reusable scratch buffer)
    → extract sorted/deduplicated feature IDs into reusable scratch
    → enumerate the title's arity-1/arity-2 signature keys
    → probe candidate index → union of candidate SegmentLocalQueryIds
    → exact integer verification (mask + sorted-slice/phrase checks)
    → map survivors to GlobalLogicalQueryId  → emit matches
```

The compile phase is allowed to be expensive and clever. The match phase is dumb, branch-predictable
integer work. **No parsing, no strings, no regex, no allocation, no generic AST interpretation on the
hot path** — those are all pushed into compile time.

The DSL, normalizer, and feature dictionary are detailed in [`normalization.md`](normalization.md);
the optimizer, candidate index, exact matcher, cost classes, and explain in
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
and *required any-of groups* (see [`matching.md`](matching.md) §1). An unconditional required anchor is
present in every match. For a disjunction, the compiler emits a cover family containing at least one
feature that is required by each semantic branch; therefore whichever branch a matching title
satisfies, it generates that branch's signature. A multi-token branch keeps its full conjunction for
exact verification—the retrieval feature is only a necessary proxy (ADR-119). Reverse Rusty includes
a randomized **differential oracle** test (brute-force matcher vs. engine) asserting zero false
negatives across millions of (title, query) pairs — this is how we *verify*, not just *assert*, the
contract.

---

## 3. How this design answers the spec's mandatory questions

- **Avoid evaluating too many queries/title?** Semantic signatures (more selective than terms) +
  union-of-tiny-postings candidate retrieval (near-duplicates share signature anchors, so a single
  failed anchor probe drops the whole cluster's candidates) + broad-query quarantine.
- **Guarantee no false negatives?** The lossless-cover contract (signatures built only from required
  features; every OR branch covered; negatives never gate), verified by a differential oracle test.
- **Scale toward 100M queries?** Immutable mmap segments + content-routed sharding; only `u32` local
  IDs on the hot path; compact SoA + adaptive postings. The measured captures and extrapolation are
  owned by [`../performance/results.md`](../performance/results.md).
- **Frequent updates?** Mutable in-memory delta + tombstones + atomic snapshot publication;
  background flush/compaction; no in-place mutation of immutable-segment postings.
- **Isolate broad and hot work?** Compile-time classes A/B/C/D/H with independent visibility and
  scheduling: C and accepted D live in the opt-in broad lane, while H stays default-visible and runs
  in the always-probed hot tier.
- **Minimize memory bandwidth?** Common-mask gate rejects most candidates in two `u64` reads; SoA
  layout; segment-local `u32` IDs; resolve to `u64` global IDs only on confirmed match.
- **Observe and control skew?** The broad/hot lanes have separate batch evaluation and cost metrics;
  operators can tune the hot threshold and placement. Automatic skew-driven reclassification and
  split policy remain roadmap work.
- **vs generic percolator?** Semantic (not term) gating, integer (not Scorer) verification, and
  broad-query quarantine — each removes a class of work generic percolators still pay.

---

## 4. Module map (code ↔ design)

This maps design *topics* to their implementing module. The broader repository responsibility map
and task router live in [`../../AGENTS.md`](../../AGENTS.md) (`CLAUDE.md` is only a compatibility
pointer).

| Design topic | Code module |
|---|---|
| DSL ([normalization](normalization.md)) | `src/dsl.rs` (parser + AST) |
| Normalizer ([normalization](normalization.md)) | `src/normalize.rs` |
| Feature dictionary ([normalization](normalization.md)) | `src/dict.rs` |
| Signature optimizer ([matching](matching.md)) | `src/compile.rs` + `src/compile/` |
| Candidate index ([matching](matching.md)) | `src/index.rs` |
| Exact matcher ([matching](matching.md)) | `src/exact.rs` |
| Broad/hot lanes + placement classes ([matching](matching.md)) | `src/compile.rs`, `src/segment.rs` + `src/segment/` |
| Segments / delta / tombstones ([ingestion](ingestion-and-updates.md)) | `src/segment.rs` + `src/segment/` |
| Explain ([matching](matching.md)) | `src/explain.rs` |
| data generator | `src/gen.rs` |
| corpus feature learner | `src/corpus.rs`, `src/vocab/learn.rs`; `src/bin/learn.rs` is the CLI wrapper |
| title introspection | `src/bin/norm.rs` |
| benchmarks / oracles | `src/bin/{bench,clusterbench,segbench,snapbench,rankbench}.rs`, `src/bin/perfgate/`, `tests/oracle/`, `tests/independent_oracle/` |
