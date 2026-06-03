# ADR-019: Query-family factoring evaluated and declined

> [Back to the decisions index](../DECISIONS.md) · **Status:** Declined


- **Context:** The design carried an explicit **query-family / shared-prefix DAG** as a roadmap item
  (formerly `matching.md` §5, listed in `STATUS.md` as "the next optimization to push selective
  candidates below ~54"). The idea: near-duplicate product queries share a required-feature prefix
  (`1994 upper_deck series0001 michael_jordan` + per-leaf card term / grade / negatives); store the
  shared prefix once and, at match time, check it once — if the title lacks a shared feature, prune the
  whole subtree in one test instead of rejecting each leaf. This ADR records the decision **not** to
  build it, so the rationale is durable and the item is not silently re-added later.
- **Research:** The academic basis is **PRETTI** (prefix-tree set-containment join); its successor
  **LIMIT+** exists *specifically because PRETTI's full prefix tree grows too large*, and bounds the
  depth with a cost model. The same "evaluate a shared predicate once for many rules/subscriptions"
  pattern appears in **RETE** rule engines (alpha-node sharing) and the **Fabret et al.** content-based
  pub/sub *counting algorithm* (examine common predicates first, recursively eliminate groups that
  cannot match). A spectrum was considered: **L1** a posting-prefix gate (one shared-prefix mask+tail
  per anchor posting; gate the whole posting before iterating), **L2** explicit family grouping (a
  two-level prefix→leaf-residual store + family-level dedup), **L3** a full multi-level DAG. The
  lossless-cover contract is preserved by construction in every variant — the shared prefix is a subset
  of each leaf's required features, and forbidden features are never shared or gated — so it would be a
  pure performance optimization (results must be *bit-identical* to today, the strongest possible test).
- **Decision:** **Do not build it.** Keep the *implicit* clustering the candidate index already
  provides: near-duplicates share signature anchors, so a single failed anchor probe drops the whole
  cluster's candidates. Reasons: **(1) it optimizes a non-bottleneck** — the selective path is already
  ~255× the spec target with a flat ~54 candidates/title and a common-mask gate (two `u64` ops,
  ADR-002) that rejects most candidates almost for free; the measured bottlenecks are the **broad lane**
  and **memory bandwidth** (`performance/results.md` §9), which family factoring barely touches (broad
  queries are short and don't share prefixes). **(2) The cost is concentrated in the wrong place** — not
  the algorithm but the mmap `.seg` format (version bump + back-compat read path), the `compact_from`
  rebuild, and a two-level SoA: the bug-prone surfaces, for a speculative and probably-modest win on a
  number that is already excellent. **(3) The literature already walked back** from the unbounded tree
  (LIMIT+). The synthetic generator's clean `family_size=8` clusters also flatter the feature versus
  messy real titles.
- **Consequence:** No `src/family.rs`; `matching.md` §5 is removed; the "four moves vs generic
  percolators" thesis becomes **three** (semantic signatures, integer verification, broad-query
  quarantine). The roadmap redirects that energy to the actual bottlenecks — **broad-lane batch/columnar
  evaluation** and **dictionary interning / tighter SoA** (`STATUS.md`). The decision is **reversible**:
  implicit anchor-sharing is unchanged, so nothing precludes a future *bounded* L1 posting-prefix gate
  if real-data measurement ever justifies it — the entry point would be a measurement spike + L1, gated
  by an on/off differential and the existing oracle, never the full DAG.
- **See also:** ADR-002 (integer verification / common-mask gate — why the verifier is already cheap),
  ADR-003 (broad-lane quarantine — the actual #1 opportunity), `research/prior-art.md` §6 (PRETTI /
  LIMIT+ / FreshJoin), `performance/results.md` §9 (bottleneck analysis).

