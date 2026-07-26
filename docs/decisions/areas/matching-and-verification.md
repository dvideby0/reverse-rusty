# Matching & verification decisions

> [Architecture decision hub](../../DECISIONS.md)

Candidate retrieval, lossless signature cover, exact verification, broad-query handling, and query-cost controls.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [001](../adr-001-semantic-signatures.md) | Semantic signatures over term-level gating | Gate candidates on 2–3 *semantic* feature combinations from a domain-aware normalizer, not raw terms → flat ~54 candidates/title at any corpus size. | Accepted |
| [002](../adr-002-integer-exact-verification.md) | Integer-only exact verification | Push all parsing/AST work to compile time; the match hot path is pure `u64`-mask + sorted-`u32` work — no strings/regex/alloc. | Accepted |
| [003](../adr-003-broad-query-quarantine.md) | Broad-query quarantine via cost classes | Classify queries A–D at compile time; route non-selective class C to a batch lane, reject unconstrained class D — keep the selective path fast. | Accepted |
| [006](../adr-006-forbidden-features-never-gate.md) | Forbidden features never gate (structural) | MUST_NOT features are invisible to the signature optimizer and checked only in exact verification — gating on an absent feature would be a false negative. | Accepted |
| [011](../adr-011-cache-line-blocked-bloom.md) | Cache-line blocked bloom skip-filter | Per-segment anchor skip-filter is a 512-bit cache-line blocked bloom (1 memory access), chosen over binary-fuse/u64-blocked to fit the probe budget. | Accepted |
| [019](../adr-019-query-family-factoring-declined.md) | Query-family factoring evaluated and declined | Declined the shared-prefix/family DAG — it optimizes a non-bottleneck at high format/rebuild cost; implicit anchor-sharing already prunes near-duplicates. Reversible. | **Declined** |
| [025](../adr-025-query-complexity-limits.md) | Wire query-complexity limits into the parser | Thread configured max length/clauses/any-of-size into front-door parsing; acknowledged WAL/source recovery uses the durable format's structural ceilings, never a since-tightened policy or lower current default. | Accepted |
| [026](../adr-026-broad-lane-batch-evaluation.md) | Broad-lane batch / columnar evaluation | Evaluate the broad lane once per title-batch via columnar bitmap algebra (`/_mpercolate`); byte-identical to per-title, removes the broad bottleneck. | Accepted |

---

Each summary links to the canonical ADR record. Implementation status belongs in
[STATUS.md](../../STATUS.md); documentation placement rules belong in
[the documentation hub](../../README.md).
