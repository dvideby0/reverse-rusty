# ADR-115: Defer exact competitive pruning — profiling gate not met

- **Status:** Declined for now
- **Date:** 2026-07-22

## Context

The ranked-percolation plan made exact competitive pruning optional. It was to begin only if
profiling showed that exact verification, rather than result delivery, remained the dominant
cost after bounded top-K, distributed ownership, title batching, PIT/cursors, and exhaustive
streaming had shipped.

Competitive pruning here is not a local heap tweak. A useful implementation needs sound
per-query score upper bounds, high-score-first candidate visitation across every segment and
cost lane, an increasing minimum competitive score, threshold-aware total semantics, distributed
equivalence, observe-only telemetry, and a new randomized exhaustive oracle. Any unsound bound or
visitation shortcut could create a false negative in the ranked result.

## Evidence

The canonical release-profile command was rerun on 2026-07-22:

```text
cargo run --release --bin rankbench -- 20000 500 8 275775489
```

All four semantic checksum pairs remained byte-identical to the ADR-107 baseline. On the
synthetic broad-heavy workload:

- K=10 local Boolean match plus rank collection: **1.989 ms** for 500 titles and 18,313 rank
  evaluations;
- collect-all compatibility ranking alone: **0.793 ms**;
- in-process cluster collect plus merge: **26.857 ms**;
- winner source fetch: **0.239 ms**;
- a 256-title K=10 batch: **0.735 ms local / 0.745 ms cluster**.

The local remainder after subtracting compatibility ranking is only about 1.2 ms and combines
candidate retrieval, integer verification, logical-id handling, total tracking, and collector
work. It does not establish verification as the dominant component. At the distributed request
level, coordinator/shard orchestration is more than an order of magnitude larger than the whole
local kernel in this capture. The real-corpus distribution requested by the source plan is not
available.

## Decision

Do not add score-bound storage, score-ordered candidate indexes, or competitive early
termination now. Increment 8 exits through its explicit “remove or disable when the complexity
does not pay” gate: the optimization remains absent, so exact top-K, total relations, persistence
formats, and the hot path are unchanged.

Reconsider only with a reproducible workload where:

1. phase-level profiling attributes a material majority of ranked request time to exact
   verification;
2. K is materially smaller than the verified-match population;
3. totals may honestly downgrade to `gte`; and
4. a proposed bound has a compact integer-only representation and demonstrates a meaningful
   end-to-end win after its storage, visitation, and proof costs.

Any future proposal must start observe-only and pass the source plan's exhaustive differential
matrix over signed boosts, ties, segment boundaries, updates, every cost class, and distributed
merge before enforcement.

## Consequences

- The ranked-delivery program is complete without speculative pruning.
- Exact behavior stays simple: every retrieved candidate required by the selected visibility
  scope is verified, and collectors alone bound delivery memory.
- The canonical rankbench capture remains the re-entry baseline. Real-corpus evidence can reopen
  this decision without changing the correctness contract.
