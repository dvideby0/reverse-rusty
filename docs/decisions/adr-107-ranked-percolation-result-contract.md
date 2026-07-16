# ADR-107: Ranked percolation result contract and collector boundary

- **Status:** Accepted (Increment 0–1)
- **Date:** 2026-07-16

## Context

Reverse Rusty's lossless signature cover defines the conceptual exact Boolean match set, but the
serving API has historically used one delivery contract: materialize every matching logical ID,
optionally score the complete vector, then apply `from`/`size`. This is exact, but `size` bounds only
the final JSON page. It does not bound shard reply size, coordinator memory, or result collection.

The product needs three explicitly different projections of the same exact match set. They must not
be confused with query visibility: whether class-C broad queries are visible is independent from how
many matches a caller asks to receive.

## Decision

### Result and visibility contracts

The reserved v2 API uses:

- `query_scope = standard | with_broad`; `include_broad` remains a compatibility alias only on the
  existing endpoints;
- `result_mode = top_k | all | terminated`;
- `complete=true` only when all required work for the selected mode and scope completed. For
  `top_k`, it means the exact best K was computed, not that every true match appears in the page;
- `hits.total = {value, relation}`, where `eq` is exact and `gte` is a thresholded lower bound;
- exact `top_k` and `all` requests fail closed on timeout, required-shard failure, or generation
  disagreement. Partial results are opt-in and always carry `complete=false`;
- `terminated` is explicitly approximate and can never claim the zero-false-negative contract.

Reserved v2 defaults are `top_k`, `size=100`, `max_top_k=10_000`,
`track_total_hits_up_to=10_000`, `allow_partial_results=false`, `query_scope=standard`, and a
server-configured timeout that arms cooperative compute cancellation.

`/v2/_search` is reserved but **not registered** in Increment 0–1. Advertising a bounded v2 surface
before typed ranking and bounded collection are connected would falsely imply a scale boundary. The
existing `/_search` and `/_mpercolate` request/response bytes remain unchanged.

### Collection seam

Exact verification emits through a monomorphized collector:

```text
candidate retrieval -> exact body verdict -> member alive/tag checks -> collector.on_match(id)
```

- `AllCollector` preserves the current sorted, deduplicated `Vec<u64>` API.
- `CountCollector` retains no hits and tracks unique totals only through a declared threshold.
- `TopKCollector` keeps K winners under `(score desc, logical_id asc)` and is initially oracle-only;
  it is not connected to REST, cluster RPCs, source lookup, or persistence.

Collector memory is reserved before matching. A top-K collector stores only the K heap members and
their IDs. Because the test scorer is deterministic per logical ID and the competitive threshold can
only improve, a duplicate no longer in the heap cannot become competitive later. Total-hit tracking
uses a separate set capped at `threshold + 1`: at or below the threshold the total is exact; once the
extra distinct ID is observed, the result is `{value: threshold, relation: gte}`.

Canonical semantic-body sharing remains member-correct: one body is verified, then every alive,
tag-eligible member is independently offered to the collector. Cancellation clears all collector
state and never exposes a partial exact result.

### Delivery telemetry and baseline

`MatchStats` gains non-serialized `logical_emissions` and `duplicate_emissions`. They count rows after
exact/member checks and the logical duplicates removed locally or during coordinator fan-in. Existing
profile DTOs deliberately omit them.

The deterministic `rankbench` harness records ordinary, broad-heavy, canonical-body-duplicate, and
multi-shard duplicate-placement workloads. It reports match percentiles, emissions/deduplication,
rank time, encoded result bytes, fanout, bounded-K projections, and stable semantic checksums.
Synthetic corpora are the acceptance basis for this increment; timings are informative, while result
identity and memory bounds are hard assertions.

## Correctness

Collectors run only after exact positive/negative verification and request tag filtering. They do
not affect signature construction, candidate retrieval, cost classification, visibility, or the
never-gate-on-MUST_NOT invariant. `AllCollector` is the compatibility oracle: every existing result
and pre-existing `MatchStats` field must remain identical.

The top-K oracle compares bounded collection with collect-all, logical-ID deduplication, total-order
sorting, and truncation across duplicates, ties, signed scores, K boundaries, and total thresholds.

## Deferred

Typed numeric rank persistence, local v2 serving, deterministic distributed emission ownership,
query-then-fetch, distributed title batching, PIT/cursors, exhaustive jobs/streams, and exact
competitive pruning remain separate ADR-sized increments. No persistence, WAL, manifest, protobuf,
or compatibility response format changes in Increment 0–1.
