# ADR-123: Bounded in-segment cooperative cancellation

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** ADR-099 checks an armed request at entry, title, segment, and columnar-block
  boundaries. That stops abandoned work, but one dense posting or canonical-body group can occupy
  an entire segment. The deterministic large-segment benchmark (300k shared-body rows plus 300k
  distinct rows under one positive anchor) measured a 100 µs budget taking **3.3735 ms** to stop:
  **3.2735 ms overshoot**, essentially the whole 3.537292 ms segment traversal. Results were still
  correctly discarded; the gap was latency/QoS, not result correctness.
- **Research.** Lucene exposes a `QueryTimeout` and its `ExitableDirectoryReader` checks it
  [periodically](https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/index/ExitableDirectoryReader.html),
  rather than reading the clock for every visited item. The useful pattern is a cheap fixed work
  counter around unbounded iterators plus an occasional expensive time check. A clock read on every
  Reverse Rusty candidate was rejected because the ordinary selective path is intentionally tiny and
  the armed path should still preserve its budget.
- **Decision.** Extend the existing monomorphized deadline seam with `DeadlineCheck::ARMED` and one
  request-local `DeadlinePoll`. Armed matching samples `Instant::now()` after every **256 work
  units**; a work unit is an anchor/probe, posting entry, unique candidate, canonical-body member,
  columnar bitmap emission, or newest-live rank-metadata row. The sampler is threaded through both
  in-memory and mmap scalar probes, through the broad/hot columnar reach, candidate, body-group, and
  emission loops, and into the bounded-ranking scorer callback. The rank walk matters because one
  logical id can retain arbitrarily many newer tombstoned physical versions. Existing title/segment
  boundary checks remain and reset the interval. `NoDeadline::ARMED == false` is a compile-time
  constant, so LLVM removes the counter mutation, branch, clock read, and error arms from every
  ordinary matcher; no runtime `Option` or virtual dispatch enters the hot path.

  Parser and exact-store ceilings already bound one query's integer verification program. The
  sampler therefore targets the segment-owned collections whose cardinality grows with corpus or
  duplicate concentration. Cancellation stays cooperative rather than preemptive: the bound is 256
  such operations plus the currently executing bounded exact operation and one clock read.
- **Failure contract.** An in-loop expiry propagates through the same typed `MatchCancelled` path as
  ADR-099. Scalar collectors call `abort()` only after restoring reusable title buffers; batch
  collectors clear every title slot. No partial id vector, partially-filled multi-response, or
  partial stats result escapes.
- **Evidence.** On the same Apple M4 Max release build and identical corpus, the post-change run
  stopped in **100.5 µs**, only **0.5 µs overshoot** (about **6,500× less overshoot**), while the
  unarmed traversal remained the same millisecond-scale workload. Reproduce with:

  ```text
  cargo test --release --test stress \
    cancellation::large_segment_overshoot_benchmark -- --ignored --nocapture
  ```

  The durable capture is in `docs/performance/benchmark-results.txt`.
- **Testing.** Counter-driven unit tests avoid wall-clock races: one pins the exact 256-work cadence
  and boundary reset; one cancels inside a 4,096-member scalar body group, proves traversal stopped
  early, and proves the output was cleared; one does the same through the columnar batch kernel;
  scalar and columnar ranked regressions cancel inside a 2,048-row newest-live metadata walk and
  prove no winner escapes; and one pins that an already-recorded collector/sink failure wins over a
  coincident deadline poll. ADR-099's armed-unexpired equivalence, expired-at-entry, parallel/batch
  all-or-nothing, and self-calibrating stress tests remain unchanged. The ignored benchmark asserts
  cancellation is materially faster than full traversal.
- **Consequences.** No public API, REST shape, result semantics, persistence format, placement
  rule, or unarmed clock behavior changes. Armed requests perform one cheap decrement/branch per
  work unit and one clock read per 256 units. The interval is deliberately a compile-time constant:
  making it dynamic would add configuration and inhibit the structural zero-cost unarmed proof for
  no demonstrated operational benefit.
- **See also:** ADR-026 (columnar broad evaluation), ADR-099 (typed cooperative cancellation and
  bounded concurrency), ADR-106 (canonical-body groups).
