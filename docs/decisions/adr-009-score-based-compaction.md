# ADR-009: ClickHouse-inspired score-based compaction over RocksDB-style leveled compaction

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Compaction needs to bound the base segment count because percolation (unlike a
  KV point read) must probe **every** segment for every incoming title — read amplification
  is proportional to segment count. The initial design (ADR-004, ingestion-and-updates.md)
  specified RocksDB-style tiered L0 + leveled below. However, RocksDB's level/tier structure
  is optimized for *point reads* (stop at the first SSTable containing the key + Bloom skip).
  We can't stop early — our reads look like ClickHouse `SELECT * FROM table`, not `GET key`.
- **Decision:** Use a ClickHouse SimpleMergeSelector-inspired score-based greedy algorithm.
  Evaluate every contiguous range of ≥2 base segments with the scoring function
  `(sum_size + FIXED_COST * count) / (count - 1.9)`. Pick the lowest-scoring range and
  merge it. The `FIXED_COST` biases toward merging small segments first (cheap wins). This
  directly minimizes the time-integrated average segment count — the exact metric that drives
  our read performance. No level metadata, no L0/L1/L2 distinction. Also provide `compact_all()`
  and `compact_range(lo, hi)` for direct control.
- **Consequence:** Simpler than leveled compaction (no levels to track), directly optimizes for
  our performance objective (minimum segment count over time), and produces O(log N) merge tree
  depth with O(N log N) total work. The `compact(max_segments)` trigger is the only policy knob.
  Verified by two oracle tests (compact-all, compact-range): zero false negatives, zero false
  positives, identical match results pre- vs post-compaction.
- **See also:** [ingestion-and-updates.md](../design/ingestion-and-updates.md) §5–6

