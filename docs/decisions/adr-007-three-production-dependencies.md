# ADR-007: Three production dependencies (daachorse, roaring, rayon)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Reverse Rusty started std-only with hand-rolled alternatives (token-trie for alias
  matching, Vec-only postings, single-threaded matching).
- **Decision:** Replace each hand-rolled component with the production-grade crate once the
  design was validated: daachorse v3 for O(n) multiword alias matching, roaring v0.10
  for compressed bitmaps on large postings, rayon v1 for data-parallel matching.
- **Consequence:** Identical semantics with better performance characteristics. daachorse
  gives O(n) scan time regardless of vocab size. Roaring compresses large postings (>256
  entries) and enables future SIMD intersection. Rayon delivers ~3.8× speedup on 4 threads.
  Zero other external dependencies.

