# ADR-002: Integer-only exact verification (no strings on the hot path)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Most percolators re-run a scorer or mini-query-engine on each candidate. This
  pulls in string comparison, regex, allocation, and virtual dispatch — expensive per
  candidate.
- **Decision:** Push all parsing, normalization, and AST interpretation into compile time.
  The match-time exact check uses only `u64` mask operations and sorted `u32` slice
  galloping. No strings, regex, allocation, or generic AST interpretation on the hot path.
- **Consequence:** ~710k titles/sec/core. The common-mask gate (two `u64` reads) rejects
  most candidates before any further memory traffic. Trade-off: any change to query semantics
  requires recompilation of the affected query.

