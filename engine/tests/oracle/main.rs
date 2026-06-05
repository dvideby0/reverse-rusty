//! Differential oracle: the CONTRACT verification.
//!
//! For a synthetic dataset, compute ground truth with a brute-force matcher
//! (check every query's extracted features against every title) and compare to
//! the engine's output. We assert:
//!   * ZERO false negatives  (every true match is returned)  <-- the hard requirement
//!   * ZERO false positives  (the exact matcher is exact)
//!
//! The brute-force side uses its own Dict/Normalizer *instances* and independently
//! reimplements candidate retrieval + exact verification — so an index / retrieval /
//! verify bug can't hide here. It does NOT independently verify the FRONT END: it calls
//! the engine's own `dsl::parse`, `compile::extract`, and `Normalizer` (and, except in
//! `zero_false_negatives_with_populated_vocab`, the empty `default_vocab`). The parser,
//! extractor, and normalization-model semantics are pinned instead by the spec-authored
//! golden tests in `src/{dsl,normalize,compile}.rs` (`mod golden`). See DECISIONS.md ADR-050.

mod harness;

mod alias;
mod batch;
mod core;
mod corpus;
mod equivalence;
mod filtered;
mod reanchor;
mod segments;
mod vocab;
