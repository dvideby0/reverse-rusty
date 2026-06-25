//! Independent correctness oracle (ADR-087) — the FRONT-END differential.
//!
//! Unlike `tests/oracle/`, the ground-truth side here reuses NONE of the engine's front end. It is
//! the `reverse-rusty-ref-matcher` crate, a from-scratch reimplementation of the DSL parser, the
//! normalizer, the extractor, and the match predicate (built from the spec, depending on nothing in
//! `reverse-rusty`). Diffing the real engine against it catches a parser / normalizer / extractor
//! bug that the in-tree oracle — which calls the engine's own `dsl::parse` / `compile::extract` /
//! `Normalizer` — structurally cannot see (the ADR-050 shared-front-end blind spot).
//!
//! We assert, exactly as the in-tree oracle does:
//!   * ZERO false negatives (every reference match is returned by the engine) — the hard requirement
//!   * ZERO false positives (the engine matches nothing the reference does not)

mod harness;

mod aliases;
mod core;
mod corpus;
mod gotcha;
