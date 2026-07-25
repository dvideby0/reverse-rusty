//! Independent correctness oracle (ADR-087) — the FRONT-END differential.
//!
//! Unlike `tests/oracle/`, the ground-truth side here reuses NONE of the engine's front end. It is
//! the `reverse-rusty-ref-matcher` crate: a from-scratch parser/normalizer plus a direct semantic
//! predicate tree built from the public grammar, depending on nothing in `reverse-rusty` and
//! containing none of the production compiler's retrieval lowering. Diffing the real engine
//! against it catches both shared-front-end bugs and independently copied proxy/flattening bugs
//! that the in-tree oracle — which calls the engine's own `dsl::parse` / `compile::extract` /
//! `Normalizer` — structurally cannot see.
//!
//! We assert, exactly as the in-tree oracle does:
//!   * ZERO false negatives (every reference match is returned by the engine) — the hard requirement
//!   * ZERO false positives (the engine matches nothing the reference does not)
//!   * ZERO candidate false negatives (candidate generation is compared for recall only)

mod harness;

mod aliases;
mod core;
mod corpus;
mod gotcha;
