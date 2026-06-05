//! Production-hardening test coverage: parallel matching, repeated compaction,
//! broad-lane isolation, and edge-case inputs.
//!
//! These tests close gaps identified in the production-readiness audit. Each
//! section targets a specific area that had no dedicated test:
//!   * Parallel matching correctness (par == sequential)
//!   * Repeated / interleaved compaction (multi-round stress)
//!   * Broad-lane isolation (class-C routing, include_broad flag)
//!   * Edge-case inputs (empty, oversized, Unicode, adversarial)

mod broad_lane;
mod compaction;
mod edge_cases;
mod harness;
mod parallel;
mod settings;
