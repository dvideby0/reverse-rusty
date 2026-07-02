//! Stress tests: concurrent-style read/write/delete workloads.
//!
//! These tests simulate real-world mixed workloads — inserts, deletes, updates,
//! and searches happening in staged phases, single-threaded and multi-threaded.
//! They are NOT part of the default test suite (run with `cargo test --test stress`).
//!
//! Each test logs engine events + metrics so you can watch the mechanics:
//!   cargo test --release --test stress -- --nocapture
//!
//! The tests verify:
//!   * Zero false negatives under churn (oracle comparison)
//!   * Metrics consistency (counts, segments, tombstones)
//!   * Event emission (flush, ingest, compaction triggers)
//!   * Correct delete/update visibility
//!   * Parallel vs sequential agreement under mutation

mod harness;

mod cancellation;
mod doc_store;
mod interleaved;
mod oracle_lifecycle;
mod parallel;
mod single_thread;
mod soak;
mod tombstone_compaction;
