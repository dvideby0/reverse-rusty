//! Cluster durability oracle — the CONTRACT verification for the externalized
//! coordinator log (ADR-031, clustering build-path step 3a).
//!
//! A `ClusterEngine` built with a `data_dir` must be rebuildable from its manifest +
//! base snapshot + mutation log alone. After a crash (drop without clean shutdown),
//! `ClusterEngine::open` must reconstruct a cluster that returns EXACTLY what the
//! pre-crash cluster returned AND exactly the independent brute-force oracle's set —
//! across shard counts {1, 3, 8}, broad on/off, live add/remove churn, and a checkpoint.
//!
//! The `Brute`, `vocab`, and `build_corpus` helpers are copied from
//! `tests/cluster_oracle.rs` (the same deliberate "shares nothing with the engine"
//! oracle), so a compile/index/exact bug cannot hide by being present on both sides.
//!
//! Split into per-concern modules — the shared harness lives in `harness.rs`; each
//! `#[test]` group reaches it via `use crate::harness::*;`.

mod harness;

mod attach;
mod backends;
mod backup;
mod checkpoint;
mod core;
mod replication;
mod resize;
mod torn_tail;
mod upsert;
mod vocab;
mod vocab_stale_sources;
