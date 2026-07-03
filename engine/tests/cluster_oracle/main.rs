//! Multi-shard differential oracle — the CONTRACT verification for clustering.
//!
//! The acceptance gate for the in-process multi-shard core. For a synthetic
//! corpus plus hand-injected coverage queries, it asserts that the cluster
//! returns EXACTLY the single-node result set AND exactly the independent
//! brute-force oracle's set, across shard counts {1, 3, 8, 16} and broad on/off:
//!   * ZERO false negatives  (every true match is returned)  <-- the hard requirement
//!   * ZERO false positives  (per-shard exact verify is exact; union dedups)
//!
//! The brute-force matcher uses its own independent Dict/Normalizer so it cannot
//! share a bug with the engine or the cluster. The generated corpus is class A
//! (rare-anchored families) + class C (broad); the generator never emits any-of
//! or all-hot-required queries, so we inject those to exercise class-B any-of
//! (multi-shard placement) and class-B arity-2 (the replicated lane), plus
//! multi-entity titles to exercise multi-shard fan-out.

mod harness;

mod class_d;
mod differential;
mod dynamic_vocab;
mod filtered;
mod hot;
mod placement;
mod ranking;
mod replication;
mod resize;
mod vocab_learning;
mod vocab_reopen;
