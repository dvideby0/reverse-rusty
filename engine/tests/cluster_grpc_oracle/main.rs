//! gRPC differential oracle — the CONTRACT verification for the distributed shard
//! transport (build behind `--features distributed`).
//!
//! Stands up K real `ShardServer`s on localhost, assembles a `ClusterEngine` whose
//! shards are gRPC `RemoteShard`s, loads the corpus over the wire (IngestExtracted),
//! and asserts the gRPC-backed cluster returns EXACTLY the independent brute-force
//! oracle's set AND the single-node engine's set — broad on and off. This proves the
//! seam + transport + the sync→async (`block_on`) bridge preserve the zero
//! false-negative contract across a process boundary (here, same-process sockets; the
//! servers share the SAME frozen `Arc<Dict>`/`Arc<Normalizer>`, which is how the
//! cross-process dict-identity requirement is satisfied in-test — see ADR-029).
//!
//! Whole file is gated; the default `cargo test` skips it.
//!
//! The original single-file test was split into a directory of cohesive groups (a pure
//! mechanical move): the shared harness lives in `harness.rs`, and the 13 `#[test]`
//! functions are grouped by concern across the submodules below. All groups reach the
//! harness via `use crate::harness::*;`.
#![cfg(feature = "distributed")]

mod harness;

mod block_on;
mod class_d;
mod colocation;
mod core;
mod dict_shipping;
mod filtered;
mod fingerprint;
mod gc;
mod handoff;
mod health;
mod legacy_layout;
mod parallel;
mod partial_apply;
mod reassign;
mod rebalance;
mod reconcile;
mod reconcile_replicated;
mod recovery;
mod relocation;
mod replication;
mod replication_colocation;
mod routing;
mod security;
mod transport;
