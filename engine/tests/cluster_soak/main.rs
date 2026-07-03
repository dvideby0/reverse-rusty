//! Cluster scale soak (ADR-104): the ≥20M-query multi-shard proof — the scale
//! half of Distributed-v1 criterion 12 (ADR-065).
//!
//! One `#[ignore]`d test, run explicitly by name. It is wired into NO gate and
//! NO CI workflow — a deliberate one-off acceptance run whose captured numbers
//! live in `docs/performance/` (the gate only ever *compiles* this target):
//!
//!   cargo test --release --test cluster_soak -- --ignored --nocapture
//!
//! Scale knobs (defaults = the canonical 20M / 50k-title / K=8 run; drop them
//! for a seconds-long harness smoke):
//!   RR_CLUSTER_SOAK_QUERIES / RR_CLUSTER_SOAK_TITLES / RR_CLUSTER_SOAK_SHARDS
//!   RR_CLUSTER_SOAK_THETA (per-shard hot-anchor θ, ADR-105; default 0 = off,
//!     which reproduces the canonical ADR-104 run byte-identically)
//!   RR_CLUSTER_SOAK_DIR (base dir for the durable cluster; default temp_dir())
//!
//! What it proves — and what it deliberately does not (no brute force at 20M,
//! no K-sweep, no gRPC leg) — is recorded in ADR-104.

mod harness;
mod soak;
