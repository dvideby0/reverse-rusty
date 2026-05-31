//! In-process multi-shard core — clustering build-path steps 1–2.
//!
//! Design: docs/design/clustering-and-scaling.md (§3 sharding model, §7 broad
//! queries, §10 build path). Status: the FIRST, dependency-free step of the
//! clustering roadmap — a consistent-hash ring + content-routing coordinator over
//! K shards in ONE process, validated by a multi-shard differential oracle
//! (`tests/cluster_oracle.rs`). gRPC, a durable externalized log, Raft, object
//! storage, autoscaling, and auto-split are deferred to later steps.
//!
//! Correctness rests on a single decision: the coordinator owns ONE authoritative
//! [`Dict`](crate::dict::Dict), built over the whole corpus and then frozen and
//! shared read-only into every shard. With one feature space, `FeatureId`s,
//! `sig_key`s, and hotness are globally consistent, so a shard's internal indexing
//! matches the coordinator's placement decision by construction — and the
//! cross-shard cover stays lossless (zero false negatives). See
//! [`coordinator`] for the placement/routing rules and the no-false-negative
//! argument.

mod coordinator;
mod ring;
mod shard;

pub use coordinator::{AddOutcome, ClusterConfig, ClusterEngine};
pub use ring::{HashRing, DEFAULT_VNODES};
