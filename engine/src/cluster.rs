//! Multi-shard core — clustering build-path steps 1–2 plus step 1's gRPC transport.
//!
//! Design: docs/design/clustering-and-scaling.md (§3 sharding model, §7 broad
//! queries, §10 build path). The dependency-free heart — a consistent-hash ring +
//! content-routing coordinator over K shards, validated by a multi-shard differential
//! oracle (`tests/cluster_oracle.rs`) — runs in ONE process. Behind the off-by-default
//! `distributed` feature, the [`Shard`](shard::Shard) seam also has a gRPC
//! implementation: [`ShardServer`] serves one shard, and a [`RemoteShard`] client lets
//! the coordinator drive a shard across the network ([`ClusterEngine::connect_remote`]).
//! The coordinator's mutations are made durable by an externalized, ordered
//! [`ClusterLog`](clog::ClusterLog) (build-path step 3a, ADR-031), so a cluster built
//! with a `data_dir` is rebuildable from its log alone ([`ClusterEngine::open`]). Raft
//! quorum replication of that log, object-storage segments, autoscaling, and auto-split
//! remain later steps.
//!
//! Correctness rests on a single decision: the coordinator owns ONE authoritative
//! [`Dict`](crate::dict::Dict), built over the whole corpus and then frozen and
//! shared read-only into every shard (the same `Arc<Dict>` in-process; a byte-identical
//! copy per node when remote). With one feature space, `FeatureId`s, `sig_key`s, and
//! hotness are globally consistent, so a shard's internal indexing matches the
//! coordinator's placement decision by construction — and the cross-shard cover stays
//! lossless (zero false negatives). See [`coordinator`] for the placement/routing rules
//! and the no-false-negative argument.

mod allocator;
mod autoscale;
mod clog;
mod control;
mod coordinator;
mod http_status;
mod replica;
mod ring;
mod shard;
mod translog;
mod transport_metrics;

#[cfg(feature = "distributed")]
mod control_raft;
#[cfg(feature = "distributed")]
mod control_server;
#[cfg(feature = "distributed")]
mod control_store;
#[cfg(feature = "distributed")]
mod control_wire;
#[cfg(feature = "distributed")]
mod handoff;
#[cfg(feature = "distributed")]
mod health;
#[cfg(feature = "distributed")]
mod node_metrics;
#[cfg(feature = "distributed")]
mod proto;
#[cfg(feature = "distributed")]
mod ranked_wire;
#[cfg(feature = "distributed")]
mod remote;
#[cfg(feature = "distributed")]
mod remote_control;
#[cfg(feature = "distributed")]
mod security;
#[cfg(feature = "distributed")]
mod server;

pub use autoscale::{evaluate, AutoscaleConfig, AutoscaleDecision, LoadSnapshot, ScalingAction};
pub use control::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, InMemoryControlPlane,
    NodeDescriptor, NodeId, NodeRole, ShardAssignment, StateVersion,
};
pub use coordinator::{
    recommended_shard_count, resolve_topology, route_topology, seed_position_preserving,
    AddOutcome, ClusterConfig, ClusterEngine, ClusterRankedError, ClusterRankedHit,
    ClusterRankedMatch, ResyncReport, ShardEndpoints,
};
pub use ring::{HashRing, DEFAULT_VNODES};
pub use shard::ShardError;
pub use transport_metrics::{MethodStat, TransportMetrics, TransportMetricsSnapshot};

#[cfg(feature = "distributed")]
pub use control_raft::{
    durable_single_node, in_process_cluster, start_grpc_node, start_grpc_node_with_security,
    RaftControlPlane, TypeConfig,
};
#[cfg(feature = "distributed")]
pub use control_server::{ControlMetricsSource, ControlServer};
#[cfg(feature = "distributed")]
pub use coordinator::{
    GcReport, OrphanSlot, ReassignOutcome, RebalanceMoveReport, ReconcileConfig, ReconcileReport,
    ShardGroup,
};
#[cfg(feature = "distributed")]
pub use node_metrics::{serve_metrics, MetricsHandle};
#[cfg(feature = "distributed")]
pub use remote::RemoteShard;
#[cfg(feature = "distributed")]
pub use remote_control::RemoteControlPlane;
#[cfg(feature = "distributed")]
pub use security::{
    resolve_mesh_token, ClientSecurity, MeshTransport, ServerSecurity, TlsClientConfig,
    TlsServerIdentity,
};
#[cfg(feature = "distributed")]
pub use server::{
    ShardMetricsSource, ShardServer, DEFAULT_MAX_GRPC_RESULT_BYTES, MAX_GRPC_RESULT_BYTES,
};
