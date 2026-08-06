//! Shared-nothing multi-shard core.
//!
//! Current architecture and maturity:
//! `docs/design/clustering-and-scaling.md`. The dependency-free heart is a
//! consistent-hash ring plus content-routing [`ClusterEngine`] over K shard
//! positions, validated by the differential suites in `tests/cluster_oracle/`
//! and `tests/cluster_durability_oracle/`. Behind the off-by-default
//! `distributed` feature, the [`Shard`](shard::Shard) seam adds gRPC nodes,
//! replication and peer recovery, a Raft-backed topology control plane,
//! data-moving reassignment/reconciliation, co-location, and exact ranked or
//! exhaustive delivery. Those layers are tested over localhost and single-host
//! container networks but remain experimental pending independent-machine
//! acceptance.
//!
//! Correctness rests on one shared frozen [`Dict`](crate::dict::Dict) and
//! [`TagDict`](crate::tagdict::TagDict): the same `Arc` values in-process and
//! fingerprint-attested byte-identical spaces remotely. Globally consistent
//! feature IDs, compiler semantics, placement generation, and anchor plans keep
//! placement and title routing aligned. Durable reopen uses the committed
//! manifest-selected per-shard segments and source sidecars plus only the log
//! tail after that checkpoint; no object store is part of the design.

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

/// Orphan-slot GC wire contract. Version 2 makes rename failure transactional and exposes
/// durable trash that remains after a prior sweep; coordinators refuse older ambiguous replies.
#[cfg(feature = "distributed")]
pub(crate) const GC_PROTOCOL_VERSION: u32 = 2;

pub use autoscale::{evaluate, AutoscaleConfig, AutoscaleDecision, LoadSnapshot, ScalingAction};
pub use control::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, InMemoryControlPlane,
    NodeDescriptor, NodeId, NodeRole, ShardAssignment, StateVersion,
};
pub use coordinator::{
    recommended_shard_count, resolve_topology, route_topology, seed_position_preserving,
    AddOutcome, ClusterBatchRankedMatch, ClusterConfig, ClusterEngine, ClusterExhaustiveMatch,
    ClusterPitError, ClusterRankedError, ClusterRankedHit, ClusterRankedMatch, ClusterRankedTitle,
    ClusterReadView, ResyncReport, ShardEndpoints,
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
    GcReport, HandoffOutcome, OrphanSlot, ReassignOutcome, RebalanceMoveReport, ReconcileConfig,
    ReconcileReport, ShardGroup,
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
    ShardMetricsSource, ShardServer, DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS,
    DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION, DEFAULT_MAX_GRPC_RESULT_BYTES, MAX_GRPC_RESULT_BYTES,
};
