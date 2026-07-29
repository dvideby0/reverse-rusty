//! `ShardServer` — serves the gRPC `ShardService` over ONE in-process `LocalShard`.
//!
//! Construct it over the SAME frozen `Arc<Dict>` / `Arc<Normalizer>` the coordinator
//! uses for placement. The write path carries raw DSL (not pre-extracted feature
//! ids), so the server re-compiles read-only against ITS copy of that dict — a
//! dict-agnostic wire that fails loud on mismatch rather than corrupting matches.
//! Placement + routing stay the coordinator's job; the server is a dumb executor of
//! `percolate` / `ingest` / `insert` / `delete` / `flush`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use tonic::Status;

use crate::cluster::coordinator::shard_dir;
use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::rank::RankProfiles;
use crate::segment::PlacedQuery;
use crate::tagdict::TagDict;

use super::proto::shard_service_server::ShardServiceServer;
use super::security::{
    ClientSecurity, CoordinatorLease, CoordinatorLeaseService, MeshAuthVerify, ServerSecurity,
    TlsServerIdentity,
};
use super::shard::{LocalShard, Shard, ShardError};

/// Tonic's default receive-message ceiling. Operators may lower the application
/// cap but ADR-110 deliberately does not permit raising this transport cliff.
pub const DEFAULT_MAX_GRPC_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_GRPC_RESULT_BYTES: usize = DEFAULT_MAX_GRPC_RESULT_BYTES;
/// Node-local admission for long-running exhaustive shard streams. This is
/// deliberately independent of the coordinator HTTP job quota because a
/// shard endpoint can be called by more than one coordinator or directly.
pub const DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS: usize = 2;
/// Server-owned ceiling for one exhaustive shard stream. The request carries
/// its coordinator's remaining budget, but a direct caller must not be able to
/// retain a blocking worker and admission permit indefinitely.
pub const DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION: Duration = Duration::from_mins(5);

mod durable;
mod metrics_source;
mod service;

use durable::{read_adopted_space, restore_durable_slots, sweep_dropped_trash};
pub use metrics_source::ShardMetricsSource;

#[cfg(test)]
mod tests;

struct ServerState {
    dict: Arc<Dict>,
    /// The frozen per-query tag space (ADR-049/055), shipped by the coordinator via `AdoptDict`
    /// alongside the dict. Held so the server resolves ingested tags read-only against the same
    /// space the coordinator's filter `TagId`s came from. Empty until adopted (a pre-built `new`
    /// server starts empty; the coordinator's adopt installs the real one). An `Arc` clone of the
    /// node-scope [`AdoptedSpace`] — every slot on the node shares the one deserialized dict/tag pair.
    tag_dict: Arc<TagDict>,
    shard: LocalShard,
}

/// One hosted shard on a multi-shard node (ADR-093): its swappable engine state + its OWN fence
/// generation. Keying the fence PER SLOT is the codex-P1 fix — fencing one shard for a handoff no
/// longer write-quiesces a co-located shard on the same node (a shared `AtomicU64` could not do this).
struct ShardSlot {
    /// `None` until this slot adopts a dict; reads/writes against a pending slot return
    /// `failed_precondition`.
    state: ArcSwapOption<ServerState>,
    /// The fence generation for THIS slot (ADR-044 semantics, now per-shard, ADR-093): `0` ⇒ not
    /// fenced; `> 0` ⇒ this slot has been demoted at that generation, so its data-mutating writes
    /// return `failed_precondition`. Set monotonically by `Fence`, CAS-cleared by `Unfence`.
    fenced_at_generation: AtomicU64,
    /// Per-RPC service-latency histograms (ADR-100), rendered by the `/_metrics` exposition. On
    /// the SLOT (not the swappable state) so an in-place `recover_from` state swap keeps the
    /// series continuous; a whole-slot replacement is an ordinary Prometheus counter reset.
    latency: super::node_metrics::SlotLatency,
    /// Cumulative broad-lane cost counters (ADR-101), accumulated from each percolate's
    /// `MatchStats` at the handler boundary — same slot-lifetime semantics as `latency`.
    broad: super::node_metrics::SlotBroadCost,
    /// Bounded rank-delivery counters (ADR-110), slot-lifetime like latency.
    ranked: super::node_metrics::SlotRankDelivery,
}

impl ShardSlot {
    /// A slot holding an already-built [`ServerState`], not fenced.
    fn loaded(state: ServerState) -> Arc<Self> {
        Arc::new(ShardSlot {
            state: ArcSwapOption::from(Some(Arc::new(state))),
            fenced_at_generation: AtomicU64::new(0),
            latency: super::node_metrics::SlotLatency::new(),
            broad: super::node_metrics::SlotBroadCost::new(),
            ranked: super::node_metrics::SlotRankDelivery::new(),
        })
    }

    /// This slot's adopted state, or `failed_precondition` if the slot has not adopted a dict yet.
    fn loaded_state(&self) -> Result<Arc<ServerState>, Status> {
        self.state
            .load_full()
            .ok_or_else(|| Status::failed_precondition("shard has not adopted a dict yet"))
    }

    /// Reject a data-mutating write if this slot has been fenced (demoted by a live handoff,
    /// ADR-044). Called by `insert`/`delete`/`ingest` only — reads + the recovery RPCs deliberately
    /// do NOT call it, so a demoted owner keeps serving them until the coordinator stops routing to it
    /// (serve-then-drop), and an in-flight read never hits the fence.
    fn check_not_fenced(&self) -> Result<(), Status> {
        let gen = self.fenced_at_generation.load(Ordering::Acquire);
        if gen > 0 {
            return Err(Status::failed_precondition(format!(
                "shard is fenced at generation {gen} (demoted by a handoff); writes are rejected"
            )));
        }
        Ok(())
    }
}

/// The node-scope adopted feature space (ADR-093): ONE frozen dict + tag dict, deserialized once per
/// node and shared by `Arc` into every slot's [`ServerState`], so co-locating N shards on a node never
/// deserializes N dicts. The node-level `DictFingerprint` handshake reads this, independent of any slot.
struct AdoptedSpace {
    dict: Arc<Dict>,
    tag_dict: Arc<TagDict>,
    placement_generation: crate::ownership::PlacementGeneration,
    num_shards: u32,
}

/// The map of shards this node hosts, keyed by `shard_id` (= global position, ADR-093).
type ShardMap = Arc<RwLock<HashMap<u32, Arc<ShardSlot>>>>;

/// The irrevocable fence value a `DropShard` removal swaps in (ADR-096): no legitimate handoff
/// ever fences at `u64::MAX`, `Fence`'s `fetch_max` can never lower it, and `unfence` explicitly
/// refuses to clear it — so once a slot is tombstoned mid-drop, no concurrent fence traffic
/// (e.g. a stale-fence probe's `unfence(probe)`) can resurrect its writability.
pub(in crate::cluster::server) const DROPPED_TOMBSTONE: u64 = u64::MAX;

/// A node-scope adopted-space cell holding the given (already-deserialized) dict + tag space.
fn node_space_cell(dict: Arc<Dict>, tag_dict: Arc<TagDict>) -> Arc<ArcSwapOption<AdoptedSpace>> {
    Arc::new(ArcSwapOption::from(Some(Arc::new(AdoptedSpace {
        dict,
        tag_dict,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL,
        num_shards: 1,
    }))))
}

/// A shard map holding one slot at shard-id 0 — the pre-built / 1:1 deployment.
fn single_slot(slot: Arc<ShardSlot>) -> ShardMap {
    let mut map = HashMap::new();
    map.insert(0, slot);
    Arc::new(RwLock::new(map))
}

/// A gRPC server wrapping ONE in-process shard.
///
/// The (dict, shard) pair is **swappable**: a server may start *pending* (dict-less) via
/// [`ShardServer::pending`] and adopt the coordinator's frozen dict through the `AdoptDict`
/// RPC, so a data node need not rebuild a byte-identical dict from the corpus out-of-band
/// (ADR-034). `norm` + `config` are fixed for the server's life (the normalizer must still
/// match the coordinator's — `default_vocab()` today; see ADR-034 scope note).
pub struct ShardServer {
    norm: Arc<Normalizer>,
    config: EngineConfig,
    /// Immutable node-local profile registry. Ranked requests carry a semantic
    /// identity and resolve through this registry before scoring; replies echo
    /// the resolved identity so the coordinator can reject old or divergent
    /// peers instead of accepting different scores.
    rank_profiles: Arc<RankProfiles>,
    /// `Some` ⇒ a **durable** node: its shard persists segments under this dir (ADR-035), so
    /// the node can serve `FetchSegments` (stream its segments to a recovering peer) and accept
    /// `RecoverFrom` (pull a peer's segments + attach). `None` ⇒ in-memory (today's default).
    /// When set, `AdoptDict` builds a durable (segments-only) shard rather than an in-memory one.
    data_dir: Option<PathBuf>,
    /// The shards this node hosts, keyed by `shard_id` (= global position, ADR-093). ONE process can
    /// host many, each independently adopted / fenced / recovered; the 1:1 deployment holds exactly one
    /// slot (its position). A std `RwLock` keeps the lean dependency tree (no `dashmap`); the read path
    /// clones the slot `Arc` out and drops the guard immediately, so it is NEVER held across an
    /// RPC/`await` (the `recover_from` handler dials a peer). Empty ⇒ pending (awaiting `AdoptDict`).
    shards: ShardMap,
    /// The node-scope adopted dict/tag space (ADR-093): deserialized ONCE, its `Arc`s shared into every
    /// slot's [`ServerState`]. `None` until the first adopt (or, for a durable node, until
    /// `open_durable` reads it back). The node-level `DictFingerprint` handshake reads this — the
    /// dict/tag-dict fingerprints are a node-wide content invariant, independent of any slot.
    node_dict: Arc<ArcSwapOption<AdoptedSpace>>,
    /// Exclusive renewable owner for remote coordination. Explicit handshakes
    /// claim the node, owner RPCs renew the bounded lease, and takeover drains
    /// admitted response bodies before publishing a replacement id. This
    /// prevents two process-local mutation barriers from both certifying the
    /// same remote shard set as an exact snapshot.
    coordinator_lease: Arc<CoordinatorLease>,
    /// Mesh security (ADR-071): TLS identity + expected cluster token, applied by the
    /// `serve*` methods. Default (none) ⇒ the historical plaintext/open behavior.
    security: ServerSecurity,
    /// The CLIENT half of the mesh security (ADR-071) — what THIS node presents when it
    /// dials OUT (the `RecoverFrom` handler's pull from a peer source). Default (none) ⇒
    /// plaintext, the historical behavior.
    client_security: ClientSecurity,
    /// `Some` ⇒ also serve the standard `grpc.health.v1.Health` service on this SEPARATE
    /// plaintext port for Kubernetes liveness/readiness probes (ADR-084). `None` (default)
    /// ⇒ no second listener — byte-identical to the historical single-port behavior.
    health_addr: Option<SocketAddr>,
    /// Exact protobuf encoded-result cap for unary result messages and each
    /// `FetchMatches` stream item (ADR-110).
    max_grpc_result_bytes: usize,
    /// Node-scope non-queuing admission for `PercolateAll` blocking workers.
    /// An owned permit lives for the complete stream worker, including any
    /// bounded-channel backpressure wait.
    exhaustive_permits: Arc<tokio::sync::Semaphore>,
    /// Hard node-local wall-clock ceiling for `PercolateAll`, independent of
    /// the caller-supplied remaining budget.
    max_exhaustive_stream_duration: Duration,
}

mod construct;
mod serve;
mod slots;

/// Build the tonic `ServerTlsConfig` from an operator identity — shared with
/// [`ControlServer`](super::control_server::ControlServer) via the same shapes.
pub(crate) fn server_tls_config(tls: &TlsServerIdentity) -> tonic::transport::ServerTlsConfig {
    tonic::transport::ServerTlsConfig::new().identity(tonic::transport::Identity::from_pem(
        &tls.cert_pem,
        &tls.key_pem,
    ))
}

/// Compile one raw query read-only against the shared frozen dict (parse failure →
/// `None`, counted by the caller as a rejected-parse).
fn compile_item(norm: &Normalizer, dict: &Dict, dsl: &str, lc: &mut String) -> Option<Extracted> {
    let ast = crate::dsl::parse(dsl).ok()?;
    Some(extract_readonly(&ast, norm, dict, lc))
}

/// An empty but FINALIZED tag space — the placeholder a pre-built / pending server holds until the
/// coordinator's `AdoptDict` installs the real one (ADR-055). Finalized so the engine's read-only
/// tag-resolution invariant (`debug_assert!(is_finalized())`) holds even before an adopt.
fn finalized_empty_tag_dict() -> TagDict {
    let mut td = TagDict::new();
    td.mark_finalized();
    td
}
