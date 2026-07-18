//! `ClusterEngine` — the in-process coordinator: placement (writes), content
//! routing (reads), and cross-shard merge.
//!
//! Design: docs/design/clustering-and-scaling.md §3 (placement + routing), §7
//! (broad lane). Owns the ONE authoritative, frozen [`Dict`]/[`Normalizer`] shared
//! into every shard, the [`HashRing`] over `FeatureId`, and `K` [`Shard`]s.
//!
//! ## Placement (by cost class, derived from [`anchor_plan`], never re-derived)
//! - **A** (one rare required anchor `r1`): one shard = `ring.lookup(r1)`.
//! - **B any-of** (members all rare): one shard per any-of member, deduped.
//! - **B arity-2** (rarest required is hot ⇒ all required hot ⇒ no rare anchor):
//!   the broad lane → every shard.
//! - **C** (broad, hot-only anchor): the broad lane → every shard.
//! - **D** (no anchorable feature): the broad lane → every shard, under the
//!   universal signature, when `accept_class_d` is on (the always-candidate lane,
//!   ADR-068); rejected and stored nowhere otherwise.
//!
//! The broad lane (class C / B-arity-2 / accepted D) is **replicated to every
//! shard** (ADR-080, graduating ADR-027's shard-0 stand-in for §7's "replicate the
//! broad lane to every node"). It is small (~0.2% of the corpus), so N copies cost
//! little; in return no single shard is a broad hotspot. To avoid double-counting,
//! each title evaluates the broad lane on **exactly one** shard — its broad-eval
//! shard (see Routing) — and the cross-shard merge dedups by logical id.
//!
//! ## Routing (reads)
//! A title is probed on the shard owning each of its *anchor-eligible* (non-hot)
//! features — a ~2–5 shard fan-out, never all N — plus its **broad-eval shard**:
//! one shard, picked by a stable title hash, that runs the replicated broad lane
//! with `include_broad`. The broad-eval shard free-rides an already-probed
//! selective target when the title has one (zero extra fan-out), else a hashed
//! shard (a title with no selective anchor — all-hot or empty). The other probed
//! shards run without `include_broad`. Results are unioned and deduped.
//!
//! ## Why this is lossless
//! For any query `Q` a title `T` truly matches: if `Q` is class A / B-any-of, its
//! anchor (resp. some matched member) is a *required* feature, hence present in
//! `T` and non-hot, so `T` routes to `ring.lookup(anchor) =` `Q`'s shard; if `Q`
//! is class B-arity-2 / C / accepted D it lives on the broad lane, which is on
//! **every** shard — and `T`'s broad-eval shard (always one of the shards `T`
//! probes) holds the complete broad lane and evaluates it under `include_broad`.
//! Each shard is a verbatim single-node engine, so its lossless cover + exact
//! verify finish the job. No shard boundary can drop a match.

mod autoscale;
mod control_plane;
mod ingest;
mod lifecycle;
mod logical_ids;
mod matching;
mod ranked;
mod ranked_batch;
mod resize;
mod topology;
mod vocab;

pub use ranked::{ClusterRankedError, ClusterRankedHit, ClusterRankedMatch};
pub use ranked_batch::{ClusterBatchRankedMatch, ClusterRankedTitle};
pub use resize::recommended_shard_count;
pub use topology::{resolve_topology, route_topology, seed_position_preserving, ShardEndpoints};

#[cfg(feature = "distributed")]
mod distributed;
#[cfg(feature = "distributed")]
mod gc;
#[cfg(feature = "distributed")]
mod reassign;
#[cfg(feature = "distributed")]
mod reconcile;
#[cfg(feature = "distributed")]
pub use gc::{GcReport, OrphanSlot};
#[cfg(feature = "distributed")]
pub use reassign::{ReassignOutcome, RebalanceMoveReport};
#[cfg(feature = "distributed")]
pub use reconcile::{ReconcileConfig, ReconcileReport};

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};

use crate::compile::{anchor_plan, CostClass, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::error::ParseError;
use crate::events::EngineEvent;
use crate::normalize::Normalizer;
use crate::tagdict::TagDict;

use super::clog::{ClusterLog, ClusterMutation, NullClusterLog};
use super::control::{ControlPlane, InMemoryControlPlane};
#[cfg(feature = "distributed")]
use super::handoff::HandoffShard;
use super::ring::{HashRing, DEFAULT_VNODES};
use super::shard::{LocalShard, Shard, ShardError};
use super::transport_metrics::TransportMetrics;

/// Filename of the coordinator manifest (the cluster-state document) within `data_dir`.
const CLUSTER_MANIFEST_FILE: &str = "cluster_manifest.bin";
/// Filename of the incremental mutation log within `data_dir`.
const CLUSTER_LOG_FILE: &str = "cluster.log";

/// Directory holding shard `i`'s durable compiled segments (under the cluster `data_dir`).
/// Zero-padded so the dirs sort in shard order. Each is a segments-only engine `data_dir`
/// (`shard_<i>/segments/seg_*.seg` + `shard_<i>/sources.dat`), no per-shard WAL/manifest.
///
/// `pub(crate)` so the multi-shard `ShardServer` (ADR-093) roots each hosted slot's per-shard subdir
/// with the SAME `shard_<NNN>` naming — one source of truth for the on-disk layout.
pub(crate) fn shard_dir(base: &Path, shard: usize) -> PathBuf {
    base.join(format!("shard_{shard:03}"))
}

/// Directory holding shard `shard`'s replica `r` (r ≥ 1; the primary lives at [`shard_dir`]).
/// A throwaway durable copy rebuilt from the primary via peer recovery on every `open`, so it
/// is NOT recorded in the coordinator manifest (replicas are allocated, not catalogued —
/// ADR-035/033, the Elasticsearch model).
fn replica_dir(base: &Path, shard: usize, r: usize) -> PathBuf {
    shard_dir(base, shard).join(format!("replica_{r:03}"))
}

/// Configuration for a [`ClusterEngine`].
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// Number of shards (K). Must be ≥ 1; K = 1 reduces to a single-node engine.
    pub num_shards: usize,
    /// Virtual nodes per shard on the consistent-hash ring.
    pub vnodes: u32,
    /// Replication factor: copies per shard POSITION (1 = primary only — the default, and
    /// byte-identical to pre-ADR-035 behavior; N = primary + N-1 replicas). Replicas are
    /// extra copies kept set-equal by write fan-out that serve reads on primary failover
    /// (clustering build-path step 4). A durable cluster roots the primary at `shard_<i>/`
    /// (the manifest-recorded copy) and each replica at `shard_<i>/replica_<r>/` (rebuilt
    /// from the primary via peer recovery on `open`; not catalogued in the manifest). Must
    /// be ≥ 1.
    pub replication_factor: usize,
    /// Per-shard engine configuration (forwarded to each shard's `Engine`). Leave
    /// `per_shard.data_dir` unset: the coordinator derives each shard's directory
    /// (`shard_<i>/`) from the cluster `data_dir` below and overrides this field per
    /// shard, so segments persist there (ADR-032) with no per-shard WAL/manifest. For an
    /// in-memory cluster (`data_dir = None`) the shards are non-durable.
    pub per_shard: EngineConfig,
    /// Default broad-lane toggle for [`ClusterEngine::percolate`].
    pub include_broad: bool,
    /// Directory for the coordinator's durable artifacts (manifest + base snapshot +
    /// mutation log, ADR-031). When `None`, the cluster is in-memory only (a
    /// [`NullClusterLog`] backs it — byte-identical to the pre-ADR-031 behavior). When
    /// `Some`, [`ClusterEngine::build`] writes durable artifacts and the cluster can be
    /// reopened crash-consistently via [`ClusterEngine::open`].
    pub data_dir: Option<PathBuf>,
    /// Per-append fsync policy for the durable cluster log: `false` (default) fsyncs
    /// only at checkpoints (survives process crash), `true` fsyncs every append
    /// (survives power loss). Mirrors `EngineConfig::wal_sync_on_write`.
    pub wal_sync_on_write: bool,
    /// Live-handoff (ADR-044) pre-fence drain passes: the best-effort drain of the source's tail
    /// to the target while writes still flow, before fencing. Correctness rests on the post-fence
    /// drain CONVERGING, not on this cap — so it is purely a tuning knob (a larger value shrinks
    /// the post-fence quiesce window). Only consulted by `execute_handoff` (the `distributed`
    /// feature); ignored otherwise.
    pub handoff_drain_passes: usize,
    /// Live-handoff (ADR-044) post-fence drain-to-convergence cap: the fenced source's tail is
    /// finite + frozen, so the drain converges in O(in-flight writes) passes; this cap only bounds
    /// a misbehaving source. Past it the flip aborts fail-closed and the source AUTO-UNFENCES
    /// (ADR-048) so it is not left permanently write-quiesced. Only consulted by `execute_handoff`
    /// (the `distributed` feature). A test sets it to `0` to force the abort deterministically.
    pub handoff_final_drain_cap: usize,
}

impl ClusterConfig {
    /// Default pre-fence handoff drain passes (best-effort while writes flow).
    pub const DEFAULT_HANDOFF_DRAIN_PASSES: usize = 8;
    /// Default post-fence drain-to-convergence cap (bounds a misbehaving source).
    pub const DEFAULT_HANDOFF_FINAL_DRAIN_CAP: usize = 1024;
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            num_shards: 8,
            vnodes: DEFAULT_VNODES,
            replication_factor: 1,
            per_shard: EngineConfig::default(),
            include_broad: true,
            data_dir: None,
            wal_sync_on_write: false,
            handoff_drain_passes: Self::DEFAULT_HANDOFF_DRAIN_PASSES,
            handoff_final_drain_cap: Self::DEFAULT_HANDOFF_FINAL_DRAIN_CAP,
        }
    }
}

/// Where a freshly added query landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddOutcome {
    /// Selective query (class A / B any-of): placed on these shard(s).
    Placed { shards: Vec<usize> },
    /// Broad-lane query (class C / B arity-2 / accepted class D): replicated to
    /// every shard (ADR-080).
    Replicated,
    /// Compiled but rejected as cost-class D with `accept_class_d` off — no
    /// anchorable feature, stored nowhere.
    RejectedClassD,
    /// The DSL failed to parse.
    RejectedParse(ParseError),
}

/// One mutation that applied to some target shards but failed on others, queued for repair by
/// [`ClusterEngine::resync`] (ADR-047). Held in memory only — the durable backstop is the
/// cluster log, whose replay on [`ClusterEngine::open`] re-drives every target shard.
#[derive(Clone)]
struct PendingRepair {
    /// The mutation to re-drive (raw DSL for an Add; just the id for a Remove).
    mutation: ClusterMutation,
    /// Target shards that did NOT yet apply it — the only shards `resync` re-drives.
    failed_shards: Vec<usize>,
}

/// Outcome of a [`ClusterEngine::resync`] pass (ADR-047): how many queued partial-apply
/// mutations fully converged this pass, and how many remain (a target shard still unreachable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResyncReport {
    /// Mutations that converged (every previously-failed shard applied this pass).
    pub repaired: usize,
    /// Mutations still pending (at least one target shard still failed); they stay queued.
    pub still_pending: usize,
}

/// Internal placement decision for one compiled query.
enum Target {
    /// Class D with `accept_class_d` off — no anchorable feature, stored nowhere.
    Reject,
    /// A class-B pair is replicated to every shard but remains always-visible.
    ReplicatedAlwaysVisible,
    /// Class C / accepted class D is replicated to every shard and evaluated on
    /// one broad-evaluation position per request.
    ReplicatedBroad,
    /// Selective shards (class A / B any-of), sorted + deduped, non-empty.
    Selective(Vec<usize>),
}

impl Target {
    fn placement(
        &self,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<crate::ownership::QueryPlacement, ShardError> {
        use crate::ownership::QueryPlacement;
        match self {
            Self::Reject => Ok(QueryPlacement::standalone()),
            Self::ReplicatedAlwaysVisible => Ok(QueryPlacement::replicated_always_visible(
                generation, num_shards,
            )?),
            Self::ReplicatedBroad => Ok(QueryPlacement::replicated_broad(generation, num_shards)?),
            Self::Selective(positions) => Ok(QueryPlacement::selective(
                generation,
                num_shards,
                positions.iter().map(|&position| position as u32).collect(),
            )?),
        }
    }
}

/// The durability-related parts of a [`ClusterEngine`], grouped so the [`from_parts`]
/// construction seam takes one bundle instead of five loose arguments.
///
/// [`from_parts`]: ClusterEngine::from_parts
pub(crate) struct ClusterDurable {
    /// The ordered mutation log (the durable tail / source of truth for everything since
    /// the last checkpoint). `NullClusterLog` for an in-memory cluster; `FileClusterLog`
    /// for a durable one.
    pub log: Box<dyn ClusterLog>,
    /// The durable-artifact directory (`Some` ⇔ durable).
    pub data_dir: Option<PathBuf>,
    /// The current checkpoint generation / log epoch (the future Raft term; lives in the
    /// manifest, the cluster-state document, not in the log).
    pub epoch: u64,
    /// ADR-109 placement generation restored from the durable cluster fence.
    pub placement_generation: crate::ownership::PlacementGeneration,
    /// Ring vnode count, captured so the manifest can re-derive a byte-identical ring.
    pub vnodes: u32,
    /// The cluster-state control plane (membership + shard→node map + ring params + model
    /// version + epoch — ADR-037). A single-node [`InMemoryControlPlane`] today (one logical
    /// node owns every shard ⇒ the default path is byte-identical to before ADR-037); an
    /// openraft-backed backend drops in here in step 5b.
    pub control: Box<dyn ControlPlane>,
}

impl ClusterDurable {
    /// The non-durable bundle: a `NullClusterLog`, no `data_dir`, and a single-node
    /// [`InMemoryControlPlane`] over the build's ring params + dict fingerprint.
    fn in_memory(num_shards: u32, vnodes: u32, dict_fingerprint: u64) -> Self {
        ClusterDurable {
            log: Box::new(NullClusterLog::new()),
            data_dir: None,
            epoch: 0,
            placement_generation: crate::ownership::PlacementGeneration::INITIAL,
            vnodes,
            control: Box::new(InMemoryControlPlane::single_node(
                num_shards,
                vnodes,
                dict_fingerprint,
            )),
        }
    }
}

/// An in-process multi-shard reverse query matcher.
pub struct ClusterEngine {
    /// The one shared feature space (frozen after [`Self::build`]).
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    /// The one shared, frozen per-query tag space (ADR-049/055), the `TagDict` analogue of `dict`:
    /// shared read-only into every shard so a tagged write and a percolate filter resolve a given
    /// `(key,value)` to the SAME `TagId` everywhere. Built over the corpus tags at
    /// [`Self::build_with_tags`], finalized, and persisted in the cluster manifest. Empty +
    /// finalized for an untagged cluster ⇒ the byte-identical pre-tag path.
    tag_dict: Arc<TagDict>,
    /// Latch: has any query EVER been written with a non-empty tag set (ADR-055)? `tag_dict`
    /// emptiness is NOT a sufficient proxy — a tag added *after* the dict froze resolves to a
    /// *synthetic* id and is never interned into `tag_dict`, so an untagged-built cluster with live
    /// tagged adds keeps an empty `tag_dict` yet holds tags. Operator introspection only
    /// (cluster-mode `/_stats` via [`Self::has_tagged_queries`]): the vocab rebuild no longer
    /// consults it — tags are carried through `set_vocab` by stored `TagId` (ADR-074), read from
    /// the shards themselves, so correctness doesn't ride this latch (it is best-effort across
    /// reopen: a checkpointed synthetic-only cluster restores it `false`). Set by every tagged
    /// write path; restored on `open` from a non-empty `tag_dict`. `Relaxed` suffices — a
    /// monotonic latch, never the hot path.
    tags_present: AtomicBool,
    /// The vocabulary behind the current normalizer, if one was installed via
    /// [`Self::set_vocab`] (ADR-046). `None` when the cluster was built directly
    /// from a `Normalizer`. Retained so a durable cluster can persist it and a
    /// re-learn can merge into it.
    vocab: Option<Arc<crate::vocab::Vocab>>,
    ring: HashRing,
    shards: Vec<Box<dyn Shard>>,
    /// Exact live logical-id directory. Distributed bounded ranking requires one
    /// live query row per logical id; content-derived placement cannot co-route
    /// arbitrary duplicate-id rows to one emission owner.
    logical_ids: RwLock<logical_ids::LogicalIdDirectory>,
    /// Same-id mutation serialization without globally serializing independent
    /// writes. See `logical_ids` for the compact base + overlay directory.
    logical_write_stripes: Box<[Mutex<()>]>,
    include_broad: bool,
    /// The durable mutation log (the tail); a `NullClusterLog` when in-memory.
    log: Box<dyn ClusterLog>,
    /// Checkpoint generation / log epoch (manifest-resident; bumped on `checkpoint`).
    epoch: AtomicU64,
    /// Monotonic placement identity. Changes only on vocabulary or shard-count
    /// blue/green rebuilds, never on checkpoints or physical data movement.
    placement_generation: AtomicU64,
    /// Ring vnode count (for re-deriving the ring in the manifest on checkpoint).
    vnodes: u32,
    /// Replication factor (copies per shard position) — retained so a vocabulary
    /// change ([`Self::set_vocab`], ADR-046) can rebuild every position's copies.
    replication_factor: usize,
    /// Per-shard engine config — retained so [`Self::set_vocab`] can reconstruct
    /// shards under a new normalizer with the settings the cluster was built with.
    per_shard: EngineConfig,
    /// Durable-artifact directory (`Some` ⇔ durable).
    data_dir: Option<PathBuf>,
    /// Optional observer for durability events (recovery torn-tail, append failures).
    /// Buffered until set, mirroring the engine's `set_observer` pattern.
    observer: Mutex<Option<ClusterObserver>>,
    pending_events: Mutex<Vec<EngineEvent>>,
    /// Multi-shard mutations that applied to some target shards but failed on others (ADR-047),
    /// keyed by logical id so a later mutation for the same id supersedes an earlier pending one
    /// (a successful full apply / a Remove clears any stale entry). Drained + re-driven by
    /// [`Self::resync`]. Empty on the in-process / RF=1 path (its `LocalShard` writes never
    /// fail), so the default path is byte-identical. In memory only — the durable backstop is
    /// the cluster log, replayed on [`Self::open`].
    pending_repair: Mutex<BTreeMap<u64, PendingRepair>>,
    /// The cluster-state control plane: membership + the shard→node map + ring params +
    /// feature-model version + epoch (ADR-037). Read at assembly / introspection time only,
    /// never on the per-title hot path. [`InMemoryControlPlane`] today; openraft-backed later.
    control: Box<dyn ControlPlane>,
    /// Per-RPC gRPC transport metrics (ADR-085), shared (`Arc`) into every `RemoteShard`
    /// so a percolate's per-shard RPC latency / errors / timeouts / retries aggregate
    /// cluster-wide. All-zero on the in-process / RF=1 path (no `RemoteShard` is built), so
    /// the default behavior is byte-identical. Read via [`Self::transport_metrics`].
    transport_metrics: Arc<TransportMetrics>,
    /// Per-position handoff handles (ADR-043), index-aligned with `shards`. Empty on the
    /// in-process/default path (no position is handoff-wrapped ⇒ byte-identical to pre-6a);
    /// populated by the gRPC builders, which wrap each position's backing in a [`HandoffShard`]
    /// so a position can be re-pointed at a new owner at runtime (Stage 6b's `execute_handoff`)
    /// without downcasting `dyn Shard`. `handoffs[i]` and `shards[i]` share one `HandoffShard`.
    #[cfg(feature = "distributed")]
    handoffs: Vec<Arc<HandoffShard>>,
    /// Live-handoff drain caps (ADR-044/048), retained from `ClusterConfig` by the gRPC builders so
    /// `execute_handoff` can read them. Defaults (8 / 1024) on the in-process path, which never
    /// hands off; the gRPC builders override them via `with_handoff_caps`. Overridable so an
    /// operator can tune drain aggressiveness and a test can force the abort (final cap = 0).
    #[cfg(feature = "distributed")]
    handoff_drain_passes: usize,
    #[cfg(feature = "distributed")]
    handoff_final_drain_cap: usize,
    /// The tokio runtime handle the gRPC builders connected on (ADR-048), retained so the
    /// autoscaler's `tick` can drive `execute_handoff` (which needs a handle for its `block_on`
    /// bridge). `None` for an in-process `build` cluster — which has no remote endpoints to hand
    /// off to anyway, so a `Handoff` action is simply skipped there. Set by the gRPC builders via
    /// `with_handle`.
    #[cfg(feature = "distributed")]
    handle: Option<tokio::runtime::Handle>,
    /// Mesh client security (ADR-071), retained from the secure gRPC builders so every
    /// INTERNAL connection the coordinator later makes — peer recovery, live handoff —
    /// rides the same TLS + token as the initial connects. Default (empty) on the
    /// in-process path ⇒ byte-identical.
    #[cfg(feature = "distributed")]
    client_security: super::security::ClientSecurity,
    /// The busy-endpoint move ledger (ADR-095, replacing ADR-090's whole-coordinator
    /// `reassign_serial: Mutex<()>`): every DATA-MOVING op — an operator
    /// `reassign_and_move`/`reassign_group_and_move`/`rebalance_and_move`, a raw
    /// `execute_handoff`, and the autoscaler-driven handoff — reserves its resolved endpoint
    /// footprint here for the whole move-then-commit. Conflicting moves (any shared node: chained
    /// reshuffle, shared source/destination, the same position's committed primary) serialize
    /// exactly as under the old global mutex; moves over DISJOINT node sets may now run in
    /// parallel (the opt-in `max_parallel_moves` waves). It does NOT guard the hot path
    /// (percolate/ingest never touch it) — only the rare admin/autoscaler move path — so a long
    /// segment copy here never stalls reads or writes. Gated, so the lean struct is unchanged;
    /// the in-process path has no `execute_handoff` to serialize.
    #[cfg(feature = "distributed")]
    move_ledger: reassign::MoveLedger,
}

/// Observer callback for cluster durability events — the `Arc` analogue of the
/// engine's `EventObserver` (`segment.rs`), held so buffered events can be replayed
/// when an observer attaches.
type ClusterObserver = Arc<dyn Fn(&EngineEvent) + Send + Sync>;

/// One shard position's placement across nodes for [`ClusterEngine::connect_replicated`]: a
/// primary endpoint + N replica endpoints (RF = 1 + `replicas.len()`). Supplied by the caller
/// in this increment — there is no allocator / control plane yet (that is the Raft step; ADR-036).
#[cfg(feature = "distributed")]
#[derive(Clone, Debug)]
pub struct ShardGroup {
    /// The primary node's endpoint (e.g. `"http://127.0.0.1:50051"`).
    pub primary: String,
    /// Replica node endpoints — on different nodes than the primary.
    pub replicas: Vec<String>,
}

/// Wrap one shard position's copies into a `Box<dyn Shard>`: a bare [`LocalShard`] at RF=1
/// (byte-identical to pre-ADR-035 — no composite overhead at the default), else a
/// [`ReplicatedShard`](super::replica::ReplicatedShard) over the primary (copy 0) + replicas.
fn into_shard(copies: Vec<LocalShard>) -> Result<Box<dyn Shard>, ShardError> {
    let mut it = copies.into_iter();
    let Some(primary) = it.next() else {
        return Err(ShardError::Config(
            "internal: a shard position has no copies (replication_factor must be ≥ 1)".into(),
        ));
    };
    let replicas: Vec<Box<dyn Shard>> = it.map(|c| Box::new(c) as Box<dyn Shard>).collect();
    Ok(if replicas.is_empty() {
        Box::new(primary) as Box<dyn Shard>
    } else {
        Box::new(super::replica::ReplicatedShard::new(
            Box::new(primary) as Box<dyn Shard>,
            replicas,
        )) as Box<dyn Shard>
    })
}

/// The placement decision for one compiled query — see the module-level table. A free
/// fn over (`dict`, `ring`) so [`ClusterEngine::build`] can bucket the corpus before
/// the cluster value exists, and [`ClusterEngine::placement`] can delegate. Forbidden
/// features can't leak in: `anchor_plan` reads only `required`/`anyof`, never
/// `forbidden` (ADR-006 holds structurally).
///
/// `accept_class_d` (the per-shard [`EngineConfig`](crate::config::EngineConfig) knob)
/// gates the cluster always-candidate lane (ADR-068/080): a negation-only class-D query
/// is placed on the broad lane (every shard, under the universal signature) when the knob
/// is on, and rejected otherwise. The decision is re-derived identically on log replay
/// (same frozen dict + same config), so live ≡ replay.
///
/// `theta` is the hot-anchor threshold (ADR-105). A class-H query places
/// **selectively, exactly like class A**: its anchors are non-top-64 required
/// features, which `route()` ring-routes on the title side, so every matching
/// title probes the shard(s) holding it — no replication, no broad-eval-shard
/// gating (the tier is always-visible on the shards that own it). Because A and
/// H produce the IDENTICAL `Target`, placement is θ-invariant: a θ change (or a
/// coordinator/shard θ mismatch) can never move a query to a different shard,
/// only between the two always-probed indexes on the same shard — the ADR-105
/// benign-divergence property.
fn placement_of(
    dict: &Dict,
    ring: &HashRing,
    ex: &Extracted,
    accept_class_d: bool,
    theta: u32,
) -> Target {
    let ap = anchor_plan(ex, dict, theta);
    match ap.class {
        CostClass::D => {
            // Stored only when the lane is on AND there is something to forbid: an
            // effectively-empty query (no positives, no negatives) would match every title,
            // so the shard engines reject it regardless (`rejects_class_d`). Rejecting HERE —
            // before fan-out — is load-bearing for `upsert`: a plan every shard would reject
            // must not tombstone the prior version first (a silent delete-with-no-replace).
            if accept_class_d && !ex.forbidden.is_empty() {
                Target::ReplicatedBroad
            } else {
                Target::Reject
            }
        }
        CostClass::C => Target::ReplicatedBroad,
        CostClass::A | CostClass::B | CostClass::H => {
            // A class-B-arity-2 query's only main anchor is an all-hot PAIR (a len-2
            // group): no rare feature to hash on, so it joins the replicated lane.
            // Class A and class-B any-of have only arity-1 non-hot anchors, which the
            // ring distributes selectively — and class H's arity-1 anchors are
            // non-top-64 by definition, so they ring-place the same way (chained
            // below; the defensive len!=1 guard would fail a future arity>1 hot
            // anchor safe into the replicated lane rather than mis-hashing it).
            if ap
                .main_anchors
                .iter()
                .chain(ap.hot_anchors.iter())
                .any(|g| g.len() != 1)
            {
                return Target::ReplicatedAlwaysVisible;
            }
            let mut shards: Vec<usize> = ap
                .main_anchors
                .iter()
                .chain(ap.hot_anchors.iter())
                .filter_map(|g| g.first().copied())
                .map(|f| ring.lookup(f))
                .collect();
            shards.sort_unstable();
            shards.dedup();
            if shards.is_empty() {
                Target::Reject
            } else {
                Target::Selective(shards)
            }
        }
    }
}
