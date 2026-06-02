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
//!   the replicated lane → shard 0.
//! - **C** (broad, hot-only anchor): the replicated lane → shard 0.
//! - **D** (no anchorable feature): rejected, stored nowhere.
//!
//! Shard 0 is the in-process stand-in for "replicate the broad lane to every node"
//! (§7): it holds the complete class-C + class-B-arity-2 set and is the only shard
//! that evaluates it, so there is no double-counting.
//!
//! ## Routing (reads)
//! A title is probed on shard 0 (always, for the replicated lane) plus the shard
//! owning each of the title's *anchor-eligible* (non-hot) features — a ~2–5 shard
//! fan-out, never all N. Shard 0 runs with `include_broad`; the selective shards
//! run without it (they hold only main-index queries). Results are unioned and
//! deduped.
//!
//! ## Why this is lossless
//! For any query `Q` a title `T` truly matches: if `Q` is class A / B-any-of, its
//! anchor (resp. some matched member) is a *required* feature, hence present in
//! `T` and non-hot, so `T` routes to `ring.lookup(anchor) =` `Q`'s shard; if `Q`
//! is class B-arity-2 / C it lives on shard 0, which `T` always probes. Each shard
//! is a verbatim single-node engine, so its lossless cover + exact verify finish
//! the job. No shard boundary can drop a match.

mod autoscale;
mod control_plane;
mod ingest;
mod lifecycle;
mod matching;
mod vocab;

#[cfg(feature = "distributed")]
mod distributed;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use crate::compile::{anchor_plan, CostClass, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::error::ParseError;
use crate::events::EngineEvent;
use crate::normalize::Normalizer;

use super::clog::{ClusterLog, ClusterMutation, NullClusterLog};
use super::control::{ControlPlane, InMemoryControlPlane};
#[cfg(feature = "distributed")]
use super::handoff::HandoffShard;
use super::ring::{HashRing, DEFAULT_VNODES};
use super::shard::{LocalShard, Shard, ShardError};

/// Filename of the coordinator manifest (the cluster-state document) within `data_dir`.
const CLUSTER_MANIFEST_FILE: &str = "cluster_manifest.bin";
/// Filename of the incremental mutation log within `data_dir`.
const CLUSTER_LOG_FILE: &str = "cluster.log";

/// Directory holding shard `i`'s durable compiled segments (under the cluster `data_dir`).
/// Zero-padded so the dirs sort in shard order. Each is a segments-only engine `data_dir`
/// (`shard_<i>/segments/seg_*.seg` + `shard_<i>/sources.dat`), no per-shard WAL/manifest.
fn shard_dir(base: &Path, shard: usize) -> PathBuf {
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
        }
    }
}

/// Where a freshly added query landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddOutcome {
    /// Selective query (class A / B any-of): placed on these shard(s).
    Placed { shards: Vec<usize> },
    /// Replicated-lane query (class C / B arity-2): placed on the designated shard.
    Replicated,
    /// Compiled but rejected as cost-class D — no anchorable feature, stored nowhere.
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
    Reject,
    /// The replicated lane (class C / B arity-2) → shard 0.
    Replicated,
    /// Selective shards (class A / B any-of), sorted + deduped, non-empty.
    Selective(Vec<usize>),
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
    /// The vocabulary behind the current normalizer, if one was installed via
    /// [`Self::set_vocab`] (ADR-046). `None` when the cluster was built directly
    /// from a `Normalizer`. Retained so a durable cluster can persist it and a
    /// re-learn can merge into it.
    vocab: Option<Arc<crate::vocab::Vocab>>,
    ring: HashRing,
    shards: Vec<Box<dyn Shard>>,
    include_broad: bool,
    /// The durable mutation log (the tail); a `NullClusterLog` when in-memory.
    log: Box<dyn ClusterLog>,
    /// Checkpoint generation / log epoch (manifest-resident; bumped on `checkpoint`).
    epoch: AtomicU64,
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
    /// Per-position handoff handles (ADR-043), index-aligned with `shards`. Empty on the
    /// in-process/default path (no position is handoff-wrapped ⇒ byte-identical to pre-6a);
    /// populated by the gRPC builders, which wrap each position's backing in a [`HandoffShard`]
    /// so a position can be re-pointed at a new owner at runtime (Stage 6b's `execute_handoff`)
    /// without downcasting `dyn Shard`. `handoffs[i]` and `shards[i]` share one `HandoffShard`.
    #[cfg(feature = "distributed")]
    handoffs: Vec<Arc<HandoffShard>>,
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
fn placement_of(dict: &Dict, ring: &HashRing, ex: &Extracted) -> Target {
    let ap = anchor_plan(ex, dict);
    match ap.class {
        CostClass::D => Target::Reject,
        CostClass::C => Target::Replicated,
        CostClass::A | CostClass::B => {
            // A class-B-arity-2 query's only main anchor is an all-hot PAIR (a len-2
            // group): no rare feature to hash on, so it joins the replicated lane.
            // Class A and class-B any-of have only arity-1 non-hot anchors, which the
            // ring distributes selectively.
            if ap.main_anchors.iter().any(|g| g.len() != 1) {
                return Target::Replicated;
            }
            let mut shards: Vec<usize> = ap
                .main_anchors
                .iter()
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
