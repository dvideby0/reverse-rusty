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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::compile::{anchor_plan, extract, extract_readonly, is_hot, CostClass, Extracted};
use crate::config::EngineConfig;
use crate::dict::{Dict, FeatureId};
use crate::error::ParseError;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::segment::MatchStats;

use super::clog::{ClusterLog, ClusterMutation, FileClusterLog, LogPos, NullClusterLog};
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
}

impl ClusterDurable {
    /// The non-durable bundle: a `NullClusterLog`, no `data_dir`.
    fn in_memory(vnodes: u32) -> Self {
        ClusterDurable {
            log: Box::new(NullClusterLog::new()),
            data_dir: None,
            epoch: 0,
            vnodes,
        }
    }
}

/// An in-process multi-shard reverse query matcher.
pub struct ClusterEngine {
    /// The one shared feature space (frozen after [`Self::build`]).
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    ring: HashRing,
    shards: Vec<Box<dyn Shard>>,
    include_broad: bool,
    /// The durable mutation log (the tail); a `NullClusterLog` when in-memory.
    log: Box<dyn ClusterLog>,
    /// Checkpoint generation / log epoch (manifest-resident; bumped on `checkpoint`).
    epoch: AtomicU64,
    /// Ring vnode count (for re-deriving the ring in the manifest on checkpoint).
    vnodes: u32,
    /// Durable-artifact directory (`Some` ⇔ durable).
    data_dir: Option<PathBuf>,
    /// Optional observer for durability events (recovery torn-tail, append failures).
    /// Buffered until set, mirroring the engine's `set_observer` pattern.
    observer: Mutex<Option<ClusterObserver>>,
    pending_events: Mutex<Vec<EngineEvent>>,
}

/// Observer callback for cluster durability events — the `Arc` analogue of the
/// engine's `EventObserver` (`segment.rs`), held so buffered events can be replayed
/// when an observer attaches.
type ClusterObserver = Arc<dyn Fn(&EngineEvent) + Send + Sync>;

impl ClusterEngine {
    /// Build a cluster from an initial corpus. This is the primary constructor:
    /// it builds the ONE authoritative dict over the whole corpus (pass A), freezes
    /// it, creates `K` shards sharing it, then distributes each query to its
    /// placement shard(s) (pass B). One immutable base segment per shard.
    ///
    /// After this the dict is frozen: [`Self::add_query`] can only use vocabulary
    /// already present (it compiles read-only against the shared dict), which is
    /// the in-process limitation noted in the design (new-vocabulary adds need the
    /// deferred feature-model-epoch machinery).
    pub fn build(
        norm: Normalizer,
        config: &ClusterConfig,
        queries: &[(u64, String)],
    ) -> Result<Self, ShardError> {
        if config.num_shards == 0 {
            return Err(ShardError::Config(
                "cluster needs at least one shard".into(),
            ));
        }
        if config.replication_factor == 0 {
            return Err(ShardError::Config(
                "replication_factor must be ≥ 1 (1 = primary only)".into(),
            ));
        }
        let norm = Arc::new(norm);

        // Pass A — build the authoritative dict over the WHOLE corpus, then freeze.
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut extracted: Vec<(u64, Extracted, String)> = Vec::with_capacity(queries.len());
        for (logical, text) in queries {
            if let Ok(ast) = crate::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                extracted.push((*logical, ex, text.clone()));
            }
        }
        dict.finalize_mask();
        let dict = Arc::new(dict);

        let ring = HashRing::new(config.num_shards, config.vnodes)?;

        // Construct concrete local shards: `replication_factor` copies per position (copy 0 =
        // primary, copies 1.. = replicas). A durable cluster roots the primary at `shard_<i>/`
        // (the manifest-recorded copy) and each replica at `shard_<i>/replica_<r>/` (rebuilt
        // from the primary on `open`); an in-memory cluster uses plain in-RAM copies. `build`
        // only makes `LocalShard`s (remote shards arrive via `from_parts`), so pass-B ingest can
        // use the infallible inherent `ingest_local` path on every copy.
        let rf = config.replication_factor;
        let mut groups: Vec<Vec<LocalShard>> = Vec::with_capacity(config.num_shards);
        for s in 0..config.num_shards {
            let mut copies = Vec::with_capacity(rf);
            for r in 0..rf {
                let shard = if let Some(dir) = &config.data_dir {
                    let mut sc = config.per_shard.clone();
                    sc.data_dir = Some(if r == 0 {
                        shard_dir(dir, s)
                    } else {
                        replica_dir(dir, s, r)
                    });
                    LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), sc)?
                } else {
                    LocalShard::new(
                        Arc::clone(&norm),
                        Arc::clone(&dict),
                        config.per_shard.clone(),
                    )
                };
                copies.push(shard);
            }
            groups.push(copies);
        }

        // Pass B — bucket by placement, then ingest one base segment per shard. For a
        // durable cluster each shard's `ingest_local` persists a compiled `.seg`; the
        // initial corpus becomes the committed base (the Aurora "segments are the
        // materialized view" base), recorded in the coordinator manifest below rather
        // than as a raw-DSL snapshot.
        let mut buckets: Vec<Vec<(u64, Extracted, String, u32)>> =
            (0..config.num_shards).map(|_| Vec::new()).collect();
        for (logical, ex, text) in extracted {
            match placement_of(&dict, &ring, &ex) {
                Target::Reject => {}
                Target::Replicated => buckets[0].push((logical, ex, text, 1)),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push((logical, ex.clone(), text.clone(), 1));
                    }
                }
            }
        }
        // Ingest the same bucket into EVERY copy of the owning position (identical op stream
        // ⇒ all copies set-equal by construction).
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                for copy in &groups[s] {
                    copy.ingest_local(&bucket);
                }
            }
        }

        // Durability: commit the coordinator manifest (the atomic base = per-shard
        // segment registry + dict + ring + epoch 0) and open an empty log, or fall back
        // to an in-memory log. Construction fails loud on a durable-setup error (fresh
        // construction — nothing to lose yet); a shard whose segment write fell back to
        // in-memory makes `segment_filenames` error, aborting the build rather than
        // committing a registry that would lose it.
        // Durability: commit the manifest from the PRIMARIES (copy 0 of each position); this
        // borrow of `groups` ends before the positions are consumed into shards below.
        let durable = match &config.data_dir {
            Some(dir) => {
                let primaries: Vec<&LocalShard> = groups.iter().map(|g| &g[0]).collect();
                Self::commit_durable_base(dir, &dict, &ring, config, &primaries)?
            }
            None => ClusterDurable::in_memory(config.vnodes),
        };

        // Assemble each position into a shard: a bare `LocalShard` at RF=1, else a
        // `ReplicatedShard` composite over the primary + replicas.
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(config.num_shards);
        for copies in groups {
            shards.push(into_shard(copies)?);
        }
        Self::from_parts(norm, dict, ring, shards, config.include_broad, durable)
    }

    /// Commit the initial durable base for a freshly built cluster: collect each shard's
    /// segment registry + next-seg-id, write the coordinator manifest (epoch 0,
    /// snapshot_pos 0 — the atomic commit point), and open an empty log. The per-shard
    /// `.seg` files were already written by pass-B ingest; this records which ones are
    /// committed. Returns the durability bundle for [`from_parts`].
    fn commit_durable_base(
        dir: &Path,
        dict: &Dict,
        ring: &HashRing,
        config: &ClusterConfig,
        primaries: &[&LocalShard],
    ) -> Result<ClusterDurable, ShardError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| ShardError::Log(format!("creating cluster data dir: {e}")))?;
        // Only the PRIMARY of each position is committed to the manifest; replicas are not
        // catalogued (rebuilt from the primary via peer recovery on reopen — ADR-035).
        let mut segment_registry = Vec::with_capacity(primaries.len());
        let mut next_seg_ids = Vec::with_capacity(primaries.len());
        for p in primaries {
            segment_registry.push(p.segment_filenames()?);
            next_seg_ids.push(p.next_seg_id()?);
        }
        let manifest = crate::storage::ClusterManifest {
            epoch: 0,
            snapshot_pos: 0,
            dict_fingerprint: dict.fingerprint(),
            num_shards: ring.num_shards() as u32,
            vnodes: config.vnodes,
            include_broad: config.include_broad,
            segment_registry,
            next_seg_ids,
            dict_data: crate::storage::serialize_dict(dict),
        };
        crate::storage::write_cluster_manifest(&manifest, &dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|e| ShardError::Log(format!("writing cluster manifest: {e}")))?;
        let log = FileClusterLog::open(
            &dir.join(CLUSTER_LOG_FILE),
            config.wal_sync_on_write,
            LogPos(0),
        )
        .map_err(|e| ShardError::Log(format!("opening cluster log: {e}")))?;
        Ok(ClusterDurable {
            log: Box::new(log),
            data_dir: Some(dir.to_path_buf()),
            epoch: 0,
            vnodes: config.vnodes,
        })
    }

    /// Assemble a cluster from pre-built parts — the construction seam shared by
    /// [`Self::build`] (which supplies `LocalShard`s) and the distributed builder /
    /// gRPC integration test (which supply boxed `RemoteShard`s). `shards.len()` must
    /// equal `ring.num_shards()`.
    pub(crate) fn from_parts(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        ring: HashRing,
        shards: Vec<Box<dyn Shard>>,
        include_broad: bool,
        durable: ClusterDurable,
    ) -> Result<Self, ShardError> {
        if shards.len() != ring.num_shards() {
            return Err(ShardError::Config(format!(
                "shard count {} must match the ring's shard count {}",
                shards.len(),
                ring.num_shards()
            )));
        }
        Ok(ClusterEngine {
            norm,
            dict,
            ring,
            shards,
            include_broad,
            log: durable.log,
            epoch: AtomicU64::new(durable.epoch),
            vnodes: durable.vnodes,
            data_dir: durable.data_dir,
            observer: Mutex::new(None),
            pending_events: Mutex::new(Vec::new()),
        })
    }

    /// Bulk-load queries into an already-built (frozen-dict) cluster — the load path
    /// for a cluster assembled via [`Self::from_parts`] (e.g. a remote cluster), and
    /// the distributed analog of `build`'s pass B. Buckets each query by placement
    /// (compiling read-only against the shared frozen dict) and ingests each bucket
    /// into its shard through the seam. Parse failures and class-D queries are skipped
    /// (mirroring `build`); a shard write error propagates. Requires a freshly assembled
    /// (empty) cluster: it errors with [`ShardError::Config`] if the cluster already holds
    /// queries, rather than silently re-indexing them as duplicates (use
    /// [`Self::add_query`] for incremental adds).
    pub fn ingest(&self, queries: &[(u64, String)]) -> Result<(), ShardError> {
        // ingest re-indexes from scratch; on a populated cluster it would create duplicate
        // entries. Refuse loudly instead (the doc contract: a freshly assembled cluster).
        if self.num_queries()? > 0 {
            return Err(ShardError::Config(
                "ingest() requires an empty cluster; it re-indexes from scratch — use \
                 add_query for incremental adds"
                    .into(),
            ));
        }
        let entries: Vec<(u64, u32, String)> =
            queries.iter().map(|(l, t)| (*l, 1, t.clone())).collect();
        self.bucket_and_ingest(&entries)?;
        // These bulk adds bypassed the log (they go straight to base segments), so on a
        // durable cluster a checkpoint commits them into the coordinator manifest's
        // per-shard segment registry to survive reopen.
        if self.data_dir.is_some() {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Bucket a set of `(logical, version, dsl)` queries by placement and bulk-ingest one
    /// base segment per shard — the load path for [`Self::ingest`] (a freshly assembled,
    /// e.g. remote, cluster). Compiles read-only against the frozen dict, so placement is
    /// byte-identical to the original build. (Recovery no longer re-ingests; [`Self::open`]
    /// attaches each shard's committed segments instead — ADR-032.)
    fn bucket_and_ingest(&self, entries: &[(u64, u32, String)]) -> Result<(), ShardError> {
        let mut buckets: Vec<Vec<(u64, Extracted, String, u32)>> =
            (0..self.ring.num_shards()).map(|_| Vec::new()).collect();
        let mut lc = String::new();
        for (logical, version, text) in entries {
            let Ok(ast) = crate::dsl::parse(text) else {
                continue;
            };
            let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
            match self.placement(&ex) {
                Target::Reject => {}
                Target::Replicated => buckets[0].push((*logical, ex, text.clone(), *version)),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push((*logical, ex.clone(), text.clone(), *version));
                    }
                }
            }
        }
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.shards[s].ingest_extracted(&bucket)?;
            }
        }
        Ok(())
    }

    /// The placement decision for one compiled query — see the module-level table.
    /// Delegates to the free [`placement_of`] so `build` can bucket the corpus before
    /// the cluster value exists.
    fn placement(&self, ex: &Extracted) -> Target {
        placement_of(&self.dict, &self.ring, ex)
    }

    /// Add one query incrementally (lands in the target shard's memtable). Uses a
    /// read-only compile against the frozen shared dict, so vocabulary not seen at
    /// [`Self::build`] time is dropped (the deferred new-vocabulary limitation).
    ///
    /// WAL-first: the mutation is durably logged BEFORE it is applied to any shard, so a
    /// crash can never leave an acknowledged add that [`Self::open`] would lose. A log
    /// append failure rejects the add (shards untouched) and surfaces a
    /// [`DurabilityFailure`](EngineEvent::DurabilityFailure) — the cluster analogue of
    /// the engine's WAL-first write path (ADR-013).
    pub fn add_query(&self, id: u64, dsl: &str) -> Result<AddOutcome, ShardError> {
        // Reject malformed DSL up front: it carries no replayable mutation, so it must
        // never reach the log (a logged record must parse on replay).
        if let Err(e) = crate::dsl::parse(dsl) {
            return Ok(AddOutcome::RejectedParse(e));
        }
        let m = ClusterMutation::Add {
            logical: id,
            version: 1,
            dsl: dsl.to_string(),
        };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster add_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_add(id, 1, dsl)
    }

    /// Remove a query by logical id. Fans the (idempotent) delete out to every
    /// shard and sums the count — sidestepping any placement journal (a replicated
    /// or any-of query may live on several shards; a re-add may have moved it).
    /// WAL-first, like [`Self::add_query`].
    pub fn remove_query(&self, id: u64) -> Result<usize, ShardError> {
        let m = ClusterMutation::Remove { logical: id };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster remove_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_remove(id)
    }

    /// Apply an ADD to the shards — the state-machine `apply` for adds, shared by the live
    /// write path ([`Self::add_query`], after logging) and log replay ([`Self::open`]).
    /// Re-deriving placement here from the frozen dict makes live and replayed application
    /// byte-identical.
    fn apply_add(&self, id: u64, version: u32, dsl: &str) -> Result<AddOutcome, ShardError> {
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok(AddOutcome::RejectedParse(e)),
        };
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        let outcome = match self.placement(&ex) {
            // Class D is logged-but-unplaceable: a harmless no-op on replay (stored
            // nowhere), matching the caller-visible "rejected, stored nowhere".
            Target::Reject => return Ok(AddOutcome::RejectedClassD),
            Target::Replicated => {
                self.shards[0].insert_extracted(&ex, id, version, dsl)?;
                AddOutcome::Replicated
            }
            Target::Selective(shards) => {
                for &s in &shards {
                    self.shards[s].insert_extracted(&ex, id, version, dsl)?;
                }
                AddOutcome::Placed { shards }
            }
        };
        Ok(outcome)
    }

    /// Apply a REMOVE to the shards — the state-machine `apply` for removes. The shard
    /// memtable/segment liveness is the authority; there is no separate coordinator live
    /// set to keep in sync (the durable base is the per-shard segments — ADR-032).
    fn apply_remove(&self, id: u64) -> Result<usize, ShardError> {
        self.shards
            .iter()
            .map(|s| s.delete_by_logical_id(id))
            .sum::<Result<usize, _>>()
    }

    /// Replay one recovered mutation through the same `apply` funnel as live writes.
    fn replay_apply(&self, m: ClusterMutation) -> Result<(), ShardError> {
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
            } => {
                self.apply_add(logical, version, &dsl)?;
            }
            ClusterMutation::Remove { logical } => {
                self.apply_remove(logical)?;
            }
        }
        Ok(())
    }

    /// Seal every shard's memtable into an immutable base segment.
    pub fn flush(&self) -> Result<(), ShardError> {
        for s in &self.shards {
            s.flush()?;
        }
        Ok(())
    }

    /// The shards a title is routed to: shard 0 (the replicated-lane evaluator)
    /// plus the shard owning each anchor-eligible (non-hot) title feature. Reuses
    /// the same `match_features` primitive the match path uses, so routing and
    /// matching cannot drift.
    fn route(&self, title: &str) -> Vec<usize> {
        let mut lc = String::new();
        let mut feats: Vec<FeatureId> = Vec::new();
        self.norm
            .match_features(title, &self.dict, &mut lc, &mut feats);
        let mut targets: Vec<usize> = Vec::with_capacity(feats.len() + 1);
        targets.push(0);
        for &f in &feats {
            if !is_hot(&self.dict, f) {
                targets.push(self.ring.lookup(f));
            }
        }
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    /// Match one title against the cluster, using the cluster's default broad-lane
    /// setting. Returns matched logical ids (sorted, deduped).
    pub fn percolate(&self, title: &str) -> Result<Vec<u64>, ShardError> {
        Ok(self.percolate_inner(title, self.include_broad)?.0)
    }

    /// [`Self::percolate`] plus merged [`MatchStats`] across the probed shards.
    pub fn percolate_with_stats(&self, title: &str) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.percolate_inner(title, self.include_broad)
    }

    /// Match one title with an explicit broad-lane toggle (overriding the cluster
    /// default) — used by the oracle to sweep broad on/off on one cluster.
    pub fn percolate_with_broad(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<Vec<u64>, ShardError> {
        Ok(self.percolate_inner(title, include_broad)?.0)
    }

    fn percolate_inner(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let targets = self.route(title);
        // Broad is evaluated ONLY on shard 0 (the replicated lane); selective
        // shards hold only main-index queries, so probing their (empty) broad
        // index would be pure waste — and double-counting a broadcast query.
        // A failed shard probe propagates rather than being dropped: a silently
        // missing shard would shrink the union into a FALSE NEGATIVE.
        let parts: Vec<(Vec<u64>, MatchStats)> = if targets.len() <= 1 {
            targets
                .iter()
                .map(|&s| self.shards[s].percolate(title, include_broad && s == 0))
                .collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(|&s| self.shards[s].percolate(title, include_broad && s == 0))
                .collect::<Result<_, _>>()?
        };

        let mut out = Vec::new();
        let mut stats = MatchStats::default();
        for (ids, st) in parts {
            out.extend_from_slice(&ids);
            stats.merge(st);
        }
        out.sort_unstable();
        out.dedup();
        stats.matches = out.len() as u32;
        Ok((out, stats))
    }

    /// Introspection: the shards a title would be routed to (its fan-out).
    pub fn shard_fanout(&self, title: &str) -> Vec<usize> {
        self.route(title)
    }

    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        self.ring.num_shards()
    }

    /// Total physical query count across shards (a replicated/any-of query is
    /// counted once per shard holding it — physical, not distinct-logical).
    pub fn num_queries(&self) -> Result<usize, ShardError> {
        self.shards.iter().map(|s| s.num_queries()).sum()
    }

    /// Per-shard physical query counts (introspection / tests).
    pub fn shard_query_counts(&self) -> Result<Vec<usize>, ShardError> {
        self.shards.iter().map(|s| s.num_queries()).collect()
    }

    /// Cluster-wide per-class entry tally `[A, B, C, D]`, summed across shards
    /// (replicated/any-of queries counted per holding shard). Used by the oracle
    /// to assert each placement branch is actually exercised.
    pub fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        let mut total = [0u64; 4];
        for s in &self.shards {
            let c = s.class_counts()?;
            for i in 0..4 {
                total[i] += c[i];
            }
        }
        Ok(total)
    }

    /// Reopen a durable cluster from `data_dir` (built earlier with a `data_dir` set).
    /// Each shard **attaches-and-mmaps** its committed compiled segments (the
    /// `cluster_manifest.bin` registry) — NOT re-ingest — then the log tail strictly
    /// after the manifest's `snapshot_pos` is replayed through the same apply funnel as
    /// live writes (ADR-032). The frozen dict is restored from the manifest
    /// (fingerprint-checked — a mismatch is a loud [`ShardError::DictMismatch`], ADR-030
    /// parity) and the ring re-derived deterministically, so placement is byte-identical
    /// to the original → zero false negatives across the restart. `config` supplies the
    /// per-shard engine config + fsync policy (defaults if `None`).
    pub fn open(
        data_dir: impl Into<PathBuf>,
        norm: Normalizer,
        config: Option<&ClusterConfig>,
    ) -> Result<Self, ShardError> {
        let data_dir = data_dir.into();
        let manifest_path = data_dir.join(CLUSTER_MANIFEST_FILE);
        if !manifest_path.exists() {
            return Err(ShardError::Config(format!(
                "no cluster manifest at {}; use build() to create a durable cluster",
                manifest_path.display()
            )));
        }
        let manifest = crate::storage::read_cluster_manifest(&manifest_path)
            .map_err(|e| ShardError::Config(format!("reading cluster manifest: {e}")))?;
        let dict = crate::storage::deserialize_dict(&manifest.dict_data)
            .map_err(|e| ShardError::Config(format!("deserializing cluster dict: {e}")))?;
        let dict = Arc::new(dict);
        // Fail loud if the restored dict's fingerprint disagrees with the manifest's —
        // the one false-negative path the fallible seam can't otherwise catch.
        let actual_fp = dict.fingerprint();
        if actual_fp != manifest.dict_fingerprint {
            return Err(ShardError::DictMismatch {
                expected: manifest.dict_fingerprint,
                actual: actual_fp,
            });
        }
        let num_shards = manifest.num_shards as usize;
        // Defensive: the registry + next-seg-id columns must agree with num_shards. A
        // malformed manifest must fail loud, never silently attach the wrong segments.
        if manifest.segment_registry.len() != num_shards
            || manifest.next_seg_ids.len() != num_shards
        {
            return Err(ShardError::Config(format!(
                "cluster manifest is inconsistent: num_shards={num_shards} but registry has \
                 {} shard list(s) and {} next-seg-id(s)",
                manifest.segment_registry.len(),
                manifest.next_seg_ids.len()
            )));
        }
        let norm = Arc::new(norm);
        let ring = HashRing::new(num_shards, manifest.vnodes)?;

        let per_shard = config.map(|c| c.per_shard.clone()).unwrap_or_default();
        let fsync = config.is_some_and(|c| c.wal_sync_on_write);

        // Attach each shard's committed compiled segments (mmap) against the shared dict —
        // NOT re-ingest. Fails loud on a missing / CRC-corrupt segment (a skipped segment
        // is a silent shard-sized false negative).
        let rf = config.map_or(1, |c| c.replication_factor.max(1));
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(num_shards);
        for s in 0..num_shards {
            let primary_dir = shard_dir(&data_dir, s);
            let mut sc = per_shard.clone();
            sc.data_dir = Some(primary_dir.clone());
            let primary = LocalShard::open_segments(
                Arc::clone(&norm),
                Arc::clone(&dict),
                sc,
                &manifest.segment_registry[s],
                manifest.next_seg_ids[s],
            )?;
            // Re-seed replicas (rf-1) by peer recovery from the just-attached primary — replicas
            // are not in the manifest, so they are rebuilt from the durable primary on every open.
            // The log-tail replay below then feeds primary AND replicas through the composite.
            let mut copies: Vec<LocalShard> = Vec::with_capacity(rf);
            let mut recovered: Vec<LocalShard> = Vec::with_capacity(rf.saturating_sub(1));
            for r in 1..rf {
                recovered.push(super::replica::peer_recover(
                    Arc::clone(&norm),
                    Arc::clone(&dict),
                    per_shard.clone(),
                    &primary,
                    &primary_dir,
                    &replica_dir(&data_dir, s, r),
                )?);
            }
            copies.push(primary);
            copies.extend(recovered);
            shards.push(into_shard(copies)?);
        }

        let log = FileClusterLog::open(
            &data_dir.join(CLUSTER_LOG_FILE),
            fsync,
            LogPos(manifest.snapshot_pos),
        )
        .map_err(|e| ShardError::Log(format!("opening cluster log: {e}")))?;

        let durable = ClusterDurable {
            log: Box::new(log),
            data_dir: Some(data_dir.clone()),
            epoch: manifest.epoch,
            vnodes: manifest.vnodes,
        };
        let engine = Self::from_parts(norm, dict, ring, shards, manifest.include_broad, durable)?;

        // The attached segments ARE the base (all entries ≤ snapshot_pos). Replay only the
        // log tail strictly after snapshot_pos, through the SAME apply funnel as live
        // writes — those entries are not in the attached segments, so no double-apply.
        let replay = engine.log.replay(LogPos(manifest.snapshot_pos))?;
        if replay.skipped_bytes > 0 {
            engine.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalTornTail,
                detail: format!(
                    "cluster log torn tail: {} trailing byte(s) skipped during recovery",
                    replay.skipped_bytes
                ),
                error: format!("{} bytes", replay.skipped_bytes),
            });
        }
        for (_pos, m) in replay.entries {
            engine.replay_apply(m)?;
        }
        Ok(engine)
    }

    /// Checkpoint the durable state: seal each shard (flush memtable + re-seal tombstoned
    /// base segments so the on-disk set is a clean materialization of the live state ≤
    /// `up_to`), then commit the coordinator manifest (the atomic commit point: new
    /// per-shard segment registry + log cursor + bumped epoch), then truncate the captured
    /// log prefix and GC orphaned segment files. A no-op on an in-memory cluster.
    ///
    /// Crash-safety: the manifest write is the single commit point (tmp + CRC + rename).
    /// A crash BEFORE it leaves the old (registry, cursor) authoritative — the freshly
    /// written `.seg` are orphans (not in the old registry) recovered via log replay, so
    /// no double-apply and no loss. A crash AFTER it (before truncation) loads the new
    /// segments and replays only the (now shorter) tail — also correct.
    pub fn checkpoint(&self) -> Result<(), ShardError> {
        let Some(dir) = self.data_dir.clone() else {
            return Ok(());
        };
        let up_to = self.log.last_pos()?;
        let new_epoch = self.epoch.load(Ordering::Relaxed) + 1;

        // 1. Seal each shard: memtable → base segment, then bake base-segment tombstones
        //    onto disk. After this every shard's on-disk segments reflect live state ≤ up_to.
        for s in &self.shards {
            s.seal_for_checkpoint()?;
        }

        // 2. Collect the per-shard segment registry + next-seg-ids. An error here (e.g. a
        //    segment write fell back to in-memory) aborts BEFORE the commit, leaving the
        //    old manifest authoritative — nothing is lost.
        let mut segment_registry = Vec::with_capacity(self.shards.len());
        let mut next_seg_ids = Vec::with_capacity(self.shards.len());
        for s in &self.shards {
            segment_registry.push(s.segment_filenames()?);
            next_seg_ids.push(s.next_seg_id()?);
        }

        // 3. Coordinator manifest = the atomic commit point (new base + new cursor).
        let manifest = crate::storage::ClusterManifest {
            epoch: new_epoch,
            snapshot_pos: up_to.0,
            dict_fingerprint: self.dict.fingerprint(),
            num_shards: self.ring.num_shards() as u32,
            vnodes: self.vnodes,
            include_broad: self.include_broad,
            segment_registry: segment_registry.clone(),
            next_seg_ids,
            dict_data: crate::storage::serialize_dict(&self.dict),
        };
        crate::storage::write_cluster_manifest(&manifest, &dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|e| ShardError::Log(format!("writing cluster manifest: {e}")))?;

        // 4. Committed. Truncate the captured log prefix + GC orphaned segment files (both
        //    best-effort: a crash here just replays an already-captured tail / leaves
        //    orphan files that are ignored on open).
        self.epoch.store(new_epoch, Ordering::Relaxed);
        if let Err(e) = self.log.checkpoint(up_to) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalReset,
                detail: "cluster log truncation after checkpoint failed (benign: \
                         replayed on next open)"
                    .into(),
                error: e.to_string(),
            });
        }
        self.gc_orphan_segments(&dir, &segment_registry);
        Ok(())
    }

    /// Best-effort GC of segment files no longer in the committed registry (superseded by
    /// a re-seal/compaction during the just-committed checkpoint, or left by an earlier
    /// crashed checkpoint). An orphan left behind is benign — it is not in the manifest,
    /// so `open` never attaches it.
    fn gc_orphan_segments(&self, dir: &Path, registry: &[Vec<String>]) {
        for (s, files) in registry.iter().enumerate() {
            let keep: HashSet<&str> = files.iter().map(String::as_str).collect();
            let seg_dir = shard_dir(dir, s).join("segments");
            let Ok(rd) = std::fs::read_dir(&seg_dir) else {
                continue;
            };
            for entry in rd.flatten() {
                let path = entry.path();
                let is_seg = path
                    .extension()
                    .is_some_and(|e| e.eq_ignore_ascii_case("seg"));
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if !is_seg || keep.contains(name) {
                    continue;
                }
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::WalReset,
                        detail: format!(
                            "removing orphaned segment {name} after checkpoint failed \
                             (ignored on open)"
                        ),
                        error: e.to_string(),
                    }),
                }
            }
        }
    }

    /// The current checkpoint generation / log epoch (0 for an in-memory cluster).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Register an observer for durability events (recovery torn-tail, append failures).
    /// Any events buffered before this call are delivered immediately, mirroring the
    /// engine's `set_observer`.
    pub fn set_observer(&self, observer: ClusterObserver) {
        let pending: Vec<EngineEvent> = {
            let mut p = self
                .pending_events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *p)
        };
        for ev in &pending {
            observer(ev);
        }
        // Fan the observer into each shard as an event sink, so a `ReplicatedShard` surfaces its
        // degraded-redundancy (`ReplicaDesync`) events through the same observer (ADR-035). A
        // plain shard's default `set_event_sink` is a no-op.
        for shard in &self.shards {
            shard.set_event_sink(Arc::clone(&observer));
        }
        *self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observer);
    }

    /// Emit a durability event: deliver to the observer if set, else buffer it for
    /// delivery on [`Self::set_observer`]. Library code never writes stderr (ADR-021).
    fn emit(&self, ev: EngineEvent) {
        let obs = self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(obs) = obs {
            obs(&ev);
        } else {
            self.pending_events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(ev);
        }
    }
}

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

/// gRPC remote-cluster construction (behind the `distributed` feature).
#[cfg(feature = "distributed")]
impl ClusterEngine {
    /// Assemble a cluster whose K shards are REMOTE (gRPC) — one per `endpoints[i]`,
    /// connected on the given tokio `handle`. Placement + routing run here on the
    /// coordinator, while each server re-compiles DSL read-only against its copy of the
    /// frozen dict, so the ids line up only when the dicts match. To guarantee that, the
    /// coordinator **ships** its dict to each server at connect (ADR-034): an empty/pending
    /// server adopts it, a server already holding it no-ops, and a server holding *data*
    /// under a divergent dict refuses — surfaced as [`ShardError::DictMismatch`], so a
    /// divergent feature space fails loud instead of dropping matches silently (the ADR-029
    /// handshake, now backed by shipping). A data node therefore need not rebuild a
    /// byte-identical dict from the corpus out-of-band; only `norm` must still match the
    /// servers' (`default_vocab()` today — normalizer shipping is a later step, ADR-034).
    /// `endpoints.len()` must equal `config.num_shards`; endpoint `i` serves shard `i`.
    /// Load the corpus afterwards with [`Self::ingest`].
    pub fn connect_remote(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        if endpoints.len() != config.num_shards {
            return Err(ShardError::Config(format!(
                "connect_remote needs exactly one endpoint per shard: got {} endpoints \
                 for {} shards",
                endpoints.len(),
                config.num_shards
            )));
        }
        if config.replication_factor > 1 {
            return Err(ShardError::Config(
                "connect_remote does not support replication_factor > 1; remote per-shard \
                 replication is clustering step 4b (ADR-036)"
                    .into(),
            ));
        }
        let ring = HashRing::new(config.num_shards, config.vnodes)?;
        // Cross-process shared-dict invariant: placement/routing ids line up only when every
        // server's frozen dict equals this coordinator's. SHIP it (ADR-034): serialize once,
        // then adopt per endpoint. An empty server adopts; a server already holding this dict
        // no-ops; a server holding data under a divergent dict refuses → DictMismatch (loud,
        // never a silent drop). Servers therefore needn't rebuild the dict from the corpus.
        let expected = dict.fingerprint();
        let dict_bytes = crate::storage::serialize_dict(&dict);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(endpoints.len());
        for ep in endpoints {
            let remote = super::remote::RemoteShard::connect_and_adopt(
                ep.clone(),
                handle.clone(),
                dict_bytes.clone(),
                expected,
            )?;
            shards.push(Box::new(remote) as Box<dyn Shard>);
        }
        // A remote cluster is non-durable at the coordinator in this increment (the
        // coordinator-level durable log is the in-process story; cross-node durability
        // is a later step). Use the in-memory log so behavior is unchanged.
        Self::from_parts(
            norm,
            dict,
            ring,
            shards,
            config.include_broad,
            ClusterDurable::in_memory(config.vnodes),
        )
    }

    /// Assemble a cluster whose K shard POSITIONS are each a [`ReplicatedShard`](super::replica::ReplicatedShard)
    /// over RF gRPC [`RemoteShard`]s (a primary + replicas), one [`ShardGroup`] per position. Ships +
    /// adopts the frozen dict on EVERY endpoint (ADR-034), then wraps position `i`'s RemoteShards
    /// into one composite boxed as the `i`-th shard — so the coordinator's placement / routing /
    /// merge is identical to a non-replicated remote cluster, while reads fail over to a replica and
    /// writes fan out (ADR-035). `groups.len()` must equal `config.num_shards`; a group with no
    /// replicas degenerates to a bare `RemoteShard` (identical to [`Self::connect_remote`]). Load the
    /// corpus afterwards with [`Self::ingest`].
    pub fn connect_replicated(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        if groups.len() != config.num_shards {
            return Err(ShardError::Config(format!(
                "connect_replicated needs one ShardGroup per shard: got {} for {} shards",
                groups.len(),
                config.num_shards
            )));
        }
        let ring = HashRing::new(config.num_shards, config.vnodes)?;
        let expected = dict.fingerprint();
        let dict_bytes = crate::storage::serialize_dict(&dict);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(groups.len());
        for g in groups {
            let primary = super::remote::RemoteShard::connect_and_adopt(
                g.primary.clone(),
                handle.clone(),
                dict_bytes.clone(),
                expected,
            )?;
            let mut replicas: Vec<Box<dyn Shard>> = Vec::with_capacity(g.replicas.len());
            for ep in &g.replicas {
                let r = super::remote::RemoteShard::connect_and_adopt(
                    ep.clone(),
                    handle.clone(),
                    dict_bytes.clone(),
                    expected,
                )?;
                replicas.push(Box::new(r) as Box<dyn Shard>);
            }
            let shard: Box<dyn Shard> = if replicas.is_empty() {
                Box::new(primary)
            } else {
                Box::new(super::replica::ReplicatedShard::new(
                    Box::new(primary) as Box<dyn Shard>,
                    replicas,
                ))
            };
            shards.push(shard);
        }
        Self::from_parts(
            norm,
            dict,
            ring,
            shards,
            config.include_broad,
            ClusterDurable::in_memory(config.vnodes),
        )
    }

    /// Cross-node peer recovery (ADR-036): bring a fresh, durable, **pending** node up as a copy of
    /// a shard by streaming a peer's segments. Ships the frozen dict to `target_endpoint` (adopt),
    /// then drives its `RecoverFrom`, which pulls `source_endpoint`'s sealed segments, attaches them,
    /// and starts serving. Returns the recovered node's post-attach query count (so the caller can
    /// verify parity or fold it into a group). Writes to the position MUST be quiesced for the
    /// window — there is no durable remote coordinator log to replay a concurrent tail from in this
    /// increment (that couples to the Raft control plane, step 5).
    pub fn peer_recover_replica(
        &self,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        let expected = self.dict.fingerprint();
        let dict_bytes = crate::storage::serialize_dict(&self.dict);
        // Ship the dict so the fresh node attaches segments against the right feature space.
        let target = super::remote::RemoteShard::connect_and_adopt(
            target_endpoint.to_string(),
            handle.clone(),
            dict_bytes,
            expected,
        )?;
        let (_segments, num_queries) = target.recover_from(source_endpoint, expected)?;
        Ok(num_queries)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn vocab() -> Normalizer {
        Normalizer::default_vocab().expect("built-in vocab")
    }

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rr_clog_coord_{}_{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// WAL-first fail-closed: when the durable log append fails, the add is rejected with
    /// `ShardError::Log` AND no shard is mutated (the query never becomes matchable). Needs
    /// private `log` access, so it lives here rather than in the integration oracle.
    #[test]
    fn add_query_is_fail_closed_when_log_append_fails() {
        let dir = scratch_dir("failclosed");
        let cfg = ClusterConfig {
            num_shards: 3,
            data_dir: Some(dir.clone()),
            ..Default::default()
        };
        // Build over a seed corpus so the frozen dict knows these tokens.
        let seed = vec![(1u64, "1994 topps".to_string())];
        let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");
        let before = cluster.num_queries().expect("count");

        // Break the durable log, then attempt an add of an in-vocabulary query.
        cluster.log.break_writes_for_test();
        let res = cluster.add_query(2, "1995 fleer");
        assert!(
            matches!(res, Err(ShardError::Log(_))),
            "expected Log error, got {res:?}"
        );

        // No shard was mutated: count unchanged and id 2 is not matchable.
        assert_eq!(cluster.num_queries().expect("count"), before);
        let hits = cluster.percolate("1995 fleer").expect("percolate");
        assert!(
            !hits.contains(&2),
            "rejected add must not be matchable: {hits:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// On-disk fingerprint guard: a manifest whose stored `dict_fingerprint` disagrees with
    /// the dict it carries must fail `open` loud with `ShardError::DictMismatch` (ADR-030
    /// parity for persisted state), never silently opening a divergent feature space. The
    /// manifest is rewritten through `write_cluster_manifest` so its trailing CRC stays valid,
    /// which exercises the fingerprint check itself — not the CRC check the integration
    /// oracle's `corrupt_manifest_*` test already covers.
    #[test]
    fn open_rejects_manifest_with_divergent_dict_fingerprint() {
        let dir = scratch_dir("fpmismatch");
        let seed = vec![(1u64, "1994 topps".to_string())];
        let cfg = ClusterConfig {
            num_shards: 3,
            data_dir: Some(dir.clone()),
            ..Default::default()
        };
        ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

        // Flip only the stored fingerprint, then rewrite with a fresh (valid) CRC. The dict
        // bytes are untouched, so on open the dict's recomputed fingerprint won't match.
        let mpath = dir.join(CLUSTER_MANIFEST_FILE);
        let mut manifest = crate::storage::read_cluster_manifest(&mpath).expect("read manifest");
        manifest.dict_fingerprint ^= 0xDEAD_BEEF_DEAD_BEEF;
        crate::storage::write_cluster_manifest(&manifest, &mpath).expect("rewrite manifest");

        // ClusterEngine isn't Debug, so match explicitly rather than `{:?}`-printing the Ok arm.
        match ClusterEngine::open(dir.clone(), vocab(), None) {
            Err(ShardError::DictMismatch { .. }) => {}
            Err(other) => panic!("expected DictMismatch, got {other:?}"),
            Ok(_) => panic!("expected DictMismatch, but open() succeeded"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
