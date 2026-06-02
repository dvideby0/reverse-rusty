//! `Shard` — the local↔remote seam — and `LocalShard`, its in-process implementation.
//!
//! [`Shard`] abstracts the OPERATION a coordinator performs on a shard, never the
//! shard's internal data: a remote shard has no in-process [`EngineSnapshot`], so the
//! trait exposes [`Shard::percolate`] (the matched ids + stats for one title) rather
//! than handing back a snapshot. [`LocalShard`] is the in-process impl — an owned
//! [`Engine`] (writes serialized behind a `std::sync::Mutex`) plus an
//! `ArcSwap<EngineSnapshot>` for lock-free reads, exactly the per-engine pattern the
//! HTTP server uses. It does NOT re-implement matching; `percolate` delegates to
//! [`EngineSnapshot::match_title`]. Every `LocalShard` is constructed with
//! [`Engine::with_shared`] over the coordinator's frozen normalizer + dict, and all
//! writes go through the read-only `*_extracted` paths so the shared `Arc<Dict>` is
//! never forked.
//!
//! Every operation returns [`Result<_, ShardError>`]: a `LocalShard` is infallible
//! (it always returns `Ok`), but a remote shard can fail on the wire. Surfacing that
//! as an error — rather than swallowing it into an empty result — is load-bearing for
//! the zero-false-negative contract: a dropped shard probe must fail the percolate,
//! not silently shrink the answer. The remote implementation (`RemoteShard`, behind
//! the `distributed` feature) lives in `super::remote` and satisfies the same trait
//! by issuing gRPC calls.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;

use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::segment::{Engine, EngineSnapshot, IngestReport, MatchScratch, MatchStats};

use super::clog::{ClusterMutation, LogPos};
use super::translog;

/// An error from cluster construction or a shard operation. In-process
/// ([`LocalShard`]) *operations* are infallible and never produce this; a `RemoteShard`
/// produces [`ShardError::Remote`] on gRPC transport or status failure, and
/// [`ShardError::DictMismatch`] when a server's frozen dict diverges from the
/// coordinator's (the connect-time fingerprint handshake). Cluster *construction* (the
/// `ClusterEngine` builders and `HashRing::new`) produces [`ShardError::Config`] on an
/// invalid configuration. Kept transport-agnostic (a `String` detail, not a
/// `tonic::Status`) so it lives in the always-compiled core alongside the trait, rather
/// than dragging the gated networking stack into the lean build.
#[derive(Debug, Clone)]
pub enum ShardError {
    /// A remote shard was unreachable or returned an error status (detail included).
    Remote(String),
    /// Invalid cluster configuration / construction precondition — e.g. zero shards, or
    /// a shard/endpoint count that disagrees with the ring. Replaces the old
    /// construction-time `assert!`s so library code never panics on bad input.
    Config(String),
    /// A remote shard's frozen-dict fingerprint disagreed with the coordinator's at
    /// connect time. The cross-process shared-dict invariant is broken, so matching
    /// against that shard would *silently* drop results — fail loud instead. This is the
    /// one false-negative path the otherwise-fallible seam cannot catch (ADR-029).
    DictMismatch { expected: u64, actual: u64 },
    /// A cluster mutation could not be durably logged (the coordinator's externalized
    /// `ClusterLog`, ADR-031). The mutation is *rejected*, not applied — surfacing it
    /// rather than acknowledging an unlogged write is load-bearing for the
    /// rebuild-from-log contract (an un-logged add/remove would silently vanish on
    /// reopen). Parallels the engine's WAL-first write path (ADR-013).
    Log(String),
    /// A cluster-state transition could not be committed by the control plane (no quorum,
    /// not the leader, or a backend error — ADR-037). The transition is *rejected*, not
    /// applied; surfacing it rather than serving a stale/blind shard→node map is
    /// load-bearing (a silently-wrong assignment routes a title to the wrong node — a
    /// shard-sized false negative). The structured cause is in
    /// [`ControlError`](super::control::ControlError); this is the folded form crossing the
    /// coordinator boundary. The in-memory single-node control plane never produces it.
    ControlPlane(String),
    /// A selective multi-shard mutation applied to some target shards but FAILED on others (a
    /// remote shard write errored mid-fan-out — ADR-047). Distinguished from a clean failure
    /// (`Remote`/`Log`, where nothing applied) so a higher layer can act precisely: the
    /// mutation IS durably logged (committed), the `applied` shards already hold it, the
    /// `failed` shards do not yet, and the coordinator has queued the failed shards for repair.
    /// Call [`ClusterEngine::resync`](crate::cluster::ClusterEngine::resync) to converge them
    /// (or reopen, whose log replay re-drives every target); do NOT re-`add_query`, which would
    /// double-log. Never produced by the in-process / RF=1 path (its `LocalShard` writes are
    /// infallible — an empty failure set yields the normal `Ok` outcome).
    PartiallyApplied {
        /// Logical id of the mutation that partially applied.
        logical: u64,
        /// Shards that DID apply it (they already hold the new state).
        applied: Vec<usize>,
        /// Shards that did NOT (queued for repair; a transient false-negative window).
        failed: Vec<usize>,
        /// The first underlying shard error, for context.
        detail: String,
    },
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardError::Remote(m) => write!(f, "remote shard error: {m}"),
            ShardError::Config(m) => write!(f, "cluster config error: {m}"),
            ShardError::DictMismatch { expected, actual } => write!(
                f,
                "dict fingerprint mismatch: coordinator {expected:#018x} != shard \
                 {actual:#018x} (every shard must share the coordinator's frozen dict)"
            ),
            ShardError::Log(m) => write!(f, "cluster log durability error: {m}"),
            ShardError::ControlPlane(m) => write!(f, "cluster control-plane error: {m}"),
            ShardError::PartiallyApplied {
                logical,
                applied,
                failed,
                detail,
            } => write!(
                f,
                "cluster mutation for logical {logical} partially applied: applied on shards \
                 {applied:?}, FAILED on {failed:?} ({detail}); durably logged — resync or reopen \
                 to converge"
            ),
        }
    }
}

impl std::error::Error for ShardError {}

/// Sink for shard-level observability events (e.g. a [`ReplicatedShard`] replica
/// dropping out of its in-sync set). The `Arc` analogue of the engine's event
/// observer; the coordinator fans its observer in via `ClusterEngine::set_observer`.
///
/// [`ReplicatedShard`]: super::replica::ReplicatedShard
pub(crate) type EventSink = Arc<dyn Fn(&crate::events::EngineEvent) + Send + Sync>;

/// One shard, local or remote — the seam that lets a coordinator hold a mix of
/// in-process and (eventually) networked shards behind one type.
///
/// Abstracts the OPERATION, not the data: there is deliberately no `snapshot()`,
/// because a remote shard has no local [`EngineSnapshot`]. [`Shard::percolate`] IS
/// the per-shard probe (matched logical ids + [`MatchStats`]); `include_broad` is the
/// ALREADY-RESOLVED per-shard toggle — the coordinator applies the "broad lane only
/// on shard 0" rule before calling, and the shard never re-derives it.
///
/// `Send + Sync` is a supertrait because the coordinator fans probes out across rayon
/// worker threads, which borrow `&dyn Shard`. Object-safety and the `Send + Sync`
/// bound are enforced for free by `ClusterEngine.shards: Vec<Box<dyn Shard>>` plus the
/// `assert_send_sync::<ClusterEngine>()` guard in `lib.rs`.
pub(crate) trait Shard: Send + Sync {
    // ---- reads ----
    /// Probe this shard for one title; returns matched logical ids + match stats.
    fn percolate(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError>;
    /// Physical query count held by this shard (a replicated/any-of query is counted
    /// once per local entry, so it is counted on each shard holding it).
    fn num_queries(&self) -> Result<usize, ShardError>;
    /// Per-class entry tally `[A, B, C, D]` for this shard (introspection/tests).
    fn class_counts(&self) -> Result<[u64; 4], ShardError>;

    /// This shard's live `(logical_id, dsl)` source set — the corpus the shard's
    /// index is a materialized view of. Used by `ClusterEngine::set_vocab` to
    /// rebuild every shard under a new normalizer (ADR-046). Default: `Err` — only
    /// a shard backed by an in-process [`Engine`] (`LocalShard`/`ReplicatedShard`)
    /// can enumerate its sources; a `RemoteShard`'s sources live in another process
    /// (a cross-process vocabulary change is out of scope for v1, and `set_vocab`
    /// refuses a non-local cluster before ever calling this).
    fn live_sources(&self) -> Result<Vec<(u64, String)>, ShardError> {
        Err(ShardError::Config(
            "live_sources is only supported for in-process shards".into(),
        ))
    }

    /// Whether this shard is backed by an in-process [`Engine`], so its normalizer
    /// can be swapped in place by a vocabulary change. `false` for a
    /// `RemoteShard`/`HandoffShard`, whose normalizer lives in another process and
    /// is NOT shipped a vocabulary change in v1 — `ClusterEngine::set_vocab` refuses
    /// to run unless every shard is local, so an alias can never silently diverge
    /// across processes (a cross-process false negative the dict-fingerprint
    /// handshake would not catch, since an alias is normalizer-only).
    fn is_local(&self) -> bool {
        false
    }

    // ---- writes ----
    /// Bulk-ingest a pre-extracted bucket into a new immutable base segment — the
    /// distributed load path ([`crate::cluster::ClusterEngine::ingest`]). NOTE:
    /// `ClusterEngine::build` does NOT use this; it ingests via the infallible inherent
    /// [`LocalShard::ingest_local`] so that constructing an in-process cluster stays
    /// infallible. This seam method is what lets the coordinator load a *remote* shard.
    fn ingest_extracted(
        &self,
        items: &[(u64, Extracted, String, u32)],
    ) -> Result<IngestReport, ShardError>;
    /// Insert one pre-extracted query into the memtable (live add).
    fn insert_extracted(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
    ) -> Result<Option<u32>, ShardError>;
    /// Tombstone every live entry for `logical` (idempotent; a cheap no-op on a shard
    /// that doesn't hold it).
    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError>;
    /// Seal the memtable into an immutable base segment.
    fn flush(&self) -> Result<(), ShardError>;

    // ---- durable checkpoint (ADR-032; local shards only) ----
    /// Seal for a cluster checkpoint: flush the memtable AND re-seal any tombstoned base
    /// segment, so the ON-DISK segment set reflects every applied delete. Without the
    /// re-seal a `Remove` against a base segment lives only in the in-RAM alive overlay
    /// and would resurrect the query on reopen once its log entry is truncated.
    ///
    /// Returns the per-shard translog position `P` the sealed segments now capture through
    /// (ADR-039): every op `≤ P` is durably in the segments, and the translog is trimmed to
    /// `P` so its remaining tail is exactly the un-sealed ops `> P`. A recovering replica
    /// streams the segments (`≤ P`) then replays the tail (`> P`) — no overlap, no
    /// double-apply (the zero-false-negative boundary). In-memory shards return `LogPos(0)`.
    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError>;
    /// This shard's live (mmap'd) base-segment filenames — the registry the coordinator
    /// commits into `cluster_manifest.bin`. `Err` (never a silent empty list) if any
    /// segment is in-memory (a write fell back), which would otherwise lose data on reopen.
    fn segment_filenames(&self) -> Result<Vec<String>, ShardError>;
    /// This shard's next segment-id counter — committed per shard so a flush after reopen
    /// never reuses a committed segment filename.
    fn next_seg_id(&self) -> Result<u64, ShardError>;

    // ---- per-shard query log / translog (ADR-039; clustering step 5c) ----
    /// The un-sealed tail of this shard's durable query log: every logged mutation with
    /// position strictly after `from`, oldest-first (the ops NOT yet baked into a sealed
    /// segment). A recovering replica calls this after attaching the source's segments at
    /// `P = seal_for_checkpoint()` to replay the writes that landed during the copy window —
    /// the durable+replicated tail that lets recovery proceed WITHOUT quiescing writes
    /// (closing ADR-036's gap). A non-durable (in-memory) shard returns an empty tail.
    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError>;

    // ---- translog retention leases (ADR-040; clustering step 5d) ----
    /// Acquire a retention lease pinning this shard's current un-sealed translog tail: until
    /// the lease is renewed or released, [`seal_for_checkpoint`](Shard::seal_for_checkpoint)
    /// will NOT trim the translog past the returned position. A peer recovery acquires one
    /// before snapshotting segments, so even if the source seals AGAIN during the copy (another
    /// concurrent recovery, a checkpoint) the tail the recovery still needs survives — closing a
    /// latent false negative in ADR-039's no-quiesce path (a concurrent seal could strand it).
    /// Returns `(lease_id, pinned_pos)`. Default (in-memory / non-durable): a no-op lease at
    /// `LogPos(0)` — such a shard has no on-disk tail to retain and is never a recovery source.
    fn acquire_retention_lease(&self) -> Result<(u64, LogPos), ShardError> {
        Ok((0, LogPos(0)))
    }
    /// Advance a retention lease to `to` as a recovery consumer catches up, so the source may GC
    /// the now-consumed prefix on its next seal (the lease only ever moves forward). Idempotent;
    /// an unknown lease id is ignored. Default: a no-op.
    fn renew_retention_lease(&self, _lease: u64, _to: LogPos) -> Result<(), ShardError> {
        Ok(())
    }
    /// Release a retention lease — the recovery finished or aborted, so the source may again trim
    /// freely to its checkpoint. Idempotent (releasing twice, or an unknown id, is a no-op).
    /// Default: a no-op.
    fn release_retention_lease(&self, _lease: u64) -> Result<(), ShardError> {
        Ok(())
    }

    // ---- runtime replica growth (ADR-040; clustering step 5d) ----
    /// Bring up a NEW replica for this position from its primary and add it to the in-sync set
    /// WITHOUT quiescing writes for the segment-copy window — peer-recover a snapshot + tail, loop
    /// the catch-up to shrink the residual, then promote under a brief write quiesce (the finalize).
    /// A retention lease pins the primary's tail across the flow, so a concurrent seal can't strand
    /// it. Default: error — only a replicated position ([`ReplicatedShard`](super::replica::ReplicatedShard))
    /// can grow a local replica here (a bare/remote position has no in-process primary to copy from).
    fn add_recovered_replica(
        &self,
        _norm: &Arc<Normalizer>,
        _dict: &Arc<Dict>,
        _config: EngineConfig,
        _primary_dir: &Path,
        _replica_dir: &Path,
        _max_passes: usize,
    ) -> Result<(), ShardError> {
        Err(ShardError::Config(
            "this shard position cannot grow an in-process replica (not a replicated local position)"
                .into(),
        ))
    }

    // ---- observability (ADR-035) ----
    /// Install an event sink so this shard can surface degraded-redundancy events — e.g. a
    /// [`ReplicatedShard`](super::replica::ReplicatedShard) replica falling out of its
    /// in-sync set. Default: a no-op (a plain [`LocalShard`]/`RemoteShard` emits nothing
    /// here). The coordinator fans its observer in via `ClusterEngine::set_observer`.
    fn set_event_sink(&self, _sink: EventSink) {}
}

/// Translog retention leases (ADR-040): a set of `lease_id → retained_pos`. A recovery source
/// keeps every translog op strictly after `min(retained_pos)`, so an in-flight peer recovery's
/// tail is never trimmed out from under it by a concurrent seal. With no leases the floor is
/// absent and a seal trims to its checkpoint `P` — byte-identical to ADR-039.
#[derive(Default)]
struct RetentionLeases {
    next_id: u64,
    held: BTreeMap<u64, u64>,
}

impl RetentionLeases {
    /// Register a lease pinning ops `> at`; returns its id.
    fn acquire(&mut self, at: u64) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.held.insert(id, at);
        id
    }
    /// Advance a lease forward (monotonic — a lease never moves a consumer's cursor back).
    fn renew(&mut self, id: u64, to: u64) {
        if let Some(p) = self.held.get_mut(&id) {
            *p = (*p).max(to);
        }
    }
    fn release(&mut self, id: u64) {
        self.held.remove(&id);
    }
    /// The lowest pinned position across all leases (`None` ⇒ no lease ⇒ trim freely to `P`).
    fn floor(&self) -> Option<u64> {
        self.held.values().copied().min()
    }
}

/// One in-process shard: owned engine for writes + lock-free snapshot for reads, plus a
/// per-shard durable query log (the translog, ADR-039). The translog is a no-op
/// [`NullClusterLog`](super::clog::NullClusterLog) for an in-memory shard (byte-identical to
/// pre-ADR-039) and a CRC-framed [`FileClusterLog`](super::clog::FileClusterLog) for a durable
/// shard (the un-sealed-write tail a recovering replica replays). Replay re-derives features
/// from the raw DSL against the frozen dict, so the caller (which always holds the shared
/// `norm`/`dict`) supplies them to [`apply_mutation`] — the shard need not retain them.
pub(crate) struct LocalShard {
    engine: Mutex<Engine>,
    snapshot: ArcSwap<EngineSnapshot>,
    translog: Box<translog::ShardLog>,
    /// Open peer-recovery retention leases (ADR-040): while any is held, `seal_for_checkpoint`
    /// trims the translog only to `min(P, leases.floor())`, so a concurrent seal can't strand an
    /// in-flight recovery's tail. A separate `Mutex` from `engine` (lock order is always
    /// engine→retention; the lease methods take only this one).
    retention: Mutex<RetentionLeases>,
    /// Retained for translog replay (re-derive features from raw DSL) on self-restart, and to
    /// stamp the per-shard checkpoint sidecar's dict fingerprint.
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    /// `Some` ⇒ durable (segments + translog + checkpoint sidecar live here); `None` ⇒ in-memory.
    data_dir: Option<PathBuf>,
}

impl LocalShard {
    /// Build a shard sharing the coordinator's frozen normalizer + dict. In-memory ⇒ a
    /// no-op [`NullClusterLog`](super::clog::NullClusterLog) translog (byte-identical to
    /// pre-ADR-039) and no checkpoint sidecar.
    pub(crate) fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        let engine = Engine::with_shared(Arc::clone(&norm), Arc::clone(&dict), config);
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog: translog::null(),
            retention: Mutex::new(RetentionLeases::default()),
            norm,
            dict,
            data_dir: None,
        }
    }

    /// Build a DURABLE shard (ADR-032): an engine that persists compiled segments under
    /// `config.data_dir` with no WAL and no own manifest, plus a durable translog (ADR-039).
    /// **Self-restart (ADR-039 §6):** if a checkpoint sidecar is already present in the dir, this
    /// is a node restarting over its own prior data — attach its committed segments and replay the
    /// translog tail instead of starting fresh. Otherwise a fresh empty durable shard.
    pub(crate) fn new_durable(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
    ) -> Result<Self, ShardError> {
        let dir = config.data_dir.clone().ok_or_else(|| {
            ShardError::Log("durable shard requires a data_dir for its translog".into())
        })?;
        if let Some(ckpt) = translog::read_sidecar(&dir)? {
            return Self::open_durable_self(norm, dict, config, &ckpt);
        }
        let translog = translog::open_fresh(&dir, config.wal_sync_on_write)?;
        let engine =
            Engine::with_shared_segments_only(Arc::clone(&norm), Arc::clone(&dict), config)
                .map_err(|e| ShardError::Log(format!("creating durable shard: {e}")))?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        Ok(LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog,
            retention: Mutex::new(RetentionLeases::default()),
            norm,
            dict,
            data_dir: Some(dir),
        })
    }

    /// Reopen a durable shard by attaching an EXPLICIT committed segment list (ADR-032) against
    /// the shared dict — attach-and-mmap, not re-ingest. `files`/`next_seg_id` come from the
    /// coordinator's `cluster_manifest.bin`; the attached segments are the durable base, and the
    /// translog starts FRESH (ADR-039) — the coordinator `ClusterLog` (in-process) or the
    /// peer-recovery tail repopulates it. (Distinct from `new_durable`'s sidecar-driven
    /// self-restart: this is the coordinator-managed attach.)
    pub(crate) fn open_segments(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
    ) -> Result<Self, ShardError> {
        let dir = config.data_dir.clone();
        let translog = match &dir {
            Some(d) => translog::open_fresh(d, config.wal_sync_on_write)?,
            None => translog::null(),
        };
        let engine = Engine::open_shared_segments(
            Arc::clone(&norm),
            Arc::clone(&dict),
            config,
            files,
            next_seg_id,
        )
        .map_err(|e| ShardError::Log(format!("attaching shard segments: {e}")))?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        Ok(LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog,
            retention: Mutex::new(RetentionLeases::default()),
            norm,
            dict,
            data_dir: dir,
        })
    }

    /// Self-restart a durable shard from its checkpoint sidecar (ADR-039 §6): attach the committed
    /// segments (ops ≤ `local_checkpoint`), open the EXISTING translog (the on-disk tail is the
    /// authority — not reset), and replay the un-sealed tail (ops > `local_checkpoint`) into the
    /// engine. Fail-loud if the sidecar's dict fingerprint diverges (never attach segments built
    /// for a different feature space). Replay is engine-only (the ops are already in the translog).
    fn open_durable_self(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
        ckpt: &translog::ShardCheckpoint,
    ) -> Result<Self, ShardError> {
        let dir = config
            .data_dir
            .clone()
            .ok_or_else(|| ShardError::Log("durable self-restart requires a data_dir".into()))?;
        if ckpt.dict_fingerprint != dict.fingerprint() {
            return Err(ShardError::DictMismatch {
                expected: dict.fingerprint(),
                actual: ckpt.dict_fingerprint,
            });
        }
        let floor = LogPos(ckpt.local_checkpoint);
        let translog = translog::open_existing(&dir, config.wal_sync_on_write, floor)?;
        let engine = Engine::open_shared_segments(
            Arc::clone(&norm),
            Arc::clone(&dict),
            config,
            &ckpt.segment_files,
            ckpt.next_seg_id,
        )
        .map_err(|e| ShardError::Log(format!("attaching shard segments on self-restart: {e}")))?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        let shard = LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog,
            retention: Mutex::new(RetentionLeases::default()),
            norm,
            dict,
            data_dir: Some(dir),
        };
        // Replay the un-sealed tail (ops > P) into the engine ONLY — the ops are already on disk
        // in the translog, so re-appending would duplicate them. Position-filtered, so it never
        // double-applies an op already baked into the attached segments.
        let tail = shard.translog.replay(floor)?.entries;
        for (_pos, m) in &tail {
            shard.apply_to_engine(m);
        }
        Ok(shard)
    }

    /// Apply one logged mutation to the engine WITHOUT re-appending it to the translog — used by
    /// self-restart replay (ADR-039 §6), where the op is already durable in the translog. The
    /// translog-appending counterpart is the seam's `insert_extracted`/`delete_by_logical_id`.
    /// Infallible: a segments-only engine has no WAL, so neither apply can error.
    fn apply_to_engine(&self, m: &ClusterMutation) {
        let mut eng = self.lock();
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
            } => {
                if let Ok(ast) = crate::dsl::parse(dsl) {
                    let mut lc = String::new();
                    let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
                    eng.insert_extracted(&ex, *logical, *version, dsl);
                }
            }
            ClusterMutation::Remove { logical } => {
                eng.delete_by_logical_id(*logical).unwrap_or(0);
            }
        }
        Self::publish(&eng, &self.snapshot);
    }

    /// Bulk-ingest, infallibly — the build path uses this directly on a concrete
    /// `LocalShard` (before boxing) so `ClusterEngine::build` stays infallible. The
    /// trait's `ingest_extracted` is the `Result`-wrapped view of the same work.
    pub(crate) fn ingest_local(&self, items: &[(u64, Extracted, String, u32)]) -> IngestReport {
        let mut eng = self.lock();
        let report = eng.ingest_extracted(items);
        Self::publish(&eng, &self.snapshot);
        report
    }

    /// Lock the engine, recovering the guard if a prior writer panicked: a poisoned
    /// shard mutex must not take down the whole cluster, and the engine state behind
    /// it is still self-consistent (writes are atomic at this layer).
    fn lock(&self) -> std::sync::MutexGuard<'_, Engine> {
        self.engine.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Republish the lock-free read snapshot after a write.
    fn publish(eng: &Engine, slot: &ArcSwap<EngineSnapshot>) {
        slot.store(Arc::new(eng.snapshot()));
    }

    /// The current lock-free read snapshot (an `Arc` clone — no engine lock). Private:
    /// the seam exposes `percolate`, not the snapshot, so a remote shard need not have
    /// one.
    fn snapshot(&self) -> Arc<EngineSnapshot> {
        self.snapshot.load_full()
    }
}

impl Shard for LocalShard {
    /// Verbatim the body of the coordinator's old `query_shard`: allocate scratch,
    /// match one title against the lock-free snapshot, return ids + stats. Infallible
    /// — wrapped in `Ok` to satisfy the (remote-capable) trait.
    fn percolate(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let stats = self
            .snapshot()
            .match_title(title, &mut scratch, &mut out, include_broad);
        Ok((out, stats))
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        Ok(self.snapshot.load().num_queries())
    }

    fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        Ok(self.snapshot.load().class_counts())
    }

    fn live_sources(&self) -> Result<Vec<(u64, String)>, ShardError> {
        Ok(self.lock().live_sources())
    }

    fn is_local(&self) -> bool {
        true
    }

    fn ingest_extracted(
        &self,
        items: &[(u64, Extracted, String, u32)],
    ) -> Result<IngestReport, ShardError> {
        Ok(self.ingest_local(items))
    }

    fn insert_extracted(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
    ) -> Result<Option<u32>, ShardError> {
        let mut eng = self.lock();
        // Log-first / fail-closed (ADR-039): durably record the mutation in this shard's
        // translog BEFORE applying it, under the engine lock so the log order equals the
        // apply order (a re-add then re-remove of one id must replay in the same order it
        // applied). A durable translog is the un-sealed tail a recovering peer replays; the
        // in-memory translog is a no-op. An append failure rejects the write (engine
        // untouched), mirroring the coordinator's WAL-first add_query.
        self.translog.append(&ClusterMutation::Add {
            logical,
            version,
            dsl: text.to_string(),
        })?;
        let out = eng.insert_extracted(ex, logical, version, text);
        Self::publish(&eng, &self.snapshot);
        Ok(out)
    }

    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        let mut eng = self.lock();
        // Log-first (ADR-039): see `insert_extracted`. Idempotent on replay.
        self.translog.append(&ClusterMutation::Remove { logical })?;
        // The engine delete itself never errors for a cluster shard (segments-only, no WAL).
        let n = eng.delete_by_logical_id(logical).unwrap_or(0);
        Self::publish(&eng, &self.snapshot);
        Ok(n)
    }

    fn flush(&self) -> Result<(), ShardError> {
        let mut eng = self.lock();
        eng.flush();
        Self::publish(&eng, &self.snapshot);
        // NOTE: a bare flush seals the memtable into a segment but does NOT trim the translog
        // — a `Remove` against a base segment is only baked by `reseal_tombstoned_segments`,
        // so only `seal_for_checkpoint` (flush + reseal) may advance the checkpoint and trim.
        Ok(())
    }

    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        let mut eng = self.lock();
        eng.flush(); // seal the memtable into a base segment
        eng.reseal_tombstoned_segments(); // bake base-segment tombstones onto disk
        Self::publish(&eng, &self.snapshot);
        // Everything ≤ `p` is now durably in the sealed/resealed segments; trim the translog
        // to it so its remaining tail is exactly the un-sealed ops > `p` (ADR-039). Held under
        // the engine lock, so no concurrent write advances `last_pos` between flush and read.
        let p = self.translog.last_pos()?;
        // A durable shard records a checkpoint sidecar so the data node can self-recover after a
        // crash (ADR-039 §6): write it AFTER the segments are durable and BEFORE trimming the
        // translog, so a crash in between just replays an already-captured (position-filtered)
        // prefix — never a loss, never a double-apply.
        if let Some(dir) = &self.data_dir {
            let segment_files = eng.segment_filenames().map_err(|e| {
                ShardError::Log(format!("collecting segment filenames for checkpoint: {e}"))
            })?;
            translog::write_sidecar(
                dir,
                &translog::ShardCheckpoint {
                    next_seg_id: eng.next_seg_id(),
                    local_checkpoint: p.0,
                    dict_fingerprint: self.dict.fingerprint(),
                    segment_files,
                },
            )?;
        }
        // Trim the translog only to the retention floor (ADR-040): a held peer-recovery lease
        // keeps the tail a recovery still needs even though we seal here. With no lease the floor
        // is absent and this is `p` — byte-identical to ADR-039. The segments still capture every
        // op ≤ `p` (the sidecar's `local_checkpoint` is `p`); any retained ops in `(trim_to, p]`
        // are redundant with the segments and position-filtered out on replay (replay is from `p`).
        let trim_to = {
            let r = self
                .retention
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            r.floor().map_or(p.0, |f| p.0.min(f))
        };
        self.translog.checkpoint(LogPos(trim_to))?;
        Ok(p)
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        self.lock()
            .segment_filenames()
            .map_err(|e| ShardError::Log(format!("collecting shard segment filenames: {e}")))
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        Ok(self.lock().next_seg_id())
    }

    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        Ok(self.translog.replay(from)?.entries)
    }

    fn acquire_retention_lease(&self) -> Result<(u64, LogPos), ShardError> {
        // Pin at the current high-water so every un-sealed op is retained for the recovery. The
        // read-then-register is benign under a racing seal: a seal that trims to `L' > at` before
        // this lease registers also sealed `(at, L']` into segments, so a recovery copying segments
        // at `P ≥ L'` still has them; once registered, no future seal trims past `at`.
        let at = self.translog.last_pos()?;
        let id = self
            .retention
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .acquire(at.0);
        Ok((id, at))
    }

    fn renew_retention_lease(&self, lease: u64, to: LogPos) -> Result<(), ShardError> {
        self.retention
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .renew(lease, to.0);
        Ok(())
    }

    fn release_retention_lease(&self, lease: u64) -> Result<(), ShardError> {
        self.retention
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .release(lease);
        Ok(())
    }
}

/// Apply one logged mutation to a shard through its normal write path — so the op is itself
/// re-logged into that shard's translog (a recovered replica's tail stays consistent) and
/// applied to its engine. Re-derives features from the raw DSL against the frozen `dict`
/// (the ADR-029 DSL-on-wire invariant), so a replayed op is byte-identical to the original
/// live write → the recovered shard converges to the same logical set (zero false negatives).
/// Used by both in-process peer recovery ([`super::replica::peer_recover`]) and the
/// coordinator's gRPC tail-replay.
pub(crate) fn apply_mutation(
    shard: &dyn Shard,
    norm: &Normalizer,
    dict: &Dict,
    m: &ClusterMutation,
) -> Result<(), ShardError> {
    match m {
        ClusterMutation::Add {
            logical,
            version,
            dsl,
        } => {
            // Only parseable DSL is ever logged, but stay defensive: an unparseable record
            // carries no applicable mutation, so skip it rather than fail the whole replay.
            if let Ok(ast) = crate::dsl::parse(dsl) {
                let mut lc = String::new();
                let ex = extract_readonly(&ast, norm, dict, &mut lc);
                shard.insert_extracted(&ex, *logical, *version, dsl)?;
            }
        }
        ClusterMutation::Remove { logical } => {
            shard.delete_by_logical_id(*logical)?;
        }
    }
    Ok(())
}
