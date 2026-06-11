//! [`LocalShard`] — the in-process [`Shard`] implementation.
//!
//! An owned [`Engine`] (writes serialized behind a `Mutex`) plus an
//! `ArcSwap<EngineSnapshot>` for lock-free reads, plus a per-shard durable query log
//! (the translog, ADR-039). Holds the struct, every constructor (in-memory / durable /
//! attach / self-restart), the inherent write+read helpers, the `Shard` trait impl, and
//! the clock-injectable seal core ([`LocalShard::seal_for_checkpoint_at`]).

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use crate::cluster::clog::{ClusterMutation, LogPos};
use crate::cluster::translog;
use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::segment::{Engine, EngineSnapshot, IngestReport, MatchScratch, MatchStats, PlacedQuery};
use crate::tagdict::TagDict;

use super::retention::{resolve_lease_ttl, RetentionLeases};
use super::{EventSink, Shard, ShardError};

/// One in-process shard: owned engine for writes + lock-free snapshot for reads, plus a
/// per-shard durable query log (the translog, ADR-039). The translog is a no-op
/// [`NullClusterLog`](crate::cluster::clog::NullClusterLog) for an in-memory shard (byte-identical to
/// pre-ADR-039) and a CRC-framed [`FileClusterLog`](crate::cluster::clog::FileClusterLog) for a durable
/// shard (the un-sealed-write tail a recovering replica replays). Replay re-derives features
/// from the raw DSL against the frozen dict, so the caller (which always holds the shared
/// `norm`/`dict`) supplies them to [`apply_mutation`](super::apply_mutation) — the shard need not retain them.
pub(crate) struct LocalShard {
    engine: Mutex<Engine>,
    snapshot: ArcSwap<EngineSnapshot>,
    translog: Box<translog::ShardLog>,
    /// Open peer-recovery retention leases (ADR-040): while any is held, `seal_for_checkpoint`
    /// trims the translog only to `min(P, leases.floor())`, so a concurrent seal can't strand an
    /// in-flight recovery's tail. A separate `Mutex` from `engine` (lock order is always
    /// engine→retention; the lease methods take only this one).
    retention: Mutex<RetentionLeases>,
    /// Retention-lease TTL (ADR-048): a lease that has not heartbeated within this window is
    /// reaped at the next `seal_for_checkpoint`, so a crashed recovery can no longer pin the
    /// tail forever. `None` ⇒ disabled (a lease never expires — byte-identical to ADR-040).
    /// Derived once at construction from `EngineConfig::retention_lease_ttl_secs`.
    retention_lease_ttl: Option<Duration>,
    /// Optional event sink (ADR-021), installed by the coordinator's `set_observer`. A plain
    /// `LocalShard` emitted nothing before ADR-048; now it surfaces a TTL lease reap so an
    /// abandoned recovery is observable rather than silent. `None` ⇒ no observer (events
    /// dropped — byte-identical default path; a reap only fires at checkpoint time, long after
    /// an observer would have attached at cluster build/open).
    event_sink: Mutex<Option<EventSink>>,
    /// Retained for translog replay (re-derive features from raw DSL) on self-restart, and to
    /// stamp the per-shard checkpoint sidecar's dict fingerprint.
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    /// `Some` ⇒ durable (segments + translog + checkpoint sidecar live here); `None` ⇒ in-memory.
    data_dir: Option<PathBuf>,
}

impl LocalShard {
    /// Build a shard sharing the coordinator's frozen normalizer + dict. In-memory ⇒ a
    /// no-op [`NullClusterLog`](crate::cluster::clog::NullClusterLog) translog (byte-identical to
    /// pre-ADR-039) and no checkpoint sidecar.
    pub(crate) fn new(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
    ) -> Self {
        let retention_lease_ttl = resolve_lease_ttl(&config);
        // `tag_dict` is moved into the engine (the shard keeps no separate copy — the engine holds
        // the shared frozen tag space and does all read-only resolution against it).
        let engine = Engine::with_shared(Arc::clone(&norm), Arc::clone(&dict), tag_dict, config);
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog: translog::null(),
            retention: Mutex::new(RetentionLeases::default()),
            retention_lease_ttl,
            event_sink: Mutex::new(None),
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
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
    ) -> Result<Self, ShardError> {
        let dir = config.data_dir.clone().ok_or_else(|| {
            ShardError::Log("durable shard requires a data_dir for its translog".into())
        })?;
        if let Some(ckpt) = translog::read_sidecar(&dir)? {
            return Self::open_durable_self(norm, dict, tag_dict, config, &ckpt);
        }
        let retention_lease_ttl = resolve_lease_ttl(&config);
        let translog = translog::open_fresh(&dir, config.wal_sync_on_write)?;
        let engine = Engine::with_shared_segments_only(
            Arc::clone(&norm),
            Arc::clone(&dict),
            tag_dict,
            config,
        )
        .map_err(|e| ShardError::Log(format!("creating durable shard: {e}")))?;
        // Write the INITIAL (empty) checkpoint sidecar so a durable shard is
        // self-restartable from the moment it exists (ADR-072): a crash before the
        // first seal then takes the `open_durable_self` path above — open the
        // EXISTING translog and replay its whole tail — instead of this fresh path,
        // whose `open_fresh` resets the translog (which would drop acknowledged
        // live writes) and ignores bulk-written segments.
        translog::write_sidecar(
            &dir,
            &translog::ShardCheckpoint {
                next_seg_id: engine.next_seg_id(),
                local_checkpoint: 0,
                dict_fingerprint: dict.fingerprint(),
                segment_files: Vec::new(),
            },
        )?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        Ok(LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog,
            retention: Mutex::new(RetentionLeases::default()),
            retention_lease_ttl,
            event_sink: Mutex::new(None),
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
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
    ) -> Result<Self, ShardError> {
        let dir = config.data_dir.clone();
        let retention_lease_ttl = resolve_lease_ttl(&config);
        let translog = match &dir {
            Some(d) => translog::open_fresh(d, config.wal_sync_on_write)?,
            None => translog::null(),
        };
        let engine = Engine::open_shared_segments(
            Arc::clone(&norm),
            Arc::clone(&dict),
            tag_dict,
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
            retention_lease_ttl,
            event_sink: Mutex::new(None),
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
        tag_dict: Arc<TagDict>,
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
        let retention_lease_ttl = resolve_lease_ttl(&config);
        let translog = translog::open_existing(&dir, config.wal_sync_on_write, floor)?;
        let engine = Engine::open_shared_segments(
            Arc::clone(&norm),
            Arc::clone(&dict),
            tag_dict,
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
            retention_lease_ttl,
            event_sink: Mutex::new(None),
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
                tags,
            } => {
                if let Ok(ast) = crate::dsl::parse(dsl) {
                    let mut lc = String::new();
                    let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
                    eng.insert_extracted(&ex, *logical, *version, dsl, tags);
                }
            }
            ClusterMutation::Remove { logical } => {
                eng.delete_by_logical_id(*logical).unwrap_or(0);
            }
            // Defensive: a per-shard translog never holds an Upsert frame today — the
            // coordinator decomposes a cluster upsert into per-shard delete + insert seam
            // calls, each re-logged as its own Remove/Add record (ADR-070). Replay one
            // anyway (same delete-then-insert semantics) rather than panic on a future
            // writer that logs it whole.
            ClusterMutation::Upsert {
                logical,
                version,
                dsl,
                tags,
            } => {
                eng.delete_by_logical_id(*logical).unwrap_or(0);
                if let Ok(ast) = crate::dsl::parse(dsl) {
                    let mut lc = String::new();
                    let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
                    eng.insert_extracted(&ex, *logical, *version, dsl, tags);
                }
            }
        }
        Self::publish(&eng, &self.snapshot);
    }

    /// Bulk-ingest, infallibly — the build path uses this directly on a concrete
    /// `LocalShard` (before boxing) so `ClusterEngine::build` stays infallible. The
    /// trait's `ingest_extracted` is the `Result`-wrapped view of the same work.
    pub(crate) fn ingest_local(&self, items: &[PlacedQuery]) -> IngestReport {
        let mut eng = self.lock();
        let report = eng.ingest_extracted(items);
        Self::publish(&eng, &self.snapshot);
        // Bulk ingest writes durable segments WITHOUT riding the translog, so the
        // checkpoint sidecar must learn about them or a self-restart would attach a
        // stale registry and silently lose the bulk (ADR-072). Refresh it here,
        // PRESERVING local_checkpoint — the un-sealed translog tail is unchanged, and
        // advancing it would skip replaying live ops (a false negative).
        self.refresh_sidecar_segments(&eng);
        report
    }

    /// Refresh the durable checkpoint sidecar's segment registry after an
    /// off-translog write (bulk ingest). Best-effort like the engine's degraded
    /// paths: the segments themselves are already durable, so a failed pointer
    /// update is surfaced as a [`DurabilityFailure`](crate::events::EngineEvent)
    /// (data-at-risk: a self-restart before the next successful seal would miss
    /// the bulk) rather than failing the infallible build-path ingest.
    fn refresh_sidecar_segments(&self, eng: &Engine) {
        let Some(dir) = &self.data_dir else { return };
        let emit_fail = |detail: String, error: String| {
            self.emit(&crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::ManifestWrite,
                detail,
                error,
            });
        };
        let prev = match translog::read_sidecar(dir) {
            Ok(c) => c.map_or(0, |c| c.local_checkpoint),
            Err(e) => {
                emit_fail(
                    "reading shard.ckpt to refresh after bulk ingest".into(),
                    e.to_string(),
                );
                return;
            }
        };
        let segment_files = match eng.segment_filenames() {
            Ok(f) => f,
            Err(e) => {
                emit_fail(
                    "collecting segment filenames after bulk ingest".into(),
                    e.to_string(),
                );
                return;
            }
        };
        if let Err(e) = translog::write_sidecar(
            dir,
            &translog::ShardCheckpoint {
                next_seg_id: eng.next_seg_id(),
                local_checkpoint: prev,
                dict_fingerprint: self.dict.fingerprint(),
                segment_files,
            },
        ) {
            emit_fail("writing shard.ckpt after bulk ingest".into(), e.to_string());
        }
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
    fn percolate_filtered(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        // The coordinator already resolved `pred` against the shared frozen tag space; an empty
        // predicate is byte-identical to the unfiltered `match_title` (snapshot.rs).
        let stats = self.snapshot().match_title_filtered(
            title,
            &mut scratch,
            &mut out,
            include_broad,
            pred,
        );
        Ok((out, stats))
    }

    fn percolate_filtered_ranked(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        // ONE snapshot serves both the match and the scoring, so the tags scored are
        // exactly the tags of the copies that matched (no publish race in between).
        let snap = self.snapshot();
        let stats = snap.match_title_filtered(title, &mut scratch, &mut out, include_broad, pred);
        Ok((snap.rank(&out, spec), stats))
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

    fn live_sources_tagged(
        &self,
    ) -> Result<Vec<(u64, String, Vec<crate::tagdict::TagId>)>, ShardError> {
        Ok(self.lock().live_sources_tagged())
    }

    fn is_local(&self) -> bool {
        true
    }

    fn source_of(&self, logical: u64) -> Result<Option<String>, ShardError> {
        // Lock-free: the snapshot's query store carries the live source set (ADR-014).
        Ok(self.snapshot().get_query_source(logical))
    }

    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        Ok(self.ingest_local(items))
    }

    fn insert_extracted_with_tags(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        let mut eng = self.lock();
        // Log-first / fail-closed (ADR-039): durably record the mutation in this shard's
        // translog BEFORE applying it, under the engine lock so the log order equals the
        // apply order (a re-add then re-remove of one id must replay in the same order it
        // applied). A durable translog is the un-sealed tail a recovering peer replays; the
        // in-memory translog is a no-op. An append failure rejects the write (engine
        // untouched), mirroring the coordinator's WAL-first add_query. Raw tags ride the log
        // alongside the DSL (ADR-055) so a replayed insert re-resolves them identically.
        self.translog.append(&ClusterMutation::Add {
            logical,
            version,
            dsl: text.to_string(),
            tags: tags.to_vec(),
        })?;
        let out = eng.insert_extracted(ex, logical, version, text, tags);
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
        // Delegate to the clock-injectable core with the real wall clock. The split keeps the
        // whole seal path (including the ADR-048 lease reap) deterministically testable.
        self.seal_for_checkpoint_at(Instant::now())
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
            .acquire(at.0, Instant::now());
        Ok((id, at))
    }

    fn renew_retention_lease(&self, lease: u64, to: LogPos) -> Result<(), ShardError> {
        self.retention
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .renew(lease, to.0, Instant::now());
        Ok(())
    }

    fn release_retention_lease(&self, lease: u64) -> Result<(), ShardError> {
        self.retention
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .release(lease);
        Ok(())
    }

    // ---- observability (ADR-021/048) ----
    /// Install the coordinator's observer (fanned in by `ClusterEngine::set_observer`). Before
    /// ADR-048 a plain `LocalShard` ignored this; it now stores the sink so a TTL lease reap is
    /// observable. No pending-event buffer: a reap only fires at checkpoint time, long after an
    /// observer attaches at cluster build/open, so there is nothing to replay.
    fn set_event_sink(&self, sink: EventSink) {
        *self
            .event_sink
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(sink);
    }
}

impl LocalShard {
    /// Deliver a degraded-path event to the installed sink, if any (best-effort: dropped when no
    /// observer is attached — the default, byte-identical path). Library code never writes stderr
    /// (ADR-021); the observer turns this into logs + metrics.
    fn emit(&self, ev: &EngineEvent) {
        let sink = self
            .event_sink
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(sink) = sink {
            sink(ev);
        }
    }

    /// The clock-injectable core of [`Shard::seal_for_checkpoint`]: flush + reseal + publish, write
    /// the durable sidecar, reap any stuck retention lease as of `now` (ADR-048), then trim the
    /// translog to the retention floor (ADR-040). `now` is the wall clock the lease-TTL reap
    /// measures against; the trait method passes `Instant::now()`, while a test passes a synthetic
    /// instant to drive expiry deterministically (no sleeps). Visible within the cluster module for
    /// that test; production code always reaches it through the trait method.
    pub(in crate::cluster) fn seal_for_checkpoint_at(
        &self,
        now: Instant,
    ) -> Result<LogPos, ShardError> {
        let mut eng = self.lock();
        // Seal the memtable into a base segment; ALSO persists `sources.dat` when the
        // memtable is empty (a plain `flush` would early-return past its sources save),
        // so the on-disk source store mirrors the live set as of `p` — otherwise a
        // reopen's `live_sources` omits bulk-loaded ids / resurrects tombstone-deleted
        // ones into the vocabulary rebuild (ADR-074).
        eng.flush_and_persist_sources_for_checkpoint();
        eng.reseal_tombstoned_segments(); // bake base-segment tombstones onto disk
                                          // Fail closed (ADR-051): if the flush / reseal / sources write could not durably
                                          // persist, the on-disk state does NOT yet reflect every flushed write / applied
                                          // delete (a failed reseal keeps the original, un-baked segment). Bail BEFORE
                                          // reading `p` and trimming the translog, so its tail still carries those ops for
                                          // the next recovery — advancing the checkpoint now would let a delete resurrect
                                          // (false positive) or a write vanish on reopen. The caller treats this as a
                                          // transient failed checkpoint; the data is safe in the translog.
        if !eng.persistence_healthy {
            return Err(ShardError::Log(
                "checkpoint aborted: flush/reseal could not durably persist; translog left intact \
                 so the un-sealed tail replays on recovery"
                    .into(),
            ));
        }
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
        // Reap any stuck retention lease (ADR-048) before reading the floor: a lease that has not
        // heartbeated within the TTL belongs to a crashed/stalled recovery and must no longer pin
        // the tail (`renew` is the heartbeat, so a live recovery is never reaped). Disabled
        // (`None`) ⇒ no reap ⇒ byte-identical to ADR-040.
        //
        // Then trim the translog only to the retention floor (ADR-040): a live, heartbeating lease
        // keeps the tail a recovery still needs even though we seal here. With no lease the floor
        // is absent and this is `p` — byte-identical to ADR-039. The segments still capture every
        // op ≤ `p` (the sidecar's `local_checkpoint` is `p`); any retained ops in `(trim_to, p]`
        // are redundant with the segments and position-filtered out on replay (replay is from `p`).
        let (trim_to, reaped) = {
            let mut r = self
                .retention
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let reaped = match self.retention_lease_ttl {
                Some(ttl) => r.reap_expired(now, ttl),
                None => 0,
            };
            (r.floor().map_or(p.0, |f| p.0.min(f)), reaped)
        };
        self.translog.checkpoint(LogPos(trim_to))?;
        // Release the engine lock before emitting so a slow sink can't block other writers (the
        // emit also takes the separate event-sink lock; ordering it after the drop avoids any
        // lock-order question with the engine→retention path above).
        drop(eng);
        if reaped > 0 {
            // A reap means a recovery was abandoned — surface it (ADR-021/048) rather than
            // silently reclaiming its tail. `ReplicaDesync` (benign housekeeping ⇒ warn) is the
            // same op the handoff lease-release failure uses.
            self.emit(&EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: "expired stuck peer-recovery retention lease(s) past the TTL; a crashed \
                         or stalled recovery's translog tail is now reclaimable"
                    .into(),
                error: format!("{reaped} lease(s) reaped"),
            });
        }
        Ok(p)
    }
}
