//! `impl ClusterEngine` — construction + durability lifecycle: initial `build`, the
//! `from_parts` assembly seam, durable-base commit, crash-recovery `open`, `checkpoint`,
//! and the `epoch` / orphan-GC helpers.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::cluster::clog::{FileClusterLog, LogPos};
use crate::cluster::control::InMemoryControlPlane;
use crate::cluster::ring::HashRing;
use crate::cluster::shard::{LocalShard, Shard, ShardError};
use crate::compile::{extract, Extracted};
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;

use super::{
    into_shard, placement_of, replica_dir, shard_dir, ClusterConfig, ClusterDurable, ClusterEngine,
    Target, CLUSTER_LOG_FILE, CLUSTER_MANIFEST_FILE,
};

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
            None => ClusterDurable::in_memory(
                config.num_shards as u32,
                config.vnodes,
                dict.fingerprint(),
            ),
        };

        // Assemble each position into a shard: a bare `LocalShard` at RF=1, else a
        // `ReplicatedShard` composite over the primary + replicas.
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(config.num_shards);
        for copies in groups {
            shards.push(into_shard(copies)?);
        }
        Self::from_parts(
            norm,
            dict,
            ring,
            shards,
            config.include_broad,
            config.replication_factor,
            config.per_shard.clone(),
            durable,
        )
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
            // A freshly built cluster has no runtime vocabulary change yet; a
            // declared alias lands here on a later `set_vocab` → `checkpoint`.
            vocab_data: Vec::new(),
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
            control: Box::new(InMemoryControlPlane::single_node(
                ring.num_shards() as u32,
                config.vnodes,
                dict.fingerprint(),
            )),
        })
    }

    /// Assemble a cluster from pre-built parts — the construction seam shared by
    /// [`Self::build`] (which supplies `LocalShard`s) and the distributed builder /
    /// gRPC integration test (which supply boxed `RemoteShard`s). `shards.len()` must
    /// equal `ring.num_shards()`.
    // The internal assembly seam genuinely takes many independent parts (feature
    // space, ring, shards, the broad toggle, the rebuild config, and the durability
    // bundle); grouping them further would only obscure the construction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        ring: HashRing,
        shards: Vec<Box<dyn Shard>>,
        include_broad: bool,
        replication_factor: usize,
        per_shard: crate::config::EngineConfig,
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
            vocab: None,
            ring,
            shards,
            include_broad,
            replication_factor: replication_factor.max(1),
            per_shard,
            log: durable.log,
            epoch: AtomicU64::new(durable.epoch),
            vnodes: durable.vnodes,
            data_dir: durable.data_dir,
            control: durable.control,
            observer: Mutex::new(None),
            pending_events: Mutex::new(Vec::new()),
            pending_repair: Mutex::new(std::collections::BTreeMap::new()),
            // No position is handoff-wrapped by default; the gRPC builders install handles via
            // `with_handoffs`. Empty here ⇒ the in-process/default path is byte-identical (ADR-043).
            #[cfg(feature = "distributed")]
            handoffs: Vec::new(),
            // Handoff drain caps default here (the in-process path never hands off); the gRPC
            // builders override them from `ClusterConfig` via `with_handoff_caps` (ADR-044/048).
            #[cfg(feature = "distributed")]
            handoff_drain_passes: ClusterConfig::DEFAULT_HANDOFF_DRAIN_PASSES,
            #[cfg(feature = "distributed")]
            handoff_final_drain_cap: ClusterConfig::DEFAULT_HANDOFF_FINAL_DRAIN_CAP,
            // No runtime handle on the in-process path (it never hands off); the gRPC builders set
            // it via `with_handle` so the autoscaler can drive `execute_handoff` (ADR-048).
            #[cfg(feature = "distributed")]
            handle: None,
        })
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
        // ADR-046: if a runtime vocabulary change was committed, the manifest carries
        // the serialized vocab — rebuild the normalizer from IT (authoritative over the
        // caller-supplied one) so a declared alias survives the restart, and retain the
        // vocab so a later checkpoint re-persists it (else the next reopen would lose it).
        let restored_vocab = if manifest.vocab_data.is_empty() {
            None
        } else {
            let json = std::str::from_utf8(&manifest.vocab_data)
                .map_err(|e| ShardError::Config(format!("cluster vocab not utf-8: {e}")))?;
            let v = crate::vocab::Vocab::from_json(json)
                .map_err(|e| ShardError::Config(format!("deserializing cluster vocab: {e}")))?;
            Some(Arc::new(v))
        };
        let norm = match &restored_vocab {
            Some(v) => v.to_normalizer().map_err(|e| {
                ShardError::Config(format!("building normalizer from cluster vocab: {e}"))
            })?,
            None => norm,
        };
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
                // The high-water is irrelevant here: at open there are no concurrent writes,
                // so the primary's translog tail is empty and this peer_recover is a pure
                // segment copy; the coordinator-log replay below repopulates all copies.
                let (replica, _hwm) = crate::cluster::replica::peer_recover(
                    &norm,
                    &dict,
                    per_shard.clone(),
                    &primary,
                    &primary_dir,
                    &replica_dir(&data_dir, s, r),
                )?;
                recovered.push(replica);
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
            control: Box::new(InMemoryControlPlane::single_node(
                manifest.num_shards,
                manifest.vnodes,
                manifest.dict_fingerprint,
            )),
        };
        let mut engine = Self::from_parts(
            norm,
            dict,
            ring,
            shards,
            manifest.include_broad,
            rf,
            per_shard,
            durable,
        )?;
        // Retain the vocab restored from the manifest so a later checkpoint re-persists it.
        engine.vocab = restored_vocab;

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
        //    Persist the installed vocab (ADR-046) so a runtime alias survives reopen;
        //    serialization failure fails the checkpoint loudly rather than silently
        //    dropping the alias (which would be a false negative on the next open).
        let vocab_data = match &self.vocab {
            Some(v) => v
                .to_json()
                .map_err(|e| ShardError::Log(format!("serializing cluster vocab: {e}")))?
                .into_bytes(),
            None => Vec::new(),
        };
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
            vocab_data,
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
}
