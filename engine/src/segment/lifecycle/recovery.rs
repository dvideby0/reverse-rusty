//! `impl Engine` — crash recovery / reopen: [`open`](Engine::open) (manifest +
//! mmap'd segments + WAL replay, skip-corrupt-and-degrade) and
//! [`open_shared_segments`](Engine::open_shared_segments) (the cluster-shard
//! attach-an-explicit-file-list path, fail-loud). The construction builders live
//! in [`construct`](super::construct).

use crate::segment::{BaseSegment, Engine, Segment};
use std::sync::Arc;

use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::storage::{MmapSegment, SourceStore};
use crate::tagdict::TagDict;
use crate::wal::{Wal, WalEntry};

/// Map a [`NormalizerError`](crate::error::NormalizerError) into the `io::Result` space of
/// the open path.
fn invalid_input(e: &crate::error::NormalizerError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string())
}

fn seed_next_source_generation(
    segments: &[Arc<BaseSegment>],
    query_store: &SourceStore,
) -> std::io::Result<u64> {
    let exact_max = segments
        .iter()
        .map(|segment| segment.max_source_generation())
        .max()
        .unwrap_or(0);
    exact_max
        .max(query_store.max_source_generation())
        .checked_add(1)
        .filter(|&generation| generation != 0)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source generation space exhausted",
            )
        })
}

/// Replay the WAL tail (entries after the last flush checkpoint) into a constructed
/// engine — the ONE recovery loop, shared by the manifest path and the fresh
/// (no-manifest-yet) path so ADR-013's contract ("every acknowledged mutation
/// recovers") holds on both. `watermark` is the manifest's `wal_seq_watermark`
/// (ADR-066) — 0 on the fresh path, where nothing is baked anywhere.
fn replay_wal_tail(
    engine: &mut Engine,
    wal_path: &std::path::Path,
    watermark: u64,
) -> std::io::Result<()> {
    let recovery = Wal::recover(wal_path)?;
    if recovery.skipped_bytes > 0 {
        engine
            .pending_events
            .push(crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::WalTornTail,
                detail: "WAL recovery skipped corrupt/torn data at tail".to_string(),
                error: format!("{} bytes", recovery.skipped_bytes),
            });
    }
    for entry in recovery.entries {
        match entry {
            WalEntry::Insert {
                logical,
                version,
                text,
                tags,
                priority,
                source_generation,
                class_d_accepted,
                ..
            } => {
                // Replay without re-writing to WAL — tags included so a recovered
                // insert keeps its metadata (ADR-049). The class-D accept decision
                // is the FRAME's marker (WAL v5, ADR-068), never the live knob: an
                // op-5 frame was accepted at its write and must survive a knob
                // flip; a legacy op-0 frame may have been acknowledged as rejected
                // (pre-v5 binaries logged before classifying) and must not
                // resurrect.
                engine.replay_insert(
                    &text,
                    logical,
                    version,
                    &tags,
                    priority.map(|priority| crate::rank::RankValues { priority }),
                    source_generation,
                    class_d_accepted,
                );
            }
            WalEntry::Tombstone {
                seq,
                seg_idx,
                local_id,
            } => {
                // ADR-066: a positional frame targeting a BASE segment is valid
                // only against the segment list it was written under. Frames at or
                // below the manifest's watermark are already baked into the commit
                // (tombstone bitmap, or the entry was dropped by a merge) — and the
                // positions they address may have been renumbered since, so
                // replaying one could tombstone an unrelated query. Frames above
                // the watermark were appended against exactly the committed list
                // (every segments-vec mutation commits a manifest), so they replay
                // correctly. Memtable frames (the u32::MAX sentinel) always replay:
                // the memtable is rebuilt purely from this WAL tail.
                if seg_idx == u32::MAX || seq > watermark {
                    engine.replay_tombstone(seg_idx, local_id);
                }
            }
            WalEntry::DeleteByLogical { seq, logical } => {
                // Address-free (ADR-066): re-derive the affected copies from the
                // recovered state. Frames at/below the watermark are SKIPPED, not
                // just for economy: bulk ingest bypasses the WAL (its segment +
                // manifest commit IS its durability, ADR-017), so a same-id query
                // bulk-ingested AFTER this delete is already in the attached
                // segments — replaying the older delete over it would erase the
                // newer query (codex P1). The manifest commit that covered this
                // frame also baked its tombstones, so skipping loses nothing.
                if seq > watermark {
                    engine.apply_delete_by_logical(logical);
                }
            }
            WalEntry::Upsert {
                seq,
                logical,
                version,
                text,
                tags,
                priority,
                source_generation,
                class_d_accepted,
            } => {
                // ADR-067: the insert half ALWAYS replays — the new memtable copy
                // exists only in this frame (a flush would have reset the WAL and
                // dropped it). The segment-tombstone half follows the watermark
                // rule (baked bitmaps below it; and a same-id bulk ingest after
                // the frame must not be erased), while prior MEMTABLE copies are
                // always re-tombstoned — they are WAL-truth, recreated by earlier
                // replayed frames. See `apply_upsert`. `class_d_accepted` is the
                // frame's marker (op 6, ADR-068): a legacy op-4 frame replays
                // under the old reject gate, so a logged-but-rejected class-D
                // upsert can never tombstone the acknowledged-live prior version.
                engine.replay_upsert(
                    &text,
                    logical,
                    version,
                    &tags,
                    priority.map(|priority| crate::rank::RankValues { priority }),
                    source_generation,
                    seq > watermark,
                    class_d_accepted,
                );
            }
            WalEntry::FlushCheckpoint { .. } => {
                // Skip — already handled by manifest
            }
        }
    }
    Ok(())
}

impl Engine {
    /// Open an engine from an existing data directory, recovering state from
    /// the manifest and WAL. The normalizer must be the same one used when the
    /// engine was originally built (feature spaces must align).
    ///
    /// **If the engine was built with a [`Vocab`](crate::vocab::Vocab), prefer
    /// [`open_with_vocab`](Self::open_with_vocab)**: the equivalence map (ADR-054) is
    /// transient — never persisted in the dict — and the WAL tail is recompiled HERE,
    /// so opening with the bare normalizer and adopting the vocab afterwards would
    /// compile those recovered queries without alias expansion (`adopt_vocab` detects
    /// that hazard and escalates to a full recompile, codex R13).
    pub fn open(norm: Normalizer, config: EngineConfig) -> std::io::Result<Self> {
        Self::open_inner(norm, config, None)
    }

    /// [`open`](Self::open) for a vocab-built engine: rebuilds the normalizer FROM the
    /// vocab and installs its equivalence groups (ADR-054) on the recovered dict **before**
    /// the WAL tail is replayed — the same order the cluster's `ClusterEngine::open` uses —
    /// so queries written after the last flush recover with their alias expansion intact
    /// (codex R13). Resolution is read-only against the recovered dict (no interning), the
    /// recovered-engine ID-stability rule of [`adopt_vocab`](Self::adopt_vocab); a missing
    /// manifest falls back to a fresh [`with_vocab`](Self::with_vocab) build (which interns).
    pub fn open_with_vocab(
        vocab: crate::vocab::Vocab,
        config: EngineConfig,
    ) -> std::io::Result<Self> {
        let norm = vocab.to_normalizer().map_err(|e| invalid_input(&e))?;
        Self::open_inner(norm, config, Some(vocab))
    }

    fn open_inner(
        norm: Normalizer,
        config: EngineConfig,
        vocab: Option<crate::vocab::Vocab>,
    ) -> std::io::Result<Self> {
        let dir = config.data_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "data_dir required for open",
            )
        })?;

        let manifest_path = dir.join("manifest.bin");
        if !manifest_path.exists() {
            // No manifest yet — construct fresh (fresh-dir vocab path interns the
            // active equivalence forms for ID stability, exactly as `with_vocab`
            // documents), then REPLAY any existing WAL tail. A crash before the
            // FIRST manifest commit (no flush/bulk/build yet) leaves acknowledged
            // writes only in wal.log; skipping the replay here silently lost them
            // (the engine came up empty) — voiding ADR-013's recovery contract on
            // exactly the start-empty-and-PUT path a fresh server runs.
            let fresh_wal_path = dir.join("wal.log");
            let mut engine = match vocab {
                Some(v) => Self::with_vocab(v, config).map_err(|e| invalid_input(&e))?,
                None => Self::with_config(norm, config),
            };
            if fresh_wal_path.exists() {
                // Watermark 0: with no manifest, nothing is baked anywhere.
                replay_wal_tail(&mut engine, &fresh_wal_path, 0)?;
            }
            return Ok(engine);
        }

        let manifest = crate::storage::read_manifest(&manifest_path)?;
        let dict = crate::storage::deserialize_dict(&manifest.dict_data)?;
        // The frozen tag space (ADR-049); empty for a v1 manifest (no tags).
        let tag_dict = crate::storage::deserialize_tagdict(&manifest.tag_dict_data)?;

        // Open mmap'd segments (skip corrupt ones rather than failing startup)
        let seg_dir = dir.join("segments");
        let mut segments = Vec::with_capacity(manifest.segment_files.len());
        let mut skipped_segments = 0usize;
        // Recovery diagnostics raised here predate any observer; buffer them for
        // delivery on `set_observer` (see `pending_events`).
        let mut pending_events = Vec::new();
        for name in &manifest.segment_files {
            let seg_path = seg_dir.join(name);
            match MmapSegment::open(&seg_path) {
                Ok(mut mmap_seg) => {
                    // ADR-066: restore the segment's committed tombstone state. The
                    // on-disk alive flags are frozen at write time; deletes applied
                    // since live only in this manifest-carried bitmap (their WAL
                    // frames may have been dropped by a flush-time reset).
                    if let Some((_, bytes)) = manifest
                        .segment_tombstones
                        .iter()
                        .find(|(file, _)| file == name)
                    {
                        match roaring::RoaringBitmap::deserialize_from(&bytes[..]) {
                            Ok(dead) => {
                                for local in dead {
                                    // Out-of-range ids no-op inside `tombstone` —
                                    // never a wrong tombstone.
                                    mmap_seg.tombstone(local);
                                }
                            }
                            Err(e) => {
                                // Apply nothing rather than guess: a resurrected
                                // delete is a bounded false positive; a wrong
                                // tombstone would be a false negative.
                                pending_events.push(
                                    crate::events::EngineEvent::DurabilityFailure {
                                        op: crate::events::DurabilityOp::SegmentRecovery,
                                        detail: format!(
                                            "corrupt tombstone bitmap for {name}; its baked \
                                             deletes are not restored (entries may resurrect)"
                                        ),
                                        error: e.to_string(),
                                    },
                                );
                            }
                        }
                    }
                    segments.push(Arc::new(BaseSegment::Mmap(mmap_seg)));
                }
                Err(e) => {
                    pending_events.push(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::SegmentRecovery,
                        detail: format!(
                            "skipping corrupt segment {} during recovery",
                            seg_path.display()
                        ),
                        error: e.to_string(),
                    });
                    skipped_segments += 1;
                }
            }
        }

        // Open WAL and replay
        let wal_path = dir.join("wal.log");
        let mut wal_file = Wal::open(&wal_path, config.wal_sync_on_write)?;
        // ADR-066: a reset (header-only) WAL rescans to seq 1, but the manifest
        // keeps its watermark — pin the sequence past it so frames appended after
        // this reopen can never sort at/below the watermark and be skipped by the
        // NEXT recovery (which would resurrect an acknowledged delete).
        wal_file.ensure_seq_after(manifest.wal_seq_watermark);
        let wal = Some(wal_file);

        // Load persisted query sources — resident, or lazily mmap'd per
        // config.retain_source (ADR-020 Item 1).
        let sources_path = dir.join("sources.dat");
        let query_store =
            match crate::storage::SourceStore::open(&sources_path, config.retain_source) {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    // An absent file yields an empty store; an error here means a
                    // corrupt sources.dat — surface it (display-only data) rather
                    // than silently dropping all query _source data.
                    pending_events.push(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::SourceStoreLoad,
                        detail: format!(
                            "failed to load query sources from {} — _source will be \
                             unavailable for recovered queries",
                            sources_path.display()
                        ),
                        error: e.to_string(),
                    });
                    Arc::new(crate::storage::SourceStore::empty(config.retain_source))
                }
            };
        let next_source_generation = seed_next_source_generation(&segments, &query_store)?;

        let mut engine = Engine {
            config: Arc::new(config),
            norm: Arc::new(norm),
            vocab: None,
            dict: Arc::new(dict),
            tag_dict: Arc::new(tag_dict),
            segments,
            memtable: Arc::new(Segment::new()),
            rejected_parse: manifest.rejected_parse,
            rejected_class_d: manifest.rejected_class_d,
            // Process-lifetime observe counter (deliberately not in the manifest);
            // the WAL-tail replay below re-counts the tail's compiles.
            would_be_hot: 0,
            bodies_total: 0,
            dup_joined: 0,
            dup_sketch: None,
            observer: None,
            pending_events,
            wal,
            next_seg_id: manifest.next_seg_id,
            next_source_generation,
            wal_healthy: true,
            persistence_healthy: skipped_segments == 0,
            skipped_segments,
            query_store,
            vocab_epoch: 0,
            owns_manifest: true,
        };

        // Install the vocab BEFORE the WAL replay below (codex R13): the replay recompiles the
        // tail queries from raw text, and without the equivalence map installed they would
        // compile unexpanded — a recovery false negative. Resolution is read-only against the
        // recovered dict (no interning — the recovered-engine ID-stability rule, see
        // `adopt_vocab`); stale-active aliases the live normalizer cannot express are demoted
        // first, exactly as every other install seam does.
        if let Some(mut v) = vocab {
            let dict = Arc::make_mut(&mut engine.dict);
            if v.aliases_mut().demote_unexpressible(&engine.norm, dict) > 0 {
                engine.norm = Arc::new(v.to_normalizer().map_err(|e| invalid_input(&e))?);
            }
            let equiv = v.resolve_equivalences(&engine.norm, dict);
            dict.set_equivalences(equiv);
            engine.vocab = Some(Arc::new(v));
        }

        // Replay WAL entries after last checkpoint
        replay_wal_tail(&mut engine, &wal_path, manifest.wal_seq_watermark)?;

        // ADR-118 compiler-semantics migration: joining positive bare terms
        // across an intervening clause could change any context-sensitive query
        // normalization (phrases, grader state, number context, aliases, ...).
        // Rebuild every legacy live materialization from retained `_source`
        // before returning an engine that could serve it. The segment header
        // stamp makes this idempotent; a missing/inconsistent source sidecar or
        // failed durable commit refuses startup rather than retaining a silent
        // false negative.
        engine.migrate_legacy_clause_boundary_semantics()?;

        Ok(engine)
    }

    /// Whether any live row is materialized under the pre-ADR-118 AST lowering.
    pub(crate) fn has_legacy_compiler_segments(&self) -> bool {
        let current = crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION;
        self.segments.iter().any(|segment| {
            segment.alive_count() != 0 && segment.compiler_semantics_version() < current
        }) || (!self.memtable.is_empty() && self.memtable.compiler_semantics_version() < current)
    }

    /// Whether serving this engine requires the ADR-118 source-driven compiler
    /// migration. Every live semantics-v0 row is suspect: the old cross-clause
    /// stream could affect ordinary phrase consumption, grader state, or number
    /// context even when no alias is installed.
    pub(crate) fn needs_clause_boundary_compiler_migration(&self) -> bool {
        self.has_legacy_compiler_segments()
    }

    /// Standalone upgrade path for ADR-118. The normalizer and dict do not
    /// change, but every live source must be re-lowered so clause boundaries are
    /// reflected in exact predicates, signatures, and placement.
    pub(crate) fn migrate_legacy_clause_boundary_semantics(&mut self) -> std::io::Result<()> {
        if !self.needs_clause_boundary_compiler_migration() {
            return Ok(());
        }
        if !self.owns_manifest {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot migrate legacy compiler semantics inside one cluster shard: query \
                 placement must be rebuilt and committed by the coordinator",
            ));
        }

        let live = self.live_source_documents_tagged().map_err(|logical| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cannot migrate legacy compiler semantics: live query {logical} has no \
                     matching retained source document"
                ),
            )
        })?;

        // A legacy joint stream may have consumed component features that the
        // persisted standalone dict never interned. Recompiling read-only would
        // freeze those newly exposed names as synthetic IDs; a later ordinary
        // standalone insert would intern the same name densely, making titles
        // resolve dense while the migrated row still required synthetic. Build
        // an append-only candidate dict off to the side by running the current
        // mutable extractor over the complete live corpus. Existing IDs and
        // frozen mask bits stay fixed. Existing frequencies are restored after
        // the discovery pass; newly exposed features retain their corpus counts.
        let mut proposed_dict = self.dict.as_ref().clone();
        let old_len = proposed_dict.len();
        let old_freqs: Vec<u32> = (0..old_len)
            .map(|id| proposed_dict.freq(id as crate::dict::FeatureId))
            .collect();
        let old_masks: Vec<u8> = (0..old_len)
            .map(|id| proposed_dict.mask_bit(id as crate::dict::FeatureId))
            .collect();
        let mut lc = String::new();
        for (logical, text, ..) in &live {
            let ast = crate::dsl::parse(text).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "cannot migrate legacy compiler semantics: stored query {logical} \
                         no longer parses: {error}"
                    ),
                )
            })?;
            let ex = crate::compile::extract(&ast, &self.norm, &mut proposed_dict, &mut lc);
            if let Some(width) = ex.column_overflow() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "cannot migrate legacy compiler semantics: stored query {logical} \
                         exceeds the exact-store column limit ({width} features)"
                    ),
                ));
            }
        }
        for id in 0..old_len {
            proposed_dict.set_freq_and_mask(
                id as crate::dict::FeatureId,
                old_freqs[id],
                old_masks[id],
            );
        }
        // Newly interned equivalence members must be keyed by their new dense
        // IDs before the read-only materialization pass below.
        if let Some(vocab) = self.vocab.as_deref() {
            let equiv = vocab.resolve_equivalences(&self.norm, &proposed_dict);
            proposed_dict.set_equivalences(equiv);
        }
        self.dict = Arc::new(proposed_dict);

        let previous_epoch = self.vocab_epoch;
        self.vocab_epoch = self.vocab_epoch.checked_add(1).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot migrate legacy compiler semantics: vocab epoch exhausted",
            )
        })?;
        let rebuilt = self.recompile_stale_segments();

        if rebuilt != live.len() || self.has_legacy_compiler_segments() || !self.persistence_healthy
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "legacy compiler-semantics migration did not commit completely \
                     (expected {} live queries, rebuilt {rebuilt}, persistence_healthy={})",
                    live.len(),
                    self.persistence_healthy
                ),
            ));
        }

        // The epoch was only a process-local trigger for a same-normalizer
        // compiler migration. Restore it so introspection does not report a
        // vocabulary change; the durable idempotency marker is the segment's
        // compiler-semantics header word.
        self.vocab_epoch = previous_epoch;
        Arc::make_mut(&mut self.memtable).vocab_epoch = previous_epoch;
        for segment in &mut self.segments {
            Arc::make_mut(segment).set_vocab_epoch(previous_epoch);
        }
        Ok(())
    }

    /// Reopen a **cluster-shard** engine (ADR-032) by attaching an EXPLICIT list of
    /// committed segment files against the SUPPLIED shared dict — no per-shard manifest,
    /// no dict deserialize, no WAL. The coordinator supplies `files` (relative `.seg`
    /// names under `config.data_dir/segments/`) and `next_seg_id` from its
    /// `cluster_manifest.bin`, having already fingerprint-checked the dict. This is
    /// attach-and-mmap, NOT re-ingest: the compiled segments ARE the materialized base.
    ///
    /// Fails LOUD (returns `Err`) on any missing or CRC-corrupt segment — deliberately
    /// unlike [`open`](Self::open), which skips corrupt segments and degrades. A skipped
    /// shard segment is a silent, shard-sized false negative, which the cluster's
    /// zero-false-negative contract forbids; the caller surfaces the error instead.
    pub fn open_shared_segments(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
    ) -> std::io::Result<Self> {
        Self::open_shared_segments_inner(norm, dict, tag_dict, config, files, next_seg_id, false)
    }

    /// Coordinator-only attach seam used while an old durable cluster is being
    /// opened and immediately blue/green rebuilt under an atomic cluster
    /// manifest commit. Every other shared-segment attach refuses any live
    /// legacy compiler materialization.
    pub(crate) fn open_shared_segments_for_compiler_migration(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
    ) -> std::io::Result<Self> {
        Self::open_shared_segments_inner(norm, dict, tag_dict, config, files, next_seg_id, true)
    }

    #[allow(clippy::too_many_arguments)]
    fn open_shared_segments_inner(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
        allow_legacy_compiler_semantics: bool,
    ) -> std::io::Result<Self> {
        let dir = config.data_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "open_shared_segments requires config.data_dir",
            )
        })?;
        Self::init_segments_dir(dir)?;
        let seg_dir = dir.join("segments");
        let mut segments = Vec::with_capacity(files.len());
        for name in files {
            // Fail loud: a missing / CRC-corrupt committed segment is a false-negative risk.
            let mmap_seg = MmapSegment::open(&seg_dir.join(name))?;
            segments.push(Arc::new(BaseSegment::Mmap(mmap_seg)));
        }
        let query_store = Arc::new(SourceStore::open(
            &dir.join("sources.dat"),
            config.retain_source,
        )?);
        let next_source_generation = seed_next_source_generation(&segments, &query_store)?;
        let engine = Engine {
            config: Arc::new(config),
            norm,
            vocab: None,
            dict,
            // The cluster shard shares the coordinator's frozen tag space (ADR-049/055): the
            // attached segments already carry resolved `TagId`s, and this shared dict resolves any
            // later live-add / translog-replayed tags consistently. Empty ⇒ untagged cluster.
            tag_dict,
            segments,
            memtable: Arc::new(Segment::new()),
            rejected_parse: 0,
            rejected_class_d: 0,
            would_be_hot: 0,
            bodies_total: 0,
            dup_joined: 0,
            dup_sketch: None,
            observer: None,
            pending_events: Vec::new(),
            wal: None,
            next_seg_id,
            next_source_generation,
            wal_healthy: true,
            persistence_healthy: true,
            skipped_segments: 0,
            query_store,
            vocab_epoch: 0,
            owns_manifest: false,
        };
        if !allow_legacy_compiler_semantics && engine.needs_clause_boundary_compiler_migration() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "legacy compiler semantics require an atomic source-driven rebuild and \
                 re-placement; reopen through ClusterEngine or recover this shard from a \
                 current peer",
            ));
        }
        Ok(engine)
    }
}

#[cfg(test)]
mod compiler_migration_tests {
    use super::*;
    use crate::segment::MatchScratch;

    fn scratch_dir() -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "reverse_rusty_clause_migration_ids_{}_{}",
            std::process::id(),
            nonce
        ))
    }

    fn stamp_legacy(path: &std::path::Path) {
        let mut bytes = std::fs::read(path).expect("read segment");
        bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
        let body = bytes.len() - 4;
        let crc = crate::storage::crc32(&bytes[..body]);
        bytes[body..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(path, bytes).expect("write legacy stamp");
    }

    fn matches(engine: &Engine, title: &str, logical: u64) -> bool {
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        engine.match_title(title, &mut scratch, &mut out, true);
        out.contains(&logical)
    }

    #[test]
    fn migration_interns_features_exposed_by_splitting_the_legacy_stream() {
        let dir = scratch_dir();
        let config = EngineConfig {
            data_dir: Some(dir.clone()),
            ..EngineConfig::default()
        };
        let mut vocab = crate::vocab::Vocab::new();
        vocab.import_solr_aliases(
            "ny => new york",
            &Normalizer::default_vocab().expect("normalizer"),
            &Dict::new(),
        );

        {
            let mut engine =
                Engine::with_vocab(vocab.clone(), config.clone()).expect("vocab engine");
            // This exact plan contains only the collapsed alias entity, matching
            // what the legacy cross-clause stream produced.
            engine.build_from_queries(&[(1, "new york".to_string())]);
            assert!(engine.dict().get("term:new").is_none());
            assert!(engine.dict().get("term:york").is_none());

            // Retain the same exact-row metadata but substitute the true source
            // predicate that the legacy compiler mis-lowered.
            let source = engine
                .snapshot()
                .get_query_document(1)
                .expect("source metadata");
            engine.query_store.insert_document_with_generation(
                1,
                "new -used york".to_string(),
                source.version(),
                source.source_generation(),
                source.tags(),
            );
            engine.save_query_sources();
        }

        let manifest = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
        for name in &manifest.segment_files {
            stamp_legacy(&dir.join("segments").join(name));
        }

        let mut reopened = Engine::open_with_vocab(vocab, config).expect("source-driven migration");
        let new_id = reopened
            .dict()
            .get("term:new")
            .expect("newly exposed term is interned");
        assert!(
            reopened.dict().get("term:york").is_some(),
            "every separated component must be dense before commit"
        );
        assert!(matches(&reopened, "new vintage collectible york", 1));

        // A later standalone insert uses the same dense ID; it cannot turn the
        // migrated row's synthetic feature into an unreachable split brain.
        reopened
            .try_insert_live("new", 2, 1)
            .expect("post-migration insert");
        assert_eq!(reopened.dict().get("term:new"), Some(new_id));
        assert!(matches(&reopened, "new vintage collectible york", 1));

        drop(reopened);
        std::fs::remove_dir_all(dir).expect("cleanup");
    }
}
