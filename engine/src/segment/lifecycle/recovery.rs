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
            // No existing data — return a fresh engine (fresh-dir vocab path interns the
            // active equivalence forms for ID stability, exactly as `with_vocab` documents).
            return match vocab {
                Some(v) => Self::with_vocab(v, config).map_err(|e| invalid_input(&e)),
                None => Ok(Self::with_config(norm, config)),
            };
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
            observer: None,
            pending_events,
            wal,
            next_seg_id: manifest.next_seg_id,
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
        let recovery = Wal::recover(&wal_path)?;
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
                    ..
                } => {
                    // Replay without re-writing to WAL — tags included so a recovered
                    // insert keeps its metadata (ADR-049).
                    engine.replay_insert(&text, logical, version, &tags);
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
                    if seg_idx == u32::MAX || seq > manifest.wal_seq_watermark {
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
                    if seq > manifest.wal_seq_watermark {
                        engine.apply_delete_by_logical(logical);
                    }
                }
                WalEntry::FlushCheckpoint { .. } => {
                    // Skip — already handled by manifest
                }
            }
        }

        Ok(engine)
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
        Ok(Engine {
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
            observer: None,
            pending_events: Vec::new(),
            wal: None,
            next_seg_id,
            wal_healthy: true,
            persistence_healthy: true,
            skipped_segments: 0,
            query_store,
            vocab_epoch: 0,
            owns_manifest: false,
        })
    }
}
