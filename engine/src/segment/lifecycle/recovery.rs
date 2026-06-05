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

impl Engine {
    /// Open an engine from an existing data directory, recovering state from
    /// the manifest and WAL. The normalizer must be the same one used when the
    /// engine was originally built (feature spaces must align).
    pub fn open(norm: Normalizer, config: EngineConfig) -> std::io::Result<Self> {
        let dir = config.data_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "data_dir required for open",
            )
        })?;

        let manifest_path = dir.join("manifest.bin");
        if !manifest_path.exists() {
            // No existing data — return a fresh engine
            return Ok(Self::with_config(norm, config));
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
                Ok(mmap_seg) => segments.push(Arc::new(BaseSegment::Mmap(mmap_seg))),
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
        let wal = Some(Wal::open(&wal_path, config.wal_sync_on_write)?);

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
                    seg_idx, local_id, ..
                } => {
                    engine.replay_tombstone(seg_idx, local_id);
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
