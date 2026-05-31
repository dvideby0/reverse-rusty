//! `impl Engine` — construction, configuration, vocabulary, crash recovery
//! (`open`), the observer hook, and engine-handle accessors. Heavier lifecycle
//! work (ingest, flush/compaction, persistence) lives in sibling submodules.

use super::{BaseSegment, Engine, EngineSnapshot, Segment};
use std::sync::Arc;

use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::storage::{MmapSegment, SourceStore};
use crate::wal::{Wal, WalEntry};

impl Engine {
    /// Create an engine with default configuration.
    pub fn new(norm: Normalizer) -> Self {
        Self::with_config(norm, EngineConfig::default())
    }

    /// Create an engine with explicit configuration. If `config.data_dir` is set,
    /// initializes the data directory and WAL.
    pub fn with_config(norm: Normalizer, config: EngineConfig) -> Self {
        Self::with_shared(Arc::new(norm), Arc::new(Dict::new()), config)
    }

    /// Create an engine that SHARES a pre-built normalizer and dictionary (by
    /// `Arc`) instead of owning fresh ones. This is how a cluster shard is built:
    /// every shard shares the coordinator's one authoritative, already-finalized
    /// `Dict` so `FeatureId`s / `sig_key`s / hotness are globally consistent (see
    /// [`crate::cluster`]). The dict must be treated as frozen — shard ingest uses
    /// the read-only `*_extracted` paths so it is never `Arc::make_mut`'d (which
    /// would fork it and break cross-shard agreement).
    pub fn with_shared(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        let mut wal_healthy = true;
        // Diagnostics raised here predate any observer (it is attached after
        // construction via `set_observer`), so they are buffered and replayed on
        // attach rather than dropped — see `pending_events` / `set_observer`.
        let mut pending_events = Vec::new();
        let wal = if let Some(ref dir) = config.data_dir {
            match Self::init_data_dir(dir, config.wal_sync_on_write) {
                Ok(wal) => Some(wal),
                Err(e) => {
                    // A configured data_dir means durability was requested. If we
                    // cannot create it or open the WAL, do NOT silently run without
                    // durability: mark the engine unhealthy (surfaced via /_health)
                    // and emit a DurabilityFailure so ops can alert.
                    wal_healthy = false;
                    pending_events.push(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::WalInit,
                        detail: format!(
                            "failed to initialize data dir / WAL at {} — running WITHOUT \
                             durability (writes will not survive restart)",
                            dir.display()
                        ),
                        error: e.to_string(),
                    });
                    None
                }
            }
        } else {
            None
        };
        let query_store = Arc::new(SourceStore::empty(config.retain_source));
        Engine {
            config: Arc::new(config),
            norm,
            vocab: None,
            dict,
            segments: Vec::new(),
            memtable: Arc::new(Segment::new()),
            rejected_parse: 0,
            rejected_class_d: 0,
            observer: None,
            pending_events,
            wal,
            next_seg_id: 1,
            wal_healthy,
            persistence_healthy: wal_healthy,
            skipped_segments: 0,
            query_store,
            vocab_epoch: 0,
        }
    }

    /// Create the data directory (and its `segments` subdirectory) and open the
    /// WAL. Returns an error if any filesystem operation fails so callers can
    /// surface loss of durability instead of silently running without a WAL.
    fn init_data_dir(dir: &std::path::Path, wal_sync_on_write: bool) -> std::io::Result<Wal> {
        std::fs::create_dir_all(dir)?;
        std::fs::create_dir_all(dir.join("segments"))?;
        Wal::open(&dir.join("wal.log"), wal_sync_on_write)
    }

    /// Create an engine from a [`Vocab`](crate::vocab::Vocab), which is
    /// converted to a Normalizer internally. The vocab is stored so it can
    /// be queried or serialized later.
    pub fn with_vocab(
        vocab: crate::vocab::Vocab,
        config: EngineConfig,
    ) -> Result<Self, crate::error::NormalizerError> {
        let norm = vocab.to_normalizer()?;
        let mut eng = Self::with_config(norm, config);
        eng.vocab = Some(Arc::new(vocab));
        Ok(eng)
    }

    /// The vocabulary used to build this engine's normalizer, if one was set.
    pub fn vocab(&self) -> Option<&crate::vocab::Vocab> {
        self.vocab.as_deref()
    }

    /// Replace the engine's vocabulary and normalizer. Existing compiled
    /// queries become stale — the caller must reingest for consistent matching.
    /// Returns the number of stale segments that need reingestion.
    pub fn set_vocab(
        &mut self,
        vocab: crate::vocab::Vocab,
    ) -> Result<usize, crate::error::NormalizerError> {
        self.norm = Arc::new(vocab.to_normalizer()?);
        self.vocab = Some(Arc::new(vocab));
        self.vocab_epoch += 1;
        Ok(self.stale_segment_count())
    }

    /// Number of base segments compiled against an older vocab epoch.
    pub fn stale_segment_count(&self) -> usize {
        let current = self.vocab_epoch;
        self.segments
            .iter()
            .filter(|s| s.vocab_epoch() < current)
            .count()
            + usize::from(self.memtable.vocab_epoch < current && !self.memtable.is_empty())
    }

    /// True if any segment was compiled with a different normalizer than the
    /// current one. Matching still works (no panic) but may produce incorrect
    /// results until stale queries are reingested.
    pub fn has_stale_segments(&self) -> bool {
        self.stale_segment_count() > 0
    }

    /// The current vocab epoch. Segments compiled at this epoch are up-to-date.
    pub fn vocab_epoch(&self) -> u64 {
        self.vocab_epoch
    }

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
                    ..
                } => {
                    // Replay without re-writing to WAL
                    engine.replay_insert(&text, logical, version);
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

    /// Set an observer callback that receives [`EngineEvent`](crate::events::EngineEvent)s
    /// for flush, compaction, ingest, and other lifecycle events. The callback
    /// must be `Send + Sync` (safe to call from rayon threads). Pass `None` to
    /// clear a previously set observer.
    ///
    /// Any events buffered during construction/recovery (e.g. a
    /// [`DurabilityFailure`](crate::events::EngineEvent::DurabilityFailure) from a
    /// corrupt segment skipped in [`open`](Self::open)) are delivered to the
    /// observer synchronously here, before this returns, then cleared — so an
    /// operator who wires the observer right after `open` still sees the recovery
    /// diagnostics through the structured stack.
    pub fn set_observer<F: Fn(&crate::events::EngineEvent) + Send + Sync + 'static>(
        &mut self,
        observer: F,
    ) {
        self.observer = Some(Box::new(observer));
        if !self.pending_events.is_empty() {
            let pending = std::mem::take(&mut self.pending_events);
            if let Some(ref cb) = self.observer {
                for ev in &pending {
                    cb(ev);
                }
            }
        }
    }

    /// Clear the observer callback.
    pub fn clear_observer(&mut self) {
        self.observer = None;
    }

    /// Emit an event to the observer (if set). No-op when no observer is registered.
    // The event is built at the call site solely to be emitted, then dropped; taking
    // it by value (vs `&`) costs nothing and keeps every call site free of `&` noise.
    #[allow(clippy::needless_pass_by_value)]
    #[inline]
    pub(in crate::segment) fn emit(&self, event: crate::events::EngineEvent) {
        if let Some(ref cb) = self.observer {
            cb(&event);
        }
    }

    /// Read-only access to the current configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Replace the runtime tuning configuration (copy-on-write: swaps in a new
    /// `Arc`, so any already-published snapshot keeps its own view).
    ///
    /// Only the **dynamic** knobs take effect retroactively — compaction/flush
    /// thresholds, query-complexity limits, merge cost, and the auto-compact
    /// flags are re-read on the next maintenance decision. The **static** fields
    /// (`data_dir`, `wal_sync_on_write`, `retain_source`) are bound at
    /// construction — the data dirs, WAL fsync policy, and source-store mode are
    /// already established — so they must equal the current values; changing them
    /// here has no retroactive effect and may split on-disk state. The server's
    /// `PUT /_settings` enforces this by rejecting those keys as non-dynamic.
    pub fn set_config(&mut self, config: EngineConfig) {
        self.config = Arc::new(config);
    }

    /// Create an immutable [`EngineSnapshot`] of the current read-path state.
    ///
    /// This is O(number of base segments) pointer copies, *not* O(corpus): the
    /// normalizer, dictionary, each base segment, the memtable, and the query
    /// store are all shared structurally via `Arc` (segments by per-segment
    /// pointer; the dict/memtable copy-on-write on the next write). Publishing a
    /// snapshot after every mutation is therefore cheap — the deep-clone-the-whole-
    /// engine cost the audit flagged (P1-16) is gone. Readers match against the
    /// snapshot without holding any lock on the engine.
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            norm: Arc::clone(&self.norm),
            dict: Arc::clone(&self.dict),
            segments: self.segments.clone(),
            memtable: Arc::clone(&self.memtable),
            query_store: Arc::clone(&self.query_store),
            vocab: self.vocab.clone(),
            config: Arc::clone(&self.config),
            rejected_parse: self.rejected_parse,
            rejected_class_d: self.rejected_class_d,
            vocab_epoch: self.vocab_epoch,
            wal_healthy: self.wal_healthy,
            persistence_healthy: self.persistence_healthy,
            skipped_segments: self.skipped_segments,
            wal_size_bytes: self.wal.as_ref().map_or(0, Wal::size_bytes),
            wal_pending_entries: self.wal.as_ref().map_or(0, Wal::pending_entries),
        }
    }

    /// Read-only access to the shared feature dictionary.
    pub fn dict(&self) -> &Dict {
        &self.dict
    }
    /// Read-only access to the normalizer.
    pub fn normalizer(&self) -> &Normalizer {
        &self.norm
    }

    /// Look up the original query text for a logical ID. Returns `None` if
    /// the ID was never ingested or has been deleted.
    pub fn get_query_source(&self, logical_id: u64) -> Option<String> {
        self.query_store.get(logical_id)
    }

    /// Explain why a stored query matched (or would match) a given title.
    /// Re-derives the CompiledQuery from stored source text using the
    /// read-only compile path. Returns `None` if the query source is
    /// unavailable.
    pub fn explain_hit(
        &self,
        logical_id: u64,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let source = self.get_query_source(logical_id)?;
        let mut lc = String::new();
        let cq = crate::compile::compile_one_readonly(
            &source, logical_id, &self.norm, &self.dict, &mut lc,
        )
        .ok()?;
        Some(crate::explain::explain_match_structured(
            &cq, title, &self.norm, &self.dict,
        ))
    }
}
