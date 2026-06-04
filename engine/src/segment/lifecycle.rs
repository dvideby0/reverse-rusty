//! `impl Engine` — construction, configuration, vocabulary, crash recovery
//! (`open`), the observer hook, and engine-handle accessors. Heavier lifecycle
//! work (ingest, flush/compaction, persistence) lives in sibling submodules.

use super::{BaseSegment, Engine, EngineSnapshot, Segment};
use std::sync::Arc;

use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::storage::{MmapSegment, SourceStore};
use crate::tagdict::TagDict;
use crate::wal::{Wal, WalEntry};

impl Engine {
    /// Create an engine with default configuration.
    pub fn new(norm: Normalizer) -> Self {
        Self::with_config(norm, EngineConfig::default())
    }

    /// Create an engine with explicit configuration. If `config.data_dir` is set,
    /// initializes the data directory and WAL.
    pub fn with_config(norm: Normalizer, config: EngineConfig) -> Self {
        Self::with_shared(
            Arc::new(norm),
            Arc::new(Dict::new()),
            Arc::new(TagDict::new()),
            config,
        )
    }

    /// Create an engine that SHARES a pre-built normalizer and dictionary (by
    /// `Arc`) instead of owning fresh ones. This is how a cluster shard is built:
    /// every shard shares the coordinator's one authoritative, already-finalized
    /// `Dict` so `FeatureId`s / `sig_key`s / hotness are globally consistent (see
    /// [`crate::cluster`]). The dict must be treated as frozen — shard ingest uses
    /// the read-only `*_extracted` paths so it is never `Arc::make_mut`'d (which
    /// would fork it and break cross-shard agreement).
    pub fn with_shared(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
    ) -> Self {
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
            tag_dict,
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
            owns_manifest: true,
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

    /// Create the data directory and its `segments` subdirectory WITHOUT opening a WAL —
    /// the segments-only-durable path ([`with_shared_segments_only`](Self::with_shared_segments_only)).
    fn init_segments_dir(dir: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir.join("segments"))
    }

    /// Create a **cluster-shard** engine (ADR-032): shares the coordinator's frozen
    /// normalizer + dict (like [`with_shared`](Self::with_shared)) and persists sealed
    /// segments under `config.data_dir`, but runs WITHOUT a WAL and WITHOUT writing its
    /// own `manifest.bin`. The coordinator's `ClusterLog` is the durable tail and its
    /// `cluster_manifest.bin` is the sole segment registry + dict store, so a per-shard
    /// WAL would double-log the tail and a per-shard manifest would duplicate the shared
    /// dict. `config.data_dir` MUST be set; reopen via
    /// [`open_shared_segments`](Self::open_shared_segments).
    pub fn with_shared_segments_only(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
    ) -> std::io::Result<Self> {
        let dir = config.data_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "with_shared_segments_only requires config.data_dir",
            )
        })?;
        Self::init_segments_dir(dir)?;
        let query_store = Arc::new(SourceStore::empty(config.retain_source));
        Ok(Engine {
            config: Arc::new(config),
            norm,
            vocab: None,
            dict,
            tag_dict,
            segments: Vec::new(),
            memtable: Arc::new(Segment::new()),
            rejected_parse: 0,
            rejected_class_d: 0,
            observer: None,
            pending_events: Vec::new(),
            wal: None,
            next_seg_id: 1,
            wal_healthy: true,
            persistence_healthy: true,
            skipped_segments: 0,
            query_store,
            vocab_epoch: 0,
            owns_manifest: false,
        })
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
        let norm = Arc::new(vocab.to_normalizer()?);
        // Resolve any declared/learned equivalence groups against the frozen dict under the
        // new normalizer and install them, so the subsequent recompile (and future inserts)
        // expand queries through them (ADR-054). No groups ⇒ empty map ⇒ no-op (the dict
        // clone is dwarfed by the recompile this set_vocab triggers).
        let equiv = vocab.resolve_equivalences(&norm, &self.dict);
        Arc::make_mut(&mut self.dict).set_equivalences(equiv);
        self.norm = norm;
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

    /// Record a vocabulary on an engine that is ALREADY consistent with it,
    /// WITHOUT recompiling or bumping the epoch. Used at startup after
    /// [`open`](Self::open): the engine was opened with this vocab's normalizer,
    /// so its segments already align with it and only the [`Vocab`](crate::vocab::Vocab)
    /// object needs installing (so `GET /_vocab` can serve it). Unlike
    /// [`set_vocab`](Self::set_vocab) — which signals a normalizer *change* by
    /// bumping the epoch and marking segments stale — this is a pure metadata
    /// record. Use [`set_vocab`] + [`recompile_stale_segments`](Self::recompile_stale_segments)
    /// to actually *change* the vocabulary at runtime.
    pub fn adopt_vocab(
        &mut self,
        vocab: crate::vocab::Vocab,
    ) -> Result<(), crate::error::NormalizerError> {
        let norm = Arc::new(vocab.to_normalizer()?);
        // Re-install equivalence groups (ADR-054) so inserts AFTER reopen expand through them.
        // Already-compiled segments were persisted with their expansion baked in, so matching
        // recovered queries needs no re-resolution — this only equips the live compile path.
        let equiv = vocab.resolve_equivalences(&norm, &self.dict);
        Arc::make_mut(&mut self.dict).set_equivalences(equiv);
        self.norm = norm;
        self.vocab = Some(Arc::new(vocab));
        Ok(())
    }

    /// The current live `(logical_id, query_text)` set — the source corpus the
    /// index is a materialized view of, sorted by logical id for deterministic
    /// rebuilds. Backed by the query store (kept in sync with the index by the
    /// insert/delete paths), so it reflects exactly the queries that should be
    /// matchable. Used by [`recompile_stale_segments`](Self::recompile_stale_segments).
    pub fn live_sources(&self) -> Vec<(u64, String)> {
        let mut out: Vec<(u64, String)> = Vec::with_capacity(self.query_store.len());
        self.query_store
            .for_each_live(|logical, text| out.push((logical, text.to_string())));
        out.sort_unstable_by_key(|&(l, _)| l);
        out
    }

    /// The current `TagId`s of the live entry for `logical` (ADR-049), read from the
    /// memtable or a base segment. Used by [`recompile_stale_segments`] to carry a
    /// query's tags through a vocabulary change unchanged (same tag space ⇒ the ids stay
    /// valid). Empty when the query is untagged or absent.
    fn live_tag_ids_for(&self, logical: u64) -> Vec<crate::tagdict::TagId> {
        for &local in self.memtable.locals_for_logical(logical) {
            if self.memtable.is_alive(local) {
                return self.memtable.tags_of(local).to_vec();
            }
        }
        for seg in &self.segments {
            for &local in seg.locals_for_logical(logical) {
                if seg.is_alive(local) {
                    return seg.tags_of(local).to_vec();
                }
            }
        }
        Vec::new()
    }

    /// Recompile every live query under the CURRENT normalizer, replacing all
    /// base segments (and the memtable) with one freshly-compiled segment at the
    /// current vocab epoch. This is the recompile pass that makes a normalizer
    /// change ([`set_vocab`](Self::set_vocab)) actually take effect on
    /// already-ingested queries: without it, segments compiled under the old
    /// normalizer carry stale feature ids, and a title normalized with the new
    /// normalizer can miss them — a **false negative**.
    ///
    /// Queries are recompiled READ-ONLY against the existing (frozen) dict via
    /// [`extract_readonly`](crate::compile::extract_readonly): a declared alias
    /// collapses both surface forms to one feature (so both now match), and a new
    /// alias canonical that isn't interned resolves to a stable synthetic id
    /// (mechanism 1). The dict's feature space is unchanged.
    ///
    /// A no-op (returns 0) when nothing is stale; after it, `has_stale_segments()`
    /// is false. Returns the number of queries recompiled.
    ///
    /// Atomicity: a caller that publishes snapshots (e.g. the server) must call
    /// this **before** publishing the next snapshot, so readers never observe the
    /// new normalizer against not-yet-recompiled segments.
    pub fn recompile_stale_segments(&mut self) -> usize {
        if !self.has_stale_segments() {
            return 0;
        }
        // Recompile the live source set read-only against the frozen dict under
        // the current normalizer into one fresh segment.
        let live = self.live_sources();
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut lc = String::new();
        let mut recompiled = 0usize;
        for (logical, text) in &live {
            if let Ok(ast) = crate::dsl::parse(text) {
                let ex = crate::compile::extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
                // Carry the query's existing tags forward unchanged — tags are orthogonal
                // to the normalizer, so a vocabulary change must not drop them (ADR-049).
                let tags = self.live_tag_ids_for(*logical);
                if seg
                    .add_compiled(&ex, &tags, &self.dict, *logical, 1)
                    .is_some()
                {
                    recompiled += 1;
                }
            }
        }
        seg.build_filter();

        // Atomic swap: drop every (stale) base segment + the memtable and install
        // the one freshly-compiled segment, so no live query is left at an old
        // epoch. Old segment files are GC'd after the manifest commit.
        let old_files = self.collect_mmap_paths();
        self.segments.clear();
        let mut fresh_mem = Segment::new();
        fresh_mem.vocab_epoch = self.vocab_epoch;
        self.memtable = Arc::new(fresh_mem);
        let persisted = self.seal_and_push(seg);

        // Persist like a flush, but FAIL CLOSED (ADR-051): only retire the old
        // segment files and advance the WAL (checkpoint marks the live queries
        // materialized, reset truncates them) once the freshly-compiled segment is
        // durably on disk AND the manifest — the commit point referencing it — has
        // been written. We just cleared the old segments from the vec, so if the
        // recompiled segment did NOT persist, deleting the old files or resetting
        // the WAL would erase the only durable copy of the whole corpus. Leaving
        // both intact lets a restart recover the pre-recompile state and re-apply
        // the vocab change. The recompiled segment is still served from memory
        // meanwhile; `persistence_healthy` is false to signal the degraded state.
        if persisted && self.save_manifest_if_persistent() {
            self.checkpoint_wal();
            self.reset_wal_if_safe();
            self.cleanup_segment_files(&old_files);
        }
        recompiled
    }

    /// Learn alias/synonym rules from this engine's live corpus (ADR-015 any-of learning)
    /// and apply them (ADR-046 mechanism 2): a synonym appearing in at least `min_count`
    /// any-of groups (e.g. `(rookie,rc)` ⇒ `rc → rookie`) is merged UNDER the current
    /// vocabulary (a previously set alias wins) and the index is recompiled so the change
    /// takes effect immediately. Returns the number of queries recompiled.
    ///
    /// A thin wrapper over [`learn_and_apply_with`](Self::learn_and_apply_with) with NPMI
    /// corpus phrase induction disabled — behaviorally unchanged.
    pub fn learn_and_apply(
        &mut self,
        min_count: usize,
    ) -> Result<usize, crate::error::NormalizerError> {
        self.learn_and_apply_with(&crate::vocab::CorpusLearnConfig {
            anyof_min_count: min_count,
            ..Default::default()
        })
    }

    /// Like [`learn_and_apply`](Self::learn_and_apply) but also runs opt-in **NPMI corpus
    /// phrase induction** when `cfg.corpus_phrases` is set (ADR-053): multi-token entities
    /// induced from the live query text (e.g. `upper deck`) are merged UNDER the current
    /// vocabulary (a declared alias/phrase wins on a token collision) and the index is
    /// recompiled. With `corpus_phrases = false` this is identical to
    /// `learn_and_apply(cfg.anyof_min_count)`. Phrases only — never aliases — so the
    /// same-normalizer gluing is lossless-cover safe (zero false negatives). Returns the
    /// number of queries recompiled.
    pub fn learn_and_apply_with(
        &mut self,
        cfg: &crate::vocab::CorpusLearnConfig,
    ) -> Result<usize, crate::error::NormalizerError> {
        let corpus = self.live_sources();
        let learned = crate::vocab::learn_vocab_from_corpus(&corpus, cfg);
        let mut merged = crate::vocab::Vocab::new();
        if let Some(v) = &self.vocab {
            merged.merge(v);
        }
        merged.merge(&learned);
        self.set_vocab(merged)?; // bumps the epoch / marks segments stale
        Ok(self.recompile_stale_segments())
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
            tag_dict: Arc::clone(&self.tag_dict),
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

    /// The current next segment-id counter — recorded per shard in the cluster manifest
    /// so a flush after reopen never reuses a committed segment filename (ADR-032).
    pub fn next_seg_id(&self) -> u64 {
        self.next_seg_id
    }

    /// The filenames of this engine's live (mmap'd) base segments, in order — the
    /// per-shard registry the cluster coordinator commits (ADR-032). Returns `Err` if
    /// ANY base segment is in-memory: that means a segment write fell back to `Memory`
    /// (e.g. a disk error, `persistence.rs`), and committing a registry that omits it
    /// would silently lose that segment's data on reopen, so the caller must refuse to
    /// commit and surface the failure instead.
    pub fn segment_filenames(&self) -> std::io::Result<Vec<String>> {
        let mut names = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            match seg.as_ref() {
                BaseSegment::Mmap(m) => {
                    let name = m
                        .path()
                        .file_name()
                        .and_then(|f| f.to_str())
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "segment path has no filename",
                            )
                        })?;
                    names.push(name.to_string());
                }
                BaseSegment::Memory(_) => {
                    return Err(std::io::Error::other(
                        "a base segment is in-memory (segment write fell back); refusing \
                         to commit a cluster segment registry that would lose it on reopen",
                    ));
                }
            }
        }
        Ok(names)
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
