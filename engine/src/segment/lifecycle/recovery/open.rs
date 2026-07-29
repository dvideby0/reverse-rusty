use super::{
    fresh_segment_generations, invalid_input, replay_wal_tail, seed_next_source_generation, Arc,
    BaseSegment, Engine, EngineConfig, MmapSegment, Normalizer, Segment, SourceCommitState, Wal,
};

impl Engine {
    /// Whether this exact WAL mutation generation is already represented by a
    /// physical row. Source generations are allocated once before the WAL append
    /// and stored unchanged in the exact row, so `(logical, generation)` is the
    /// durable mutation identity. Liveness is intentionally irrelevant: a later
    /// mutation may have tombstoned the captured row, but replaying the older
    /// insert would still resurrect or duplicate it.
    ///
    /// Legacy frames without a generation cannot be distinguished safely from
    /// intentionally additive same-id inserts, so they never take this shortcut.
    pub(super) fn has_materialized_source_generation(
        &self,
        logical: u64,
        source_generation: Option<u64>,
    ) -> bool {
        let Some(source_generation) = source_generation.filter(|&generation| generation != 0)
        else {
            return false;
        };
        self.memtable
            .locals_for_logical(logical)
            .iter()
            .any(|&local| self.memtable.source_generation_of(local) == source_generation)
            || self.segments.iter().any(|segment| {
                segment
                    .locals_for_logical(logical)
                    .iter()
                    .any(|&local| segment.source_generation_of(local) == source_generation)
            })
    }

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
                Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        format!("cannot open committed segment {}: {e}", seg_path.display()),
                    ));
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
        let sources_path = dir.join(&manifest.source_file_name);
        let source_is_required = manifest.source_file_name != "sources.dat";
        let mut source_store_failed = false;
        let query_store = match if source_is_required && !sources_path.exists() {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "manifest-selected source sidecar is missing",
            ))
        } else {
            crate::storage::SourceStore::open(&sources_path, config.retain_source)
        } {
            Ok(s) => Arc::new(s),
            Err(e) => {
                source_store_failed = true;
                // A legacy absent file yields an empty store. A selected v7
                // sidecar is mandatory; corruption or absence is surfaced while
                // matching remains available from the committed segments.
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
        let live_phrase_segments = segments
            .iter()
            .filter(|segment| segment.has_phrase_predicates())
            .count();
        let segment_generations = fresh_segment_generations(segments.len());
        // A skipped committed segment would leave a hole in the manifest's
        // positional address space while `segments` is dense. Reject new
        // positional WAL frames in that degraded state instead of guessing at
        // ordinals; a later successful manifest commit can publish a fresh map.
        let committed_segment_generations = if skipped_segments == 0 {
            segment_generations.clone()
        } else {
            Vec::new()
        };

        let mut engine = Engine {
            config: Arc::new(config),
            norm: Arc::new(norm),
            vocab: None,
            dict: Arc::new(dict),
            tag_dict: Arc::new(tag_dict),
            segments,
            segment_generations,
            committed_segment_generations,
            memtable: Arc::new(Segment::new()),
            live_phrase_segments,
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
            persistence_healthy: skipped_segments == 0 && !source_store_failed,
            skipped_segments,
            query_store,
            source_file_name: manifest.source_file_name,
            source_commit_state: if skipped_segments == 0 && !source_store_failed {
                SourceCommitState::Ready
            } else {
                SourceCommitState::IncompleteRecovery
            },
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

        // ADR-118/119/120/#123/162 compiler-semantics migration. Rebuild every older live
        // materialization from retained `_source` before returning an engine
        // that could serve it: semantics 0 joined positive terms across clause
        // boundaries, semantics 1 discarded all but one feature from a
        // multi-token any-of member, semantics 2 flattened quoted adjacency,
        // semantics 3 flattened multi-feature negated bare terms, and semantics
        // 5 did not retain pre-dedup ranking feature counts. The
        // header stamp makes this idempotent; a
        // missing/inconsistent source sidecar or failed durable commit refuses
        // startup rather than retaining a silent false negative.
        engine.migrate_legacy_compiler_semantics()?;

        Ok(engine)
    }
}
