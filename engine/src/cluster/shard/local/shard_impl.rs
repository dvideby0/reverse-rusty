use super::{
    Arc, ClusterMutation, EventSink, Extracted, FetchedMatch, IngestReport, Instant, LocalShard,
    LogPos, MatchScratch, MatchStats, PlacedQuery, PoisonError, Shard, ShardError,
    ShardRankedMatch, TagPredicate,
};

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

    fn percolate_filtered_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        context.validate()?;
        context.require_routed(current_position)?;
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let stats = self.snapshot().match_title_filtered_owned(
            title,
            &mut scratch,
            &mut out,
            include_broad,
            pred,
            crate::ownership::UniqueOwner::new(context, current_position),
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

    fn percolate_filtered_ranked_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        context.validate()?;
        context.require_routed(current_position)?;
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let snap = self.snapshot();
        let stats = snap.match_title_filtered_owned(
            title,
            &mut scratch,
            &mut out,
            include_broad,
            pred,
            crate::ownership::UniqueOwner::new(context, current_position),
        );
        Ok((snap.rank(&out, spec), stats))
    }

    fn percolate_top_k_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        Self::top_k_on(
            &self.snapshot(),
            title,
            include_broad,
            pred,
            program,
            options,
            context,
            current_position,
            deadline,
        )
    }

    fn percolate_all_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: Option<&crate::rank::CompiledRankProgram>,
        chunk_size: usize,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<Instant>,
        sink: &mut dyn crate::delivery::ChunkSink,
    ) -> Result<crate::delivery::ExhaustiveMatchResult, ShardError> {
        context.validate()?;
        context.require_routed(current_position)?;
        let query_scope = if include_broad {
            crate::result::QueryScope::WithBroad
        } else {
            crate::result::QueryScope::Standard
        };
        self.snapshot()
            .try_match_title_chunks_owned(
                title,
                crate::delivery::ExhaustiveOptions {
                    query_scope,
                    chunk_size,
                },
                program,
                pred,
                &mut MatchScratch::new(),
                deadline,
                sink,
                crate::ownership::UniqueOwner::new(context, current_position),
            )
            .map_err(ShardError::from)
    }

    fn open_pit(&self, pit: u64) -> Result<(), ShardError> {
        let snapshot = self.snapshot();
        self.pits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(pit, snapshot);
        Ok(())
    }

    fn close_pit(&self, pit: u64) -> Result<(), ShardError> {
        self.pits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&pit);
        Ok(())
    }

    fn percolate_top_k_owned_pit(
        &self,
        pit: u64,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        let pinned = self
            .pits
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&pit)
            .map(Arc::clone)
            .ok_or(ShardError::PitNotFound(pit))?;
        Self::top_k_on(
            &pinned,
            title,
            include_broad,
            pred,
            program,
            options,
            context,
            current_position,
            deadline,
        )
    }

    fn percolate_top_k_batch_owned(
        &self,
        titles: &[crate::cluster::shard::BatchTitleRequest<'_>],
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        mut options: crate::result::TopKOptions,
        current_position: u32,
        deadline: Option<Instant>,
    ) -> Result<crate::cluster::shard::ShardBatchRankedMatch, ShardError> {
        // Fail closed up front: one invalid/unrouted context fails the whole
        // batch before any matching work.
        for request in titles {
            request.context.validate()?;
            request.context.require_routed(current_position)?;
        }
        options.query_scope = if include_broad {
            crate::result::QueryScope::WithBroad
        } else {
            crate::result::QueryScope::Standard
        };
        let snap = self.snapshot();
        let cfg = snap.config();
        let batch_opts = crate::segment::BatchMatchOptions {
            include_broad,
            broad_batch_size: cfg.broad_batch_size,
            broad_strategy: if cfg.broad_columnar {
                crate::segment::BroadStrategy::Columnar
            } else {
                crate::segment::BroadStrategy::Inline
            },
            broad_materialize: cfg.broad_materialize,
            broad_prefilter: cfg.broad_prefilter,
        };
        let title_strs: Vec<&str> = titles.iter().map(|request| request.title).collect();
        let contexts: Vec<crate::ownership::OwnershipContext> = titles
            .iter()
            .map(|request| request.context.clone())
            .collect();
        let ranked = snap.try_match_titles_batch_top_k_owned(
            &title_strs,
            batch_opts,
            options,
            program,
            pred,
            &contexts,
            current_position,
            deadline,
        )?;
        Ok(crate::cluster::shard::ShardBatchRankedMatch {
            titles: ranked
                .titles
                .into_iter()
                .map(|title| crate::cluster::shard::ShardRankedTitle {
                    hits: title.hits,
                    total_hits: title.total_hits,
                    rank_stats: title.rank_stats,
                })
                .collect(),
            stats: ranked.stats,
            result_bytes: 0,
        })
    }

    fn fetch_matches(
        &self,
        logical_ids: &[u64],
        max_source_bytes: usize,
        deadline: Option<Instant>,
    ) -> Result<Vec<FetchedMatch>, ShardError> {
        // One immutable current-view snapshot for the whole group: concurrent
        // writes cannot make different winners in one response observe different
        // source-store generations.
        let snap = self.snapshot();
        let mut out = Vec::with_capacity(logical_ids.len());
        let mut remaining = max_source_bytes;
        for &logical_id in logical_ids {
            let source = super::super::fetch_source_step(
                &snap,
                logical_id,
                &mut remaining,
                max_source_bytes,
                deadline,
            )?;
            out.push(FetchedMatch { logical_id, source });
        }
        Ok(out)
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        Ok(self.snapshot.load().num_queries())
    }

    fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        Ok(self.snapshot.load().class_counts())
    }

    fn validate_ownership(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), ShardError> {
        self.snapshot()
            .validate_ownership_for_shard(position, generation, num_shards)
            .map_err(ShardError::from)
    }

    fn live_sources(&self) -> Result<Vec<(u64, String)>, ShardError> {
        Ok(self.lock().live_sources())
    }

    fn live_logical_ids(&self) -> Result<Vec<u64>, ShardError> {
        // The enumeration walks the source store; a source-less / partial
        // `sources.dat` (a legacy or tampered restore) would under-enumerate and
        // let insert-only admission re-admit a LIVE id — the ADR-097 fingerprint
        // refusal for the same root cause. One lock hold = one point in time;
        // `num_live_queries` is the index-side tombstone-aware count (codex review).
        let (ids, live) = {
            let eng = self.lock();
            (eng.live_logical_ids(), eng.num_live_queries())
        };
        if ids.len() != live {
            return Err(ShardError::Config(format!(
                "logical-id enumeration covers {} of {live} live queries (a source-less \
                 or partial store); refusing to seed insert-only admission from it",
                ids.len()
            )));
        }
        Ok(ids)
    }

    fn live_sources_tagged(&self) -> Result<Vec<super::super::LiveTaggedQuery>, ShardError> {
        self.lock()
            .live_source_documents_tagged()
            .map_err(ShardError::SourceUnavailable)
    }

    fn is_local(&self) -> bool {
        true
    }

    fn source_of(&self, logical: u64) -> Result<Option<String>, ShardError> {
        // Lock-free: the snapshot's query store carries the live source set (ADR-014).
        Ok(self.snapshot().get_query_source(logical))
    }

    fn document_of(
        &self,
        logical: u64,
    ) -> Result<Option<crate::storage::StoredSource>, ShardError> {
        // Lock-free, like `source_of`; metadata is decoded only for this point read.
        let snapshot = self.snapshot();
        match snapshot.get_query_document(logical) {
            Some(document) => Ok(Some(document)),
            None if snapshot.has_live_query(logical) => Err(ShardError::SourceUnavailable(logical)),
            None => Ok(None),
        }
    }

    fn has_live_query(&self, logical: u64) -> Result<bool, ShardError> {
        Ok(self.snapshot().has_live_query(logical))
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
            placement: crate::ownership::QueryPlacement::standalone(),
        })?;
        let out = eng.insert_extracted(ex, logical, version, text, tags);
        Self::publish(&eng, &self.snapshot);
        Ok(out)
    }

    fn insert_extracted_with_placement(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<Option<u32>, ShardError> {
        placement.validate()?;
        let mut eng = self.lock();
        self.translog.append(&ClusterMutation::Add {
            logical,
            version,
            dsl: text.to_string(),
            tags: tags.to_vec(),
            placement: placement.clone(),
        })?;
        let out = eng.insert_extracted_with_placement(ex, logical, version, text, tags, placement);
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

    fn source_file_name(&self) -> Result<String, ShardError> {
        Ok(self.lock().source_file_name().to_string())
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
