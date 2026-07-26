use super::{
    infallible, ChunkCollector, ChunkSink, DeadlineAt, EngineSnapshot, ExhaustiveDeduper,
    ExhaustiveMatchError, ExhaustiveMatchResult, ExhaustiveOptions, Instant, MatchCancelled,
    MatchScratch, MatchStats, MatchView, NoDeadline, TagPredicate, MAX_MATCH_CHUNK_SIZE,
};

impl EngineSnapshot {
    /// THE HOT PATH. Match one title against the snapshot, appending matched
    /// logical IDs to `out`. Identical semantics to [`Engine::match_title`]:
    /// both build a [`MatchView`] over their read-path state and call its
    /// `match_title`, so the engine and snapshot paths share one body and cannot
    /// drift.
    pub fn match_title(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
    ) -> MatchStats {
        self.match_title_filtered(title, s, out, include_broad, &TagPredicate::empty())
    }

    /// Whether the real stored candidate traversal reaches `logical_id` for
    /// `title`. This diagnostic observes postings, segment filters, and lane
    /// visibility but stops before tag/exact verification.
    #[doc(hidden)]
    pub fn diagnostic_candidate_hit(
        &self,
        logical_id: u64,
        title: &str,
        s: &mut MatchScratch,
        include_broad: bool,
    ) -> bool {
        infallible(
            MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                has_phrase_predicates: self.has_phrase_predicates,
                pred: &TagPredicate::empty(),
            }
            .candidate_hit(title, logical_id, s, include_broad, NoDeadline),
        )
    }

    /// [`match_title`](Self::match_title) narrowed by a tag filter (ADR-049). An empty
    /// predicate is byte-identical to `match_title`; a non-empty one drops, in the
    /// post-candidate verify stage, every match whose query does not satisfy the filter.
    pub fn match_title_filtered(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> MatchStats {
        infallible(
            MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                has_phrase_predicates: self.has_phrase_predicates,
                pred,
            }
            .match_title(title, s, out, include_broad, NoDeadline),
        )
    }

    /// Cluster-only scalar path: exact verification and member-level alive/tag
    /// checks are unchanged, then ADR-109 suppresses non-owner emissions.
    pub(crate) fn match_title_filtered_owned(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
        pred: &TagPredicate,
        emission: crate::ownership::UniqueOwner<'_>,
    ) -> MatchStats {
        infallible(
            MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                has_phrase_predicates: self.has_phrase_predicates,
                pred,
            }
            .match_title_with_policy(
                title,
                s,
                out,
                include_broad,
                NoDeadline,
                emission,
            ),
        )
    }

    /// [`match_title_filtered`](Self::match_title_filtered) with an optional cooperative
    /// deadline (ADR-099). `None` delegates to the unarmed path (byte-identical);
    /// `Some(d)` re-checks the clock at entry, at each segment boundary, and
    /// after bounded runs of in-segment work. Once `Instant::now() >= d` it
    /// abandons the match with [`MatchCancelled`] — `out` is cleared, so no
    /// partial result escapes. Cancellation remains cooperative, not preemptive.
    pub fn try_match_title_filtered(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
        pred: &TagPredicate,
        deadline: Option<Instant>,
    ) -> Result<MatchStats, MatchCancelled> {
        let view = MatchView {
            norm: &self.norm,
            dict: &self.dict,
            segments: &self.segments,
            memtable: &self.memtable,
            has_phrase_predicates: self.has_phrase_predicates,
            pred,
        };
        match deadline {
            Some(d) => view.match_title(title, s, out, include_broad, DeadlineAt(d)),
            None => Ok(infallible(view.match_title(
                title,
                s,
                out,
                include_broad,
                NoDeadline,
            ))),
        }
    }

    /// Exact exhaustive matching with `O(chunk_size)` result memory (ADR-114).
    /// Chunks are provisional; the caller may commit them only after this
    /// method returns a terminal summary.
    #[allow(clippy::too_many_arguments)]
    pub fn try_match_title_chunks<S: ChunkSink + ?Sized>(
        &self,
        title: &str,
        options: ExhaustiveOptions,
        program: Option<&crate::rank::CompiledRankProgram>,
        pred: &TagPredicate,
        scratch: &mut MatchScratch,
        deadline: Option<Instant>,
        sink: &mut S,
    ) -> Result<ExhaustiveMatchResult, ExhaustiveMatchError> {
        self.try_match_title_chunks_with_policy(
            title,
            options,
            program,
            pred,
            scratch,
            deadline,
            sink,
            crate::ownership::EmitAll,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_match_title_chunks_owned<S: ChunkSink + ?Sized>(
        &self,
        title: &str,
        options: ExhaustiveOptions,
        program: Option<&crate::rank::CompiledRankProgram>,
        pred: &TagPredicate,
        scratch: &mut MatchScratch,
        deadline: Option<Instant>,
        sink: &mut S,
        emission: crate::ownership::UniqueOwner<'_>,
    ) -> Result<ExhaustiveMatchResult, ExhaustiveMatchError> {
        self.try_match_title_chunks_with_policy(
            title, options, program, pred, scratch, deadline, sink, emission,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_match_title_chunks_with_policy<
        S: ChunkSink + ?Sized,
        P: crate::ownership::EmissionPolicy,
    >(
        &self,
        title: &str,
        options: ExhaustiveOptions,
        program: Option<&crate::rank::CompiledRankProgram>,
        pred: &TagPredicate,
        scratch: &mut MatchScratch,
        deadline: Option<Instant>,
        sink: &mut S,
        emission: P,
    ) -> Result<ExhaustiveMatchResult, ExhaustiveMatchError> {
        if options.chunk_size == 0 || options.chunk_size > MAX_MATCH_CHUNK_SIZE {
            return Err(ExhaustiveMatchError::InvalidChunkSize {
                requested: options.chunk_size,
                max: MAX_MATCH_CHUNK_SIZE,
            });
        }
        // Fail before title normalization and exhaustive-deduper allocation.
        // Jobs can already be cancelled (or expired while waiting for the
        // cluster view barrier), and setup must honor that bound too.
        if deadline.is_some_and(|at| Instant::now() >= at) {
            return Err(ExhaustiveMatchError::Cancelled);
        }
        sink.check_cancelled().map_err(ExhaustiveMatchError::Sink)?;
        let include_broad = options.query_scope == crate::result::QueryScope::WithBroad;
        let mut deduper = ExhaustiveDeduper::new(self, title, pred, include_broad, emission);
        let canonical = move |source, local, logical, should_stop: &mut dyn FnMut() -> bool| {
            deduper.is_first_matching(source, local, logical, should_stop)
        };
        let scorer = |logical_id, should_stop: &mut dyn FnMut() -> bool| {
            program.and_then(|rank| {
                self.rank_metadata_for_logical_with_poll(logical_id, should_stop)
                    .map(|(values, tags)| crate::rank::score_program(values, tags, rank))
            })
        };
        let mut collector =
            ChunkCollector::new(sink, options.chunk_size, canonical, scorer, deadline);
        let view = MatchView {
            norm: &self.norm,
            dict: &self.dict,
            segments: &self.segments,
            memtable: &self.memtable,
            has_phrase_predicates: self.has_phrase_predicates,
            pred,
        };
        let mut stats = match deadline {
            Some(at) => view
                .match_title_collect(
                    title,
                    scratch,
                    &mut collector,
                    include_broad,
                    DeadlineAt(at),
                    emission,
                )
                .map_err(|_| ExhaustiveMatchError::Cancelled)?,
            None => infallible(view.match_title_collect(
                title,
                scratch,
                &mut collector,
                include_broad,
                NoDeadline,
                emission,
            )),
        };
        if collector.deadline_expired() {
            return Err(ExhaustiveMatchError::Cancelled);
        }
        let summary = collector.result().map_err(ExhaustiveMatchError::Sink)?;
        stats.matches = u32::try_from(summary.exact_total).unwrap_or(u32::MAX);
        Ok(ExhaustiveMatchResult { summary, stats })
    }

    /// Compile a request filter — a conjunction of `(key, [values])` groups — into a
    /// [`TagPredicate`] against this snapshot's tag space (ADR-049). Each value resolves
    /// via [`get_or_synthetic`](crate::tagdict::TagDict::get_or_synthetic), so a value
    /// never seen at ingest yields a `TagId` no stored query carries — it matches nothing
    /// (the safe `terms` semantics), never an over-match.
    pub fn compile_tag_predicate(&self, filter: &[(String, Vec<String>)]) -> TagPredicate {
        let groups = filter
            .iter()
            .map(|(key, values)| {
                values
                    .iter()
                    .map(|v| self.tag_dict.get_or_synthetic(key, v))
                    .collect()
            })
            .collect();
        TagPredicate::new(groups)
    }

    /// Compile a [`RankSpec`](crate::rank::RankSpec) against this snapshot's tag
    /// space (ADR-049 §5.4 / ADR-059). Boost `(key,value)`s resolve via
    /// [`get_or_synthetic`](crate::tagdict::TagDict::get_or_synthetic) — exactly as
    /// [`compile_tag_predicate`](Self::compile_tag_predicate) does — so a boost
    /// value never seen at ingest yields a `TagId` no stored query carries and
    /// simply never fires (no over-boost), mirroring the safe `terms`-filter semantics.
    pub fn compile_rank_spec(&self, spec: &crate::rank::RankSpec) -> crate::rank::CompiledRankSpec {
        let boosts = spec
            .boosts
            .iter()
            .map(|(key, value, weight)| (self.tag_dict.get_or_synthetic(key, value), *weight))
            .collect();
        crate::rank::CompiledRankSpec::new(spec.priority_key.clone(), boosts)
    }

    /// Compile the fixed typed bounded-ranking program. Only the canonical
    /// `priority` field is admitted in Increment 2; boosts resolve to TagIds at
    /// request setup so scoring remains integer-only.
    pub fn compile_rank_program(
        &self,
        spec: &crate::rank::RankProgramSpec,
    ) -> Result<crate::rank::CompiledRankProgram, crate::rank::RankProgramError> {
        crate::rank::compile_rank_program(&self.tag_dict, spec)
    }

    pub(super) fn tags_for_logical(&self, logical_id: u64) -> Option<&[crate::tagdict::TagId]> {
        self.source_metadata_for_logical(logical_id)
            .map(|(_, _, tags)| tags)
    }

    /// Newest-live typed rank values and tags for a logical id. The same reverse
    /// walk as compatibility ranking prevents an older physical duplicate from
    /// determining score merely because it emitted first.
    pub(super) fn rank_metadata_for_logical(
        &self,
        logical_id: u64,
    ) -> Option<(crate::rank::RankValues, &[crate::tagdict::TagId])> {
        self.rank_metadata_for_logical_with_poll(logical_id, &mut || false)
    }

    /// Cancellable exhaustive counterpart to [`Self::rank_metadata_for_logical`].
    /// A legacy logical id may have arbitrarily many newer tombstoned physical
    /// copies, so poll between reverse-index entries rather than turning score
    /// resolution into one uninterruptible scan.
    pub(super) fn rank_metadata_for_logical_with_poll<C>(
        &self,
        logical_id: u64,
        should_stop: &mut C,
    ) -> Option<(crate::rank::RankValues, &[crate::tagdict::TagId])>
    where
        C: FnMut() -> bool + ?Sized,
    {
        let mut best: Option<(u64, crate::rank::RankValues, &[crate::tagdict::TagId])> = None;
        for &local in self.memtable.locals_for_logical(logical_id).iter().rev() {
            if should_stop() {
                return None;
            }
            if self.memtable.is_alive(local) {
                let source_generation = self.memtable.source_generation_of(local);
                let replace = match best {
                    Some((best_generation, _, _)) => source_generation > best_generation,
                    None => true,
                };
                if !replace {
                    continue;
                }
                let tags = self.memtable.tags_of(local);
                let mut rank = self.memtable.rank_values(local);
                if rank.priority == 0 {
                    rank.priority = self.tag_dict.legacy_priority_for_tags(tags);
                }
                best = Some((source_generation, rank, tags));
            }
        }
        for seg in self.segments.iter().rev() {
            for &local in seg.locals_for_logical(logical_id).iter().rev() {
                if should_stop() {
                    return None;
                }
                if seg.is_alive(local) {
                    let source_generation = seg.source_generation_of(local);
                    let replace = match best {
                        Some((best_generation, _, _)) => source_generation > best_generation,
                        None => true,
                    };
                    if !replace {
                        continue;
                    }
                    let tags = seg.tags_of(local);
                    let mut rank = seg.rank_values(local);
                    if rank.priority == 0 {
                        rank.priority = self.tag_dict.legacy_priority_for_tags(tags);
                    }
                    best = Some((source_generation, rank, tags));
                }
            }
        }
        best.map(|(_, rank, tags)| (rank, tags))
    }

    /// Build the newest-live integer scorer for one compiled rank program —
    /// the ONE closure the scalar and batch bounded collectors both score
    /// through (`Fn`, so a batch can share it across per-title slots).
    pub(in crate::segment) fn program_scorer<'a>(
        &'a self,
        program: &'a crate::rank::CompiledRankProgram,
    ) -> impl Fn(u64) -> i64 + Sync + 'a {
        move |logical_id| {
            self.rank_metadata_for_logical(logical_id)
                .map_or(0, |(values, tags)| {
                    crate::rank::score_program(values, tags, program)
                })
        }
    }

    /// Poll-aware scorer for armed bounded-ranking requests. It lends the
    /// matcher's request-local deadline sampler to the newest-live metadata
    /// walk, so one logical id with many tombstoned physical versions cannot
    /// become an uninterruptible region.
    pub(in crate::segment) fn program_scorer_with_poll<'a>(
        &'a self,
        program: &'a crate::rank::CompiledRankProgram,
    ) -> impl Fn(u64, &mut dyn FnMut() -> bool) -> Option<i64> + Sync + 'a {
        move |logical_id, should_stop| {
            let mut stopped = should_stop();
            if stopped {
                return None;
            }
            let metadata = self.rank_metadata_for_logical_with_poll(logical_id, &mut || {
                stopped = should_stop();
                stopped
            });
            if stopped {
                None
            } else {
                Some(metadata.map_or(0, |(values, tags)| {
                    crate::rank::score_program(values, tags, program)
                }))
            }
        }
    }
}
