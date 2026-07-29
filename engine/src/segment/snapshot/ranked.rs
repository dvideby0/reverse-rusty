use super::{
    infallible, DeadlineAt, DeadlineCheck, EngineSnapshot, Instant, MatchCancelled, MatchScratch,
    MatchView, NoDeadline, TagPredicate, TopKCollector, TopKScorer,
};

impl EngineSnapshot {
    /// Bounded local ranked percolation over the scalar matcher. Collection is
    /// `O(K + total-threshold)` and every score resolves newest-live metadata.
    pub fn try_match_title_top_k(
        &self,
        title: &str,
        options: crate::result::TopKOptions,
        program: &crate::rank::CompiledRankProgram,
        pred: &TagPredicate,
        scratch: &mut MatchScratch,
        deadline: Option<Instant>,
    ) -> Result<crate::rank::RankedMatch, crate::rank::RankedMatchError> {
        self.try_match_title_top_k_with_policy(
            title,
            options,
            program,
            pred,
            scratch,
            deadline,
            crate::ownership::EmitAll,
        )
    }

    /// Cluster-only bounded ranked path. Boolean verification is identical to
    /// [`try_match_title_top_k`](Self::try_match_title_top_k); ADR-109's
    /// [`UniqueOwner`](crate::ownership::UniqueOwner) policy is applied only at
    /// the final emission boundary, before the bounded collector observes a row.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_match_title_top_k_owned(
        &self,
        title: &str,
        options: crate::result::TopKOptions,
        program: &crate::rank::CompiledRankProgram,
        pred: &TagPredicate,
        scratch: &mut MatchScratch,
        deadline: Option<Instant>,
        emission: crate::ownership::UniqueOwner<'_>,
    ) -> Result<crate::rank::RankedMatch, crate::rank::RankedMatchError> {
        self.try_match_title_top_k_with_policy(
            title, options, program, pred, scratch, deadline, emission,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_match_title_top_k_with_policy<P: crate::ownership::EmissionPolicy>(
        &self,
        title: &str,
        options: crate::result::TopKOptions,
        program: &crate::rank::CompiledRankProgram,
        pred: &TagPredicate,
        scratch: &mut MatchScratch,
        deadline: Option<Instant>,
        emission: P,
    ) -> Result<crate::rank::RankedMatch, crate::rank::RankedMatchError> {
        if options.size > crate::result::MAX_TOP_K {
            return Err(crate::rank::RankedMatchError::Admission(
                crate::result::TopKAdmissionError::SizeTooLarge {
                    requested: options.size,
                    max: crate::result::MAX_TOP_K,
                },
            ));
        }
        if options.track_total_hits_up_to > crate::result::DEFAULT_TRACK_TOTAL_HITS_UP_TO {
            return Err(crate::rank::RankedMatchError::Admission(
                crate::result::TopKAdmissionError::TotalHitsThresholdTooLarge {
                    requested: options.track_total_hits_up_to,
                    max: crate::result::DEFAULT_TRACK_TOTAL_HITS_UP_TO,
                },
            ));
        }
        // Queue time belongs to the request deadline. Rich-profile title
        // extraction is linear in title bytes, so reject an already-expired
        // request before doing that preprocessing; the matcher retains its own
        // entry check for expiry after extraction.
        if deadline.is_some_and(|at| Instant::now() >= at) {
            return Err(crate::rank::RankedMatchError::Cancelled(MatchCancelled));
        }
        let threshold =
            usize::try_from(options.track_total_hits_up_to).unwrap_or(crate::result::MAX_TOP_K);
        let title_features = [if program.is_static_profile() {
            crate::rank::RankTitleFeatures::default()
        } else {
            crate::rank::RankTitleFeatures::from_title(title)
        }];
        if let Some(at) = deadline {
            let mut collector = TopKCollector::new_polling(
                options.size,
                threshold,
                options.search_after,
                self.program_scorer_with_poll(program, &title_features),
            );
            self.collect_top_k_with_policy(
                title,
                options,
                pred,
                scratch,
                emission,
                &mut collector,
                DeadlineAt(at),
            )
            .map_err(crate::rank::RankedMatchError::Cancelled)
        } else {
            let mut collector = TopKCollector::new(
                options.size,
                threshold,
                options.search_after,
                self.program_scorer(program, &title_features),
            );
            Ok(infallible(self.collect_top_k_with_policy(
                title,
                options,
                pred,
                scratch,
                emission,
                &mut collector,
                NoDeadline,
            )))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_top_k_with_policy<
        D: DeadlineCheck,
        S: TopKScorer,
        P: crate::ownership::EmissionPolicy,
    >(
        &self,
        title: &str,
        options: crate::result::TopKOptions,
        pred: &TagPredicate,
        scratch: &mut MatchScratch,
        emission: P,
        collector: &mut TopKCollector<S>,
        deadline: D,
    ) -> Result<crate::rank::RankedMatch, D::Cancelled> {
        let view = MatchView {
            norm: &self.norm,
            dict: &self.dict,
            segments: &self.segments,
            memtable: &self.memtable,
            has_phrase_predicates: self.has_phrase_predicates,
            pred,
        };
        let include_broad = options.query_scope == crate::result::QueryScope::WithBroad;
        let mut stats =
            view.match_title_collect(title, scratch, collector, include_broad, deadline, emission)?;
        let total_hits = collector.total_hits();
        stats.matches = u32::try_from(total_hits.value).unwrap_or(u32::MAX);
        let hits = collector
            .winners()
            .iter()
            .map(|&(logical_id, score)| crate::rank::RankedHit { logical_id, score })
            .collect();
        Ok(crate::rank::RankedMatch {
            hits,
            total_hits,
            stats,
            rank_stats: collector.rank_stats(),
        })
    }

    /// Score matched logical ids for ranking (ADR-049 §5.4 / ADR-059). Returns
    /// `(id, score)` aligned to `ids`, UNSORTED — the caller owns ordering (score
    /// desc, then `_id` asc for a total order), `from`/`size` pagination, and
    /// `_score` emission. A pure post-match step: it touches neither the candidate
    /// index nor the verifier, so it can only reorder, never add or drop a match.
    /// An id with no live tags (or no tags) scores 0.
    pub fn rank(&self, ids: &[u64], spec: &crate::rank::CompiledRankSpec) -> Vec<(u64, i64)> {
        ids.iter()
            .map(|&id| {
                let s = self
                    .tags_for_logical(id)
                    .map_or(0, |tags| crate::rank::score(tags, &self.tag_dict, spec));
                (id, s)
            })
            .collect()
    }
}
