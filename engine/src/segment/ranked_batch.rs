//! ADR-112 bounded ranked batch entry points on [`EngineSnapshot`] — the batch
//! analogues of `try_match_title_top_k` / `try_match_title_top_k_owned`.
//!
//! One shared [`TopKOptions`] serves every title (per-title K would make the
//! admission bound gameable and the wire attestation ambiguous); ownership is
//! per title via index-aligned [`OwnershipContext`]s. Admission adds the two
//! ADR-112 batch bounds — title count and the `size × titles` aggregate
//! eager-heap budget — on top of the scalar per-title checks, all before any
//! matching work.

use std::time::Instant;

use crate::exact::TagPredicate;
use crate::ownership::{BatchEmissionPolicy, EmitAll, OwnershipContext, PerTitleUniqueOwner};
use crate::rank::{
    BatchRankedMatch, BatchRankedTitle, CompiledRankProgram, RankedHit, RankedMatchError,
};
use crate::result::{
    QueryScope, TopKAdmissionError, TopKOptions, DEFAULT_TRACK_TOTAL_HITS_UP_TO,
    MAX_RANKED_BATCH_HEAP_ROWS, MAX_RANKED_BATCH_TITLES, MAX_TOP_K,
};

use super::broad_batch::try_batch_top_k;
use super::snapshot::MatchView;
use super::{BatchMatchOptions, EngineSnapshot};

impl EngineSnapshot {
    /// Standalone bounded ranked batch (emit-all): per-title exact top-K
    /// winners + honest totals through the columnar batch kernel. The broad
    /// lane is governed by `options.query_scope` (the ranked-path authority);
    /// the remaining `batch_opts` knobs (chunk size, strategy, materialize,
    /// prefilter) are honored as-is.
    pub fn try_match_titles_batch_top_k(
        &self,
        titles: &[impl AsRef<str> + Sync],
        batch_opts: BatchMatchOptions,
        options: TopKOptions,
        program: &CompiledRankProgram,
        pred: &TagPredicate,
        deadline: Option<Instant>,
    ) -> Result<BatchRankedMatch, RankedMatchError> {
        self.batch_top_k_with_policy(
            titles,
            batch_opts,
            options,
            program,
            pred,
            deadline,
            &|_base, _len| EmitAll,
        )
    }

    /// Cluster-only bounded ranked batch: per-title ADR-109 ownership contexts,
    /// index-aligned with `titles`. Boolean verification is identical to the
    /// standalone entry; the per-title [`PerTitleUniqueOwner`] policy applies
    /// only at the final emission boundary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_match_titles_batch_top_k_owned(
        &self,
        titles: &[impl AsRef<str> + Sync],
        batch_opts: BatchMatchOptions,
        options: TopKOptions,
        program: &CompiledRankProgram,
        pred: &TagPredicate,
        contexts: &[OwnershipContext],
        current_position: u32,
        deadline: Option<Instant>,
    ) -> Result<BatchRankedMatch, RankedMatchError> {
        debug_assert_eq!(
            contexts.len(),
            titles.len(),
            "one ownership context per batch title, index-aligned"
        );
        self.batch_top_k_with_policy(
            titles,
            batch_opts,
            options,
            program,
            pred,
            deadline,
            // Chunk-local slice from the SAME base the titles chunk was cut
            // from: `ti` indexes titles and contexts consistently.
            &|base, len| PerTitleUniqueOwner::new(&contexts[base..base + len], current_position),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn batch_top_k_with_policy<P: BatchEmissionPolicy>(
        &self,
        titles: &[impl AsRef<str> + Sync],
        batch_opts: BatchMatchOptions,
        options: TopKOptions,
        program: &CompiledRankProgram,
        pred: &TagPredicate,
        deadline: Option<Instant>,
        policy_for: &(impl Fn(usize, usize) -> P + Sync),
    ) -> Result<BatchRankedMatch, RankedMatchError> {
        if options.size > MAX_TOP_K {
            return Err(RankedMatchError::Admission(
                TopKAdmissionError::SizeTooLarge {
                    requested: options.size,
                    max: MAX_TOP_K,
                },
            ));
        }
        if options.track_total_hits_up_to > DEFAULT_TRACK_TOTAL_HITS_UP_TO {
            return Err(RankedMatchError::Admission(
                TopKAdmissionError::TotalHitsThresholdTooLarge {
                    requested: options.track_total_hits_up_to,
                    max: DEFAULT_TRACK_TOTAL_HITS_UP_TO,
                },
            ));
        }
        if titles.len() > MAX_RANKED_BATCH_TITLES {
            return Err(RankedMatchError::Admission(
                TopKAdmissionError::BatchTitlesTooLarge {
                    requested: titles.len(),
                    max: MAX_RANKED_BATCH_TITLES,
                },
            ));
        }
        let requested_rows = (options.size as u64).saturating_mul(titles.len() as u64);
        if requested_rows > MAX_RANKED_BATCH_HEAP_ROWS {
            return Err(RankedMatchError::Admission(
                TopKAdmissionError::BatchHeapBudgetExceeded {
                    requested_rows,
                    max: MAX_RANKED_BATCH_HEAP_ROWS,
                },
            ));
        }
        let threshold = usize::try_from(options.track_total_hits_up_to).unwrap_or(MAX_TOP_K);
        let scorer = self.program_scorer(program);
        let view = MatchView {
            norm: &self.norm,
            dict: &self.dict,
            segments: &self.segments,
            memtable: &self.memtable,
            pred,
        };
        let mut opts = batch_opts;
        opts.include_broad = options.query_scope == QueryScope::WithBroad;
        let (slots, stats) = try_batch_top_k(
            &view,
            titles,
            opts,
            options.size,
            threshold,
            &scorer,
            policy_for,
            deadline,
        )
        .map_err(RankedMatchError::Cancelled)?;
        Ok(BatchRankedMatch {
            titles: slots
                .into_iter()
                .map(|slot| BatchRankedTitle {
                    hits: slot
                        .hits
                        .into_iter()
                        .map(|(logical_id, score)| RankedHit { logical_id, score })
                        .collect(),
                    total_hits: slot.total_hits,
                    rank_stats: slot.rank_stats,
                })
                .collect(),
            stats,
        })
    }
}
