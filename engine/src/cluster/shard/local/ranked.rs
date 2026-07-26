use super::{
    EngineSnapshot, Instant, LocalShard, MatchScratch, ShardError, ShardRankedMatch, TagPredicate,
};

impl LocalShard {
    /// The one bounded-top-K body, parameterized by the snapshot it reads —
    /// the current view (`percolate_top_k_owned`) and a pinned PIT
    /// (`percolate_top_k_owned_pit`, ADR-113) cannot fork behavior.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn top_k_on(
        snap: &EngineSnapshot,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        mut options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        context.validate()?;
        context.require_routed(current_position)?;
        options.query_scope = if include_broad {
            crate::result::QueryScope::WithBroad
        } else {
            crate::result::QueryScope::Standard
        };
        let mut scratch = MatchScratch::new();
        let ranked = snap.try_match_title_top_k_owned(
            title,
            options,
            program,
            pred,
            &mut scratch,
            deadline,
            crate::ownership::UniqueOwner::new(context, current_position),
        )?;
        Ok(ShardRankedMatch {
            hits: ranked.hits,
            total_hits: ranked.total_hits,
            stats: ranked.stats,
            rank_stats: ranked.rank_stats,
            result_bytes: 0,
        })
    }
}
