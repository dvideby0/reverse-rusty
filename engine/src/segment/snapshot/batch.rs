use super::{
    BatchMatchOptions, DeadlineAt, EngineSnapshot, Instant, MatchCancelled, MatchScratch,
    MatchStats, MatchView, TagPredicate,
};

impl EngineSnapshot {
    /// Parallel matching on the snapshot.
    pub fn match_titles_par(
        &self,
        titles: &[impl AsRef<str> + Sync],
        include_broad: bool,
    ) -> Vec<(usize, Vec<u64>, MatchStats)> {
        self.match_titles_par_filtered(titles, include_broad, &TagPredicate::empty())
    }

    /// [`match_titles_par`](Self::match_titles_par) narrowed by a tag filter (ADR-049).
    pub fn match_titles_par_filtered(
        &self,
        titles: &[impl AsRef<str> + Sync],
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Vec<(usize, Vec<u64>, MatchStats)> {
        use rayon::prelude::*;
        titles
            .par_iter()
            .enumerate()
            .map_init(
                || (MatchScratch::new(), Vec::new()),
                |(scratch, out), (idx, title)| {
                    let stats = self.match_title_filtered(
                        title.as_ref(),
                        scratch,
                        out,
                        include_broad,
                        pred,
                    );
                    (idx, out.clone(), stats)
                },
            )
            .collect()
    }

    /// [`match_titles_par_filtered`](Self::match_titles_par_filtered) with an optional
    /// cooperative deadline (ADR-099/123). `None` delegates unarmed (byte-identical).
    /// Armed, every in-flight title self-checks per segment and at bounded
    /// intervals inside segment traversal, and the `Result` collect
    /// short-circuits the batch: the FIRST cancellation abandons the whole request —
    /// per-title results are all-or-nothing, never a partially-filled batch.
    pub fn try_match_titles_par_filtered(
        &self,
        titles: &[impl AsRef<str> + Sync],
        include_broad: bool,
        pred: &TagPredicate,
        deadline: Option<Instant>,
    ) -> Result<Vec<(usize, Vec<u64>, MatchStats)>, MatchCancelled> {
        use rayon::prelude::*;
        let Some(d) = deadline else {
            return Ok(self.match_titles_par_filtered(titles, include_broad, pred));
        };
        let view = MatchView {
            norm: &self.norm,
            dict: &self.dict,
            segments: &self.segments,
            memtable: &self.memtable,
            has_phrase_predicates: self.has_phrase_predicates,
            pred,
        };
        titles
            .par_iter()
            .enumerate()
            .map_init(
                || (MatchScratch::new(), Vec::new()),
                |(scratch, out), (idx, title)| {
                    let stats = view.match_title(
                        title.as_ref(),
                        scratch,
                        out,
                        include_broad,
                        DeadlineAt(d),
                    )?;
                    Ok((idx, out.clone(), stats))
                },
            )
            .collect()
    }

    pub fn match_titles_par_stats(
        &self,
        titles: &[impl AsRef<str> + Sync],
        include_broad: bool,
    ) -> MatchStats {
        use rayon::prelude::*;
        titles
            .par_iter()
            .map_init(
                || (MatchScratch::new(), Vec::new()),
                |(scratch, out), title| {
                    self.match_title(title.as_ref(), scratch, out, include_broad)
                },
            )
            .reduce(MatchStats::default, |mut a, b| {
                // The ONE shared merge body — a new field cannot be silently
                // dropped from this reduce (the ADR-101 under-count lesson).
                a.merge(b);
                a
            })
    }

    /// Batch match on the snapshot: selective lane per title + broad lane once
    /// per batch (columnar). Per-title `(index, matched_logical_ids)`, identical
    /// to per-title [`EngineSnapshot::match_title`]. Lock-free read path.
    pub fn match_titles_batch(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
    ) -> Vec<(usize, Vec<u64>)> {
        self.match_titles_batch_filtered(titles, opts, &TagPredicate::empty())
    }

    /// [`match_titles_batch`](Self::match_titles_batch) narrowed by a tag filter
    /// (ADR-049). The columnar broad lane applies the same filter as the selective lane,
    /// so the batch result stays byte-identical to the per-title filtered path.
    pub fn match_titles_batch_filtered(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
        pred: &TagPredicate,
    ) -> Vec<(usize, Vec<u64>)> {
        super::super::broad_batch::batch_results(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                has_phrase_predicates: self.has_phrase_predicates,
                pred,
            },
            titles,
            opts,
        )
    }

    /// Batch match returning only aggregate [`MatchStats`].
    pub fn match_titles_batch_stats(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
    ) -> MatchStats {
        super::super::broad_batch::batch_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                has_phrase_predicates: self.has_phrase_predicates,
                pred: &TagPredicate::empty(),
            },
            titles,
            opts,
        )
    }

    /// Batch match returning per-title `(index, matched_logical_ids)` AND the
    /// aggregate [`MatchStats`] in a single pass — for callers that need both the
    /// results and the broad-lane meters (the HTTP `/_mpercolate` handler) without
    /// matching twice. Same result contract as [`Self::match_titles_batch`].
    pub fn match_titles_batch_with_stats(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
    ) -> (Vec<(usize, Vec<u64>)>, MatchStats) {
        self.match_titles_batch_with_stats_filtered(titles, opts, &TagPredicate::empty())
    }

    /// [`match_titles_batch_with_stats`](Self::match_titles_batch_with_stats) narrowed by
    /// a tag filter (ADR-049) — the `/_mpercolate` filtered path.
    pub fn match_titles_batch_with_stats_filtered(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
        pred: &TagPredicate,
    ) -> (Vec<(usize, Vec<u64>)>, MatchStats) {
        super::super::broad_batch::batch_results_with_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                has_phrase_predicates: self.has_phrase_predicates,
                pred,
            },
            titles,
            opts,
        )
    }

    /// [`match_titles_batch_with_stats_filtered`](Self::match_titles_batch_with_stats_filtered)
    /// with an optional cooperative deadline (ADR-099/123). `None` delegates
    /// unarmed (byte-identical). Armed, each chunk checks per title (Phase 0),
    /// per segment block, and at bounded intervals inside the columnar kernels;
    /// the first cancellation abandons the whole batch — never a
    /// partially-filled `responses[]`.
    pub fn try_match_titles_batch_with_stats_filtered(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
        pred: &TagPredicate,
        deadline: Option<Instant>,
    ) -> Result<super::super::BatchResultsWithStats, MatchCancelled> {
        super::super::broad_batch::try_batch_results_with_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                has_phrase_predicates: self.has_phrase_predicates,
                pred,
            },
            titles,
            opts,
            deadline,
        )
    }
}
