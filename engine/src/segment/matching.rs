//! `impl Engine` — THE HOT PATH: match one title (`match_title`) and the
//! rayon-parallel batch matchers. `match_title` builds a
//! [`MatchView`](super::snapshot::MatchView) over the engine's read-path state
//! and calls its `match_title` — the single body also used by the
//! [`EngineSnapshot`] matchers — so the engine and snapshot read paths cannot
//! drift.

use super::snapshot::MatchView;
use super::{infallible, BatchMatchOptions, Engine, MatchScratch, MatchStats, NoDeadline};
use crate::exact::TagPredicate;

impl Engine {
    /// THE HOT PATH. Match one title, appending matched logical IDs to `out`.
    /// Probes EVERY segment (all base segments + memtable) and unions the
    /// matched logical ids. `include_broad` controls whether the broad lane is
    /// evaluated inline. Shares its body with [`EngineSnapshot`] via
    /// [`MatchView`](super::snapshot::MatchView).
    pub fn match_title(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
    ) -> MatchStats {
        self.match_title_filtered(title, s, out, include_broad, &TagPredicate::empty())
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
                pred,
            }
            .match_title(title, s, out, include_broad, NoDeadline),
        )
    }

    /// Parallel matching: match a batch of titles across all available cores.
    /// Returns a Vec of (title_index, matched_logical_ids, stats) tuples.
    /// Each thread gets its own MatchScratch (allocated once, reused across
    /// titles assigned to that thread). The Engine is shared read-only.
    pub fn match_titles_par(
        &self,
        titles: &[impl AsRef<str> + Sync],
        include_broad: bool,
    ) -> Vec<(usize, Vec<u64>, MatchStats)> {
        use rayon::prelude::*;
        titles
            .par_iter()
            .enumerate()
            .map_init(
                || (MatchScratch::new(), Vec::new()),
                |(scratch, out), (idx, title)| {
                    let stats = self.match_title(title.as_ref(), scratch, out, include_broad);
                    (idx, out.clone(), stats)
                },
            )
            .collect()
    }

    /// Parallel matching returning only aggregate stats (no per-title results).
    /// Useful for benchmarks measuring throughput without allocating result vecs.
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
                a.unique_candidates += b.unique_candidates;
                a.postings_scanned += b.postings_scanned;
                a.broad_postings_scanned += b.broad_postings_scanned;
                a.main_candidates += b.main_candidates;
                a.broad_candidates += b.broad_candidates;
                a.matches += b.matches;
                a.probes_attempted += b.probes_attempted;
                a.probes_skipped += b.probes_skipped;
                a.broad_queries_evaluated += b.broad_queries_evaluated;
                a.broad_anchors_scanned += b.broad_anchors_scanned;
                a.broad_batches += b.broad_batches;
                a
            })
    }

    /// Batch match: selective lane per title + broad lane once per batch
    /// (columnar). Returns per-title `(index, matched_logical_ids)`, sorted +
    /// deduped, byte-identical to per-title [`Engine::match_title`]. See
    /// [`BatchMatchOptions`] and the `broad_batch` module.
    pub fn match_titles_batch(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
    ) -> Vec<(usize, Vec<u64>)> {
        super::broad_batch::batch_results(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                pred: &TagPredicate::empty(),
            },
            titles,
            opts,
        )
    }

    /// Batch match returning only aggregate [`MatchStats`] (for benchmarks).
    pub fn match_titles_batch_stats(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
    ) -> MatchStats {
        super::broad_batch::batch_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                pred: &TagPredicate::empty(),
            },
            titles,
            opts,
        )
    }

    /// Batch match returning per-title `(index, matched_logical_ids)` AND the
    /// aggregate [`MatchStats`] in a single pass. Snapshot twin:
    /// [`EngineSnapshot::match_titles_batch_with_stats`](super::EngineSnapshot::match_titles_batch_with_stats).
    pub fn match_titles_batch_with_stats(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
    ) -> (Vec<(usize, Vec<u64>)>, MatchStats) {
        super::broad_batch::batch_results_with_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                pred: &TagPredicate::empty(),
            },
            titles,
            opts,
        )
    }
}
