//! `impl Engine` — THE HOT PATH: match one title (`match_title`) and the
//! rayon-parallel batch matchers. `match_title` builds a
//! [`MatchView`](super::snapshot::MatchView) over the engine's read-path state
//! and calls its `match_title` — the single body also used by the
//! [`EngineSnapshot`] matchers — so the engine and snapshot read paths cannot
//! drift.

use super::snapshot::MatchView;
use super::{Engine, MatchScratch, MatchStats};

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
        MatchView {
            norm: &self.norm,
            dict: &self.dict,
            segments: &self.segments,
            memtable: &self.memtable,
        }
        .match_title(title, s, out, include_broad)
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
                a.main_candidates += b.main_candidates;
                a.broad_candidates += b.broad_candidates;
                a.matches += b.matches;
                a.probes_attempted += b.probes_attempted;
                a.probes_skipped += b.probes_skipped;
                a
            })
    }
}
