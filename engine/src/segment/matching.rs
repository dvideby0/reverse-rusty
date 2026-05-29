//! `impl Engine` — THE HOT PATH: match one title (`match_title`) and the
//! rayon-parallel batch matchers. These mirror the [`EngineSnapshot`] matchers
//! but operate directly on the (single-threaded-owned) engine.

use super::{Engine, MatchScratch, MatchStats};

impl Engine {
    /// THE HOT PATH. Match one title, appending matched logical IDs to `out`.
    /// Probes EVERY segment (all base segments + memtable) and unions the
    /// matched logical ids. `include_broad` controls whether the broad lane is
    /// evaluated inline.
    pub fn match_title(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
    ) -> MatchStats {
        // per-segment seen-buffer sizing (base segments first, memtable last)
        let n_base = self.segments.len();
        let mut seg_lens: Vec<usize> = Vec::with_capacity(n_base + 1);
        for seg in &self.segments {
            seg_lens.push(seg.len());
        }
        seg_lens.push(self.memtable.len());
        s.ensure(&seg_lens);

        s.epoch = s.epoch.wrapping_add(1);
        if s.epoch == 0 {
            // epoch wrapped: reset all stamps
            for buf in &mut s.seen {
                for v in buf.iter_mut() {
                    *v = 0;
                }
            }
            s.epoch = 1;
        }
        let epoch = s.epoch;
        out.clear();

        // 1) normalize -> dense feature ids (sorted). Take the buffer out so we
        // can iterate it while mutating `s.seen` (no aliasing, no allocation).
        self.norm
            .match_features(title, &self.dict, &mut s.lc, &mut s.feats);
        let feats = std::mem::take(&mut s.feats);

        // 2) title common-mask word
        let mut tmask = 0u64;
        for &f in &feats {
            let b = self.dict.mask_bit(f);
            if b != crate::dict::NO_MASK_BIT {
                tmask |= 1u64 << b;
            }
        }

        let mut stats = MatchStats::default();

        // 3) probe every base segment, each with its own seen buffer
        for (i, base) in self.segments.iter().enumerate() {
            base.match_into(
                &feats,
                tmask,
                &self.dict,
                epoch,
                &mut s.seen[i],
                out,
                include_broad,
                &mut stats,
            );
        }
        self.memtable.match_into(
            &feats,
            tmask,
            &self.dict,
            epoch,
            &mut s.seen[n_base],
            out,
            include_broad,
            &mut stats,
        );

        // 4) dedup logical ids across segments (a logical id can live in more
        // than one segment, e.g. base + an updated copy in a later segment).
        out.sort_unstable();
        out.dedup();

        // restore the reusable buffer
        s.feats = feats;
        stats.matches = out.len() as u32;
        stats
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
