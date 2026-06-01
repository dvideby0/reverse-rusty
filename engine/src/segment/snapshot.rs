//! `MatchScratch` reusable buffers and `EngineSnapshot` — the immutable,
//! lock-free read view and THE HOT PATH (`match_title` and the rayon-parallel
//! batch matchers). Type definitions live in the `segment` module root.

use super::{BaseSegment, BatchMatchOptions, EngineSnapshot, MatchScratch, MatchStats, Segment};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::vocab::Vocab;
use std::sync::Arc;

impl MatchScratch {
    pub fn new() -> Self {
        MatchScratch {
            lc: String::with_capacity(256),
            feats: Vec::with_capacity(64),
            seen: Vec::new(),
            epoch: 0,
        }
    }

    /// Make sure we have one seen-buffer per segment (base segments first, then
    /// the memtable last), each at least as large as that segment's length.
    /// Reuses existing allocations (steady-state: no-op) and — unlike taking a
    /// materialized `&[usize]` — allocates no per-call scratch on the hot path.
    pub(in crate::segment) fn ensure(
        &mut self,
        segments: &[Arc<BaseSegment>],
        memtable_len: usize,
    ) {
        let n = segments.len() + 1;
        if self.seen.len() < n {
            self.seen.resize_with(n, Vec::new);
        }
        for (buf, seg) in self.seen.iter_mut().zip(segments.iter()) {
            let len = seg.len();
            if buf.len() < len {
                buf.resize(len, 0);
            }
        }
        // The memtable's seen-buffer is the last one (index `segments.len()`).
        let mbuf = &mut self.seen[segments.len()];
        if mbuf.len() < memtable_len {
            mbuf.resize(memtable_len, 0);
        }
    }
}

impl Default for MatchScratch {
    fn default() -> Self {
        Self::new()
    }
}

/// A borrowed view over the read-path state needed to match a title: the
/// normalizer, dictionary, base segments, and memtable. Both the mutable
/// [`Engine`](super::Engine) and an immutable [`EngineSnapshot`] expose exactly
/// these four, so [`MatchView::match_title`] is the single hot-path body for
/// both — there is no second copy to drift (a fix or new counter lands once).
pub(in crate::segment) struct MatchView<'a> {
    pub(in crate::segment) norm: &'a Normalizer,
    pub(in crate::segment) dict: &'a Dict,
    pub(in crate::segment) segments: &'a [Arc<BaseSegment>],
    pub(in crate::segment) memtable: &'a Segment,
}

impl MatchView<'_> {
    /// THE HOT PATH. Probe every base segment plus the memtable, union the
    /// matched logical IDs into `out`, then dedup. `#[inline]` + monomorphic, so
    /// each caller compiles to exactly the code it had when the body was
    /// duplicated (no call overhead, no dynamic dispatch). Allocation-free:
    /// scratch is reused via [`MatchScratch`].
    #[inline]
    pub(in crate::segment) fn match_title(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
    ) -> MatchStats {
        // per-segment seen-buffer sizing (base segments first, memtable last)
        let segments = self.segments;
        let n_base = segments.len();
        s.ensure(segments, self.memtable.len());

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
            .match_features(title, self.dict, &mut s.lc, &mut s.feats);
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
        for (i, base) in segments.iter().enumerate() {
            base.match_into(
                &feats,
                tmask,
                self.dict,
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
            self.dict,
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
}

impl std::fmt::Debug for EngineSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineSnapshot")
            .field("base_segments", &self.segments.len())
            .field("memtable_entries", &self.memtable.len())
            .field("query_store_entries", &self.query_store.len())
            .field("vocab_epoch", &self.vocab_epoch)
            .finish()
    }
}

impl EngineSnapshot {
    pub fn normalizer(&self) -> &Normalizer {
        &self.norm
    }

    pub fn dict(&self) -> &Dict {
        &self.dict
    }

    /// The vocabulary captured at snapshot time, if one was set. Lets read
    /// endpoints (`GET /_vocab`) serve the vocab from the lock-free snapshot
    /// without locking the engine (ADR-016).
    pub fn vocab(&self) -> Option<&Vocab> {
        self.vocab.as_deref()
    }

    /// The engine configuration captured at snapshot time. Lets `GET /_settings`
    /// serve the live settings from the lock-free snapshot (ADR-016).
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn num_queries(&self) -> usize {
        self.segments.iter().map(|s| s.len()).sum::<usize>() + self.memtable.len()
    }

    pub fn num_segments(&self) -> usize {
        self.segments.len() + 1
    }

    pub fn rejected_parse(&self) -> u64 {
        self.rejected_parse
    }

    pub fn rejected_class_d(&self) -> u64 {
        self.rejected_class_d
    }

    pub fn vocab_epoch(&self) -> u64 {
        self.vocab_epoch
    }

    pub fn wal_healthy(&self) -> bool {
        self.wal_healthy
    }

    pub fn persistence_healthy(&self) -> bool {
        self.persistence_healthy
    }

    pub fn skipped_segments(&self) -> usize {
        self.skipped_segments
    }

    pub fn stale_segment_count(&self) -> usize {
        let current = self.vocab_epoch;
        self.segments
            .iter()
            .filter(|s| s.vocab_epoch() < current)
            .count()
            + usize::from(self.memtable.vocab_epoch < current && !self.memtable.is_empty())
    }

    pub fn has_stale_segments(&self) -> bool {
        self.stale_segment_count() > 0
    }

    pub fn get_query_source(&self, logical_id: u64) -> Option<String> {
        self.query_store.get(logical_id)
    }

    pub fn explain_hit(
        &self,
        logical_id: u64,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let source = self.get_query_source(logical_id)?;
        let mut lc = String::new();
        let cq = crate::compile::compile_one_readonly(
            &source, logical_id, &self.norm, &self.dict, &mut lc,
        )
        .ok()?;
        Some(crate::explain::explain_match_structured(
            &cq, title, &self.norm, &self.dict,
        ))
    }

    pub fn class_counts(&self) -> [u64; 4] {
        let mut c = [0u64; 4];
        for seg in &self.segments {
            match seg.as_ref() {
                BaseSegment::Memory(s) => s.class_counts(&mut c),
                BaseSegment::Mmap(s) => s.class_counts(&mut c),
            }
        }
        self.memtable.class_counts(&mut c);
        c[3] = self.rejected_class_d;
        c
    }

    /// Per-segment introspection rows (base segments oldest-first, then the
    /// memtable), read lock-free from this snapshot. Backs the server's
    /// `GET /_cat/segments`. See [`SegmentInfo`](crate::events::SegmentInfo).
    pub fn segment_infos(&self) -> Vec<crate::events::SegmentInfo> {
        super::metrics::collect_segment_infos(&self.segments, &self.memtable, self.vocab_epoch)
    }

    pub fn metrics(&self) -> crate::events::EngineMetrics {
        let segment_sizes: Vec<usize> = self.segments.iter().map(|s| s.len()).collect();
        let segment_holes: Vec<f64> = self.segments.iter().map(|s| s.holes_ratio()).collect();
        crate::events::EngineMetrics {
            total_queries: self.num_queries(),
            base_segments: self.segments.len(),
            memtable_entries: self.memtable.len(),
            segment_sizes,
            segment_holes,
            rejected_parse: self.rejected_parse,
            rejected_class_d: self.rejected_class_d,
            dict_features: self.dict.len(),
            exact_bytes: self.segments.iter().map(|s| s.exact_bytes()).sum::<usize>()
                + self.memtable.exact_bytes(),
            index_bytes: self
                .segments
                .iter()
                .map(|s| s.main_bytes() + s.broad_bytes())
                .sum::<usize>()
                + self.memtable.main_bytes()
                + self.memtable.broad_bytes(),
            filter_bytes: self
                .segments
                .iter()
                .map(|s| s.filter_bytes())
                .sum::<usize>(),
            stale_segments: self.stale_segment_count(),
            dict_bytes: self.dict.heap_bytes(),
            query_store_bytes: self.query_store.resident_bytes(),
            logical_index_bytes: self
                .segments
                .iter()
                .map(|s| s.logical_index_bytes())
                .sum::<usize>()
                + self.memtable.logical_index_bytes(),
            alive_bytes: self.segments.iter().map(|s| s.alive_bytes()).sum::<usize>()
                + self.memtable.alive_bytes(),
            wal_size_bytes: self.wal_size_bytes,
            wal_pending_entries: self.wal_pending_entries,
        }
    }

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
        MatchView {
            norm: &self.norm,
            dict: &self.dict,
            segments: &self.segments,
            memtable: &self.memtable,
        }
        .match_title(title, s, out, include_broad)
    }

    /// Parallel matching on the snapshot.
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

    /// Batch match on the snapshot: selective lane per title + broad lane once
    /// per batch (columnar). Per-title `(index, matched_logical_ids)`, identical
    /// to per-title [`EngineSnapshot::match_title`]. Lock-free read path.
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
        super::broad_batch::batch_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
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
        super::broad_batch::batch_results_with_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
            },
            titles,
            opts,
        )
    }
}
