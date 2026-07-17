//! `MatchScratch` reusable buffers and `EngineSnapshot` — the immutable,
//! lock-free read view and THE HOT PATH (`match_title` and the rayon-parallel
//! batch matchers). Type definitions live in the `segment` module root.

use super::{
    infallible, BaseSegment, BatchMatchOptions, DeadlineAt, DeadlineCheck, EngineSnapshot,
    MatchCancelled, MatchScratch, MatchStats, NoDeadline, Segment,
};
use crate::collect::{AllCollector, MatchCollector, TopKCollector};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::vocab::Vocab;
use std::sync::Arc;
use std::time::Instant;

impl MatchScratch {
    pub fn new() -> Self {
        MatchScratch {
            lc: String::with_capacity(256),
            feats: Vec::with_capacity(64),
            feats_pos: Vec::with_capacity(64),
            norm: crate::normalize::NormScratch::new(),
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
    /// Request-scoped tag filter (ADR-049). `TagPredicate::empty()` ⇒ no filtering, so
    /// every existing (unfiltered) caller is byte-identical to before tags.
    pub(in crate::segment) pred: &'a crate::exact::TagPredicate,
}

impl MatchView<'_> {
    /// THE HOT PATH. Probe every base segment plus the memtable, union the
    /// matched logical IDs into `out`, then dedup. `#[inline]` + monomorphic, so
    /// each caller compiles to exactly the code it had when the body was
    /// duplicated (no call overhead, no dynamic dispatch). Allocation-free:
    /// scratch is reused via [`MatchScratch`].
    #[inline]
    pub(in crate::segment) fn match_title<D: DeadlineCheck>(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
        dl: D,
    ) -> Result<MatchStats, D::Cancelled> {
        self.match_title_with_policy(title, s, out, include_broad, dl, crate::ownership::EmitAll)
    }

    #[inline]
    pub(in crate::segment) fn match_title_with_policy<
        D: DeadlineCheck,
        P: crate::ownership::EmissionPolicy,
    >(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
        dl: D,
        emission: P,
    ) -> Result<MatchStats, D::Cancelled> {
        let mut collector = AllCollector::new(out);
        self.match_title_collect(title, s, &mut collector, include_broad, dl, emission)
    }

    /// Generic internal collector path. Public compatibility entry points use
    /// `AllCollector`; bounded local ranking uses `TopKCollector` through the
    /// same post-verification seam.
    #[inline]
    pub(in crate::segment) fn match_title_collect<
        D: DeadlineCheck,
        C: MatchCollector,
        P: crate::ownership::EmissionPolicy,
    >(
        &self,
        title: &str,
        s: &mut MatchScratch,
        collector: &mut C,
        include_broad: bool,
        dl: D,
        emission: P,
    ) -> Result<MatchStats, D::Cancelled> {
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
        collector.reset();

        // Cooperative-deadline entry check (ADR-099): a match that spent its whole
        // budget queued on the rayon pool dies here, before doing any work. The
        // unarmed monomorph compiles this away.
        dl.check()?;

        // 1) normalize -> the title feature view(s) (ADR-061). The default (no active multi-word
        // alias) takes the **single-view fast path** — one feature set, one mask, no second copy —
        // so it is byte-identical AND zero-overhead vs the pre-ADR path. Only when a multi-word
        // alias is active does `match_features_dual` produce the canonical `N(T)` (forbidden) +
        // the overlapping superset `P(T)` (retrieval/required/any-of). Take the buffers out so we
        // can iterate them while mutating `s.seen` (no aliasing, no allocation).
        let dual = self.norm.has_multiword_aliases();
        let (feats, feats_pos);
        if dual {
            self.norm.match_features_dual(
                title,
                self.dict,
                &mut s.lc,
                &mut s.norm,
                &mut s.feats,
                &mut s.feats_pos,
            );
            feats = std::mem::take(&mut s.feats);
            feats_pos = std::mem::take(&mut s.feats_pos);
        } else {
            self.norm
                .match_features(title, self.dict, &mut s.lc, &mut s.norm, &mut s.feats);
            feats = std::mem::take(&mut s.feats);
            feats_pos = Vec::new();
        }

        // 2) title common-mask word(s) + the verifier view.
        let neg_mask = self.title_mask(&feats);
        let view = if dual {
            crate::exact::TitleView::dual(self.title_mask(&feats_pos), &feats_pos, neg_mask, &feats)
        } else {
            crate::exact::TitleView::single(neg_mask, &feats)
        };

        let mut stats = MatchStats::default();

        // 3) probe every base segment, each with its own seen buffer. The cooperative
        // deadline is re-checked at each SEGMENT boundary (coarse — never per candidate,
        // the hot-path invariant); on expiry we fall through to the shared buffer-restore
        // epilogue and return Err with the output cleared (ADR-099).
        let mut cancelled = None;
        for (i, base) in segments.iter().enumerate() {
            if let Err(c) = dl.check() {
                cancelled = Some(c);
                break;
            }
            base.match_collect(
                &view,
                self.dict,
                epoch,
                &mut s.seen[i],
                collector,
                // The scalar path evaluates the always-visible hot tier INLINE
                // (include_hot is a batch-driver cost switch, never visibility).
                crate::segment::ProbeLanes {
                    include_broad,
                    include_hot: true,
                },
                self.pred,
                &mut stats,
                emission,
            );
        }
        if cancelled.is_none() {
            match dl.check() {
                Err(c) => cancelled = Some(c),
                Ok(()) => self.memtable.match_collect(
                    &view,
                    self.dict,
                    epoch,
                    &mut s.seen[n_base],
                    collector,
                    crate::segment::ProbeLanes {
                        include_broad,
                        include_hot: true,
                    },
                    self.pred,
                    &mut stats,
                    emission,
                ),
            }
        }

        // restore the reusable buffers (the positive buffer only when it was used)
        s.feats = feats;
        if dual {
            s.feats_pos = feats_pos;
        }
        if let Some(c) = cancelled {
            // Anti-partial guarantee at the lowest level: a cancelled match returns
            // NO ids, never a truncated union (ADR-099).
            collector.abort();
            return Err(c);
        }
        // 4) finalize after every lane and segment has emitted. A logical id can
        // live in more than one segment (for example base + an updated copy).
        let summary = collector.finish();
        stats.logical_emissions = stats
            .logical_emissions
            .saturating_add(summary.logical_emissions);
        stats.duplicate_emissions = stats
            .duplicate_emissions
            .saturating_add(summary.duplicate_emissions.unwrap_or(0));
        stats.matches = summary.retained as u32;
        Ok(stats)
    }

    /// The title's common-mask word for a feature view: bit `mask_bit(f)` set for each
    /// feature `f` that has a hot-mask slot. Computed per view (ADR-061); shared with the
    /// broad-batch driver, which builds the same two views.
    #[inline]
    pub(in crate::segment) fn title_mask(&self, feats: &[crate::dict::FeatureId]) -> u64 {
        let mut m = 0u64;
        for &f in feats {
            let b = self.dict.mask_bit(f);
            if b != crate::dict::NO_MASK_BIT {
                m |= 1u64 << b;
            }
        }
        m
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
    pub(crate) fn validate_ownership_for_shard(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), crate::ownership::OwnershipError> {
        for segment in &self.segments {
            for local in 0..segment.len() as u32 {
                segment
                    .placement(local)
                    .to_owned()
                    .validate_for_shard(position, generation, num_shards)?;
            }
        }
        for local in 0..self.memtable.len() as u32 {
            self.memtable
                .placement(local)
                .to_owned()
                .validate_for_shard(position, generation, num_shards)?;
        }
        Ok(())
    }

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

    /// Observe-first hot-tier telemetry (the Broad-Query Cost Program): accepted
    /// compiles since process start whose plan would reclassify to the hot tier
    /// under [`DEFAULT_HOT_ANCHOR_THETA`](crate::config::DEFAULT_HOT_ANCHOR_THETA).
    pub fn would_be_hot(&self) -> u64 {
        self.would_be_hot
    }

    /// Dedup Stage A telemetry (process-lifetime): accepted compile events.
    pub fn bodies_total(&self) -> u64 {
        self.bodies_total
    }

    /// Dedup Stage A telemetry: accepted compiles that joined an existing body
    /// group in their segment (what per-segment sharing actually captured).
    pub fn dup_joined(&self) -> u64 {
        self.dup_joined
    }

    /// Linear-counting estimate of DISTINCT canonical bodies seen since process
    /// start — global duplication, incl. the cross-segment share Stage A's
    /// per-segment groups cannot capture (Stage B sizing evidence). 0 until the
    /// first accepted compile.
    pub fn distinct_bodies_est(&self) -> u64 {
        self.distinct_bodies_est
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

    /// Winner-fetch lookup with pre-allocation byte credit. `Err(actual_len)`
    /// means the current source exists but does not fit; the source store checks
    /// its borrowed resident/mmap value before cloning.
    pub(crate) fn get_query_source_bounded(
        &self,
        logical_id: u64,
        max_bytes: usize,
    ) -> Result<Option<String>, usize> {
        self.query_store.get_bounded(logical_id, max_bytes)
    }

    pub fn explain_hit(
        &self,
        logical_id: u64,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let source = self.get_query_source(logical_id)?;
        self.explain_source(logical_id, &source, title)
    }

    /// Compile a structured explanation from already-fetched current source.
    /// Ranked delivery uses this to fetch and budget each winner source once.
    pub fn explain_source(
        &self,
        logical_id: u64,
        source: &str,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let mut lc = String::new();
        let cq = crate::compile::compile_one_readonly(
            source,
            logical_id,
            &self.norm,
            &self.dict,
            &mut lc,
            self.config.hot_anchor_threshold,
        )
        .ok()?;
        Some(crate::explain::explain_match_structured(
            &cq, title, &self.norm, &self.dict,
        ))
    }

    pub fn class_counts(&self) -> [u64; 5] {
        let mut c = [0u64; 5];
        for seg in &self.segments {
            match seg.as_ref() {
                BaseSegment::Memory(s) => s.class_counts(&mut c),
                BaseSegment::Mmap(s) => s.class_counts(&mut c),
            }
        }
        self.memtable.class_counts(&mut c);
        // c[3] = STORED class-D always-candidates (ADR-068), symmetric with A/B/C;
        // rejections are the separate `rejected_class_d` metric.
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
            would_be_hot: self.would_be_hot,
            bodies_total: self.bodies_total,
            dup_joined: self.dup_joined,
            distinct_bodies_est: self.distinct_bodies_est,
            dict_features: self.dict.len(),
            exact_bytes: self.segments.iter().map(|s| s.exact_bytes()).sum::<usize>()
                + self.memtable.exact_bytes(),
            index_bytes: self
                .segments
                .iter()
                .map(|s| s.main_bytes() + s.broad_bytes() + s.hot_bytes())
                .sum::<usize>()
                + self.memtable.main_bytes()
                + self.memtable.broad_bytes()
                + self.memtable.hot_bytes(),
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
    /// `Some(d)` re-checks the clock at entry and at each segment boundary, and once
    /// `Instant::now() >= d` abandons the match with [`MatchCancelled`] — `out` is
    /// cleared, so no partial result escapes. Cancellation is bounded staleness, not
    /// preemption: at most one segment's work runs past the deadline.
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
        let use_priority = match spec.priority_field.as_deref() {
            None => false,
            Some("priority") => true,
            Some(field) => {
                return Err(crate::rank::RankProgramError::UnsupportedField(
                    field.to_string(),
                ));
            }
        };
        let boosts = spec
            .boosts
            .iter()
            .map(|(key, value, weight)| (self.tag_dict.get_or_synthetic(key, value), *weight))
            .collect();
        Ok(crate::rank::CompiledRankProgram::new(use_priority, boosts))
    }

    /// The live `TagId` slice for a matched logical id, picking the NEWEST live
    /// copy. Ordering is newest-first at both levels: the memtable before the base
    /// segments (all writes land in the memtable), base segments newest→oldest
    /// (`segments` is oldest-first, so walk it reversed), AND **within** each
    /// container the locals slice reversed — `locals_for_logical` lists a logical
    /// id's physical copies in ascending (insertion) order, so the LAST live local
    /// is the newest version. This matters when a logical id has two live copies in
    /// one container (e.g. a re-`PUT`/`insert_live` that has not yet tombstoned the
    /// old copy, or a flush of such a memtable). Returns `None` if no live copy
    /// exists — not expected for a just-matched id, but total for safety.
    fn tags_for_logical(&self, logical_id: u64) -> Option<&[crate::tagdict::TagId]> {
        for &local in self.memtable.locals_for_logical(logical_id).iter().rev() {
            if self.memtable.is_alive(local) {
                return Some(self.memtable.tags_of(local));
            }
        }
        for seg in self.segments.iter().rev() {
            for &local in seg.locals_for_logical(logical_id).iter().rev() {
                if seg.is_alive(local) {
                    return Some(seg.tags_of(local));
                }
            }
        }
        None
    }

    /// Newest-live typed rank values and tags for a logical id. The same reverse
    /// walk as compatibility ranking prevents an older physical duplicate from
    /// determining score merely because it emitted first.
    fn rank_metadata_for_logical(
        &self,
        logical_id: u64,
    ) -> Option<(crate::rank::RankValues, &[crate::tagdict::TagId])> {
        for &local in self.memtable.locals_for_logical(logical_id).iter().rev() {
            if self.memtable.is_alive(local) {
                let tags = self.memtable.tags_of(local);
                let mut rank = self.memtable.rank_values(local);
                if rank.priority == 0 {
                    rank.priority = self.tag_dict.legacy_priority_for_tags(tags);
                }
                return Some((rank, tags));
            }
        }
        for seg in self.segments.iter().rev() {
            for &local in seg.locals_for_logical(logical_id).iter().rev() {
                if seg.is_alive(local) {
                    let tags = seg.tags_of(local);
                    let mut rank = seg.rank_values(local);
                    if rank.priority == 0 {
                        rank.priority = self.tag_dict.legacy_priority_for_tags(tags);
                    }
                    return Some((rank, tags));
                }
            }
        }
        None
    }

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
        let threshold =
            usize::try_from(options.track_total_hits_up_to).unwrap_or(crate::result::MAX_TOP_K);
        let mut collector = TopKCollector::new(options.size, threshold, |logical_id| {
            self.rank_metadata_for_logical(logical_id)
                .map_or(0, |(values, tags)| {
                    crate::rank::score_program(values, tags, program)
                })
        });
        let view = MatchView {
            norm: &self.norm,
            dict: &self.dict,
            segments: &self.segments,
            memtable: &self.memtable,
            pred,
        };
        let include_broad = options.query_scope == crate::result::QueryScope::WithBroad;
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
                .map_err(crate::rank::RankedMatchError::Cancelled)?,
            None => infallible(view.match_title_collect(
                title,
                scratch,
                &mut collector,
                include_broad,
                NoDeadline,
                emission,
            )),
        };
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
    /// cooperative deadline (ADR-099). `None` delegates unarmed (byte-identical). Armed,
    /// every in-flight title self-checks per segment and the `Result` collect
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
        super::broad_batch::batch_results(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
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
        super::broad_batch::batch_results_with_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                pred,
            },
            titles,
            opts,
        )
    }

    /// [`match_titles_batch_with_stats_filtered`](Self::match_titles_batch_with_stats_filtered)
    /// with an optional cooperative deadline (ADR-099). `None` delegates unarmed
    /// (byte-identical). Armed, each chunk checks per title (Phase 0) and per segment
    /// block (the columnar broad pass), and the first cancellation abandons the whole
    /// batch — never a partially-filled `responses[]`.
    pub fn try_match_titles_batch_with_stats_filtered(
        &self,
        titles: &[impl AsRef<str> + Sync],
        opts: BatchMatchOptions,
        pred: &TagPredicate,
        deadline: Option<Instant>,
    ) -> Result<super::BatchResultsWithStats, MatchCancelled> {
        super::broad_batch::try_batch_results_with_stats(
            &MatchView {
                norm: &self.norm,
                dict: &self.dict,
                segments: &self.segments,
                memtable: &self.memtable,
                pred,
            },
            titles,
            opts,
            deadline,
        )
    }
}
