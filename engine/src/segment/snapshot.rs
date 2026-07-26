//! `MatchScratch` reusable buffers and `EngineSnapshot` — the immutable,
//! lock-free read view and THE HOT PATH (`match_title` and the rayon-parallel
//! batch matchers). Type definitions live in the `segment` module root.

mod read;

use super::{
    infallible, BaseSegment, BatchMatchOptions, DeadlineAt, DeadlineCheck, DeadlinePoll,
    EngineSnapshot, MatchCancelled, MatchScratch, MatchStats, NoDeadline, Segment,
};
use crate::collect::{
    AllCollector, CandidateHitCollector, ChunkCollector, MatchCollector, TopKCollector, TopKScorer,
};
use crate::compile::CostClass;
use crate::delivery::{
    ChunkSink, ExhaustiveMatchError, ExhaustiveMatchResult, ExhaustiveOptions, MAX_MATCH_CHUNK_SIZE,
};
use crate::dict::Dict;
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use std::sync::Arc;
use std::time::Instant;

impl MatchScratch {
    pub fn new() -> Self {
        MatchScratch {
            lc: String::with_capacity(256),
            feats: Vec::with_capacity(64),
            feats_pos: Vec::with_capacity(64),
            probe_feats: Vec::with_capacity(64),
            phrase_arcs: Vec::with_capacity(64),
            phrase_arcs_pos: Vec::with_capacity(64),
            phrase_match: std::cell::RefCell::new(crate::exact::PhraseMatchScratch::with_capacity(
                64,
            )),
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
    /// Cached aggregate capability from the owning engine/snapshot.
    pub(in crate::segment) has_phrase_predicates: bool,
    /// Request-scoped tag filter (ADR-049). `TagPredicate::empty()` ⇒ no filtering, so
    /// every existing (unfiltered) caller is byte-identical to before tags.
    pub(in crate::segment) pred: &'a crate::exact::TagPredicate,
}

/// Exhaustive-only logical-id dedup (ADR-114). It reuses the snapshot's reverse
/// indexes to select the deterministic first physical copy that matches this
/// already-normalized title, avoiding a result-sized `HashSet`.
struct PositionedTitle {
    positions: u32,
    pos_graph_complete: bool,
    neg_arcs: Vec<crate::normalize::PositionArc>,
    pos_arcs: Vec<crate::normalize::PositionArc>,
    phrase_match: std::cell::RefCell<crate::exact::PhraseMatchScratch>,
}

struct ExhaustiveDeduper<'a, P> {
    snapshot: &'a EngineSnapshot,
    pred: &'a TagPredicate,
    include_broad: bool,
    emission: P,
    neg: Vec<crate::dict::FeatureId>,
    pos: Vec<crate::dict::FeatureId>,
    probe: Vec<crate::dict::FeatureId>,
    neg_mask: u64,
    pos_mask: u64,
    dual: bool,
    positioned: Option<PositionedTitle>,
}

impl<'a, P: crate::ownership::EmissionPolicy> ExhaustiveDeduper<'a, P> {
    fn new(
        snapshot: &'a EngineSnapshot,
        title: &str,
        pred: &'a TagPredicate,
        include_broad: bool,
        emission: P,
    ) -> Self {
        let mut lc = String::new();
        let mut norm = crate::normalize::NormScratch::new();
        let mut neg = Vec::new();
        let mut pos = Vec::new();
        let mut probe = Vec::new();
        let mut neg_arcs = Vec::new();
        let mut pos_arcs = Vec::new();
        let has_positioned = snapshot.has_phrase_predicates;
        let dual = snapshot.norm.has_multiword_aliases() || has_positioned;
        let positioned = if has_positioned {
            let (positions, pos_graph_complete) = snapshot.norm.match_phrase_views(
                title,
                &snapshot.dict,
                &mut lc,
                &mut norm,
                &mut neg,
                &mut pos,
                &mut probe,
                &mut neg_arcs,
                &mut pos_arcs,
            );
            Some(PositionedTitle {
                positions,
                pos_graph_complete,
                neg_arcs,
                pos_arcs,
                phrase_match: std::cell::RefCell::new(
                    crate::exact::PhraseMatchScratch::with_capacity(64),
                ),
            })
        } else if dual {
            snapshot.norm.match_features_dual(
                title,
                &snapshot.dict,
                &mut lc,
                &mut norm,
                &mut neg,
                &mut pos,
            );
            None
        } else {
            snapshot
                .norm
                .match_features(title, &snapshot.dict, &mut lc, &mut norm, &mut neg);
            None
        };
        let neg_mask = title_mask(&snapshot.dict, &neg);
        let pos_mask = if dual {
            title_mask(&snapshot.dict, &pos)
        } else {
            neg_mask
        };
        Self {
            snapshot,
            pred,
            include_broad,
            emission,
            neg,
            pos,
            probe,
            neg_mask,
            pos_mask,
            dual,
            positioned,
        }
    }

    fn view(&self) -> crate::exact::TitleView<'_> {
        if let Some(positioned) = &self.positioned {
            crate::exact::TitleView::dual_positioned(
                &self.probe,
                self.pos_mask,
                &self.pos,
                positioned.positions,
                &positioned.pos_arcs,
                positioned.pos_graph_complete,
                self.neg_mask,
                &self.neg,
                positioned.positions,
                &positioned.neg_arcs,
                &positioned.phrase_match,
            )
        } else if self.dual {
            crate::exact::TitleView::dual(self.pos_mask, &self.pos, self.neg_mask, &self.neg)
        } else {
            crate::exact::TitleView::single(self.neg_mask, &self.neg)
        }
    }

    fn visible(&self, class: CostClass) -> bool {
        self.include_broad || !matches!(class, CostClass::C | CostClass::D)
    }

    fn base_matches(&self, segment: &BaseSegment, local: u32) -> bool {
        segment.is_alive(local)
            && segment
                .class_of(local)
                .is_some_and(|class| self.visible(class))
            && self.emission.should_emit(segment.placement(local))
            && segment.verify_local(local, &self.view(), self.pred)
    }

    fn memtable_matches(&self, local: u32) -> bool {
        self.snapshot.memtable.is_alive(local)
            && self
                .snapshot
                .memtable
                .class_of(local)
                .is_some_and(|class| self.visible(class))
            && self
                .emission
                .should_emit(self.snapshot.memtable.placement(local))
            && self
                .snapshot
                .memtable
                .verify_local(local, &self.view(), self.pred)
    }

    fn is_first_matching(
        &mut self,
        source: usize,
        current: u32,
        logical: u64,
        should_stop: &mut dyn FnMut() -> bool,
    ) -> bool {
        let base_count = self.snapshot.segments.len();
        let preceding_bases = source.min(base_count);
        for segment in self.snapshot.segments.iter().take(preceding_bases) {
            for &local in segment.locals_for_logical(logical) {
                if should_stop() {
                    return false;
                }
                if self.base_matches(segment, local) {
                    return false;
                }
            }
        }
        if source < base_count {
            for &local in self.snapshot.segments[source].locals_for_logical(logical) {
                if local >= current {
                    break;
                }
                if should_stop() {
                    return false;
                }
                if self.base_matches(&self.snapshot.segments[source], local) {
                    return false;
                }
            }
            return true;
        }
        for &local in self.snapshot.memtable.locals_for_logical(logical) {
            if local >= current {
                break;
            }
            if should_stop() {
                return false;
            }
            if self.memtable_matches(local) {
                return false;
            }
        }
        true
    }
}

fn title_mask(dict: &Dict, feats: &[crate::dict::FeatureId]) -> u64 {
    feats.iter().fold(0u64, |mask, &feature| {
        let bit = dict.mask_bit(feature);
        if bit == crate::dict::NO_MASK_BIT {
            mask
        } else {
            mask | (1u64 << bit)
        }
    })
}

impl MatchView<'_> {
    #[inline]
    pub(in crate::segment) fn has_phrase_predicates(&self) -> bool {
        self.has_phrase_predicates
    }

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

    /// Run the real stored posting/filter/lane traversal and stop when it
    /// reaches `logical_id`, before exact verification. Diagnostic only; the
    /// ordinary match path uses its existing collectors whose candidate
    /// callback compiles to a no-op.
    pub(in crate::segment) fn candidate_hit<D: DeadlineCheck>(
        &self,
        title: &str,
        logical_id: u64,
        s: &mut MatchScratch,
        include_broad: bool,
        dl: D,
    ) -> Result<bool, D::Cancelled> {
        let mut collector = CandidateHitCollector::new(logical_id);
        self.match_title_collect(
            title,
            s,
            &mut collector,
            include_broad,
            dl,
            crate::ownership::EmitAll,
        )?;
        Ok(collector.hit())
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

        // Exhaustive sinks can be cancelled before matching produces its first
        // member (for example DELETE on a zero-result job). Poll before
        // normalization so that cancellation does not depend on chunk emission.
        if collector.should_stop() {
            return Ok(MatchStats::default());
        }

        let mut deadline = DeadlinePoll::new(dl);

        // Cooperative-deadline entry check (ADR-099): a match that spent its whole
        // budget queued on the rayon pool dies here, before doing any work. The
        // unarmed monomorph compiles this away.
        deadline.check_now()?;

        // 1) normalize -> the title feature view(s) (ADR-061). The default (no active multi-word
        // alias) takes the **single-view fast path** — one feature set, one mask, no second copy —
        // so it is byte-identical AND zero-overhead vs the pre-ADR path. Only when a multi-word
        // alias is active does `match_features_dual` produce the canonical `N(T)` (forbidden) +
        // the overlapping superset `P(T)` (retrieval/required/any-of). Take the buffers out so we
        // can iterate them while mutating `s.seen` (no aliasing, no allocation).
        let positioned = self.has_phrase_predicates();
        let dual = self.norm.has_multiword_aliases() || positioned;
        let (
            feats,
            feats_pos,
            probe_feats,
            phrase_arcs,
            phrase_arcs_pos,
            phrase_positions,
            pos_graph_complete,
        );
        if positioned {
            (phrase_positions, pos_graph_complete) = self.norm.match_phrase_views(
                title,
                self.dict,
                &mut s.lc,
                &mut s.norm,
                &mut s.feats,
                &mut s.feats_pos,
                &mut s.probe_feats,
                &mut s.phrase_arcs,
                &mut s.phrase_arcs_pos,
            );
            feats = std::mem::take(&mut s.feats);
            feats_pos = std::mem::take(&mut s.feats_pos);
            probe_feats = std::mem::take(&mut s.probe_feats);
            phrase_arcs = std::mem::take(&mut s.phrase_arcs);
            phrase_arcs_pos = std::mem::take(&mut s.phrase_arcs_pos);
        } else if dual {
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
            probe_feats = Vec::new();
            phrase_arcs = Vec::new();
            phrase_arcs_pos = Vec::new();
            phrase_positions = 0;
            pos_graph_complete = true;
        } else {
            self.norm
                .match_features(title, self.dict, &mut s.lc, &mut s.norm, &mut s.feats);
            feats = std::mem::take(&mut s.feats);
            feats_pos = Vec::new();
            probe_feats = Vec::new();
            phrase_arcs = Vec::new();
            phrase_arcs_pos = Vec::new();
            phrase_positions = 0;
            pos_graph_complete = true;
        }

        // 2) title common-mask word(s) + the verifier view.
        let neg_mask = self.title_mask(&feats);
        let view = if positioned {
            crate::exact::TitleView::dual_positioned(
                &probe_feats,
                self.title_mask(&feats_pos),
                &feats_pos,
                phrase_positions,
                &phrase_arcs_pos,
                pos_graph_complete,
                neg_mask,
                &feats,
                phrase_positions,
                &phrase_arcs,
                &s.phrase_match,
            )
        } else if dual {
            crate::exact::TitleView::dual(self.title_mask(&feats_pos), &feats_pos, neg_mask, &feats)
        } else {
            crate::exact::TitleView::single(neg_mask, &feats)
        };

        let mut stats = MatchStats::default();

        // 3) probe every base segment, each with its own seen buffer. The cooperative
        // deadline is re-checked at each SEGMENT boundary and every bounded run of
        // posting/candidate/body-group work inside it (ADR-123). On expiry we
        // fall through to the shared buffer-restore epilogue and return Err with
        // the output cleared.
        let mut cancelled = None;
        for (i, base) in segments.iter().enumerate() {
            if let Err(c) = deadline.check_now() {
                cancelled = Some(c);
                break;
            }
            collector.begin_source(i);
            if let Err(c) = base.match_collect(
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
                &mut deadline,
            ) {
                cancelled = Some(c);
                break;
            }
            if collector.should_stop() {
                break;
            }
        }
        if cancelled.is_none() && !collector.should_stop() {
            if let Err(c) = deadline.check_now() {
                cancelled = Some(c);
            } else {
                collector.begin_source(n_base);
                if let Err(c) = self.memtable.match_collect(
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
                    &mut deadline,
                ) {
                    cancelled = Some(c);
                }
            }
        }

        // restore the reusable buffers (the positive buffer only when it was used)
        s.feats = feats;
        if dual {
            s.feats_pos = feats_pos;
        }
        if positioned {
            s.probe_feats = probe_feats;
            s.phrase_arcs = phrase_arcs;
            s.phrase_arcs_pos = phrase_arcs_pos;
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

    fn tags_for_logical(&self, logical_id: u64) -> Option<&[crate::tagdict::TagId]> {
        self.source_metadata_for_logical(logical_id)
            .map(|(_, _, tags)| tags)
    }

    /// Newest-live typed rank values and tags for a logical id. The same reverse
    /// walk as compatibility ranking prevents an older physical duplicate from
    /// determining score merely because it emitted first.
    fn rank_metadata_for_logical(
        &self,
        logical_id: u64,
    ) -> Option<(crate::rank::RankValues, &[crate::tagdict::TagId])> {
        self.rank_metadata_for_logical_with_poll(logical_id, &mut || false)
    }

    /// Cancellable exhaustive counterpart to [`Self::rank_metadata_for_logical`].
    /// A legacy logical id may have arbitrarily many newer tombstoned physical
    /// copies, so poll between reverse-index entries rather than turning score
    /// resolution into one uninterruptible scan.
    fn rank_metadata_for_logical_with_poll<C>(
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
        if let Some(at) = deadline {
            let mut collector = TopKCollector::new_polling(
                options.size,
                threshold,
                options.search_after,
                self.program_scorer_with_poll(program),
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
                self.program_scorer(program),
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
        super::broad_batch::batch_results(
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
        super::broad_batch::batch_stats(
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
        super::broad_batch::batch_results_with_stats(
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
    ) -> Result<super::BatchResultsWithStats, MatchCancelled> {
        super::broad_batch::try_batch_results_with_stats(
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

#[cfg(test)]
mod exhaustive_dedup_tests {
    use super::*;

    struct CancelImmediately {
        polls: usize,
    }

    impl ChunkSink for CancelImmediately {
        fn send_chunk(
            &mut self,
            _chunk: &crate::delivery::MatchChunk,
        ) -> Result<(), crate::delivery::ChunkSinkError> {
            Ok(())
        }

        fn check_cancelled(&mut self) -> Result<(), crate::delivery::ChunkSinkError> {
            self.polls += 1;
            Err(crate::delivery::ChunkSinkError::new(
                "already cancelled before setup",
            ))
        }
    }

    #[test]
    fn exhaustive_entry_polls_before_setup() {
        let engine = crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        let snapshot = engine.snapshot();
        let mut sink = CancelImmediately { polls: 0 };
        let error = snapshot
            .try_match_title_chunks(
                "an alias-heavy or otherwise expensive title must not be normalized",
                ExhaustiveOptions::default(),
                None,
                &TagPredicate::empty(),
                &mut MatchScratch::new(),
                None,
                &mut sink,
            )
            .expect_err("pre-cancelled entry must fail before setup");
        assert!(matches!(error, ExhaustiveMatchError::Sink(_)));
        assert_eq!(sink.polls, 1);
    }

    #[test]
    fn legacy_duplicate_scan_polls_cancellation_between_physical_copies() {
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        for version in 0..2_048 {
            engine
                .try_insert_live("zzlegacyhay", 7, version)
                .expect("legacy duplicate");
        }
        engine.flush();
        engine
            .try_insert_live("zzmatchingneedle", 7, 2_048)
            .expect("current matching copy");
        let snapshot = engine.snapshot();
        assert_eq!(
            snapshot.segments[0].locals_for_logical(7).len(),
            2_048,
            "test must exercise a long reverse-index walk"
        );
        let current = snapshot.memtable.locals_for_logical(7)[0];
        let pred = TagPredicate::empty();
        let mut deduper = ExhaustiveDeduper::new(
            &snapshot,
            "zzmatchingneedle",
            &pred,
            true,
            crate::ownership::EmitAll,
        );
        let mut polls = 0usize;
        let accepted = deduper.is_first_matching(snapshot.segments.len(), current, 7, &mut || {
            polls += 1;
            polls >= 17
        });

        assert!(!accepted, "a cancelled walk must not emit its current copy");
        assert_eq!(
            polls, 17,
            "the walk must stop at the cancellation poll, not scan all duplicates"
        );
    }

    #[test]
    fn ranked_metadata_scan_polls_cancellation_between_legacy_copies() {
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        engine
            .try_insert_live("zzrankcancel", 7, 0)
            .expect("oldest live copy");
        for version in 1..=2_048 {
            let crate::segment::InsertOutcome::Inserted(local) = engine
                .try_insert_live("zzrankcancel", 7, version)
                .expect("newer legacy copy")
            else {
                panic!("selective test query was unexpectedly rejected");
            };
            engine.tombstone(local).expect("tombstone newer copy");
        }
        let snapshot = engine.snapshot();
        assert_eq!(
            snapshot.memtable.locals_for_logical(7).len(),
            2_049,
            "test must exercise a long newest-first metadata walk"
        );

        let mut polls = 0usize;
        let metadata = snapshot.rank_metadata_for_logical_with_poll(7, &mut || {
            polls += 1;
            polls >= 17
        });
        assert!(
            metadata.is_none(),
            "a cancelled metadata scan must not return an older score"
        );
        assert_eq!(
            polls, 17,
            "the walk must stop at the cancellation poll, not scan all copies"
        );
    }
}

#[cfg(test)]
mod bounded_deadline_tests {
    use super::*;
    use crate::collect::MatchSink;
    use crate::ownership::EmissionPolicy;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    struct CancelOnCheck<'a> {
        checks: &'a AtomicUsize,
        cancel_at: usize,
    }

    impl DeadlineCheck for CancelOnCheck<'_> {
        const ARMED: bool = true;
        type Cancelled = MatchCancelled;

        fn check(self) -> Result<(), Self::Cancelled> {
            let current = self.checks.fetch_add(1, Ordering::Relaxed) + 1;
            if current >= self.cancel_at {
                Err(MatchCancelled)
            } else {
                Ok(())
            }
        }
    }

    #[derive(Clone, Copy)]
    struct CountEmissions<'a>(&'a AtomicUsize);

    impl EmissionPolicy for CountEmissions<'_> {
        fn should_emit(self, _placement: crate::ownership::QueryPlacementRef<'_>) -> bool {
            self.0.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    #[derive(Default)]
    struct StopAfterFirstMatch {
        matches: usize,
        stopped: bool,
    }

    impl MatchSink for StopAfterFirstMatch {
        fn on_match(&mut self, _logical_id: u64) {
            self.matches += 1;
            self.stopped = true;
        }

        fn should_stop(&mut self) -> bool {
            self.stopped
        }
    }

    #[test]
    fn collector_failure_precedes_a_simultaneous_deadline_poll() {
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        engine
            .try_insert_live("anchorw", 1, 1)
            .expect("insert matching row");
        let snapshot = engine.snapshot();
        let mut title_scratch = MatchScratch::new();
        snapshot.norm.match_features(
            "anchorw",
            &snapshot.dict,
            &mut title_scratch.lc,
            &mut title_scratch.norm,
            &mut title_scratch.feats,
        );
        let title = crate::exact::TitleView::single(0, &title_scratch.feats);
        let mut seen = vec![0; snapshot.memtable.len()];
        let mut collector = StopAfterFirstMatch::default();
        let pred = TagPredicate::empty();
        let mut stats = MatchStats::default();
        let checks = AtomicUsize::new(0);
        let mut deadline = DeadlinePoll::new(CancelOnCheck {
            checks: &checks,
            cancel_at: 1,
        });
        // The anchor probe and its posting consume two work units. The next
        // loop edge is therefore both the first deadline sample and the first
        // chance to observe the collector's already-recorded failure.
        deadline.remaining = 3;

        let result = snapshot.memtable.match_collect(
            &title,
            &snapshot.dict,
            1,
            &mut seen,
            &mut collector,
            crate::segment::ProbeLanes {
                include_broad: false,
                include_hot: true,
            },
            &pred,
            &mut stats,
            crate::ownership::EmitAll,
            &mut deadline,
        );

        assert_eq!(result, Ok(()));
        assert_eq!(collector.matches, 1);
        assert!(collector.stopped);
        assert_eq!(
            checks.load(Ordering::Relaxed),
            0,
            "an already-recorded collector failure must win before the clock poll"
        );
    }

    #[test]
    fn counter_deadline_stops_inside_one_body_group_and_clears_results() {
        const ROWS: u64 = 4_096;
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        for logical in 0..ROWS {
            engine
                .try_insert_live("anchorw", logical, 1)
                .expect("insert duplicate body");
        }
        let snapshot = engine.snapshot();
        assert!(snapshot.segments.is_empty());
        assert!(snapshot.memtable.has_dup_groups());

        let pred = TagPredicate::empty();
        let view = MatchView {
            norm: &snapshot.norm,
            dict: &snapshot.dict,
            segments: &snapshot.segments,
            memtable: &snapshot.memtable,
            has_phrase_predicates: snapshot.has_phrase_predicates,
            pred: &pred,
        };
        let checks = AtomicUsize::new(0);
        let emissions = AtomicUsize::new(0);
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let result = view.match_title_with_policy(
            "anchorw",
            &mut scratch,
            &mut out,
            true,
            CancelOnCheck {
                checks: &checks,
                // Entry + memtable boundary pass; the first in-segment sample
                // cancels deterministically without consulting wall time.
                cancel_at: 3,
            },
            CountEmissions(&emissions),
        );

        assert_eq!(result, Err(MatchCancelled));
        assert_eq!(checks.load(Ordering::Relaxed), 3);
        assert!(
            emissions.load(Ordering::Relaxed) < ROWS as usize,
            "the sampler must stop within the group instead of finishing the segment"
        );
        assert!(
            out.is_empty(),
            "the lowest-level abort must clear every pre-cancellation emission"
        );
    }

    #[test]
    fn ranked_scalar_metadata_walk_uses_the_active_sampler_and_aborts() {
        const LEGACY_COPIES: u32 = 2_048;
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        engine
            .try_insert_live("zzrankneedle", 7, 0)
            .expect("live matching copy");
        for version in 1..=LEGACY_COPIES {
            let query = format!("zzlegacyterm{version}");
            let crate::segment::InsertOutcome::Inserted(local) = engine
                .try_insert_live(&query, 7, version)
                .expect("newer legacy copy")
            else {
                panic!("selective test query was unexpectedly rejected");
            };
            engine.tombstone(local).expect("tombstone legacy copy");
        }
        let snapshot = engine.snapshot();
        assert_eq!(
            snapshot.memtable.locals_for_logical(7).len(),
            LEGACY_COPIES as usize + 1
        );
        assert!(
            !snapshot.memtable.has_dup_groups(),
            "unique legacy bodies keep cancellation inside rank metadata, not body emission"
        );

        let program = snapshot
            .compile_rank_program(&crate::rank::RankProgramSpec::default())
            .expect("rank program");
        let pred = TagPredicate::empty();
        let view = MatchView {
            norm: &snapshot.norm,
            dict: &snapshot.dict,
            segments: &snapshot.segments,
            memtable: &snapshot.memtable,
            has_phrase_predicates: snapshot.has_phrase_predicates,
            pred: &pred,
        };
        let checks = AtomicUsize::new(0);
        let mut collector =
            TopKCollector::new_polling(10, 100, None, snapshot.program_scorer_with_poll(&program));
        let mut scratch = MatchScratch::new();
        let result = view.match_title_collect(
            "zzrankneedle",
            &mut scratch,
            &mut collector,
            false,
            CancelOnCheck {
                checks: &checks,
                // Entry + memtable boundary pass. The next fixed-interval
                // sample must fire inside newest-live rank metadata.
                cancel_at: 3,
            },
            crate::ownership::EmitAll,
        );

        assert_eq!(result, Err(MatchCancelled));
        assert_eq!(checks.load(Ordering::Relaxed), 3);
        assert!(
            collector.winners().is_empty(),
            "a cancelled rank metadata walk must not leak a partial winner"
        );
        assert_eq!(collector.total_hits().value, 0);
    }
}
