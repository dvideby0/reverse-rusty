use super::{
    AllCollector, Arc, BaseSegment, CandidateHitCollector, CostClass, DeadlineCheck, DeadlinePoll,
    Dict, EngineSnapshot, MatchCollector, MatchScratch, MatchStats, Normalizer, Segment,
    TagPredicate,
};

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

pub(super) struct ExhaustiveDeduper<'a, P> {
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
    pub(super) fn new(
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

    pub(super) fn is_first_matching(
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
