use super::{
    infallible, is_hot, sig_key, CandidateIndex, DeadlineCheck, DeadlinePoll, Dict, MatchSink,
    MatchStats, NoDeadline, ProbeLane, ProbeLanes, Segment, VecSink,
};

impl Segment {
    /// Probe this segment for one title and append matched LOGICAL ids to `out`.
    /// This compatibility surface intentionally preserves its existing
    /// signature; engine-owned matching uses the collector-generic twin below.
    #[allow(clippy::too_many_arguments)]
    pub fn match_into(
        &self,
        view: &crate::exact::TitleView,
        dict: &Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        lanes: ProbeLanes,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
    ) {
        let mut ignored_emissions = 0;
        let mut collector = VecSink::new(out, &mut ignored_emissions);
        let mut deadline = DeadlinePoll::new(NoDeadline);
        infallible(self.match_collect(
            view,
            dict,
            epoch,
            seen,
            &mut collector,
            lanes,
            pred,
            stats,
            crate::ownership::EmitAll,
            &mut deadline,
        ));
    }

    /// Probe this segment for one title and emit matched LOGICAL ids.
    /// `seen` is this segment's epoch-stamp dedup array (size = self.len()).
    ///
    /// If the segment has an anchor filter (sealed base segments), each signature
    /// key is tested against the filter first. Keys that are definitely not
    /// present are skipped without touching the candidate index, cutting read
    /// amplification across multiple segments.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn match_collect<
        C: MatchSink,
        P: crate::ownership::EmissionPolicy,
        D: DeadlineCheck,
    >(
        &self,
        view: &crate::exact::TitleView,
        dict: &Dict,
        epoch: u32,
        seen: &mut [u32],
        collector: &mut C,
        lanes: ProbeLanes,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
        emission: P,
        deadline: &mut DeadlinePoll<D>,
    ) -> Result<(), D::Cancelled> {
        let filter = self.filter.as_ref();
        // Graph-only phrase proxies are arity-1 MAIN signatures. Keep them out
        // of the pair/hot/broad loops: those lanes are planned exclusively from
        // flat positive semantics, so widening them only manufactures probes
        // and false candidates that exact verification must reject.
        let probe_feats = view.probe;
        let feats = view.pos;
        if collector.should_stop() {
            return Ok(());
        }

        // arity-1 signatures (one per feature)
        for &f in probe_feats {
            if collector.should_stop() {
                return Ok(());
            }
            deadline.check_work()?;
            let key = sig_key(&[f]);
            stats.probes_attempted += 1;
            if let Some(flt) = filter {
                if !flt.may_contain(key) {
                    stats.probes_skipped += 1;
                    continue;
                }
            }
            self.probe(
                key,
                &self.main,
                epoch,
                view,
                seen,
                collector,
                pred,
                stats,
                ProbeLane::Main,
                emission,
                deadline,
            )?;
        }
        // arity-2 signatures: {hot feature} x {every other feature}. Deliberately
        // keyed to the FROZEN top-64 mask (`is_hot`), never θ — this loop is the
        // title side of the class-B pair predicate, and extending it is lever 3's
        // fenced change, not the hot tier's (ADR-105).
        for &h in feats {
            if collector.should_stop() {
                return Ok(());
            }
            deadline.check_work()?;
            if is_hot(dict, h) {
                for &o in feats {
                    if collector.should_stop() {
                        return Ok(());
                    }
                    deadline.check_work()?;
                    if o != h {
                        let (a, b) = if h < o { (h, o) } else { (o, h) };
                        let key = sig_key(&[a, b]);
                        stats.probes_attempted += 1;
                        if let Some(flt) = filter {
                            if !flt.may_contain(key) {
                                stats.probes_skipped += 1;
                                continue;
                            }
                        }
                        self.probe(
                            key,
                            &self.main,
                            epoch,
                            view,
                            seen,
                            collector,
                            pred,
                            stats,
                            ProbeLane::Main,
                            emission,
                            deadline,
                        )?;
                    }
                }
            }
        }
        // Hot tier (class H, ADR-105): arity-1 anchors, probed on EVERY request —
        // always-visible like main, so this is NOT gated by `include_broad`. The
        // `lanes.include_hot` gate only lets the batch driver lift the lane into
        // its columnar pass (evaluated exactly once either way). Skipped outright
        // when the segment holds no hot entries — one branch per segment per
        // title, the structural zero-overhead answer for hot-free corpora.
        if lanes.include_hot && self.has_hot_entries() {
            for &f in feats {
                if collector.should_stop() {
                    return Ok(());
                }
                deadline.check_work()?;
                let key = sig_key(&[f]);
                stats.probes_attempted += 1;
                if let Some(flt) = filter {
                    if !flt.may_contain(key) {
                        stats.probes_skipped += 1;
                        continue;
                    }
                }
                self.probe(
                    key,
                    &self.hot,
                    epoch,
                    view,
                    seen,
                    collector,
                    pred,
                    stats,
                    ProbeLane::Hot,
                    emission,
                    deadline,
                )?;
            }
        }
        // broad lane (arity-1 anchors), measured separately
        if lanes.include_broad {
            for &f in feats {
                if collector.should_stop() {
                    return Ok(());
                }
                deadline.check_work()?;
                let key = sig_key(&[f]);
                stats.probes_attempted += 1;
                if let Some(flt) = filter {
                    if !flt.may_contain(key) {
                        stats.probes_skipped += 1;
                        continue;
                    }
                }
                self.probe(
                    key,
                    &self.broad,
                    epoch,
                    view,
                    seen,
                    collector,
                    pred,
                    stats,
                    ProbeLane::Broad,
                    emission,
                    deadline,
                )?;
            }
            // Universal signature: class-D always-candidates (ADR-068). Probed
            // unconditionally — the accept knob gates ingest, never visibility, so a
            // stored entry stays reachable however the knob is later toggled. With no
            // class-D entries this is one filter (or hash) miss per segment.
            if collector.should_stop() {
                return Ok(());
            }
            deadline.check_work()?;
            let key = crate::util::universal_sig();
            stats.probes_attempted += 1;
            let skip = filter.is_some_and(|flt| !flt.may_contain(key));
            if skip {
                stats.probes_skipped += 1;
            } else {
                self.probe(
                    key,
                    &self.broad,
                    epoch,
                    view,
                    seen,
                    collector,
                    pred,
                    stats,
                    ProbeLane::Broad,
                    emission,
                    deadline,
                )?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn probe<C: MatchSink, P: crate::ownership::EmissionPolicy, D: DeadlineCheck>(
        &self,
        key: u64,
        index: &CandidateIndex,
        epoch: u32,
        view: &crate::exact::TitleView,
        seen: &mut [u32],
        collector: &mut C,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
        lane: ProbeLane,
        emission: P,
        deadline: &mut DeadlinePoll<D>,
    ) -> Result<(), D::Cancelled> {
        // Dedup Stage A: on a segment with shared body groups, a posting entry is
        // a group LEADER — verified once per body, emitted per alive/tag-passing
        // member. Dup-free segments (incl. every mmap-attached one) take the
        // exact pre-dedup path below: one segment-level branch, zero per-candidate
        // cost.
        let has_dups = self.has_dup_groups();
        if let Some(posting) = index.get(key) {
            stats.postings_scanned += posting.len() as u32;
            match lane {
                ProbeLane::Broad => stats.broad_postings_scanned += posting.len() as u32,
                ProbeLane::Hot => stats.hot_postings_scanned += posting.len() as u32,
                ProbeLane::Main => {}
            }
            let mut cancelled = None;
            posting.for_each_while(|local| {
                if collector.should_stop() {
                    return false;
                }
                if let Err(error) = deadline.check_work() {
                    cancelled = Some(error);
                    return false;
                }
                // dedup across signatures with an epoch stamp (O(1), no alloc)
                if seen[local as usize] == epoch {
                    return true;
                }
                seen[local as usize] = epoch;
                stats.unique_candidates += 1;
                match lane {
                    ProbeLane::Broad => stats.broad_candidates += 1,
                    ProbeLane::Hot => stats.hot_candidates += 1,
                    ProbeLane::Main => stats.main_candidates += 1,
                }
                if !has_dups {
                    if !self.alive[local as usize] {
                        return true; // tombstoned
                    }
                    if C::OBSERVE_CANDIDATES {
                        collector.on_candidate(self.exact.logical(local));
                        if collector.should_stop() {
                            return false;
                        }
                    }
                    // Tag filter (ADR-049) — applied post-candidate inside verify.
                    if self.exact.verify(local, view, pred)
                        && emission.should_emit(self.exact.placement(local))
                    {
                        if let Err(error) = crate::segment::collect_match_at(
                            collector,
                            self.exact.logical(local),
                            local,
                            deadline,
                        ) {
                            cancelled = Some(error);
                            return false;
                        }
                    }
                    return !collector.should_stop();
                }
                // Group-aware path. The leader may itself be tombstoned while a
                // member lives, so aliveness gates EMISSION, never the body
                // verification; the tag filter (per-member identity, ADR-049) is
                // likewise applied per member, after the shared body check.
                let members = self.members_of(local);
                if members.is_empty() && !self.alive[local as usize] {
                    return true; // tombstoned singleton — the cheap skip
                }
                // One stored leader posting retrieves every live identity in
                // its canonical-body group. Observe all of those identities
                // before shared-body verification so candidate diagnostics do
                // not confuse an exact rejection with a retrieval miss.
                if C::OBSERVE_CANDIDATES {
                    if self.alive[local as usize] {
                        collector.on_candidate(self.exact.logical(local));
                        if collector.should_stop() {
                            return false;
                        }
                    }
                    for &member in members {
                        if collector.should_stop() {
                            return false;
                        }
                        if let Err(error) = deadline.check_work() {
                            cancelled = Some(error);
                            return false;
                        }
                        if self.alive[member as usize] {
                            collector.on_candidate(self.exact.logical(member));
                            if collector.should_stop() {
                                return false;
                            }
                        }
                    }
                }
                if !self
                    .exact
                    .verify(local, view, &crate::exact::TagPredicate::empty())
                {
                    return true;
                }
                if self.alive[local as usize]
                    && pred.matches(self.exact.tags_of(local))
                    && emission.should_emit(self.exact.placement(local))
                {
                    if let Err(error) = crate::segment::collect_match_at(
                        collector,
                        self.exact.logical(local),
                        local,
                        deadline,
                    ) {
                        cancelled = Some(error);
                        return false;
                    }
                    if collector.should_stop() {
                        return false;
                    }
                }
                for &m in members {
                    // A canonical-body group can be arbitrarily large, and a
                    // dead, tag-filtered, or ownership-suppressed member never
                    // reaches the post-emission poll below. Poll before every
                    // member's filters so those groups remain cooperatively
                    // cancellable even when none of their members emits.
                    if collector.should_stop() {
                        return false;
                    }
                    if let Err(error) = deadline.check_work() {
                        cancelled = Some(error);
                        return false;
                    }
                    if self.alive[m as usize]
                        && pred.matches(self.exact.tags_of(m))
                        && emission.should_emit(self.exact.placement(m))
                    {
                        if let Err(error) = crate::segment::collect_match_at(
                            collector,
                            self.exact.logical(m),
                            m,
                            deadline,
                        ) {
                            cancelled = Some(error);
                            return false;
                        }
                        if collector.should_stop() {
                            return false;
                        }
                    }
                }
                true
            });
            if let Some(error) = cancelled {
                return Err(error);
            }
        }
        Ok(())
    }
}
