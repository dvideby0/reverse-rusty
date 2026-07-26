use super::{
    frozen_probe, infallible, DeadlineCheck, DeadlinePoll, Lane, MatchSink, MatchStats,
    MmapSegment, NoDeadline, VecSink,
};

impl MmapSegment {
    /// Filter check: is this signature key possibly in this segment?
    #[inline]
    pub(super) fn may_contain(&self, key: u64) -> bool {
        if self.filter_num_blocks == 0 {
            return true; // no filter = don't skip
        }
        crate::filter::bloom_check(key, self.filter_data(), self.filter_mask)
    }

    /// Probe this segment for one title — same semantics as Segment::match_into.
    #[allow(clippy::too_many_arguments)]
    pub fn match_into(
        &self,
        view: &crate::exact::TitleView,
        dict: &crate::dict::Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        lanes: crate::segment::ProbeLanes,
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn match_collect<
        C: MatchSink,
        P: crate::ownership::EmissionPolicy,
        D: DeadlineCheck,
    >(
        &self,
        view: &crate::exact::TitleView,
        dict: &crate::dict::Dict,
        epoch: u32,
        seen: &mut [u32],
        collector: &mut C,
        lanes: crate::segment::ProbeLanes,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
        emission: P,
        deadline: &mut DeadlinePoll<D>,
    ) -> Result<(), D::Cancelled> {
        let has_filter = self.filter_num_blocks > 0;
        // Graph-only phrase proxies are arity-1 MAIN signatures. The pair,
        // hot, and broad lanes are planned only from flat positive semantics.
        let probe_feats = view.probe;
        let feats = view.pos;
        if collector.should_stop() {
            return Ok(());
        }

        // arity-1 signatures
        for &f in probe_feats {
            if collector.should_stop() {
                return Ok(());
            }
            deadline.check_work()?;
            let key = crate::util::sig_key(&[f]);
            stats.probes_attempted += 1;
            if has_filter && !self.may_contain(key) {
                stats.probes_skipped += 1;
                continue;
            }
            self.probe_index(
                key,
                Lane::Main,
                epoch,
                view,
                seen,
                collector,
                pred,
                stats,
                emission,
                deadline,
            )?;
        }
        // arity-2 signatures
        for &h in feats {
            if collector.should_stop() {
                return Ok(());
            }
            deadline.check_work()?;
            if crate::compile::is_hot(dict, h) {
                for &o in feats {
                    if collector.should_stop() {
                        return Ok(());
                    }
                    deadline.check_work()?;
                    if o != h {
                        let (a, b) = if h < o { (h, o) } else { (o, h) };
                        let key = crate::util::sig_key(&[a, b]);
                        stats.probes_attempted += 1;
                        if has_filter && !self.may_contain(key) {
                            stats.probes_skipped += 1;
                            continue;
                        }
                        self.probe_index(
                            key,
                            Lane::Main,
                            epoch,
                            view,
                            seen,
                            collector,
                            pred,
                            stats,
                            emission,
                            deadline,
                        )?;
                    }
                }
            }
        }
        // Hot tier (class H, ADR-105): arity-1, probed on EVERY request — mirrors
        // `Segment::match_into` (see the invariants there); skipped outright when
        // the segment holds no hot entries.
        if lanes.include_hot && self.has_hot_entries() {
            for &f in feats {
                if collector.should_stop() {
                    return Ok(());
                }
                deadline.check_work()?;
                let key = crate::util::sig_key(&[f]);
                stats.probes_attempted += 1;
                if has_filter && !self.may_contain(key) {
                    stats.probes_skipped += 1;
                    continue;
                }
                self.probe_index(
                    key,
                    Lane::Hot,
                    epoch,
                    view,
                    seen,
                    collector,
                    pred,
                    stats,
                    emission,
                    deadline,
                )?;
            }
        }
        // broad lane
        if lanes.include_broad {
            for &f in feats {
                if collector.should_stop() {
                    return Ok(());
                }
                deadline.check_work()?;
                let key = crate::util::sig_key(&[f]);
                stats.probes_attempted += 1;
                if has_filter && !self.may_contain(key) {
                    stats.probes_skipped += 1;
                    continue;
                }
                self.probe_index(
                    key,
                    Lane::Broad,
                    epoch,
                    view,
                    seen,
                    collector,
                    pred,
                    stats,
                    emission,
                    deadline,
                )?;
            }
            // Universal signature: class-D always-candidates (ADR-068). Probed
            // unconditionally (the accept knob gates ingest, never visibility);
            // with no class-D entries this is one filter miss. Mirrors
            // `Segment::match_into` exactly.
            if collector.should_stop() {
                return Ok(());
            }
            deadline.check_work()?;
            let key = crate::util::universal_sig();
            stats.probes_attempted += 1;
            if has_filter && !self.may_contain(key) {
                stats.probes_skipped += 1;
            } else {
                self.probe_index(
                    key,
                    Lane::Broad,
                    epoch,
                    view,
                    seen,
                    collector,
                    pred,
                    stats,
                    emission,
                    deadline,
                )?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn probe_index<C: MatchSink, P: crate::ownership::EmissionPolicy, D: DeadlineCheck>(
        &self,
        key: u64,
        lane: Lane,
        epoch: u32,
        view: &crate::exact::TitleView,
        seen: &mut [u32],
        collector: &mut C,
        pred: &crate::exact::TagPredicate,
        stats: &mut MatchStats,
        emission: P,
        deadline: &mut DeadlinePoll<D>,
    ) -> Result<(), D::Cancelled> {
        let (slots, blob, mask) = match lane {
            Lane::Main => (self.main_slots(), self.main_blob(), self.main_mask),
            Lane::Broad => (self.broad_slots(), self.broad_blob(), self.broad_mask),
            Lane::Hot => (self.hot_slots(), self.hot_blob(), self.hot_mask),
        };

        if let Some(posting) = frozen_probe(key, slots, blob, mask) {
            stats.postings_scanned += posting.len() as u32;
            // Per-lane subset of postings_scanned — the memory-path `Segment::probe` and
            // the columnar reach paths both count it; this per-title mmap path once missed
            // the broad subset (codex, ADR-101), under-counting the exported per-shard
            // cost counters on durable shards.
            match lane {
                Lane::Broad => stats.broad_postings_scanned += posting.len() as u32,
                Lane::Hot => stats.hot_postings_scanned += posting.len() as u32,
                Lane::Main => {}
            }
            for &local in posting {
                if collector.should_stop() {
                    break;
                }
                deadline.check_work()?;
                if seen[local as usize] == epoch {
                    continue;
                }
                seen[local as usize] = epoch;
                stats.unique_candidates += 1;
                match lane {
                    Lane::Broad => stats.broad_candidates += 1,
                    Lane::Hot => stats.hot_candidates += 1,
                    Lane::Main => stats.main_candidates += 1,
                }
                if !self.alive_overlay[local as usize] {
                    continue;
                }
                if C::OBSERVE_CANDIDATES {
                    collector.on_candidate(self.logical(local));
                    if collector.should_stop() {
                        break;
                    }
                }
                // Tag filter (ADR-049) — applied post-candidate inside verify.
                if self.verify(local, view, pred) && emission.should_emit(self.placement(local)) {
                    crate::segment::collect_match_at(
                        collector,
                        self.logical(local),
                        local,
                        deadline,
                    )?;
                }
            }
        }
        Ok(())
    }
}
