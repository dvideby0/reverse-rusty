use super::{
    frozen_probe, CostClass, DeadlineCheck, DeadlinePoll, FeatureId, MatchStats, MmapSegment,
};

impl MmapSegment {
    /// Integer-only exact verification — same logic as ExactStore::verify but
    /// operating on mmap'd slices. `pred` is the request's compiled tag filter
    /// (`TagPredicate::empty()` ⇒ no filtering); the tag columns come from the mmap and are
    /// empty for a pre-tag (v1/v2) segment (every query reads back untagged).
    #[inline]
    pub fn verify(
        &self,
        id: u32,
        view: &crate::exact::TitleView,
        pred: &crate::exact::TagPredicate,
    ) -> bool {
        crate::exact::verify_slices(
            id,
            view,
            self.req_mask(),
            self.forb_mask(),
            self.req_off(),
            self.req_len(),
            self.req_blob(),
            self.forb_off(),
            self.forb_len(),
            self.forb_blob(),
            self.q_group_start(),
            self.q_group_count(),
            self.group_off(),
            self.group_len(),
            self.anyof_blob(),
            self.predicate_off(),
            self.predicate_len(),
            self.predicate_blob(),
            pred,
            self.tag_off(),
            self.tag_len(),
            self.tag_blob(),
        )
    }

    pub(crate) fn class_of(&self, id: u32) -> Option<CostClass> {
        if id as usize >= self.num_queries as usize {
            return None;
        }
        // SAFETY: the bounds check above proves `id < num_queries`; `class_arr`
        // is the validated `num_queries`-long class column owned by this mmap.
        let class = unsafe { *self.class_arr.add(id as usize) };
        match class {
            0 => Some(CostClass::A),
            1 => Some(CostClass::B),
            2 => Some(CostClass::C),
            3 => Some(CostClass::D),
            4 => Some(CostClass::H),
            _ => None,
        }
    }

    // ---- broad-lane batch evaluation surface (mmap twin of the in-memory
    // `Segment` accessors used by `segment::broad_batch`). Lets the columnar
    // broad evaluator drive mmap and in-memory segments through one body. ----

    /// Probe the broad frozen table for `key` (after the anchor-filter check),
    /// appending reachable local IDs to `cands` (epoch-deduped via `seen`). The
    /// reachability primitive for the batch broad lane — mirrors the broad block
    /// of `match_into` (filter gate + probe) so the columnar path skips the same
    /// probes the per-title path would.
    #[inline]
    pub(crate) fn broad_reach<D: DeadlineCheck>(
        &self,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
        deadline: &mut DeadlinePoll<D>,
    ) -> Result<(), D::Cancelled> {
        stats.probes_attempted += 1;
        if self.filter_num_blocks > 0 && !self.may_contain(key) {
            stats.probes_skipped += 1;
            return Ok(());
        }
        if let Some(posting) =
            frozen_probe(key, self.broad_slots(), self.broad_blob(), self.broad_mask)
        {
            stats.postings_scanned += posting.len() as u32;
            stats.broad_postings_scanned += posting.len() as u32;
            for &local in posting {
                deadline.check_work()?;
                if seen[local as usize] != epoch {
                    seen[local as usize] = epoch;
                    cands.push(local);
                }
            }
        }
        Ok(())
    }

    /// The hot-tier twin of [`broad_reach`](Self::broad_reach) (class H,
    /// ADR-105): probe the hot index for `key`, appending reachable locals to
    /// `cands` (epoch-deduped), counting into the hot-lane meters.
    pub(crate) fn hot_reach<D: DeadlineCheck>(
        &self,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
        deadline: &mut DeadlinePoll<D>,
    ) -> Result<(), D::Cancelled> {
        stats.probes_attempted += 1;
        if self.filter_num_blocks > 0 && !self.may_contain(key) {
            stats.probes_skipped += 1;
            return Ok(());
        }
        if let Some(posting) = frozen_probe(key, self.hot_slots(), self.hot_blob(), self.hot_mask) {
            stats.postings_scanned += posting.len() as u32;
            stats.hot_postings_scanned += posting.len() as u32;
            for &local in posting {
                deadline.check_work()?;
                if seen[local as usize] != epoch {
                    seen[local as usize] = epoch;
                    cands.push(local);
                }
            }
        }
        Ok(())
    }

    /// Liveness for one local ID (mmap tombstone overlay).
    #[inline]
    pub(crate) fn is_alive_at(&self, local: u32) -> bool {
        self.alive_overlay[local as usize]
    }

    /// The hot-tier vacuous-accept twin (class H, ADR-105). Mmap twin of
    /// [`crate::exact::ExactStore::pure_tail_anchor`]: the single required
    /// feature lives in the TAIL (a θ-hot anchor has no mask bit), so equality
    /// with the reaching anchor proves retrieval == match.
    #[inline]
    pub(crate) fn pure_tail_anchor(&self, local: u32, anchor: crate::dict::FeatureId) -> bool {
        let i = local as usize;
        self.req_mask()[i] == 0
            && self.req_len()[i] == 1
            && self.forb_mask()[i] == 0
            && self.forb_len()[i] == 0
            && self.q_group_count()[i] == 0
            && self.predicate_len().get(i).copied().unwrap_or(0) == 0
            && self.req_blob()[self.req_off()[i] as usize] == anchor
    }

    /// Whether `local`'s entire semantics is its hot anchor — the pure-anchor
    /// skip-verify fast path. Mmap twin of [`crate::exact::ExactStore::is_pure_anchor`].
    #[inline]
    pub(crate) fn is_pure_anchor(&self, local: u32) -> bool {
        let i = local as usize;
        self.req_len()[i] == 0
            && self.forb_mask()[i] == 0
            && self.forb_len()[i] == 0
            && self.q_group_count()[i] == 0
            && self.predicate_len().get(i).copied().unwrap_or(0) == 0
            && self.req_mask()[i].is_power_of_two()
    }

    /// Batch-level count-gate pre-reject — the mmap twin of
    /// [`ExactStore::can_match_batch`](crate::exact::ExactStore::can_match_batch),
    /// sharing [`prefilter_slices`](crate::exact::prefilter_slices) so the two
    /// paths cannot drift (Broad-Query Cost Program lever 5a).
    #[inline]
    pub(crate) fn can_match_batch(
        &self,
        local: u32,
        batch_mask_union: u64,
        present: impl Fn(FeatureId) -> bool,
    ) -> bool {
        crate::exact::prefilter_slices(
            local as usize,
            batch_mask_union,
            present,
            self.req_mask(),
            self.req_off(),
            self.req_len(),
            self.req_blob(),
            self.q_group_start(),
            self.q_group_count(),
            self.group_off(),
            self.group_len(),
            self.anyof_blob(),
        )
    }

    /// Columnar batch verification for one query against a title batch, writing
    /// the matching-title bitmap into `acc`. Mmap twin of
    /// [`crate::exact::ExactStore::eval_batch`]; shares
    /// [`crate::exact::eval_batch_slices`] so the in-memory and mmap broad-batch
    /// paths cannot drift.
    // The four mutable bitmap slices are independent, caller-owned reusable
    // buffers. Keeping them explicit avoids a wrapper/indirection on this hot
    // path and mirrors `eval_batch_slices`, which carries the same exemption.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(crate) fn eval_batch<'a>(
        &self,
        local: u32,
        tmask_batch: &[u64],
        lookup: impl Fn(FeatureId) -> Option<&'a [u64]>,
        acc: &mut [u64],
        grp: &mut [u64],
        member: &mut [u64],
        choice: &mut [u64],
        pred: &crate::exact::TagPredicate,
    ) -> Result<(), crate::exact::BatchEvalError> {
        crate::exact::eval_batch_slices(
            local as usize,
            tmask_batch,
            lookup,
            acc,
            grp,
            self.req_mask(),
            self.forb_mask(),
            self.req_off(),
            self.req_len(),
            self.req_blob(),
            self.forb_off(),
            self.forb_len(),
            self.forb_blob(),
            self.q_group_start(),
            self.q_group_count(),
            self.group_off(),
            self.group_len(),
            self.anyof_blob(),
            self.predicate_off(),
            self.predicate_len(),
            self.predicate_blob(),
            member,
            choice,
            pred,
            self.tag_off(),
            self.tag_len(),
            self.tag_blob(),
        )
    }
}
