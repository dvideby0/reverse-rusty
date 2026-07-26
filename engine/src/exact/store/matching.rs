use super::{
    eval_batch_slices, query_passes_tags, verify_predicate, BatchEvalError, ExactStore, FeatureId,
    TagPredicate, TitleView,
};

impl ExactStore {
    /// Verify one candidate against a title's two feature views (ADR-061). `view.pos`
    /// (the overlapping superset `P(T)`) drives the required-mask gate, required tail, and
    /// any-of; `view.neg` (the canonical leftmost-longest `N(T)`) drives ONLY the forbidden
    /// checks, so a MUST_NOT clause stays recall-correct. `pred` is the request's compiled tag
    /// filter (`TagPredicate::empty()` ⇒ no filtering). With a single-view title
    /// ([`TitleView::single`]) this is byte-identical to the pre-ADR-061 path.
    #[inline]
    pub fn verify(&self, id: u32, view: &TitleView, pred: &TagPredicate) -> bool {
        let i = id as usize;

        // 1) common-mask gate — the cheap reject (two u64 ops, no memory traffic). Required
        //    against the positive view, forbidden against the negative (canonical) view.
        let rm = self.req_mask[i];
        if (rm & view.pos_mask) != rm {
            return false; // missing a masked required feature
        }
        if (self.forb_mask[i] & view.neg_mask) != 0 {
            return false; // has a masked forbidden feature
        }

        // 2) required tail: every non-mask required feature must be present (positive view)
        let ro = self.req_off[i] as usize;
        let rl = self.req_len[i] as usize;
        for &f in &self.req_blob[ro..ro + rl] {
            if view.pos.binary_search(&f).is_err() {
                return false;
            }
        }

        // 3) forbidden tail: no non-mask forbidden feature may be present (negative view)
        let fo = self.forb_off[i] as usize;
        let fl = self.forb_len[i] as usize;
        for &f in &self.forb_blob[fo..fo + fl] {
            if view.neg.binary_search(&f).is_ok() {
                return false;
            }
        }

        // 4) any-of groups: each group needs >=1 member present (positive view)
        let gs = self.q_group_start[i] as usize;
        let gc = self.q_group_count[i] as usize;
        for gi in gs..gs + gc {
            let go = self.group_off[gi] as usize;
            let gl = self.group_len[gi] as usize;
            let mut hit = false;
            for &f in &self.anyof_blob[go..go + gl] {
                if view.pos.binary_search(&f).is_ok() {
                    hit = true;
                    break;
                }
            }
            if !hit {
                return false;
            }
        }

        // 5) compound members (integer-only program; empty for the common path).
        let predicate_off = self.predicate_off[i] as usize;
        let predicate_len = self.predicate_len[i] as usize;
        if predicate_len != 0
            && !verify_predicate(
                &self.predicate_blob[predicate_off..predicate_off + predicate_len],
                view,
            )
        {
            return false;
        }

        // 6) tag predicate (post-candidate; never gates — matching.md §5.3). Mirrors
        //    `verify_slices` clause 6; skipped (one untaken branch) with no filter.
        if !pred.is_empty()
            && !query_passes_tags(i, pred, &self.tag_off, &self.tag_len, &self.tag_blob)
        {
            return false;
        }

        true
    }

    /// Columnar batch verification for one query against a title batch. Writes
    /// the matching-title bitmap into `acc`. The bitmap transpose of [`verify`],
    /// sharing [`eval_batch_slices`] with the mmap path so the two cannot drift. `pred`
    /// is the request's compiled tag filter (applied as a per-query scalar gate).
    ///
    /// # Errors
    ///
    /// Returns [`BatchEvalError::PositionedPredicate`] (with `acc` cleared) when
    /// `local` contains a quoted predicate. This positionless bitmap API cannot
    /// represent adjacency; use [`Self::verify`] with a positioned [`TitleView`].
    // The four mutable bitmap slices are independent, caller-owned reusable
    // buffers. Keeping them explicit avoids a wrapper/indirection on this hot
    // path and mirrors `eval_batch_slices`, which carries the same exemption.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub fn eval_batch<'a>(
        &self,
        local: u32,
        tmask_batch: &[u64],
        lookup: impl Fn(FeatureId) -> Option<&'a [u64]>,
        acc: &mut [u64],
        grp: &mut [u64],
        member: &mut [u64],
        choice: &mut [u64],
        pred: &TagPredicate,
    ) -> Result<(), BatchEvalError> {
        eval_batch_slices(
            local as usize,
            tmask_batch,
            lookup,
            acc,
            grp,
            &self.req_mask,
            &self.forb_mask,
            &self.req_off,
            &self.req_len,
            &self.req_blob,
            &self.forb_off,
            &self.forb_len,
            &self.forb_blob,
            &self.q_group_start,
            &self.q_group_count,
            &self.group_off,
            &self.group_len,
            &self.anyof_blob,
            &self.predicate_off,
            &self.predicate_len,
            &self.predicate_blob,
            member,
            choice,
            pred,
            &self.tag_off,
            &self.tag_len,
            &self.tag_blob,
        )
    }

    /// Batch-level count-gate pre-reject (Broad-Query Cost Program lever 5a):
    /// `false` only when NO title in the batch can possibly satisfy `local`, so
    /// the columnar pass may skip its full bitmap verification. Shares
    /// [`prefilter_slices`](super::super::slices::prefilter_slices) with the mmap path
    /// so the two cannot drift. Under-reject is the only possible error
    /// direction; forbidden features are never consulted.
    #[inline]
    pub fn can_match_batch(
        &self,
        local: u32,
        batch_mask_union: u64,
        present: impl Fn(FeatureId) -> bool,
    ) -> bool {
        super::super::slices::prefilter_slices(
            local as usize,
            batch_mask_union,
            present,
            &self.req_mask,
            &self.req_off,
            &self.req_len,
            &self.req_blob,
            &self.q_group_start,
            &self.q_group_count,
            &self.group_off,
            &self.group_len,
            &self.anyof_blob,
        )
    }
}
