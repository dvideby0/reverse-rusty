//! The [`ExactStore`] — the struct-of-arrays exact-verification store indexed by
//! SegmentLocalQueryId. Holds the common-mask words, the required/forbidden tails,
//! the any-of groups, the per-query tag column (ADR-049), and identity; plus the
//! scalar [`verify`](ExactStore::verify), the columnar
//! [`eval_batch`](ExactStore::eval_batch) (delegating to
//! [`eval_batch_slices`](super::eval_batch_slices)), the pure-anchor derivation,
//! the compaction copy/re-anchor helpers, and the serialization slice accessors.

use super::{eval_batch_slices, query_passes_tags, TagPredicate};
use crate::compile::Extracted;
use crate::dict::{Dict, FeatureId, NO_MASK_BIT};
use crate::tagdict::TagId;

#[derive(Clone, Default)]
pub struct ExactStore {
    // common-mask words (the 64 hottest features)
    req_mask: Vec<u64>,
    forb_mask: Vec<u64>,
    // required tail (non-mask features), sorted, sliced from req_blob
    req_off: Vec<u32>,
    req_len: Vec<u16>,
    req_blob: Vec<u32>,
    // forbidden tail
    forb_off: Vec<u32>,
    forb_len: Vec<u16>,
    forb_blob: Vec<u32>,
    // any-of groups: per query a run of groups in the groups table
    q_group_start: Vec<u32>,
    q_group_count: Vec<u16>,
    group_off: Vec<u32>,
    group_len: Vec<u16>,
    anyof_blob: Vec<u32>,
    // per-query metadata tags (ADR-049): sorted TagIds sliced from tag_blob, exactly
    // parallel to the required tail. Verify-stage only — never gates retrieval (§5.3).
    tag_off: Vec<u32>,
    tag_len: Vec<u16>,
    tag_blob: Vec<TagId>,
    // identity, resolved only on a confirmed match
    version: Vec<u32>,
    logical: Vec<u64>,
}

impl std::fmt::Debug for ExactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExactStore")
            .field("queries", &self.req_mask.len())
            .field("req_blob_len", &self.req_blob.len())
            .field("forb_blob_len", &self.forb_blob.len())
            .field("anyof_blob_len", &self.anyof_blob.len())
            .finish()
    }
}

impl ExactStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.req_mask.len()
    }
    pub fn is_empty(&self) -> bool {
        self.req_mask.is_empty()
    }

    /// Append a compiled query; returns its SegmentLocalQueryId. `tags` are the query's
    /// interned metadata `TagId`s (ADR-049); the caller MUST pass them sorted + deduped
    /// (like `ex.required`) so the verify-stage filter is a sorted-slice intersection.
    pub fn push(
        &mut self,
        ex: &Extracted,
        tags: &[TagId],
        dict: &Dict,
        version: u32,
        logical: u64,
    ) -> u32 {
        let id = self.req_mask.len() as u32;

        let mut rmask = 0u64;
        let r_off = self.req_blob.len() as u32;
        let mut r_len = 0u16;
        for &f in &ex.required {
            let b = dict.mask_bit(f);
            if b == NO_MASK_BIT {
                self.req_blob.push(f);
                r_len += 1;
            } else {
                rmask |= 1u64 << b;
            }
        }

        let mut fmask = 0u64;
        let f_off = self.forb_blob.len() as u32;
        let mut f_len = 0u16;
        for &f in &ex.forbidden {
            let b = dict.mask_bit(f);
            if b == NO_MASK_BIT {
                self.forb_blob.push(f);
                f_len += 1;
            } else {
                fmask |= 1u64 << b;
            }
        }

        let g_start = self.group_off.len() as u32;
        let g_count = ex.anyof.len() as u16;
        for group in &ex.anyof {
            let off = self.anyof_blob.len() as u32;
            for &f in group {
                self.anyof_blob.push(f);
            }
            self.group_off.push(off);
            self.group_len.push(group.len() as u16);
        }

        self.req_mask.push(rmask);
        self.forb_mask.push(fmask);
        self.req_off.push(r_off);
        self.req_len.push(r_len);
        self.forb_off.push(f_off);
        self.forb_len.push(f_len);
        self.q_group_start.push(g_start);
        self.q_group_count.push(g_count);

        let t_off = self.tag_blob.len() as u32;
        self.tag_blob.extend_from_slice(tags);
        self.tag_off.push(t_off);
        self.tag_len.push(tags.len() as u16);

        self.version.push(version);
        self.logical.push(logical);
        id
    }

    #[inline]
    pub fn logical(&self, id: u32) -> u64 {
        self.logical[id as usize]
    }
    #[inline]
    pub fn version(&self, id: u32) -> u32 {
        self.version[id as usize]
    }
    /// The sorted `TagId` slice for query `id` (ADR-049). Used by the `set_vocab`
    /// recompile to carry a query's tags forward unchanged (same tag space).
    #[inline]
    pub fn tags_of(&self, id: u32) -> &[TagId] {
        let i = id as usize;
        let o = self.tag_off[i] as usize;
        let l = self.tag_len[i] as usize;
        &self.tag_blob[o..o + l]
    }

    /// Verify one candidate against a title. `tmask` is the title's common-mask
    /// word; `tfeats` is the title's full sorted feature slice (for tail checks); `pred`
    /// is the request's compiled tag filter (`TagPredicate::empty()` ⇒ no filtering).
    #[inline]
    pub fn verify(&self, id: u32, tmask: u64, tfeats: &[FeatureId], pred: &TagPredicate) -> bool {
        let i = id as usize;

        // 1) common-mask gate — the cheap reject (two u64 ops, no memory traffic)
        let rm = self.req_mask[i];
        if (rm & tmask) != rm {
            return false; // missing a masked required feature
        }
        if (self.forb_mask[i] & tmask) != 0 {
            return false; // has a masked forbidden feature
        }

        // 2) required tail: every non-mask required feature must be present
        let ro = self.req_off[i] as usize;
        let rl = self.req_len[i] as usize;
        for &f in &self.req_blob[ro..ro + rl] {
            if tfeats.binary_search(&f).is_err() {
                return false;
            }
        }

        // 3) forbidden tail: no non-mask forbidden feature may be present
        let fo = self.forb_off[i] as usize;
        let fl = self.forb_len[i] as usize;
        for &f in &self.forb_blob[fo..fo + fl] {
            if tfeats.binary_search(&f).is_ok() {
                return false;
            }
        }

        // 4) any-of groups: each group needs >=1 member present
        let gs = self.q_group_start[i] as usize;
        let gc = self.q_group_count[i] as usize;
        for gi in gs..gs + gc {
            let go = self.group_off[gi] as usize;
            let gl = self.group_len[gi] as usize;
            let mut hit = false;
            for &f in &self.anyof_blob[go..go + gl] {
                if tfeats.binary_search(&f).is_ok() {
                    hit = true;
                    break;
                }
            }
            if !hit {
                return false;
            }
        }

        // 5) tag predicate (post-candidate; never gates — matching.md §5.3). Mirrors
        //    `verify_slices` clause 5; skipped (one untaken branch) with no filter.
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
    #[inline]
    pub fn eval_batch<'a>(
        &self,
        local: u32,
        tmask_batch: &[u64],
        lookup: impl Fn(FeatureId) -> Option<&'a [u64]>,
        acc: &mut [u64],
        grp: &mut [u64],
        pred: &TagPredicate,
    ) {
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
            pred,
            &self.tag_off,
            &self.tag_len,
            &self.tag_blob,
        );
    }

    /// Whether query `local`'s ENTIRE semantics is its single hot anchor: one
    /// masked required feature, no required tail, no forbidden, no any-of. Such a
    /// query matches any title containing the anchor with NO exact verification —
    /// the pure-anchor skip-verify fast path (the streaming-safe analog of the
    /// design's "materialized subscriptions"). Derived purely from the SoA
    /// columns, so it composes through compaction with no extra state.
    #[inline]
    pub fn is_pure_anchor(&self, local: u32) -> bool {
        let i = local as usize;
        self.req_len[i] == 0
            && self.forb_mask[i] == 0
            && self.forb_len[i] == 0
            && self.q_group_count[i] == 0
            && self.req_mask[i].is_power_of_two()
    }

    /// Copy entry `id` from `self` into `dest`, returning the new local id in
    /// `dest`. Used by compaction to migrate alive entries into a fresh segment.
    pub fn copy_entry(&self, id: u32, dest: &mut ExactStore) -> u32 {
        let i = id as usize;
        let new_id = dest.req_mask.len() as u32;

        // common-mask words
        dest.req_mask.push(self.req_mask[i]);
        dest.forb_mask.push(self.forb_mask[i]);

        // required tail blob
        let ro = self.req_off[i] as usize;
        let rl = self.req_len[i] as usize;
        let new_ro = dest.req_blob.len() as u32;
        dest.req_blob.extend_from_slice(&self.req_blob[ro..ro + rl]);
        dest.req_off.push(new_ro);
        dest.req_len.push(rl as u16);

        // forbidden tail blob
        let fo = self.forb_off[i] as usize;
        let fl = self.forb_len[i] as usize;
        let new_fo = dest.forb_blob.len() as u32;
        dest.forb_blob
            .extend_from_slice(&self.forb_blob[fo..fo + fl]);
        dest.forb_off.push(new_fo);
        dest.forb_len.push(fl as u16);

        // any-of groups
        let gs = self.q_group_start[i] as usize;
        let gc = self.q_group_count[i] as usize;
        let new_gs = dest.group_off.len() as u32;
        for gi in gs..gs + gc {
            let go = self.group_off[gi] as usize;
            let gl = self.group_len[gi] as usize;
            let new_go = dest.anyof_blob.len() as u32;
            dest.anyof_blob
                .extend_from_slice(&self.anyof_blob[go..go + gl]);
            dest.group_off.push(new_go);
            dest.group_len.push(gl as u16);
        }
        dest.q_group_start.push(new_gs);
        dest.q_group_count.push(gc as u16);

        // tag column — compaction carries tags through the merge (ingestion §11)
        let to = self.tag_off[i] as usize;
        let tl = self.tag_len[i] as usize;
        let new_to = dest.tag_blob.len() as u32;
        dest.tag_blob.extend_from_slice(&self.tag_blob[to..to + tl]);
        dest.tag_off.push(new_to);
        dest.tag_len.push(tl as u16);

        // identity
        dest.version.push(self.version[i]);
        dest.logical.push(self.logical[i]);
        new_id
    }

    /// Reconstruct the *anchor-relevant* inputs for stored query `id` — its `required`
    /// features and `anyof` groups — from the SoA, for the compaction "improve" pass
    /// (ADR-056). The masked-required features (kept only as set bits in `req_mask`) are
    /// recovered via `mask_inverse` (bit → feature, from the frozen [`Dict`] mask); the
    /// non-masked required tail and the any-of groups are read directly (already feature
    /// IDs). Forbidden features are deliberately NOT returned: the anchor optimizer never
    /// reads them (the lossless-cover invariant), and the stored forbidden columns are
    /// carried forward verbatim by [`copy_entry`](Self::copy_entry), never rebuilt.
    ///
    /// The returned pair feeds `build_signatures(&Extracted { required, forbidden: vec![],
    /// anyof }, dict)` to re-derive the cover. `mask_inverse` MUST come from the same
    /// frozen dict the segment was built against (the engine's frozen-mask invariant), or
    /// a set bit could map to the wrong feature. A query built before the mask was
    /// finalized has `req_mask == 0`, so the un-masking loop is a natural no-op.
    pub fn anchoring_inputs(
        &self,
        id: u32,
        mask_inverse: &[Option<FeatureId>; 64],
    ) -> (Vec<FeatureId>, Vec<Vec<FeatureId>>) {
        let i = id as usize;

        // required = un-masked hot features ++ the non-masked tail. The two sets are
        // disjoint by construction (`push` routes each feature to mask XOR tail), so no
        // dedup is needed; `anchor_plan` re-sorts by frequency internally, so order here
        // is irrelevant.
        let mut required: Vec<FeatureId> = Vec::new();
        let mut bits = self.req_mask[i];
        while bits != 0 {
            let b = bits.trailing_zeros() as usize;
            if let Some(f) = mask_inverse[b] {
                required.push(f);
            }
            bits &= bits - 1; // clear the lowest set bit
        }
        let ro = self.req_off[i] as usize;
        let rl = self.req_len[i] as usize;
        required.extend_from_slice(&self.req_blob[ro..ro + rl]);

        // any-of groups are stored directly as feature IDs.
        let gs = self.q_group_start[i] as usize;
        let gc = self.q_group_count[i] as usize;
        let mut anyof: Vec<Vec<FeatureId>> = Vec::with_capacity(gc);
        for gi in gs..gs + gc {
            let go = self.group_off[gi] as usize;
            let gl = self.group_len[gi] as usize;
            anyof.push(self.anyof_blob[go..go + gl].to_vec());
        }

        (required, anyof)
    }

    // ---- slice accessors for serialization (storage.rs) ----
    pub fn req_masks(&self) -> &[u64] {
        &self.req_mask
    }
    pub fn forb_masks(&self) -> &[u64] {
        &self.forb_mask
    }
    pub fn req_offs(&self) -> &[u32] {
        &self.req_off
    }
    pub fn req_lens(&self) -> &[u16] {
        &self.req_len
    }
    pub fn req_blobs(&self) -> &[u32] {
        &self.req_blob
    }
    pub fn forb_offs(&self) -> &[u32] {
        &self.forb_off
    }
    pub fn forb_lens(&self) -> &[u16] {
        &self.forb_len
    }
    pub fn forb_blobs(&self) -> &[u32] {
        &self.forb_blob
    }
    pub fn q_group_starts(&self) -> &[u32] {
        &self.q_group_start
    }
    pub fn q_group_counts(&self) -> &[u16] {
        &self.q_group_count
    }
    pub fn group_offs(&self) -> &[u32] {
        &self.group_off
    }
    pub fn group_lens(&self) -> &[u16] {
        &self.group_len
    }
    pub fn anyof_blobs(&self) -> &[u32] {
        &self.anyof_blob
    }
    pub fn versions(&self) -> &[u32] {
        &self.version
    }
    pub fn logicals(&self) -> &[u64] {
        &self.logical
    }
    pub fn tag_offs(&self) -> &[u32] {
        &self.tag_off
    }
    pub fn tag_lens(&self) -> &[u16] {
        &self.tag_len
    }
    pub fn tag_blobs(&self) -> &[TagId] {
        &self.tag_blob
    }

    /// Push a raw entry (pre-computed masks and blobs). Used by MmapSegment::to_memory_segment
    /// to reconstruct an in-memory ExactStore from mmap'd data. `tags` is the query's sorted
    /// `TagId` slice (ADR-049).
    // Args mirror the SoA columns being reconstructed; a struct would add no clarity.
    #[allow(clippy::too_many_arguments)]
    pub fn push_raw(
        &mut self,
        rmask: u64,
        fmask: u64,
        req_tail: &[u32],
        forb_tail: &[u32],
        groups: (usize, usize, &[u32], &[u16], &[u32]), // (gs, gc, group_off, group_len, anyof_blob)
        tags: &[TagId],
        version: u32,
        logical: u64,
    ) -> u32 {
        let id = self.req_mask.len() as u32;
        self.req_mask.push(rmask);
        self.forb_mask.push(fmask);

        let r_off = self.req_blob.len() as u32;
        self.req_blob.extend_from_slice(req_tail);
        self.req_off.push(r_off);
        self.req_len.push(req_tail.len() as u16);

        let f_off = self.forb_blob.len() as u32;
        self.forb_blob.extend_from_slice(forb_tail);
        self.forb_off.push(f_off);
        self.forb_len.push(forb_tail.len() as u16);

        let (gs, gc, src_group_off, src_group_len, src_anyof) = groups;
        let new_gs = self.group_off.len() as u32;
        for gi in gs..gs + gc {
            let go = src_group_off[gi] as usize;
            let gl = src_group_len[gi] as usize;
            let new_go = self.anyof_blob.len() as u32;
            self.anyof_blob.extend_from_slice(&src_anyof[go..go + gl]);
            self.group_off.push(new_go);
            self.group_len.push(gl as u16);
        }
        self.q_group_start.push(new_gs);
        self.q_group_count.push(gc as u16);

        let t_off = self.tag_blob.len() as u32;
        self.tag_blob.extend_from_slice(tags);
        self.tag_off.push(t_off);
        self.tag_len.push(tags.len() as u16);

        self.version.push(version);
        self.logical.push(logical);
        id
    }

    pub fn heap_bytes(&self) -> usize {
        use std::mem::size_of;
        self.req_mask.capacity() * size_of::<u64>()
            + self.forb_mask.capacity() * size_of::<u64>()
            + self.req_off.capacity() * size_of::<u32>()
            + self.req_len.capacity() * size_of::<u16>()
            + self.req_blob.capacity() * size_of::<u32>()
            + self.forb_off.capacity() * size_of::<u32>()
            + self.forb_len.capacity() * size_of::<u16>()
            + self.forb_blob.capacity() * size_of::<u32>()
            + self.q_group_start.capacity() * size_of::<u32>()
            + self.q_group_count.capacity() * size_of::<u16>()
            + self.group_off.capacity() * size_of::<u32>()
            + self.group_len.capacity() * size_of::<u16>()
            + self.anyof_blob.capacity() * size_of::<u32>()
            + self.tag_off.capacity() * size_of::<u32>()
            + self.tag_len.capacity() * size_of::<u16>()
            + self.tag_blob.capacity() * size_of::<TagId>()
            + self.version.capacity() * size_of::<u32>()
            + self.logical.capacity() * size_of::<u64>()
    }
}
