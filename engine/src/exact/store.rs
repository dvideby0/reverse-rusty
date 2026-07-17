//! The [`ExactStore`] — the struct-of-arrays exact-verification store indexed by
//! SegmentLocalQueryId. Holds the common-mask words, the required/forbidden tails,
//! the any-of groups, the per-query tag column (ADR-049), and identity; plus the
//! scalar [`verify`](ExactStore::verify), the columnar
//! [`eval_batch`](ExactStore::eval_batch) (delegating to
//! [`eval_batch_slices`](super::eval_batch_slices)), the pure-anchor derivation,
//! the compaction copy/re-anchor helpers, and the serialization slice accessors.

use super::{eval_batch_slices, query_passes_tags, TagPredicate, TitleView};
use crate::compile::Extracted;
use crate::dict::{Dict, FeatureId, NO_MASK_BIT};
use crate::ownership::{PlacementGeneration, PlacementMode, QueryPlacement, QueryPlacementRef};
use crate::rank::RankValues;
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
    // Fixed signed typed rank column (ADR-108), parallel to logical/version.
    priority: Vec<i64>,
    // Distributed emission ownership (ADR-109). The fixed-width columns are
    // parallel to identity; selective positions are sliced from placement_blob.
    placement_generation: Vec<u64>,
    placement_num_shards: Vec<u32>,
    placement_mode: Vec<u8>,
    placement_off: Vec<u32>,
    placement_len: Vec<u32>,
    placement_blob: Vec<u32>,
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
        self.push_ranked(ex, tags, dict, version, logical, RankValues::default())
    }

    /// Append a compiled query with its fixed typed rank values.
    pub fn push_ranked(
        &mut self,
        ex: &Extracted,
        tags: &[TagId],
        dict: &Dict,
        version: u32,
        logical: u64,
        rank: RankValues,
    ) -> u32 {
        self.push_ranked_with_placement(
            ex,
            tags,
            dict,
            version,
            logical,
            rank,
            &QueryPlacement::standalone(),
        )
    }

    /// Append a compiled query and its write-time distributed placement
    /// metadata. The metadata is identity-only and does not enter verification.
    #[allow(clippy::too_many_arguments)]
    pub fn push_ranked_with_placement(
        &mut self,
        ex: &Extracted,
        tags: &[TagId],
        dict: &Dict,
        version: u32,
        logical: u64,
        rank: RankValues,
        placement: &QueryPlacement,
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
        self.priority.push(rank.priority);
        self.push_placement(placement);
        id
    }

    fn push_placement(&mut self, placement: &QueryPlacement) {
        let off = self.placement_blob.len() as u32;
        self.placement_blob.extend_from_slice(placement.positions());
        self.placement_generation.push(placement.generation().0);
        self.placement_num_shards.push(placement.num_shards());
        self.placement_mode.push(placement.mode() as u8);
        self.placement_off.push(off);
        self.placement_len.push(placement.positions().len() as u32);
    }

    #[inline]
    pub fn logical(&self, id: u32) -> u64 {
        self.logical[id as usize]
    }
    #[inline]
    pub fn version(&self, id: u32) -> u32 {
        self.version[id as usize]
    }
    #[inline]
    pub fn rank_values(&self, id: u32) -> RankValues {
        RankValues {
            priority: self.priority[id as usize],
        }
    }
    #[inline]
    pub fn placement(&self, id: u32) -> QueryPlacementRef<'_> {
        let i = id as usize;
        let off = self.placement_off[i] as usize;
        let len = self.placement_len[i] as usize;
        QueryPlacementRef {
            generation: PlacementGeneration(self.placement_generation[i]),
            num_shards: self.placement_num_shards[i],
            // ExactStore rows can only be populated by validated constructors or
            // the validated mmap decoder.
            mode: match self.placement_mode[i] {
                1 => PlacementMode::Selective,
                2 => PlacementMode::ReplicatedAlwaysVisible,
                3 => PlacementMode::ReplicatedBroad,
                _ => PlacementMode::Standalone,
            },
            positions: &self.placement_blob[off..off + len],
        }
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

    /// Batch-level count-gate pre-reject (Broad-Query Cost Program lever 5a):
    /// `false` only when NO title in the batch can possibly satisfy `local`, so
    /// the columnar pass may skip its full bitmap verification. Shares
    /// [`prefilter_slices`](super::slices::prefilter_slices) with the mmap path
    /// so the two cannot drift. Under-reject is the only possible error
    /// direction; forbidden features are never consulted.
    #[inline]
    pub fn can_match_batch(
        &self,
        local: u32,
        batch_mask_union: u64,
        present: impl Fn(FeatureId) -> bool,
    ) -> bool {
        super::slices::prefilter_slices(
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

    /// The CANONICAL body signature of stored query `local` (dedup Stage A):
    /// a 64-bit hash over the query's SEMANTIC columns only — the two mask
    /// words, the required/forbidden tails as SORTED sets, and the any-of
    /// groups as a SORTED multiset of sorted member sets. Tags, version and
    /// logical id are deliberately excluded (they are per-member identity, not
    /// semantics). Two queries with equal signatures are *candidates* for
    /// sharing; the caller must confirm with [`bodies_equal`](Self::bodies_equal)
    /// (a hash collision must never cause false sharing — that would be a
    /// correctness bug, not a missed optimization).
    pub fn body_signature(&self, local: u32) -> u64 {
        let i = local as usize;
        let mut h = crate::util::fnv1a64(b"body");
        let mut mix = |v: u64| {
            for b in v.to_le_bytes() {
                h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01B3);
            }
        };
        mix(self.req_mask[i]);
        mix(self.forb_mask[i]);
        let sorted = |off: u32, len: u16, blob: &[u32]| -> Vec<u32> {
            let mut v = blob[off as usize..off as usize + len as usize].to_vec();
            v.sort_unstable();
            v
        };
        let req = sorted(self.req_off[i], self.req_len[i], &self.req_blob);
        mix(0xA1); // domain separators between variable-length sections
        for f in &req {
            mix(u64::from(*f));
        }
        let forb = sorted(self.forb_off[i], self.forb_len[i], &self.forb_blob);
        mix(0xA2);
        for f in &forb {
            mix(u64::from(*f));
        }
        let mut groups = self.canonical_groups(i);
        mix(0xA3);
        for g in groups.drain(..) {
            mix(0xA4);
            for f in g {
                mix(u64::from(f));
            }
        }
        h
    }

    /// The any-of groups of row `i` in canonical form: each group's members
    /// sorted, and the groups themselves sorted (a multiset comparison key).
    fn canonical_groups(&self, i: usize) -> Vec<Vec<u32>> {
        let gs = self.q_group_start[i] as usize;
        let gc = self.q_group_count[i] as usize;
        let mut groups: Vec<Vec<u32>> = (gs..gs + gc)
            .map(|gi| {
                let go = self.group_off[gi] as usize;
                let gl = self.group_len[gi] as usize;
                let mut g = self.anyof_blob[go..go + gl].to_vec();
                g.sort_unstable();
                g
            })
            .collect();
        groups.sort_unstable();
        groups
    }

    /// Exact canonical-body equality between two stored rows — the collision
    /// check behind [`body_signature`](Self::body_signature). Compares the
    /// SEMANTIC columns only (masks, sorted tails, canonicalized groups); never
    /// tags/version/logical.
    pub fn bodies_equal(&self, a: u32, b: u32) -> bool {
        let (ia, ib) = (a as usize, b as usize);
        if self.req_mask[ia] != self.req_mask[ib]
            || self.forb_mask[ia] != self.forb_mask[ib]
            || self.req_len[ia] != self.req_len[ib]
            || self.forb_len[ia] != self.forb_len[ib]
            || self.q_group_count[ia] != self.q_group_count[ib]
        {
            return false;
        }
        let sorted = |off: u32, len: u16, blob: &[u32]| -> Vec<u32> {
            let mut v = blob[off as usize..off as usize + len as usize].to_vec();
            v.sort_unstable();
            v
        };
        if sorted(self.req_off[ia], self.req_len[ia], &self.req_blob)
            != sorted(self.req_off[ib], self.req_len[ib], &self.req_blob)
        {
            return false;
        }
        if sorted(self.forb_off[ia], self.forb_len[ia], &self.forb_blob)
            != sorted(self.forb_off[ib], self.forb_len[ib], &self.forb_blob)
        {
            return false;
        }
        self.canonical_groups(ia) == self.canonical_groups(ib)
    }

    /// The hot-tier twin of [`is_pure_anchor`](Self::is_pure_anchor) (ADR-105):
    /// whether query `local`'s ENTIRE semantics is the single TAIL-stored
    /// required feature `anchor`. A class-H anchor is θ-hot but NOT top-64, so
    /// it has no mask bit — the query stores as `req_mask == 0, req_len == 1`
    /// with the anchor in the required tail, which `is_pure_anchor` structurally
    /// never matches (`is_power_of_two()` fails on 0). The caller supplies the
    /// feature that reached the candidate; equality proves it IS the stored
    /// anchor, so retrieval is proof of match (the vacuous accept), exactly like
    /// the masked case. Derived purely from the SoA columns.
    #[inline]
    pub fn pure_tail_anchor(&self, local: u32, anchor: FeatureId) -> bool {
        let i = local as usize;
        self.req_mask[i] == 0
            && self.req_len[i] == 1
            && self.forb_mask[i] == 0
            && self.forb_len[i] == 0
            && self.q_group_count[i] == 0
            && self.req_blob[self.req_off[i] as usize] == anchor
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
        dest.priority.push(self.priority[i]);
        dest.push_placement(&self.placement(id).to_owned());
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
    pub fn priorities(&self) -> &[i64] {
        &self.priority
    }
    pub fn placement_generations(&self) -> &[u64] {
        &self.placement_generation
    }
    pub fn placement_num_shards(&self) -> &[u32] {
        &self.placement_num_shards
    }
    pub fn placement_modes(&self) -> &[u8] {
        &self.placement_mode
    }
    pub fn placement_offs(&self) -> &[u32] {
        &self.placement_off
    }
    pub fn placement_lens(&self) -> &[u32] {
        &self.placement_len
    }
    pub fn placement_blobs(&self) -> &[u32] {
        &self.placement_blob
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
        priority: i64,
    ) -> u32 {
        self.push_raw_placed(
            rmask,
            fmask,
            req_tail,
            forb_tail,
            groups,
            tags,
            version,
            logical,
            priority,
            &QueryPlacement::standalone(),
        )
    }

    /// Raw-row reconstruction including validated v7 ownership metadata.
    #[allow(clippy::too_many_arguments)]
    pub fn push_raw_placed(
        &mut self,
        rmask: u64,
        fmask: u64,
        req_tail: &[u32],
        forb_tail: &[u32],
        groups: (usize, usize, &[u32], &[u16], &[u32]),
        tags: &[TagId],
        version: u32,
        logical: u64,
        priority: i64,
        placement: &QueryPlacement,
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
        self.priority.push(priority);
        self.push_placement(placement);
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
            + self.priority.capacity() * size_of::<i64>()
            + self.placement_generation.capacity() * size_of::<u64>()
            + self.placement_num_shards.capacity() * size_of::<u32>()
            + self.placement_mode.capacity() * size_of::<u8>()
            + self.placement_off.capacity() * size_of::<u32>()
            + self.placement_len.capacity() * size_of::<u32>()
            + self.placement_blob.capacity() * size_of::<u32>()
    }
}
