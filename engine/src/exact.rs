//! Exact, integer-only verification — the final filter on the match hot path.
//!
//! Design: docs/design/matching.md §3
//! Invariant: No strings, no regex, no virtual dispatch, no alloc — integer
//!   masks and sorted-slice galloping only
//! Hot path: yes — called for every candidate that passes the index probe
//!
//! Struct-of-arrays layout indexed by SegmentLocalQueryId. Most candidates are
//! rejected by the common-mask gate (two u64 ops) before any memory traffic
//! beyond the candidate's own two mask words.

use crate::compile::Extracted;
use crate::dict::{Dict, FeatureId, NO_MASK_BIT};
use crate::tagdict::TagId;

/// A compiled tag filter (ADR-049): a conjunction of per-key value-sets, each value-set a
/// sorted, deduped list of `TagId`s. A query passes iff EVERY group shares at least one
/// `TagId` with the query's sorted tag set (AND across keys, OR within a key). Compiled
/// once per request from the REST filter; tested only in the post-candidate verify stage,
/// never in candidate retrieval — so it can only ever *remove* queries the caller did not
/// ask for, never drop a wanted match (the "tags never gate" invariant, matching.md §5.3).
///
/// - **Empty** (`groups` empty) ⇒ no filter ⇒ every query passes. The verify clause is a
///   single never-taken branch, so the no-filter path is unchanged from before tags.
/// - **A present-but-empty group** ⇒ matches nothing (a filter on an all-unknown value):
///   intersecting a sorted set with an empty slice is empty, so the group fails — the
///   safe direction (a filter can never over-return).
#[derive(Clone, Debug, Default)]
pub struct TagPredicate {
    groups: Vec<Vec<TagId>>,
}

impl TagPredicate {
    /// The empty predicate — matches everything (no filtering). `const` so callers can
    /// pass `&TagPredicate::empty()` with no allocation.
    #[inline]
    #[must_use]
    pub const fn empty() -> Self {
        TagPredicate { groups: Vec::new() }
    }

    /// Build from per-key value-sets — each inner vec is one key's accepted `TagId`s.
    /// Each group is sorted+deduped here (off the hot path) so the per-candidate check is
    /// a sorted-slice intersection.
    #[must_use]
    pub fn new(mut groups: Vec<Vec<TagId>>) -> Self {
        for g in &mut groups {
            g.sort_unstable();
            g.dedup();
        }
        TagPredicate { groups }
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    #[inline]
    #[must_use]
    pub fn groups(&self) -> &[Vec<TagId>] {
        &self.groups
    }

    /// Whether a query's sorted `TagId` slice satisfies this predicate (every group must
    /// intersect `qtags`). Empty predicate ⇒ always true. Used by the broad-lane
    /// pure-anchor fast path, which has the query's tags but bypasses `verify`.
    #[inline]
    #[must_use]
    pub fn matches(&self, qtags: &[TagId]) -> bool {
        for group in &self.groups {
            if !sorted_intersects(qtags, group) {
                return false;
            }
        }
        true
    }
}

/// True if two sorted `TagId` slices share at least one element. Gallops the smaller
/// slice via binary search through the larger — integer-only, allocation-free. An empty
/// slice on either side yields `false` (a group with no resolvable value matches nothing).
#[inline]
fn sorted_intersects(a: &[TagId], b: &[TagId]) -> bool {
    let (small, large) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    for &x in small {
        if large.binary_search(&x).is_ok() {
            return true;
        }
    }
    false
}

/// Whether query `i`'s tag set satisfies `pred`: every group must intersect the query's
/// sorted `tag_blob[tag_off[i]..+tag_len[i]]`. An untagged query (empty tag slice) fails
/// any non-empty predicate — exactly the back-compat reading of a tag-less (v1/v2) segment.
/// Integer-only and allocation-free; only reached when `pred` is non-empty.
#[inline]
fn query_passes_tags(
    i: usize,
    pred: &TagPredicate,
    tag_off: &[u32],
    tag_len: &[u16],
    tag_blob: &[TagId],
) -> bool {
    // A query with no tag entry — a pre-tag (v1/v2) segment exposes empty tag columns —
    // is untagged, so its tag slice is empty and it fails any non-empty group.
    let qtags: &[TagId] = match (tag_off.get(i), tag_len.get(i)) {
        (Some(&o), Some(&l)) => &tag_blob[o as usize..o as usize + l as usize],
        _ => &[],
    };
    for group in &pred.groups {
        if !sorted_intersects(qtags, group) {
            return false;
        }
    }
    true
}

/// Shared exact-verification logic operating on raw slices. Used by both
/// in-memory ExactStore::verify and MmapSegment::verify to avoid duplication.
// Args mirror the SoA columns one-to-one; bundling them into a struct would add
// indirection on the match hot path for no readability gain.
#[allow(clippy::too_many_arguments)]
#[inline]
pub fn verify_slices(
    id: u32,
    tmask: u64,
    tfeats: &[FeatureId],
    req_mask: &[u64],
    forb_mask: &[u64],
    req_off: &[u32],
    req_len: &[u16],
    req_blob: &[u32],
    forb_off: &[u32],
    forb_len: &[u16],
    forb_blob: &[u32],
    q_group_start: &[u32],
    q_group_count: &[u16],
    group_off: &[u32],
    group_len: &[u16],
    anyof_blob: &[u32],
    pred: &TagPredicate,
    tag_off: &[u32],
    tag_len: &[u16],
    tag_blob: &[TagId],
) -> bool {
    let i = id as usize;

    // 1) common-mask gate
    let rm = req_mask[i];
    if (rm & tmask) != rm {
        return false;
    }
    if (forb_mask[i] & tmask) != 0 {
        return false;
    }

    // 2) required tail
    let ro = req_off[i] as usize;
    let rl = req_len[i] as usize;
    for &f in &req_blob[ro..ro + rl] {
        if tfeats.binary_search(&f).is_err() {
            return false;
        }
    }

    // 3) forbidden tail
    let fo = forb_off[i] as usize;
    let fl = forb_len[i] as usize;
    for &f in &forb_blob[fo..fo + fl] {
        if tfeats.binary_search(&f).is_ok() {
            return false;
        }
    }

    // 4) any-of groups
    let gs = q_group_start[i] as usize;
    let gc = q_group_count[i] as usize;
    for gi in gs..gs + gc {
        let go = group_off[gi] as usize;
        let gl = group_len[gi] as usize;
        let mut hit = false;
        for &f in &anyof_blob[go..go + gl] {
            if tfeats.binary_search(&f).is_ok() {
                hit = true;
                break;
            }
        }
        if !hit {
            return false;
        }
    }

    // 5) tag predicate (post-candidate; NEVER gates retrieval — matching.md §5.3). Only a
    //    candidate that already satisfies the query is filtered by the caller's tags, so a
    //    filter can only remove, never drop a wanted match. Skipped entirely (one untaken
    //    branch) when no filter is supplied, keeping the no-filter path unchanged.
    if !pred.is_empty() && !query_passes_tags(i, pred, tag_off, tag_len, tag_blob) {
        return false;
    }

    true
}

/// Columnar batch verification — the bitmap transpose of [`verify_slices`].
///
/// Computes, for one stored query `i`, the set of titles in a batch that satisfy
/// it, written as a bitmap into `acc` (one bit per batch-local title, `acc.len()`
/// = `ceil(batch / 64)` words). `tmask_batch[t]` is title `t`'s common-mask word;
/// `lookup(f)` returns the bitmap of titles containing feature `f` (or `None` if
/// `f` is absent from the whole batch). `grp` is a reused scratch bitmap of the
/// same width as `acc`.
///
/// This reproduces [`verify_slices`] clause-for-clause so the batch (broad-lane)
/// path returns *exactly* the same matches as the scalar per-title path — the
/// load-bearing correctness obligation (no false negatives, no false positives).
/// Each scalar test becomes a bitwise transpose: the common-mask gate → a
/// per-title gate bitmap; required-tail present → AND of the feature bitmaps;
/// forbidden-tail absent → AND-NOT; any-of → AND of (OR over members). Forbidden
/// features are consulted ONLY here in verification, never to retrieve/prune
/// candidates — the "never gate on MUST_NOT" invariant, identical to the scalar
/// path.
#[allow(clippy::too_many_arguments)]
#[inline]
pub fn eval_batch_slices<'a>(
    i: usize,
    tmask_batch: &[u64],
    lookup: impl Fn(FeatureId) -> Option<&'a [u64]>,
    acc: &mut [u64],
    grp: &mut [u64],
    req_mask: &[u64],
    forb_mask: &[u64],
    req_off: &[u32],
    req_len: &[u16],
    req_blob: &[u32],
    forb_off: &[u32],
    forb_len: &[u16],
    forb_blob: &[u32],
    q_group_start: &[u32],
    q_group_count: &[u16],
    group_off: &[u32],
    group_len: &[u16],
    anyof_blob: &[u32],
    pred: &TagPredicate,
    tag_off: &[u32],
    tag_len: &[u16],
    tag_blob: &[TagId],
) {
    // 0) tag predicate (post-candidate; NEVER gates). The filter is title-independent, so
    //    it is a per-query scalar gate: a query failing the caller's tags matches no title.
    //    Mirrors verify step 5; skipped (one untaken branch) when no filter is supplied.
    if !pred.is_empty() && !query_passes_tags(i, pred, tag_off, tag_len, tag_blob) {
        for a in acc.iter_mut() {
            *a = 0;
        }
        return;
    }

    // 1) common-mask gate -> per-title gate bitmap (verify step 1, transposed)
    let rm = req_mask[i];
    let fm = forb_mask[i];
    for a in acc.iter_mut() {
        *a = 0;
    }
    for (t, &tm) in tmask_batch.iter().enumerate() {
        if (rm & tm) == rm && (fm & tm) == 0 {
            acc[t >> 6] |= 1u64 << (t & 63);
        }
    }

    // 2) required tail: AND of each feature's title bitmap (verify step 2)
    let ro = req_off[i] as usize;
    let rl = req_len[i] as usize;
    for &f in &req_blob[ro..ro + rl] {
        if let Some(b) = lookup(f) {
            for (a, x) in acc.iter_mut().zip(b) {
                *a &= *x;
            }
        } else {
            // feature absent from the whole batch -> no title can match
            for a in acc.iter_mut() {
                *a = 0;
            }
            return;
        }
    }

    // 3) forbidden tail: AND-NOT each feature's title bitmap (verify step 3)
    let fo = forb_off[i] as usize;
    let fl = forb_len[i] as usize;
    for &f in &forb_blob[fo..fo + fl] {
        if let Some(b) = lookup(f) {
            for (a, x) in acc.iter_mut().zip(b) {
                *a &= !*x;
            }
        }
    }

    // 4) any-of groups: AND of (OR over members) (verify step 4)
    let gs = q_group_start[i] as usize;
    let gc = q_group_count[i] as usize;
    for gi in gs..gs + gc {
        let go = group_off[gi] as usize;
        let gl = group_len[gi] as usize;
        for g in grp.iter_mut() {
            *g = 0;
        }
        for &m in &anyof_blob[go..go + gl] {
            if let Some(b) = lookup(m) {
                for (g, x) in grp.iter_mut().zip(b) {
                    *g |= *x;
                }
            }
        }
        for (a, x) in acc.iter_mut().zip(grp.iter()) {
            *a &= *x;
        }
    }
}

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

#[cfg(test)]
mod tag_filter_tests {
    use super::*;
    use crate::dict::FeatureKind;

    #[test]
    fn sorted_intersects_basics() {
        assert!(sorted_intersects(&[1, 5, 9], &[5])); // shares 5
        assert!(sorted_intersects(&[5], &[1, 5, 9])); // order-independent
        assert!(!sorted_intersects(&[1, 2, 3], &[4, 5, 6])); // disjoint
        assert!(!sorted_intersects(&[1, 2, 3], &[])); // empty group ⇒ no match
        assert!(!sorted_intersects(&[], &[1, 2, 3])); // untagged query ⇒ no match
    }

    #[test]
    fn predicate_is_and_across_keys_or_within_a_key() {
        // q0 tags {10,20}, q1 tags {11}, q2 untagged — laid out as the SoA tag column.
        let tag_blob = [10u32, 20, 11];
        let tag_off = [0u32, 2, 3];
        let tag_len = [2u16, 1, 0];
        let passes = |i: usize, pred: &TagPredicate| {
            pred.is_empty() || query_passes_tags(i, pred, &tag_off, &tag_len, &tag_blob)
        };

        // empty predicate ⇒ everything passes (no filter)
        let none = TagPredicate::empty();
        assert!(passes(0, &none) && passes(1, &none) && passes(2, &none));

        // category ∈ {A=10, B=11}: tagged q0/q1 pass, untagged q2 fails
        let cat = TagPredicate::new(vec![vec![11, 10]]); // unsorted input → new() sorts
        assert!(passes(0, &cat) && passes(1, &cat));
        assert!(!passes(2, &cat));

        // category ∈ {A=10} AND status ∈ {X=20}: only q0 has both (AND across keys)
        let both = TagPredicate::new(vec![vec![10], vec![20]]);
        assert!(passes(0, &both));
        assert!(!passes(1, &both) && !passes(2, &both));

        // a present-but-empty group matches nothing (filter on an all-unknown value) —
        // the load-bearing "can never over-return" rule.
        let empty_group = TagPredicate::new(vec![Vec::new()]);
        assert!(!empty_group.is_empty());
        assert!(!passes(0, &empty_group) && !passes(1, &empty_group));
    }

    #[test]
    fn verify_filters_post_candidate_and_only_removes() {
        // A store with one query requiring feature id 7, tagged {10, 20} (sorted).
        let mut dict = Dict::new();
        for i in 0..8 {
            dict.intern(&format!("f{i}"), FeatureKind::Generic);
        }
        let ex = Extracted {
            required: vec![7],
            forbidden: vec![],
            anyof: vec![],
        };
        let mut store = ExactStore::new();
        store.push(&ex, &[10, 20], &dict, 1, 100);

        let tfeats = [7u32]; // a title that satisfies the query's expression
        let tmask = 0u64;

        // No filter → matches (the query's expression is satisfied).
        assert!(store.verify(0, tmask, &tfeats, &TagPredicate::empty()));
        // A filter the query satisfies (category=A=10) → still matches.
        assert!(store.verify(0, tmask, &tfeats, &TagPredicate::new(vec![vec![10]])));
        // A filter the query does NOT satisfy (category=99) → removed, even though the
        // expression matches. Proves filtering happens post-candidate and only removes.
        assert!(!store.verify(0, tmask, &tfeats, &TagPredicate::new(vec![vec![99]])));

        // eval_batch (columnar) must agree with verify for the same predicate.
        let mut acc = [0u64; 1];
        let mut grp = [0u64; 1];
        let lookup = |f: FeatureId| -> Option<&[u64]> {
            if f == 7 {
                Some(&[1u64]) // title 0 contains feature 7
            } else {
                None
            }
        };
        store.eval_batch(
            0,
            &[tmask],
            lookup,
            &mut acc,
            &mut grp,
            &TagPredicate::new(vec![vec![10]]),
        );
        assert_eq!(
            acc[0] & 1,
            1,
            "columnar path matches with a satisfied filter"
        );
        let mut acc2 = [0u64; 1];
        store.eval_batch(
            0,
            &[tmask],
            lookup,
            &mut acc2,
            &mut grp,
            &TagPredicate::new(vec![vec![99]]),
        );
        assert_eq!(
            acc2[0] & 1,
            0,
            "columnar path drops with an unsatisfied filter"
        );
    }
}
