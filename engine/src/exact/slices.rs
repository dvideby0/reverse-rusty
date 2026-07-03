//! The free raw-slice verifiers — the shared exact-verification kernels operating
//! directly on the SoA columns as borrowed slices, so the in-memory [`ExactStore`]
//! and the mmap-backed `MmapSegment` run byte-identical logic and cannot drift.
//!
//! [`verify_slices`] is the scalar per-candidate gate; [`eval_batch_slices`] is its
//! columnar (bitmap-transpose) twin for the broad-lane batch path.

use super::{query_passes_tags, TagPredicate};
use crate::dict::FeatureId;
use crate::tagdict::TagId;

/// Shared exact-verification logic operating on raw slices. Used by both
/// in-memory ExactStore::verify and MmapSegment::verify to avoid duplication.
///
/// The title is supplied as **two views** (ADR-061): the positive superset
/// (`pos_mask` / `pos_feats` = `P(T)`) drives the required-mask gate, the required tail, and
/// any-of; the canonical leftmost-longest negative view (`neg_mask` / `neg_feats` = `N(T)`)
/// drives ONLY the forbidden-mask gate and the forbidden tail. The caller passes the same
/// mask + slice for both when there is no active multi-word alias, making this byte-identical
/// to the pre-ADR-061 single-view path.
// Args mirror the SoA columns one-to-one; bundling them into a struct would add
// indirection on the match hot path for no readability gain.
#[allow(clippy::too_many_arguments)]
#[inline]
pub fn verify_slices(
    id: u32,
    pos_mask: u64,
    pos_feats: &[FeatureId],
    neg_mask: u64,
    neg_feats: &[FeatureId],
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

    // 1) common-mask gate — required against the positive view, forbidden against the
    //    negative (canonical) view so a MUST_NOT cannot trip on an overlap-only entity.
    let rm = req_mask[i];
    if (rm & pos_mask) != rm {
        return false;
    }
    if (forb_mask[i] & neg_mask) != 0 {
        return false;
    }

    // 2) required tail (positive view)
    let ro = req_off[i] as usize;
    let rl = req_len[i] as usize;
    for &f in &req_blob[ro..ro + rl] {
        if pos_feats.binary_search(&f).is_err() {
            return false;
        }
    }

    // 3) forbidden tail (negative / canonical view)
    let fo = forb_off[i] as usize;
    let fl = forb_len[i] as usize;
    for &f in &forb_blob[fo..fo + fl] {
        if neg_feats.binary_search(&f).is_ok() {
            return false;
        }
    }

    // 4) any-of groups (positive view)
    let gs = q_group_start[i] as usize;
    let gc = q_group_count[i] as usize;
    for gi in gs..gs + gc {
        let go = group_off[gi] as usize;
        let gl = group_len[gi] as usize;
        let mut hit = false;
        for &f in &anyof_blob[go..go + gl] {
            if pos_feats.binary_search(&f).is_ok() {
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

/// Batch-level count-gate pre-reject — the broad/hot batch pass's necessary-
/// condition filter (Broad-Query Cost Program lever 5a, Vespa's `min_feature`
/// adapted to batch granularity). Returns `false` only when NO title in the
/// batch can possibly satisfy stored query `i`, so the caller may skip its full
/// bitmap verification. `batch_mask_union` is the OR of every batch title's
/// common-mask word; `present(f)` reports whether feature `f` occurs in ANY
/// batch title (the batch's distinct-feature set).
///
/// Sound by the same clauses [`eval_batch_slices`] evaluates: a title matches
/// only if it contains every required feature (mask + tail) and ≥1 member of
/// every any-of group — if a required feature is absent from the whole batch,
/// or an entire any-of group is, steps 1/2/4 provably produce an all-zero
/// bitmap for every title. **Under-reject is the only possible error
/// direction** (a `true` just means "run the full verification").
///
/// Forbidden features are deliberately never consulted — the never-gate-on-
/// MUST_NOT invariant: skipping work based on a *present* forbidden feature
/// would be gating on a negative. Tags are not consulted either (the tag gate
/// never prunes retrieval; `eval_batch_slices` step 0 handles it).
#[allow(clippy::too_many_arguments)]
#[inline]
pub fn prefilter_slices(
    i: usize,
    batch_mask_union: u64,
    present: impl Fn(FeatureId) -> bool,
    req_mask: &[u64],
    req_off: &[u32],
    req_len: &[u16],
    req_blob: &[u32],
    q_group_start: &[u32],
    q_group_count: &[u16],
    group_off: &[u32],
    group_len: &[u16],
    anyof_blob: &[u32],
) -> bool {
    // 1) every masked required feature appears somewhere in the batch (one AND).
    let rm = req_mask[i];
    if (rm & batch_mask_union) != rm {
        return false;
    }
    // 2) every required-tail feature appears in the batch's distinct set.
    let ro = req_off[i] as usize;
    let rl = req_len[i] as usize;
    for &f in &req_blob[ro..ro + rl] {
        if !present(f) {
            return false;
        }
    }
    // 3) every any-of group has at least one member in the batch.
    let gs = q_group_start[i] as usize;
    let gc = q_group_count[i] as usize;
    for gi in gs..gs + gc {
        let go = group_off[gi] as usize;
        let gl = group_len[gi] as usize;
        if !anyof_blob[go..go + gl].iter().any(|&m| present(m)) {
            return false;
        }
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
///
/// **Single-view (ADR-061):** this columnar kernel takes one `tmask_batch`/`lookup`, i.e. one title
/// view. That is correct because the broad-lane driver forces the *inline* two-view path
/// (`verify_slices`, which splits positive/negative) whenever a multi-word alias is active, so this
/// kernel only ever runs when `P(T) == N(T)` and a single view is exact. A columnar two-view is a
/// deferred perf follow-on.
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

    // Steps 2–4 below short-circuit the moment `acc` goes all-zero: every
    // remaining clause is an AND / AND-NOT, which preserves all-zero, so
    // returning early is byte-identical — it just skips provably-dead word
    // loops (the count-gate's in-kernel twin, lever 5a). The running OR costs
    // one register OR per word already being written.

    // 2) required tail: AND of each feature's title bitmap (verify step 2)
    let ro = req_off[i] as usize;
    let rl = req_len[i] as usize;
    for &f in &req_blob[ro..ro + rl] {
        if let Some(b) = lookup(f) {
            let mut nz = 0u64;
            for (a, x) in acc.iter_mut().zip(b) {
                *a &= *x;
                nz |= *a;
            }
            if nz == 0 {
                return;
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
            let mut nz = 0u64;
            for (a, x) in acc.iter_mut().zip(b) {
                *a &= !*x;
                nz |= *a;
            }
            if nz == 0 {
                return;
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
        let mut nz = 0u64;
        for (a, x) in acc.iter_mut().zip(grp.iter()) {
            *a &= *x;
            nz |= *a;
        }
        if nz == 0 {
            return;
        }
    }
}
