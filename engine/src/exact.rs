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

/// Shared exact-verification logic operating on raw slices. Used by both
/// in-memory ExactStore::verify and MmapSegment::verify to avoid duplication.
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

    true
}

#[derive(Clone)]
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
    // identity, resolved only on a confirmed match
    version: Vec<u32>,
    logical: Vec<u64>,
}

impl Default for ExactStore {
    fn default() -> Self {
        ExactStore {
            req_mask: Vec::new(),
            forb_mask: Vec::new(),
            req_off: Vec::new(),
            req_len: Vec::new(),
            req_blob: Vec::new(),
            forb_off: Vec::new(),
            forb_len: Vec::new(),
            forb_blob: Vec::new(),
            q_group_start: Vec::new(),
            q_group_count: Vec::new(),
            group_off: Vec::new(),
            group_len: Vec::new(),
            anyof_blob: Vec::new(),
            version: Vec::new(),
            logical: Vec::new(),
        }
    }
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

    /// Append a compiled query; returns its SegmentLocalQueryId.
    pub fn push(&mut self, ex: &Extracted, dict: &Dict, version: u32, logical: u64) -> u32 {
        let id = self.req_mask.len() as u32;

        let mut rmask = 0u64;
        let r_off = self.req_blob.len() as u32;
        let mut r_len = 0u16;
        for &f in &ex.required {
            let b = dict.mask_bit(f);
            if b != NO_MASK_BIT {
                rmask |= 1u64 << b;
            } else {
                self.req_blob.push(f);
                r_len += 1;
            }
        }

        let mut fmask = 0u64;
        let f_off = self.forb_blob.len() as u32;
        let mut f_len = 0u16;
        for &f in &ex.forbidden {
            let b = dict.mask_bit(f);
            if b != NO_MASK_BIT {
                fmask |= 1u64 << b;
            } else {
                self.forb_blob.push(f);
                f_len += 1;
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

    /// Verify one candidate against a title. `tmask` is the title's common-mask
    /// word; `tfeats` is the title's full sorted feature slice (for tail checks).
    #[inline]
    pub fn verify(&self, id: u32, tmask: u64, tfeats: &[FeatureId]) -> bool {
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

        true
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
        dest.forb_blob.extend_from_slice(&self.forb_blob[fo..fo + fl]);
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
            dest.anyof_blob.extend_from_slice(&self.anyof_blob[go..go + gl]);
            dest.group_off.push(new_go);
            dest.group_len.push(gl as u16);
        }
        dest.q_group_start.push(new_gs);
        dest.q_group_count.push(gc as u16);

        // identity
        dest.version.push(self.version[i]);
        dest.logical.push(self.logical[i]);
        new_id
    }

    // ---- slice accessors for serialization (storage.rs) ----
    pub fn req_masks(&self) -> &[u64] { &self.req_mask }
    pub fn forb_masks(&self) -> &[u64] { &self.forb_mask }
    pub fn req_offs(&self) -> &[u32] { &self.req_off }
    pub fn req_lens(&self) -> &[u16] { &self.req_len }
    pub fn req_blobs(&self) -> &[u32] { &self.req_blob }
    pub fn forb_offs(&self) -> &[u32] { &self.forb_off }
    pub fn forb_lens(&self) -> &[u16] { &self.forb_len }
    pub fn forb_blobs(&self) -> &[u32] { &self.forb_blob }
    pub fn q_group_starts(&self) -> &[u32] { &self.q_group_start }
    pub fn q_group_counts(&self) -> &[u16] { &self.q_group_count }
    pub fn group_offs(&self) -> &[u32] { &self.group_off }
    pub fn group_lens(&self) -> &[u16] { &self.group_len }
    pub fn anyof_blobs(&self) -> &[u32] { &self.anyof_blob }
    pub fn versions(&self) -> &[u32] { &self.version }
    pub fn logicals(&self) -> &[u64] { &self.logical }

    /// Push a raw entry (pre-computed masks and blobs). Used by MmapSegment::to_memory_segment
    /// to reconstruct an in-memory ExactStore from mmap'd data.
    pub fn push_raw(
        &mut self,
        rmask: u64,
        fmask: u64,
        req_tail: &[u32],
        forb_tail: &[u32],
        groups: (usize, usize, &[u32], &[u16], &[u32]), // (gs, gc, group_off, group_len, anyof_blob)
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
            + self.version.capacity() * size_of::<u32>()
            + self.logical.capacity() * size_of::<u64>()
    }
}
