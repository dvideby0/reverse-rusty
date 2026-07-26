use super::{predicate_has_phrases, ExactStore, FeatureId};

impl ExactStore {
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
            && self.predicate_len[i] == 0
            && self.req_mask[i].is_power_of_two()
    }

    /// The CANONICAL body signature of stored query `local` (dedup Stage A):
    /// a 64-bit hash over the query's SEMANTIC columns only — the two mask
    /// words, the required/forbidden tails as SORTED sets, the any-of groups as
    /// a SORTED multiset of sorted proxy sets, and the canonical compound
    /// predicate program. Tags, version and logical id are deliberately
    /// excluded (they are per-member identity, not semantics). Two queries with
    /// equal signatures are *candidates* for sharing; the caller must confirm
    /// with [`bodies_equal`](Self::bodies_equal) (a hash collision must never
    /// cause false sharing — that would be a correctness bug, not a missed
    /// optimization).
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
        mix(0xA5);
        let predicate_off = self.predicate_off[i] as usize;
        let predicate_len = self.predicate_len[i] as usize;
        for &word in &self.predicate_blob[predicate_off..predicate_off + predicate_len] {
            mix(u64::from(word));
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
    /// SEMANTIC columns only (masks, sorted tails, canonicalized proxy groups,
    /// compound predicate); never tags/version/logical.
    pub fn bodies_equal(&self, a: u32, b: u32) -> bool {
        let (ia, ib) = (a as usize, b as usize);
        if self.req_mask[ia] != self.req_mask[ib]
            || self.forb_mask[ia] != self.forb_mask[ib]
            || self.req_len[ia] != self.req_len[ib]
            || self.forb_len[ia] != self.forb_len[ib]
            || self.q_group_count[ia] != self.q_group_count[ib]
            || self.predicate_len[ia] != self.predicate_len[ib]
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
        if self.canonical_groups(ia) != self.canonical_groups(ib) {
            return false;
        }
        let predicate = |i: usize| {
            let off = self.predicate_off[i] as usize;
            let len = self.predicate_len[i] as usize;
            &self.predicate_blob[off..off + len]
        };
        predicate(ia) == predicate(ib)
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
            && self.predicate_len[i] == 0
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

        // compound exact predicate
        let po = self.predicate_off[i] as usize;
        let pl = self.predicate_len[i] as usize;
        let new_po = dest.predicate_blob.len() as u32;
        dest.predicate_blob
            .extend_from_slice(&self.predicate_blob[po..po + pl]);
        dest.predicate_off.push(new_po);
        dest.predicate_len.push(pl as u32);
        dest.has_phrase_predicates |= predicate_has_phrases(&self.predicate_blob[po..po + pl]);

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
        dest.source_generation.push(self.source_generation[i]);
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
    /// The returned pair feeds `build_signatures` through an `Extracted` whose
    /// non-anchor semantic columns are empty; the original exact predicate is
    /// carried separately and never rebuilt from these inputs. `mask_inverse`
    /// MUST come from the same frozen dict the segment was built against (the
    /// engine's frozen-mask invariant), or a set bit could map to the wrong
    /// feature. A query built before the mask was finalized has `req_mask == 0`,
    /// so the un-masking loop is a natural no-op.
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
}
