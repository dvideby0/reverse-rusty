use super::{
    encode_predicate, predicate_has_phrases, Dict, ExactStore, Extracted, PlacementGeneration,
    PlacementMode, QueryPlacement, QueryPlacementRef, RankValues, TagId, NO_MASK_BIT,
};

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

    #[inline]
    pub fn has_phrase_predicates(&self) -> bool {
        self.has_phrase_predicates
    }

    /// Whether one row carries the v2 positioned predicate program.
    #[inline]
    pub(crate) fn row_has_phrase_predicates(&self, id: u32) -> bool {
        let i = id as usize;
        let off = self.predicate_off[i] as usize;
        let len = self.predicate_len[i] as usize;
        predicate_has_phrases(&self.predicate_blob[off..off + len])
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
        self.push_ranked_with_placement_and_source_generation(
            ex, tags, dict, version, logical, rank, placement, 0,
        )
    }

    /// Append a compiled query with distributed placement and the engine's
    /// internal source generation. Public compatibility callers use
    /// [`Self::push_ranked_with_placement`], whose generation `0` denotes a
    /// legacy/unknown row; engine-owned writes always pass a non-zero value.
    #[allow(clippy::too_many_arguments)]
    pub fn push_ranked_with_placement_and_source_generation(
        &mut self,
        ex: &Extracted,
        tags: &[TagId],
        dict: &Dict,
        version: u32,
        logical: u64,
        rank: RankValues,
        placement: &QueryPlacement,
        source_generation: u64,
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
        let (predicate_off, predicate_len) = encode_predicate(ex, &mut self.predicate_blob);
        self.has_phrase_predicates |=
            !ex.required_phrases.is_empty() || !ex.forbidden_phrases.is_empty();

        self.req_mask.push(rmask);
        self.forb_mask.push(fmask);
        self.req_off.push(r_off);
        self.req_len.push(r_len);
        self.forb_off.push(f_off);
        self.forb_len.push(f_len);
        self.q_group_start.push(g_start);
        self.q_group_count.push(g_count);
        self.predicate_off.push(predicate_off);
        self.predicate_len.push(predicate_len);

        let t_off = self.tag_blob.len() as u32;
        self.tag_blob.extend_from_slice(tags);
        self.tag_off.push(t_off);
        self.tag_len.push(tags.len() as u16);

        self.version.push(version);
        self.logical.push(logical);
        self.priority.push(rank.priority);
        self.push_placement(placement);
        self.source_generation.push(source_generation);
        id
    }

    pub(super) fn push_placement(&mut self, placement: &QueryPlacement) {
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
    pub fn source_generation(&self, id: u32) -> u64 {
        self.source_generation[id as usize]
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
}
