use super::{predicate_has_phrases, ExactStore, QueryPlacement, TagId};

impl ExactStore {
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
    pub fn predicate_offs(&self) -> &[u32] {
        &self.predicate_off
    }
    pub fn predicate_lens(&self) -> &[u32] {
        &self.predicate_len
    }
    pub fn predicate_blobs(&self) -> &[u32] {
        &self.predicate_blob
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
    pub fn source_generations(&self) -> &[u64] {
        &self.source_generation
    }
    pub fn max_source_generation(&self) -> u64 {
        self.source_generation.iter().copied().max().unwrap_or(0)
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
        self.push_raw_placed_with_source_generation(
            rmask, fmask, req_tail, forb_tail, groups, tags, version, logical, priority, placement,
            0,
        )
    }

    /// Raw-row reconstruction including validated ownership and source-generation
    /// metadata. A generation of zero is the compatibility value for pre-v8 rows.
    #[allow(clippy::too_many_arguments)]
    pub fn push_raw_placed_with_source_generation(
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
        source_generation: u64,
    ) -> u32 {
        self.push_raw_placed_with_source_generation_and_predicate(
            rmask,
            fmask,
            req_tail,
            forb_tail,
            groups,
            &[],
            tags,
            version,
            logical,
            priority,
            placement,
            source_generation,
        )
    }

    /// Raw-row reconstruction including an already-validated compound predicate
    /// program. Used by the v9/v10 mmap compaction path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_raw_placed_with_source_generation_and_predicate(
        &mut self,
        rmask: u64,
        fmask: u64,
        req_tail: &[u32],
        forb_tail: &[u32],
        groups: (usize, usize, &[u32], &[u16], &[u32]),
        predicate: &[u32],
        tags: &[TagId],
        version: u32,
        logical: u64,
        priority: i64,
        placement: &QueryPlacement,
        source_generation: u64,
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

        let predicate_off = self.predicate_blob.len() as u32;
        self.predicate_blob.extend_from_slice(predicate);
        self.predicate_off.push(predicate_off);
        self.predicate_len.push(predicate.len() as u32);
        self.has_phrase_predicates |= predicate_has_phrases(predicate);

        let t_off = self.tag_blob.len() as u32;
        self.tag_blob.extend_from_slice(tags);
        self.tag_off.push(t_off);
        self.tag_len.push(tags.len() as u16);

        self.version.push(version);
        self.logical.push(logical);
        self.priority.push(priority);
        self.push_placement(placement);
        self.source_generation.push(source_generation);
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
            + self.predicate_off.capacity() * size_of::<u32>()
            + self.predicate_len.capacity() * size_of::<u32>()
            + self.predicate_blob.capacity() * size_of::<u32>()
            + self.tag_off.capacity() * size_of::<u32>()
            + self.tag_len.capacity() * size_of::<u16>()
            + self.tag_blob.capacity() * size_of::<TagId>()
            + self.version.capacity() * size_of::<u32>()
            + self.logical.capacity() * size_of::<u64>()
            + self.priority.capacity() * size_of::<i64>()
            + self.source_generation.capacity() * size_of::<u64>()
            + self.placement_generation.capacity() * size_of::<u64>()
            + self.placement_num_shards.capacity() * size_of::<u32>()
            + self.placement_mode.capacity() * size_of::<u8>()
            + self.placement_off.capacity() * size_of::<u32>()
            + self.placement_len.capacity() * size_of::<u32>()
            + self.placement_blob.capacity() * size_of::<u32>()
    }
}
