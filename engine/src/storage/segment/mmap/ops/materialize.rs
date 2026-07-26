use super::{CandidateIndex, CostClass, MmapSegment, Segment};

impl MmapSegment {
    /// Reconstruct an in-memory Segment from this mmap'd segment. Used by
    /// compaction to produce source data for Segment::compact_from.
    pub fn to_memory_segment(&self, tag_dict: &crate::tagdict::TagDict) -> Segment {
        use crate::exact::ExactStore;
        let n = self.num_queries as usize;

        let mut exact = ExactStore::new();
        let mut classes = Vec::with_capacity(n);
        let mut alive = Vec::with_capacity(n);

        // Copy exact store arrays
        for i in 0..n {
            let rm = self.req_mask()[i];
            let fm = self.forb_mask()[i];
            let ro = self.req_off()[i] as usize;
            let rl = self.req_len()[i] as usize;
            let fo = self.forb_off()[i] as usize;
            let fl = self.forb_len()[i] as usize;
            let gs = self.q_group_start()[i] as usize;
            let gc = self.q_group_count()[i] as usize;
            let po = self.predicate_off().get(i).copied().unwrap_or(0) as usize;
            let pl = self.predicate_len().get(i).copied().unwrap_or(0) as usize;
            // SAFETY: the loop runs `i` over `0..n` where `n == num_queries`, and
            // `version_arr`/`logical_arr` are both `num_queries`-long arrays parsed
            // from the mmap in `open`, so both offsets are in bounds of the
            // immutable mapping `self` owns.
            let (ver, log) = unsafe { (*self.version_arr.add(i), *self.logical_arr.add(i)) };

            // Per-query tag slice (ADR-049); empty for a pre-tag (v1/v2) segment whose
            // tag column has no entries, so compaction carries tags through faithfully.
            let tags: &[crate::tagdict::TagId] =
                match (self.tag_off().get(i), self.tag_len().get(i)) {
                    (Some(&o), Some(&l)) => &self.tag_blob()[o as usize..o as usize + l as usize],
                    _ => &[],
                };

            let stored = self.rank_values(i as u32).priority;
            let priority = if self.priority_count == 0 {
                tag_dict.legacy_priority_for_tags(tags)
            } else {
                stored
            };
            let placement = self.placement(i as u32).to_owned();
            exact.push_raw_placed_with_source_generation_and_predicate(
                rm,
                fm,
                &self.req_blob()[ro..ro + rl],
                &self.forb_blob()[fo..fo + fl],
                (
                    gs,
                    gc,
                    self.group_off(),
                    self.group_len(),
                    self.anyof_blob(),
                ),
                &self.predicate_blob()[po..po + pl],
                tags,
                ver,
                log,
                priority,
                &placement,
                self.source_generation(i as u32),
            );

            // SAFETY: `i < n == num_queries`, and `class_arr` is the
            // `num_queries`-long class byte array parsed from the mmap, so the
            // offset is in bounds of the immutable mapping.
            let class_byte = unsafe { *self.class_arr.add(i) };
            classes.push(match class_byte {
                0 => CostClass::A,
                1 => CostClass::B,
                2 => CostClass::C,
                4 => CostClass::H,
                // 3 is the only remaining byte `open`'s validation admits.
                _ => CostClass::D,
            });
            alive.push(self.alive_overlay[i]);
        }

        // Rebuild candidate indexes from frozen tables
        let mut main = CandidateIndex::new();
        for slot in self.main_slots() {
            if slot.key != 0 {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                for &id in &self.main_blob()[start..end] {
                    main.insert(slot.key, id);
                }
            }
        }

        let mut broad = CandidateIndex::new();
        for slot in self.broad_slots() {
            if slot.key != 0 {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                for &id in &self.broad_blob()[start..end] {
                    broad.insert(slot.key, id);
                }
            }
        }

        // Hot-tier index (class H, ADR-105): empty pre-v5 / on hot-free files.
        // Skipping this rebuild would silently unanchor every class-H entry
        // through a compaction — a false negative.
        let mut hot = CandidateIndex::new();
        for slot in self.hot_slots() {
            if slot.key != 0 {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                for &id in &self.hot_blob()[start..end] {
                    hot.insert(slot.key, id);
                }
            }
        }

        let mut seg = Segment::from_parts(main, broad, hot, exact, classes, alive);
        seg.vocab_epoch = self.vocab_epoch;
        seg.compiler_semantics_version = self.compiler_semantics_version();
        seg
    }
}
