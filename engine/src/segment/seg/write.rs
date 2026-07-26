use super::{
    build_signatures, rejects_class_d, AddedCompiled, CandidateIndex, CompileKnobs, CostClass,
    Dict, ExactStore, Extracted, Segment, SegmentFilter,
};

impl Segment {
    pub fn new() -> Self {
        Segment {
            main: CandidateIndex::new(),
            broad: CandidateIndex::new(),
            hot: CandidateIndex::new(),
            exact: ExactStore::new(),
            class: Vec::new(),
            alive: Vec::new(),
            alive_counter: 0,
            live_phrase_predicates: 0,
            filter: None,
            vocab_epoch: 0,
            compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
            logical_index: crate::util::fast_map(),
            dup_of: Vec::new(),
            dup_members: crate::util::fast_map(),
            body_index: crate::util::fast_map(),
        }
    }

    /// Whether this segment holds any shared body groups (dedup Stage A). The
    /// per-segment gate that keeps every match path byte-identical (and
    /// zero-extra-cost) on dup-free segments — incl. every mmap-attached
    /// segment, whose on-disk postings are always expanded.
    #[inline]
    pub fn has_dup_groups(&self) -> bool {
        !self.dup_members.is_empty()
    }

    /// Leader → duplicate members (empty slice for a singleton). Only meaningful
    /// on segments where [`has_dup_groups`](Self::has_dup_groups) is true.
    /// `pub(crate)` for the flush writer, which EXPANDS groups back into plain
    /// postings (the on-disk format carries no group indirection in Stage A).
    #[inline]
    pub(crate) fn members_of(&self, leader: u32) -> &[u32] {
        self.dup_members.get(&leader).map_or(&[], |v| v.as_slice())
    }

    /// This segment's body-group leader for `local` (`local` itself unless it
    /// was deduplicated into another entry's group).
    #[inline]
    pub fn dup_leader_of(&self, local: u32) -> u32 {
        self.dup_of.get(local as usize).copied().unwrap_or(local)
    }

    /// Build and attach the anchor filter from the current main + broad + hot
    /// index keys. Called once when a segment is sealed (flush, bulk_ingest,
    /// compaction). After this, `match_into` will use the filter to skip probes.
    pub(in crate::segment) fn build_filter(&mut self) {
        let mut keys = self.main.keys();
        keys.extend(self.broad.keys());
        keys.extend(self.hot.keys());
        self.filter = Some(SegmentFilter::build(&keys));
        // Sealing also retires the building-time body index (dedup Stage A):
        // sealed read paths and the merges consult only `dup_of`/`dup_members`
        // (a merge regroups into the DEST's fresh index), so keeping ~one map
        // entry per distinct body would be pure resident overhead at scale
        // (codex review). The memtable keeps its index until IT seals here.
        self.body_index = crate::util::fast_map();
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.exact.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty()
    }

    /// AST→compiled-query lowering semantics carried by this segment.
    #[inline]
    pub fn compiler_semantics_version(&self) -> u32 {
        self.compiler_semantics_version
    }

    pub fn main_index(&self) -> &CandidateIndex {
        &self.main
    }
    pub fn broad_index(&self) -> &CandidateIndex {
        &self.broad
    }
    /// The hot tier's candidate index (class H, ADR-105).
    pub fn hot_index(&self) -> &CandidateIndex {
        &self.hot
    }
    /// Whether this segment holds any hot-tier entries — the per-segment skip
    /// that makes the hot tier structurally free on hot-empty corpora.
    #[inline]
    pub fn has_hot_entries(&self) -> bool {
        self.hot.num_signatures() > 0
    }

    #[inline]
    pub fn has_phrase_predicates(&self) -> bool {
        self.live_phrase_predicates != 0
    }

    /// Append one already-extracted query. Returns the new segment-local id plus
    /// the plan's [`would_be_hot`](crate::compile::SigPlan::would_be_hot)
    /// observe-first flag (the Broad-Query Cost Program's reclassification
    /// telemetry — the `Engine` accumulates it per accepted compile), or `None`
    /// if the query is class D and rejected. `tags` are the query's interned,
    /// sorted `TagId`s (ADR-049); pass `&[]` for an untagged query.
    ///
    /// `accept_class_d` (ADR-068): when set, a negation-only query (class D with a
    /// non-empty forbidden set) is stored as an **always-candidate** under the
    /// universal broad signature its plan carries. A query with no positives AND no
    /// forbidden features (an effectively empty query — it would match every title
    /// outright) is rejected regardless. Ingest paths pass the
    /// `EngineConfig::accept_class_d` knob; WAL replay and the vocab recompile pass
    /// `true` unconditionally (an acknowledged/stored query must never be dropped
    /// by a since-flipped knob).
    pub fn add_compiled(
        &mut self,
        ex: &Extracted,
        tags: &[crate::tagdict::TagId],
        dict: &Dict,
        logical: u64,
        version: u32,
        knobs: CompileKnobs,
    ) -> Option<AddedCompiled> {
        self.add_compiled_ranked(
            ex,
            tags,
            dict,
            logical,
            version,
            crate::rank::RankValues::default(),
            knobs,
        )
    }

    /// [`add_compiled`](Self::add_compiled) with the fixed typed rank columns.
    #[allow(clippy::too_many_arguments)]
    pub fn add_compiled_ranked(
        &mut self,
        ex: &Extracted,
        tags: &[crate::tagdict::TagId],
        dict: &Dict,
        logical: u64,
        version: u32,
        rank: crate::rank::RankValues,
        knobs: CompileKnobs,
    ) -> Option<AddedCompiled> {
        self.add_compiled_ranked_placed(
            ex,
            tags,
            dict,
            logical,
            version,
            rank,
            &crate::ownership::QueryPlacement::standalone(),
            knobs,
        )
    }

    /// Engine-owned standalone write path carrying the internal source
    /// generation. Kept crate-private so external segment builders retain the
    /// generation-zero compatibility contract.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_compiled_ranked_with_source_generation(
        &mut self,
        ex: &Extracted,
        tags: &[crate::tagdict::TagId],
        dict: &Dict,
        logical: u64,
        version: u32,
        rank: crate::rank::RankValues,
        source_generation: u64,
        knobs: CompileKnobs,
    ) -> Option<AddedCompiled> {
        self.add_compiled_ranked_placed_with_source_generation(
            ex,
            tags,
            dict,
            logical,
            version,
            rank,
            &crate::ownership::QueryPlacement::standalone(),
            source_generation,
            knobs,
        )
    }

    /// [`add_compiled_ranked`](Self::add_compiled_ranked) carrying identity-only
    /// distributed emission placement through dedup and compaction (ADR-109).
    #[allow(clippy::too_many_arguments)]
    pub fn add_compiled_ranked_placed(
        &mut self,
        ex: &Extracted,
        tags: &[crate::tagdict::TagId],
        dict: &Dict,
        logical: u64,
        version: u32,
        rank: crate::rank::RankValues,
        placement: &crate::ownership::QueryPlacement,
        knobs: CompileKnobs,
    ) -> Option<AddedCompiled> {
        self.add_compiled_ranked_placed_with_source_generation(
            ex, tags, dict, logical, version, rank, placement, 0, knobs,
        )
    }

    /// Engine-owned write path carrying the internal source generation that
    /// fences exact rows from stale `_source` records. A generation of zero is
    /// reserved for legacy/public compatibility builders.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_compiled_ranked_placed_with_source_generation(
        &mut self,
        ex: &Extracted,
        tags: &[crate::tagdict::TagId],
        dict: &Dict,
        logical: u64,
        version: u32,
        rank: crate::rank::RankValues,
        placement: &crate::ownership::QueryPlacement,
        source_generation: u64,
        knobs: CompileKnobs,
    ) -> Option<AddedCompiled> {
        let plan = build_signatures(ex, dict, knobs.hot_anchor_threshold);
        if rejects_class_d(plan.class, ex, knobs.accept_class_d) {
            return None;
        }
        let local = self.exact.push_ranked_with_placement_and_source_generation(
            ex,
            tags,
            dict,
            version,
            logical,
            rank,
            placement,
            source_generation,
        );

        // Canonical-body dedup (Stage A): an entry whose SEMANTIC body equals an
        // existing leader's joins that group instead of inserting postings — it
        // is reached, verified once, and emitted through the leader. Identity
        // (logical/version/tags) stays per-member; a hash hit is confirmed with
        // exact body equality (a collision must never cause false sharing).
        let body_hash = self.exact.body_signature(local);
        let mut is_duplicate = false;
        if knobs.dedup_bodies {
            if let Some(leaders) = self.body_index.get(&body_hash) {
                if let Some(&leader) = leaders.iter().find(|&&l| self.exact.bodies_equal(l, local))
                {
                    self.dup_of.push(leader);
                    self.dup_members.entry(leader).or_default().push(local);
                    is_duplicate = true;
                    // ADOPT the leader's class: the member rides the leader's
                    // postings, so its class byte must describe the lane it
                    // actually lives in. (Identical bodies CAN plan different
                    // classes — a θ-crossing frequency bump between two adds
                    // flips A→H — and A/B/H are all always-visible, so the
                    // adoption is lossless. The structural classes C/D cannot
                    // diverge between identical bodies under the frozen mask.)
                    self.class.push(self.class[leader as usize]);
                }
            }
        }
        if !is_duplicate {
            self.dup_of.push(local);
            if knobs.dedup_bodies {
                self.body_index.entry(body_hash).or_default().push(local);
            }
            for &s in &plan.main_sigs {
                self.main.insert(s, local);
            }
            for &s in &plan.broad_sigs {
                self.broad.insert(s, local);
            }
            for &s in &plan.hot_sigs {
                self.hot.insert(s, local);
            }
            self.class.push(plan.class);
        }
        self.alive.push(true);
        self.alive_counter += 1;
        if self.exact.row_has_phrase_predicates(local) {
            self.live_phrase_predicates += 1;
        }
        self.logical_index.entry(logical).or_default().push(local);
        Some(AddedCompiled {
            local,
            would_be_hot: plan.would_be_hot,
            body_hash,
            is_duplicate,
        })
    }

    pub fn tombstone(&mut self, local_id: u32) {
        if let Some(slot) = self.alive.get_mut(local_id as usize) {
            if *slot {
                self.alive_counter -= 1;
                if self.exact.row_has_phrase_predicates(local_id) {
                    self.live_phrase_predicates -= 1;
                }
            }
            *slot = false;
        }
    }

    pub fn locals_for_logical(&self, logical_id: u64) -> &[u32] {
        self.logical_index
            .get(&logical_id)
            .map_or(&[], |v| v.as_slice())
    }

    /// The sorted `TagId` slice for a local id (ADR-049) — read back for the
    /// `set_vocab` recompile so tags survive a vocabulary change.
    pub fn tags_of(&self, local_id: u32) -> &[crate::tagdict::TagId] {
        self.exact.tags_of(local_id)
    }

    pub fn rank_values(&self, local_id: u32) -> crate::rank::RankValues {
        self.exact.rank_values(local_id)
    }

    pub fn placement(&self, local_id: u32) -> crate::ownership::QueryPlacementRef<'_> {
        self.exact.placement(local_id)
    }

    /// The stored per-query version for a local id — read back for the cluster
    /// rebuild gather (ADR-074) so a `set_vocab`/resize preserves a query's stored
    /// version rather than resetting it to 1.
    pub fn version_of(&self, local_id: u32) -> u32 {
        self.exact.version(local_id)
    }

    /// Internal source generation paired with this exact row. Zero denotes a
    /// pre-v8 legacy row.
    pub(in crate::segment) fn source_generation_of(&self, local_id: u32) -> u64 {
        self.exact.source_generation(local_id)
    }

    pub(in crate::segment) fn max_source_generation(&self) -> u64 {
        self.exact.max_source_generation()
    }

    pub(in crate::segment) fn class_of(&self, local_id: u32) -> Option<CostClass> {
        self.class.get(local_id as usize).copied()
    }

    pub(in crate::segment) fn verify_local(
        &self,
        local_id: u32,
        view: &crate::exact::TitleView<'_>,
        pred: &crate::exact::TagPredicate,
    ) -> bool {
        self.exact.verify(local_id, view, pred)
    }

    /// Whether a local id is alive (not tombstoned).
    #[inline]
    pub fn is_alive(&self, local_id: u32) -> bool {
        self.alive.get(local_id as usize).copied().unwrap_or(false)
    }

    pub fn class_counts(&self, c: &mut [u64; 5]) {
        for &cl in &self.class {
            match cl {
                CostClass::A => c[0] += 1,
                CostClass::B => c[1] += 1,
                CostClass::C => c[2] += 1,
                CostClass::D => c[3] += 1,
                // Index 4 is APPENDED (never reordered): the autoscaler and the
                // class-D pins read c[2]/c[3] positionally.
                CostClass::H => c[4] += 1,
            }
        }
    }
}
