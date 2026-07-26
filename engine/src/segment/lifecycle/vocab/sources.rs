use super::Engine;

impl Engine {
    /// The current live `(logical_id, query_text)` set — the source corpus the
    /// index is a materialized view of, sorted by logical id for deterministic
    /// rebuilds. Backed by the query store (kept in sync with the index by the
    /// insert/delete paths) and **cross-checked against index liveness**: a store
    /// entry with no live copy in this engine is stale residue (e.g. a query a
    /// pre-fix green rebuild moved to another shard — codex retro-review, ADR-074)
    /// and is skipped, so a polluted `sources.dat` self-heals at the next gather
    /// rather than resurrecting moved or deleted queries.
    /// Used by [`recompile_stale_segments`](Self::recompile_stale_segments).
    pub fn live_sources(&self) -> Vec<(u64, String)> {
        let mut out: Vec<(u64, String)> = Vec::with_capacity(self.query_store.len());
        self.query_store.for_each_live(|logical, text| {
            if self.live_metadata_for(logical).is_some() {
                out.push((logical, text.to_string()));
            }
        });
        out.sort_unstable_by_key(|&(l, _)| l);
        out
    }

    /// The current live logical-id set without copying query source text.
    /// Durable cluster open uses this to rebuild its compact unique-id directory;
    /// the same liveness cross-check as [`Self::live_sources`] excludes stale
    /// source-store residue.
    pub(crate) fn live_logical_ids(&self) -> Vec<u64> {
        let mut out = Vec::with_capacity(self.query_store.len());
        self.query_store.for_each_live(|logical, _| {
            if self.live_metadata_for(logical).is_some() {
                out.push(logical);
            }
        });
        out.sort_unstable();
        out
    }

    /// [`live_sources`](Self::live_sources) plus each live query's current `TagId`s — the
    /// gather behind the CLUSTER blue/green rebuild (`ClusterEngine::set_vocab`, ADR-074),
    /// which re-places every query and must carry its tags to the new shard. Ids — interned
    /// dense or post-freeze synthetic — are carried verbatim: the tag space is preserved
    /// across a vocabulary change, so they stay valid (the same ADR-049 carry-through
    /// [`recompile_stale_segments`](Self::recompile_stale_segments) uses in-place).
    pub fn live_sources_tagged(&self) -> Vec<crate::segment::LiveTaggedSource> {
        let mut out = Vec::with_capacity(self.query_store.len());
        self.query_store.for_each_live(|logical, text| {
            if let Some((version, _, tags, rank, placement)) = self.live_metadata_for(logical) {
                out.push((logical, text.to_string(), version, tags, rank, placement));
            }
        });
        out.sort_unstable_by_key(|&(l, ..)| l);
        out
    }

    /// Distinct logical ids represented by at least one live exact row. This is
    /// the index-side half of rebuild-source completeness; counting physical
    /// rows is insufficient because supported additive histories may leave
    /// multiple live copies of one logical id.
    fn live_exact_logical_ids(&self) -> crate::util::FastSet<u64> {
        let mut logicals = crate::util::FastSet::default();
        for local in 0..self.memtable.len() {
            let local = local as u32;
            if self.memtable.is_alive(local) {
                logicals.insert(self.memtable.exact_store().logical(local));
            }
        }
        for segment in &self.segments {
            for local in 0..segment.len() {
                let local = local as u32;
                if segment.is_alive(local) {
                    logicals.insert(segment.logical(local));
                }
            }
        }
        logicals
    }

    /// Lowest logical id represented by more than one live physical exact row.
    /// Ordinary vocabulary rebuilds intentionally canonicalize such additive
    /// histories to the newest source generation. A compiler compatibility
    /// migration has a stricter contract: it may not change the pre-upgrade
    /// matched set, so it uses this check to refuse when the one-document source
    /// store cannot reconstruct every physical predicate.
    pub(in crate::segment) fn duplicate_live_logical_id(&self) -> Option<u64> {
        let mut logicals = crate::util::FastSet::default();
        let mut duplicate = None;
        for local in 0..self.memtable.len() {
            let local = local as u32;
            if self.memtable.is_alive(local) {
                let logical = self.memtable.exact_store().logical(local);
                if !logicals.insert(logical) {
                    duplicate = Some(duplicate.map_or(logical, |seen: u64| seen.min(logical)));
                }
            }
        }
        for segment in &self.segments {
            for local in 0..segment.len() {
                let local = local as u32;
                if segment.is_alive(local) {
                    let logical = segment.logical(local);
                    if !logicals.insert(logical) {
                        duplicate = Some(duplicate.map_or(logical, |seen: u64| seen.min(logical)));
                    }
                }
            }
        }
        duplicate
    }

    /// Internal document-complete variant of [`Self::live_sources_tagged`].
    /// Carries canonical raw tags for cluster source read-back across rebuilds
    /// and fails if either durable domain lacks a matching logical id.
    pub(crate) fn live_source_documents_tagged(
        &self,
    ) -> Result<Vec<crate::segment::LiveSourceDocument>, u64> {
        let mut out = Vec::with_capacity(self.query_store.len());
        let mut mismatch = None;
        let mut uncovered = self.live_exact_logical_ids();
        // One liveness scan per entry: `None` = no live copy in this engine (stale store
        // residue — skipped, see `live_sources`), `Some((version, tags))` = live, possibly
        // untagged. The version is the live copy's stored version, carried through the rebuild
        // so a `set_vocab`/resize re-places at version N rather than resetting to 1 (ADR-074).
        self.query_store.for_each_live_document(
            |logical,
             text,
             source_version,
             stored_generation,
             raw_tags,
             tags_known,
             metadata_known| {
                if mismatch.is_some() {
                    return;
                }
                if let Some((version, exact_generation, tags, rank, placement)) =
                    self.live_metadata_for(logical)
                {
                    if (metadata_known
                        && (source_version != version || stored_generation != exact_generation))
                        || (!metadata_known && (stored_generation != 0 || exact_generation != 0))
                    {
                        mismatch = Some(logical);
                        return;
                    }
                    let raw_tags = if tags_known {
                        raw_tags.to_vec()
                    } else {
                        let Some(recovered) = tags
                            .iter()
                            .map(|&id| {
                                self.tag_dict
                                    .key_value(id)
                                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                            })
                            .collect::<Option<Vec<_>>>()
                        else {
                            mismatch = Some(logical);
                            return;
                        };
                        recovered
                    };
                    out.push((
                        logical,
                        text.to_string(),
                        version,
                        exact_generation,
                        raw_tags,
                        tags,
                        rank,
                        placement,
                    ));
                    uncovered.remove(&logical);
                }
            },
        );
        if mismatch.is_none() {
            // A missing/partial sources.dat has no entry for the callback above
            // to reject. Check from exact → source as well so a vocabulary
            // change cannot rebuild an acknowledged live corpus from a strict
            // subset and silently drop the uncovered rows.
            mismatch = uncovered.into_iter().min();
        }
        if let Some(logical) = mismatch {
            return Err(logical);
        }
        out.sort_unstable_by_key(|&(l, ..)| l);
        Ok(out)
    }

    /// The current `TagId`s of the live entry for `logical` (ADR-049), read from the
    /// memtable or a base segment. Used by [`recompile_stale_segments`] to carry a
    /// query's tags through a vocabulary change unchanged (same tag space ⇒ the ids stay
    /// valid), and by the gathers above as the index-liveness check. `None` when the
    /// query has NO live copy in this engine (distinct from `Some(vec![])` — live but
    /// untagged): conflating the two is exactly what let a stale store entry shadow a
    /// moved query's tagged copy (codex retro-review, ADR-074).
    fn live_metadata_for(
        &self,
        logical: u64,
    ) -> Option<(
        u32,
        u64,
        Vec<crate::tagdict::TagId>,
        crate::rank::RankValues,
        crate::ownership::QueryPlacement,
    )> {
        // Source generations, not storage tiers, define mutation order. A supported
        // additive history can write to the memtable and then append a newer bulk base
        // segment for the same logical id. Scan newest-looking locations first only as
        // the generation-zero legacy tie-break; any larger generation wins globally.
        let mut best = None;
        for &local in self.memtable.locals_for_logical(logical).iter().rev() {
            if self.memtable.is_alive(local) {
                let source_generation = self.memtable.source_generation_of(local);
                let replace = match &best {
                    Some((_, best_generation, _, _, _)) => source_generation > *best_generation,
                    None => true,
                };
                if !replace {
                    continue;
                }
                let tags = self.memtable.tags_of(local);
                let mut rank = self.memtable.rank_values(local);
                if rank.priority == 0 {
                    rank.priority = self.tag_dict.legacy_priority_for_tags(tags);
                }
                best = Some((
                    self.memtable.version_of(local),
                    source_generation,
                    tags.to_vec(),
                    rank,
                    self.memtable.placement(local).to_owned(),
                ));
            }
        }
        for seg in self.segments.iter().rev() {
            for &local in seg.locals_for_logical(logical).iter().rev() {
                if seg.is_alive(local) {
                    let source_generation = seg.source_generation_of(local);
                    let replace = match &best {
                        Some((_, best_generation, _, _, _)) => source_generation > *best_generation,
                        None => true,
                    };
                    if !replace {
                        continue;
                    }
                    let tags = seg.tags_of(local);
                    let mut rank = seg.rank_values(local);
                    if rank.priority == 0 {
                        rank.priority = self.tag_dict.legacy_priority_for_tags(tags);
                    }
                    best = Some((
                        seg.version_of(local),
                        source_generation,
                        tags.to_vec(),
                        rank,
                        seg.placement(local).to_owned(),
                    ));
                }
            }
        }
        best
    }
}
