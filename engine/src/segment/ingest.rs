//! `impl Engine` — the write path: initial build, live insert, tombstone/delete,
//! bulk ingest, and the WAL-replay helpers used by recovery (`open`).

use super::{
    AcceptedSource, Engine, IngestItemStatus, IngestReport, InsertOutcome, PlacedQuery, Segment,
    SegmentAddress,
};
use std::sync::Arc;

use crate::compile::{extract, Extracted};
use crate::segment::UpsertOutcome;
use crate::tagdict::TagId;

impl Engine {
    /// Reserve one non-zero internal source generation. Gaps are intentional:
    /// compilation or durable-commit failure may consume a reservation, but a
    /// generation is never reused during the engine's lifetime. Reopen seeds the
    /// counter above every generation found in either durable domain.
    pub(in crate::segment) fn allocate_source_generation(&mut self) -> u64 {
        let generation = self.next_source_generation.max(1);
        self.next_source_generation = generation.wrapping_add(1).max(1);
        generation
    }

    /// Recover a WAL v7 generation verbatim and advance the live allocator past
    /// it. Generation-less legacy frames stay at zero: pre-v8 exact/source rows
    /// use storage order as their tie-break, and inventing a fresh post-reopen
    /// generation would let an older WAL row outrank a later bulk segment.
    pub(in crate::segment) fn replay_source_generation(
        &mut self,
        source_generation: Option<u64>,
    ) -> u64 {
        let Some(source_generation) = source_generation.filter(|&generation| generation != 0)
        else {
            return 0;
        };
        if source_generation >= self.next_source_generation {
            self.next_source_generation = source_generation.wrapping_add(1).max(1);
        }
        source_generation
    }

    /// Reject a query whose tag set exceeds `config.max_tags` (ADR-049) BEFORE any
    /// durable write, so an over-large set never reaches the SoA tag column (whose
    /// per-query count is a `u16` — truncation there would silently drop a real tag
    /// and break filtered percolation's match guarantee). Conservative: checks the
    /// raw `(key,value)` count, which is `>=` the deduped count the column stores, so
    /// it never lets a truncating set through. Empty / within-limit ⇒ `Ok`.
    ///
    /// Enforced on the live/build ingest front doors only; WAL replay does NOT call
    /// this (an already-acknowledged write must never be dropped on recovery — the
    /// same policy the clause/any-of limits follow, see [`replay_insert`]).
    fn check_tag_limit(&self, tags: &[(String, String)]) -> Result<(), crate::error::ParseError> {
        if tags.len() > self.config.max_tags {
            return Err(crate::error::ParseError::new(
                crate::error::ParseErrorKind::TooManyTags,
                0,
            ));
        }
        Ok(())
    }

    /// Reject a COMPILED query whose required / forbidden / any-of column would
    /// overflow the SoA exact store's `u16` count encoding, BEFORE any durable
    /// write — so the truncating `as u16` cast in [`ExactStore::push`] is never
    /// reached. The parser ceilings (`max_query_clauses`, `max_anyof_group_size`)
    /// bound the AST but NOT the compiled columns (e.g. two negated any-of clauses
    /// flatten into one forbidden column that can exceed `u16::MAX`), so this is the
    /// structural backstop. Runs on the FINAL `Extracted` (after equivalence
    /// expansion, which can widen the columns). See [`Extracted::column_overflow`].
    fn check_column_limit(ex: &Extracted) -> Result<(), crate::error::ParseError> {
        if ex.column_overflow().is_some() {
            return Err(crate::error::ParseError::new(
                crate::error::ParseErrorKind::CompiledColumnTooLarge,
                0,
            ));
        }
        Ok(())
    }

    /// Intern a query's `(key, value)` metadata tags into the engine's tag dictionary
    /// (copy-on-write, like the feature dict), returning a sorted + deduped `TagId` slice
    /// ready for the SoA tag column (ADR-049). Empty input ⇒ empty (no CoW clone).
    fn intern_tags(&mut self, tags: &[(String, String)]) -> Vec<TagId> {
        if tags.is_empty() {
            return Vec::new();
        }
        let td = Arc::make_mut(&mut self.tag_dict);
        let mut ids: Vec<TagId> = tags.iter().map(|(k, v)| td.intern(k, v)).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    fn legacy_rank_values(&self, tag_ids: &[TagId]) -> crate::rank::RankValues {
        crate::rank::RankValues {
            priority: self.tag_dict.legacy_priority_for_tags(tag_ids),
        }
    }

    /// Cluster logs/translogs already persist raw tags, so a post-freeze typed
    /// priority mirrored into `tags.priority` can be reconstructed without a
    /// durable-format change. Exactly one parseable raw value wins; ambiguous
    /// legacy tag sets retain the established TagDict behavior.
    fn cluster_rank_values(
        &self,
        raw_tags: &[(String, String)],
        tag_ids: &[TagId],
    ) -> crate::rank::RankValues {
        let mut priorities = raw_tags
            .iter()
            .filter(|(key, _)| key == "priority")
            .filter_map(|(_, value)| value.parse::<i64>().ok());
        match (priorities.next(), priorities.next()) {
            (Some(priority), None) => crate::rank::RankValues { priority },
            _ => self.legacy_rank_values(tag_ids),
        }
    }

    /// Resolve a query's raw `(key,value)` tags to a sorted + deduped `TagId` slice **read-only**
    /// against the engine's tag dict — the cluster-shard analogue of [`intern_tags`](Self::intern_tags)
    /// (ADR-055). Uses `get_or_synthetic` and NEVER `Arc::make_mut`, so the coordinator's frozen,
    /// shared `TagDict` is never forked: an interned tag keeps its dense id and a post-freeze tag
    /// resolves to a deterministic *synthetic* id every shard/coordinator agrees on (ADR-046) — the
    /// cross-shard consistency filtered percolation needs. Forking here would assign inconsistent
    /// dense ids per shard and silently mis-filter. Empty input ⇒ empty (the untagged path).
    fn resolve_tags_readonly(&self, tags: &[(String, String)]) -> Vec<TagId> {
        if tags.is_empty() {
            return Vec::new();
        }
        debug_assert!(
            self.tag_dict.is_finalized(),
            "cluster tag resolution must use the coordinator's finalized (frozen) shared tag dict"
        );
        let mut ids: Vec<TagId> = tags
            .iter()
            .map(|(k, v)| self.tag_dict.get_or_synthetic(k, v))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }
    /// Build the first BASE segment from a batch of `(logical_id, query_text)`.
    /// Two passes:
    ///   A: parse + extract + bump frequencies
    ///   (finalize the common mask)
    ///   B: choose signatures, classify, append to the base segment.
    /// Compile a batch into the first immutable base segment (the initial bulk
    /// load). Infallible convenience wrapper over [`try_build_from_queries`](Self::try_build_from_queries):
    /// in persistent mode a failure to durably write the segment or manifest is
    /// surfaced only via [`persistence_healthy`](Self::persistence_healthy) and
    /// an empty report. Callers that must distinguish a durable commit from a
    /// persistence failure should call [`try_build_from_queries`](Self::try_build_from_queries).
    pub fn build_from_queries(&mut self, queries: &[(u64, String)]) -> IngestReport {
        match self.try_build_from_queries(queries) {
            Ok(report) => report,
            Err(e) => {
                self.persistence_healthy = false;
                self.emit(crate::events::EngineEvent::DurabilityFailure {
                    op: crate::events::DurabilityOp::IngestRollback,
                    detail: "initial build_from_queries could not be durably committed; \
                             batch rolled back"
                        .to_string(),
                    error: e.to_string(),
                });
                IngestReport::default()
            }
        }
    }

    /// Compile a batch into the first immutable base segment, surfacing a
    /// persistence failure as an [`io::Error`](std::io::Error) instead of folding
    /// it into a degraded in-memory state. The batch is all-or-nothing: on a
    /// segment-write or manifest-write failure the in-memory segment is dropped,
    /// the orphan file is deleted, and nothing is committed (see ADR-017). Parse
    /// and cost-class-D rejections are non-fatal and counted in the returned
    /// [`IngestReport`].
    pub fn try_build_from_queries(
        &mut self,
        queries: &[(u64, String)],
    ) -> std::io::Result<IngestReport> {
        self.try_build_from_queries_with_tags(queries, &[])
    }

    /// [`try_build_from_queries`](Self::try_build_from_queries) carrying per-query
    /// metadata tags (ADR-049). `tags` is parallel to `queries` (`tags[i]` describes
    /// `queries[i]`); an empty slice means no query is tagged.
    pub fn try_build_from_queries_with_tags(
        &mut self,
        queries: &[(u64, String)],
        tags: &[Vec<(String, String)>],
    ) -> std::io::Result<IngestReport> {
        let mut report = IngestReport::default();
        let mut lc = String::new();
        // carry the original query index so we can pair each accepted query with its tags
        let mut extracted: Vec<(usize, u64, Extracted, &str)> = Vec::with_capacity(queries.len());
        let limits = self.config.parse_limits();

        // Pass A — intern features + bump frequencies. Take a single copy-on-write
        // handle to the dict for the whole pass (clones at most once if shared).
        {
            let dict = Arc::make_mut(&mut self.dict);
            for (idx, (logical, text)) in queries.iter().enumerate() {
                if let Ok(ast) = crate::dsl::parse_with_limits(text, &limits) {
                    let ex = extract(&ast, &self.norm, dict, &mut lc);
                    extracted.push((idx, *logical, ex, text));
                } else {
                    self.rejected_parse += 1;
                    report.rejected_parse += 1;
                }
            }
            // finalize the 64-bit common mask now that all frequencies are known
            dict.finalize_mask();
        }

        // ADR-054: if the build vocab declared equivalences, install them on the now-built
        // dict and expand the extracted queries so the INITIAL build applies them (mirrors
        // set_vocab + the cluster rebuild). Resolved against the populated dict so each form
        // maps to its real interned id; no equivalences ⇒ no-op (byte-identical).
        if let Some(v) = self.vocab.clone() {
            let equiv = v.resolve_equivalences(&self.norm, &self.dict);
            if !equiv.is_empty() {
                Arc::make_mut(&mut self.dict).set_equivalences(equiv);
                let map = self.dict.equivalences();
                for (_, _, ex, _) in &mut extracted {
                    ex.expand_equivalences(map);
                }
            }
        }

        // Intern each accepted query's tags (separate pass to avoid borrowing `self`
        // mutably while the dict is read in pass B). A query whose tag set exceeds
        // `max_tags` is rejected here (marked `None`) rather than truncated into the
        // u16 tag column — counted as a parse-level reject in pass B.
        let mut tag_ids: Vec<Option<Vec<TagId>>> = Vec::with_capacity(extracted.len());
        for (idx, _, _, _) in &extracted {
            let qtags = tags.get(*idx).map_or(&[][..], Vec::as_slice);
            if self.check_tag_limit(qtags).is_err() {
                tag_ids.push(None);
            } else {
                tag_ids.push(Some(self.intern_tags(qtags)));
            }
        }

        // Pass B -> first base segment. Accepted source text is collected and
        // applied to the query store only after the durable commit succeeds
        // (see commit_base_segment), so a failed batch leaves no partial sources.
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut accepted: Vec<AcceptedSource> = Vec::new();
        let knobs = self.config.compile_knobs();
        for (i, (idx, logical, ex, text)) in extracted.iter().enumerate() {
            let Some(qtag_ids) = &tag_ids[i] else {
                // Over-large tag set: rejected, never stored truncated.
                self.rejected_parse += 1;
                report.rejected_parse += 1;
                continue;
            };
            if Self::check_column_limit(ex).is_err() {
                // Column would overflow the u16 exact-store counts: rejected, never
                // stored truncated (silent false negative).
                self.rejected_parse += 1;
                report.rejected_parse += 1;
                continue;
            }
            let rank = self.legacy_rank_values(qtag_ids);
            let source_generation = self.allocate_source_generation();
            match seg.add_compiled_ranked_with_source_generation(
                ex,
                qtag_ids,
                &self.dict,
                *logical,
                1,
                rank,
                source_generation,
                knobs,
            ) {
                None => {
                    self.rejected_class_d += 1;
                    report.rejected_class_d += 1;
                }
                Some(added) => {
                    self.record_compiled(&added);
                    accepted.push(AcceptedSource::known(
                        *logical,
                        (*text).to_string(),
                        1,
                        source_generation,
                        tags.get(*idx).cloned().unwrap_or_default(),
                    ));
                    report.ingested += 1;
                }
            }
        }
        // Seal: build anchor filter before pushing as immutable base segment.
        seg.build_filter();
        self.commit_base_segment(seg, accepted, report)
    }
}

mod bulk;
mod delete;
mod extracted;
mod live;

#[cfg(test)]
mod tests;
