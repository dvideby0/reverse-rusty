//! `impl Engine` — the write path: initial build, live insert, tombstone/delete,
//! bulk ingest, and the WAL-replay helpers used by recovery (`open`).

use super::{
    AcceptedSource, Engine, IngestItemStatus, IngestReport, InsertOutcome, PlacedQuery, Segment,
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

    /// Live insert (hot delta -> memtable). New features get fresh ids; since
    /// their freq is low they are treated as non-hot (selective), which is
    /// correct. Returns the new memtable-local id (or None if class D).
    ///
    /// If the memtable grows beyond `config.memtable_flush_threshold`, an
    /// automatic flush is triggered (which may in turn trigger compaction if
    /// `auto_compact_on_flush` is set).
    pub fn insert_live(&mut self, text: &str, logical: u64, version: u32) -> Option<u32> {
        self.insert_live_with_tags(text, logical, version, &[])
    }

    /// [`insert_live`](Self::insert_live) carrying per-query metadata tags (ADR-049).
    pub fn insert_live_with_tags(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
    ) -> Option<u32> {
        match self.try_insert_live_with_tags(text, logical, version, tags) {
            Ok(InsertOutcome::Inserted(local)) => Some(local),
            Ok(InsertOutcome::RejectedClassD) => None,
            Err(crate::error::WriteError::Parse(_)) => {
                self.rejected_parse += 1;
                None
            }
            Err(crate::error::WriteError::Wal(e)) => {
                // The mutation was rejected (not applied). This infallible
                // convenience wrapper can only signal it by returning None;
                // callers that need to distinguish durability failures from
                // class-D/parse rejections must use `try_insert_live`.
                self.emit(crate::events::EngineEvent::DurabilityFailure {
                    op: crate::events::DurabilityOp::WalAppend,
                    detail: "WAL insert write failed; mutation rejected (not applied)".to_string(),
                    error: e.to_string(),
                });
                None
            }
        }
    }

    /// Live insert that surfaces failures as a typed [`WriteError`] instead of
    /// folding them into a silent `None`. Two failure modes: `Parse` (the query
    /// DSL was malformed — a caller error) and `Wal` (the mutation could not be
    /// durably logged). On success returns the outcome (inserted id, or class-D
    /// rejection). Class-D rejections are still counted toward
    /// `rejected_class_d()`; parse errors are the caller's to handle (and are
    /// NOT counted here, since they are returned).
    ///
    /// A `Wal` error means the write was *not* applied: the in-memory state is
    /// left untouched so it never diverges from the durable log. The caller must
    /// treat it as a failed write (the server returns HTTP 503), not success.
    pub fn try_insert_live(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
    ) -> Result<InsertOutcome, crate::error::WriteError> {
        self.try_insert_live_with_tags(text, logical, version, &[])
    }

    /// [`try_insert_live`](Self::try_insert_live) carrying per-query metadata tags
    /// (ADR-049). Tags ride the same WAL-first / fail-closed path as the query: they are
    /// logged before the in-memory apply, so a recovered insert keeps its tags.
    ///
    /// An accepted insert honors `config.memtable_flush_threshold` (ADR-073,
    /// closing ADR-064 item 5: the REST PUT path calls this directly, so the
    /// knob was inert for single-doc HTTP writes — WAL-durable, but memtable +
    /// WAL grew until a manual `/_flush`). The flush may invalidate the returned
    /// memtable-local id, exactly as on the infallible wrapper; address-stable
    /// callers key on the logical id.
    pub fn try_insert_live_with_tags(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
    ) -> Result<InsertOutcome, crate::error::WriteError> {
        // Interning happens in the shared implementation after validation; raw
        // legacy priority is re-derived from the resulting dense ids there.
        self.try_insert_live_ranked(text, logical, version, tags, None)
    }

    /// Typed-rank insert used by the v2 ingest surface. `None` preserves the
    /// permissive legacy tag behavior; `Some` stores the caller-validated fixed
    /// priority and appends it to WAL v6.
    pub fn try_insert_live_ranked(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
        rank: Option<crate::rank::RankValues>,
    ) -> Result<InsertOutcome, crate::error::WriteError> {
        // Parse first: a malformed query is a caller error and must never reach
        // the WAL (it carries no replayable mutation). Enforce the configured
        // complexity limits here, at the front door.
        let ast = crate::dsl::parse_with_limits(text, &self.config.parse_limits())
            .map_err(crate::error::WriteError::Parse)?;
        // Reject an over-large tag set at the front door too, before the WAL: it
        // would otherwise truncate the u16 tag column and silently drop a real tag.
        self.check_tag_limit(tags)
            .map_err(crate::error::WriteError::Parse)?;
        // Extract + class-gate BEFORE the WAL (ADR-068): the log records only
        // ACCEPTED mutations, so replay re-applies unconditionally — live ≡ replay
        // by construction even if the accept_class_d knob flips between runs.
        // (The dict mutation moving ahead of a possible WAL failure is benign:
        // a phantom interned feature / frequency bump is advisory state that
        // nothing references.)
        let mut lc = String::new();
        let ex = {
            let dict = Arc::make_mut(&mut self.dict);
            extract(&ast, &self.norm, dict, &mut lc)
        };
        // Reject a compiled query whose columns would overflow the u16 exact-store
        // counts BEFORE the WAL — a truncated store is a silent false negative.
        Self::check_column_limit(&ex).map_err(crate::error::WriteError::Parse)?;
        let class =
            crate::compile::anchor_plan(&ex, &self.dict, self.config.hot_anchor_threshold).class;
        if super::seg::rejects_class_d(class, &ex, self.config.accept_class_d) {
            self.rejected_class_d += 1;
            return Ok(InsertOutcome::RejectedClassD);
        }
        // Reserve the source generation before the WAL so that same value is
        // durable in the frame and installed in both the exact row and source
        // store. A failed append may leave a harmless gap; no visible state is
        // published.
        let source_generation = self.allocate_source_generation();
        // WAL (durability before visibility). If the append fails the mutation
        // is not durable, so reject it and leave in-memory state untouched
        // rather than acknowledge a write a crash would lose. Tags are logged
        // alongside the query so a replayed insert recovers them. An accepted
        // class-D insert uses its own op code (WAL v5, ADR-068) — the per-frame
        // marker that lets replay store it unconditionally while legacy frames
        // (logged before classification by pre-v5 binaries) keep the old gate.
        if let Some(ref mut wal) = self.wal {
            let appended = wal.append_insert_with_source_generation(
                logical,
                version,
                text,
                tags,
                rank.map(|values| values.priority),
                source_generation,
                class == crate::compile::CostClass::D,
            );
            if let Err(e) = appended {
                self.wal_healthy = false;
                return Err(crate::error::WriteError::Wal(e));
            }
        }
        let tag_ids = self.intern_tags(tags);
        let rank = rank.unwrap_or_else(|| self.legacy_rank_values(&tag_ids));
        let knobs = crate::segment::CompileKnobs {
            accept_class_d: true, // gated pre-WAL above (ADR-068)
            ..self.config.compile_knobs()
        };
        let outcome = Arc::make_mut(&mut self.memtable).add_compiled_ranked_with_source_generation(
            &ex,
            &tag_ids,
            &self.dict,
            logical,
            version,
            rank,
            source_generation,
            knobs,
        );
        if let Some(added) = outcome {
            self.record_compiled(&added);
            self.query_store.insert_document_with_generation(
                logical,
                text.to_string(),
                version,
                source_generation,
                tags,
            );
            self.maybe_flush();
            Ok(InsertOutcome::Inserted(added.local))
        } else {
            // Unreachable: the pre-WAL gate shares its predicate with
            // add_compiled, and the dict is unchanged in between. Kept as a
            // counted reject rather than a panic (no unwrap in library code).
            self.rejected_class_d += 1;
            Ok(InsertOutcome::RejectedClassD)
        }
    }

    /// Atomic upsert — ES `index` semantics, replace-by-id (ADR-067, closing the
    /// ADR-064 item-1 divergence): insert the new version of `logical` and
    /// tombstone every prior live copy, in one writer critical section backed by
    /// ONE WAL frame. Unlike a re-PUT through [`try_insert_live_with_tags`]
    /// (which leaves the old copy live and *matchable* until an explicit DELETE)
    /// or the DELETE-then-PUT recipe (whose two steps leave a no-match window —
    /// in the WAL too, where a crash between the frames recovered the deleted
    /// state without the insert), the upsert is all-or-nothing: a crash either
    /// recovers both halves or neither.
    ///
    /// Failure modes mirror [`try_insert_live_with_tags`]: `Parse` never reaches
    /// the WAL; `Wal` rejects the whole upsert (nothing applied, prior copies
    /// intact); a class-D rejection of the NEW version leaves the prior copies
    /// untouched (a failed replace never deletes — see [`UpsertOutcome`]).
    pub fn try_upsert_live(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
    ) -> Result<UpsertOutcome, crate::error::WriteError> {
        self.try_upsert_live_with_tags(text, logical, version, &[])
    }

    /// [`try_upsert_live`](Self::try_upsert_live) carrying per-query metadata tags
    /// (ADR-049). Tags ride the upsert WAL frame exactly as on the insert path.
    /// An accepted upsert honors `config.memtable_flush_threshold` exactly as
    /// [`try_insert_live_with_tags`](Self::try_insert_live_with_tags) does
    /// (ADR-073 — the REST PUT path calls this directly).
    pub fn try_upsert_live_with_tags(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
    ) -> Result<UpsertOutcome, crate::error::WriteError> {
        self.try_upsert_live_ranked(text, logical, version, tags, None)
    }

    /// Typed-rank atomic upsert; see [`try_insert_live_ranked`](Self::try_insert_live_ranked).
    pub fn try_upsert_live_ranked(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
        rank: Option<crate::rank::RankValues>,
    ) -> Result<UpsertOutcome, crate::error::WriteError> {
        // Parse first: a malformed query is a caller error and must never reach
        // the WAL — and must never tombstone the prior version.
        let ast = crate::dsl::parse_with_limits(text, &self.config.parse_limits())
            .map_err(crate::error::WriteError::Parse)?;
        // Reject an over-large tag set before the WAL too, for the same reason as on
        // insert — and so a failed replace never tombstones the prior version.
        self.check_tag_limit(tags)
            .map_err(crate::error::WriteError::Parse)?;
        // Extract + class-gate BEFORE the WAL (ADR-068): the log records only
        // ACCEPTED mutations, so replay re-applies unconditionally — live ≡
        // replay by construction even if the accept_class_d knob flips between
        // runs. A rejected new version leaves the prior copies untouched (a
        // failed replace never deletes) and writes no frame. Counted on the LIVE
        // path only (the manifest persists the counter; a replayed frame must
        // not re-increment it — codex).
        let mut lc = String::new();
        let ex = {
            let dict = Arc::make_mut(&mut self.dict);
            extract(&ast, &self.norm, dict, &mut lc)
        };
        // Reject a column-overflowing compiled query before the WAL too — and so a
        // failed replace never tombstones the prior version (same reason as tags).
        Self::check_column_limit(&ex).map_err(crate::error::WriteError::Parse)?;
        let class =
            crate::compile::anchor_plan(&ex, &self.dict, self.config.hot_anchor_threshold).class;
        if super::seg::rejects_class_d(class, &ex, self.config.accept_class_d) {
            self.rejected_class_d += 1;
            return Ok(UpsertOutcome::RejectedClassD);
        }
        // Reserve before the WAL so replay can reinstall this exact mutation
        // generation rather than minting a newer one after restart.
        let source_generation = self.allocate_source_generation();
        // WAL (durability before visibility) — one frame for both halves. An
        // accepted class-D upsert uses its own op code (WAL v5, ADR-068): replaying
        // a legacy logged-but-rejected op-4 frame as accepted would not just
        // resurrect the new version, it would tombstone the acknowledged-live prior
        // one — a false negative.
        if let Some(ref mut wal) = self.wal {
            let appended = wal.append_upsert_with_source_generation(
                logical,
                version,
                text,
                tags,
                rank.map(|values| values.priority),
                source_generation,
                class == crate::compile::CostClass::D,
            );
            if let Err(e) = appended {
                self.wal_healthy = false;
                return Err(crate::error::WriteError::Wal(e));
            }
        }
        let outcome = self.apply_upsert(
            &ex,
            text,
            logical,
            version,
            tags,
            rank,
            source_generation,
            true,
            true,
        );
        if matches!(
            outcome,
            UpsertOutcome::Created(_) | UpsertOutcome::Updated { .. }
        ) {
            self.maybe_flush();
        }
        Ok(outcome)
    }

    /// The shared apply funnel behind [`try_upsert_live_with_tags`](Self::try_upsert_live_with_tags)
    /// and its WAL replay: capture the prior live copies of `logical`, insert the
    /// new version, and — only if the insert was accepted — tombstone the
    /// captured copies and publish the new source text. The capture runs BEFORE
    /// the insert so the just-inserted copy is never tombstoned.
    ///
    /// `accept_class_d` reproduces the WRITER's class-D decision (ADR-068): the
    /// live path class-gates BEFORE logging, so it passes `true`; replay passes
    /// the frame's own marker — `true` for an op-6 `UpsertClassD` frame, `false`
    /// for a legacy op-4 frame, which a pre-v5 binary logged BEFORE classifying
    /// and may therefore have acknowledged as `RejectedClassD`. Replaying such a
    /// frame as accepted would tombstone the acknowledged-live prior version — a
    /// false negative. A rejected new version leaves the old copies live. No WAL
    /// involvement (the caller logged or is replaying).
    ///
    /// `source_generation` is reserved before the live WAL append and decoded
    /// verbatim on replay, coupling this exact row to the same source mutation.
    ///
    /// `tombstone_in_segments` separates the two state domains at replay
    /// (ADR-067): the MEMTABLE is WAL-truth — its prior copies are recreated by
    /// earlier replayed frames, so this funnel must always re-tombstone them —
    /// while the SEGMENTS are manifest-truth. A frame at/below the manifest's
    /// watermark passes `false`: its segment tombstones are already baked in the
    /// commit's bitmaps, and a same-id query bulk-ingested AFTER the frame (bulk
    /// bypasses the WAL, ADR-017) lives in those segments — tombstoning it would
    /// erase the newer query (the ADR-066 ordering inversion, upsert edition).
    /// The live path always passes `true`.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::segment) fn apply_upsert(
        &mut self,
        ex: &Extracted,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
        rank: Option<crate::rank::RankValues>,
        source_generation: u64,
        tombstone_in_segments: bool,
        accept_class_d: bool,
    ) -> UpsertOutcome {
        // Capture prior live copies: (segment index, local) with usize::MAX as
        // the memtable sentinel. Same reverse-index walk as the delete funnel.
        let mut prior: Vec<(usize, u32)> = Vec::new();
        if tombstone_in_segments {
            for (seg_idx, seg) in self.segments.iter().enumerate() {
                for &local in seg.locals_for_logical(logical) {
                    if seg.is_alive(local) {
                        prior.push((seg_idx, local));
                    }
                }
            }
        }
        for &local in self.memtable.locals_for_logical(logical) {
            if self
                .memtable
                .alive
                .get(local as usize)
                .copied()
                .unwrap_or(false)
            {
                prior.push((usize::MAX, local));
            }
        }

        let tag_ids = self.intern_tags(tags);
        let rank = rank.unwrap_or_else(|| self.legacy_rank_values(&tag_ids));
        let knobs = crate::segment::CompileKnobs {
            accept_class_d,
            ..self.config.compile_knobs()
        };
        let Some(added) = Arc::make_mut(&mut self.memtable)
            .add_compiled_ranked_with_source_generation(
                ex,
                &tag_ids,
                &self.dict,
                logical,
                version,
                rank,
                source_generation,
                knobs,
            )
        else {
            // The new version is class D and not marked accepted (a legacy op-4
            // frame on replay, or an effectively empty query): leave the prior
            // copies untouched — a failed replace must never delete (ES `index`
            // parity). NOT counted: rejection counters are live-path-only
            // (manifest-persisted — codex).
            return UpsertOutcome::RejectedClassD;
        };
        self.record_compiled(&added);
        let new_local = added.local;

        let replaced = prior.len();
        for (seg_idx, local) in prior {
            if seg_idx == usize::MAX {
                Arc::make_mut(&mut self.memtable).tombstone(local);
            } else if let Some(seg) = self.segments.get_mut(seg_idx) {
                Arc::make_mut(seg).tombstone(local);
            }
        }
        self.query_store.insert_document_with_generation(
            logical,
            text.to_string(),
            version,
            source_generation,
            tags,
        );
        if replaced == 0 {
            UpsertOutcome::Created(new_local)
        } else {
            UpsertOutcome::Updated {
                local: new_local,
                replaced,
            }
        }
    }

    /// Replay an upsert from WAL recovery (does NOT write back to WAL). Same
    /// default-parse-ceiling rule as [`replay_insert`](Self::replay_insert).
    /// `tombstone_in_segments` is `seq > wal_seq_watermark` at the dispatch site —
    /// see [`apply_upsert`](Self::apply_upsert) for the two state domains.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::segment) fn replay_upsert(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
        rank: Option<crate::rank::RankValues>,
        source_generation: Option<u64>,
        tombstone_in_segments: bool,
        class_d_accepted: bool,
    ) {
        if let Ok(ast) = crate::dsl::parse(text) {
            let mut lc = String::new();
            let ex = {
                let dict = Arc::make_mut(&mut self.dict);
                extract(&ast, &self.norm, dict, &mut lc)
            };
            let source_generation = self.replay_source_generation(source_generation);
            self.apply_upsert(
                &ex,
                text,
                logical,
                version,
                tags,
                rank,
                source_generation,
                tombstone_in_segments,
                class_d_accepted,
            );
        }
    }

    /// Tombstone a query version in the MEMTABLE (update = insert_live new +
    /// tombstone old). `local_id` is a memtable-local id (as returned by
    /// `insert_live`).
    ///
    /// Returns `Err` if the tombstone could not be durably logged; in that case
    /// the in-memory tombstone is not applied (the entry stays alive) so the
    /// memtable never diverges from the WAL.
    pub fn tombstone(&mut self, local_id: u32) -> std::io::Result<()> {
        // WAL: memtable tombstones use seg_idx = u32::MAX as sentinel
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_tombstone(u32::MAX, local_id) {
                self.wal_healthy = false;
                return Err(e);
            }
        }
        Arc::make_mut(&mut self.memtable).tombstone(local_id);
        Ok(())
    }

    /// Tombstone a query in a specific base segment (for callers that track
    /// (segment, local) addresses). `seg_idx` indexes `self.segments`.
    ///
    /// Returns `Err` (without applying the tombstone) if the WAL append fails.
    pub fn tombstone_in(&mut self, seg_idx: usize, local_id: u32) -> std::io::Result<()> {
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_tombstone(seg_idx as u32, local_id) {
                self.wal_healthy = false;
                return Err(e);
            }
        }
        if let Some(seg) = self.segments.get_mut(seg_idx) {
            Arc::make_mut(seg).tombstone(local_id);
        }
        Ok(())
    }

    /// Delete all live entries with a given logical ID across all segments
    /// and the memtable. Uses the per-segment reverse index for O(segments)
    /// lookup instead of O(total_entries) full scan. Returns the number of
    /// entries tombstoned.
    ///
    /// Durability (ADR-066): the delete is logged as ONE address-free
    /// `DeleteByLogical` WAL frame *before* anything is applied — all-or-nothing
    /// (a WAL failure rejects the whole delete; the server returns HTTP 503 and a
    /// retry is idempotent). The frame carries the logical id, not `(seg_idx,
    /// local)` addresses, so a later compaction that renumbers the address space
    /// can never make a crash-recovery replay tombstone an unrelated query.
    pub fn delete_by_logical_id(&mut self, logical_id: u64) -> std::io::Result<usize> {
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_delete_logical(logical_id) {
                self.wal_healthy = false;
                return Err(e);
            }
        }
        Ok(self.apply_delete_by_logical(logical_id))
    }

    /// The shared apply funnel behind [`delete_by_logical_id`](Self::delete_by_logical_id)
    /// and its WAL replay: tombstone every live copy of `logical_id` in the base
    /// segments and the memtable, then drop the source text. No WAL involvement —
    /// the caller has already logged (live path) or is replaying (recovery). Live
    /// and replay running the same funnel is what makes replay deterministic:
    /// at the frame's position in the log, the recovered live set is exactly the
    /// live set the original call saw.
    pub(in crate::segment) fn apply_delete_by_logical(&mut self, logical_id: u64) -> usize {
        let mut count = 0usize;
        for seg in &mut self.segments {
            let locals: Vec<u32> = seg
                .locals_for_logical(logical_id)
                .iter()
                .copied()
                .filter(|&local| seg.is_alive(local))
                .collect();
            for local in locals {
                Arc::make_mut(seg).tombstone(local);
                count += 1;
            }
        }

        let mem_locals: Vec<u32> = self
            .memtable
            .locals_for_logical(logical_id)
            .iter()
            .copied()
            .filter(|&local| {
                self.memtable
                    .alive
                    .get(local as usize)
                    .copied()
                    .unwrap_or(false)
            })
            .collect();
        for local in mem_locals {
            Arc::make_mut(&mut self.memtable).tombstone(local);
            count += 1;
        }

        if count > 0 {
            self.query_store.remove(logical_id);
        }
        count
    }

    /// Compile a batch DIRECTLY into a new immutable base segment and append it.
    /// Does not touch or rebuild any existing segment. Bumps global frequencies
    /// (so the shared dict stays accurate), but uses the already-finalized mask
    /// for signature selection (finalizing once if it was never done).
    pub fn bulk_ingest(&mut self, queries: &[(u64, String)]) -> IngestReport {
        match self.try_bulk_ingest(queries) {
            Ok(report) => report,
            Err(e) => {
                self.persistence_healthy = false;
                self.emit(crate::events::EngineEvent::DurabilityFailure {
                    op: crate::events::DurabilityOp::IngestRollback,
                    detail: "bulk_ingest could not be durably committed; batch rolled back"
                        .to_string(),
                    error: e.to_string(),
                });
                IngestReport::default()
            }
        }
    }

    /// Durable [`bulk_ingest`](Self::bulk_ingest): surfaces a persistence failure
    /// as an [`io::Error`](std::io::Error). Bulk ingest deliberately bypasses the
    /// WAL — the segment file is itself the durable artifact and the manifest
    /// update is the atomic commit point (the RocksDB `IngestExternalFile`
    /// pattern, ADR-017) — so there is no WAL backstop and a failed write must be
    /// reported, not silently degraded to an in-memory segment. All-or-nothing:
    /// on failure nothing is committed. Parse / cost-class-D rejections are
    /// non-fatal and counted in the returned [`IngestReport`].
    pub fn try_bulk_ingest(&mut self, queries: &[(u64, String)]) -> std::io::Result<IngestReport> {
        self.try_bulk_ingest_detailed(queries)
            .map(|(report, _)| report)
    }

    /// [`try_bulk_ingest`](Self::try_bulk_ingest) that additionally returns a
    /// per-item outcome for every input query, in submission order
    /// (`items[i]` describes `queries[i]`). The HTTP `/_bulk` handler uses this
    /// to report exactly which items were rejected and why — ES-style per-item
    /// status — instead of an aggregate count that leaves the caller unable to
    /// tell *which* queries were dropped. The returned [`IngestReport`] is the
    /// same aggregate as `try_bulk_ingest` and is consistent with the per-item
    /// vec (its counts equal the variant tallies). Durability semantics are
    /// identical (all-or-nothing, ADR-017); per-item statuses are only reported
    /// once the batch has durably committed.
    pub fn try_bulk_ingest_detailed(
        &mut self,
        queries: &[(u64, String)],
    ) -> std::io::Result<(IngestReport, Vec<IngestItemStatus>)> {
        self.try_bulk_ingest_detailed_with_tags(queries, &[])
    }

    /// [`try_bulk_ingest_detailed`](Self::try_bulk_ingest_detailed) carrying per-query
    /// metadata tags (ADR-049). `tags` is parallel to `queries` (`tags[i]` describes
    /// `queries[i]`); an empty slice means no query is tagged.
    pub fn try_bulk_ingest_detailed_with_tags(
        &mut self,
        queries: &[(u64, String)],
        tags: &[Vec<(String, String)>],
    ) -> std::io::Result<(IngestReport, Vec<IngestItemStatus>)> {
        self.try_bulk_ingest_detailed_with_tags_and_ranks(queries, tags, &[])
    }

    /// Bulk ingest with optional fixed typed rank values parallel to `queries`.
    /// An absent entry lowers permissive legacy `tags.priority`; a present value
    /// is stored verbatim after the HTTP layer has validated/mirrored it.
    pub fn try_bulk_ingest_detailed_with_tags_and_ranks(
        &mut self,
        queries: &[(u64, String)],
        tags: &[Vec<(String, String)>],
        ranks: &[Option<crate::rank::RankValues>],
    ) -> std::io::Result<(IngestReport, Vec<IngestItemStatus>)> {
        let mut report = IngestReport::default();
        let mut lc = String::new();
        let mut extracted: Vec<(usize, u64, Extracted, &str)> = Vec::with_capacity(queries.len());
        let mut item_status: Vec<IngestItemStatus> = Vec::with_capacity(queries.len());
        let limits = self.config.parse_limits();
        {
            let dict = Arc::make_mut(&mut self.dict);
            for (idx, (logical, text)) in queries.iter().enumerate() {
                match crate::dsl::parse_with_limits(text, &limits) {
                    Ok(ast) => {
                        let ex = extract(&ast, &self.norm, dict, &mut lc);
                        extracted.push((idx, *logical, ex, text));
                        // Provisional — Pass B may downgrade this to RejectedClassD.
                        item_status.push(IngestItemStatus::Ingested);
                    }
                    Err(e) => {
                        self.rejected_parse += 1;
                        report.rejected_parse += 1;
                        item_status.push(IngestItemStatus::RejectedParse(e));
                    }
                }
            }
            if !dict.is_finalized() {
                dict.finalize_mask();
            }
        }
        // Intern each accepted query's tags (separate pass so `self` is not borrowed
        // mutably while the dict is read in pass B). A query whose tag set exceeds
        // `max_tags` is rejected here (`None`) rather than truncated into the u16 tag
        // column — reported as a parse-level reject in pass B.
        let mut tag_ids: Vec<Option<Vec<TagId>>> = Vec::with_capacity(extracted.len());
        for (idx, _, _, _) in &extracted {
            let qtags = tags.get(*idx).map_or(&[][..], Vec::as_slice);
            match self.check_tag_limit(qtags) {
                Ok(()) => tag_ids.push(Some(self.intern_tags(qtags))),
                Err(_) => tag_ids.push(None),
            }
        }
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut accepted: Vec<AcceptedSource> = Vec::new();
        let knobs = self.config.compile_knobs();
        for (i, (idx, logical, ex, text)) in extracted.iter().enumerate() {
            let Some(qtag_ids) = &tag_ids[i] else {
                // Over-large tag set: rejected, never stored truncated.
                self.rejected_parse += 1;
                report.rejected_parse += 1;
                item_status[*idx] = IngestItemStatus::RejectedParse(crate::error::ParseError::new(
                    crate::error::ParseErrorKind::TooManyTags,
                    0,
                ));
                continue;
            };
            if Self::check_column_limit(ex).is_err() {
                // Column would overflow the u16 exact-store counts: rejected, never
                // stored truncated (silent false negative).
                self.rejected_parse += 1;
                report.rejected_parse += 1;
                item_status[*idx] = IngestItemStatus::RejectedParse(crate::error::ParseError::new(
                    crate::error::ParseErrorKind::CompiledColumnTooLarge,
                    0,
                ));
                continue;
            }
            let rank = ranks
                .get(*idx)
                .copied()
                .flatten()
                .unwrap_or_else(|| self.legacy_rank_values(qtag_ids));
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
                    item_status[*idx] = IngestItemStatus::RejectedClassD;
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
        let report = self.commit_base_segment(seg, accepted, report)?;
        if self.config.auto_compact_on_ingest {
            self.maybe_compact();
        }
        Ok((report, item_status))
    }

    /// Build a fresh immutable base segment from PRE-EXTRACTED queries, indexing
    /// against the engine's shared dict WITHOUT mutating it (no interning, no
    /// frequency bump, no mask re-finalize — `Segment::add_compiled` only *reads*
    /// the dict). This is the cluster shard's bulk path: every shard shares the
    /// coordinator's one frozen dict, so each query is indexed under exactly the
    /// `sig_key` the coordinator placed it on. `items` is
    /// `(logical_id, extracted, source_text, version)`; class-D queries follow
    /// the `accept_class_d` knob as on every other ingest path (the cluster
    /// coordinator rejects them at placement regardless — ADR-068 defers the
    /// cluster lane, so a knob here is fail-closed defense). In-memory only (the
    /// cluster step keeps shards non-durable); no WAL/manifest involvement.
    pub fn ingest_extracted(&mut self, items: &[PlacedQuery]) -> IngestReport {
        let mut report = IngestReport::default();
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut accepted: Vec<AcceptedSource> = Vec::new();
        for item in items {
            // Resolve the query's FRESH raw tags read-only against the shared frozen tag
            // space (ADR-055) — never the CoW `intern_tags`, which would fork the shared
            // dict per shard. Empty ⇒ empty slice ⇒ byte-identical to the pre-tag `&[]`
            // path.
            let resolved = self.resolve_tags_readonly(&item.tags);
            // Cap ONLY the fresh raw-tag ingestion (`item.tags`), NOT the carry-through.
            // `item.tag_ids` is ALREADY-STORED tags travelling through a resize / vocab
            // rebuild (ADR-074): those were accepted under the prior limit, and the rebuild
            // ignores this report and swaps in the new shards — so skipping them here would
            // PERMANENTLY drop acknowledged data (a false negative). Fresh raw tags, by
            // contrast, must be rejected rather than truncated into the u16 column. (The
            // cluster front door already caps fresh tags via `check_tag_limit`; this is the
            // defense for the build/bulk path that reaches here with raw tags directly.)
            if item.tag_ids.is_empty() && resolved.len() > self.config.max_tags {
                self.rejected_parse += 1;
                report.rejected_parse += 1;
                continue;
            }
            // Union the stored carry-through ids in, re-establishing the sorted/deduped
            // column invariant `resolve_tags_readonly` provides.
            let mut tag_ids = resolved;
            if !item.tag_ids.is_empty() {
                tag_ids.extend_from_slice(&item.tag_ids);
                tag_ids.sort_unstable();
                tag_ids.dedup();
            }
            // `tag_ids` is a public carry-through field, so its non-emptiness cannot by
            // itself be trusted as proof that the final set fits the exact store. The
            // runtime `max_tags` exception above preserves already-acknowledged rebuild
            // rows, but the structural u16 ceiling is absolute: crossing it would wrap
            // `tag_len` and let a valid filter miss the query.
            if tag_ids.len() > usize::from(u16::MAX) {
                self.rejected_parse += 1;
                report.rejected_parse += 1;
                continue;
            }
            let (source_tags, source_tags_known) = if !item.tags.is_empty() || tag_ids.is_empty() {
                (item.tags.clone(), true)
            } else {
                match tag_ids
                    .iter()
                    .map(|&id| {
                        self.tag_dict
                            .key_value(id)
                            .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    })
                    .collect::<Option<Vec<_>>>()
                {
                    Some(tags) => (tags, true),
                    None => (Vec::new(), false),
                }
            };
            let mut rank = item.rank;
            if rank.priority == 0 {
                rank = self.cluster_rank_values(&item.tags, &tag_ids);
            }
            let source_generation = self.allocate_source_generation();
            if let Some(added) = seg.add_compiled_ranked_placed_with_source_generation(
                &item.ex,
                &tag_ids,
                &self.dict,
                item.logical,
                item.version,
                rank,
                &item.placement,
                source_generation,
                self.config.compile_knobs(),
            ) {
                self.record_compiled(&added);
                accepted.push(AcceptedSource::with_tag_status(
                    item.logical,
                    item.dsl.clone(),
                    item.version,
                    source_generation,
                    source_tags,
                    source_tags_known,
                ));
                report.ingested += 1;
            } else {
                self.rejected_class_d += 1;
                report.rejected_class_d += 1;
            }
        }
        seg.build_filter();
        self.seal_and_push(seg);
        let accepted_any = !accepted.is_empty();
        for source in accepted {
            self.query_store.insert_document_with_generation_and_status(
                source.logical,
                source.text,
                source.version,
                source.source_generation,
                &source.tags,
                source.tags_known,
            );
        }
        // Bulk ingest has no WAL/translog backstop (mirroring `commit_base_segment`):
        // this is the sole point at which the bulk's source text becomes durable. A
        // segments-only cluster shard that skipped this would reopen with durable
        // segments but an EMPTY source store — and the vocabulary rebuild, which
        // gathers `live_sources`, would silently erase the bulk-loaded corpus
        // (ADR-074). In-memory engines no-op (no data_dir); a write failure degrades
        // `persistence_healthy` via the DurabilityFailure event path.
        if accepted_any {
            self.save_query_sources();
        }
        report
    }

    /// Insert ONE pre-extracted query into the memtable without mutating the
    /// shared dict — the live-update analog of [`ingest_extracted`](Self::ingest_extracted),
    /// used by the cluster's incremental `add_query`. Returns the new
    /// memtable-local id, or `None` for a class-D rejection.
    pub fn insert_extracted(
        &mut self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Option<u32> {
        self.insert_extracted_with_placement(
            ex,
            logical,
            version,
            text,
            tags,
            &crate::ownership::QueryPlacement::standalone(),
        )
    }

    /// Cluster write path carrying ADR-109 placement metadata into the memtable.
    pub fn insert_extracted_with_placement(
        &mut self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Option<u32> {
        // Resolve tags read-only against the shared frozen tag space (ADR-055); never the CoW
        // `intern_tags`. Empty ⇒ empty slice ⇒ byte-identical to the pre-tag `&[]` path.
        let tag_ids = self.resolve_tags_readonly(tags);
        let rank = self.cluster_rank_values(tags, &tag_ids);
        let source_generation = self.allocate_source_generation();
        let outcome = Arc::make_mut(&mut self.memtable)
            .add_compiled_ranked_placed_with_source_generation(
                ex,
                &tag_ids,
                &self.dict,
                logical,
                version,
                rank,
                placement,
                source_generation,
                self.config.compile_knobs(),
            );
        if let Some(added) = outcome {
            self.record_compiled(&added);
            self.query_store.insert_document_with_generation(
                logical,
                text.to_string(),
                version,
                source_generation,
                tags,
            );
            Some(added.local)
        } else {
            self.rejected_class_d += 1;
            None
        }
    }

    /// Replay an insert from WAL recovery (does NOT write back to WAL).
    ///
    /// Replay uses the default (compiled-in) parse ceiling, NOT the configured
    /// `parse_limits()`: a WAL entry was already accepted at its front-door write,
    /// so re-applying a (possibly since-tightened) limit here could silently drop
    /// an already-acknowledged write and diverge the recovered state from the log.
    /// The compiled-in ceiling still bounds resource use during replay.
    ///
    /// `class_d_accepted` is the frame's own marker (WAL v5, ADR-068), NOT the
    /// engine's knob: an op-5 frame was accepted at its write (the live path gates
    /// BEFORE logging) and replays stored even if the knob has since flipped off;
    /// a legacy op-0 frame replays under the old reject gate, because a pre-v5
    /// binary logged BEFORE classifying and may have acknowledged the write as
    /// `RejectedClassD`.
    // These arguments intentionally mirror the decoded insert-shaped WAL frame.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::segment) fn replay_insert(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
        rank: Option<crate::rank::RankValues>,
        source_generation: Option<u64>,
        class_d_accepted: bool,
    ) {
        if let Ok(ast) = crate::dsl::parse(text) {
            let tag_ids = self.intern_tags(tags);
            let rank = rank.unwrap_or_else(|| self.legacy_rank_values(&tag_ids));
            let mut lc = String::new();
            let ex = {
                let dict = Arc::make_mut(&mut self.dict);
                extract(&ast, &self.norm, dict, &mut lc)
            };
            let knobs = crate::segment::CompileKnobs {
                accept_class_d: class_d_accepted,
                ..self.config.compile_knobs()
            };
            let source_generation = self.replay_source_generation(source_generation);
            if let Some(added) = Arc::make_mut(&mut self.memtable)
                .add_compiled_ranked_with_source_generation(
                    &ex,
                    &tag_ids,
                    &self.dict,
                    logical,
                    version,
                    rank,
                    source_generation,
                    knobs,
                )
            {
                self.record_compiled(&added);
                self.query_store.insert_document_with_generation(
                    logical,
                    text.to_string(),
                    version,
                    source_generation,
                    tags,
                );
            }
        }
    }

    /// Replay a tombstone from WAL recovery.
    pub(in crate::segment) fn replay_tombstone(&mut self, seg_idx: u32, local_id: u32) {
        if seg_idx == u32::MAX {
            Arc::make_mut(&mut self.memtable).tombstone(local_id);
        } else if let Some(seg) = self.segments.get_mut(seg_idx as usize) {
            Arc::make_mut(seg).tombstone(local_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracted_ingest_rejects_a_merged_tag_column_over_u16() {
        let mut engine =
            Engine::new(crate::normalize::Normalizer::default_vocab().expect("normalizer"));
        let seed = vec![(1, "1994 upper deck".to_string())];
        assert_eq!(engine.build_from_queries(&seed).ingested, 1);

        let ast = crate::dsl::parse("1994 upper deck").expect("parse");
        let mut lc = String::new();
        let ex = crate::compile::extract_readonly(&ast, &engine.norm, &engine.dict, &mut lc);
        let item = PlacedQuery {
            logical: 2,
            ex,
            dsl: "1994 upper deck".into(),
            version: 1,
            tags: Vec::new(),
            // Nonempty carry-through bypasses the runtime max_tags check, but
            // the exact-store u16 count ceiling remains unconditional.
            tag_ids: (0..=u32::from(u16::MAX)).collect(),
            rank: crate::rank::RankValues::default(),
            placement: crate::ownership::QueryPlacement::standalone(),
        };
        let report = engine.ingest_extracted(&[item]);
        assert_eq!(report.ingested, 0);
        assert_eq!(report.rejected_parse, 1);
        assert!(
            !engine.snapshot().has_live_query(2),
            "a wrapping tag column must never reach the exact store"
        );
    }
}
