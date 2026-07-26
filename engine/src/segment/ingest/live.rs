use super::{extract, Arc, Engine, Extracted, InsertOutcome, UpsertOutcome};

impl Engine {
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
        if super::super::seg::rejects_class_d(class, &ex, self.config.accept_class_d) {
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
            self.refresh_phrase_capability();
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
        if super::super::seg::rejects_class_d(class, &ex, self.config.accept_class_d) {
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
        self.refresh_phrase_capability();
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
    /// recovery-parse-ceiling rule as [`replay_insert`](Self::replay_insert).
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
        if let Ok(ast) = crate::dsl::parse_for_recovery(text) {
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
}
