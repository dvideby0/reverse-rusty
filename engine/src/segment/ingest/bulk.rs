use super::{
    extract, AcceptedSource, Arc, Engine, Extracted, IngestItemStatus, IngestReport, Segment, TagId,
};

impl Engine {
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
}
