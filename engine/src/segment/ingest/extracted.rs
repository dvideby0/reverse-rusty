use super::{extract, AcceptedSource, Arc, Engine, Extracted, IngestReport, PlacedQuery, Segment};

impl Engine {
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
            let source_generation = match item.source_generation {
                Some(source_generation) => self.replay_source_generation(Some(source_generation)),
                None => self.allocate_source_generation(),
            };
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
            self.refresh_phrase_capability();
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
    /// Replay uses the durable format's structural parse ceiling, NOT the
    /// configured `parse_limits()` or today's defaults: a WAL entry was already
    /// accepted at its front-door write, so re-applying a possibly tightened (or
    /// originally looser) policy here could silently drop an acknowledged write
    /// and diverge recovered state from the log.
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
        if let Ok(ast) = crate::dsl::parse_for_recovery(text) {
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
                self.refresh_phrase_capability();
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
        self.refresh_phrase_capability();
    }
}
