//! `impl Engine` — the write path: initial build, live insert, tombstone/delete,
//! bulk ingest, and the WAL-replay helpers used by recovery (`open`).

use super::{Engine, IngestItemStatus, IngestReport, InsertOutcome, PlacedQuery, Segment};
use std::sync::Arc;

use crate::compile::{extract, Extracted};
use crate::tagdict::TagId;

impl Engine {
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
        // mutably while the dict is read in pass B).
        let mut tag_ids: Vec<Vec<TagId>> = Vec::with_capacity(extracted.len());
        for (idx, _, _, _) in &extracted {
            let qtags = tags.get(*idx).map_or(&[][..], Vec::as_slice);
            tag_ids.push(self.intern_tags(qtags));
        }

        // Pass B -> first base segment. Accepted source text is collected and
        // applied to the query store only after the durable commit succeeds
        // (see commit_base_segment), so a failed batch leaves no partial sources.
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut accepted: Vec<(u64, String)> = Vec::new();
        for (i, (_, logical, ex, text)) in extracted.iter().enumerate() {
            if seg
                .add_compiled(ex, &tag_ids[i], &self.dict, *logical, 1)
                .is_none()
            {
                self.rejected_class_d += 1;
                report.rejected_class_d += 1;
            } else {
                accepted.push((*logical, (*text).to_string()));
                report.ingested += 1;
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
            Ok(InsertOutcome::Inserted(local)) => {
                self.maybe_flush();
                Some(local)
            }
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
    pub fn try_insert_live_with_tags(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
    ) -> Result<InsertOutcome, crate::error::WriteError> {
        // Parse first: a malformed query is a caller error and must never reach
        // the WAL (it carries no replayable mutation). Enforce the configured
        // complexity limits here, at the front door.
        let ast = crate::dsl::parse_with_limits(text, &self.config.parse_limits())
            .map_err(crate::error::WriteError::Parse)?;
        // WAL FIRST (durability before visibility). If the append fails the
        // mutation is not durable, so reject it and leave in-memory state
        // untouched rather than acknowledge a write a crash would lose. Tags are
        // logged alongside the query so a replayed insert recovers them.
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_insert(logical, version, text, tags) {
                self.wal_healthy = false;
                return Err(crate::error::WriteError::Wal(e));
            }
        }
        let tag_ids = self.intern_tags(tags);
        let mut lc = String::new();
        let ex = {
            let dict = Arc::make_mut(&mut self.dict);
            extract(&ast, &self.norm, dict, &mut lc)
        };
        let outcome = Arc::make_mut(&mut self.memtable)
            .add_compiled(&ex, &tag_ids, &self.dict, logical, version);
        if let Some(local) = outcome {
            self.query_store.insert(logical, text.to_string());
            Ok(InsertOutcome::Inserted(local))
        } else {
            self.rejected_class_d += 1;
            Ok(InsertOutcome::RejectedClassD)
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
    /// Each tombstone is WAL-logged before it is applied. If a WAL append
    /// fails, the delete stops and returns `Err`: the tombstones already
    /// applied are durably logged (and replay correctly), and the failed one
    /// is not applied. A retried delete is idempotent, so the caller can treat
    /// the `Err` as "try again" (the server returns HTTP 503).
    pub fn delete_by_logical_id(&mut self, logical_id: u64) -> std::io::Result<usize> {
        let mut count = 0usize;

        for (seg_idx, seg) in self.segments.iter_mut().enumerate() {
            let locals: Vec<u32> = seg
                .locals_for_logical(logical_id)
                .iter()
                .copied()
                .filter(|&local| seg.is_alive(local))
                .collect();
            for local in locals {
                if let Some(ref mut wal) = self.wal {
                    if let Err(e) = wal.append_tombstone(seg_idx as u32, local) {
                        self.wal_healthy = false;
                        return Err(e);
                    }
                }
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
            if let Some(ref mut wal) = self.wal {
                if let Err(e) = wal.append_tombstone(u32::MAX, local) {
                    self.wal_healthy = false;
                    return Err(e);
                }
            }
            Arc::make_mut(&mut self.memtable).tombstone(local);
            count += 1;
        }

        if count > 0 {
            self.query_store.remove(logical_id);
        }
        Ok(count)
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
        // mutably while the dict is read in pass B).
        let mut tag_ids: Vec<Vec<TagId>> = Vec::with_capacity(extracted.len());
        for (idx, _, _, _) in &extracted {
            let qtags = tags.get(*idx).map_or(&[][..], Vec::as_slice);
            tag_ids.push(self.intern_tags(qtags));
        }
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut accepted: Vec<(u64, String)> = Vec::new();
        for (i, (idx, logical, ex, text)) in extracted.iter().enumerate() {
            if seg
                .add_compiled(ex, &tag_ids[i], &self.dict, *logical, 1)
                .is_none()
            {
                self.rejected_class_d += 1;
                report.rejected_class_d += 1;
                item_status[*idx] = IngestItemStatus::RejectedClassD;
            } else {
                accepted.push((*logical, (*text).to_string()));
                report.ingested += 1;
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
    /// `(logical_id, extracted, source_text, version)`; class-D queries are
    /// skipped, as on every other ingest path. In-memory only (the cluster step
    /// keeps shards non-durable); no WAL/manifest involvement.
    pub fn ingest_extracted(&mut self, items: &[PlacedQuery]) -> IngestReport {
        let mut report = IngestReport::default();
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut accepted: Vec<(u64, String)> = Vec::new();
        for item in items {
            // Resolve the query's tags read-only against the shared frozen tag space (ADR-055) —
            // never the CoW `intern_tags`, which would fork the shared dict per shard. Empty ⇒
            // empty slice ⇒ byte-identical to the pre-tag `&[]` path.
            let tag_ids = self.resolve_tags_readonly(&item.tags);
            if seg
                .add_compiled(&item.ex, &tag_ids, &self.dict, item.logical, item.version)
                .is_some()
            {
                accepted.push((item.logical, item.dsl.clone()));
                report.ingested += 1;
            } else {
                self.rejected_class_d += 1;
                report.rejected_class_d += 1;
            }
        }
        seg.build_filter();
        self.seal_and_push(seg);
        for (logical, text) in accepted {
            self.query_store.insert(logical, text);
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
        // Resolve tags read-only against the shared frozen tag space (ADR-055); never the CoW
        // `intern_tags`. Empty ⇒ empty slice ⇒ byte-identical to the pre-tag `&[]` path.
        let tag_ids = self.resolve_tags_readonly(tags);
        let outcome = Arc::make_mut(&mut self.memtable)
            .add_compiled(ex, &tag_ids, &self.dict, logical, version);
        if outcome.is_some() {
            self.query_store.insert(logical, text.to_string());
        } else {
            self.rejected_class_d += 1;
        }
        outcome
    }

    /// Replay an insert from WAL recovery (does NOT write back to WAL).
    ///
    /// Replay uses the default (compiled-in) parse ceiling, NOT the configured
    /// `parse_limits()`: a WAL entry was already accepted at its front-door write,
    /// so re-applying a (possibly since-tightened) limit here could silently drop
    /// an already-acknowledged write and diverge the recovered state from the log.
    /// The compiled-in ceiling still bounds resource use during replay.
    pub(in crate::segment) fn replay_insert(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
        tags: &[(String, String)],
    ) {
        if let Ok(ast) = crate::dsl::parse(text) {
            let tag_ids = self.intern_tags(tags);
            let mut lc = String::new();
            let ex = {
                let dict = Arc::make_mut(&mut self.dict);
                extract(&ast, &self.norm, dict, &mut lc)
            };
            if Arc::make_mut(&mut self.memtable)
                .add_compiled(&ex, &tag_ids, &self.dict, logical, version)
                .is_some()
            {
                self.query_store.insert(logical, text.to_string());
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
