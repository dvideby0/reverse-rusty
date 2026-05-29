//! `impl Engine` — the write path: initial build, live insert, tombstone/delete,
//! bulk ingest, and the WAL-replay helpers used by recovery (`open`).

use super::{Engine, IngestItemStatus, IngestReport, InsertOutcome, Segment};
use std::sync::Arc;

use crate::compile::{extract, Extracted};

impl Engine {
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
        let mut report = IngestReport::default();
        let mut lc = String::new();
        let mut extracted: Vec<(u64, Extracted, &str)> = Vec::with_capacity(queries.len());

        // Pass A — intern features + bump frequencies. Take a single copy-on-write
        // handle to the dict for the whole pass (clones at most once if shared).
        {
            let dict = Arc::make_mut(&mut self.dict);
            for (logical, text) in queries {
                if let Ok(ast) = crate::dsl::parse(text) {
                    let ex = extract(&ast, &self.norm, dict, &mut lc);
                    extracted.push((*logical, ex, text));
                } else {
                    self.rejected_parse += 1;
                    report.rejected_parse += 1;
                }
            }
            // finalize the 64-bit common mask now that all frequencies are known
            dict.finalize_mask();
        }

        // Pass B -> first base segment. Accepted source text is collected and
        // applied to the query store only after the durable commit succeeds
        // (see commit_base_segment), so a failed batch leaves no partial sources.
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut accepted: Vec<(u64, String)> = Vec::new();
        for (logical, ex, text) in &extracted {
            if seg.add_compiled(ex, &self.dict, *logical, 1).is_none() {
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
        match self.try_insert_live(text, logical, version) {
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
        // Parse first: a malformed query is a caller error and must never reach
        // the WAL (it carries no replayable mutation).
        let ast = crate::dsl::parse(text).map_err(crate::error::WriteError::Parse)?;
        // WAL FIRST (durability before visibility). If the append fails the
        // mutation is not durable, so reject it and leave in-memory state
        // untouched rather than acknowledge a write a crash would lose.
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_insert(logical, version, text) {
                self.wal_healthy = false;
                return Err(crate::error::WriteError::Wal(e));
            }
        }
        let mut lc = String::new();
        let ex = {
            let dict = Arc::make_mut(&mut self.dict);
            extract(&ast, &self.norm, dict, &mut lc)
        };
        let outcome =
            Arc::make_mut(&mut self.memtable).add_compiled(&ex, &self.dict, logical, version);
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
        let mut report = IngestReport::default();
        let mut lc = String::new();
        let mut extracted: Vec<(usize, u64, Extracted, &str)> = Vec::with_capacity(queries.len());
        let mut item_status: Vec<IngestItemStatus> = Vec::with_capacity(queries.len());
        {
            let dict = Arc::make_mut(&mut self.dict);
            for (idx, (logical, text)) in queries.iter().enumerate() {
                match crate::dsl::parse(text) {
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
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut accepted: Vec<(u64, String)> = Vec::new();
        for (idx, logical, ex, text) in &extracted {
            if seg.add_compiled(ex, &self.dict, *logical, 1).is_none() {
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

    /// Replay an insert from WAL recovery (does NOT write back to WAL).
    pub(in crate::segment) fn replay_insert(&mut self, text: &str, logical: u64, version: u32) {
        if let Ok(ast) = crate::dsl::parse(text) {
            let mut lc = String::new();
            let ex = {
                let dict = Arc::make_mut(&mut self.dict);
                extract(&ast, &self.norm, dict, &mut lc)
            };
            if Arc::make_mut(&mut self.memtable)
                .add_compiled(&ex, &self.dict, logical, version)
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
