//! `impl Engine` — flush (seal the memtable) and the LSM compaction machinery:
//! the score-based merge selector, the explicit/policy compaction entry points,
//! and the auto-flush trigger.

use super::{BaseSegment, CompactionReport, Engine, Segment};
use std::path::PathBuf;
use std::sync::Arc;

/// Materialize an `Arc<BaseSegment>` into an owned in-memory `Segment` (for
/// compaction). Unwraps the `Arc` in place when uniquely held; otherwise — when
/// a published snapshot still references the segment — clones it out, leaving
/// that snapshot's view intact.
fn arc_into_memory(seg: Arc<BaseSegment>) -> Segment {
    Arc::try_unwrap(seg)
        .unwrap_or_else(|a| (*a).clone())
        .into_memory()
}

impl Engine {
    /// Seal the current memtable into an immutable base segment and start a
    /// fresh (empty) memtable. If `auto_compact_on_flush` is enabled in the
    /// config, runs `maybe_compact` after the flush.
    pub fn flush(&mut self) {
        if self.memtable.is_empty() {
            return;
        }
        let entries = self.memtable.len();
        let flush_start = std::time::Instant::now();
        let fresh = Arc::new({
            let mut s = Segment::new();
            s.vocab_epoch = self.vocab_epoch;
            s
        });
        let sealed_arc = std::mem::replace(&mut self.memtable, fresh);
        // Take ownership of the sealed memtable. If a snapshot still references
        // it (the common case — we publish after every write), clone it out;
        // that snapshot keeps its pre-flush view, which is correct.
        let mut sealed = Arc::try_unwrap(sealed_arc).unwrap_or_else(|a| (*a).clone());
        sealed.build_filter();
        // Seal the memtable into a base segment. `persisted` is false only when
        // persistent-mode disk write failed and the segment fell back to in-memory.
        let persisted = self.seal_and_push(sealed);
        self.emit(crate::events::EngineEvent::Flush {
            entries,
            base_segments_after: self.segments.len(),
            duration_secs: flush_start.elapsed().as_secs_f64(),
        });
        // Fail closed (ADR-051): only advance the WAL once the flushed segment is
        // durably on disk AND the manifest — the commit point that references it —
        // has been written. If either step fails, the just-flushed queries live only
        // in the in-memory segment, but every memtable mutation is still in the WAL;
        // leaving the WAL intact lets a restart replay them rather than silently
        // losing acknowledged writes. The checkpoint is written *after* the manifest
        // (not before, as it once was), so a manifest failure can never strand a
        // checkpoint marker that would make recovery skip not-yet-referenced entries.
        self.save_query_sources();
        if persisted && self.save_manifest_if_persistent() {
            self.checkpoint_wal();
            self.reset_wal_if_safe();
        }
        if self.config.auto_compact_on_flush {
            self.maybe_compact();
        }
    }

    /// Compact base segments: merge them into fewer segments to reduce read
    /// amplification. Drops tombstoned entries, reclaims space, renumbers to
    /// dense local IDs. The memtable is NOT touched (it stays as the mutable
    /// hot delta).
    ///
    /// **Policy (ClickHouse-inspired score-based greedy selector):**
    /// Evaluates every contiguous range of ≥2 base segments and picks the one
    /// with the lowest score = `(sum_size + FIXED_COST * count) / (count - 1.9)`.
    /// This minimizes time-integrated average segment count — exactly the right
    /// objective when reads must probe every segment (as in ClickHouse and
    /// Reverse Rusty). `max_segments` is the threshold: if the current base segment
    /// count is ≤ max_segments, no compaction runs.
    ///
    /// Correctness: the merged segment contains exactly the alive entries from
    /// all sources with their exact-match data and signature postings preserved.
    /// The oracle test (`tests/oracle.rs`) verifies this end-to-end.
    pub fn compact(&mut self, max_segments: usize) -> Option<CompactionReport> {
        if self.segments.len() <= max_segments {
            return None;
        }
        // Score-based: find the best contiguous range to merge.
        let (lo, hi) = self.pick_merge_range();
        self.compact_range(lo, hi)
    }

    /// Score-based merge range selection (ClickHouse SimpleMergeSelector style).
    /// Evaluates all contiguous ranges of ≥2 segments. Score formula:
    ///   `(sum_size + FIXED_COST * count) / (count - 1.9)`
    /// Lower score = better merge (cheapest way to reduce segment count).
    /// The FIXED_COST biases toward merging small segments first (cheap wins).
    fn pick_merge_range(&self) -> (usize, usize) {
        let fixed_cost = self.config.compaction_fixed_cost;
        let n = self.segments.len();
        let sizes: Vec<f64> = self.segments.iter().map(|s| s.len() as f64).collect();

        let mut best_score = f64::MAX;
        let mut best_lo = 0usize;
        let mut best_hi = n; // fallback: merge everything

        for lo in 0..n {
            let mut sum = sizes[lo];
            for hi in (lo + 2)..=n {
                sum += sizes[hi - 1];
                let count = (hi - lo) as f64;
                let score = (sum + fixed_cost * count) / (count - 1.9);
                if score < best_score {
                    best_score = score;
                    best_lo = lo;
                    best_hi = hi;
                }
            }
        }
        (best_lo, best_hi)
    }

    /// Unconditionally merge ALL base segments into one. Returns a report if
    /// there was anything to merge (i.e. more than one base segment existed), or
    /// `None` if there was nothing to merge OR the merge could not be durably
    /// committed (in which case it was rolled back — see [`Self::do_compact_range`]).
    pub fn compact_all(&mut self) -> Option<CompactionReport> {
        self.do_compact_range(
            0,
            self.segments.len(),
            crate::events::CompactionTrigger::ExplicitAll,
        )
    }

    /// Re-seal every base segment that carries tombstones, dropping the dead entries so
    /// the ON-DISK `.seg` reflects all applied deletes. The cluster checkpoint calls this
    /// (ADR-032): a [`MmapSegment::tombstone`](crate::storage::MmapSegment::tombstone)
    /// mutates only the in-RAM alive overlay, so without re-sealing, a `Remove` against a
    /// base segment would be lost once the log tail carrying it is truncated — the deleted
    /// query would resurrect on reopen (a false positive). No-op when every segment is
    /// already clean. Unlike [`compact_all`](Self::compact_all) it re-seals a lone dirty
    /// segment too (it does not require ≥2 segments) and re-seals each dirty segment in
    /// place (it does not merge clean ones), so it stays O(tombstoned data), not O(corpus).
    pub fn reseal_tombstoned_segments(&mut self) {
        if self.segments.iter().all(|s| s.alive_count() == s.len()) {
            return; // nothing tombstoned — fast common case
        }
        let mut new_segments = Vec::with_capacity(self.segments.len());
        let mut old_files = Vec::new();
        for arc in std::mem::take(&mut self.segments) {
            if arc.alive_count() == arc.len() {
                new_segments.push(arc); // already clean — keep as-is (no rewrite)
                continue;
            }
            let old_path = if let BaseSegment::Mmap(m) = arc.as_ref() {
                Some(m.path().to_path_buf())
            } else {
                None
            };
            // Clone the alive entries out for the reseal WITHOUT consuming the
            // original (Arc::clone keeps it live), so a failed write can keep
            // serving — and keep on disk — the original segment.
            let seg = arc_into_memory(Arc::clone(&arc));
            let clean = Segment::compact_from(&[&seg]); // copies only alive entries
                                                        // An all-tombstoned segment compacts to empty — drop it rather than writing
                                                        // an empty `.seg` (and let its old file be cleaned up below).
            if clean.is_empty() {
                if let Some(p) = old_path {
                    old_files.push(p);
                }
                continue;
            }
            // Build the resealed segment durably BEFORE retiring the original
            // (ADR-051: build durable, then destroy).
            match self.build_durable_base(clean) {
                Ok((base, _path)) => {
                    new_segments.push(Arc::new(base));
                    if let Some(p) = old_path {
                        old_files.push(p); // retire the original only after a durable reseal
                    }
                }
                Err(e) => {
                    // Reseal failed: keep the ORIGINAL segment — its deletes stay in
                    // the in-RAM liveness overlay and its `.seg` is unchanged and
                    // still valid, so nothing is lost. Its file is NOT retired.
                    // `persistence_healthy` is now false; the cluster checkpoint that
                    // calls us must not trim the translog past these still-un-baked
                    // tombstones (see `seal_for_checkpoint_at`), else the delete would
                    // resurrect on reopen.
                    self.emit(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::Compaction,
                        detail: "reseal of a tombstoned segment failed; kept the original \
                                 (its deletes remain in the in-RAM overlay / translog)"
                            .to_string(),
                        error: e.to_string(),
                    });
                    new_segments.push(arc); // original retained, file kept
                }
            }
        }
        self.segments = new_segments;
        // Manifest is the commit point: only after it succeeds is it safe to delete
        // the retired files. On a manifest failure the old files stay (still
        // referenced by the on-disk manifest) and the freshly resealed files become
        // orphans GC'd on reopen — fail closed, no resurrection.
        if self.save_manifest_if_persistent() {
            self.cleanup_segment_files(&old_files);
        }
    }

    /// Merge a specific range of base segments `[lo..hi)` into one, replacing
    /// them in the segments vec. Useful for leveled/tiered policies that pick
    /// adjacent pairs. Returns a report if the merge happened, or `None` if the
    /// range was degenerate OR the merge could not be durably committed (rolled
    /// back — see [`Self::do_compact_range`]).
    pub fn compact_range(&mut self, lo: usize, hi: usize) -> Option<CompactionReport> {
        self.do_compact_range(
            lo,
            hi,
            crate::events::CompactionTrigger::ExplicitRange { lo, hi },
        )
    }

    /// Check the compaction policy and run a merge if any threshold is exceeded.
    ///
    /// Two triggers are checked in order:
    /// 1. **Holes ratio** — if any base segment's tombstone fraction exceeds
    ///    `config.holes_ratio_threshold`, pick the best merge range containing
    ///    that segment and compact it.
    /// 2. **Segment count** — if the base segment count exceeds
    ///    `config.max_segments`, pick the best merge range and compact it.
    ///
    /// Returns the compaction report if a merge happened, `None` otherwise.
    pub fn maybe_compact(&mut self) -> Option<CompactionReport> {
        // Check holes ratio first — tombstone-heavy segments need reclamation
        // regardless of segment count.
        let holes_threshold = self.config.holes_ratio_threshold;
        if holes_threshold < 1.0 {
            for i in 0..self.segments.len() {
                if self.segments[i].holes_ratio() > holes_threshold {
                    // Found a segment with excessive tombstones. Use the
                    // score-based picker to find the best range to merge.
                    let (lo, hi) = self.pick_merge_range();
                    return self.do_compact_range(
                        lo,
                        hi,
                        crate::events::CompactionTrigger::HolesRatio {
                            segment_idx: i,
                            ratio: self.segments[i].holes_ratio(),
                        },
                    );
                }
            }
        }

        // Check segment count
        if self.segments.len() > self.config.max_segments {
            let (lo, hi) = self.pick_merge_range();
            return self.do_compact_range(
                lo,
                hi,
                crate::events::CompactionTrigger::SegmentCount {
                    count: self.segments.len(),
                },
            );
        }

        None
    }

    /// Compact the base-segment range `[lo..hi)` into one, emitting the given
    /// trigger reason. The single implementation behind [`compact_all`](Self::compact_all),
    /// [`compact_range`](Self::compact_range), and the auto-policy in
    /// [`maybe_compact`](Self::maybe_compact).
    ///
    /// **Durability (ADR-051): build durable, THEN destroy.** The merge first
    /// writes the new merged segment to disk and writes the manifest — the atomic
    /// commit point that re-points the registry from the old files to the merged
    /// file — and only *then* deletes the old `.seg` files. If the merged-segment
    /// write fails, the compaction is aborted before anything is mutated; if the
    /// manifest write fails, the range is restored and the orphan merged file is
    /// removed. Either way the engine is left in its exact pre-compaction state
    /// with the old segments still durable, so a crash mid-compaction can never
    /// strand a manifest that references deleted files or lose the merged data to
    /// an in-memory-only fallback. Returns `None` on a degenerate range or on a
    /// failed (rolled-back) commit; in the failure case `persistence_healthy` is
    /// set false and a [`DurabilityOp::Compaction`](crate::events::DurabilityOp::Compaction)
    /// event is emitted.
    fn do_compact_range(
        &mut self,
        lo: usize,
        hi: usize,
        trigger: crate::events::CompactionTrigger,
    ) -> Option<CompactionReport> {
        if hi <= lo + 1 || hi > self.segments.len() {
            return None;
        }
        let compact_start = std::time::Instant::now();
        let segments_merged = hi - lo;
        let entries_before: usize = self.segments[lo..hi].iter().map(|s| s.len()).sum();
        let old_files: Vec<PathBuf> = self.segments[lo..hi]
            .iter()
            .filter_map(|s| {
                if let BaseSegment::Mmap(m) = s.as_ref() {
                    Some(m.path().to_path_buf())
                } else {
                    None
                }
            })
            .collect();
        // Materialize in-memory copies of the range for the merge WITHOUT removing
        // the originals from the vec: `Arc::clone` keeps each original live, so
        // `arc_into_memory` clones the data out rather than unwrapping it. Retaining
        // the originals is what makes the commit point reversible — a failed
        // manifest write rolls straight back to the (still-durable) source segments.
        let memory_segs: Vec<Segment> = self.segments[lo..hi]
            .iter()
            .map(|a| arc_into_memory(Arc::clone(a)))
            .collect();
        let refs: Vec<&Segment> = memory_segs.iter().collect();
        let merged = Segment::compact_from(&refs);
        let entries_after = merged.len();

        // Build the merged segment durably BEFORE any destructive action. On a
        // write/mmap failure, abort: the segments vec and the old files are
        // untouched, so the engine stays in its exact pre-compaction state.
        let (merged_base, merged_path) = match self.build_durable_base(merged) {
            Ok(v) => v,
            Err(e) => {
                self.emit(crate::events::EngineEvent::DurabilityFailure {
                    op: crate::events::DurabilityOp::Compaction,
                    detail: "compaction merged-segment write failed; compaction aborted \
                             (source segments untouched)"
                        .to_string(),
                    error: e.to_string(),
                });
                return None;
            }
        };

        // Splice the merged segment in for the range; we still hold the originals
        // in `old`. The manifest write is the commit point.
        let old: Vec<Arc<BaseSegment>> = self
            .segments
            .splice(lo..hi, std::iter::once(Arc::new(merged_base)))
            .collect();
        if !self.save_manifest_if_persistent() {
            // Commit point failed — roll back to the pre-compaction state: restore
            // the originals (still durable on disk), drop the merged segment, and
            // delete the orphan merged file. The old files were never touched.
            // `save_manifest_if_persistent` already set persistence_healthy=false
            // and emitted a ManifestWrite failure.
            self.segments.splice(lo..=lo, old);
            if let Some(p) = merged_path {
                self.best_effort_remove_segment(&p);
            }
            return None;
        }

        // Committed: the merged file is durable and referenced by the manifest, so
        // the old files are now unreferenced and safe to delete.
        drop(old);
        self.cleanup_segment_files(&old_files);
        let report = CompactionReport {
            segments_merged,
            entries_before,
            entries_after,
            tombstones_reclaimed: entries_before - entries_after,
        };
        self.emit(crate::events::EngineEvent::Compaction {
            report,
            trigger,
            base_segments_after: self.segments.len(),
            duration_secs: compact_start.elapsed().as_secs_f64(),
        });
        Some(report)
    }

    /// Check the memtable size against `config.memtable_flush_threshold` and
    /// flush if exceeded. Called automatically after `insert_live`.
    pub(in crate::segment) fn maybe_flush(&mut self) {
        if self.memtable.len() >= self.config.memtable_flush_threshold {
            self.flush();
        }
    }
}
