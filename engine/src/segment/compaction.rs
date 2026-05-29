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
        self.seal_and_push(sealed);
        self.emit(crate::events::EngineEvent::Flush {
            entries,
            base_segments_after: self.segments.len(),
            duration_secs: flush_start.elapsed().as_secs_f64(),
        });
        // Write WAL checkpoint + save manifest + query sources, then reset WAL
        self.checkpoint_wal();
        let manifest_ok = self.save_manifest_if_persistent();
        self.save_query_sources();
        if manifest_ok {
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
    /// objective when reads must probe every segment (as in ClickHouse and our
    /// percolator). `max_segments` is the threshold: if the current base segment
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
    /// there was anything to merge (i.e. more than one base segment existed).
    pub fn compact_all(&mut self) -> Option<CompactionReport> {
        if self.segments.len() < 2 {
            return None;
        }
        let compact_start = std::time::Instant::now();
        let segments_merged = self.segments.len();
        let entries_before: usize = self.segments.iter().map(|s| s.len()).sum();
        // Collect old mmap paths before draining
        let old_files = self.collect_mmap_paths();
        // Drain and materialize all segments to in-memory for compaction
        let memory_segs: Vec<Segment> = self.segments.drain(..).map(arc_into_memory).collect();
        let refs: Vec<&Segment> = memory_segs.iter().collect();
        let merged = Segment::compact_from(&refs);
        let entries_after = merged.len();
        self.seal_and_push(merged);
        self.cleanup_segment_files(&old_files);
        let report = CompactionReport {
            segments_merged,
            entries_before,
            entries_after,
            tombstones_reclaimed: entries_before - entries_after,
        };
        self.emit(crate::events::EngineEvent::Compaction {
            report,
            trigger: crate::events::CompactionTrigger::ExplicitAll,
            base_segments_after: self.segments.len(),
            duration_secs: compact_start.elapsed().as_secs_f64(),
        });
        self.save_manifest_if_persistent();
        Some(report)
    }

    /// Merge a specific range of base segments `[lo..hi)` into one, replacing
    /// them in the segments vec. Useful for leveled/tiered policies that pick
    /// adjacent pairs. Returns a report if the merge happened.
    pub fn compact_range(&mut self, lo: usize, hi: usize) -> Option<CompactionReport> {
        if hi <= lo + 1 || hi > self.segments.len() {
            return None;
        }
        let compact_start = std::time::Instant::now();
        let segments_merged = hi - lo;
        let entries_before: usize = self.segments[lo..hi].iter().map(|s| s.len()).sum();
        // Collect old mmap paths before draining
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
        // Drain the range and materialize to in-memory for compaction
        let memory_segs: Vec<Segment> = self.segments.drain(lo..hi).map(arc_into_memory).collect();
        let refs: Vec<&Segment> = memory_segs.iter().collect();
        let merged = Segment::compact_from(&refs);
        let entries_after = merged.len();
        let merged_base = self.make_base_segment(merged);
        self.segments.insert(lo, Arc::new(merged_base));
        self.cleanup_segment_files(&old_files);
        let report = CompactionReport {
            segments_merged,
            entries_before,
            entries_after,
            tombstones_reclaimed: entries_before - entries_after,
        };
        self.emit(crate::events::EngineEvent::Compaction {
            report,
            trigger: crate::events::CompactionTrigger::ExplicitRange { lo, hi },
            base_segments_after: self.segments.len(),
            duration_secs: compact_start.elapsed().as_secs_f64(),
        });
        self.save_manifest_if_persistent();
        Some(report)
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
                    return self.compact_range_with_trigger(
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
            return self.compact_range_with_trigger(
                lo,
                hi,
                crate::events::CompactionTrigger::SegmentCount {
                    count: self.segments.len(),
                },
            );
        }

        None
    }

    /// Internal: compact a range and emit an event with the given trigger reason.
    fn compact_range_with_trigger(
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
        // Collect old mmap paths before draining
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
        // Drain the range and materialize to in-memory for compaction
        let memory_segs: Vec<Segment> = self.segments.drain(lo..hi).map(arc_into_memory).collect();
        let refs: Vec<&Segment> = memory_segs.iter().collect();
        let merged = Segment::compact_from(&refs);
        let entries_after = merged.len();
        let merged_base = self.make_base_segment(merged);
        self.segments.insert(lo, Arc::new(merged_base));
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
        self.save_manifest_if_persistent();
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
