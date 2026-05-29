//! `impl Engine` — introspection: the [`EngineMetrics`](crate::events::EngineMetrics)
//! snapshot, per-component byte accounting, and the count/index accessors used by
//! the server's `/_stats` and bench harnesses.

use super::{BaseSegment, Engine};
use crate::index::CandidateIndex;
use crate::wal::Wal;

impl Engine {
    pub fn num_queries(&self) -> usize {
        self.segments.iter().map(|s| s.len()).sum::<usize>() + self.memtable.len()
    }
    pub fn num_segments(&self) -> usize {
        // base segments + the memtable as one logical segment
        self.segments.len() + 1
    }
    /// Total queries ever rejected (parse failures + class-D), across all
    /// ingest paths. Kept for back-compat; prefer the split accessors below.
    pub fn rejected(&self) -> u64 {
        self.rejected_parse + self.rejected_class_d
    }
    /// Queries dropped because their DSL string failed to parse.
    pub fn rejected_parse(&self) -> u64 {
        self.rejected_parse
    }
    /// Queries dropped as cost-class D (no anchorable required/any-of feature).
    pub fn rejected_class_d(&self) -> u64 {
        self.rejected_class_d
    }
    /// First base segment's main index (kept for bench/back-compat callers).
    /// Falls back to the memtable if no base segments exist.
    pub fn main_index(&self) -> &CandidateIndex {
        match self.segments.first().map(std::convert::AsRef::as_ref) {
            Some(BaseSegment::Memory(s)) => s.main_index(),
            _ => self.memtable.main_index(),
        }
    }
    pub fn broad_index(&self) -> &CandidateIndex {
        match self.segments.first().map(std::convert::AsRef::as_ref) {
            Some(BaseSegment::Memory(s)) => s.broad_index(),
            _ => self.memtable.broad_index(),
        }
    }
    pub fn class_counts(&self) -> [u64; 4] {
        let mut c = [0u64; 4];
        for seg in &self.segments {
            match seg.as_ref() {
                BaseSegment::Memory(s) => s.class_counts(&mut c),
                BaseSegment::Mmap(_) => {} // mmap segments don't expose class_counts cheaply
            }
        }
        self.memtable.class_counts(&mut c);
        c[3] = self.rejected_class_d; // D never enters any segment's `class`
        c
    }

    /// Snapshot of current engine metrics for monitoring and introspection.
    pub fn metrics(&self) -> crate::events::EngineMetrics {
        let segment_sizes: Vec<usize> = self.segments.iter().map(|s| s.len()).collect();
        let segment_holes: Vec<f64> = self.segments.iter().map(|s| s.holes_ratio()).collect();
        crate::events::EngineMetrics {
            total_queries: self.num_queries(),
            base_segments: self.segments.len(),
            memtable_entries: self.memtable.len(),
            segment_sizes,
            segment_holes,
            rejected_parse: self.rejected_parse,
            rejected_class_d: self.rejected_class_d,
            dict_features: self.dict.len(),
            exact_bytes: self.exact_bytes(),
            index_bytes: self.main_bytes() + self.broad_bytes(),
            filter_bytes: self.filter_bytes(),
            stale_segments: self.stale_segment_count(),
            dict_bytes: self.dict.heap_bytes(),
            query_store_bytes: self.query_store.resident_bytes(),
            logical_index_bytes: self
                .segments
                .iter()
                .map(|s| s.logical_index_bytes())
                .sum::<usize>()
                + self.memtable.logical_index_bytes(),
            alive_bytes: self.segments.iter().map(|s| s.alive_bytes()).sum::<usize>()
                + self.memtable.alive_bytes(),
            wal_size_bytes: self.wal.as_ref().map_or(0, Wal::size_bytes),
            wal_pending_entries: self.wal.as_ref().map_or(0, Wal::pending_entries),
        }
    }

    // ---- memory accounting for the perf report ----
    pub fn exact_bytes(&self) -> usize {
        self.segments.iter().map(|s| s.exact_bytes()).sum::<usize>() + self.memtable.exact_bytes()
    }
    pub fn main_bytes(&self) -> usize {
        self.segments.iter().map(|s| s.main_bytes()).sum::<usize>() + self.memtable.main_bytes()
    }
    pub fn broad_bytes(&self) -> usize {
        self.segments.iter().map(|s| s.broad_bytes()).sum::<usize>() + self.memtable.broad_bytes()
    }
    pub fn filter_bytes(&self) -> usize {
        self.segments
            .iter()
            .map(|s| s.filter_bytes())
            .sum::<usize>()
    }
    pub fn dict_len(&self) -> usize {
        self.dict.len()
    }
}
