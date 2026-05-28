//! Engine — LSM-shaped multi-segment index with memtable, flush, and bulk ingest.
//!
//! Design: docs/design/ingestion-and-updates.md
//! Invariant: Segments are immutable once sealed; writes go only to the memtable;
//!   matching unions across all segments with per-segment epoch-dedup
//! Hot path: yes — match_titles / match_titles_par are the main entry points
//!
//! Holds a vector of immutable BASE segments plus one mutable MEMTABLE segment
//! (the hot delta). Reads probe every segment and union the matched logical ids;
//! writes (insert_live / tombstone) touch only the memtable; `flush` seals the
//! memtable into an immutable base segment; `bulk_ingest` compiles a batch
//! directly into a fresh immutable base segment without rebuilding any existing
//! one. The shared dictionary + normalizer live on the engine (one global
//! feature space / frequency table across all segments).

use std::path::PathBuf;

use crate::compile::{build_signatures, extract, is_hot, CostClass, Extracted};
use crate::config::EngineConfig;
use crate::dict::{Dict, FeatureId};
use crate::exact::ExactStore;
use crate::filter::SegmentFilter;
use crate::index::CandidateIndex;
use crate::normalize::Normalizer;
use crate::storage::MmapSegment;
use crate::util::sig_key;
use crate::wal::{Wal, WalEntry};

#[derive(Default, Clone, Copy, Debug)]
pub struct MatchStats {
    pub unique_candidates: u32, // distinct queries exact-checked
    pub postings_scanned: u32,  // total posting entries unioned
    pub main_candidates: u32,
    pub broad_candidates: u32,
    pub matches: u32,
    pub probes_attempted: u32,  // total signature probes (before filter)
    pub probes_skipped: u32,    // probes skipped by anchor filter (definitely-not-present)
}

/// One immutable (or, for the memtable, mutable) slice of the index. Owns the
/// per-segment SoA + candidate indexes + liveness; the shared dict/norm stay on
/// the Engine. Local ids are segment-local (indexes into this segment's SoA).
///
/// Sealed (immutable) segments carry an anchor filter — a bloom filter over the
/// signature keys present in main + broad indexes. The filter lets `match_into`
/// skip probes that would definitely miss, cutting read amplification when
/// multiple segments exist. The memtable (mutable) has no filter; it's built
/// at seal time (flush / bulk_ingest / compaction).
#[derive(Default, Debug)]
pub struct Segment {
    main: CandidateIndex,
    broad: CandidateIndex,
    exact: ExactStore,
    class: Vec<CostClass>,
    alive: Vec<bool>,
    /// O(1) counter of alive (non-tombstoned) entries.
    alive_counter: usize,
    /// Anchor filter: present only on sealed (immutable) base segments.
    /// `None` for the memtable (mutable, entries added dynamically).
    filter: Option<SegmentFilter>,
    /// Vocab epoch at which this segment's queries were compiled.
    pub vocab_epoch: u64,
    /// Reverse index: logical_id → local_ids in this segment. Enables O(1)
    /// delete lookups instead of full segment scans.
    logical_index: crate::util::FastMap<u64, Vec<u32>>,
}

impl Segment {
    pub fn new() -> Self {
        Segment {
            main: CandidateIndex::new(),
            broad: CandidateIndex::new(),
            exact: ExactStore::new(),
            class: Vec::new(),
            alive: Vec::new(),
            alive_counter: 0,
            filter: None,
            vocab_epoch: 0,
            logical_index: crate::util::fast_map(),
        }
    }

    /// Build and attach the anchor filter from the current main + broad index
    /// keys. Called once when a segment is sealed (flush, bulk_ingest, compaction).
    /// After this, `match_into` will use the filter to skip probes.
    fn build_filter(&mut self) {
        let mut keys = self.main.keys();
        keys.extend(self.broad.keys());
        self.filter = Some(SegmentFilter::build(&keys));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.exact.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty()
    }

    pub fn main_index(&self) -> &CandidateIndex {
        &self.main
    }
    pub fn broad_index(&self) -> &CandidateIndex {
        &self.broad
    }

    /// Append one already-extracted query. Returns the new segment-local id, or
    /// `None` if the query is class D (rejected, not stored).
    pub fn add_compiled(
        &mut self,
        ex: &Extracted,
        dict: &Dict,
        logical: u64,
        version: u32,
    ) -> Option<u32> {
        let plan = build_signatures(ex, dict);
        if plan.class == CostClass::D {
            return None;
        }
        let local = self.exact.push(ex, dict, version, logical);
        for &s in &plan.main_sigs {
            self.main.insert(s, local);
        }
        for &s in &plan.broad_sigs {
            self.broad.insert(s, local);
        }
        self.class.push(plan.class);
        self.alive.push(true);
        self.alive_counter += 1;
        self.logical_index.entry(logical).or_default().push(local);
        Some(local)
    }

    pub fn tombstone(&mut self, local_id: u32) {
        if let Some(slot) = self.alive.get_mut(local_id as usize) {
            if *slot {
                self.alive_counter -= 1;
            }
            *slot = false;
        }
    }

    pub fn locals_for_logical(&self, logical_id: u64) -> &[u32] {
        self.logical_index.get(&logical_id).map_or(&[], |v| v.as_slice())
    }

    pub fn class_counts(&self, c: &mut [u64; 4]) {
        for &cl in &self.class {
            match cl {
                CostClass::A => c[0] += 1,
                CostClass::B => c[1] += 1,
                CostClass::C => c[2] += 1,
                CostClass::D => c[3] += 1,
            }
        }
    }

    /// Probe this segment for one title and append matched LOGICAL ids to `out`.
    /// `seen` is this segment's epoch-stamp dedup array (size = self.len()).
    ///
    /// If the segment has an anchor filter (sealed base segments), each signature
    /// key is tested against the filter first. Keys that are definitely not
    /// present are skipped without touching the candidate index, cutting read
    /// amplification across multiple segments.
    #[allow(clippy::too_many_arguments)]
    pub fn match_into(
        &self,
        feats: &[FeatureId],
        tmask: u64,
        dict: &Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        include_broad: bool,
        stats: &mut MatchStats,
    ) {
        let filter = self.filter.as_ref();

        // arity-1 signatures (one per feature)
        for &f in feats {
            let key = sig_key(&[f]);
            stats.probes_attempted += 1;
            if let Some(flt) = filter {
                if !flt.may_contain(key) {
                    stats.probes_skipped += 1;
                    continue;
                }
            }
            self.probe(key, &self.main, epoch, tmask, feats, seen, out, stats, false);
        }
        // arity-2 signatures: {hot feature} x {every other feature}
        for &h in feats {
            if is_hot(dict, h) {
                for &o in feats {
                    if o != h {
                        let (a, b) = if h < o { (h, o) } else { (o, h) };
                        let key = sig_key(&[a, b]);
                        stats.probes_attempted += 1;
                        if let Some(flt) = filter {
                            if !flt.may_contain(key) {
                                stats.probes_skipped += 1;
                                continue;
                            }
                        }
                        self.probe(
                            key,
                            &self.main,
                            epoch,
                            tmask,
                            feats,
                            seen,
                            out,
                            stats,
                            false,
                        );
                    }
                }
            }
        }
        // broad lane (arity-1 anchors), measured separately
        if include_broad {
            for &f in feats {
                let key = sig_key(&[f]);
                stats.probes_attempted += 1;
                if let Some(flt) = filter {
                    if !flt.may_contain(key) {
                        stats.probes_skipped += 1;
                        continue;
                    }
                }
                self.probe(key, &self.broad, epoch, tmask, feats, seen, out, stats, true);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn probe(
        &self,
        key: u64,
        index: &CandidateIndex,
        epoch: u32,
        tmask: u64,
        feats: &[FeatureId],
        seen: &mut [u32],
        out: &mut Vec<u64>,
        stats: &mut MatchStats,
        is_broad: bool,
    ) {
        if let Some(posting) = index.get(key) {
            stats.postings_scanned += posting.len() as u32;
            posting.for_each(|local| {
                // dedup across signatures with an epoch stamp (O(1), no alloc)
                if seen[local as usize] == epoch {
                    return;
                }
                seen[local as usize] = epoch;
                stats.unique_candidates += 1;
                if is_broad {
                    stats.broad_candidates += 1;
                } else {
                    stats.main_candidates += 1;
                }
                if !self.alive[local as usize] {
                    return; // tombstoned
                }
                if self.exact.verify(local, tmask, feats) {
                    out.push(self.exact.logical(local));
                }
            });
        }
    }

    /// Number of alive (non-tombstoned) entries in this segment (O(1)).
    pub fn alive_count(&self) -> usize {
        self.alive_counter
    }

    /// Fraction of entries that are tombstoned (holes_ratio for merge scoring).
    pub fn holes_ratio(&self) -> f64 {
        let total = self.len();
        if total == 0 {
            return 0.0;
        }
        1.0 - (self.alive_count() as f64 / total as f64)
    }

    /// Merge multiple source segments into one fresh segment, dropping tombstoned
    /// entries and renumbering local IDs to be dense/contiguous. This is the core
    /// compaction mechanic.
    ///
    /// Correctness argument: every alive entry is copied verbatim (exact store
    /// data, cost class); every signature posting that pointed to an alive entry
    /// is remapped to the new local ID. Dead entries are simply skipped, reclaiming
    /// their space. The resulting segment is equivalent to the union of the alive
    /// entries from all sources.
    pub fn compact_from(sources: &[&Segment]) -> Segment {
        let mut dest = Segment::new();

        for &src in sources {
            // Build the old→new local-id remap for this source segment.
            // Dead entries get u32::MAX (sentinel); alive entries get dense IDs.
            let n = src.len();
            let mut remap: Vec<u32> = vec![u32::MAX; n];
            for old in 0..n {
                if src.alive[old] {
                    let new_id = src.exact.copy_entry(old as u32, &mut dest.exact);
                    let logical = dest.exact.logical(new_id);
                    dest.class.push(src.class[old]);
                    dest.alive.push(true);
                    dest.alive_counter += 1;
                    dest.logical_index.entry(logical).or_default().push(new_id);
                    remap[old] = new_id;
                }
            }

            // Remap main index postings
            src.main.for_each_posting(|key, posting| {
                posting.for_each(|old_id| {
                    let new_id = remap[old_id as usize];
                    if new_id != u32::MAX {
                        dest.main.insert(key, new_id);
                    }
                });
            });

            // Remap broad index postings
            src.broad.for_each_posting(|key, posting| {
                posting.for_each(|old_id| {
                    let new_id = remap[old_id as usize];
                    if new_id != u32::MAX {
                        dest.broad.insert(key, new_id);
                    }
                });
            });
        }
        // Build anchor filter for the newly compacted (sealed) segment.
        dest.build_filter();
        // Merged segment inherits the minimum epoch — still stale if any source was.
        dest.vocab_epoch = sources.iter().map(|s| s.vocab_epoch).min().unwrap_or(0);
        dest
    }

    /// Reconstruct a Segment from pre-built parts. Used by MmapSegment::to_memory_segment
    /// to convert mmap'd data back into an in-memory segment (for compaction).
    pub fn from_parts(
        main: CandidateIndex,
        broad: CandidateIndex,
        exact: ExactStore,
        class: Vec<CostClass>,
        alive: Vec<bool>,
    ) -> Self {
        let alive_counter = alive.iter().filter(|&&a| a).count();
        let mut logical_index: crate::util::FastMap<u64, Vec<u32>> = crate::util::fast_map();
        for i in 0..exact.len() {
            if alive[i] {
                logical_index.entry(exact.logical(i as u32)).or_default().push(i as u32);
            }
        }
        let mut seg = Segment { main, broad, exact, class, alive, alive_counter, filter: None, vocab_epoch: 0, logical_index };
        seg.build_filter();
        seg
    }

    // ---- accessors for serialization (storage.rs) ----
    pub fn exact_store(&self) -> &ExactStore { &self.exact }
    pub fn classes(&self) -> &[CostClass] { &self.class }
    pub fn alive_flags(&self) -> &[bool] { &self.alive }
    pub fn filter_ref(&self) -> Option<&SegmentFilter> { self.filter.as_ref() }

    // ---- memory accounting for the perf report ----
    pub fn exact_bytes(&self) -> usize {
        self.exact.heap_bytes()
    }
    pub fn main_bytes(&self) -> usize {
        self.main.heap_bytes()
    }
    pub fn broad_bytes(&self) -> usize {
        self.broad.heap_bytes()
    }
    pub fn filter_bytes(&self) -> usize {
        self.filter.as_ref().map_or(0, |f| f.heap_bytes())
    }
}

// ---- BaseSegment: in-memory or mmap'd sealed segment ----

/// A sealed (immutable) base segment, either in-memory or backed by mmap.
/// The memtable is always an in-memory `Segment` (mutable).
pub enum BaseSegment {
    Memory(Segment),
    Mmap(MmapSegment),
}

impl BaseSegment {
    /// The vocab epoch at which this segment's queries were compiled.
    pub fn vocab_epoch(&self) -> u64 {
        match self {
            BaseSegment::Memory(s) => s.vocab_epoch,
            BaseSegment::Mmap(s) => s.vocab_epoch,
        }
    }
    pub fn set_vocab_epoch(&mut self, epoch: u64) {
        match self {
            BaseSegment::Memory(s) => s.vocab_epoch = epoch,
            BaseSegment::Mmap(s) => s.vocab_epoch = epoch,
        }
    }
}

impl std::fmt::Debug for BaseSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BaseSegment::Memory(s) => f.debug_tuple("Memory").field(s).finish(),
            BaseSegment::Mmap(s) => f.debug_tuple("Mmap").field(s).finish(),
        }
    }
}

impl BaseSegment {
    pub fn len(&self) -> usize {
        match self { BaseSegment::Memory(s) => s.len(), BaseSegment::Mmap(s) => s.len() }
    }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn holes_ratio(&self) -> f64 {
        match self {
            BaseSegment::Memory(s) => s.holes_ratio(),
            BaseSegment::Mmap(s) => s.holes_ratio(),
        }
    }
    pub fn alive_count(&self) -> usize {
        match self {
            BaseSegment::Memory(s) => s.alive_count(),
            BaseSegment::Mmap(s) => s.alive_count(),
        }
    }
    pub fn is_alive(&self, local_id: u32) -> bool {
        match self {
            BaseSegment::Memory(s) => *s.alive.get(local_id as usize).unwrap_or(&false),
            BaseSegment::Mmap(s) => *s.alive_overlay.get(local_id as usize).unwrap_or(&false),
        }
    }
    pub fn logical(&self, local_id: u32) -> u64 {
        match self {
            BaseSegment::Memory(s) => s.exact.logical(local_id),
            BaseSegment::Mmap(s) => s.logical(local_id),
        }
    }
    pub fn tombstone(&mut self, local_id: u32) {
        match self {
            BaseSegment::Memory(s) => s.tombstone(local_id),
            BaseSegment::Mmap(s) => s.tombstone(local_id),
        }
    }
    pub fn locals_for_logical(&self, logical_id: u64) -> &[u32] {
        match self {
            BaseSegment::Memory(s) => s.locals_for_logical(logical_id),
            BaseSegment::Mmap(s) => s.locals_for_logical(logical_id),
        }
    }
    pub fn match_into(
        &self,
        feats: &[FeatureId],
        tmask: u64,
        dict: &Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        include_broad: bool,
        stats: &mut MatchStats,
    ) {
        match self {
            BaseSegment::Memory(s) => s.match_into(feats, tmask, dict, epoch, seen, out, include_broad, stats),
            BaseSegment::Mmap(s) => s.match_into(feats, tmask, dict, epoch, seen, out, include_broad, stats),
        }
    }
    pub fn exact_bytes(&self) -> usize {
        match self { BaseSegment::Memory(s) => s.exact_bytes(), BaseSegment::Mmap(_) => 0 }
    }
    pub fn main_bytes(&self) -> usize {
        match self { BaseSegment::Memory(s) => s.main_bytes(), BaseSegment::Mmap(_) => 0 }
    }
    pub fn broad_bytes(&self) -> usize {
        match self { BaseSegment::Memory(s) => s.broad_bytes(), BaseSegment::Mmap(_) => 0 }
    }
    pub fn filter_bytes(&self) -> usize {
        match self { BaseSegment::Memory(s) => s.filter_bytes(), BaseSegment::Mmap(_) => 0 }
    }

    /// Convert to an owned in-memory Segment (needed by compact_from).
    /// Memory segments are returned directly; mmap segments are materialized.
    fn into_memory(self) -> Segment {
        match self {
            BaseSegment::Memory(s) => s,
            BaseSegment::Mmap(s) => s.to_memory_segment(),
        }
    }
}

/// Reusable per-thread scratch — keeps the hot path allocation-free in steady
/// state. `seen` is now per-segment: `seen[seg_idx]` is that segment's epoch
/// stamp array, sized to that segment's `len()`. Buffers are reused across calls.
#[derive(Debug)]
pub struct MatchScratch {
    lc: String,
    feats: Vec<FeatureId>,
    seen: Vec<Vec<u32>>,
    epoch: u32,
}

impl MatchScratch {
    pub fn new() -> Self {
        MatchScratch {
            lc: String::with_capacity(256),
            feats: Vec::with_capacity(64),
            seen: Vec::new(),
            epoch: 0,
        }
    }

    /// Make sure we have one seen-buffer per segment, each at least as large as
    /// that segment's length. Reuses existing allocations (steady-state: no-op).
    fn ensure(&mut self, seg_lens: &[usize]) {
        if self.seen.len() < seg_lens.len() {
            self.seen.resize_with(seg_lens.len(), Vec::new);
        }
        for (buf, &n) in self.seen.iter_mut().zip(seg_lens.iter()) {
            if buf.len() < n {
                buf.resize(n, 0);
            }
        }
    }
}

impl Default for MatchScratch {
    fn default() -> Self {
        Self::new()
    }
}

/// Outcome of ingesting a batch of stored queries. Lets callers see how many
/// queries actually entered the index versus why the rest were dropped, instead
/// of silently losing them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestReport {
    /// Queries successfully compiled into the index.
    pub ingested: usize,
    /// Queries dropped because the DSL string failed to parse.
    pub rejected_parse: usize,
    /// Queries dropped as cost-class D (no required feature / any-of to anchor).
    pub rejected_class_d: usize,
}

/// Outcome of a single live insert. Distinguishes a successful insert (with its
/// memtable-local id) from a class-D rejection. A parse failure is surfaced as
/// `Err(ParseError)` by [`Engine::try_insert_live`], never folded in here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// Inserted; carries the memtable-local id (for a later `tombstone`).
    Inserted(u32),
    /// Compiled but rejected as cost-class D — not stored.
    RejectedClassD,
}

/// Result of a compaction operation. Tells callers what happened so they can
/// log it, tune the policy, or feed it to telemetry.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompactionReport {
    /// Number of source segments that were merged.
    pub segments_merged: usize,
    /// Total entries in the source segments (alive + dead).
    pub entries_before: usize,
    /// Alive entries in the output segment (dead entries dropped).
    pub entries_after: usize,
    /// Number of tombstoned entries reclaimed.
    pub tombstones_reclaimed: usize,
}

pub struct Engine {
    config: EngineConfig,
    norm: Normalizer,
    /// Vocabulary used to build the normalizer (if set via `with_vocab`).
    vocab: Option<crate::vocab::Vocab>,
    dict: Dict,
    /// immutable base segments (sealed; never mutated after creation)
    segments: Vec<BaseSegment>,
    /// mutable hot delta — insert_live / tombstone land here
    memtable: Segment,
    rejected_parse: u64,   // queries dropped because the DSL failed to parse
    rejected_class_d: u64, // class-D queries rejected at compile (not stored)
    /// Optional observer callback for engine events (flush, compact, ingest, etc.)
    observer: Option<Box<dyn Fn(&crate::events::EngineEvent) + Send + Sync>>,
    /// Write-ahead log (present when data_dir is set).
    wal: Option<Wal>,
    /// Next segment file sequence number (for naming: seg_000001.seg, etc.)
    next_seg_id: u64,
    /// Health flag: false if a WAL write has failed (durability degraded).
    pub wal_healthy: bool,
    /// Health flag: false if a manifest or segment file write has failed.
    pub persistence_healthy: bool,
    /// Number of corrupt segments skipped during Engine::open().
    pub skipped_segments: usize,
    /// Maps logical_id → original query text for retrieval and search hit enrichment.
    query_store: crate::util::FastMap<u64, String>,
    /// Monotonic counter incremented on each `set_vocab()` call. Segments compiled
    /// at an earlier epoch are stale (their normalizer differs from the current one).
    vocab_epoch: u64,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("config", &self.config)
            .field("norm", &self.norm)
            .field("dict", &self.dict)
            .field("base_segments", &self.segments.len())
            .field("memtable_entries", &self.memtable.len())
            .field("rejected_parse", &self.rejected_parse)
            .field("rejected_class_d", &self.rejected_class_d)
            .field("has_observer", &self.observer.is_some())
            .field("has_wal", &self.wal.is_some())
            .field("next_seg_id", &self.next_seg_id)
            .field("wal_healthy", &self.wal_healthy)
            .field("persistence_healthy", &self.persistence_healthy)
            .field("skipped_segments", &self.skipped_segments)
            .field("query_store_entries", &self.query_store.len())
            .field("vocab_epoch", &self.vocab_epoch)
            .finish()
    }
}

impl Engine {
    /// Create an engine with default configuration.
    pub fn new(norm: Normalizer) -> Self {
        Self::with_config(norm, EngineConfig::default())
    }

    /// Create an engine with explicit configuration. If `config.data_dir` is set,
    /// initializes the data directory and WAL.
    pub fn with_config(norm: Normalizer, config: EngineConfig) -> Self {
        let wal = if let Some(ref dir) = config.data_dir {
            std::fs::create_dir_all(dir).ok();
            let seg_dir = dir.join("segments");
            std::fs::create_dir_all(&seg_dir).ok();
            Wal::open(&dir.join("wal.log")).ok()
        } else {
            None
        };
        Engine {
            config,
            norm,
            vocab: None,
            dict: Dict::new(),
            segments: Vec::new(),
            memtable: Segment::new(),
            rejected_parse: 0,
            rejected_class_d: 0,
            observer: None,
            wal,
            next_seg_id: 1,
            wal_healthy: true,
            persistence_healthy: true,
            skipped_segments: 0,
            query_store: crate::util::fast_map(),
            vocab_epoch: 0,
        }
    }

    /// Create an engine from a [`Vocab`](crate::vocab::Vocab), which is
    /// converted to a Normalizer internally. The vocab is stored so it can
    /// be queried or serialized later.
    pub fn with_vocab(
        vocab: crate::vocab::Vocab,
        config: EngineConfig,
    ) -> Result<Self, crate::error::NormalizerError> {
        let norm = vocab.to_normalizer()?;
        let mut eng = Self::with_config(norm, config);
        eng.vocab = Some(vocab);
        Ok(eng)
    }

    /// The vocabulary used to build this engine's normalizer, if one was set.
    pub fn vocab(&self) -> Option<&crate::vocab::Vocab> {
        self.vocab.as_ref()
    }

    /// Replace the engine's vocabulary and normalizer. Existing compiled
    /// queries become stale — the caller must reingest for consistent matching.
    /// Returns the number of stale segments that need reingestion.
    pub fn set_vocab(
        &mut self,
        vocab: crate::vocab::Vocab,
    ) -> Result<usize, crate::error::NormalizerError> {
        self.norm = vocab.to_normalizer()?;
        self.vocab = Some(vocab);
        self.vocab_epoch += 1;
        Ok(self.stale_segment_count())
    }

    /// Number of base segments compiled against an older vocab epoch.
    pub fn stale_segment_count(&self) -> usize {
        let current = self.vocab_epoch;
        self.segments.iter().filter(|s| s.vocab_epoch() < current).count()
            + if self.memtable.vocab_epoch < current && !self.memtable.is_empty() { 1 } else { 0 }
    }

    /// True if any segment was compiled with a different normalizer than the
    /// current one. Matching still works (no panic) but may produce incorrect
    /// results until stale queries are reingested.
    pub fn has_stale_segments(&self) -> bool {
        self.stale_segment_count() > 0
    }

    /// The current vocab epoch. Segments compiled at this epoch are up-to-date.
    pub fn vocab_epoch(&self) -> u64 {
        self.vocab_epoch
    }

    /// Open an engine from an existing data directory, recovering state from
    /// the manifest and WAL. The normalizer must be the same one used when the
    /// engine was originally built (feature spaces must align).
    pub fn open(norm: Normalizer, config: EngineConfig) -> std::io::Result<Self> {
        let dir = config.data_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "data_dir required for open")
        })?;

        let manifest_path = dir.join("manifest.bin");
        if !manifest_path.exists() {
            // No existing data — return a fresh engine
            return Ok(Self::with_config(norm, config));
        }

        let manifest = crate::storage::read_manifest(&manifest_path)?;
        let dict = crate::storage::deserialize_dict(&manifest.dict_data)?;

        // Open mmap'd segments (skip corrupt ones rather than failing startup)
        let seg_dir = dir.join("segments");
        let mut segments = Vec::with_capacity(manifest.segment_files.len());
        let mut skipped_segments = 0usize;
        for name in &manifest.segment_files {
            let seg_path = seg_dir.join(name);
            match MmapSegment::open(&seg_path) {
                Ok(mmap_seg) => segments.push(BaseSegment::Mmap(mmap_seg)),
                Err(e) => {
                    eprintln!("[percolator] skipping corrupt segment {:?}: {}", seg_path, e);
                    skipped_segments += 1;
                }
            }
        }

        // Open WAL and replay
        let wal_path = dir.join("wal.log");
        let wal = if wal_path.exists() {
            Some(Wal::open(&wal_path)?)
        } else {
            Some(Wal::open(&wal_path)?)
        };

        // Load persisted query sources (if available)
        let sources_path = dir.join("sources.dat");
        let query_store = crate::storage::load_query_sources(&sources_path)
            .unwrap_or_default();

        let mut engine = Engine {
            config,
            norm,
            vocab: None,
            dict,
            segments,
            memtable: Segment::new(),
            rejected_parse: manifest.rejected_parse,
            rejected_class_d: manifest.rejected_class_d,
            observer: None,
            wal,
            next_seg_id: manifest.next_seg_id,
            wal_healthy: true,
            persistence_healthy: skipped_segments == 0,
            skipped_segments,
            query_store,
            vocab_epoch: 0,
        };

        // Replay WAL entries after last checkpoint
        let recovery = Wal::recover(&wal_path)?;
        if recovery.skipped_bytes > 0 {
            eprintln!(
                "WARNING: WAL recovery skipped {} bytes of corrupt/torn data at tail",
                recovery.skipped_bytes,
            );
        }
        for entry in recovery.entries {
            match entry {
                WalEntry::Insert { logical, version, text, .. } => {
                    // Replay without re-writing to WAL
                    engine.replay_insert(&text, logical, version);
                }
                WalEntry::Tombstone { seg_idx, local_id, .. } => {
                    engine.replay_tombstone(seg_idx, local_id);
                }
                WalEntry::FlushCheckpoint { .. } => {
                    // Skip — already handled by manifest
                }
            }
        }

        Ok(engine)
    }

    /// Set an observer callback that receives [`EngineEvent`](crate::events::EngineEvent)s
    /// for flush, compaction, ingest, and other lifecycle events. The callback
    /// must be `Send + Sync` (safe to call from rayon threads). Pass `None` to
    /// clear a previously set observer.
    pub fn set_observer<F: Fn(&crate::events::EngineEvent) + Send + Sync + 'static>(
        &mut self,
        observer: F,
    ) {
        self.observer = Some(Box::new(observer));
    }

    /// Clear the observer callback.
    pub fn clear_observer(&mut self) {
        self.observer = None;
    }

    /// Emit an event to the observer (if set). No-op when no observer is registered.
    #[inline]
    fn emit(&self, event: crate::events::EngineEvent) {
        if let Some(ref cb) = self.observer {
            cb(&event);
        }
    }

    /// Read-only access to the current configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Read-only access to the shared feature dictionary.
    pub fn dict(&self) -> &Dict {
        &self.dict
    }
    /// Read-only access to the normalizer.
    pub fn normalizer(&self) -> &Normalizer {
        &self.norm
    }

    /// Look up the original query text for a logical ID. Returns `None` if
    /// the ID was never ingested or has been deleted.
    pub fn get_query_source(&self, logical_id: u64) -> Option<&str> {
        self.query_store.get(&logical_id).map(|s| s.as_str())
    }

    /// Explain why a stored query matched (or would match) a given title.
    /// Re-derives the CompiledQuery from stored source text using the
    /// read-only compile path. Returns `None` if the query source is
    /// unavailable.
    pub fn explain_hit(
        &self,
        logical_id: u64,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let source = self.get_query_source(logical_id)?;
        let mut lc = String::new();
        let cq = crate::compile::compile_one_readonly(
            source, logical_id, &self.norm, &self.dict, &mut lc,
        ).ok()?;
        Some(crate::explain::explain_match_structured(&cq, title, &self.norm, &self.dict))
    }

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
        match self.segments.first() {
            Some(BaseSegment::Memory(s)) => s.main_index(),
            _ => self.memtable.main_index(),
        }
    }
    pub fn broad_index(&self) -> &CandidateIndex {
        match self.segments.first() {
            Some(BaseSegment::Memory(s)) => s.broad_index(),
            _ => self.memtable.broad_index(),
        }
    }
    pub fn class_counts(&self) -> [u64; 4] {
        let mut c = [0u64; 4];
        for seg in &self.segments {
            match seg {
                BaseSegment::Memory(s) => s.class_counts(&mut c),
                BaseSegment::Mmap(_) => {} // mmap segments don't expose class_counts cheaply
            }
        }
        self.memtable.class_counts(&mut c);
        c[3] = self.rejected_class_d; // D never enters any segment's `class`
        c
    }

    /// Build the first BASE segment from a batch of `(logical_id, query_text)`.
    /// Two passes:
    ///   A: parse + extract + bump frequencies
    ///   (finalize the common mask)
    ///   B: choose signatures, classify, append to the base segment.
    pub fn build_from_queries(&mut self, queries: &[(u64, String)]) -> IngestReport {
        let mut report = IngestReport::default();
        let mut lc = String::new();
        let mut extracted: Vec<(u64, Extracted, &str)> = Vec::with_capacity(queries.len());

        // Pass A
        for (logical, text) in queries {
            match crate::dsl::parse(text) {
                Ok(ast) => {
                    let ex = extract(&ast, &self.norm, &mut self.dict, &mut lc);
                    extracted.push((*logical, ex, text));
                }
                Err(_) => {
                    self.rejected_parse += 1;
                    report.rejected_parse += 1;
                }
            }
        }

        // finalize the 64-bit common mask now that all frequencies are known
        self.dict.finalize_mask();

        // Pass B -> first base segment
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        for (logical, ex, text) in &extracted {
            if seg.add_compiled(ex, &self.dict, *logical, 1).is_none() {
                self.rejected_class_d += 1;
                report.rejected_class_d += 1;
            } else {
                self.query_store.insert(*logical, (*text).to_string());
                report.ingested += 1;
            }
        }
        // Seal: build anchor filter before pushing as immutable base segment.
        seg.build_filter();
        self.seal_and_push(seg);
        self.emit(crate::events::EngineEvent::Ingest {
            ingested: report.ingested,
            rejected_parse: report.rejected_parse,
            rejected_class_d: report.rejected_class_d,
            base_segments_after: self.segments.len(),
        });
        self.save_manifest_if_persistent();
        report
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
            Err(_) => {
                self.rejected_parse += 1;
                None
            }
        }
    }

    /// Live insert that surfaces a parse failure as `Err(ParseError)` instead of
    /// folding it into a silent `None`. On success returns the outcome (inserted
    /// id, or class-D rejection). Class-D rejections are still counted toward
    /// `rejected_class_d()`; parse errors are the caller's to handle (and are
    /// NOT counted here, since they are returned).
    pub fn try_insert_live(
        &mut self,
        text: &str,
        logical: u64,
        version: u32,
    ) -> Result<InsertOutcome, crate::error::ParseError> {
        // Write to WAL FIRST (durability before visibility)
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_insert(logical, version, text) {
                eprintln!("[percolator] WAL insert write failed: {}", e);
                self.wal_healthy = false;
            }
        }
        let ast = crate::dsl::parse(text)?;
        let mut lc = String::new();
        let ex = extract(&ast, &self.norm, &mut self.dict, &mut lc);
        match self.memtable.add_compiled(&ex, &self.dict, logical, version) {
            Some(local) => {
                self.query_store.insert(logical, text.to_string());
                Ok(InsertOutcome::Inserted(local))
            }
            None => {
                self.rejected_class_d += 1;
                Ok(InsertOutcome::RejectedClassD)
            }
        }
    }

    /// Tombstone a query version in the MEMTABLE (update = insert_live new +
    /// tombstone old). `local_id` is a memtable-local id (as returned by
    /// `insert_live`).
    pub fn tombstone(&mut self, local_id: u32) {
        // WAL: memtable tombstones use seg_idx = u32::MAX as sentinel
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_tombstone(u32::MAX, local_id) {
                eprintln!("[percolator] WAL tombstone write failed: {}", e);
                self.wal_healthy = false;
            }
        }
        self.memtable.tombstone(local_id);
    }

    /// Tombstone a query in a specific base segment (for callers that track
    /// (segment, local) addresses). `seg_idx` indexes `self.segments`.
    pub fn tombstone_in(&mut self, seg_idx: usize, local_id: u32) {
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_tombstone(seg_idx as u32, local_id) {
                eprintln!("[percolator] WAL tombstone write failed: {}", e);
                self.wal_healthy = false;
            }
        }
        if let Some(seg) = self.segments.get_mut(seg_idx) {
            seg.tombstone(local_id);
        }
    }

    /// Delete all live entries with a given logical ID across all segments
    /// and the memtable. Uses the per-segment reverse index for O(segments)
    /// lookup instead of O(total_entries) full scan.
    pub fn delete_by_logical_id(&mut self, logical_id: u64) -> usize {
        let mut count = 0usize;

        for (seg_idx, seg) in self.segments.iter_mut().enumerate() {
            let locals: Vec<u32> = seg.locals_for_logical(logical_id)
                .iter().copied()
                .filter(|&local| seg.is_alive(local))
                .collect();
            for local in locals {
                if let Some(ref mut wal) = self.wal {
                    if let Err(e) = wal.append_tombstone(seg_idx as u32, local) {
                        eprintln!("[percolator] WAL tombstone write failed: {}", e);
                        self.wal_healthy = false;
                    }
                }
                seg.tombstone(local);
                count += 1;
            }
        }

        let mem_locals: Vec<u32> = self.memtable.locals_for_logical(logical_id)
            .iter().copied()
            .filter(|&local| self.memtable.alive.get(local as usize).copied().unwrap_or(false))
            .collect();
        for local in mem_locals {
            if let Some(ref mut wal) = self.wal {
                if let Err(e) = wal.append_tombstone(u32::MAX, local) {
                    eprintln!("[percolator] WAL tombstone write failed: {}", e);
                    self.wal_healthy = false;
                }
            }
            self.memtable.tombstone(local);
            count += 1;
        }

        if count > 0 {
            self.query_store.remove(&logical_id);
        }
        count
    }

    /// Seal the current memtable into an immutable base segment and start a
    /// fresh (empty) memtable. If `auto_compact_on_flush` is enabled in the
    /// config, runs `maybe_compact` after the flush.
    pub fn flush(&mut self) {
        if self.memtable.is_empty() {
            return;
        }
        let entries = self.memtable.len();
        let mut fresh = Segment::new();
        fresh.vocab_epoch = self.vocab_epoch;
        let mut sealed = std::mem::replace(&mut self.memtable, fresh);
        sealed.build_filter();
        self.seal_and_push(sealed);
        self.emit(crate::events::EngineEvent::Flush {
            entries,
            base_segments_after: self.segments.len(),
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

    /// Compile a batch DIRECTLY into a new immutable base segment and append it.
    /// Does not touch or rebuild any existing segment. Bumps global frequencies
    /// (so the shared dict stays accurate), but uses the already-finalized mask
    /// for signature selection (finalizing once if it was never done).
    pub fn bulk_ingest(&mut self, queries: &[(u64, String)]) -> IngestReport {
        let mut report = IngestReport::default();
        let mut lc = String::new();
        let mut extracted: Vec<(u64, Extracted, &str)> = Vec::with_capacity(queries.len());
        for (logical, text) in queries {
            match crate::dsl::parse(text) {
                Ok(ast) => {
                    let ex = extract(&ast, &self.norm, &mut self.dict, &mut lc);
                    extracted.push((*logical, ex, text));
                }
                Err(_) => {
                    self.rejected_parse += 1;
                    report.rejected_parse += 1;
                }
            }
        }
        if !self.dict.is_finalized() {
            self.dict.finalize_mask();
        }
        let mut seg = Segment::new();
        for (logical, ex, text) in &extracted {
            if seg.add_compiled(ex, &self.dict, *logical, 1).is_none() {
                self.rejected_class_d += 1;
                report.rejected_class_d += 1;
            } else {
                self.query_store.insert(*logical, (*text).to_string());
                report.ingested += 1;
            }
        }
        // Seal: build anchor filter before pushing as immutable base segment.
        seg.build_filter();
        self.seal_and_push(seg);
        self.emit(crate::events::EngineEvent::Ingest {
            ingested: report.ingested,
            rejected_parse: report.rejected_parse,
            rejected_class_d: report.rejected_class_d,
            base_segments_after: self.segments.len(),
        });
        self.save_manifest_if_persistent();
        if self.config.auto_compact_on_ingest {
            self.maybe_compact();
        }
        report
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
        let segments_merged = self.segments.len();
        let entries_before: usize = self.segments.iter().map(|s| s.len()).sum();
        // Collect old mmap paths before draining
        let old_files = self.collect_mmap_paths();
        // Drain and materialize all segments to in-memory for compaction
        let memory_segs: Vec<Segment> =
            self.segments.drain(..).map(|s| s.into_memory()).collect();
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
        let segments_merged = hi - lo;
        let entries_before: usize = self.segments[lo..hi].iter().map(|s| s.len()).sum();
        // Collect old mmap paths before draining
        let old_files: Vec<PathBuf> = self.segments[lo..hi].iter().filter_map(|s| {
            if let BaseSegment::Mmap(m) = s { Some(m.path().to_path_buf()) } else { None }
        }).collect();
        // Drain the range and materialize to in-memory for compaction
        let memory_segs: Vec<Segment> =
            self.segments.drain(lo..hi).map(|s| s.into_memory()).collect();
        let refs: Vec<&Segment> = memory_segs.iter().collect();
        let merged = Segment::compact_from(&refs);
        let entries_after = merged.len();
        let merged_base = self.make_base_segment(merged);
        self.segments.insert(lo, merged_base);
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
        let segments_merged = hi - lo;
        let entries_before: usize = self.segments[lo..hi].iter().map(|s| s.len()).sum();
        // Collect old mmap paths before draining
        let old_files: Vec<PathBuf> = self.segments[lo..hi].iter().filter_map(|s| {
            if let BaseSegment::Mmap(m) = s { Some(m.path().to_path_buf()) } else { None }
        }).collect();
        // Drain the range and materialize to in-memory for compaction
        let memory_segs: Vec<Segment> =
            self.segments.drain(lo..hi).map(|s| s.into_memory()).collect();
        let refs: Vec<&Segment> = memory_segs.iter().collect();
        let merged = Segment::compact_from(&refs);
        let entries_after = merged.len();
        let merged_base = self.make_base_segment(merged);
        self.segments.insert(lo, merged_base);
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
        });
        self.save_manifest_if_persistent();
        Some(report)
    }

    /// Check the memtable size against `config.memtable_flush_threshold` and
    /// flush if exceeded. Called automatically after `insert_live`.
    fn maybe_flush(&mut self) {
        if self.memtable.len() >= self.config.memtable_flush_threshold {
            self.flush();
        }
    }

    /// THE HOT PATH. Match one title, appending matched logical IDs to `out`.
    /// Probes EVERY segment (all base segments + memtable) and unions the
    /// matched logical ids. `include_broad` controls whether the broad lane is
    /// evaluated inline.
    pub fn match_title(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
        include_broad: bool,
    ) -> MatchStats {
        // per-segment seen-buffer sizing (base segments first, memtable last)
        let n_base = self.segments.len();
        let mut seg_lens: Vec<usize> = Vec::with_capacity(n_base + 1);
        for seg in &self.segments {
            seg_lens.push(seg.len());
        }
        seg_lens.push(self.memtable.len());
        s.ensure(&seg_lens);

        s.epoch = s.epoch.wrapping_add(1);
        if s.epoch == 0 {
            // epoch wrapped: reset all stamps
            for buf in s.seen.iter_mut() {
                for v in buf.iter_mut() {
                    *v = 0;
                }
            }
            s.epoch = 1;
        }
        let epoch = s.epoch;
        out.clear();

        // 1) normalize -> dense feature ids (sorted). Take the buffer out so we
        // can iterate it while mutating `s.seen` (no aliasing, no allocation).
        self.norm
            .match_features(title, &self.dict, &mut s.lc, &mut s.feats);
        let feats = std::mem::take(&mut s.feats);

        // 2) title common-mask word
        let mut tmask = 0u64;
        for &f in &feats {
            let b = self.dict.mask_bit(f);
            if b != crate::dict::NO_MASK_BIT {
                tmask |= 1u64 << b;
            }
        }

        let mut stats = MatchStats::default();

        // 3) probe every base segment, each with its own seen buffer
        for (i, base) in self.segments.iter().enumerate() {
            base.match_into(
                &feats,
                tmask,
                &self.dict,
                epoch,
                &mut s.seen[i],
                out,
                include_broad,
                &mut stats,
            );
        }
        self.memtable.match_into(
            &feats,
            tmask,
            &self.dict,
            epoch,
            &mut s.seen[n_base],
            out,
            include_broad,
            &mut stats,
        );

        // 4) dedup logical ids across segments (a logical id can live in more
        // than one segment, e.g. base + an updated copy in a later segment).
        out.sort_unstable();
        out.dedup();

        // restore the reusable buffer
        s.feats = feats;
        stats.matches = out.len() as u32;
        stats
    }

    /// Parallel matching: match a batch of titles across all available cores.
    /// Returns a Vec of (title_index, matched_logical_ids, stats) tuples.
    /// Each thread gets its own MatchScratch (allocated once, reused across
    /// titles assigned to that thread). The Engine is shared read-only.
    pub fn match_titles_par(
        &self,
        titles: &[impl AsRef<str> + Sync],
        include_broad: bool,
    ) -> Vec<(usize, Vec<u64>, MatchStats)> {
        use rayon::prelude::*;
        titles
            .par_iter()
            .enumerate()
            .map_init(
                || (MatchScratch::new(), Vec::new()),
                |(scratch, out), (idx, title)| {
                    let stats = self.match_title(title.as_ref(), scratch, out, include_broad);
                    (idx, out.clone(), stats)
                },
            )
            .collect()
    }

    /// Parallel matching returning only aggregate stats (no per-title results).
    /// Useful for benchmarks measuring throughput without allocating result vecs.
    pub fn match_titles_par_stats(
        &self,
        titles: &[impl AsRef<str> + Sync],
        include_broad: bool,
    ) -> MatchStats {
        use rayon::prelude::*;
        titles
            .par_iter()
            .map_init(
                || (MatchScratch::new(), Vec::new()),
                |(scratch, out), title| {
                    self.match_title(title.as_ref(), scratch, out, include_broad)
                },
            )
            .reduce(MatchStats::default, |mut a, b| {
                a.unique_candidates += b.unique_candidates;
                a.postings_scanned += b.postings_scanned;
                a.main_candidates += b.main_candidates;
                a.broad_candidates += b.broad_candidates;
                a.matches += b.matches;
                a.probes_attempted += b.probes_attempted;
                a.probes_skipped += b.probes_skipped;
                a
            })
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
        self.segments.iter().map(|s| s.filter_bytes()).sum::<usize>()
    }
    pub fn dict_len(&self) -> usize {
        self.dict.len()
    }

    // ---- persistence helpers ----

    /// Generate the next segment filename and increment the counter.
    fn next_segment_filename(&mut self) -> String {
        let name = format!("seg_{:06}.seg", self.next_seg_id);
        self.next_seg_id += 1;
        name
    }

    /// Seal a segment: if persistent, write to disk and mmap back;
    /// otherwise keep in memory. Pushes onto self.segments.
    fn seal_and_push(&mut self, seg: Segment) {
        let base = self.make_base_segment(seg);
        self.segments.push(base);
    }

    /// Convert a sealed Segment into a BaseSegment (mmap'd if persistent).
    fn make_base_segment(&mut self, seg: Segment) -> BaseSegment {
        let data_dir = self.config.data_dir.clone();
        if let Some(ref dir) = data_dir {
            let name = self.next_segment_filename();
            let seg_dir = dir.join("segments");
            let path = seg_dir.join(&name);
            match crate::storage::write_segment(&seg, &path) {
                Ok(()) => {
                    match MmapSegment::open(&path) {
                        Ok(mmap_seg) => return BaseSegment::Mmap(mmap_seg),
                        Err(e) => {
                            eprintln!("[percolator] segment mmap failed for {:?}, falling back to in-memory: {}", path, e);
                            self.persistence_healthy = false;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[percolator] segment write failed for {:?}, falling back to in-memory: {}", path, e);
                    self.persistence_healthy = false;
                }
            }
            // Fall back to in-memory if write/mmap fails
            BaseSegment::Memory(seg)
        } else {
            BaseSegment::Memory(seg)
        }
    }

    /// Write a WAL flush checkpoint (all prior WAL entries are in segments).
    fn checkpoint_wal(&mut self) {
        if let Some(ref mut wal) = self.wal {
            // Use the latest segment name as the checkpoint marker
            let name = format!("seg_{:06}.seg", self.next_seg_id - 1);
            if let Err(e) = wal.append_flush_checkpoint(&name) {
                eprintln!("[percolator] WAL flush checkpoint write failed: {}", e);
            }
        }
    }

    /// Reset the WAL after a successful flush + manifest write. Only call when
    /// both the checkpoint and manifest have been persisted, so no data is lost.
    fn reset_wal_if_safe(&mut self) {
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.reset() {
                eprintln!("[percolator] WAL reset failed: {}", e);
            }
        }
    }

    /// Save the manifest file if persistence is enabled. Returns true if the
    /// write succeeded (or persistence is not enabled), false on failure.
    fn save_manifest_if_persistent(&mut self) -> bool {
        if let Some(ref dir) = self.config.data_dir {
            let segment_files: Vec<String> = self.segments.iter().filter_map(|s| {
                if let BaseSegment::Mmap(m) = s {
                    m.path().file_name().and_then(|f| f.to_str()).map(|s| s.to_string())
                } else {
                    None
                }
            }).collect();
            let manifest = crate::storage::Manifest {
                segment_files,
                next_seg_id: self.next_seg_id,
                dict_data: crate::storage::serialize_dict(&self.dict),
                rejected_parse: self.rejected_parse,
                rejected_class_d: self.rejected_class_d,
            };
            let dir = dir.clone();
            if let Err(e) = crate::storage::write_manifest(&manifest, &dir.join("manifest.bin")) {
                eprintln!("[percolator] manifest write failed: {}", e);
                self.persistence_healthy = false;
                return false;
            }
        }
        true
    }

    fn save_query_sources(&mut self) {
        if let Some(ref dir) = self.config.data_dir {
            let path = dir.join("sources.dat");
            if let Err(e) = crate::storage::write_query_sources(&self.query_store, &path) {
                eprintln!("[percolator] query sources write failed: {}", e);
                self.persistence_healthy = false;
            }
        }
    }

    /// Collect paths of mmap'd segments (for cleanup during compaction).
    fn collect_mmap_paths(&self) -> Vec<PathBuf> {
        self.segments.iter().filter_map(|s| {
            if let BaseSegment::Mmap(m) = s { Some(m.path().to_path_buf()) } else { None }
        }).collect()
    }

    /// Remove old segment files after compaction replaces them.
    fn cleanup_segment_files(&self, paths: &[PathBuf]) {
        for p in paths {
            let _ = std::fs::remove_file(p);
        }
    }

    /// Replay an insert from WAL recovery (does NOT write back to WAL).
    fn replay_insert(&mut self, text: &str, logical: u64, version: u32) {
        if let Ok(ast) = crate::dsl::parse(text) {
            let mut lc = String::new();
            let ex = extract(&ast, &self.norm, &mut self.dict, &mut lc);
            if self.memtable.add_compiled(&ex, &self.dict, logical, version).is_some() {
                self.query_store.insert(logical, text.to_string());
            }
        }
    }

    /// Replay a tombstone from WAL recovery.
    fn replay_tombstone(&mut self, seg_idx: u32, local_id: u32) {
        if seg_idx == u32::MAX {
            self.memtable.tombstone(local_id);
        } else if let Some(seg) = self.segments.get_mut(seg_idx as usize) {
            seg.tombstone(local_id);
        }
    }
}
