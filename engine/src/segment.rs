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
//!
//! This file holds the data-type *definitions* shared across the module; their
//! `impl` blocks live in focused submodules so each concern is self-contained:
//!   - [`seg`]         — `impl Segment` (the in-memory / memtable slice)
//!   - [`base`]        — `impl BaseSegment` (in-memory vs mmap dispatch)
//!   - [`snapshot`]    — `MatchScratch` + `EngineSnapshot` (the lock-free read path)
//!   - [`lifecycle`]   — `Engine` construction, recovery, vocab, observer, accessors
//!   - [`ingest`]      — `Engine` write path (build / insert / tombstone / bulk / replay)
//!   - [`compaction`]  — `Engine` flush + LSM compaction
//!   - [`matching`]    — `Engine` hot-path matchers
//!   - [`persistence`] — `Engine` durability (segment files, WAL checkpoint, manifest)
//!   - [`metrics`]     — `Engine` introspection (metrics snapshot, byte accounting)

use std::sync::Arc;

use crate::compile::{CostClass, Extracted};
use crate::config::EngineConfig;
use crate::dict::{Dict, FeatureId};
use crate::exact::ExactStore;
use crate::filter::SegmentFilter;
use crate::index::CandidateIndex;
use crate::normalize::Normalizer;
use crate::tagdict::TagDict;
// `SourceStore` backs the per-query source text (`logical_id → original query
// text`), shared via `Arc` between the engine and every published snapshot.
// Display-only — it enriches search hits and feeds `explain`, and never touches
// the integer match path. Fully resident, or lazily mmap'd from `sources.dat`
// per `EngineConfig::retain_source` (ADR-020 Item 1). Publishing a snapshot is an
// `Arc::clone`, not an O(corpus) copy; reads/writes are eventually consistent
// across snapshots, which is fine for display.
use crate::storage::{MmapSegment, SourceStore};
use crate::wal::Wal;

mod base;
mod broad_batch;
mod compaction;
mod ingest;
mod lifecycle;
mod matching;
mod metrics;
mod persistence;
mod seg;
mod snapshot;

#[cfg(test)]
mod wal_failure_tests;

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct MatchStats {
    pub unique_candidates: u32, // distinct queries exact-checked
    pub postings_scanned: u32,  // total posting entries unioned (main + broad)
    /// Broad-lane subset of `postings_scanned` — the quantity the columnar batch
    /// path amortizes (each huge broad posting is scanned once per batch, not
    /// once per title). Counted on BOTH paths, so `broad_postings_scanned`
    /// columnar ÷ inline is the machine-independent amortization factor.
    pub broad_postings_scanned: u32,
    pub main_candidates: u32,
    pub broad_candidates: u32,
    pub matches: u32,
    pub probes_attempted: u32, // total signature probes (before filter)
    pub probes_skipped: u32,   // probes skipped by anchor filter (definitely-not-present)
    // ---- broad-lane batch/columnar accounting (0 on the per-title path) ----
    pub broad_queries_evaluated: u32, // distinct broad queries exact-checked via bitmap eval
    pub broad_anchors_scanned: u32,   // distinct broad anchors (postings) probed per batch
    pub broad_batches: u32,           // broad sub-batches (chunks) processed
}

impl MatchStats {
    /// Field-wise accumulate `other` into `self`. The single shared body for
    /// merging per-title stats in the parallel matchers and per-shard stats in
    /// the cluster coordinator. `matches` is summed like the rest; callers that
    /// dedup across sources (e.g. the cluster union) overwrite it afterward.
    pub fn merge(&mut self, other: MatchStats) {
        self.unique_candidates += other.unique_candidates;
        self.postings_scanned += other.postings_scanned;
        self.broad_postings_scanned += other.broad_postings_scanned;
        self.main_candidates += other.main_candidates;
        self.broad_candidates += other.broad_candidates;
        self.matches += other.matches;
        self.probes_attempted += other.probes_attempted;
        self.probes_skipped += other.probes_skipped;
        self.broad_queries_evaluated += other.broad_queries_evaluated;
        self.broad_anchors_scanned += other.broad_anchors_scanned;
        self.broad_batches += other.broad_batches;
    }
}

/// Which broad-lane strategy a batch match uses. `Columnar` is the new
/// once-per-batch bitmap evaluator; `Inline` falls back to the original
/// per-title broad probe (`Segment::match_into(include_broad=true)`) — the
/// provable kill-switch that yields byte-identical results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BroadStrategy {
    Inline,
    Columnar,
}

/// Options for batch matching. Replaces the bare `include_broad: bool` on the
/// batch entry points without churning the per-title signatures.
#[derive(Clone, Copy, Debug)]
pub struct BatchMatchOptions {
    /// Evaluate the broad lane at all (default false — broad is opt-in, as on
    /// the per-title path).
    pub include_broad: bool,
    /// Title sub-batch / rayon chunk size for the columnar broad pass.
    pub broad_batch_size: usize,
    /// Columnar (new) vs Inline (original per-title broad) — the kill-switch.
    pub broad_strategy: BroadStrategy,
    /// Use the pure-anchor materialization fast path (emit pure-anchor broad
    /// queries straight from the anchor's title bitmap, skipping verification).
    /// When false, those queries go through full bitmap verification instead —
    /// identical results, slower. A kill-switch for the optimization; only
    /// consulted on the [`BroadStrategy::Columnar`] path.
    pub broad_materialize: bool,
}

impl Default for BatchMatchOptions {
    fn default() -> Self {
        Self {
            include_broad: false,
            broad_batch_size: 256,
            broad_strategy: BroadStrategy::Columnar,
            broad_materialize: true,
        }
    }
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
#[derive(Default, Debug, Clone)]
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

// ---- BaseSegment: in-memory or mmap'd sealed segment ----

/// A sealed (immutable) base segment, either in-memory or backed by mmap.
/// The memtable is always an in-memory `Segment` (mutable).
#[derive(Clone)]
pub enum BaseSegment {
    Memory(Segment),
    Mmap(MmapSegment),
}

/// Reusable per-thread scratch — keeps the hot path allocation-free in steady
/// state. `seen` is now per-segment: `seen[seg_idx]` is that segment's epoch
/// stamp array, sized to that segment's `len()`. Buffers are reused across calls.
#[derive(Debug)]
pub struct MatchScratch {
    lc: String,
    /// Negative / canonical leftmost-longest title view `N(T)` (ADR-061). Also the single
    /// view when no multi-word alias is active.
    feats: Vec<FeatureId>,
    /// Positive overlapping superset title view `P(T) ⊇ N(T)` (ADR-061). Equal to `feats`
    /// when no multi-word alias is active.
    feats_pos: Vec<FeatureId>,
    /// Reusable per-title working buffers for the normalizer's `emit` pipeline — keeps title
    /// normalization allocation-free in steady state (the hot-path invariant). Owned here, like
    /// `lc`/`feats`, so it persists across titles instead of being re-allocated per `emit`.
    norm: crate::normalize::NormScratch,
    seen: Vec<Vec<u32>>,
    epoch: u32,
}

// ---------------------------------------------------------------------------
// EngineSnapshot — immutable, lock-free read view
// ---------------------------------------------------------------------------

/// An immutable, `Send + Sync` snapshot of the engine's read-path state.
///
/// Readers acquire a snapshot via [`Engine::snapshot`] and perform matching
/// without holding any lock.  Writers atomically publish new snapshots after
/// mutations (see the server's `ArcSwap<EngineSnapshot>` pattern).
///
/// The snapshot is genuinely cheap to create: every large structure is shared
/// structurally, so publishing is a handful of `Arc::clone`s, not a deep copy of
/// the engine (see ADR-016). The dictionary and memtable are copy-on-write
/// (`Arc<Dict>` / `Arc<Segment>`), the base-segment list shares each segment
/// (`Arc<BaseSegment>`), and the query store is shared behind an `RwLock`.
pub struct EngineSnapshot {
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    /// Tag dictionary at snapshot time (shared via `Arc`), so the read path resolves a
    /// request's `(key,value)` filter terms to `TagId`s lock-free (ADR-049).
    tag_dict: Arc<TagDict>,
    segments: Vec<Arc<BaseSegment>>,
    memtable: Arc<Segment>,
    query_store: Arc<SourceStore>,
    /// Vocabulary at snapshot time (shared via `Arc`), so vocab reads can use the
    /// lock-free snapshot instead of locking the engine (ADR-016).
    vocab: Option<Arc<crate::vocab::Vocab>>,
    /// Engine configuration at snapshot time (shared via `Arc`), so `GET /_settings`
    /// reads it lock-free like every other read endpoint (ADR-016).
    config: Arc<EngineConfig>,
    rejected_parse: u64,
    rejected_class_d: u64,
    vocab_epoch: u64,
    wal_healthy: bool,
    persistence_healthy: bool,
    skipped_segments: usize,
    wal_size_bytes: u64,
    wal_pending_entries: u64,
}

/// One pre-extracted query ready for the cluster bulk-ingest path
/// ([`Engine::ingest_extracted`]). The coordinator extracts features read-only against the shared
/// frozen dict, buckets these by placement, and hands a slice to each shard; the shard's engine
/// resolves `tags` read-only against the shared frozen [`TagDict`](crate::tagdict::TagDict)
/// (`get_or_synthetic`, never `intern` — dense ids would diverge per shard, ADR-055). Lives in the
/// engine layer (not `cluster`) because the engine's ingest path consumes it, by reference, with no
/// conversion. `tags` empty ⇒ untagged ⇒ byte-identical to the pre-tag path.
pub struct PlacedQuery {
    /// Stable cross-shard logical id of the query.
    pub logical: u64,
    /// Features the coordinator extracted read-only against the shared frozen dict.
    pub ex: Extracted,
    /// Raw query DSL / source text (stored in the query store; the replayable source of truth).
    pub dsl: String,
    /// Engine version tag (1 for in-process shards).
    pub version: u32,
    /// Raw `(key, value)` metadata tags; resolved to `TagId`s read-only at ingest. Empty ⇒ untagged.
    pub tags: Vec<(String, String)>,
    /// Pre-resolved `TagId`s carried through a blue/green vocabulary rebuild (ADR-074): the tag
    /// space is preserved across a vocab change, so a stored id — interned dense or post-freeze
    /// synthetic — stays valid and is carried verbatim (the cluster analogue of the single-node
    /// ADR-049 carry-through in `recompile_stale_segments`). Unioned with the resolved `tags` at
    /// ingest. In-process only: a synthetic id has no recoverable string, so this never crosses
    /// the dict-agnostic gRPC wire (`RemoteShard::ingest_extracted` fails loud). Empty ⇒ unused.
    pub tag_ids: Vec<crate::tagdict::TagId>,
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

/// Outcome of an alias import / learn-and-apply (ADR-060): how many groups switched to active,
/// how many stored queries were recompiled so the change took effect (zero false negatives), and
/// the registry's resulting status counts. Returned by [`Engine::import_alias_synonyms`] /
/// [`Engine::learn_aliases_and_apply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AliasApplyReport {
    /// Groups newly switched to active by this call.
    pub activated: usize,
    /// Stored queries recompiled so the change took effect immediately (zero false negatives).
    pub recompiled: usize,
    /// The registry's status counts after applying.
    pub summary: crate::vocab::AliasSummary,
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

/// Outcome of an atomic upsert (replace-by-id, ADR-067). Distinguishes a fresh
/// registration from a replacement so the HTTP layer can answer ES-style
/// (201-created vs 200-updated). A parse failure is surfaced as
/// `Err(ParseError)` by [`Engine::try_upsert_live`], never folded in here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertOutcome {
    /// No prior live copy existed; inserted fresh. Carries the memtable-local id.
    Created(u32),
    /// Inserted the new version and tombstoned `replaced` prior live copies in
    /// the same critical section (one WAL frame, one snapshot publish).
    Updated { local: u32, replaced: usize },
    /// The NEW version compiled to cost-class D and was rejected — the prior
    /// live copies are left untouched (a failed replace never deletes, matching
    /// ES `index` semantics where a failed op leaves the old document).
    RejectedClassD,
}

/// Per-item outcome for one query in a bulk batch, returned in submission order
/// by [`Engine::try_bulk_ingest_detailed`]. Lets a caller (e.g. the HTTP
/// `/_bulk` handler) report exactly which items were rejected and why — ES-style
/// per-item status — rather than only an aggregate count that hides *which*
/// queries were dropped. The variant tallies match the aggregate [`IngestReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestItemStatus {
    /// Compiled and stored in the new base segment.
    Ingested,
    /// The DSL string failed to parse; carries the diagnostic so the caller can
    /// echo the same detail the single-doc path returns.
    RejectedParse(crate::error::ParseError),
    /// Compiled but rejected as cost-class D — no anchorable feature, not stored.
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
    /// Number of queries whose signature cover was re-anchored during the merge
    /// (ADR-056). Always `0` unless `compaction_reanchor` is enabled, and `0` in a
    /// cluster shard (frozen dict ⇒ no frequency drift ⇒ no anchor change).
    pub reanchored: usize,
}

/// Boxed observer callback for engine events.
type EventObserver = Box<dyn Fn(&crate::events::EngineEvent) + Send + Sync>;

pub struct Engine {
    /// Runtime configuration. `Arc` so the current settings ride in every
    /// `EngineSnapshot` (an O(1) clone), letting `GET /_settings` read them from
    /// the lock-free snapshot; `set_config` swaps in a new `Arc` (copy-on-write).
    config: Arc<EngineConfig>,
    norm: Arc<Normalizer>,
    /// Vocabulary used to build the normalizer (if set via `with_vocab`).
    /// `Arc` so it is shared (not deep-copied) into every `EngineSnapshot`,
    /// letting `GET /_vocab` read it from the lock-free snapshot instead of the
    /// write mutex (ADR-016).
    vocab: Option<Arc<crate::vocab::Vocab>>,
    /// Feature dictionary. `Arc` so a snapshot shares it; writers take a
    /// copy-on-write handle via `Arc::make_mut` (the dict is O(vocab), which
    /// saturates, so the occasional CoW clone is bounded — not O(corpus)).
    dict: Arc<Dict>,
    /// Per-query metadata tag dictionary (ADR-049). `Arc` + CoW exactly like `dict`:
    /// a snapshot shares it; a tagged write interns new `(key,value)`s via
    /// `Arc::make_mut`. Empty until the first tagged query is stored.
    tag_dict: Arc<TagDict>,
    /// immutable base segments (sealed; never mutated after creation). Each
    /// segment is behind `Arc` so publishing a snapshot shares them by pointer
    /// instead of deep-copying every segment's SoA arrays (ADR-016 / P1-16).
    segments: Vec<Arc<BaseSegment>>,
    /// mutable hot delta — insert_live / tombstone land here. `Arc` + CoW: a
    /// write clones only the (bounded) memtable, never the base segments.
    memtable: Arc<Segment>,
    rejected_parse: u64,   // queries dropped because the DSL failed to parse
    rejected_class_d: u64, // class-D queries rejected at compile (not stored)
    /// Optional observer callback for engine events (flush, compact, ingest, etc.)
    observer: Option<EventObserver>,
    /// Events emitted during construction/recovery (`with_config`/`open`), before
    /// an observer could be attached. Delivered to the observer when `set_observer`
    /// is called, then cleared. Only construction-time `DurabilityFailure`s land
    /// here (a bounded handful); the runtime `emit` path drops events when no
    /// observer is set, exactly as before.
    pending_events: Vec<crate::events::EngineEvent>,
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
    /// Maps logical_id → original query text for retrieval and search hit
    /// enrichment. Shared (not copied) into every snapshot — see [`SourceStore`].
    query_store: Arc<SourceStore>,
    /// Monotonic counter incremented on each `set_vocab()` call. Segments compiled
    /// at an earlier epoch are stale (their normalizer differs from the current one).
    vocab_epoch: u64,
    /// Whether this engine writes its own `manifest.bin`. True for a standalone
    /// engine. False for a **cluster shard** (ADR-032): the coordinator's
    /// `cluster_manifest.bin` is the sole metadata authority (it records the
    /// per-shard segment registry + the one shared dict), so a shard suppresses its
    /// own manifest — segment `.seg` files are still written, but no per-shard dict
    /// copy. Such an engine is opened via [`Engine::open_shared_segments`], not
    /// [`Engine::open`].
    owns_manifest: bool,
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
            .field("pending_events", &self.pending_events.len())
            .field("has_wal", &self.wal.is_some())
            .field("next_seg_id", &self.next_seg_id)
            .field("wal_healthy", &self.wal_healthy)
            .field("persistence_healthy", &self.persistence_healthy)
            .field("skipped_segments", &self.skipped_segments)
            .field("query_store_entries", &self.query_store.len())
            .field("vocab_epoch", &self.vocab_epoch)
            .field("owns_manifest", &self.owns_manifest)
            .finish()
    }
}
