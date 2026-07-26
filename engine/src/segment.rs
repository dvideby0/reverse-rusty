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
mod merge;
mod metrics;
mod persistence;
mod ranked_batch;
mod seg;
mod snapshot;

#[cfg(test)]
mod wal_failure_tests;

mod match_types;
mod outcomes;

pub(in crate::segment) use match_types::DeadlineAt;
pub(crate) use match_types::{
    collect_batch_match, collect_match_at, infallible, DeadlineCheck, DeadlinePoll, NoDeadline,
};
pub use match_types::{
    BatchMatchOptions, BatchResultsWithStats, BroadStrategy, MatchCancelled, MatchStats,
};
pub use outcomes::{
    AliasApplyReport, AliasDiscoveryReport, AliasFeedbackApplyReport, CompactionReport,
    IngestItemStatus, IngestReport, InsertOutcome, UpsertOutcome,
};

/// One immutable (or, for the memtable, mutable) slice of the index. Owns the
/// per-segment SoA + candidate indexes + liveness; the shared dict/norm stay on
/// the Engine. Local ids are segment-local (indexes into this segment's SoA).
///
/// Sealed (immutable) segments carry an anchor filter — a bloom filter over the
/// signature keys present in main + broad indexes. The filter lets `match_into`
/// skip probes that would definitely miss, cutting read amplification when
/// multiple segments exist. The memtable (mutable) has no filter; it's built
/// at seal time (flush / bulk_ingest / compaction).
#[derive(Debug, Clone)]
pub struct Segment {
    main: CandidateIndex,
    broad: CandidateIndex,
    /// The hot tier's candidate index (class H, ADR-105): θ-hot-anchored
    /// queries, probed arity-1 on EVERY request (like main) but evaluated
    /// columnar on the batch path (like broad). An empty map until the first
    /// class-H entry — the structural zero-cost answer for hot-free corpora
    /// (`match_into` skips the whole lane on `hot.num_signatures() == 0`).
    hot: CandidateIndex,
    exact: ExactStore,
    class: Vec<CostClass>,
    alive: Vec<bool>,
    /// O(1) counter of alive (non-tombstoned) entries.
    alive_counter: usize,
    /// O(1) snapshot capability gate. Historical phrase programs remain in the
    /// append-only exact store after deletion, so this counts only live rows.
    live_phrase_predicates: usize,
    /// Anchor filter: present only on sealed (immutable) base segments.
    /// `None` for the memtable (mutable, entries added dynamically).
    filter: Option<SegmentFilter>,
    /// Vocab epoch at which this segment's queries were compiled.
    pub vocab_epoch: u64,
    /// AST→compiled-query lowering semantics baked into this segment. Mechanical
    /// merges preserve the oldest source version; a source-driven recompile
    /// creates a segment at the current version.
    pub(crate) compiler_semantics_version: u32,
    /// Reverse index: logical_id → local_ids in this segment. Enables O(1)
    /// delete lookups instead of full segment scans.
    logical_index: crate::util::FastMap<u64, Vec<u32>>,
    // ---- canonical-body dedup, Stage A (the Broad-Query Cost Program) ----
    /// Per-local body-group leader: `dup_of[i] == i` for a leader (or any entry
    /// ingested with dedup off / attached from disk); a non-leader (duplicate
    /// body) carries its leader's local id and has NO posting entries of its own
    /// — it is reached, verified, and emitted THROUGH its leader. In-memory
    /// only: the on-disk format expands postings back to one entry per member
    /// (no format change — Stage B is the persisted indirection).
    dup_of: Vec<u32>,
    /// Leader → its duplicate members (non-leaders only). Empty map ⇔ this
    /// segment has no shared bodies ⇔ every match path takes the exact
    /// pre-dedup code (the structural zero-cost default).
    dup_members: crate::util::FastMap<u32, Vec<u32>>,
    /// Building-time index: canonical body signature → leader locals with that
    /// signature (collision candidates; equality is confirmed with
    /// `ExactStore::bodies_equal` before any sharing). Unused after sealing.
    body_index: crate::util::FastMap<u64, Vec<u32>>,
}

impl Default for Segment {
    fn default() -> Self {
        Self::new()
    }
}

/// The compile-time knobs `Segment::add_compiled` consults, bundled (they grew
/// past bare-parameter sanity with ADR-105 + dedup Stage A). Construct from the
/// engine config via [`EngineConfig::compile_knobs`](crate::config::EngineConfig::compile_knobs);
/// the WAL-replay / recompile paths override `accept_class_d` per their
/// trust-the-log rules (ADR-068).
#[derive(Clone, Copy, Debug)]
pub struct CompileKnobs {
    /// Store a negation-only (class D) plan as an always-candidate (ADR-068).
    pub accept_class_d: bool,
    /// The hot-anchor threshold θ (class H, ADR-105; 0 = off).
    pub hot_anchor_threshold: u32,
    /// Share identical canonical bodies within the segment (dedup Stage A):
    /// duplicates skip posting insertion and ride their leader's evaluation.
    pub dedup_bodies: bool,
}

/// What [`Segment::add_compiled`] accepted — the per-compile telemetry the
/// `Engine` accumulates (the observe-first hot counter + the dedup sketch).
#[derive(Clone, Copy, Debug)]
pub struct AddedCompiled {
    /// The new segment-local id.
    pub local: u32,
    /// The plan's observe-first hot-tier flag (see `SigPlan::would_be_hot`).
    pub would_be_hot: bool,
    /// The canonical body signature (feeds the engine's duplication sketch).
    pub body_hash: u64,
    /// Whether this entry joined an existing body group as a duplicate.
    pub is_duplicate: bool,
}

/// Which candidate lanes a `match_into` call evaluates INLINE (per title).
/// The main lane (+ the hot tier) is always-visible; `include_broad` carries the
/// documented opt-in broad semantics. `include_hot` exists so the columnar batch
/// driver can lift the hot tier out of the per-title pass (evaluating it once
/// per batch instead — ADR-105); it is a COST switch, never a visibility one:
/// every entry point evaluates the hot tier exactly once, inline or columnar.
#[derive(Clone, Copy, Debug)]
pub struct ProbeLanes {
    /// Evaluate the opt-in broad lane inline (the documented request semantics).
    pub include_broad: bool,
    /// Evaluate the hot tier inline. `true` on every scalar path; the batch
    /// driver passes `false` exactly when its columnar hot pass will run.
    pub include_hot: bool,
}

// ---- BaseSegment: in-memory or mmap'd sealed segment ----

/// A sealed (immutable) base segment, either in-memory or backed by mmap.
/// The memtable is always an in-memory `Segment` (mutable).
// Always heap-allocated behind `Arc` (`Vec<Arc<BaseSegment>>`), so the
// variant-size gap (the Memory variant's inline maps) never rides the stack
// or a dense array — boxing would only add a pointer hop.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub enum BaseSegment {
    Memory(Segment),
    Mmap(MmapSegment),
}

/// Process-local identity for one installed base-segment generation.
///
/// The payload is intentionally empty: pointer identity is the generation.
/// [`SegmentAddress`] keeps an `Arc` to the generation it was resolved against,
/// while the engine keeps one `Arc` per currently installed segment. Replacing a
/// segment installs a fresh generation; ordinary liveness mutations keep it.
pub(in crate::segment) struct SegmentGeneration;

pub(in crate::segment) fn fresh_segment_generation() -> Arc<SegmentGeneration> {
    Arc::new(SegmentGeneration)
}

pub(in crate::segment) fn fresh_segment_generations(count: usize) -> Vec<Arc<SegmentGeneration>> {
    (0..count).map(|_| fresh_segment_generation()).collect()
}

/// An opaque, process-local address for a row in one base-segment generation.
///
/// Acquire this token with [`Engine::segment_address`] when the physical row is
/// resolved, then pass it to [`Engine::tombstone_in`]. It cannot be reconstructed
/// from a bare `(segment, local_id)` pair: compaction may reuse both numbers for a
/// different query. Tokens do not survive engine reopen or replacement of their
/// segment; either case returns
/// [`TombstoneError::StaleAddress`](crate::error::TombstoneError::StaleAddress)
/// before the WAL is touched.
#[derive(Clone)]
pub struct SegmentAddress {
    pub(in crate::segment) generation: Arc<SegmentGeneration>,
    pub(in crate::segment) segment: usize,
    pub(in crate::segment) local_id: u32,
    pub(in crate::segment) logical_id: u64,
}

impl SegmentAddress {
    /// Segment ordinal when the token was resolved. The current ordinal may
    /// differ if compaction moved an unchanged neighboring segment.
    #[must_use]
    pub fn segment_ordinal(&self) -> usize {
        self.segment
    }

    /// Row-local id within the addressed segment generation.
    #[must_use]
    pub fn local_id(&self) -> u32 {
        self.local_id
    }

    /// Stable logical query id captured when the token was resolved.
    #[must_use]
    pub fn logical_id(&self) -> u64 {
        self.logical_id
    }
}

impl std::fmt::Debug for SegmentAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentAddress")
            .field("segment", &self.segment)
            .field("local_id", &self.local_id)
            .field("logical_id", &self.logical_id)
            .finish_non_exhaustive()
    }
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
    /// Candidate-only positive labels. Phrase graphs can contribute retrieval
    /// labels that must never widen ordinary flat exact semantics.
    probe_feats: Vec<FeatureId>,
    /// Canonical / positive analyzed token-graph edges, populated only while
    /// quoted predicates exist in the snapshot (ADR-120).
    phrase_arcs: Vec<crate::normalize::PositionArc>,
    phrase_arcs_pos: Vec<crate::normalize::PositionArc>,
    /// Reusable graph-intersection state, interior-mutable so the borrowed
    /// `TitleView` stays `Copy` through every segment verifier.
    phrase_match: std::cell::RefCell<crate::exact::PhraseMatchScratch>,
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
    /// Aggregate read capability captured at publication. Checking this once
    /// here avoids walking every base segment for every matched title.
    has_phrase_predicates: bool,
    query_store: Arc<SourceStore>,
    /// Vocabulary at snapshot time (shared via `Arc`), so vocab reads can use the
    /// lock-free snapshot instead of locking the engine (ADR-016).
    vocab: Option<Arc<crate::vocab::Vocab>>,
    /// Engine configuration at snapshot time (shared via `Arc`), so `GET /_settings`
    /// reads it lock-free like every other read endpoint (ADR-016).
    config: Arc<EngineConfig>,
    rejected_parse: u64,
    rejected_class_d: u64,
    /// Observe-first hot-tier telemetry at snapshot time — see the `Engine` field.
    would_be_hot: u64,
    /// Dedup Stage A telemetry at snapshot time — see the `Engine` fields.
    bodies_total: u64,
    dup_joined: u64,
    /// Linear-counting estimate of distinct canonical bodies at snapshot time
    /// (0 until the first accepted compile).
    distinct_bodies_est: u64,
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
#[derive(Clone)]
pub struct PlacedQuery {
    /// Stable cross-shard logical id of the query.
    pub logical: u64,
    /// Features the coordinator extracted read-only against the shared frozen dict.
    pub ex: Extracted,
    /// Raw query DSL / source text (stored in the query store; the replayable source of truth).
    pub dsl: String,
    /// Engine version tag (1 for in-process shards).
    pub version: u32,
    /// Internal source-generation identity to preserve during a blue/green
    /// rebuild. Fresh build/ingest callers pass `None` and receive a newly
    /// allocated generation; rebuilds pass `Some` so the exact row and its
    /// canonical source document keep the same identity.
    pub source_generation: Option<u64>,
    /// Raw `(key, value)` metadata tags; resolved to `TagId`s read-only at ingest. Empty ⇒ untagged.
    pub tags: Vec<(String, String)>,
    /// Pre-resolved `TagId`s carried through a blue/green vocabulary rebuild (ADR-074): the tag
    /// space is preserved across a vocab change, so a stored id — interned dense or post-freeze
    /// synthetic — stays valid and is carried verbatim (the cluster analogue of the single-node
    /// ADR-049 carry-through in `recompile_stale_segments`). Unioned with the resolved `tags` at
    /// ingest. In-process only: a synthetic id has no recoverable string, so this never crosses
    /// the dict-agnostic gRPC wire (`RemoteShard::ingest_extracted` fails loud). Empty ⇒ unused.
    pub tag_ids: Vec<crate::tagdict::TagId>,
    /// Fixed typed rank values carried across in-process rebuild/resize. The
    /// distributed wire remains unchanged in Increment 2 and supplies zero.
    pub rank: crate::rank::RankValues,
    /// Deterministic distributed emission placement (ADR-109). Standalone
    /// engine callers use [`QueryPlacement::standalone`](crate::ownership::QueryPlacement::standalone).
    pub placement: crate::ownership::QueryPlacement,
}

/// One accepted query whose source document is published after its match data
/// reaches the durable commit point. Internal to the segment controller.
pub(in crate::segment) struct AcceptedSource {
    logical: u64,
    text: String,
    version: u32,
    source_generation: u64,
    tags: Vec<(String, String)>,
    tags_known: bool,
}

impl AcceptedSource {
    pub(in crate::segment) fn known(
        logical: u64,
        text: String,
        version: u32,
        source_generation: u64,
        tags: Vec<(String, String)>,
    ) -> Self {
        Self {
            logical,
            text,
            version,
            source_generation,
            tags,
            tags_known: true,
        }
    }

    pub(in crate::segment) fn with_tag_status(
        logical: u64,
        text: String,
        version: u32,
        source_generation: u64,
        tags: Vec<(String, String)>,
        tags_known: bool,
    ) -> Self {
        Self {
            logical,
            text,
            version,
            source_generation,
            tags,
            tags_known,
        }
    }
}

/// One live source row returned by [`Engine::live_sources_tagged`]:
/// `(logical, dsl, version, tag_ids, rank, placement)`.
///
/// Naming the tuple preserves the established public representation while
/// keeping internal signatures readable.
pub type LiveTaggedSource = (
    u64,
    String,
    u32,
    Vec<crate::tagdict::TagId>,
    crate::rank::RankValues,
    crate::ownership::QueryPlacement,
);

/// Internal source-document gather used by cluster rebuilds and content
/// fingerprints. It extends [`LiveTaggedSource`] with the internal source
/// generation and canonical raw tags.
pub(crate) type LiveSourceDocument = (
    u64,
    String,
    u32,
    u64,
    Vec<(String, String)>,
    Vec<crate::tagdict::TagId>,
    crate::rank::RankValues,
    crate::ownership::QueryPlacement,
);

/// Boxed observer callback for engine events.
type EventObserver = Box<dyn Fn(&crate::events::EngineEvent) + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::segment) enum SourceCommitState {
    Ready,
    IncompleteRecovery,
}

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
    /// Process-local identity paired positionally with `segments`. Replacements
    /// receive fresh identities; unchanged segments retain theirs even if their
    /// ordinal shifts. This is the generation fence behind `SegmentAddress`.
    segment_generations: Vec<Arc<SegmentGeneration>>,
    /// Segment generations in the exact order named by the latest successful
    /// standalone manifest commit. Memory-only fallback segments are absent.
    ///
    /// A positional WAL frame must use an ordinal from this vector, never the
    /// possibly-ahead live `segments` layout. The two differ after a failed
    /// flush/recompile commit and can also differ when a later durable segment
    /// follows an uncommitted memory fallback.
    committed_segment_generations: Vec<Arc<SegmentGeneration>>,
    /// mutable hot delta — insert_live / tombstone land here. `Arc` + CoW: a
    /// write clones only the (bounded) memtable, never the base segments.
    memtable: Arc<Segment>,
    /// Number of segment states (including the memtable) with a live phrase row.
    /// Updated on writes and reduced to an O(1) capability bit in snapshots.
    live_phrase_segments: usize,
    rejected_parse: u64,   // queries dropped because the DSL failed to parse
    rejected_class_d: u64, // class-D queries rejected at compile (not stored)
    /// Observe-first hot-tier telemetry (the Broad-Query Cost Program): accepted
    /// compiles whose plan reported
    /// [`would_be_hot`](crate::compile::SigPlan::would_be_hot) — main-lane
    /// queries that would reclassify to the hot tier under the default θ. A
    /// process-lifetime event counter (counts compile events incl. WAL replay and
    /// vocab recompiles, not distinct stored queries); deliberately NOT persisted
    /// in the manifest, so hot-free corpora keep their manifest bytes unchanged.
    would_be_hot: u64,
    /// Dedup Stage A observe telemetry (process-lifetime event counters, the
    /// `would_be_hot` discipline): accepted compile events and how many of them
    /// joined an existing body group in their segment. Not persisted.
    bodies_total: u64,
    dup_joined: u64,
    /// Linear-counting sketch of DISTINCT canonical bodies seen (2^22 bits =
    /// 512 KiB, lazily allocated on the first accepted compile). Measures GLOBAL
    /// duplication — the cross-segment potential Stage A's per-segment groups
    /// cannot reach (the Stage B sizing evidence). Fed on every accepted
    /// compile regardless of `dedup_bodies` (observe-first).
    dup_sketch: Option<Box<[u64]>>,
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
    /// Next non-zero internal generation used to couple an exact row to its
    /// canonical source document. Independent of caller-visible `_version`.
    next_source_generation: u64,
    /// Health flag: false if a WAL write has failed (durability degraded).
    pub wal_healthy: bool,
    /// Health flag: false if a manifest or segment file write has failed.
    pub persistence_healthy: bool,
    /// Number of corrupt segments skipped during Engine::open().
    pub skipped_segments: usize,
    /// Maps logical_id → original query text for retrieval and search hit
    /// enrichment. Shared (not copied) into every snapshot — see [`SourceStore`].
    query_store: Arc<SourceStore>,
    /// Basename of the durable source sidecar selected by the owning commit
    /// point. Standalone engines and ordinary shards use `sources.dat`; cluster
    /// blue/green rebuilds use a generation-specific name so the coordinator
    /// manifest selects the new source corpus atomically with its segment
    /// registry.
    source_file_name: String,
    /// Whether this process has a complete source baseline from which it may
    /// publish another standalone source generation. A missing/corrupt selected
    /// sidecar or failed post-commit lazy remap clears this fence; restart/repair
    /// must restore the selected corpus before any later manifest can replace it
    /// with an accidentally partial snapshot.
    source_commit_state: SourceCommitState,
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
            .field("would_be_hot", &self.would_be_hot)
            .field("bodies_total", &self.bodies_total)
            .field("dup_joined", &self.dup_joined)
            .field("has_observer", &self.observer.is_some())
            .field("pending_events", &self.pending_events.len())
            .field("has_wal", &self.wal.is_some())
            .field("next_seg_id", &self.next_seg_id)
            .field("wal_healthy", &self.wal_healthy)
            .field("persistence_healthy", &self.persistence_healthy)
            .field("skipped_segments", &self.skipped_segments)
            .field("query_store_entries", &self.query_store.len())
            .field("source_commit_state", &self.source_commit_state)
            .field("vocab_epoch", &self.vocab_epoch)
            .field("owns_manifest", &self.owns_manifest)
            .finish()
    }
}
