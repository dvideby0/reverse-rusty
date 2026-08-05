//! `HandoffShard` — a [`Shard`] wrapper whose backing can be ATOMICALLY RE-POINTED at
//! runtime (clustering build-path step 6a / ADR-043): the swappable-backing mechanism
//! behind a live shard handoff (serve-then-drop + the epoch-fence stamp).
//!
//! Design: docs/design/clustering-and-scaling.md §9 (serve-then-drop + epoch fencing) and
//! §4.3 (the allocator decides the shard→node map; this module is half of "peer recovery
//! moves the bytes" — the routing flip the move re-points). The cross-node move + fence +
//! drop that *drive* a swap are step 6b (`ClusterEngine::execute_handoff`, ADR-044).
//!
//! ## Why a wrapper (not a swappable `ClusterEngine.shards`)
//! The coordinator routes by ring POSITION into `shards: Vec<Box<dyn Shard>>` and never reads
//! the control-plane shard→node map on the hot path, so an in-process handoff is a no-op for
//! matching — the capability is only meaningful over gRPC (a position's [`RemoteShard`] is
//! re-pointed at a new owner). Rather than widen `shards` to `Vec<ArcSwap<…>>` (which would add
//! an `ArcSwap::load` to the lean in-process hot path for EVERY cluster, breaking the
//! byte-identical default), one position opts in by wrapping its backing in a `HandoffShard`.
//! The whole module is `distributed`-gated, so the lean core and the in-process/RF=1 default
//! path never compile it and stay byte-identical. Mirrors the [`ReplicatedShard`] composite.
//!
//! [`RemoteShard`]: super::remote::RemoteShard
//! [`ReplicatedShard`]: super::replica::ReplicatedShard
//!
//! ## Serve-then-drop, for free
//! `current` is an [`ArcSwap`] over the live backing. A probe loads a `Guard` (the *old* backing)
//! and completes against it even if a concurrent [`HandoffShard::swap_backing`] re-points the slot;
//! the old backing drops only once the last in-flight `Guard` releases. No read-path lock — safe
//! under the coordinator's rayon probe fan-out. This is the §9 "old owner serves until handoff
//! completes" property, the same `arc_swap` pattern [`LocalShard`] uses for its snapshot and
//! `ShardServer` uses for its served state.
//!
//! [`LocalShard`]: super::shard::LocalShard
//!
//! ## Representation note
//! The slot holds an `Arc<Box<dyn Shard>>` (`ArcSwap<Box<dyn Shard>>`), NOT an `Arc<dyn Shard>`:
//! `arc_swap`'s `RefCnt` is implemented only for `Arc<T: Sized>`, and `dyn Shard` is unsized — but
//! a `Box<dyn Shard>` is a Sized fat pointer, so `Arc<Box<dyn Shard>>` qualifies. Auto-deref still
//! reaches `dyn Shard` for the method forwards, so the indirection is invisible.
//!
//! ## The generation (epoch-fence stamp)
//! Each swap stamps a `generation` — the committed control-plane epoch ([`ClusterState::epoch`])
//! the new backing was installed under. It is INERT in step 6a (nothing compares it yet) but is
//! the fence token step 6b reads to tell a demoted owner "you are fenced at generation N" before
//! dropping it. It is published with `Release` AFTER the backing store, so a reader/fencer that
//! `Acquire`-observes the new generation is guaranteed to also observe the new backing (no window
//! where the fence says "demoted" while reads still hit the old backing).
//!
//! [`ClusterState::epoch`]: super::control::ClusterState::epoch

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::compile::Extracted;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::segment::{IngestReport, MatchStats, PlacedQuery};
use crate::tagdict::TagDict;

use super::clog::{ClusterMutation, LogPos};
use super::shard::{EventSink, FetchedMatch, Shard, ShardError, ShardRankedMatch};

/// A [`Shard`] whose backing is one boxed shard that can be atomically replaced at runtime.
///
/// Stored two ways that share one object: as the `i`-th `Box<dyn Shard>` in the coordinator's
/// `shards` (so reads/writes route through it transparently) AND as a typed `Arc<HandoffShard>`
/// handle in the coordinator's per-position side-table (so step 6b can call the inherent
/// [`swap_backing`](Self::swap_backing) without downcasting `dyn Shard`). [`wrap_handoff`] builds
/// both views from one allocation, guaranteeing they stay in lock-step.
pub(crate) struct HandoffShard {
    /// The live backing. Reads/writes load it lock-free; a swap re-points it atomically. Holds a
    /// `Box<dyn Shard>` (Sized) so `Arc<_>` satisfies `arc_swap`'s `RefCnt` (see the module docs).
    current: ArcSwap<Box<dyn Shard>>,
    /// The committed control-plane epoch the current backing was installed under — the fence
    /// stamp. Read in step 6a only by `ClusterEngine::handoff_generations` (introspection);
    /// consumed for real by step 6b's `execute_handoff`.
    generation: AtomicU64,
}

impl HandoffShard {
    /// Wrap an initial backing, stamping the `generation` it is installed under.
    pub(crate) fn new(initial: Box<dyn Shard>, generation: u64) -> Self {
        HandoffShard {
            current: ArcSwap::from_pointee(initial),
            generation: AtomicU64::new(generation),
        }
    }

    /// Atomically re-point the live backing at `new` and stamp the fence `generation`.
    ///
    /// Ordering is load-bearing: store the backing FIRST, then publish the generation with
    /// `Release`, so any reader/fencer that `Acquire`-loads the new generation also observes the
    /// new backing. Infallible (a pointer swap + an atomic store). In-flight probes against the
    /// previous backing complete correctly — the old `Arc` lives until the last `Guard` drops.
    /// The production caller is `ClusterEngine::execute_handoff` (ADR-044, step 6b).
    pub(crate) fn swap_backing(&self, new: Box<dyn Shard>, generation: u64) {
        self.current.store(Arc::new(new));
        self.generation.store(generation, Ordering::Release);
    }

    /// The generation the current backing was installed under (the epoch-fence stamp Stage 6b
    /// reads to fence the demoted owner). Surfaced via `ClusterEngine::handoff_generations`.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

/// Build the two index-aligned views of one `HandoffShard` for a coordinator position: the
/// `Box<dyn Shard>` that goes into `shards[i]` and the typed `Arc<HandoffShard>` handle that goes
/// into the handoff side-table. Both share the single inner `HandoffShard`, so a `swap_backing`
/// through the handle is immediately visible to reads going through `shards[i]`. `gen0` is the
/// control-plane epoch the position is initially assigned under (0 at connect today).
pub(crate) fn wrap_handoff(
    backing: Box<dyn Shard>,
    gen0: u64,
) -> (Box<dyn Shard>, Arc<HandoffShard>) {
    let handle = Arc::new(HandoffShard::new(backing, gen0));
    (Box::new(Arc::clone(&handle)) as Box<dyn Shard>, handle)
}

/// `Shard` is implemented on `Arc<HandoffShard>` (NOT the bare type) so the SAME `Arc` can be
/// cloned into both `shards[i]` (boxed) and the typed side-table — the property [`wrap_handoff`]
/// relies on. Every method forwards to the currently-installed backing (auto-deref carries the
/// `Guard<Arc<Box<dyn Shard>>>` through to `dyn Shard`).
///
/// IMPORTANT: forward EVERY `Shard` method, including the ones with trait *defaults*
/// (`add_recovered_replica`, `set_event_sink`, the retention leases). Omitting one would silently
/// fall back to the default (e.g. `add_recovered_replica` errors, `set_event_sink` no-ops) — the
/// wrong behavior for a wrapped [`ReplicatedShard`](super::replica::ReplicatedShard). When the
/// trait gains a method, add a forward here (the `forwards_defaulted_methods_to_backing` test is
/// the regression guard).
impl Shard for Arc<HandoffShard> {
    fn percolate_filtered(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.current
            .load()
            .percolate_filtered(title, include_broad, pred)
    }

    fn percolate_filtered_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.current.load().percolate_filtered_owned(
            title,
            include_broad,
            pred,
            context,
            current_position,
        )
    }

    fn percolate_filtered_ranked(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.current
            .load()
            .percolate_filtered_ranked(title, include_broad, pred, spec)
    }

    fn percolate_filtered_ranked_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.current.load().percolate_filtered_ranked_owned(
            title,
            include_broad,
            pred,
            spec,
            context,
            current_position,
        )
    }

    fn percolate_top_k_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<std::time::Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        self.current.load().percolate_top_k_owned(
            title,
            include_broad,
            pred,
            program,
            options,
            context,
            current_position,
            deadline,
        )
    }

    fn percolate_all_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: Option<&crate::rank::CompiledRankProgram>,
        chunk_size: usize,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<std::time::Instant>,
        sink: &mut dyn crate::delivery::ChunkSink,
    ) -> Result<crate::delivery::ExhaustiveMatchResult, ShardError> {
        self.current.load().percolate_all_owned(
            title,
            include_broad,
            pred,
            program,
            chunk_size,
            context,
            current_position,
            deadline,
            sink,
        )
    }

    // ---- ADR-113 PIT: forwarded to the CURRENT backing. A handoff swap
    // replaces the backing, so its pins vanish and a mid-cursor page fails
    // typed (PitNotFound → 409 stale) instead of serving the new generation.
    fn open_pit(&self, pit: u64) -> Result<(), ShardError> {
        self.current.load().open_pit(pit)
    }

    fn close_pit(&self, pit: u64) -> Result<(), ShardError> {
        self.current.load().close_pit(pit)
    }

    fn percolate_top_k_owned_pit(
        &self,
        pit: u64,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<std::time::Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        self.current.load().percolate_top_k_owned_pit(
            pit,
            title,
            include_broad,
            pred,
            program,
            options,
            context,
            current_position,
            deadline,
        )
    }

    fn percolate_top_k_batch_owned(
        &self,
        titles: &[crate::cluster::shard::BatchTitleRequest<'_>],
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        current_position: u32,
        deadline: Option<std::time::Instant>,
    ) -> Result<crate::cluster::shard::ShardBatchRankedMatch, ShardError> {
        self.current.load().percolate_top_k_batch_owned(
            titles,
            include_broad,
            pred,
            program,
            options,
            current_position,
            deadline,
        )
    }

    fn fetch_matches(
        &self,
        logical_ids: &[u64],
        max_source_bytes: usize,
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<FetchedMatch>, ShardError> {
        self.current
            .load()
            .fetch_matches(logical_ids, max_source_bytes, deadline)
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        self.current.load().num_queries()
    }

    fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        self.current.load().class_counts()
    }

    fn validate_ownership(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), ShardError> {
        self.current
            .load()
            .validate_ownership(position, generation, num_shards)
    }

    fn live_endpoints(&self) -> Vec<String> {
        self.current.load().live_endpoints()
    }

    fn live_primary_endpoint(&self) -> Option<String> {
        self.current.load().live_primary_endpoint()
    }

    fn source_of(&self, logical: u64) -> Result<Option<String>, ShardError> {
        self.current.load().source_of(logical)
    }

    fn document_of(
        &self,
        logical: u64,
    ) -> Result<Option<crate::storage::StoredSource>, ShardError> {
        self.current.load().document_of(logical)
    }

    fn has_live_query(&self, logical: u64) -> Result<bool, ShardError> {
        self.current.load().has_live_query(logical)
    }

    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        self.current.load().ingest_extracted(items)
    }

    fn insert_extracted_with_tags(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        self.current
            .load()
            .insert_extracted_with_tags(ex, logical, version, text, tags)
    }

    fn insert_extracted_with_placement(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<Option<u32>, ShardError> {
        self.current
            .load()
            .insert_extracted_with_placement(ex, logical, version, text, tags, placement)
    }

    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        self.current.load().delete_by_logical_id(logical)
    }

    fn flush(&self) -> Result<(), ShardError> {
        self.current.load().flush()
    }

    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        self.current.load().seal_for_checkpoint()
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        self.current.load().segment_filenames()
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        self.current.load().next_seg_id()
    }

    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        self.current.load().translog_tail(from)
    }

    fn acquire_retention_lease(&self) -> Result<(u64, LogPos), ShardError> {
        self.current.load().acquire_retention_lease()
    }

    fn renew_retention_lease(&self, lease: u64, to: LogPos) -> Result<(), ShardError> {
        self.current.load().renew_retention_lease(lease, to)
    }

    fn release_retention_lease(&self, lease: u64) -> Result<(), ShardError> {
        self.current.load().release_retention_lease(lease)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_recovered_replica(
        &self,
        norm: &Arc<Normalizer>,
        dict: &Arc<Dict>,
        tag_dict: &Arc<TagDict>,
        config: EngineConfig,
        primary_dir: &Path,
        replica_dir: &Path,
        max_passes: usize,
    ) -> Result<(), ShardError> {
        self.current.load().add_recovered_replica(
            norm,
            dict,
            tag_dict,
            config,
            primary_dir,
            replica_dir,
            max_passes,
        )
    }

    fn set_event_sink(&self, sink: EventSink) {
        self.current.load().set_event_sink(sink);
    }
}

#[cfg(test)]
mod tests;
