//! `Shard` — the local↔remote seam — and `LocalShard`, its in-process implementation.
//!
//! [`Shard`] abstracts the OPERATION a coordinator performs on a shard, never the
//! shard's internal data: a remote shard has no in-process [`EngineSnapshot`](crate::segment::EngineSnapshot), so the
//! trait exposes [`Shard::percolate_filtered`] (the matched ids + stats for one title) rather
//! than handing back a snapshot. [`LocalShard`] is the in-process impl — an owned
//! [`Engine`](crate::segment::Engine) (writes serialized behind a `std::sync::Mutex`) plus an
//! `ArcSwap<EngineSnapshot>` for lock-free reads, exactly the per-engine pattern the
//! HTTP server uses. It does NOT re-implement matching; `percolate_filtered` delegates to
//! [`EngineSnapshot::match_title_filtered`]. Every `LocalShard` is constructed with
//! [`Engine::with_shared`] over the coordinator's frozen normalizer + dict + tag dict, and all
//! writes go through the read-only `*_extracted` paths so the shared `Arc<Dict>` /
//! `Arc<TagDict>` is never forked.
//!
//! Every operation returns [`Result<_, ShardError>`]: a `LocalShard` is infallible
//! (it always returns `Ok`), but a remote shard can fail on the wire. Surfacing that
//! as an error — rather than swallowing it into an empty result — is load-bearing for
//! the zero-false-negative contract: a dropped shard probe must fail the percolate,
//! not silently shrink the answer. The remote implementation (`RemoteShard`, behind
//! the `distributed` feature) lives in `super::remote` and satisfies the same trait
//! by issuing gRPC calls.
//!
//! This file is the module ROOT: it holds the seam *definitions* shared across the
//! module — [`ShardError`], the [`EventSink`] alias, the [`Shard`] trait, and the
//! free-standing [`apply_mutation`] replay glue — while the `impl`-heavy concerns live
//! in focused submodules:
//!   - [`retention`] — the translog retention-lease bookkeeping ([`RetentionLeases`],
//!     ADR-040/048) plus the `resolve_lease_ttl` config helper.
//!   - [`local`]     — [`LocalShard`]: its struct, every constructor, the `Shard` impl,
//!     and the clock-injectable seal core (`seal_for_checkpoint_at`).

use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::segment::{IngestReport, MatchStats, PlacedQuery};
use crate::tagdict::TagDict;
use std::path::Path;
use std::sync::Arc;

use super::clog::{ClusterMutation, LogPos};

mod fetch;
mod local;
mod retention;

#[cfg(test)]
mod tests;

pub(crate) use fetch::fetch_source_step;
pub(crate) use local::LocalShard;

/// An error from cluster construction or a shard operation. In-process
/// ([`LocalShard`]) *operations* are infallible and never produce this; a `RemoteShard`
/// produces [`ShardError::Remote`] on gRPC transport or status failure, and
/// [`ShardError::DictMismatch`] when a server's frozen dict diverges from the
/// coordinator's (the connect-time fingerprint handshake). Cluster *construction* (the
/// `ClusterEngine` builders and `HashRing::new`) produces [`ShardError::Config`] on an
/// invalid configuration. Kept transport-agnostic (a `String` detail, not a
/// `tonic::Status`) so it lives in the always-compiled core alongside the trait, rather
/// than dragging the gated networking stack into the lean build.
#[derive(Debug, Clone)]
pub enum ShardError {
    /// A remote shard was unreachable or returned an error status (detail included).
    Remote(String),
    /// Invalid cluster configuration / construction precondition — e.g. zero shards, or
    /// a shard/endpoint count that disagrees with the ring. Replaces the old
    /// construction-time `assert!`s so library code never panics on bad input.
    Config(String),
    /// A remote shard's frozen-dict fingerprint disagreed with the coordinator's at
    /// connect time. The cross-process shared-dict invariant is broken, so matching
    /// against that shard would *silently* drop results — fail loud instead. This is the
    /// one false-negative path the otherwise-fallible seam cannot catch (ADR-029).
    DictMismatch { expected: u64, actual: u64 },
    /// Placement generation or per-row ownership metadata disagrees with the
    /// shard position/configuration. Serving it could duplicate or suppress a
    /// logical result, so ADR-109 requires a fail-closed typed error.
    OwnershipMismatch(crate::ownership::OwnershipError),
    /// A bounded read reached its one request deadline. No partial result is
    /// returned; the coordinator fails the exact request closed.
    DeadlineExceeded,
    /// A bounded read violated its static K/total admission contract.
    Admission(crate::result::TopKAdmissionError),
    /// A shard returned a malformed or dishonest bounded reply (for example,
    /// more than K rows or a missing bounded/ownership attestation).
    Protocol(String),
    /// Winner enrichment could not find the source on the owning shard.
    SourceUnavailable(u64),
    /// A cluster write attempted to create a second live row under one logical
    /// id. Distributed exact top-K requires logical ids to be unique; callers
    /// replace an existing row through `upsert_query`.
    DuplicateLogicalId(u64),
    /// Winner source materialization exceeded the caller's cumulative byte
    /// credit. This is distinct from the per-message transport cap.
    EnrichmentLimit { limit: usize },
    /// This shard cannot pin point-in-time snapshots (ADR-113) — today every
    /// remote/wire-backed shard. Carries the alternative the caller should
    /// surface (the deferral pattern: refuse loudly, name the way out).
    PitUnsupported(String),
    /// A pit-scoped read named a PIT this shard does not hold (expired,
    /// closed, replaced backing, or a failed-over replica). Serving the
    /// current view instead would silently mix generations — fail closed and
    /// let the caller surface 409 stale-cursor semantics (ADR-113).
    PitNotFound(u64),
    /// A cluster mutation could not be durably logged (the coordinator's externalized
    /// `ClusterLog`, ADR-031). The mutation is *rejected*, not applied — surfacing it
    /// rather than acknowledging an unlogged write is load-bearing for the
    /// rebuild-from-log contract (an un-logged add/remove would silently vanish on
    /// reopen). Parallels the engine's WAL-first write path (ADR-013).
    Log(String),
    /// A cluster-state transition could not be committed by the control plane (no quorum,
    /// not the leader, or a backend error — ADR-037). The transition is *rejected*, not
    /// applied; surfacing it rather than serving a stale/blind shard→node map is
    /// load-bearing (a silently-wrong assignment routes a title to the wrong node — a
    /// shard-sized false negative). The structured cause is in
    /// [`ControlError`](super::control::ControlError); this is the folded form crossing the
    /// coordinator boundary. The in-memory single-node control plane never produces it.
    ControlPlane(String),
    /// A selective multi-shard mutation applied to some target shards but FAILED on others (a
    /// remote shard write errored mid-fan-out — ADR-047). Distinguished from a clean failure
    /// (`Remote`/`Log`, where nothing applied) so a higher layer can act precisely: the
    /// mutation IS durably logged (committed), the `applied` shards already hold it, the
    /// `failed` shards do not yet, and the coordinator has queued the failed shards for repair.
    /// Call [`ClusterEngine::resync`](crate::cluster::ClusterEngine::resync) to converge them
    /// (or reopen, whose log replay re-drives every target); do NOT re-`add_query`, which would
    /// double-log. Never produced by the in-process / RF=1 path (its `LocalShard` writes are
    /// infallible — an empty failure set yields the normal `Ok` outcome).
    PartiallyApplied {
        /// Logical id of the mutation that partially applied.
        logical: u64,
        /// Shards that DID apply it (they already hold the new state).
        applied: Vec<usize>,
        /// Shards that did NOT (queued for repair; a transient false-negative window).
        failed: Vec<usize>,
        /// The first underlying shard error, for context.
        detail: String,
    },
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardError::Remote(m) => write!(f, "remote shard error: {m}"),
            ShardError::Config(m) => write!(f, "cluster config error: {m}"),
            ShardError::DictMismatch { expected, actual } => write!(
                f,
                "dict fingerprint mismatch: coordinator {expected:#018x} != shard \
                 {actual:#018x} (every shard must share the coordinator's frozen dict)"
            ),
            ShardError::OwnershipMismatch(error) => write!(f, "{error}"),
            ShardError::DeadlineExceeded => f.write_str("shard read deadline exceeded"),
            ShardError::Admission(error) => error.fmt(f),
            ShardError::Protocol(detail) => write!(f, "shard protocol error: {detail}"),
            ShardError::SourceUnavailable(logical) => {
                write!(f, "source unavailable for logical id {logical}")
            }
            ShardError::DuplicateLogicalId(logical) => write!(
                f,
                "logical id {logical} already exists; use upsert_query to replace it"
            ),
            ShardError::EnrichmentLimit { limit } => {
                write!(f, "ranked winner enrichment exceeds {limit} bytes")
            }
            ShardError::PitUnsupported(alternative) => {
                write!(
                    f,
                    "point-in-time snapshots are unsupported here: {alternative}"
                )
            }
            ShardError::PitNotFound(pit) => {
                write!(f, "point-in-time {pit} is not held by this shard")
            }
            ShardError::Log(m) => write!(f, "cluster log durability error: {m}"),
            ShardError::ControlPlane(m) => write!(f, "cluster control-plane error: {m}"),
            ShardError::PartiallyApplied {
                logical,
                applied,
                failed,
                detail,
            } => write!(
                f,
                "cluster mutation for logical {logical} partially applied: applied on shards \
                 {applied:?}, FAILED on {failed:?} ({detail}); durably logged — resync or reopen \
                 to converge"
            ),
        }
    }
}

impl std::error::Error for ShardError {}

impl From<crate::ownership::OwnershipError> for ShardError {
    fn from(value: crate::ownership::OwnershipError) -> Self {
        Self::OwnershipMismatch(value)
    }
}

impl From<crate::rank::RankedMatchError> for ShardError {
    fn from(value: crate::rank::RankedMatchError) -> Self {
        match value {
            crate::rank::RankedMatchError::Admission(error) => Self::Admission(error),
            crate::rank::RankedMatchError::Cancelled(_) => Self::DeadlineExceeded,
        }
    }
}

impl From<crate::delivery::ExhaustiveMatchError> for ShardError {
    fn from(value: crate::delivery::ExhaustiveMatchError) -> Self {
        match value {
            crate::delivery::ExhaustiveMatchError::InvalidChunkSize { requested, max } => {
                Self::Config(format!(
                    "exhaustive chunk size {requested} is outside 1..={max}"
                ))
            }
            crate::delivery::ExhaustiveMatchError::Cancelled => Self::DeadlineExceeded,
            crate::delivery::ExhaustiveMatchError::Sink(error) => {
                Self::Protocol(format!("exhaustive sink failed: {error}"))
            }
        }
    }
}

/// One ownership-filtered, bounded shard result. `result_bytes` is the exact
/// protobuf encoded size for remote replies and zero for in-process shards.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ShardRankedMatch {
    pub(crate) hits: Vec<crate::rank::RankedHit>,
    pub(crate) total_hits: crate::result::TotalHits,
    pub(crate) stats: MatchStats,
    pub(crate) rank_stats: crate::rank::RankStats,
    pub(crate) result_bytes: u64,
}

/// One current-view source returned by the owning shard during phase two.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FetchedMatch {
    pub(crate) logical_id: u64,
    pub(crate) source: String,
}

/// One batch title plus ITS routing context (ADR-112) — routed sets differ per
/// title, so ownership is per entry; slice index = title index.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BatchTitleRequest<'a> {
    pub(crate) title: &'a str,
    pub(crate) context: &'a crate::ownership::OwnershipContext,
}

/// One title's ownership-filtered bounded rows inside a batch reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ShardRankedTitle {
    pub(crate) hits: Vec<crate::rank::RankedHit>,
    pub(crate) total_hits: crate::result::TotalHits,
    pub(crate) rank_stats: crate::rank::RankStats,
}

/// One shard's bounded batch reply: per-title rows in request order + the
/// batch-aggregate match statistics. `result_bytes` sums the exact encoded
/// frame sizes for remote replies and is zero for in-process shards.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ShardBatchRankedMatch {
    pub(crate) titles: Vec<ShardRankedTitle>,
    pub(crate) stats: MatchStats,
    pub(crate) result_bytes: u64,
}

/// Sink for shard-level observability events (e.g. a [`ReplicatedShard`] replica
/// dropping out of its in-sync set). The `Arc` analogue of the engine's event
/// observer; the coordinator fans its observer in via `ClusterEngine::set_observer`.
///
/// [`ReplicatedShard`]: super::replica::ReplicatedShard
pub(crate) type EventSink = Arc<dyn Fn(&crate::events::EngineEvent) + Send + Sync>;

/// One live query as gathered for a blue/green rebuild (`set_vocab` / resize, ADR-074/109):
/// `(logical, dsl, version, raw_tags, tag_ids, rank, placement)`. The metadata rides the gather
/// so the rebuild re-places each query at the version it was durably stored with (not reset to 1)
/// and carries both read-back source tags and integer match metadata to its new shard; the old
/// placement is available for fingerprints but a rebuild deliberately derives a fresh placement
/// under its one new generation.
pub(crate) type LiveTaggedQuery = crate::segment::LiveSourceDocument;

/// One shard, local or remote — the seam that lets a coordinator hold a mix of
/// in-process and (eventually) networked shards behind one type.
///
/// Abstracts the OPERATION, not the data: there is deliberately no `snapshot()`,
/// because a remote shard has no local [`EngineSnapshot`](crate::segment::EngineSnapshot). [`Shard::percolate`] IS
/// the per-shard probe (matched logical ids + [`MatchStats`]); `include_broad` is the
/// ALREADY-RESOLVED per-shard toggle — the coordinator applies the "broad lane only
/// on shard 0" rule before calling, and the shard never re-derives it.
///
/// `Send + Sync` is a supertrait because the coordinator fans probes out across rayon
/// worker threads, which borrow `&dyn Shard`. Object-safety and the `Send + Sync`
/// bound are enforced for free by `ClusterEngine.shards: Vec<Box<dyn Shard>>` plus the
/// `assert_send_sync::<ClusterEngine>()` guard in `lib.rs`.
pub(crate) trait Shard: Send + Sync {
    // ---- reads ----
    /// Probe this shard for one title, narrowed by an ALREADY-RESOLVED tag filter (ADR-049/055):
    /// the coordinator compiles the request's `(key,[values])` groups to `TagId`s once against the
    /// shared frozen tag space and fans the same `&TagPredicate` to every probed shard, so the shard
    /// never re-resolves it. An empty predicate (`TagPredicate::empty()`) is an unfiltered probe,
    /// byte-identical to the pre-tag path (the verify clause is a never-taken branch). Returns the
    /// matched logical ids + match stats. The coordinator's unfiltered [`percolate`] passes the empty
    /// predicate; only [`percolate_filtered`] threads a real one.
    ///
    /// [`percolate`]: crate::cluster::ClusterEngine::percolate
    /// [`percolate_filtered`]: crate::cluster::ClusterEngine::percolate_filtered
    #[allow(dead_code)]
    fn percolate_filtered(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError>;
    /// Ownership-aware cluster probe. Legacy/test shards must fail closed unless
    /// they attest by implementing this method.
    fn percolate_filtered_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let _ = (title, include_broad, pred, context, current_position);
        Err(ShardError::OwnershipMismatch(
            crate::ownership::OwnershipError::PlacementDecisionMismatch,
        ))
    }
    /// [`percolate_filtered`](Self::percolate_filtered) plus a per-id ranking score
    /// (ADR-059/075): the coordinator compiles the request's `rank` block ONCE against
    /// the shared frozen tag space (exactly like the `TagPredicate`) and fans the same
    /// [`CompiledRankSpec`] to every probed shard; the shard scores its OWN matched ids
    /// against its stored tag columns (newest live copy, as single-node). Scores are
    /// UNSORTED relative to rank — `(id, score)` pairs aligned to the id set — because
    /// ordering/pagination is the coordinator/handler's job (the single-node
    /// `EngineSnapshot::rank` contract). Copies of one logical id are version-identical
    /// across shards (identical op streams), so every shard reports the same score for
    /// the same id and the coordinator's dedup is order-safe.
    #[allow(dead_code)]
    fn percolate_filtered_ranked(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError>;
    fn percolate_filtered_ranked_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        let _ = (title, include_broad, pred, spec, context, current_position);
        Err(ShardError::OwnershipMismatch(
            crate::ownership::OwnershipError::PlacementDecisionMismatch,
        ))
    }
    /// Ownership-aware bounded typed ranking (ADR-110). The default is a loud
    /// refusal so a legacy/test shard cannot silently masquerade as bounded.
    #[allow(clippy::too_many_arguments)]
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
        let _ = (
            title,
            include_broad,
            pred,
            program,
            options,
            context,
            current_position,
            deadline,
        );
        Err(ShardError::Protocol(
            "bounded top-k is not implemented by this shard".into(),
        ))
    }

    /// Ownership-aware exhaustive collection (ADR-114). The sink is called
    /// synchronously with fixed-capacity provisional chunks; success is valid
    /// only with the returned terminal summary.
    #[allow(clippy::too_many_arguments)]
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
        let _ = (
            title,
            include_broad,
            pred,
            program,
            chunk_size,
            context,
            current_position,
            deadline,
            sink,
        );
        Err(ShardError::Protocol(
            "exhaustive streaming is not implemented by this shard".into(),
        ))
    }
    /// Pin this shard's CURRENT snapshot under the coordinator-allocated pit
    /// id (ADR-113). Default: loud typed refusal — only in-process shards can
    /// pin (wire PIT is a named later increment).
    fn open_pit(&self, pit: u64) -> Result<(), ShardError> {
        let _ = pit;
        Err(ShardError::PitUnsupported(
            "this shard cannot pin snapshots; use an in-process cluster or single-node mode".into(),
        ))
    }

    /// Release a pinned snapshot. Default `Ok`: close is best-effort cleanup —
    /// a shard that never pinned has nothing to release, and failing a close
    /// would block the coordinator from reaping its own registry entry.
    fn close_pit(&self, pit: u64) -> Result<(), ShardError> {
        let _ = pit;
        Ok(())
    }

    /// [`Shard::percolate_top_k_owned`] served from the pinned `pit` snapshot
    /// instead of the current view (ADR-113). The default refuses loudly so a
    /// shard that cannot pin can never silently serve a current-view page into
    /// a cursor stream (generation mixing).
    #[allow(clippy::too_many_arguments)]
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
        let _ = (
            title,
            include_broad,
            pred,
            program,
            options,
            context,
            current_position,
            deadline,
        );
        let _ = pit;
        Err(ShardError::PitUnsupported(
            "pit-scoped bounded top-k is not implemented by this shard".into(),
        ))
    }

    /// Ownership-aware bounded ranked title batching (ADR-112): one shared
    /// program/K/threshold, per-title contexts, one deadline, per-title rows in
    /// request order. The default is a loud refusal — a legacy/test shard
    /// cannot silently masquerade as batch-capable.
    #[allow(clippy::too_many_arguments)]
    fn percolate_top_k_batch_owned(
        &self,
        titles: &[BatchTitleRequest<'_>],
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        current_position: u32,
        deadline: Option<std::time::Instant>,
    ) -> Result<ShardBatchRankedMatch, ShardError> {
        let _ = (
            titles,
            include_broad,
            pred,
            program,
            options,
            current_position,
            deadline,
        );
        Err(ShardError::Protocol(
            "bounded batch top-k is not implemented by this shard".into(),
        ))
    }
    /// Batch-fetch current source text for final winners. Implementations must
    /// return exactly one row per requested id, in request order, or fail loud.
    fn fetch_matches(
        &self,
        logical_ids: &[u64],
        max_source_bytes: usize,
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<FetchedMatch>, ShardError> {
        let _ = (logical_ids, max_source_bytes, deadline);
        Err(ShardError::Config(
            "winner source fetch is not implemented by this shard".into(),
        ))
    }
    /// Physical query count held by this shard (a replicated/any-of query is counted
    /// once per local entry, so it is counted on each shard holding it).
    fn num_queries(&self) -> Result<usize, ShardError>;
    /// Per-class entry tally `[A, B, C, D]` for this shard (introspection/tests).
    fn class_counts(&self) -> Result<[u64; 5], ShardError>;
    /// Validate every published row against the coordinator's logical position
    /// before the shard becomes reachable (ADR-109).
    fn validate_ownership(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), ShardError> {
        let _ = (position, generation, num_shards);
        Err(ShardError::OwnershipMismatch(
            crate::ownership::OwnershipError::PlacementDecisionMismatch,
        ))
    }

    /// This shard's live `(logical_id, dsl)` source set — the corpus the shard's
    /// index is a materialized view of. Used by `ClusterEngine::set_vocab` to
    /// rebuild every shard under a new normalizer (ADR-046). Default: `Err` — only
    /// a shard backed by an in-process [`Engine`](crate::segment::Engine) (`LocalShard`/`ReplicatedShard`)
    /// can enumerate its sources; a `RemoteShard`'s sources live in another process
    /// (a cross-process vocabulary change is out of scope for v1, and `set_vocab`
    /// refuses a non-local cluster before ever calling this).
    fn live_sources(&self) -> Result<Vec<(u64, String)>, ShardError> {
        Err(ShardError::Config(
            "live_sources is only supported for in-process shards".into(),
        ))
    }

    /// This shard's live logical-id set without materializing query source text.
    /// Used during durable coordinator open to rebuild unique-id admission state.
    fn live_logical_ids(&self) -> Result<Vec<u64>, ShardError> {
        Err(ShardError::Config(
            "live_logical_ids is only supported for in-process shards".into(),
        ))
    }

    /// [`live_sources`](Self::live_sources) plus each live query's stored `version` and
    /// `TagId`s — the gather behind the TAGGED vocabulary rebuild (ADR-074):
    /// `ClusterEngine::set_vocab` re-places every query and must carry its tags (interned or
    /// post-freeze synthetic, since a synthetic id has no recoverable string) AND its stored
    /// version to the new shard verbatim, so a rebuild re-places at version N rather than
    /// resetting to 1. Same in-process-only boundary (and default `Err`) as `live_sources`.
    fn live_sources_tagged(&self) -> Result<Vec<LiveTaggedQuery>, ShardError> {
        Err(ShardError::Config(
            "live_sources_tagged is only supported for in-process shards".into(),
        ))
    }

    /// Whether this shard is backed by an in-process [`Engine`](crate::segment::Engine), so its normalizer
    /// can be swapped in place by a vocabulary change. `false` for a
    /// `RemoteShard`/`HandoffShard`, whose normalizer lives in another process and
    /// is NOT shipped a vocabulary change in v1 — `ClusterEngine::set_vocab` refuses
    /// to run unless every shard is local, so an alias can never silently diverge
    /// across processes (a cross-process false negative the dict-fingerprint
    /// handshake would not catch, since an alias is normalizer-only).
    fn is_local(&self) -> bool {
        false
    }

    /// The endpoints of the remote node(s) this shard's LIVE routing currently reaches — empty for
    /// an in-process shard (nothing remote to protect). Read by the orphan-slot GC sweep
    /// (ADR-096) as a KEEP-set: a slot the coordinator is currently routing to is never dropped,
    /// however routing got there (a committed reassign, a raw handoff flip, a
    /// `MovedButNotCommitted` crash window) — the committed map alone would miss those.
    /// `RemoteShard` reports its connect endpoint; `ReplicatedShard` its primary's plus every
    /// replica's (in-sync or not — conservative); `HandoffShard` forwards to its current backing.
    /// `distributed`-gated (its sole consumer is the gRPC GC sweep) so the lean build carries no
    /// dead trait surface — the `metrics_snapshot` precedent.
    #[cfg(feature = "distributed")]
    fn live_endpoints(&self) -> Vec<String> {
        Vec::new()
    }

    /// The live source DSL of `logical` on this shard, if it holds a live copy — the
    /// point read behind `GET /_doc/{id}` in cluster mode (ADR-070). `Ok(None)` means
    /// "this shard genuinely does not hold it"; the default is a loud **error**, never
    /// `Ok(None)`, so a shard type without a source store (a `RemoteShard` in v1) makes
    /// the coordinator's lookup fail visibly instead of reporting a false "not found".
    fn source_of(&self, _logical: u64) -> Result<Option<String>, ShardError> {
        Err(ShardError::Config(
            "source_of is only supported for in-process shards in v1".into(),
        ))
    }

    /// Canonical source document for `GET /_doc/{id}`. Kept separate from
    /// [`Self::source_of`] so search-hit enrichment remains a query-text-only
    /// lookup and never decodes tag metadata. The default is loud for remote
    /// shards until the richer source shape is added to the transport.
    fn document_of(
        &self,
        _logical: u64,
    ) -> Result<Option<crate::storage::StoredSource>, ShardError> {
        Err(ShardError::Config(
            "document_of is only supported for in-process shards in v1".into(),
        ))
    }

    /// Whether the shard's live exact index contains `logical`, independent of
    /// source-sidecar availability. This is the shard seam behind
    /// `HEAD /_doc/{id}`. The default is loud so a remote implementation that
    /// cannot inspect liveness never turns "unknown" into a false 404.
    fn has_live_query(&self, _logical: u64) -> Result<bool, ShardError> {
        Err(ShardError::Config(
            "has_live_query is only supported for in-process shards in v1".into(),
        ))
    }

    // ---- writes ----
    /// Bulk-ingest a pre-extracted bucket into a new immutable base segment — the
    /// distributed load path ([`crate::cluster::ClusterEngine::ingest`]). NOTE:
    /// `ClusterEngine::build` does NOT use this; it ingests via the infallible inherent
    /// [`LocalShard::ingest_local`] so that constructing an in-process cluster stays
    /// infallible. This seam method is what lets the coordinator load a *remote* shard.
    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError>;
    /// Insert one pre-extracted query into the memtable (live add), carrying the query's raw
    /// `(key,value)` metadata tags (ADR-049/055), resolved to `TagId`s read-only against the shared
    /// frozen tag space; an empty `tags` slice is an untagged insert, byte-identical to the pre-tag
    /// path (the coordinator's untagged adds pass `&[]`).
    fn insert_extracted_with_tags(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError>;
    /// ADR-109 write path. Implementations must persist and replicate the supplied
    /// placement exactly; the default preserves standalone test doubles only.
    fn insert_extracted_with_placement(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<Option<u32>, ShardError> {
        if placement.mode() != crate::ownership::PlacementMode::Standalone {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::InvalidStandalone,
            ));
        }
        self.insert_extracted_with_tags(ex, logical, version, text, tags)
    }
    /// Tombstone every live entry for `logical` (idempotent; a cheap no-op on a shard
    /// that doesn't hold it).
    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError>;
    /// Seal the memtable into an immutable base segment.
    fn flush(&self) -> Result<(), ShardError>;

    // ---- durable checkpoint (ADR-032; local shards only) ----
    /// Seal for a cluster checkpoint: flush the memtable AND re-seal any tombstoned base
    /// segment, so the ON-DISK segment set reflects every applied delete. Without the
    /// re-seal a `Remove` against a base segment lives only in the in-RAM alive overlay
    /// and would resurrect the query on reopen once its log entry is truncated.
    ///
    /// Returns the per-shard translog position `P` the sealed segments now capture through
    /// (ADR-039): every op `≤ P` is durably in the segments, and the translog is trimmed to
    /// `P` so its remaining tail is exactly the un-sealed ops `> P`. A recovering replica
    /// streams the segments (`≤ P`) then replays the tail (`> P`) — no overlap, no
    /// double-apply (the zero-false-negative boundary). In-memory shards return `LogPos(0)`.
    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError>;
    /// This shard's live (mmap'd) base-segment filenames — the registry the coordinator
    /// commits into `cluster_manifest.bin`. `Err` (never a silent empty list) if any
    /// segment is in-memory (a write fell back), which would otherwise lose data on reopen.
    fn segment_filenames(&self) -> Result<Vec<String>, ShardError>;
    /// This shard's next segment-id counter — committed per shard so a flush after reopen
    /// never reuses a committed segment filename.
    fn next_seg_id(&self) -> Result<u64, ShardError>;

    // ---- per-shard query log / translog (ADR-039; clustering step 5c) ----
    /// The un-sealed tail of this shard's durable query log: every logged mutation with
    /// position strictly after `from`, oldest-first (the ops NOT yet baked into a sealed
    /// segment). A recovering replica calls this after attaching the source's segments at
    /// `P = seal_for_checkpoint()` to replay the writes that landed during the copy window —
    /// the durable+replicated tail that lets recovery proceed WITHOUT quiescing writes
    /// (closing ADR-036's gap). A non-durable (in-memory) shard returns an empty tail.
    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError>;

    // ---- translog retention leases (ADR-040; clustering step 5d) ----
    /// Acquire a retention lease pinning this shard's current un-sealed translog tail: until
    /// the lease is renewed or released, [`seal_for_checkpoint`](Shard::seal_for_checkpoint)
    /// will NOT trim the translog past the returned position. A peer recovery acquires one
    /// before snapshotting segments, so even if the source seals AGAIN during the copy (another
    /// concurrent recovery, a checkpoint) the tail the recovery still needs survives — closing a
    /// latent false negative in ADR-039's no-quiesce path (a concurrent seal could strand it).
    /// Returns `(lease_id, pinned_pos)`. Default (in-memory / non-durable): a no-op lease at
    /// `LogPos(0)` — such a shard has no on-disk tail to retain and is never a recovery source.
    fn acquire_retention_lease(&self) -> Result<(u64, LogPos), ShardError> {
        Ok((0, LogPos(0)))
    }
    /// Advance a retention lease to `to` as a recovery consumer catches up, so the source may GC
    /// the now-consumed prefix on its next seal (the lease only ever moves forward). Idempotent;
    /// an unknown lease id is ignored. Default: a no-op.
    fn renew_retention_lease(&self, _lease: u64, _to: LogPos) -> Result<(), ShardError> {
        Ok(())
    }
    /// Release a retention lease — the recovery finished or aborted, so the source may again trim
    /// freely to its checkpoint. Idempotent (releasing twice, or an unknown id, is a no-op).
    /// Default: a no-op.
    fn release_retention_lease(&self, _lease: u64) -> Result<(), ShardError> {
        Ok(())
    }

    // ---- runtime replica growth (ADR-040; clustering step 5d) ----
    /// Bring up a NEW replica for this position from its primary and add it to the in-sync set
    /// WITHOUT quiescing writes for the segment-copy window — peer-recover a snapshot + tail, loop
    /// the catch-up to shrink the residual, then promote under a brief write quiesce (the finalize).
    /// A retention lease pins the primary's tail across the flow, so a concurrent seal can't strand
    /// it. Default: error — only a replicated position ([`ReplicatedShard`](super::replica::ReplicatedShard))
    /// can grow a local replica here (a bare/remote position has no in-process primary to copy from).
    // The recovery genuinely needs the feature space (norm/dict/tag_dict), the engine config, both
    // dirs, and the convergence cap; bundling them would only obscure the call.
    #[allow(clippy::too_many_arguments)]
    fn add_recovered_replica(
        &self,
        _norm: &Arc<Normalizer>,
        _dict: &Arc<Dict>,
        _tag_dict: &Arc<TagDict>,
        _config: EngineConfig,
        _primary_dir: &Path,
        _replica_dir: &Path,
        _max_passes: usize,
    ) -> Result<(), ShardError> {
        Err(ShardError::Config(
            "this shard position cannot grow an in-process replica (not a replicated local position)"
                .into(),
        ))
    }

    // ---- observability (ADR-035) ----
    /// Install an event sink so this shard can surface degraded-redundancy events — e.g. a
    /// [`ReplicatedShard`](super::replica::ReplicatedShard) replica falling out of its
    /// in-sync set. Default: a no-op (a plain [`LocalShard`]/`RemoteShard` emits nothing
    /// here). The coordinator fans its observer in via `ClusterEngine::set_observer`.
    fn set_event_sink(&self, _sink: EventSink) {}
}

/// Apply one logged mutation to a shard through its normal write path — so the op is itself
/// re-logged into that shard's translog (a recovered replica's tail stays consistent) and
/// applied to its engine. Re-derives features from the raw DSL against the frozen `dict`
/// (the ADR-029 DSL-on-wire invariant), so a replayed op is byte-identical to the original
/// live write → the recovered shard converges to the same logical set (zero false negatives).
/// Used by both in-process peer recovery ([`super::replica::peer_recover`]) and the
/// coordinator's gRPC tail-replay.
pub(crate) fn apply_mutation(
    shard: &dyn Shard,
    norm: &Normalizer,
    dict: &Dict,
    m: &ClusterMutation,
    // The target's shard position when the CALLER knows it (the resync repair
    // loop); `None` when coverage holds by construction (a replica catch-up
    // replays its own position's translog). Used to gate an Upsert's insert
    // half to covered positions only — see the Upsert arm.
    position: Option<u32>,
) -> Result<(), ShardError> {
    match m {
        ClusterMutation::Add {
            logical,
            version,
            dsl,
            tags,
            placement,
        } => {
            // Only parseable DSL is ever logged, but stay defensive: an unparseable record
            // carries no applicable mutation, so skip it rather than fail the whole replay.
            if let Ok(ast) = crate::dsl::parse(dsl) {
                let mut lc = String::new();
                let ex = extract_readonly(&ast, norm, dict, &mut lc);
                shard.insert_extracted_with_placement(
                    &ex, *logical, *version, dsl, tags, placement,
                )?;
            }
        }
        ClusterMutation::Remove { logical } => {
            shard.delete_by_logical_id(*logical)?;
        }
        ClusterMutation::Upsert {
            logical,
            version,
            dsl,
            tags,
            placement,
        } => {
            // Replace-by-id ON THIS SHARD: tombstone any prior copy, then insert the new
            // version — but only where the placement actually STORES the row. An upsert's
            // delete half fans to every shard, so a repair can legitimately target a
            // delete-only position; ADR-109 made shard-side inserts validate placement
            // coverage, so re-driving the insert there is refused (`LocalPositionMissing`)
            // and would wedge `resync` on that mutation forever (multi-machine harness
            // catch). Replicated modes cover every position; only Selective restricts.
            shard.delete_by_logical_id(*logical)?;
            let covered = position.is_none_or(|p| {
                placement.mode() != crate::ownership::PlacementMode::Selective
                    || placement.positions().binary_search(&p).is_ok()
            });
            if covered {
                if let Ok(ast) = crate::dsl::parse(dsl) {
                    let mut lc = String::new();
                    let ex = extract_readonly(&ast, norm, dict, &mut lc);
                    shard.insert_extracted_with_placement(
                        &ex, *logical, *version, dsl, tags, placement,
                    )?;
                }
            }
        }
    }
    Ok(())
}
