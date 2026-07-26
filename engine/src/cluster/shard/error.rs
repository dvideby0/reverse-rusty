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
