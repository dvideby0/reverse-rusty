//! The HTTP classification tables owned by the cluster error types.
//!
//! Two REST surfaces map these errors, and their divergence is **deliberate**:
//!
//! | variant | write/admin surface ([`ShardError::write_http_class`]) | v2 no-partial read ([`ClusterRankedError::v2_http_class`]) |
//! |---|---|---|
//! | `OwnershipMismatch` | 409 `placement_generation_mismatch` | 503 `placement_generation_mismatch` |
//! | `Config` | 400 `validation_error` | 503 `cluster_unavailable` |
//! | `DictMismatch` | 500 `feature_space_mismatch` | 503 `cluster_unavailable` |
//! | `DuplicateLogicalId` | 409 `logical_id_conflict` | 503 `cluster_unavailable` |
//! | `PitNotFound` / `StalePit` | 409 `stale_cursor` | **409** `stale_cursor` |
//! | `PitUnsupported` | 400 `validation_error` | 501 `pit_unsupported` |
//!
//! The write/admin surface speaks REST conflict semantics: 409 tells the writer
//! its mutation raced a placement change (or hit an existing id) — re-resolve
//! and retry the *request*; 400 says the request/config input itself was
//! invalid. The ADR-110 exact bounded read surface has a no-partial contract
//! (`docs/reference/api/percolate.md`): the same conditions mean "this exact
//! read cannot be served right now" — retry *later*, which is 503. Aligning
//! either direction would silently break a documented surface, so the two
//! tables live side by side here where a new variant must be decided for both.
//!
//! **The one deliberate read-surface 409 (ADR-113):** a stale PIT/cursor. It is
//! NOT a retry-later condition — the pinned generation is gone forever, no
//! amount of waiting brings it back, and the client's correct move is to open a
//! new PIT and restart pagination. 409 (against the ADR-110-era read-409-free
//! rule, amended by ADR-113) tells the client precisely that; 503 would invite
//! useless retries of an unservable cursor.
//!
//! Codes are `u16` so the lean core stays free of any HTTP crate; the server
//! bin converts (every value emitted here is a valid status code).

use super::{ClusterRankedError, ShardError};

impl ShardError {
    /// HTTP `(status, error kind)` for the cluster write/admin/vocab surface.
    ///
    /// `PartiallyApplied` maps to 200 for totality only: it is not a failure of
    /// the request (the mutation is durably logged and queued for repair), and
    /// the write handlers surface it as a 200 `partial` result *before* reaching
    /// a generic error response — a retry hint here would invite a double-log.
    #[must_use]
    pub fn write_http_class(&self) -> (u16, &'static str) {
        match self {
            // `PitUnsupported` joins the 400 row (ADR-113 totality — PIT ops
            // never reach the write surface; an unsupported pin is a
            // caller-shape error there).
            ShardError::Config(_) | ShardError::Admission(_) | ShardError::PitUnsupported(_) => {
                (400, "validation_error")
            }
            ShardError::Log(_) => (503, "durability_unavailable"),
            ShardError::Remote(_) => (502, "shard_unreachable"),
            ShardError::DictMismatch { .. } => (500, "feature_space_mismatch"),
            ShardError::OwnershipMismatch(_) => (409, "placement_generation_mismatch"),
            ShardError::ControlPlane(_) => (503, "control_plane_error"),
            ShardError::DeadlineExceeded => (408, "deadline_exceeded"),
            ShardError::Protocol(_) => (502, "invalid_shard_response"),
            ShardError::SourceUnavailable(_) => (502, "source_unavailable"),
            ShardError::DuplicateLogicalId(_) => (409, "logical_id_conflict"),
            ShardError::EnrichmentLimit { .. } => (413, "rank_enrichment_limit"),
            ShardError::PartiallyApplied { .. } => (200, "partially_applied"),
            // ADR-113 totality row: a missing pin is the same stale-cursor
            // conflict the read surface reports.
            ShardError::PitNotFound(_) => (409, "stale_cursor"),
        }
    }
}

impl ClusterRankedError {
    /// HTTP `(status, error kind, metrics outcome)` for the ADR-110 v2 exact
    /// bounded read surface. No-partial contract: conditions the write surface
    /// reports as caller conflicts are "cannot serve this exact read right
    /// now" here, hence 503 rather than 409 (see the module table).
    #[must_use]
    pub fn v2_http_class(&self) -> (u16, &'static str, &'static str) {
        match self {
            ClusterRankedError::Admission(_)
            | ClusterRankedError::Shard(ShardError::Admission(_)) => {
                (400, "rank_admission_rejected", "admission")
            }
            ClusterRankedError::DeadlineExceeded
            | ClusterRankedError::Shard(ShardError::DeadlineExceeded) => {
                (408, "timeout", "timeout")
            }
            ClusterRankedError::EnrichmentLimit { .. }
            | ClusterRankedError::Shard(ShardError::EnrichmentLimit { .. }) => {
                (413, "rank_enrichment_limit", "enrichment_limit")
            }
            ClusterRankedError::InvalidShardReply { .. }
            | ClusterRankedError::DuplicateLogicalId(_)
            | ClusterRankedError::Shard(ShardError::Remote(_) | ShardError::Protocol(_)) => {
                (502, "shard_delivery_failed", "error")
            }
            ClusterRankedError::Shard(ShardError::SourceUnavailable(_)) => {
                (502, "source_unavailable", "error")
            }
            ClusterRankedError::Shard(ShardError::OwnershipMismatch(_)) => {
                (503, "placement_generation_mismatch", "error")
            }
            // ADR-113: the one deliberate read-surface 409 (see module doc) —
            // the pinned generation is gone forever; restart pagination.
            // (`Shard(PitNotFound)` folds into `StalePit` at the From seam;
            // kept in the match for totality if constructed directly.)
            ClusterRankedError::StalePit
            | ClusterRankedError::Shard(ShardError::PitNotFound(_)) => {
                (409, "stale_cursor", "stale_cursor")
            }
            // A PIT op reached a shard that cannot pin (remote assembly):
            // not stale, not retryable — the feature is absent here.
            ClusterRankedError::Shard(ShardError::PitUnsupported(_)) => {
                (501, "pit_unsupported", "validation")
            }
            ClusterRankedError::Shard(
                ShardError::Config(_)
                | ShardError::DictMismatch { .. }
                | ShardError::Log(_)
                | ShardError::ControlPlane(_)
                | ShardError::DuplicateLogicalId(_)
                | ShardError::PartiallyApplied { .. },
            ) => (503, "cluster_unavailable", "error"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ownership::OwnershipError;

    fn ownership() -> ShardError {
        ShardError::OwnershipMismatch(OwnershipError::PlacementDecisionMismatch)
    }

    #[test]
    fn ownership_mismatch_is_write_409_but_v2_503() {
        let (status, kind) = ownership().write_http_class();
        assert_eq!((status, kind), (409, "placement_generation_mismatch"));
        let (status, kind, outcome) = ClusterRankedError::Shard(ownership()).v2_http_class();
        assert_eq!(
            (status, kind, outcome),
            (503, "placement_generation_mismatch", "error")
        );
    }

    #[test]
    fn config_is_write_400_but_v2_503() {
        let error = ShardError::Config("bad".into());
        assert_eq!(error.write_http_class(), (400, "validation_error"));
        let (status, kind, _) = ClusterRankedError::Shard(error).v2_http_class();
        assert_eq!((status, kind), (503, "cluster_unavailable"));
    }

    #[test]
    fn dict_mismatch_is_write_500_but_v2_503() {
        let error = ShardError::DictMismatch {
            expected: 1,
            actual: 2,
        };
        assert_eq!(error.write_http_class(), (500, "feature_space_mismatch"));
        let (status, kind, _) = ClusterRankedError::Shard(error).v2_http_class();
        assert_eq!((status, kind), (503, "cluster_unavailable"));
    }

    #[test]
    fn duplicate_logical_id_is_write_409_but_v2_503() {
        let error = ShardError::DuplicateLogicalId(7);
        assert_eq!(error.write_http_class(), (409, "logical_id_conflict"));
        let (status, kind, _) = ClusterRankedError::Shard(error).v2_http_class();
        assert_eq!((status, kind), (503, "cluster_unavailable"));
    }

    #[test]
    fn coordinator_level_duplicate_is_v2_502_delivery_failure() {
        // Distinct from the write-surface 409: a cross-shard duplicate detected
        // during the merge is a dishonest-reply condition, not a caller conflict.
        let (status, kind, outcome) = ClusterRankedError::DuplicateLogicalId(7).v2_http_class();
        assert_eq!(
            (status, kind, outcome),
            (502, "shard_delivery_failed", "error")
        );
    }

    #[test]
    fn stale_pit_is_the_one_deliberate_read_surface_409() {
        // ADR-113 amends the read-409-free rule for exactly this condition:
        // the pinned generation is unrecoverable, so retry-later (503) would
        // be a lie — the client must open a new PIT and restart.
        let (status, kind, outcome) = ClusterRankedError::StalePit.v2_http_class();
        assert_eq!(
            (status, kind, outcome),
            (409, "stale_cursor", "stale_cursor")
        );
        let (status, kind, outcome) =
            ClusterRankedError::Shard(ShardError::PitNotFound(3)).v2_http_class();
        assert_eq!(
            (status, kind, outcome),
            (409, "stale_cursor", "stale_cursor")
        );
        // Both surfaces agree on this one (the divergence-free row).
        assert_eq!(
            ShardError::PitNotFound(3).write_http_class(),
            (409, "stale_cursor")
        );
    }

    #[test]
    fn pit_unsupported_is_read_501_write_400() {
        let error = ShardError::PitUnsupported("wire PIT is a later increment".into());
        assert_eq!(error.write_http_class(), (400, "validation_error"));
        let (status, kind, _) = ClusterRankedError::Shard(error).v2_http_class();
        assert_eq!((status, kind), (501, "pit_unsupported"));
    }

    #[test]
    fn partially_applied_is_the_totality_200() {
        let error = ShardError::PartiallyApplied {
            logical: 1,
            applied: vec![0],
            failed: vec![1],
            detail: "x".into(),
        };
        assert_eq!(error.write_http_class(), (200, "partially_applied"));
    }
}
