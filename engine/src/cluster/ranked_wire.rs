//! Structured ranked-seam error codes carried in tonic `Status` metadata
//! (ADR-111).
//!
//! The ADR-110 ranked RPCs reconstruct typed [`ShardError`]s from a failure
//! `Status`. Before ADR-111 that reconstruction keyed on the gRPC code PLUS
//! frozen message substrings written by the server's `read_status` — a
//! three-string coupling that also mistypes any `failed_precondition` whose
//! message merely contains "ownership". This module carries the error class as
//! explicit metadata instead: producers [`attach`] a compact code (+ optional
//! `u64` argument), and the client's `ranked_rpc_err` [`parse`]s it
//! metadata-first, falling back to the frozen substring ladder only for
//! version-skewed peers. The legacy messages are therefore a compatibility
//! contract and must never change (`legacy_rpc_err_preserves_messages_...`
//! in `remote.rs` pins them).
//!
//! Keys are deliberately not `grpc-*` (tonic sanitizes those away) and ASCII
//! (binary `-bin` keys buy nothing for a short token).

use tonic::metadata::{Ascii, MetadataValue};
use tonic::Status;

use super::shard::ShardError;
use crate::ownership::OwnershipError;

pub(crate) const RANKED_ERROR_KEY: &str = "rr-ranked-error";
pub(crate) const RANKED_ERROR_ARG_KEY: &str = "rr-ranked-error-arg";

const CODE_SOURCE_UNAVAILABLE: &str = "source_unavailable";
const CODE_ENRICHMENT_LIMIT: &str = "enrichment_limit";
const CODE_OWNERSHIP_MISMATCH: &str = "ownership_mismatch";

/// The ranked-seam error classes that cross the wire as structured codes —
/// exactly the set `ranked_rpc_err` reconstructs. Deadline stays typed by the
/// gRPC status code alone, and admission deliberately stays untyped here (the
/// coordinator surfaces a remote admission failure as a 502 delivery error,
/// not a caller 400; typing it would change that contract).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RankedWireCode {
    /// Argument: the logical id whose source is missing.
    SourceUnavailable,
    /// Argument: the group's full byte credit.
    EnrichmentLimit,
    OwnershipMismatch,
}

impl RankedWireCode {
    fn as_str(self) -> &'static str {
        match self {
            Self::SourceUnavailable => CODE_SOURCE_UNAVAILABLE,
            Self::EnrichmentLimit => CODE_ENRICHMENT_LIMIT,
            Self::OwnershipMismatch => CODE_OWNERSHIP_MISMATCH,
        }
    }
}

/// Attach the structured code (+ optional argument) to a failure `Status`.
/// Fail-soft by construction: the code strings are static ASCII and the
/// argument is decimal digits, so encoding cannot fail — but if it ever did,
/// the unchanged message still reconstructs through the substring fallback.
pub(crate) fn attach(mut status: Status, code: RankedWireCode, arg: Option<u64>) -> Status {
    let value: MetadataValue<Ascii> = MetadataValue::from_static(code.as_str());
    status.metadata_mut().insert(RANKED_ERROR_KEY, value);
    if let Some(arg) = arg {
        if let Ok(value) = MetadataValue::try_from(arg.to_string()) {
            status.metadata_mut().insert(RANKED_ERROR_ARG_KEY, value);
        }
    }
    status
}

/// Metadata-first decode of a ranked-seam failure. `None` means "no (usable)
/// structured code" — the caller falls back to the frozen-message ladder, so a
/// garbled or unknown code degrades to the pre-ADR-111 behavior instead of
/// failing the reconstruction.
pub(crate) fn parse(status: &Status) -> Option<ShardError> {
    let code = status.metadata().get(RANKED_ERROR_KEY)?.to_str().ok()?;
    let arg = status
        .metadata()
        .get(RANKED_ERROR_ARG_KEY)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    match code {
        // A source loss without its id is useless to the coordinator's
        // diagnostics; let the substring parser recover it instead.
        CODE_SOURCE_UNAVAILABLE => Some(ShardError::SourceUnavailable(arg?)),
        CODE_ENRICHMENT_LIMIT => Some(ShardError::EnrichmentLimit {
            limit: usize::try_from(arg.unwrap_or(0)).unwrap_or(usize::MAX),
        }),
        CODE_OWNERSHIP_MISMATCH => Some(ShardError::OwnershipMismatch(
            OwnershipError::PlacementDecisionMismatch,
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_each_code_with_argument() {
        let status = attach(
            Status::not_found("scrambled"),
            RankedWireCode::SourceUnavailable,
            Some(42),
        );
        assert!(matches!(
            parse(&status),
            Some(ShardError::SourceUnavailable(42))
        ));

        let status = attach(
            Status::resource_exhausted("scrambled"),
            RankedWireCode::EnrichmentLimit,
            Some(16),
        );
        assert!(matches!(
            parse(&status),
            Some(ShardError::EnrichmentLimit { limit: 16 })
        ));

        let status = attach(
            Status::failed_precondition("scrambled"),
            RankedWireCode::OwnershipMismatch,
            None,
        );
        assert!(matches!(
            parse(&status),
            Some(ShardError::OwnershipMismatch(_))
        ));
    }

    #[test]
    fn missing_metadata_is_none_for_the_fallback() {
        assert!(parse(&Status::not_found("source unavailable for logical id 7")).is_none());
    }

    #[test]
    fn missing_or_garbled_argument_degrades_safely() {
        // Source loss without an id: refuse, so the substring parser recovers it.
        let status = attach(
            Status::not_found("x"),
            RankedWireCode::SourceUnavailable,
            None,
        );
        assert!(parse(&status).is_none());
        // Enrichment without a limit: the pre-ADR-111 fabricated 0.
        let status = attach(
            Status::resource_exhausted("x"),
            RankedWireCode::EnrichmentLimit,
            None,
        );
        assert!(matches!(
            parse(&status),
            Some(ShardError::EnrichmentLimit { limit: 0 })
        ));
    }
}
