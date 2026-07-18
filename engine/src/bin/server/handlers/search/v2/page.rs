//! ADR-113 page resolution for `/v2/_search`: PIT/cursor token verification,
//! snapshot pinning, fingerprint enforcement, and `next_cursor` minting.
//!
//! Status contract (pinned by handler tests + the ADR): a structurally
//! garbled token is a client error (400 `validation_error`); a token that
//! authenticates but references a PIT this process cannot serve — expired,
//! closed, or minted by a previous process — is 409 `stale_cursor`; a valid
//! cursor whose resent query differs from the fingerprinted original is 400
//! `cursor_mismatch`; an at-capacity registry is 429 `pit_limit_exceeded`.

use std::sync::Arc;
use std::time::Instant;

use axum::{http::StatusCode, Json};

use crate::dto::ApiError;
use crate::pit::{
    cursor_mismatch_response, request_fingerprint, stale_cursor_response, token_failure_response,
    PitTokens, TokenError,
};
use crate::state::AppState;

use reverse_rusty::segment::EngineSnapshot;
use reverse_rusty::{PitId, QueryScope, RankProgramSpec, TopKOptions};

use super::delivery::DeliveryResult;
use super::record_outcome;

/// The page shape of one v2 search request, extracted by `prepare` (which
/// enforces pit-XOR-cursor); tokens are verified later, mode-side.
pub(in crate::handlers::search) enum PageRequest {
    None,
    Pit(String),
    Cursor(String),
}

/// What a pit-bound page needs to mint its continuation.
pub(in crate::handlers::search) struct MintCtx {
    pub(in crate::handlers::search) pit: PitId,
    pub(in crate::handlers::search) fingerprint: [u8; 32],
}

/// A resolved local page: the (possibly pinned) snapshot to serve from, the
/// collector boundary, and the minting context when pit-bound.
pub(in crate::handlers::search) struct LocalPagePlan {
    pub(in crate::handlers::search) snapshot: Arc<EngineSnapshot>,
    pub(in crate::handlers::search) search_after: Option<(i64, u64)>,
    pub(in crate::handlers::search) mint: Option<MintCtx>,
}

/// Resolve one local page: verify the token, pin the PIT snapshot (renewing
/// its keep-alive), and enforce the cursor fingerprint against the resent
/// request. Registry work is a short in-memory mutex hold — safe on the
/// request path.
pub(in crate::handlers::search) fn resolve_local(
    state: &AppState,
    page: &PageRequest,
    title: &str,
    scope: QueryScope,
    rank: &RankProgramSpec,
    filter: &[(String, Vec<String>)],
) -> Result<LocalPagePlan, (StatusCode, Json<ApiError>)> {
    let (pit, after, expected_fingerprint) = match page {
        PageRequest::None => {
            return Ok(LocalPagePlan {
                snapshot: state.snapshot.load_full(),
                search_after: None,
                mint: None,
            });
        }
        PageRequest::Pit(token) => match state.pit_tokens.verify_pit(token) {
            Ok(pit) => (pit, None, None),
            Err(error) => {
                record_token_failure(state, error, scope);
                return Err(token_failure_response(error));
            }
        },
        PageRequest::Cursor(token) => match state.pit_tokens.verify_cursor(token) {
            Ok(payload) => (payload.pit, Some(payload.after), Some(payload.fingerprint)),
            Err(error) => {
                record_token_failure(state, error, scope);
                return Err(token_failure_response(error));
            }
        },
    };

    let touched = {
        let now = Instant::now();
        let mut pits = state.pits.lock();
        // Lazy reap on every touch keeps expired pins from surviving in the
        // registry; dropping the reaped Arcs IS the release locally.
        drop(pits.reap_expired(now));
        let touched = pits.touch(pit, now).map(Arc::clone);
        state.prom.open_pits.set(pits.len() as i64);
        touched
    };
    let Some(snapshot) = touched else {
        record_outcome(&state.prom, "stale_cursor", scope);
        return Err(stale_cursor_response());
    };

    // The fingerprint is computed against the PINNED snapshot's normalizer +
    // dict, so a live vocab change cannot silently re-tokenize an in-flight
    // cursor.
    let fingerprint = request_fingerprint(
        snapshot.normalizer(),
        snapshot.dict(),
        title,
        scope,
        rank,
        filter,
    );
    if let Some(expected) = expected_fingerprint {
        if fingerprint != expected {
            record_outcome(&state.prom, "cursor_mismatch", scope);
            return Err(cursor_mismatch_response());
        }
    }
    Ok(LocalPagePlan {
        snapshot,
        search_after: after,
        mint: Some(MintCtx { pit, fingerprint }),
    })
}

fn record_token_failure(state: &AppState, error: TokenError, scope: QueryScope) {
    let outcome = match error {
        TokenError::Malformed => "validation",
        TokenError::BadMac => "stale_cursor",
    };
    record_outcome(&state.prom, outcome, scope);
}

/// Mint the continuation cursor onto a successful pit-bound page: only a FULL
/// page continues (`hits.len() == size`, size > 0); a short page is the end of
/// the ranked stream and returns no cursor.
pub(in crate::handlers::search) fn attach_next_cursor(
    tokens: &PitTokens,
    mint: &MintCtx,
    options: TopKOptions,
    result: &mut DeliveryResult,
) {
    if options.size == 0 || result.hits.len() != options.size {
        return;
    }
    if let Some(last) = result.last_ranked {
        result.next_cursor = Some(tokens.mint_cursor(mint.pit, last, mint.fingerprint));
    }
}
