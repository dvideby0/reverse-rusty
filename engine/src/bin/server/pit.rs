//! PIT/cursor token minting and verification (ADR-113, serving layer).
//!
//! The engine-side [`reverse_rusty::PitRegistry`] speaks process-local ids;
//! this module wraps them into the client-facing opaque tokens: a fixed binary
//! layout, HMAC-SHA256-tagged, hex-encoded. The MAC key is generated per
//! process (from two `uuid` v4 draws — 244 bits of OS entropy), which makes
//! every outstanding token die with the process BY DESIGN: a restarted server
//! has lost its pinned snapshots, so a token that cannot even authenticate is
//! exactly as stale as the state it referenced (409, fail closed).
//!
//! Cursor tokens additionally carry the last `(score, logical_id)` boundary
//! and a SHA-256 fingerprint of the request semantics (normalized title,
//! query scope, rank program, resolved filter). The client must resend the
//! same query with the cursor; the fingerprint turns "same cursor, different
//! query" into a typed 400 instead of silently wrong pages. `size`,
//! `timeout_ms`, and `track_total_hits_up_to` are deliberately NOT
//! fingerprinted — they may vary per page.

use axum::{http::StatusCode, Json};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::dto::ApiError;

use reverse_rusty::{Normalizer, PitError, PitId, QueryScope, RankProgramSpec};

type HmacSha256 = Hmac<Sha256>;

const TOKEN_VERSION: u8 = 1;
const KIND_PIT: u8 = b'P';
const KIND_CURSOR: u8 = b'C';
const TAG_LEN: usize = 32;
const PIT_BODY_LEN: usize = 10; // version + kind + pit_id
const CURSOR_BODY_LEN: usize = 58; // version + kind + pit_id + score + logical + fingerprint

/// Token verification failures. `Malformed` is a client error (400);
/// `BadMac` means a token from another process/generation — stale (409).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TokenError {
    Malformed,
    BadMac,
}

/// Decoded cursor payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CursorPayload {
    pub(crate) pit: PitId,
    pub(crate) after: (i64, u64),
    pub(crate) fingerprint: [u8; 32],
}

/// Per-process HMAC key for PIT + cursor tokens.
pub(crate) struct PitTokens {
    key: [u8; 32],
}

impl PitTokens {
    /// Generate a fresh per-process key from OS randomness (two v4 uuids).
    pub(crate) fn generate() -> Self {
        let mut key = [0u8; 32];
        key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        Self { key }
    }

    fn tag(&self, body: &[u8]) -> [u8; TAG_LEN] {
        // `new_from_slice` only errs on an invalid key length; ours is fixed.
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .unwrap_or_else(|_| unreachable!("HMAC accepts any key length"));
        mac.update(body);
        mac.finalize().into_bytes().into()
    }

    fn verify(&self, body: &[u8], tag: &[u8]) -> Result<(), TokenError> {
        let mut mac = HmacSha256::new_from_slice(&self.key)
            .unwrap_or_else(|_| unreachable!("HMAC accepts any key length"));
        mac.update(body);
        // Constant-time comparison via the hmac crate.
        mac.verify_slice(tag).map_err(|_| TokenError::BadMac)
    }

    fn mint(&self, body: &[u8]) -> String {
        let tag = self.tag(body);
        let mut token = Vec::with_capacity(body.len() + TAG_LEN);
        token.extend_from_slice(body);
        token.extend_from_slice(&tag);
        hex_encode(&token)
    }

    /// Decode + authenticate a token of the expected kind, returning its body.
    fn open(&self, token: &str, kind: u8, body_len: usize) -> Result<Vec<u8>, TokenError> {
        let raw = hex_decode(token).ok_or(TokenError::Malformed)?;
        if raw.len() != body_len + TAG_LEN {
            return Err(TokenError::Malformed);
        }
        let (body, tag) = raw.split_at(body_len);
        if body[0] != TOKEN_VERSION || body[1] != kind {
            return Err(TokenError::Malformed);
        }
        self.verify(body, tag)?;
        Ok(body.to_vec())
    }

    pub(crate) fn mint_pit(&self, pit: PitId) -> String {
        let mut body = [0u8; PIT_BODY_LEN];
        body[0] = TOKEN_VERSION;
        body[1] = KIND_PIT;
        body[2..10].copy_from_slice(&pit.0.to_le_bytes());
        self.mint(&body)
    }

    pub(crate) fn verify_pit(&self, token: &str) -> Result<PitId, TokenError> {
        let body = self.open(token, KIND_PIT, PIT_BODY_LEN)?;
        Ok(PitId(u64::from_le_bytes(
            body[2..10].try_into().map_err(|_| TokenError::Malformed)?,
        )))
    }

    pub(crate) fn mint_cursor(
        &self,
        pit: PitId,
        after: (i64, u64),
        fingerprint: [u8; 32],
    ) -> String {
        let mut body = [0u8; CURSOR_BODY_LEN];
        body[0] = TOKEN_VERSION;
        body[1] = KIND_CURSOR;
        body[2..10].copy_from_slice(&pit.0.to_le_bytes());
        body[10..18].copy_from_slice(&after.0.to_le_bytes());
        body[18..26].copy_from_slice(&after.1.to_le_bytes());
        body[26..58].copy_from_slice(&fingerprint);
        self.mint(&body)
    }

    pub(crate) fn verify_cursor(&self, token: &str) -> Result<CursorPayload, TokenError> {
        let body = self.open(token, KIND_CURSOR, CURSOR_BODY_LEN)?;
        let decode = |range: std::ops::Range<usize>| -> Result<[u8; 8], TokenError> {
            body[range].try_into().map_err(|_| TokenError::Malformed)
        };
        let mut fingerprint = [0u8; 32];
        fingerprint.copy_from_slice(&body[26..58]);
        Ok(CursorPayload {
            pit: PitId(u64::from_le_bytes(decode(2..10)?)),
            after: (
                i64::from_le_bytes(decode(10..18)?),
                u64::from_le_bytes(decode(18..26)?),
            ),
            fingerprint,
        })
    }
}

/// SHA-256 fingerprint of one request's page-invariant semantics. Uses the
/// PINNED snapshot's normalizer, so a vocab change on the live engine cannot
/// silently re-tokenize an in-flight cursor's title. Every component is
/// length-prefixed (no delimiter ambiguity); filter keys/values are sorted so
/// JSON field order cannot flip the fingerprint.
pub(crate) fn request_fingerprint(
    norm: &Normalizer,
    title: &str,
    scope: QueryScope,
    rank: &RankProgramSpec,
    filter: &[(String, Vec<String>)],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    let mut piece = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    };
    for token in norm.clean_tokens(title) {
        piece(token.as_bytes());
    }
    piece(&[match scope {
        QueryScope::Standard => 0u8,
        QueryScope::WithBroad => 1u8,
    }]);
    piece(rank.priority_field.as_deref().unwrap_or("").as_bytes());
    let mut boosts: Vec<&(String, String, i64)> = rank.boosts.iter().collect();
    boosts.sort();
    for (key, value, weight) in boosts {
        piece(key.as_bytes());
        piece(value.as_bytes());
        piece(&weight.to_le_bytes());
    }
    let mut filters: Vec<(String, Vec<String>)> = filter.to_vec();
    for (_, values) in &mut filters {
        values.sort();
    }
    filters.sort();
    for (key, values) in &filters {
        piece(key.as_bytes());
        for value in values {
            piece(value.as_bytes());
        }
    }
    hasher.finalize().into()
}

/// The one 409 every stale-PIT shape maps to (expired, closed, restarted
/// process, placement drift): fail closed, restart pagination.
pub(crate) fn stale_cursor_response() -> (StatusCode, Json<ApiError>) {
    ApiError::response(
        StatusCode::CONFLICT,
        "stale_cursor",
        "the referenced point-in-time is no longer available; open a new PIT and restart pagination",
    )
}

/// A valid cursor whose resent query differs from the fingerprinted original.
pub(crate) fn cursor_mismatch_response() -> (StatusCode, Json<ApiError>) {
    ApiError::response(
        StatusCode::BAD_REQUEST,
        "cursor_mismatch",
        "the request does not match the cursor's original document/query_scope/rank/filter",
    )
}

/// Map a token failure: garbage is the client's bug (400), a failed MAC is a
/// token from another process generation — stale by construction (409).
pub(crate) fn token_failure_response(error: TokenError) -> (StatusCode, Json<ApiError>) {
    match error {
        TokenError::Malformed => ApiError::response(
            StatusCode::BAD_REQUEST,
            "validation_error",
            "malformed pit/cursor token",
        ),
        TokenError::BadMac => stale_cursor_response(),
    }
}

/// Typed PIT admission mapping shared by the lifecycle endpoints: the cap is a
/// transient serving condition (429), a too-large keep-alive is a client error.
pub(crate) fn pit_error_response(error: PitError) -> (StatusCode, Json<ApiError>) {
    match error {
        PitError::LimitExceeded { .. } => ApiError::response(
            StatusCode::TOO_MANY_REQUESTS,
            "pit_limit_exceeded",
            error.to_string(),
        ),
        PitError::KeepAliveTooLarge { .. } => ApiError::response(
            StatusCode::BAD_REQUEST,
            "validation_error",
            error.to_string(),
        ),
    }
}

/// Convert a requested keep-alive into the registry's argument shape.
pub(crate) fn keep_alive_from_secs(secs: Option<u64>) -> Option<std::time::Duration> {
    secs.map(std::time::Duration::from_secs)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    let nibble = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    };
    let raw = text.as_bytes();
    let mut out = Vec::with_capacity(raw.len() / 2);
    for pair in raw.chunks_exact(2) {
        out.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm() -> Normalizer {
        Normalizer::default_vocab().expect("default vocab")
    }

    #[test]
    fn pit_and_cursor_tokens_round_trip() {
        let tokens = PitTokens::generate();
        let pit = PitId(42);
        assert_eq!(tokens.verify_pit(&tokens.mint_pit(pit)), Ok(pit));

        let payload = CursorPayload {
            pit,
            after: (-7, u64::MAX),
            fingerprint: [9u8; 32],
        };
        let minted = tokens.mint_cursor(payload.pit, payload.after, payload.fingerprint);
        assert_eq!(tokens.verify_cursor(&minted), Ok(payload));
    }

    #[test]
    fn tampering_and_garbage_are_typed() {
        let tokens = PitTokens::generate();
        let minted = tokens.mint_cursor(PitId(1), (5, 5), [0u8; 32]);

        // Flip one payload bit: MAC fails.
        let mut bytes: Vec<u8> = minted.into_bytes();
        bytes[4] = if bytes[4] == b'0' { b'1' } else { b'0' };
        let tampered = String::from_utf8(bytes).expect("hex stays utf8");
        assert_eq!(tokens.verify_cursor(&tampered), Err(TokenError::BadMac));

        // Structural garbage is Malformed, not BadMac.
        assert_eq!(tokens.verify_cursor("zz"), Err(TokenError::Malformed));
        assert_eq!(tokens.verify_cursor("abc"), Err(TokenError::Malformed));
        assert_eq!(
            tokens.verify_cursor(&hex_encode(&[0u8; 10])),
            Err(TokenError::Malformed)
        );

        // A pit token is not a cursor token (kind byte checked).
        let pit_token = tokens.mint_pit(PitId(1));
        assert_eq!(tokens.verify_cursor(&pit_token), Err(TokenError::Malformed));
    }

    #[test]
    fn another_process_key_is_stale() {
        let a = PitTokens::generate();
        let b = PitTokens::generate();
        let minted = a.mint_pit(PitId(3));
        assert_eq!(b.verify_pit(&minted), Err(TokenError::BadMac));
    }

    #[test]
    fn fingerprint_covers_semantics_and_ignores_paging_knobs() {
        let norm = norm();
        let rank = RankProgramSpec {
            priority_field: Some("priority".into()),
            boosts: vec![("tier".into(), "gold".into(), 100)],
        };
        let filter = vec![("tier".to_string(), vec!["gold".to_string()])];
        let base = request_fingerprint(&norm, "topps chrome", QueryScope::Standard, &rank, &filter);

        // Same request → same fingerprint; surface-noise title variants that
        // normalize identically also match (the fingerprint is over N(T)).
        assert_eq!(
            base,
            request_fingerprint(&norm, "topps chrome", QueryScope::Standard, &rank, &filter)
        );
        assert_eq!(
            base,
            request_fingerprint(
                &norm,
                "  Topps   CHROME ",
                QueryScope::Standard,
                &rank,
                &filter
            )
        );

        // Every covered component moves it.
        assert_ne!(
            base,
            request_fingerprint(&norm, "topps finest", QueryScope::Standard, &rank, &filter)
        );
        assert_ne!(
            base,
            request_fingerprint(&norm, "topps chrome", QueryScope::WithBroad, &rank, &filter)
        );
        assert_ne!(
            base,
            request_fingerprint(
                &norm,
                "topps chrome",
                QueryScope::Standard,
                &RankProgramSpec::default(),
                &filter
            )
        );
        assert_ne!(
            base,
            request_fingerprint(&norm, "topps chrome", QueryScope::Standard, &rank, &[])
        );

        // Filter value order is canonicalized.
        let two = vec![(
            "tier".to_string(),
            vec!["gold".to_string(), "silver".to_string()],
        )];
        let two_rev = vec![(
            "tier".to_string(),
            vec!["silver".to_string(), "gold".to_string()],
        )];
        assert_eq!(
            request_fingerprint(&norm, "topps chrome", QueryScope::Standard, &rank, &two),
            request_fingerprint(&norm, "topps chrome", QueryScope::Standard, &rank, &two_rev)
        );
    }
}
