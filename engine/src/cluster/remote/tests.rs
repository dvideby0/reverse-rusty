use super::*;

fn placed(tags: Vec<(String, String)>, tag_ids: Vec<crate::tagdict::TagId>) -> PlacedQuery {
    let norm = crate::normalize::Normalizer::default_vocab().expect("vocab");
    let mut dict = crate::dict::Dict::new();
    let mut lc = String::new();
    let ast = crate::dsl::parse("1994 north star").expect("parse");
    let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
    PlacedQuery {
        logical: 1,
        ex,
        dsl: "1994 north star".into(),
        version: 1,
        source_generation: None,
        tags,
        tag_ids,
        rank: crate::rank::RankValues::default(),
        placement: crate::ownership::QueryPlacement::standalone(),
    }
}

#[test]
fn wire_guard_passes_raw_tags_and_refuses_pre_resolved_ids() {
    // Raw (key,value) tags are the supported wire shape — no refusal.
    let raw = placed(vec![("category".into(), "items".into())], Vec::new());
    assert!(refuse_wire_tag_ids(std::slice::from_ref(&raw)).is_ok());
    // Pre-resolved ids (the ADR-074 carry-through) must be refused loudly.
    let carried = placed(
        Vec::new(),
        vec![crate::tagdict::synthetic_tag_id("region", "emea")],
    );
    let err =
        refuse_wire_tag_ids(&[raw, carried]).expect_err("ids must not cross the process boundary");
    assert!(
        format!("{err}").contains("process boundary"),
        "the refusal names the boundary: {err}"
    );
}

// ---- ADR-085 transport call-seam logic: bounded retry + per-call timeout ----

use std::sync::atomic::{AtomicU32, Ordering};

fn unavailable() -> tonic::Status {
    tonic::Status::unavailable("transient")
}

#[test]
fn timeout_bridge_constructs_its_timer_inside_the_runtime() {
    // This test runs on a plain libtest worker, the same non-Tokio context
    // as the exhaustive Rayon pool. Constructing `tokio::time::timeout`
    // before entering `runtime.handle()` panics immediately.
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let result =
        block_on_timeout_in_context(runtime.handle(), Duration::from_secs(1), async { 42u32 })
            .expect("immediate future completes before timeout");
    assert_eq!(result, 42);
}

#[tokio::test]
async fn retry_recovers_idempotent_read_after_transient_unavailable() {
    // Two transient UNAVAILABLEs then success, 2 retries allowed → Ok, 2 attempts spent.
    let calls = AtomicU32::new(0);
    let (res, attempts, timed_out) = run_with_retry(
        || {
            let n = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if n < 2 {
                    Err::<u32, _>(unavailable())
                } else {
                    Ok(42u32)
                }
            }
        },
        None,
        2,
    )
    .await;
    assert_eq!(res.ok(), Some(42));
    assert_eq!(attempts, 2);
    assert!(!timed_out);
    assert_eq!(calls.load(Ordering::Relaxed), 3, "1 initial + 2 retries");
}

#[tokio::test]
async fn retry_gives_up_after_max_and_fails_loud() {
    // Always UNAVAILABLE, 2 retries → still Err (fail loud), 2 attempts spent.
    let calls = AtomicU32::new(0);
    let (res, attempts, timed_out) = run_with_retry(
        || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err::<u32, _>(unavailable()) }
        },
        None,
        2,
    )
    .await;
    assert!(res.is_err());
    assert_eq!(attempts, 2);
    assert!(!timed_out);
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn non_transient_error_is_not_retried() {
    // A non-UNAVAILABLE status is permanent — no retry even with retries allowed.
    let calls = AtomicU32::new(0);
    let (res, attempts, _timed_out) = run_with_retry(
        || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err::<u32, _>(tonic::Status::invalid_argument("permanent")) }
        },
        None,
        5,
    )
    .await;
    assert!(res.is_err());
    assert_eq!(attempts, 0, "permanent errors do not retry");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn writes_pass_zero_retries_and_fail_loud_on_transient() {
    // max_retries = 0 (the write path) → a transient error is NOT retried.
    let calls = AtomicU32::new(0);
    let (res, attempts, _) = run_with_retry(
        || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err::<u32, _>(unavailable()) }
        },
        None,
        0,
    )
    .await;
    assert!(res.is_err());
    assert_eq!(attempts, 0);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn deadline_fires_on_a_hung_call_and_is_reported_as_timeout() {
    // A future that never completes + a short deadline → loud timeout (not a hang).
    let (res, attempts, timed_out) = run_with_retry(
        std::future::pending::<Result<u32, tonic::Status>>,
        Some(Duration::from_millis(50)),
        0,
    )
    .await;
    assert!(res.is_err());
    assert!(timed_out, "a deadline-exceeded must classify as a timeout");
    assert_eq!(attempts, 0);
}

#[tokio::test]
async fn read_timeout_is_retried_then_fails_loud() {
    // A hung read WITH retries: each attempt times out; after the budget it fails loud,
    // still classified as a timeout. Proves a hung shard can never block forever.
    let (res, attempts, timed_out) = run_with_retry(
        std::future::pending::<Result<u32, tonic::Status>>,
        Some(Duration::from_millis(20)),
        2,
    )
    .await;
    assert!(res.is_err());
    assert!(timed_out);
    assert_eq!(attempts, 2);
}

#[test]
fn backoff_is_exponential_and_capped() {
    assert_eq!(backoff_delay(1), Duration::from_millis(50));
    assert_eq!(backoff_delay(2), Duration::from_millis(100));
    assert_eq!(backoff_delay(3), Duration::from_millis(200));
    // Capped at 1s for large attempt counts.
    assert_eq!(backoff_delay(20), Duration::from_secs(1));
}

#[test]
fn transient_covers_unavailable_and_transport_not_ready() {
    assert!(is_transient(&tonic::Status::unavailable("x")));
    // tonic's "channel not ready" transport failure surfaces as UNKNOWN — transient.
    assert!(is_transient(&tonic::Status::unknown(
        "Service was not ready: transport error"
    )));
    // An arbitrary application-level UNKNOWN is NOT retried.
    assert!(!is_transient(&tonic::Status::unknown("app boom")));
    assert!(!is_transient(&tonic::Status::invalid_argument("x")));
    assert!(!is_transient(&tonic::Status::internal("x")));
    assert!(!is_transient(&tonic::Status::deadline_exceeded("x")));
}

/// The legacy seam preserves server messages; only the ranked seam
/// reconstructs typed errors, and only when code AND message form agree
/// (review finding: a relocated slot's NotFound was retyped into a phantom
/// "source unavailable for logical id 0" on every RPC).
#[test]
fn legacy_rpc_err_preserves_messages_ranked_seam_reconstructs() {
    let slot_missing = tonic::Status::not_found("shard 3 is not hosted on this node");
    assert!(matches!(
        rpc_err(&slot_missing),
        ShardError::Remote(ref m) if m.contains("not hosted")
    ));
    assert!(matches!(
        rpc_err(&tonic::Status::internal("ownership sweep failed")),
        ShardError::Remote(_)
    ));
    assert!(matches!(
        rpc_err(&tonic::Status::deadline_exceeded("x")),
        ShardError::DeadlineExceeded
    ));

    assert!(matches!(
        ranked_rpc_err(&tonic::Status::not_found(
            "source unavailable for logical id 42"
        )),
        ShardError::SourceUnavailable(42)
    ));
    assert!(matches!(
        ranked_rpc_err(&slot_missing),
        ShardError::Remote(ref m) if m.contains("not hosted")
    ));
    assert!(matches!(
        ranked_rpc_err(&tonic::Status::resource_exhausted(
            "ranked enrichment byte credit exhausted before source materialization"
        )),
        ShardError::EnrichmentLimit { .. }
    ));
    assert!(matches!(
        ranked_rpc_err(&tonic::Status::failed_precondition(
            "placement configuration mismatch"
        )),
        ShardError::OwnershipMismatch(_)
    ));
    // Code gating: an internal error mentioning ownership stays Remote.
    assert!(matches!(
        ranked_rpc_err(&tonic::Status::internal("ownership sweep failed")),
        ShardError::Remote(_)
    ));
}

/// ADR-111: with the structured metadata code present, reconstruction no
/// longer depends on the message at all — a deliberately scrambled message
/// still yields the typed error (and the true argument). The frozen-message
/// arms above remain the version-skew fallback.
#[test]
fn ranked_seam_prefers_metadata_over_message_substrings() {
    use crate::cluster::ranked_wire::{attach, RankedWireCode};
    assert!(matches!(
        ranked_rpc_err(&attach(
            tonic::Status::not_found("scrambled"),
            RankedWireCode::SourceUnavailable,
            Some(42),
        )),
        ShardError::SourceUnavailable(42)
    ));
    assert!(matches!(
        ranked_rpc_err(&attach(
            tonic::Status::resource_exhausted("scrambled"),
            RankedWireCode::EnrichmentLimit,
            Some(1024),
        )),
        ShardError::EnrichmentLimit { limit: 1024 }
    ));
    assert!(matches!(
        ranked_rpc_err(&attach(
            tonic::Status::failed_precondition("scrambled"),
            RankedWireCode::OwnershipMismatch,
            None,
        )),
        ShardError::OwnershipMismatch(_)
    ));
    // The codex-review case: a MARKED protocol failure whose message
    // contains "ownership" must stay Protocol — the metadata short-circuits
    // the substring ladder that would have retyped it.
    assert!(matches!(
        ranked_rpc_err(&attach(
            tonic::Status::failed_precondition(
                "shard protocol error: missing bounded/ownership attestation"
            ),
            RankedWireCode::Protocol,
            None,
        )),
        ShardError::Protocol(ref m) if m.contains("ownership attestation")
    ));
}
