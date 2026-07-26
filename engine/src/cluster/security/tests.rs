use super::*;
use http_body::Body as _;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

fn coordinator_claim_request(id: u64, handshake: bool) -> Request<()> {
    let mut request = Request::new(());
    request.metadata_mut().insert(
        COORDINATOR_ID_HEADER,
        id.to_string().parse().expect("metadata"),
    );
    request
        .metadata_mut()
        .insert(COORDINATOR_CLAIM_HEADER, MetadataValue::from_static("1"));
    if handshake {
        request.extensions_mut().insert(CoordinatorClaimHandshake);
    }
    request
}

fn verify(expected: Option<&str>, presented: Option<&str>) -> Result<(), Status> {
    let mut v = MeshAuthVerify::new(expected.map(|t| t.as_bytes().to_vec()));
    let mut req = Request::new(());
    if let Some(p) = presented {
        req.metadata_mut()
            .insert("authorization", p.parse().expect("header"));
    }
    v.call(req).map(|_| ())
}

#[test]
fn resolve_validates_like_the_http_gate() {
    assert_eq!(
        resolve_mesh_token(None, Err(std::env::VarError::NotPresent)),
        Ok(None)
    );
    assert_eq!(
        resolve_mesh_token(Some("s3cret".into()), Err(std::env::VarError::NotPresent)),
        Ok(Some(b"s3cret".to_vec()))
    );
    // Flag wins over env.
    assert_eq!(
        resolve_mesh_token(Some("flag".into()), Ok("env".into())),
        Ok(Some(b"flag".to_vec()))
    );
    assert!(resolve_mesh_token(Some(String::new()), Err(std::env::VarError::NotPresent)).is_err());
    assert!(resolve_mesh_token(
        Some("has space".into()),
        Err(std::env::VarError::NotPresent)
    )
    .is_err());
    assert!(
        resolve_mesh_token(Some("ünïcode".into()), Err(std::env::VarError::NotPresent)).is_err()
    );
}

#[test]
fn verifier_gates_only_when_configured() {
    // No expected token ⇒ pass-through (the historical open behavior).
    assert!(verify(None, None).is_ok());
    assert!(verify(None, Some("Bearer anything")).is_ok());
    // Expected token ⇒ exact match required; missing/wrong are UNAUTHENTICATED.
    assert!(verify(Some("tok"), Some("Bearer tok")).is_ok());
    assert!(
        verify(Some("tok"), Some("bearer tok")).is_ok(),
        "scheme case-insensitive"
    );
    assert!(verify(Some("tok"), None).is_err());
    assert!(verify(Some("tok"), Some("Bearer wrong")).is_err());
    assert!(verify(Some("tok"), Some("Basic tok")).is_err());
}

#[test]
fn injector_attaches_the_bearer_header() {
    let mut inj = MeshAuthInject::new(Some(b"tok")).expect("inject");
    let req = inj.call(Request::new(())).expect("call");
    assert_eq!(
        req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok()),
        Some("Bearer tok")
    );
    // No token ⇒ no header (byte-identical plaintext path).
    let mut inj = MeshAuthInject::new(None).expect("inject");
    let req = inj.call(Request::new(())).expect("call");
    assert!(req.metadata().get("authorization").is_none());
}

#[test]
fn injector_attaches_the_coordinator_identity() {
    let mut inj = MeshAuthInject::with_coordinator(None, Some(42)).expect("coordinator injector");
    let req = inj.call(Request::new(())).expect("call");
    assert_eq!(
        req.metadata()
            .get(COORDINATOR_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("42")
    );
    assert!(req.metadata().get(COORDINATOR_CLAIM_HEADER).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn coordinator_lease_is_exclusive_and_sticky() {
    let lease = Arc::new(CoordinatorLease::new());
    let mut verify = MeshAuthVerify::with_coordinator_lease(None, Arc::clone(&lease));

    // Compatibility traffic before adoption does not claim the node.
    verify
        .call(Request::new(()))
        .expect("unowned compatibility request");
    assert_eq!(lease.owner(), 0);

    let first = coordinator_claim_request(41, true);
    let first = verify.call(first).expect("unowned node admits handshake");
    claim_coordinator(
        &lease,
        request_coordinator_id(&first).expect("valid coordinator metadata"),
    )
    .await
    .expect("first coordinator claims");
    assert_eq!(lease.owner(), 41);

    let mut same = Request::new(());
    same.metadata_mut()
        .insert(COORDINATOR_ID_HEADER, "41".parse().expect("metadata"));
    verify.call(same).expect("owner remains authorized");

    let mut other = Request::new(());
    other
        .metadata_mut()
        .insert(COORDINATOR_ID_HEADER, "42".parse().expect("metadata"));
    assert_eq!(
        verify
            .call(other)
            .expect_err("another coordinator must be rejected")
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        verify
            .call(Request::new(()))
            .expect_err("unstamped traffic must be rejected after claim")
            .code(),
        tonic::Code::FailedPrecondition
    );
}

#[tokio::test(flavor = "current_thread")]
async fn coordinator_claim_drains_a_preclaim_handler_before_publishing() {
    let lease = Arc::new(CoordinatorLease::new());
    let in_flight = lease
        .begin_unstamped()
        .expect("unowned request receives an in-flight guard");
    let claiming = Arc::clone(&lease);
    let claimed = tokio::spawn(async move { claiming.claim(73).await });

    let wait = std::time::Instant::now();
    while !lease.is_claiming() {
        assert!(
            wait.elapsed() < Duration::from_secs(1),
            "claim did not enter the draining transition"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(
        lease.owner(),
        0,
        "ownership became visible before the old handler drained"
    );
    assert!(
        lease.begin_unstamped().is_none(),
        "new unstamped handlers must stop entering during the claim"
    );

    drop(in_flight);
    claimed
        .await
        .expect("claim task")
        .expect("claim succeeds after drain");
    assert_eq!(lease.owner(), 73);
}

#[tokio::test(flavor = "current_thread")]
async fn a_waiting_claimant_cannot_be_overwritten_by_a_rival() {
    let lease = Arc::new(CoordinatorLease::new());
    let in_flight = lease
        .begin_unstamped()
        .expect("unowned request receives an in-flight guard");

    let first_lease = Arc::clone(&lease);
    let first = tokio::spawn(async move { first_lease.claim(81).await });
    while !lease.is_claiming() {
        tokio::task::yield_now().await;
    }

    let second_lease = Arc::clone(&lease);
    let second = tokio::spawn(async move { second_lease.claim(82).await });
    assert_eq!(
        second
            .await
            .expect("rival claim task")
            .expect_err("rival cannot join an in-progress claim")
            .code(),
        tonic::Code::FailedPrecondition
    );

    drop(in_flight);
    first
        .await
        .expect("first claim task")
        .expect("first claimant wins after drain");
    assert_eq!(lease.owner(), 81);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_claim_reopens_the_unowned_transition() {
    let lease = Arc::new(CoordinatorLease::new());
    let in_flight = lease
        .begin_unstamped()
        .expect("unowned request receives an in-flight guard");
    let claiming = Arc::clone(&lease);
    let claim = tokio::spawn(async move { claiming.claim(91).await });
    while !lease.is_claiming() {
        tokio::task::yield_now().await;
    }

    claim.abort();
    assert!(claim
        .await
        .expect_err("claim task must be cancelled")
        .is_cancelled());
    assert!(
        !lease.is_claiming(),
        "a cancelled sole waiter must not strand the claim transition"
    );
    let newly_admitted = lease
        .begin_unstamped()
        .expect("compatibility admission must reopen after cancellation");
    drop(newly_admitted);
    drop(in_flight);

    lease
        .claim(92)
        .await
        .expect("a later coordinator can claim the unowned process");
    assert_eq!(lease.owner(), 92);
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_unstamped_admission_stays_rejected_after_claim_cancellation() {
    let lease = Arc::new(CoordinatorLease::new());
    let in_flight = lease
        .begin_unstamped()
        .expect("unowned request receives an in-flight guard");
    let claiming = Arc::clone(&lease);
    let claim = tokio::spawn(async move { claiming.claim(96).await });
    while !lease.is_claiming() {
        tokio::task::yield_now().await;
    }

    // This is the decision the outer HTTP service records while the claim
    // is draining. Cancel the claim before the interceptor sees it: the
    // extension, not a racy second state read, must remain authoritative.
    let mut request = Request::new(());
    request
        .extensions_mut()
        .insert(CoordinatorAdmission::Rejected);
    claim.abort();
    assert!(claim
        .await
        .expect_err("claim task must be cancelled")
        .is_cancelled());
    assert!(!lease.is_claiming());

    let mut verify = MeshAuthVerify::with_coordinator_lease(None, Arc::clone(&lease));
    assert_eq!(
        verify
            .call(request)
            .expect_err("rejected admission must not be resurrected")
            .code(),
        tonic::Code::FailedPrecondition
    );
    drop(in_flight);
}

#[tokio::test(flavor = "current_thread")]
async fn expired_owner_takeover_drains_an_active_response_body() {
    let lease = Arc::new(CoordinatorLease::with_ttl(Duration::from_millis(20)));
    lease.claim(97).await.expect("first owner");
    let active = lease
        .begin_owner(97)
        .expect("current owner call is admitted");
    assert!(lease.authorize_owner(97));
    tokio::time::sleep(Duration::from_millis(40)).await;

    let replacement_lease = Arc::clone(&lease);
    let replacement = tokio::spawn(async move { replacement_lease.claim(98).await });
    while !lease.is_claiming() {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        lease.owner(),
        97,
        "replacement published before the old response body drained"
    );

    drop(active);
    replacement
        .await
        .expect("replacement task")
        .expect("replacement claims after drain");
    assert_eq!(lease.owner(), 98);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_one_same_id_waiter_preserves_the_other() {
    let lease = Arc::new(CoordinatorLease::new());
    let in_flight = lease
        .begin_unstamped()
        .expect("unowned request receives an in-flight guard");
    let first_lease = Arc::clone(&lease);
    let first = tokio::spawn(async move { first_lease.claim(93).await });
    let second_lease = Arc::clone(&lease);
    let second = tokio::spawn(async move { second_lease.claim(93).await });
    while lease.claim_waiters() != 2 {
        tokio::task::yield_now().await;
    }

    first.abort();
    assert!(first
        .await
        .expect_err("first claim task must be cancelled")
        .is_cancelled());
    assert_eq!(
        lease.claim_waiters(),
        1,
        "one cancellation must not reopen admission around a live same-id waiter"
    );
    assert!(lease.begin_unstamped().is_none());

    drop(in_flight);
    second
        .await
        .expect("second claim task")
        .expect("remaining same-id waiter claims after the drain");
    assert_eq!(lease.owner(), 93);
}

#[test]
fn claim_metadata_is_rejected_outside_ownership_handshakes() {
    let lease = Arc::new(CoordinatorLease::new());
    let mut verify = MeshAuthVerify::with_coordinator_lease(None, Arc::clone(&lease));
    let error = verify
        .call(coordinator_claim_request(94, false))
        .expect_err("claim capability must not authorize an arbitrary RPC");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(lease.owner(), 0);

    verify
        .call(coordinator_claim_request(94, true))
        .expect("the same metadata is valid on AdoptDict/AddShard");
}

struct OneFrameBody {
    sent: bool,
}

impl http_body::Body for OneFrameBody {
    type Data = tonic::codegen::Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.sent {
            Poll::Ready(None)
        } else {
            self.sent = true;
            Poll::Ready(Some(Ok(http_body::Frame::data(
                tonic::codegen::Bytes::from_static(b"frame"),
            ))))
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn preclaim_guard_lives_through_the_stream_body() {
    let lease = Arc::new(CoordinatorLease::new());
    let unstamped = lease
        .begin_unstamped()
        .expect("unowned stream receives an in-flight guard");
    let mut body = LeaseTrackedBody::new(OneFrameBody { sent: false }, Some(unstamped));
    let claiming = Arc::clone(&lease);
    let claim = tokio::spawn(async move { claiming.claim(95).await });
    while !lease.is_claiming() {
        tokio::task::yield_now().await;
    }

    let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context))
        .await
        .expect("first body frame")
        .expect("valid body frame");
    assert_eq!(frame.into_data().expect("data frame"), &b"frame"[..]);
    assert_eq!(
        lease.owner(),
        0,
        "returning and polling a streaming response must not end its pre-claim guard"
    );

    let eof = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await;
    assert!(eof.is_none());
    claim
        .await
        .expect("claim task")
        .expect("claim succeeds only after stream EOF");
    assert_eq!(lease.owner(), 95);
}

#[test]
fn fresh_coordinator_ids_are_nonzero_and_distinct() {
    let first = fresh_coordinator_id();
    let second = fresh_coordinator_id();
    assert_ne!(first, 0);
    assert_ne!(second, 0);
    assert_ne!(first, second);
}

#[test]
fn round_trip_inject_then_verify() {
    let mut inj = MeshAuthInject::new(Some(b"mesh-secret-1")).expect("inject");
    let req = inj.call(Request::new(())).expect("inject call");
    let mut v = MeshAuthVerify::new(Some(b"mesh-secret-1".to_vec()));
    assert!(v.call(req).is_ok());
}
