use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;
use tonic::codegen::{http, Service};
use tonic::server::NamedService;
use tonic::Status;

use super::mesh::coordinator_lease_error;

pub(super) const COORDINATOR_ID_HEADER: &str = "x-reverse-rusty-coordinator-id";
pub(super) const COORDINATOR_CLAIM_HEADER: &str = "x-reverse-rusty-coordinator-claim";
const DEFAULT_COORDINATOR_LEASE_TTL: Duration = Duration::from_secs(30);

/// Process-local coordinator ownership plus the transition barrier that makes
/// the first claim linearizable with legacy, unstamped RPCs already executing.
///
/// The interceptor alone cannot close this race: an unstamped handler may pass
/// while `owner == 0`, then mutate after a later `AdoptDict` publishes a lease.
/// [`CoordinatorLeaseService`] counts each such handler for its complete service
/// future. A claim stops new unstamped admissions, drains the old ones, and only
/// then publishes the owner.
pub(crate) struct CoordinatorLease {
    owner: AtomicU64,
    transition: Mutex<CoordinatorTransition>,
    changed: Notify,
    install: tokio::sync::Mutex<()>,
    ttl: Duration,
}

#[derive(Default)]
struct CoordinatorTransition {
    active_unstamped: usize,
    active_owner: usize,
    owner_expires_at: Option<Instant>,
    claimant: Option<CoordinatorClaim>,
}

#[derive(Clone, Copy)]
struct CoordinatorClaim {
    candidate: u64,
    waiters: usize,
}

impl CoordinatorLease {
    pub(crate) fn new() -> Self {
        Self::with_ttl(DEFAULT_COORDINATOR_LEASE_TTL)
    }

    pub(super) fn with_ttl(ttl: Duration) -> Self {
        assert!(!ttl.is_zero(), "coordinator lease TTL must be positive");
        Self {
            owner: AtomicU64::new(0),
            transition: Mutex::new(CoordinatorTransition::default()),
            changed: Notify::new(),
            install: tokio::sync::Mutex::new(()),
            ttl,
        }
    }

    pub(crate) fn owner(&self) -> u64 {
        self.owner.load(Ordering::Acquire)
    }

    fn transition(&self) -> MutexGuard<'_, CoordinatorTransition> {
        self.transition
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn begin_unstamped(self: &Arc<Self>) -> Option<ActiveCoordinatorCall> {
        let mut transition = self.transition();
        if self.owner() != 0 || transition.claimant.is_some() {
            return None;
        }
        transition.active_unstamped += 1;
        Some(ActiveCoordinatorCall {
            lease: Arc::clone(self),
            kind: ActiveCoordinatorCallKind::Unstamped,
        })
    }

    pub(super) fn begin_owner(self: &Arc<Self>, candidate: u64) -> Option<ActiveCoordinatorCall> {
        let mut transition = self.transition();
        if self.owner() != candidate || transition.claimant.is_some() {
            return None;
        }
        transition.active_owner += 1;
        Some(ActiveCoordinatorCall {
            lease: Arc::clone(self),
            kind: ActiveCoordinatorCallKind::Owner,
        })
    }

    /// Complete authorization for a call already admitted by
    /// [`Self::begin_owner`]. Token verification runs between those two steps:
    /// an unauthenticated request may briefly hold a drain guard, but it cannot
    /// renew the lease and deny a legitimate restart.
    pub(super) fn authorize_owner(&self, candidate: u64) -> bool {
        let mut transition = self.transition();
        if self.owner() != candidate {
            return false;
        }
        if transition.claimant.is_none() {
            transition.owner_expires_at = Some(Instant::now() + self.ttl);
        }
        true
    }

    fn register_claim(&self, candidate: u64) -> Result<ClaimRegistration<'_>, Status> {
        let mut transition = self.transition();
        let now = Instant::now();
        let owner = self.owner();
        if owner == candidate {
            if transition
                .claimant
                .is_some_and(|claim| claim.candidate != candidate)
            {
                return Err(coordinator_lease_error());
            }
            transition.owner_expires_at = Some(now + self.ttl);
            return Ok(ClaimRegistration::Owned);
        }
        if owner != 0 {
            let expires_at = transition.owner_expires_at.unwrap_or(now);
            if expires_at > now {
                return Err(coordinator_lease_error());
            }
        }
        match &mut transition.claimant {
            Some(claim) if claim.candidate != candidate => return Err(coordinator_lease_error()),
            Some(claim) => {
                claim.waiters = claim.waiters.checked_add(1).ok_or_else(|| {
                    Status::resource_exhausted("too many concurrent coordinator claim waiters")
                })?;
            }
            None => {
                transition.claimant = Some(CoordinatorClaim {
                    candidate,
                    waiters: 1,
                });
            }
        }
        Ok(ClaimRegistration::Registered(PendingClaim::registered(
            self, candidate,
        )))
    }

    pub(super) async fn claim(&self, candidate: u64) -> Result<(), Status> {
        let _pending = match self.register_claim(candidate)? {
            ClaimRegistration::Owned => return Ok(()),
            ClaimRegistration::Registered(pending) => pending,
        };
        // Registration is represented by an RAII guard. If the handshake is
        // cancelled while it waits for old traffic to drain, the
        // final same-id waiter clears the transition and reopens compatibility
        // admission instead of wedging this process forever.
        loop {
            // Register before inspecting the state so a final `UnstampedCall`
            // cannot notify between our check and the await. `enable` puts this
            // waiter in Notify's queue while the transition mutex still
            // protects the predicate.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let wait = {
                let mut transition = self.transition();
                match self.owner() {
                    owner if owner == candidate => return Ok(()),
                    _ => {}
                }
                match transition.claimant {
                    Some(claim) if claim.candidate != candidate => {
                        return Err(coordinator_lease_error())
                    }
                    Some(_) => {}
                    // This future owns a registered waiter, so the transition
                    // cannot disappear unless ownership was published.
                    None => {
                        return Err(Status::internal(
                            "coordinator claim registration disappeared before publication",
                        ))
                    }
                }
                if transition.active_unstamped == 0 && transition.active_owner == 0 {
                    // Rechecked under the transition lock after every wake:
                    // a competing waiter can never overwrite an owner another
                    // claimant published while this future was suspended.
                    self.owner.store(candidate, Ordering::Release);
                    transition.owner_expires_at = Some(Instant::now() + self.ttl);
                    transition.claimant = None;
                    false
                } else {
                    true
                }
            };

            if !wait {
                self.changed.notify_waiters();
                return Ok(());
            }
            // Await rather than blocking a Tokio worker. The old async RPC
            // remains free to finish and drop its `UnstampedCall`.
            notified.await;
        }
    }

    pub(crate) fn is_claiming(&self) -> bool {
        self.transition().claimant.is_some()
    }

    #[cfg(test)]
    pub(crate) fn hold_unstamped_for_test(self: &Arc<Self>) -> impl Drop {
        self.begin_unstamped()
            .expect("test setup requires an unowned coordinator lease")
    }

    #[cfg(test)]
    pub(super) fn claim_waiters(&self) -> usize {
        self.transition().claimant.map_or(0, |claim| claim.waiters)
    }

    /// Serialize node/slot installation after the ownership transition.
    /// Same-id retries are authorized concurrently, but they must not both
    /// build and replace one slot around an intervening write.
    pub(crate) async fn lock_install(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.install.lock().await
    }
}

enum ClaimRegistration<'a> {
    Owned,
    Registered(PendingClaim<'a>),
}

struct PendingClaim<'a> {
    lease: &'a CoordinatorLease,
    candidate: u64,
}

impl<'a> PendingClaim<'a> {
    fn registered(lease: &'a CoordinatorLease, candidate: u64) -> Self {
        Self { lease, candidate }
    }
}

impl Drop for PendingClaim<'_> {
    fn drop(&mut self) {
        let mut transition = self.lease.transition();
        let Some(claim) = &mut transition.claimant else {
            // Another same-id waiter published ownership and cleared the
            // registration set before this future observed the owner.
            return;
        };
        if claim.candidate != self.candidate {
            return;
        }
        claim.waiters = claim.waiters.saturating_sub(1);
        if claim.waiters == 0 {
            transition.claimant = None;
            self.lease.changed.notify_waiters();
        }
    }
}

#[derive(Clone, Copy)]
enum ActiveCoordinatorCallKind {
    Unstamped,
    Owner,
}

pub(super) struct ActiveCoordinatorCall {
    lease: Arc<CoordinatorLease>,
    kind: ActiveCoordinatorCallKind,
}

impl Drop for ActiveCoordinatorCall {
    fn drop(&mut self) {
        let mut transition = self.lease.transition();
        match self.kind {
            ActiveCoordinatorCallKind::Unstamped => {
                transition.active_unstamped = transition.active_unstamped.saturating_sub(1);
            }
            ActiveCoordinatorCallKind::Owner => {
                transition.active_owner = transition.active_owner.saturating_sub(1);
            }
        }
        if transition.active_unstamped == 0 && transition.active_owner == 0 {
            self.lease.changed.notify_waiters();
        }
    }
}

/// Marker inserted from the HTTP route before Tonic strips the URI for its
/// metadata interceptor. Only the fingerprint probe and the two installation
/// handshakes may carry the one-shot claim capability.
#[derive(Clone, Copy)]
pub(super) struct CoordinatorClaimHandshake;

/// Admission is decided atomically in [`CoordinatorLeaseService`] before the
/// Tonic interceptor runs. Keeping the decision in request extensions closes
/// the check/use race where a claim could be cancelled between those layers
/// and accidentally turn a rejected unstamped request back into an admitted
/// one.
#[derive(Clone, Copy)]
pub(super) enum CoordinatorAdmission {
    Unstamped,
    Owner(u64),
    Claim,
    Rejected,
}

fn is_coordinator_claim_handshake(path: &str) -> bool {
    matches!(
        path,
        "/reverse_rusty.shard.v1.ShardService/DictFingerprint"
            | "/reverse_rusty.shard.v1.ShardService/AdoptDict"
            | "/reverse_rusty.shard.v1.ShardService/AddShard"
    )
}

/// Response-body wrapper that retains a pre-claim call until the complete gRPC
/// body reaches EOF or is dropped. A Tonic handler future for a server stream
/// completes as soon as it returns `Response<Stream>`; tying the guard only to
/// that future would let ownership publish while the old stream still runs.
pub(crate) struct LeaseTrackedBody<B> {
    inner: Pin<Box<B>>,
    active: Option<ActiveCoordinatorCall>,
}

impl<B> LeaseTrackedBody<B> {
    pub(super) fn new(inner: B, active: Option<ActiveCoordinatorCall>) -> Self {
        Self {
            inner: Box::pin(inner),
            active,
        }
    }
}

impl<B> http_body::Body for LeaseTrackedBody<B>
where
    B: http_body::Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let frame = this.inner.as_mut().poll_frame(context);
        if matches!(frame, Poll::Ready(None)) {
            this.active.take();
        }
        frame
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.as_ref().get_ref().size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().get_ref().is_end_stream()
    }
}

/// Wrap the generated shard service so an unstamped pre-claim RPC remains
/// represented in [`CoordinatorLease`] through its complete response body.
#[derive(Clone)]
pub(crate) struct CoordinatorLeaseService<S> {
    inner: S,
    lease: Arc<CoordinatorLease>,
}

impl<S> CoordinatorLeaseService<S> {
    pub(crate) fn new(inner: S, lease: Arc<CoordinatorLease>) -> Self {
        Self { inner, lease }
    }
}

impl<S, B, R> Service<http::Request<B>> for CoordinatorLeaseService<S>
where
    S: Service<http::Request<B>, Response = http::Response<R>> + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
    R: http_body::Body + Send + 'static,
{
    type Response = http::Response<LeaseTrackedBody<R>>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let claim_handshake = is_coordinator_claim_handshake(request.uri().path());
        if claim_handshake {
            request.extensions_mut().insert(CoordinatorClaimHandshake);
        }
        let claim_requested = request
            .headers()
            .get(COORDINATOR_CLAIM_HEADER)
            .is_some_and(|value| value.as_bytes() == b"1");
        let presented = request
            .headers()
            .get(COORDINATOR_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|id| *id != 0);
        let has_presented_header = request.headers().contains_key(COORDINATOR_ID_HEADER);
        let (admission, active) = if claim_requested && claim_handshake {
            (CoordinatorAdmission::Claim, None)
        } else if !has_presented_header {
            match self.lease.begin_unstamped() {
                Some(active) => (CoordinatorAdmission::Unstamped, Some(active)),
                None => (CoordinatorAdmission::Rejected, None),
            }
        } else if let Some(candidate) = presented {
            match self.lease.begin_owner(candidate) {
                Some(active) => (CoordinatorAdmission::Owner(candidate), Some(active)),
                None => (CoordinatorAdmission::Rejected, None),
            }
        } else {
            (CoordinatorAdmission::Rejected, None)
        };
        request.extensions_mut().insert(admission);
        let future = self.inner.call(request);
        Box::pin(async move {
            future.await.map(|response| {
                let (parts, body) = response.into_parts();
                http::Response::from_parts(parts, LeaseTrackedBody::new(body, active))
            })
        })
    }
}

impl<S> NamedService for CoordinatorLeaseService<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

/// One process-boot-unique, non-zero remote-coordinator identity. The shard
/// service uses it as an exclusive live-process lease: once a coordinator has
/// adopted a node, another coordinator cannot read or mutate that same node.
///
/// SplitMix64 is a bijection over the process-local sequence. The time/PID seed
/// makes equal ids across independent coordinator processes negligibly likely
/// without adding randomness to the lean distributed dependency set.
pub(crate) fn fresh_coordinator_id() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let seed = *SEED.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        (nanos as u64)
            ^ ((nanos >> 64) as u64).rotate_left(17)
            ^ u64::from(std::process::id()).rotate_left(32)
    });
    let mut value = seed.wrapping_add(NEXT.fetch_add(1, Ordering::Relaxed));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^= value >> 31;
    if value == 0 {
        1
    } else {
        value
    }
}
