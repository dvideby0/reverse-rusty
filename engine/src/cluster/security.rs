//! Mesh security for the gRPC transports (ADR-071, Distributed-v1 criterion 2):
//! TLS configuration shapes + the shared bearer-token (mesh secret) machinery used
//! by BOTH transports (shard + control plane) on both sides of the wire — one
//! implementation, so the two planes cannot drift.
//!
//! Two independent, opt-in knobs (unset ⇒ byte-identical to the plaintext paths):
//! - **TLS**: the server presents an operator-provided PEM identity; the client
//!   verifies it against an operator-provided CA (tonic's rustls integration, the
//!   `tls-ring` feature). Server authentication + wire privacy/integrity.
//! - **Mesh token**: ONE shared cluster secret attached to every RPC as standard
//!   `authorization: Bearer <token>` metadata by [`MeshAuthInject`] and verified
//!   constant-time by [`MeshAuthVerify`] BEFORE any handler runs — default-deny by
//!   construction (the interceptor wraps the whole service, so a future RPC is
//!   covered without being listed anywhere).
//!
//! Trust model (ADR-071): the token admits a node to the mesh; TLS authenticates
//! servers and protects the wire. mTLS / per-RPC authorization tiers are post-v1.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

use tonic::codegen::{http, Service};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::server::NamedService;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use super::shard::ShardError;

const COORDINATOR_ID_HEADER: &str = "x-reverse-rusty-coordinator-id";
const COORDINATOR_CLAIM_HEADER: &str = "x-reverse-rusty-coordinator-claim";
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

    fn with_ttl(ttl: Duration) -> Self {
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

    fn begin_unstamped(self: &Arc<Self>) -> Option<ActiveCoordinatorCall> {
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

    fn begin_owner(self: &Arc<Self>, candidate: u64) -> Option<ActiveCoordinatorCall> {
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
    fn authorize_owner(&self, candidate: u64) -> bool {
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

    async fn claim(&self, candidate: u64) -> Result<(), Status> {
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
    fn claim_waiters(&self) -> usize {
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

struct ActiveCoordinatorCall {
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
struct CoordinatorClaimHandshake;

/// Admission is decided atomically in [`CoordinatorLeaseService`] before the
/// Tonic interceptor runs. Keeping the decision in request extensions closes
/// the check/use race where a claim could be cancelled between those layers
/// and accidentally turn a rejected unstamped request back into an admitted
/// one.
#[derive(Clone, Copy)]
enum CoordinatorAdmission {
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
    fn new(inner: B, active: Option<ActiveCoordinatorCall>) -> Self {
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

/// A server's TLS identity: PEM-encoded certificate (chain) + private key, as read
/// from the operator's `--tls-cert` / `--tls-key` files.
#[derive(Clone)]
pub struct TlsServerIdentity {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

/// A client's TLS verification config: the PEM CA bundle that must have signed the
/// server certificate, plus an optional verification/SNI domain override (needed
/// when endpoints are raw IPs but the certificate names a DNS SAN).
#[derive(Clone)]
pub struct TlsClientConfig {
    pub ca_pem: Vec<u8>,
    pub domain: Option<String>,
}

/// Transport-resilience knobs for a mesh CLIENT connection (ADR-085), applied at two
/// layers so a hung/dead remote peer can never block a caller forever:
/// - [`configure_endpoint`] sets the channel-level `connect_timeout` + HTTP/2 keepalive
///   (a dead/half-open peer is detected and the connection broken, so in-flight RPCs
///   error out fail-loud), shared by the shard, control-plane, and Raft-peer clients.
/// - [`RemoteShard`](super::remote::RemoteShard) wraps each UNARY RPC in a per-call
///   deadline (`read_timeout`/`write_timeout`) and does bounded fail-loud retry of
///   IDEMPOTENT reads (`read_retries`) on transient errors. Streaming / long-pull
///   recovery RPCs opt out of the per-call deadline and lean on keepalive instead.
///
/// [`Default`] is the conservative always-on profile; the historical (pre-ADR-085)
/// behavior was no timeouts and no keepalive at all. A timeout only ever turns a hang
/// into a loud `ShardError` — it never drops a shard from a percolate union (that would
/// be a false negative); the cross-shard merge still fails closed.
#[derive(Clone, Debug)]
pub struct MeshTransport {
    /// Max time for the TCP + TLS dial before failing the connect.
    pub connect_timeout: Duration,
    /// HTTP/2 (and TCP) keepalive PING interval — detects a dead/idle peer mid-call.
    pub keepalive_interval: Duration,
    /// How long to wait for a keepalive PING ack before dropping the connection.
    pub keepalive_timeout: Duration,
    /// Per-call deadline for unary READ RPCs (percolate, counts, fingerprint).
    pub read_timeout: Duration,
    /// Per-call deadline for unary WRITE RPCs (ingest, insert, delete, flush, fence, lease).
    pub write_timeout: Duration,
    /// Bounded retry attempts for IDEMPOTENT reads on a transient error (total tries =
    /// `read_retries + 1`); `0` disables retry. Writes are never retried (non-idempotent).
    pub read_retries: u32,
}

impl MeshTransport {
    /// Default dial bound.
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
    /// Default keepalive PING interval.
    pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
    /// Default keepalive PING-ack timeout.
    pub const DEFAULT_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
    /// Default per-call deadline for unary reads (the latency-sensitive percolate path).
    pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);
    /// Default per-call deadline for unary writes (generous — a bulk ingest batch is large).
    pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
    /// Default bounded read-retry count (3 total tries).
    pub const DEFAULT_READ_RETRIES: u32 = 2;
}

impl Default for MeshTransport {
    fn default() -> Self {
        MeshTransport {
            connect_timeout: Self::DEFAULT_CONNECT_TIMEOUT,
            keepalive_interval: Self::DEFAULT_KEEPALIVE_INTERVAL,
            keepalive_timeout: Self::DEFAULT_KEEPALIVE_TIMEOUT,
            read_timeout: Self::DEFAULT_READ_TIMEOUT,
            write_timeout: Self::DEFAULT_WRITE_TIMEOUT,
            read_retries: Self::DEFAULT_READ_RETRIES,
        }
    }
}

/// Server-side mesh security: TLS identity + the expected mesh token, plus server-side
/// HTTP/2 keepalive (ADR-085) so a dead/half-open CLIENT connection is reclaimed instead
/// of leaked. `Default` (no TLS, no token) is the historical plaintext/unauthenticated
/// behavior; the keepalive defaults are conservative and off any hot path.
#[derive(Clone)]
pub struct ServerSecurity {
    pub tls: Option<TlsServerIdentity>,
    pub token: Option<Vec<u8>>,
    /// HTTP/2 keepalive PING interval the server applies to client connections.
    pub keepalive_interval: Duration,
    /// How long the server waits for a PING ack before dropping a client connection.
    pub keepalive_timeout: Duration,
}

impl Default for ServerSecurity {
    fn default() -> Self {
        ServerSecurity {
            tls: None,
            token: None,
            keepalive_interval: MeshTransport::DEFAULT_KEEPALIVE_INTERVAL,
            keepalive_timeout: MeshTransport::DEFAULT_KEEPALIVE_TIMEOUT,
        }
    }
}

/// Client-side mesh security: TLS verification + the mesh token to present + the
/// [`MeshTransport`] resilience knobs (ADR-085). `Default` is the historical plaintext
/// behavior for TLS/token, now with the always-on transport timeouts + keepalive.
#[derive(Clone, Default)]
pub struct ClientSecurity {
    pub tls: Option<TlsClientConfig>,
    pub token: Option<Vec<u8>>,
    pub transport: MeshTransport,
}

/// Resolve the mesh token from a CLI flag and the `RR_CLUSTER_TOKEN` environment
/// variable — flag wins; the ADR-062 validation rules verbatim (non-empty, visible
/// ASCII, no spaces), fail-loud on a malformed value rather than silently serving
/// open. `Ok(None)` ⇔ no token configured.
pub fn resolve_mesh_token(
    flag: Option<String>,
    env: Result<String, std::env::VarError>,
) -> Result<Option<Vec<u8>>, String> {
    let raw = match (flag, env) {
        // Flag wins over env; both funnel into the same validation below.
        (Some(t), _) | (None, Ok(t)) => t,
        (None, Err(std::env::VarError::NotPresent)) => return Ok(None),
        (None, Err(std::env::VarError::NotUnicode(_))) => {
            // Set-but-undecodable must refuse startup, never silently disable auth
            // (the ADR-062 fail-open fix, kept here).
            return Err("RR_CLUSTER_TOKEN is set but not valid UTF-8".into());
        }
    };
    if raw.is_empty() {
        return Err("mesh token must not be empty".into());
    }
    if !raw.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err("mesh token must be visible ASCII with no spaces".into());
    }
    Ok(Some(raw.into_bytes()))
}

/// Build the (possibly TLS-wrapped) endpoint for a mesh client connection. With
/// `tls`, the endpoint should use an `https://` URI; the CA is installed and the
/// optional domain override applied. A malformed endpoint or TLS config fails loud.
pub(crate) fn configure_endpoint(
    endpoint: &str,
    tls: Option<&TlsClientConfig>,
    transport: &MeshTransport,
) -> Result<Endpoint, ShardError> {
    let ep = Endpoint::from_shared(endpoint.to_string())
        .map_err(|e| ShardError::Remote(format!("invalid endpoint {endpoint:?}: {e}")))?;
    // Transport resilience (ADR-085): bound the dial and enable HTTP/2 keepalive so a
    // dead/half-open peer is detected — the connection is broken and in-flight RPCs error
    // out fail-loud — instead of a call hanging forever. This is the SHARED endpoint
    // builder for the shard client, the control-plane client, AND the Raft peer network,
    // so all three harden in one place; it also covers the long streaming RPCs that opt
    // out of the per-call deadline (they lean on keepalive to notice a dead peer).
    let ep = ep
        .connect_timeout(transport.connect_timeout)
        .tcp_keepalive(Some(transport.keepalive_interval))
        .http2_keep_alive_interval(transport.keepalive_interval)
        .keep_alive_timeout(transport.keepalive_timeout)
        .keep_alive_while_idle(true);
    match tls {
        // An https endpoint with NO client TLS config would die inside tonic as an
        // opaque "transport error" — name the misconfiguration instead (the node is
        // missing its --tls-ca / client security half).
        None if endpoint.starts_with("https://") => Err(ShardError::Remote(format!(
            "endpoint {endpoint:?} is https but no client TLS config is set (this node \
             needs its CA — e.g. --tls-ca / --grpc-tls-ca — to dial a TLS mesh)"
        ))),
        None => Ok(ep),
        Some(cfg) => {
            let mut tls_cfg =
                ClientTlsConfig::new().ca_certificate(Certificate::from_pem(&cfg.ca_pem));
            if let Some(domain) = &cfg.domain {
                tls_cfg = tls_cfg.domain_name(domain.clone());
            }
            ep.tls_config(tls_cfg)
                .map_err(|e| ShardError::Remote(format!("TLS config for {endpoint:?}: {e}")))
        }
    }
}

/// Client-side interceptor: inject the mesh token (when configured) into every
/// outgoing RPC as `authorization: Bearer <token>`. With no token it is a no-op, so
/// the secured and plaintext paths share one client type.
#[derive(Clone)]
pub(crate) struct MeshAuthInject {
    header: Option<MetadataValue<Ascii>>,
    coordinator_id: Option<MetadataValue<Ascii>>,
    coordinator_claim: bool,
}

impl MeshAuthInject {
    /// Build from the resolved token. The header value is pre-built once — token
    /// bytes are validated visible-ASCII at resolve time, so this cannot fail for a
    /// token that passed [`resolve_mesh_token`]; a value that somehow doesn't parse
    /// fails loud here rather than silently sending unauthenticated RPCs.
    pub(crate) fn new(token: Option<&[u8]>) -> Result<Self, ShardError> {
        Self::with_coordinator(token, None)
    }

    pub(crate) fn with_coordinator(
        token: Option<&[u8]>,
        coordinator_id: Option<u64>,
    ) -> Result<Self, ShardError> {
        Self::with_coordinator_mode(token, coordinator_id, false)
    }

    pub(crate) fn with_coordinator_claim(
        token: Option<&[u8]>,
        coordinator_id: u64,
    ) -> Result<Self, ShardError> {
        Self::with_coordinator_mode(token, Some(coordinator_id), true)
    }

    fn with_coordinator_mode(
        token: Option<&[u8]>,
        coordinator_id: Option<u64>,
        coordinator_claim: bool,
    ) -> Result<Self, ShardError> {
        let header = match token {
            None => None,
            Some(t) => {
                let v = format!("Bearer {}", String::from_utf8_lossy(t));
                Some(v.parse::<MetadataValue<Ascii>>().map_err(|e| {
                    ShardError::Remote(format!("mesh token is not a valid header value: {e}"))
                })?)
            }
        };
        let coordinator_id = coordinator_id
            .map(|id| {
                if id == 0 {
                    return Err(ShardError::Config(
                        "remote coordinator id must be non-zero".into(),
                    ));
                }
                id.to_string()
                    .parse::<MetadataValue<Ascii>>()
                    .map_err(|error| {
                        ShardError::Remote(format!(
                            "coordinator id is not a valid metadata value: {error}"
                        ))
                    })
            })
            .transpose()?;
        Ok(MeshAuthInject {
            header,
            coordinator_id,
            coordinator_claim,
        })
    }
}

impl Interceptor for MeshAuthInject {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(h) = &self.header {
            request.metadata_mut().insert("authorization", h.clone());
        }
        if let Some(id) = &self.coordinator_id {
            request
                .metadata_mut()
                .insert(COORDINATOR_ID_HEADER, id.clone());
        }
        if self.coordinator_claim {
            request
                .metadata_mut()
                .insert(COORDINATOR_CLAIM_HEADER, MetadataValue::from_static("1"));
        }
        Ok(request)
    }
}

/// Server-side interceptor: verify the mesh token on every incoming RPC BEFORE the
/// handler runs. With no expected token it is a pass-through (the historical open
/// behavior); with one, a missing/wrong token answers `UNAUTHENTICATED`. The
/// comparison is constant-time (no data-dependent branches).
#[derive(Clone)]
pub(crate) struct MeshAuthVerify {
    expected: Option<Arc<[u8]>>,
    coordinator_lease: Option<Arc<CoordinatorLease>>,
}

impl MeshAuthVerify {
    pub(crate) fn new(token: Option<Vec<u8>>) -> Self {
        MeshAuthVerify {
            expected: token.map(Arc::from),
            coordinator_lease: None,
        }
    }

    pub(crate) fn with_coordinator_lease(
        token: Option<Vec<u8>>,
        coordinator_lease: Arc<CoordinatorLease>,
    ) -> Self {
        MeshAuthVerify {
            expected: token.map(Arc::from),
            coordinator_lease: Some(coordinator_lease),
        }
    }

    fn verify_coordinator(&self, request: &Request<()>) -> Result<(), Status> {
        let Some(lease) = &self.coordinator_lease else {
            return Ok(());
        };
        let presented = request_coordinator_id(request)?;
        let claim_requested = coordinator_claim_requested(request)?;
        if claim_requested
            && request
                .extensions()
                .get::<CoordinatorClaimHandshake>()
                .is_none()
        {
            return Err(Status::failed_precondition(
                "remote coordinator claim metadata is valid only on \
                 DictFingerprint/AdoptDict/AddShard",
            ));
        }
        if claim_requested && presented.is_none() {
            return Err(Status::invalid_argument(
                "remote coordinator claim metadata requires a non-zero coordinator id",
            ));
        }
        if let Some(admission) = request.extensions().get::<CoordinatorAdmission>() {
            return match (*admission, presented, claim_requested) {
                (CoordinatorAdmission::Claim, Some(_), true)
                | (CoordinatorAdmission::Unstamped, None, false) => Ok(()),
                (CoordinatorAdmission::Owner(admitted), Some(candidate), false)
                    if admitted == candidate && lease.authorize_owner(candidate) =>
                {
                    Ok(())
                }
                (CoordinatorAdmission::Rejected, Some(_), false) if lease.owner() == 0 => {
                    Err(no_live_coordinator_lease_error())
                }
                (CoordinatorAdmission::Rejected, None, false) => Err(Status::failed_precondition(
                    "shard node rejected an unstamped request during a remote coordinator \
                         ownership transition; retry through the owning coordinator",
                )),
                _ => Err(coordinator_lease_error()),
            };
        }

        // Direct interceptor tests and embeddings that do not install the
        // outer service wrapper retain the historical state-based behavior.
        // Production traffic always carries the atomic admission extension.
        let current = lease.owner();
        if current == 0 {
            return match presented {
                None if !lease.is_claiming() => Ok(()),
                None => Err(Status::failed_precondition(
                    "shard node is completing a remote coordinator claim; retry through \
                     the owning coordinator",
                )),
                Some(_) if claim_requested => Ok(()),
                Some(_) => Err(no_live_coordinator_lease_error()),
            };
        }
        match presented {
            Some(_) if claim_requested => Ok(()),
            Some(candidate) if candidate == current && lease.authorize_owner(candidate) => Ok(()),
            _ => Err(coordinator_lease_error()),
        }
    }
}

pub(crate) fn coordinator_claim_requested<T>(request: &Request<T>) -> Result<bool, Status> {
    match request.metadata().get(COORDINATOR_CLAIM_HEADER) {
        None => Ok(false),
        Some(value) if value.as_bytes() == b"1" => Ok(true),
        Some(_) => Err(Status::invalid_argument(
            "remote coordinator claim metadata must be 1",
        )),
    }
}

impl Interceptor for MeshAuthVerify {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(expected) = &self.expected {
            let presented = request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(bearer_token);
            match presented {
                Some(t) if ct_eq(t.as_bytes(), expected) => {}
                Some(_) => return Err(Status::unauthenticated("invalid mesh token")),
                None => {
                    return Err(Status::unauthenticated(
                        "missing mesh token (authorization: Bearer <token>)",
                    ))
                }
            }
        }
        self.verify_coordinator(&request)?;
        Ok(request)
    }
}

pub(crate) fn request_coordinator_id<T>(request: &Request<T>) -> Result<Option<u64>, Status> {
    request
        .metadata()
        .get(COORDINATOR_ID_HEADER)
        .map(|raw| {
            let text = raw.to_str().map_err(|_| {
                Status::invalid_argument("remote coordinator id is not valid ASCII")
            })?;
            let id = text.parse::<u64>().map_err(|_| {
                Status::invalid_argument("remote coordinator id is not an unsigned integer")
            })?;
            if id == 0 {
                return Err(Status::invalid_argument(
                    "remote coordinator id must be non-zero",
                ));
            }
            Ok(id)
        })
        .transpose()
}

pub(crate) async fn claim_coordinator(
    lease: &CoordinatorLease,
    candidate: Option<u64>,
) -> Result<(), Status> {
    let Some(candidate) = candidate else {
        // Compatibility/direct service calls remain unowned until a remote
        // ClusterEngine performs a coordinator-stamped adopt.
        return Ok(());
    };
    lease.claim(candidate).await
}

fn coordinator_lease_error() -> Status {
    Status::failed_precondition(
        "shard node is exclusively leased to another remote coordinator; \
         shared remote coordinators are unsupported for exact delivery",
    )
}

fn no_live_coordinator_lease_error() -> Status {
    Status::failed_precondition(
        "shard node has no live coordinator lease; reconnect through a claim-capable \
         DictFingerprint/AdoptDict/AddShard handshake before issuing coordinator-owned RPCs",
    )
}

/// Extract the token from a `Bearer <token>` value — scheme case-insensitive,
/// extra spaces tolerated (RFC 6750, mirroring the HTTP gate).
fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, rest) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = rest.trim_start_matches(' ');
    (!token.is_empty()).then_some(token)
}

/// Constant-time byte equality (the ADR-062 comparator, duplicated here because the
/// HTTP gate lives in the server *binary*, not the library): the fold has no
/// data-dependent branch, so a wrong token costs the same as a nearly-right one.
/// Only the token's *length* is observable, which is not secret.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body::Body as _;

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
        assert!(
            resolve_mesh_token(Some(String::new()), Err(std::env::VarError::NotPresent)).is_err()
        );
        assert!(resolve_mesh_token(
            Some("has space".into()),
            Err(std::env::VarError::NotPresent)
        )
        .is_err());
        assert!(
            resolve_mesh_token(Some("ünïcode".into()), Err(std::env::VarError::NotPresent))
                .is_err()
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
        let mut inj =
            MeshAuthInject::with_coordinator(None, Some(42)).expect("coordinator injector");
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
}
