//! `RemoteShard` — a [`Shard`] backed by a gRPC `ShardService` client.
//!
//! Implements the SYNC [`Shard`] trait by blocking on its async tonic client via a
//! [`tokio::runtime::Handle`], confining all async to this type so the coordinator,
//! `LocalShard`, and the oracle stay synchronous. A failed RPC surfaces as
//! [`ShardError::Remote`] — never a swallowed empty result, which would shrink a
//! percolate's union into a false negative.
//!
//! All RPCs are driven through [`block_on_in_context`], which keeps the sync→async bridge
//! safe regardless of the CALLER's thread context (the seam is sync, but a coordinator may
//! probe a shard from a rayon worker, a plain thread, OR — for a future async coordinator
//! server — a tokio runtime worker). The naive `Handle::block_on` panics with a
//! nested-runtime error when called on a runtime worker, so the bridge dispatches on the
//! caller's context: off any runtime (rayon fan-out / the in-process build path) it is a
//! plain `block_on`; on a multi-thread runtime worker it wraps `block_on` in
//! `task::block_in_place` (the documented re-entry pattern); on a current-thread runtime it
//! offloads to a scoped non-runtime thread. The cost — a parked worker per in-flight RPC —
//! is the latency of distribution itself; an async fan-out is the documented later
//! optimization (ADR-029). See ADR-047 for the thread-context contract.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::{Handle, RuntimeFlavor};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::compile::Extracted;
use crate::exact::TagPredicate;
use crate::segment::{IngestReport, MatchStats, PlacedQuery};

use super::clog::{ClusterMutation, LogPos};
use super::proto;
use super::proto::shard_service_client::ShardServiceClient;
use super::security::{configure_endpoint, ClientSecurity, MeshAuthInject, MeshTransport};
use super::shard::{
    BatchTitleRequest, FetchedMatch, Shard, ShardBatchRankedMatch, ShardError, ShardRankedMatch,
    ShardRankedTitle,
};
use super::transport_metrics::{RpcMethod, RpcOutcome, TransportMetrics};

/// The mesh-aware client channel (ADR-071): every RPC flows through the
/// [`MeshAuthInject`] interceptor, which attaches the cluster token when one is
/// configured and is a no-op otherwise — so the secured and plaintext paths share
/// ONE client type and no RPC call site changes.
pub(crate) type MeshChannel = InterceptedService<Channel, MeshAuthInject>;

/// Async mesh connect (ADR-071): configure the endpoint (TLS when the security
/// config carries it), eagerly connect, wrap with the token interceptor. The
/// async core under [`connect_channel`], and the dial the server-side `RecoverFrom`
/// handler uses for its OUTBOUND peer connection — one path, so an internal dial
/// can never silently skip the mesh security.
pub(crate) async fn connect_mesh(
    endpoint: &str,
    security: &ClientSecurity,
) -> Result<ShardServiceClient<MeshChannel>, ShardError> {
    connect_mesh_with_coordinator(endpoint, security, None).await
}

/// Mesh connect carrying an optional exclusive remote-coordinator identity.
/// Ownership is claimed only by an explicitly claim-stamped
/// `DictFingerprint`/`AdoptDict`/`AddShard`; ordinary clients use this helper
/// after that handshake and can never claim a freshly restarted process
/// accidentally.
pub(crate) async fn connect_mesh_with_coordinator(
    endpoint: &str,
    security: &ClientSecurity,
    coordinator_id: Option<u64>,
) -> Result<ShardServiceClient<MeshChannel>, ShardError> {
    let ep = configure_endpoint(endpoint, security.tls.as_ref(), &security.transport)?;
    let channel = ep
        .connect()
        .await
        .map_err(|e| ShardError::Remote(format!("connect: {e}")))?;
    let inject = MeshAuthInject::with_coordinator(security.token.as_deref(), coordinator_id)?;
    Ok(ShardServiceClient::with_interceptor(channel, inject))
}

/// One shard living behind a gRPC `ShardService`.
pub struct RemoteShard {
    client: ShardServiceClient<MeshChannel>,
    /// One-shot claim-stamped client retained only for recovering this same
    /// coordinator identity after a durable shard-process restart.
    claim_client: Option<ShardServiceClient<MeshChannel>>,
    coordinator_id: Option<u64>,
    handle: Handle,
    /// The endpoint string this client was connected with (ADR-096): the coordinator's GC sweep
    /// reads it back through [`Shard::live_endpoints`] so live routing's physical targets are a
    /// KEEP-set no drop can violate, however routing got there (a committed reassign, a raw
    /// handoff flip, an uncommitted move).
    endpoint: String,
    /// The coordinator's frozen-dict fingerprint (verified equal to the server's at connect).
    /// Carried so dict-guarded RPCs (e.g. `FetchTranslog`) can present it.
    dict_fp: u64,
    /// The coordinator's frozen tag-dict fingerprint (ADR-077), verified at connect/adopt
    /// exactly like `dict_fp` and presented on every fingerprint-guarded recovery RPC.
    tag_dict_fp: u64,
    /// The global shard position this client addresses (ADR-093). ONE `ShardServer` may host many
    /// shards keyed by this id, so every per-shard request stamps `shard_id: self.shard_id` to route
    /// to the right slot. In the 1:1 deployment this is the endpoint's position. It flows via `self`
    /// (never through the `call` seam), so the ADR-085 instrumentation is unchanged.
    shard_id: u32,
    placement_generation: crate::ownership::PlacementGeneration,
    num_shards: u32,
    /// Transport-resilience knobs (ADR-085): per-call deadlines + bounded read-retry,
    /// cloned from the [`ClientSecurity`] this shard was connected with.
    transport: MeshTransport,
    /// Shared per-RPC metrics sink (ADR-085). A private throwaway by default; the gRPC
    /// builders swap in the coordinator's shared collector via [`Self::with_metrics`].
    metrics: Arc<TransportMetrics>,
}

/// Connect the mesh channel: configure the endpoint (TLS when the security config
/// carries it), eagerly connect on `handle` (a bad endpoint/handshake fails here,
/// not on the first RPC), and wrap it with the token-injecting interceptor.
fn connect_channel(
    endpoint: &str,
    handle: &Handle,
    security: &ClientSecurity,
    coordinator_id: Option<u64>,
    claim_coordinator: bool,
) -> Result<ShardServiceClient<MeshChannel>, ShardError> {
    let connected = async {
        let ep = configure_endpoint(endpoint, security.tls.as_ref(), &security.transport)?;
        let channel = ep
            .connect()
            .await
            .map_err(|error| ShardError::Remote(format!("connect: {error}")))?;
        let inject = match (coordinator_id, claim_coordinator) {
            (Some(id), true) => {
                MeshAuthInject::with_coordinator_claim(security.token.as_deref(), id)?
            }
            (id, false) => MeshAuthInject::with_coordinator(security.token.as_deref(), id)?,
            (None, true) => {
                return Err(ShardError::Config(
                    "a coordinator claim requires a non-zero coordinator id".into(),
                ))
            }
        };
        Ok(ShardServiceClient::with_interceptor(channel, inject))
    };
    block_on_in_context(handle, connected)
}

/// Read a node's actual dict fingerprint after a failed adoption handshake.
///
/// An exclusive handshake validates divergent input before it claims an
/// unowned node. Prefer the ordinary coordinator-stamped client (the node may
/// already be owned by this coordinator), then retry unstamped only when that
/// probe is rejected because the failed handshake left the node unowned. This
/// diagnostic probe intentionally stays non-claiming; the separate retained
/// claim client uses `DictFingerprint` only for restart recovery.
fn probe_actual_dict_fingerprint(
    endpoint: &str,
    handle: &Handle,
    security: &ClientSecurity,
    client: &ShardServiceClient<MeshChannel>,
    coordinator_id: Option<u64>,
) -> Option<u64> {
    let mut probe = client.clone();
    let first = block_on_in_context(handle, async move {
        probe.dict_fingerprint(proto::Empty {}).await
    });
    match first {
        Ok(reply) => Some(reply.into_inner().fingerprint),
        Err(status)
            if coordinator_id.is_some() && status.code() == tonic::Code::FailedPrecondition =>
        {
            // AdoptDict checks malformed/divergent input before publishing a
            // lease, so a stamped probe can truthfully be "too early". An
            // unstamped read is admitted only while the node is still unowned;
            // it cannot bypass another coordinator's live lease.
            let mut fallback = connect_channel(endpoint, handle, security, None, false).ok()?;
            block_on_in_context(handle, async move {
                fallback.dict_fingerprint(proto::Empty {}).await
            })
            .ok()
            .map(|reply| reply.into_inner().fingerprint)
        }
        Err(_) => None,
    }
}

mod add_shard;
mod adopt;
mod call;
mod connect;
mod shard_impl;

#[cfg(test)]
mod tests;

fn grpc_deadline_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::DeadlineExceeded
        || (status.code() == tonic::Code::Cancelled && status.message().contains("Timeout expired"))
}

fn no_live_coordinator_lease_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status
            .message()
            .contains("shard node has no live coordinator lease")
}

/// Drive `fut` on `handle` from a SYNCHRONOUS caller, dispatching on the caller's tokio
/// context so the bridge never panics with the nested-runtime error (ADR-047):
/// - **off any runtime** (a rayon fan-out worker, the in-process build path, a plain thread):
///   a plain [`Handle::block_on`] — the fast path, unchanged from before.
/// - **on a multi-thread runtime worker**: [`tokio::task::block_in_place`] around `block_on`,
///   the documented way to re-enter a multi-thread scheduler's async context without starving
///   it (`Runtime::new()` / tonic / axum are all multi-thread).
/// - **on a current-thread runtime**: `block_in_place` is unavailable there, so the drive is
///   offloaded to a scoped helper thread — not a runtime worker, so `block_on` is safe on it.
///
/// `Handle::try_current` only DETECTS the caller's context/flavor; the future is always driven
/// on the passed `handle` (the shard's runtime), which may or may not be the current one.
pub(crate) fn block_on_in_context<F>(handle: &Handle, fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    match Handle::try_current() {
        Err(_) => handle.block_on(fut),
        Ok(current) => match current.runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(fut)),
            // Current-thread (or any non-multi-thread) runtime: can't park the only worker, so
            // drive on a scoped non-runtime thread, forwarding any panic from the future intact.
            _ => std::thread::scope(|s| {
                s.spawn(|| handle.block_on(fut))
                    .join()
                    .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            }),
        },
    }
}

/// Construct AND drive a Tokio timeout inside `handle`'s runtime context.
///
/// `tokio::time::timeout` creates its timer eagerly, so constructing it before
/// [`block_on_in_context`] enters the runtime panics on the plain/Rayon worker
/// threads that normally call the synchronous [`Shard`] seam.
fn block_on_timeout_in_context<F>(
    handle: &Handle,
    duration: Duration,
    fut: F,
) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: Future + Send,
    F::Output: Send,
{
    block_on_in_context(
        handle,
        async move { tokio::time::timeout(duration, fut).await },
    )
}

/// Legacy transport error mapping (the pre-ADR-110 behavior): keep the typed
/// deadline, preserve the server's message for everything else. Reconstructing
/// typed errors by message inspection is reserved for the two ranked RPCs
/// ([`ranked_rpc_err`]) whose server half (`read_status`) writes the matching
/// strings — a NotFound from any other RPC (e.g. a relocated/GC'd slot's
/// "shard N is not hosted on this node") must surface verbatim, not be retyped
/// into a phantom rank-fetch source loss (review finding).
fn rpc_err(status: &tonic::Status) -> ShardError {
    if status.code() == tonic::Code::DeadlineExceeded {
        ShardError::DeadlineExceeded
    } else {
        ShardError::Remote(status.to_string())
    }
}

/// ADR-110 ranked-seam inverse of the server's `read_status`: reconstruct the
/// typed errors the coordinator's no-partial contract branches on (enrichment
/// limit → 413, ownership/config mismatch → 503, per-id source loss).
/// Metadata-first (the ADR-111 structured code an up-to-date peer attaches);
/// the frozen-message substring ladder below stays as the version-skew
/// fallback. Every fallback arm requires BOTH the status code and the server's
/// message form; anything else stays a message-preserving `Remote` rather than
/// a mistyped reconstruction.
fn ranked_rpc_err(status: &tonic::Status) -> ShardError {
    if let Some(error) = crate::cluster::ranked_wire::parse(status) {
        return error;
    }
    let message = status.message();
    match status.code() {
        tonic::Code::DeadlineExceeded => ShardError::DeadlineExceeded,
        tonic::Code::NotFound => match parse_source_unavailable(message) {
            Some(logical) => ShardError::SourceUnavailable(logical),
            None => ShardError::Remote(status.to_string()),
        },
        tonic::Code::ResourceExhausted if message.contains("ranked enrichment byte credit") => {
            ShardError::EnrichmentLimit { limit: 0 }
        }
        tonic::Code::FailedPrecondition
            if message.contains("placement configuration mismatch")
                || message.contains("ownership") =>
        {
            ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::PlacementDecisionMismatch,
            )
        }
        _ => ShardError::Remote(status.to_string()),
    }
}

/// Parse the id out of `read_status`'s "source unavailable for logical id N"
/// not-found form (the `ShardError::SourceUnavailable` Display), so the
/// coordinator's diagnostics keep the real id instead of a fabricated 0.
fn parse_source_unavailable(message: &str) -> Option<u64> {
    message
        .rsplit_once("source unavailable for logical id ")
        .and_then(|(_, tail)| {
            let digits: &str = tail
                .split(|c: char| !c.is_ascii_digit())
                .next()
                .unwrap_or("");
            (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
        })
}

fn remaining_micros(remaining: Duration) -> u64 {
    u64::try_from(remaining.as_micros())
        .unwrap_or(u64::MAX)
        .max(1)
}

/// How [`RemoteShard::call`] treats an RPC (ADR-085): a unary read (deadline + bounded
/// retry), a unary write (deadline, no retry — non-idempotent), or an unbounded
/// long-running / streaming RPC (no deadline; a dead peer is caught by channel keepalive).
#[derive(Clone, Copy, PartialEq, Eq)]
enum CallKind {
    Read,
    Write,
    Unbounded,
}

/// The retry/timeout core of [`RemoteShard::call`] (ADR-085): drive `mk`'s future, applying
/// `deadline` (when `Some`) and retrying up to `max_retries` times on a transient error or a
/// timeout, with exponential backoff. Returns the final result, the retry attempts spent, and
/// whether the final failure was a timeout (for metric classification + the error message).
async fn run_with_retry<R, Fut, MkFut>(
    mk: MkFut,
    deadline: Option<Duration>,
    max_retries: u32,
) -> (Result<R, tonic::Status>, u32, bool)
where
    MkFut: Fn() -> Fut,
    Fut: Future<Output = Result<R, tonic::Status>>,
{
    let mut attempts = 0u32;
    loop {
        let attempt = match deadline {
            Some(d) => tokio::time::timeout(d, mk()).await,
            None => Ok(mk().await),
        };
        match attempt {
            Ok(Ok(v)) => return (Ok(v), attempts, false),
            Ok(Err(status)) => {
                if attempts < max_retries && is_transient(&status) {
                    attempts += 1;
                    tokio::time::sleep(backoff_delay(attempts)).await;
                    continue;
                }
                return (Err(status), attempts, false);
            }
            // Our own per-call deadline fired. A timeout is transient too, so retry it
            // (reads only — writes/unbounded pass `max_retries = 0`).
            Err(_elapsed) => {
                if attempts < max_retries {
                    attempts += 1;
                    tokio::time::sleep(backoff_delay(attempts)).await;
                    continue;
                }
                return (
                    Err(tonic::Status::deadline_exceeded("rpc timeout")),
                    attempts,
                    true,
                );
            }
        }
    }
}

/// Absolute-deadline retry core for ADR-110. Backoff, attempts, transport, and
/// shard compute all consume the same budget; a retry never resets the clock.
async fn run_with_retry_until<R, Fut, MkFut>(
    mk: MkFut,
    deadline: Instant,
    max_retries: u32,
) -> (Result<R, tonic::Status>, u32, bool)
where
    MkFut: Fn(Duration) -> Fut,
    Fut: Future<Output = Result<R, tonic::Status>>,
{
    let mut attempts = 0u32;
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return (
                Err(tonic::Status::deadline_exceeded(
                    "request deadline exhausted",
                )),
                attempts,
                true,
            );
        };
        if remaining.is_zero() {
            return (
                Err(tonic::Status::deadline_exceeded(
                    "request deadline exhausted",
                )),
                attempts,
                true,
            );
        }
        match tokio::time::timeout(remaining, mk(remaining)).await {
            Ok(Ok(value)) => return (Ok(value), attempts, false),
            Ok(Err(status)) if attempts < max_retries && is_transient(&status) => {
                attempts += 1;
                let delay = backoff_delay(attempts);
                let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                    return (
                        Err(tonic::Status::deadline_exceeded(
                            "request deadline exhausted",
                        )),
                        attempts,
                        true,
                    );
                };
                if left <= delay {
                    return (
                        Err(tonic::Status::deadline_exceeded(
                            "request deadline exhausted",
                        )),
                        attempts,
                        true,
                    );
                }
                tokio::time::sleep(delay).await;
            }
            Ok(Err(status)) => return (Err(status), attempts, false),
            Err(_) => {
                return (
                    Err(tonic::Status::deadline_exceeded(
                        "request deadline exhausted",
                    )),
                    attempts,
                    true,
                );
            }
        }
    }
}

/// Whether a gRPC status is worth retrying — only `Unavailable` (a transient connect /
/// server-restarting / load-shed signal). Conservative on purpose: codes like
/// `ResourceExhausted` or `Internal` are not retried, to avoid amplifying overload.
fn is_transient(status: &tonic::Status) -> bool {
    match status.code() {
        // Connection refused/reset, server load-shedding, or a GOAWAY mid-RPC.
        tonic::Code::Unavailable => true,
        // The generated tonic client maps a not-yet-ready channel (reconnect in progress /
        // connect refused — the most common downed-shard failure) to UNKNOWN with a
        // "Service was not ready: …" message. Treat THAT transport signal as transient, but
        // not arbitrary application-level UNKNOWNs.
        tonic::Code::Unknown => status.message().contains("not ready"),
        _ => false,
    }
}

/// Exponential backoff for retry attempt `n` (1-based): 50ms, 100ms, 200ms, … capped at 1s.
fn backoff_delay(n: u32) -> Duration {
    let shift = n.clamp(1, 6) - 1;
    Duration::from_millis((50u64 << shift).min(1000))
}

fn coordinator_attestation_error(endpoint: &str, expected: u64, actual: u64) -> ShardError {
    ShardError::Remote(format!(
        "shard at {endpoint} did not attest the exclusive remote-coordinator lease \
         (expected {expected}, received {actual}; zero identifies a pre-lease server). \
         Exact remote delivery requires every shard node to enforce one coordinator."
    ))
}

/// The connect-time refusal when a shard server does not attest the ADR-080 replicate-to-all
/// broad layout (`broad_replicate_all` false — a pre-ADR-080 server, where broad lived only on
/// shard 0). This coordinator routes broad on a per-title broad-eval shard assuming EVERY shard
/// holds the replicated lane, so serving such a server would silently miss broad matches off
/// shard 0 (a false negative — the cardinal sin). Fail loud at connect instead, mirroring the
/// dict / tag-dict fingerprint handshake. The fix is to re-ingest the corpus through an ADR-080
/// coordinator (which replicates broad to every shard) or run an ADR-080 shard server binary.
fn legacy_broad_layout_err(endpoint: &str) -> ShardError {
    ShardError::Remote(format!(
        "shard at {endpoint} does not attest ADR-080's replicate-to-all broad layout \
         (broad_replicate_all=false — a pre-ADR-080 server keeps broad only on shard 0); this \
         coordinator routes broad on every shard and would silently miss those matches. Re-ingest \
         under the replicate-to-all layout, or run an ADR-080 shard server."
    ))
}

/// Fail-loud guard (ADR-074): pre-resolved `tag_ids` — the tagged vocabulary rebuild's
/// carry-through — cannot cross the dict-agnostic wire. The proto ships raw `(key,value)`
/// tags only, and a synthetic `TagId` has no recoverable string to send; silently dropping
/// the ids would lose the query's tags (a filtered-read recall loss). `set_vocab` refuses a
/// non-local cluster before ever building such a bucket, so this is defense in depth at the
/// transport seam, not a reachable path.
fn refuse_wire_tag_ids(items: &[PlacedQuery]) -> Result<(), ShardError> {
    if items.iter().any(|q| !q.tag_ids.is_empty()) {
        return Err(ShardError::Config(
            "pre-resolved tag ids cannot cross the process boundary: the gRPC wire ships raw \
             (key,value) tags only (a synthetic TagId has no recoverable string) — the tagged \
             vocabulary rebuild is in-process only (ADR-074)"
                .into(),
        ));
    }
    Ok(())
}
