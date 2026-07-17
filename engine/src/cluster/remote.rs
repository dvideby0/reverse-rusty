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
use super::shard::{FetchedMatch, Shard, ShardError, ShardRankedMatch};
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
    let ep = configure_endpoint(endpoint, security.tls.as_ref(), &security.transport)?;
    let channel = ep
        .connect()
        .await
        .map_err(|e| ShardError::Remote(format!("connect: {e}")))?;
    let inject = MeshAuthInject::new(security.token.as_deref())?;
    Ok(ShardServiceClient::with_interceptor(channel, inject))
}

/// One shard living behind a gRPC `ShardService`.
pub struct RemoteShard {
    client: ShardServiceClient<MeshChannel>,
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
) -> Result<ShardServiceClient<MeshChannel>, ShardError> {
    block_on_in_context(handle, connect_mesh(endpoint, security))
}

impl RemoteShard {
    /// Connect to a `ShardService` at `endpoint` (e.g. `"http://127.0.0.1:50051"`),
    /// driving the async connect on `handle`, then verify the server's frozen-dict
    /// fingerprint equals `expected_fp` (the coordinator's
    /// [`crate::dict::Dict::fingerprint`]) AND its frozen tag-dict fingerprint equals
    /// `expected_tag_fp` (ADR-077 — both spaces are one identity; a divergent tag space
    /// would silently mis-filter). A dict mismatch returns [`ShardError::DictMismatch`];
    /// a tag mismatch fails loud too — including against a pre-ADR-077 server, whose
    /// probe reply leaves the tag fingerprint 0 (never a silently unverified link).
    pub fn connect(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        expected_tag_fp: u64,
        shard_id: u32,
    ) -> Result<Self, ShardError> {
        Self::connect_with_security(
            endpoint,
            handle,
            expected_fp,
            expected_tag_fp,
            shard_id,
            &ClientSecurity::default(),
        )
    }

    /// [`connect`](Self::connect) over a secured mesh link (ADR-071): TLS per the
    /// client config, the mesh token attached to every RPC. A default (empty)
    /// security config is byte-identical to the plaintext path.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_with_security(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        expected_tag_fp: u64,
        shard_id: u32,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        let client = connect_channel(endpoint, &handle, security)?;
        // Handshake before trusting the shard: clone the client for the probe RPC (a cheap
        // Channel bump, mirroring the per-call pattern below).
        let mut probe = client.clone();
        let reply = block_on_in_context(&handle, async move {
            probe.dict_fingerprint(proto::Empty {}).await
        })
        .map_err(|status| rpc_err(&status))?
        .into_inner();
        if reply.fingerprint != expected_fp {
            return Err(ShardError::DictMismatch {
                expected: expected_fp,
                actual: reply.fingerprint,
            });
        }
        if reply.tag_dict_fingerprint != expected_tag_fp {
            return Err(ShardError::Remote(format!(
                "tag-dict fingerprint mismatch at connect: coordinator {expected_tag_fp:#018x} != \
                 server {:#018x} (a 0 means a pre-ADR-077 server that cannot attest its tag space)",
                reply.tag_dict_fingerprint
            )));
        }
        if !reply.broad_replicate_all {
            return Err(legacy_broad_layout_err(endpoint));
        }
        if reply.placement_generation == 0 || reply.num_shards == 0 {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::MissingGeneration,
            ));
        }
        Ok(RemoteShard {
            client,
            handle,
            endpoint: endpoint.to_string(),
            dict_fp: expected_fp,
            tag_dict_fp: expected_tag_fp,
            shard_id,
            placement_generation: crate::ownership::PlacementGeneration(reply.placement_generation),
            num_shards: reply.num_shards,
            transport: security.transport.clone(),
            metrics: Arc::new(TransportMetrics::new()),
        })
    }

    /// Connect, then **ship** the coordinator's frozen dict to the server (`AdoptDict`,
    /// ADR-034) before trusting it — so a data node need not have rebuilt a byte-identical
    /// dict from the corpus out-of-band. `dict_bytes` is `crate::storage::serialize_dict` of
    /// the coordinator's dict; `expected_fp` is its [`crate::dict::Dict::fingerprint`].
    ///
    /// The server adopts onto an empty shard and no-ops if it already holds this dict; the
    /// returned fingerprint then *is* the handshake (it must equal `expected_fp`). If the
    /// server holds data under a **different** dict it refuses (`FailedPrecondition`), which
    /// we surface as [`ShardError::DictMismatch`] (reading back its actual fingerprint) — a
    /// divergent populated server fails loud instead of dropping matches silently.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_and_adopt(
        endpoint: &str,
        handle: Handle,
        dict_bytes: Vec<u8>,
        expected_fp: u64,
        tag_dict_bytes: Vec<u8>,
        expected_tag_fp: u64,
        shard_id: u32,
    ) -> Result<Self, ShardError> {
        Self::connect_and_adopt_with_security(
            endpoint,
            handle,
            dict_bytes,
            expected_fp,
            tag_dict_bytes,
            expected_tag_fp,
            shard_id,
            crate::ownership::PlacementGeneration::INITIAL,
            shard_id.saturating_add(1),
            &ClientSecurity::default(),
        )
    }

    /// [`connect_and_adopt`](Self::connect_and_adopt) over a secured mesh link
    /// (ADR-071). A default (empty) security config is byte-identical.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_and_adopt_with_security(
        endpoint: &str,
        handle: Handle,
        dict_bytes: Vec<u8>,
        expected_fp: u64,
        tag_dict_bytes: Vec<u8>,
        expected_tag_fp: u64,
        shard_id: u32,
        placement_generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        let client = connect_channel(endpoint, &handle, security)?;
        let mut shipper = client.clone();
        // Ship the dict AND the frozen tag space (ADR-049/055) in one atomic adopt — never a window
        // where the server has the dict but not the tag space. `shard_id` names the slot to create
        // on the node (ADR-093); the node-scope dict is deserialized once and shared across slots.
        let req = proto::AdoptDictRequest {
            dict: dict_bytes,
            fingerprint: expected_fp,
            tag_dict: tag_dict_bytes,
            tag_dict_fingerprint: expected_tag_fp,
            shard_id,
            placement_generation: placement_generation.0,
            num_shards,
        };
        let (adopted, adopted_tag, adopted_replicate_all, adopted_generation, adopted_num_shards) =
            match block_on_in_context(&handle, async move { shipper.adopt_dict(req).await }) {
                Ok(reply) => {
                    let r = reply.into_inner();
                    (
                        r.fingerprint,
                        r.tag_dict_fingerprint,
                        r.broad_replicate_all,
                        r.placement_generation,
                        r.num_shards,
                    )
                }
                // The server holds data under a different dict and refused ours. Read its actual
                // fingerprint so the mismatch is truthful, then fail loud (never a silent drop).
                Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                    let mut probe = client.clone();
                    let actual = block_on_in_context(&handle, async move {
                        probe.dict_fingerprint(proto::Empty {}).await
                    })
                    .map_or(0, |r| r.into_inner().fingerprint);
                    return Err(ShardError::DictMismatch {
                        expected: expected_fp,
                        actual,
                    });
                }
                Err(status) => return Err(ShardError::Remote(format!("adopt_dict: {status}"))),
            };
        // On success the server echoes the fingerprints it now serves — this equality IS the
        // dict-identity handshake, so no separate round-trip is needed. The tag-dict fingerprint is
        // checked the same way: a divergent tag space would mis-filter reads (ADR-055).
        if adopted != expected_fp {
            return Err(ShardError::DictMismatch {
                expected: expected_fp,
                actual: adopted,
            });
        }
        if adopted_tag != expected_tag_fp {
            return Err(ShardError::Remote(format!(
                "tag-dict fingerprint mismatch after adopt: coordinator {expected_tag_fp:#018x} != \
                 server {adopted_tag:#018x} (the shipped tag space did not round-trip)"
            )));
        }
        // A populated pre-ADR-080 server whose dict matches ours would adopt as an idempotent
        // no-op and pass the fingerprint checks above, yet hold broad only on shard 0 — refuse it
        // (see `connect`), because our broad routing assumes every shard holds the replicated lane.
        if !adopted_replicate_all {
            return Err(legacy_broad_layout_err(endpoint));
        }
        if adopted_generation != placement_generation.0 || adopted_num_shards != num_shards {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::GenerationMismatch {
                    expected: placement_generation,
                    actual: crate::ownership::PlacementGeneration(adopted_generation),
                },
            ));
        }
        Ok(RemoteShard {
            client,
            handle,
            endpoint: endpoint.to_string(),
            dict_fp: expected_fp,
            tag_dict_fp: expected_tag_fp,
            shard_id,
            placement_generation,
            num_shards,
            transport: security.transport.clone(),
            metrics: Arc::new(TransportMetrics::new()),
        })
    }

    /// Connect + create a CO-LOCATED slot on a node that has ALREADY adopted this dict (ADR-093
    /// Stage 2): unlike [`connect_and_adopt`](Self::connect_and_adopt) this ships NO dict bytes — it
    /// names `shard_id` and ATTESTS the node's `dict`/`tag_dict` fingerprints, so the node reuses its
    /// node-scope frozen space by `Arc`. Used by `connect_remote` for the 2nd+ position that lands on
    /// one endpoint (the 1st adopts). A fingerprint mismatch (or a node that adopted no dict) is a
    /// loud [`ShardError`], never a silent slot.
    pub fn connect_and_add_shard(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        expected_tag_fp: u64,
        shard_id: u32,
    ) -> Result<Self, ShardError> {
        Self::connect_and_add_shard_with_security(
            endpoint,
            handle,
            expected_fp,
            expected_tag_fp,
            shard_id,
            crate::ownership::PlacementGeneration::INITIAL,
            shard_id.saturating_add(1),
            &ClientSecurity::default(),
        )
    }

    /// [`connect_and_add_shard`](Self::connect_and_add_shard) over a secured mesh link (ADR-071). A
    /// default (empty) security config is byte-identical.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_and_add_shard_with_security(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        expected_tag_fp: u64,
        shard_id: u32,
        placement_generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        let client = connect_channel(endpoint, &handle, security)?;
        let mut shipper = client.clone();
        // No dict bytes — just NAME the slot and attest the node's fingerprints (ADR-093 Stage 2).
        let req = proto::AddShardRequest {
            shard_id,
            dict_fingerprint: expected_fp,
            tag_dict_fingerprint: expected_tag_fp,
            placement_generation: placement_generation.0,
            num_shards,
        };
        let (added, added_tag, added_replicate_all, added_generation, added_num_shards) =
            match block_on_in_context(&handle, async move { shipper.add_shard(req).await }) {
                Ok(reply) => {
                    let r = reply.into_inner();
                    (
                        r.dict_fingerprint,
                        r.tag_dict_fingerprint,
                        r.broad_replicate_all,
                        r.placement_generation,
                        r.num_shards,
                    )
                }
                // The node's adopted dict differs from ours (or it adopted none). Read its actual
                // fingerprint so the mismatch is truthful, then fail loud (never a silent drop).
                Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                    let mut probe = client.clone();
                    let actual = block_on_in_context(&handle, async move {
                        probe.dict_fingerprint(proto::Empty {}).await
                    })
                    .map_or(0, |r| r.into_inner().fingerprint);
                    return Err(ShardError::DictMismatch {
                        expected: expected_fp,
                        actual,
                    });
                }
                Err(status) => return Err(ShardError::Remote(format!("add_shard: {status}"))),
            };
        // The node echoes the fingerprints it serves — this equality IS the dict-identity handshake.
        if added != expected_fp {
            return Err(ShardError::DictMismatch {
                expected: expected_fp,
                actual: added,
            });
        }
        if added_tag != expected_tag_fp {
            return Err(ShardError::Remote(format!(
                "tag-dict fingerprint mismatch after add_shard: coordinator {expected_tag_fp:#018x} \
                 != server {added_tag:#018x}"
            )));
        }
        // A populated pre-ADR-080 server would hold broad only on shard 0; our broad routing assumes
        // every shard holds the replicated lane, so refuse it (see `connect_and_adopt`).
        if !added_replicate_all {
            return Err(legacy_broad_layout_err(endpoint));
        }
        if added_generation != placement_generation.0 || added_num_shards != num_shards {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::GenerationMismatch {
                    expected: placement_generation,
                    actual: crate::ownership::PlacementGeneration(added_generation),
                },
            ));
        }
        Ok(RemoteShard {
            client,
            handle,
            endpoint: endpoint.to_string(),
            dict_fp: expected_fp,
            tag_dict_fp: expected_tag_fp,
            shard_id,
            placement_generation,
            num_shards,
            transport: security.transport.clone(),
            metrics: Arc::new(TransportMetrics::new()),
        })
    }

    /// Drive an async RPC to completion from the synchronous [`Shard`] seam, safe regardless
    /// of the caller's thread context (see the module docs + ADR-047). Every RPC method below
    /// goes through this rather than `self.handle.block_on` directly, so a percolate or write
    /// issued from a tokio runtime worker re-enters via `block_in_place` instead of panicking.
    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        block_on_in_context(&self.handle, fut)
    }

    /// Share the coordinator's transport-metrics collector (ADR-085) so this client's
    /// per-RPC outcomes + latencies aggregate cluster-wide. Defaults to a private throwaway,
    /// so a `RemoteShard` built without it still works (its stats are just unobserved); the
    /// gRPC builders call this with the engine's shared `Arc`.
    pub(crate) fn with_metrics(mut self, metrics: Arc<TransportMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// The single instrumented RPC seam (ADR-085): drive `mk`'s future with a per-call
    /// deadline (unary reads/writes) and bounded fail-loud retry of IDEMPOTENT reads on a
    /// transient error, recording the outcome + latency. `mk` is a FACTORY — a tonic call
    /// future is single-use, so each attempt rebuilds it from a cloned client + request. A
    /// timeout or exhausted retry surfaces as a loud [`ShardError`], never a dropped result,
    /// so the coordinator's fan-out still fails closed (a swallowed shard = false negative).
    fn call<R, Fut, MkFut>(
        &self,
        method: RpcMethod,
        kind: CallKind,
        mk: MkFut,
    ) -> Result<R, ShardError>
    where
        MkFut: Fn() -> Fut + Send,
        Fut: Future<Output = Result<R, tonic::Status>> + Send,
        R: Send,
    {
        let deadline = match kind {
            CallKind::Read => Some(self.transport.read_timeout),
            CallKind::Write => Some(self.transport.write_timeout),
            // Long-running / streaming: no per-call deadline — a dead peer is caught by the
            // channel keepalive (configure_endpoint), which breaks the connection.
            CallKind::Unbounded => None,
        };
        // Only idempotent READS retry; a retried write (ingest/insert/delete) could
        // double-apply, so writes fail loud and converge via the coordinator's durable log.
        let max_retries = match kind {
            CallKind::Read => self.transport.read_retries,
            CallKind::Write | CallKind::Unbounded => 0,
        };
        let started = Instant::now();
        let (result, attempts, timed_out) =
            self.block_on(run_with_retry(mk, deadline, max_retries));
        let latency = started.elapsed();
        let outcome = if result.is_ok() {
            RpcOutcome::Ok
        } else if timed_out {
            RpcOutcome::Timeout
        } else {
            RpcOutcome::Error
        };
        self.metrics.record(method, outcome, latency, attempts);
        result.map_err(|status| {
            if timed_out {
                ShardError::Remote(format!(
                    "rpc timeout: {} exceeded {:?}",
                    method.label(),
                    deadline.unwrap_or_default()
                ))
            } else {
                rpc_err(&status)
            }
        })
    }

    /// ADR-110 read seam: unlike the compatibility per-call timeout above,
    /// every retry shares one absolute caller deadline. The factory receives
    /// the current remaining budget so it can set both `grpc-timeout` and the
    /// cooperative `remaining_micros` request field.
    fn call_until<R, Fut, MkFut>(
        &self,
        method: RpcMethod,
        deadline: Instant,
        mk: MkFut,
    ) -> Result<R, ShardError>
    where
        MkFut: Fn(Duration) -> Fut + Send,
        Fut: Future<Output = Result<R, tonic::Status>> + Send,
        R: Send,
    {
        let started = Instant::now();
        let (result, attempts, timed_out) = self.block_on(run_with_retry_until(
            mk,
            deadline,
            self.transport.read_retries,
        ));
        // tonic can surface a client-side `Request::set_timeout` expiry as
        // CANCELLED/"Timeout expired" rather than DEADLINE_EXCEEDED. It is still
        // the same request deadline and must retain the typed cancellation path.
        let deadline_status = result.as_ref().err().is_some_and(grpc_deadline_status);
        let outcome = if result.is_ok() {
            RpcOutcome::Ok
        } else if timed_out || deadline_status {
            RpcOutcome::Timeout
        } else {
            RpcOutcome::Error
        };
        self.metrics
            .record(method, outcome, started.elapsed(), attempts);
        result.map_err(|status| {
            if timed_out || grpc_deadline_status(&status) {
                ShardError::DeadlineExceeded
            } else {
                rpc_err(&status)
            }
        })
    }

    fn bounded_deadline(&self, deadline: Option<Instant>) -> Result<Instant, ShardError> {
        match deadline {
            Some(at) => Ok(at),
            None => Instant::now()
                .checked_add(self.transport.read_timeout)
                .ok_or_else(|| ShardError::Config("read timeout is out of range".into())),
        }
    }

    /// Drive this remote node's `RecoverFrom` RPC (ADR-036): it pulls `source_endpoint`'s sealed
    /// segments (via that peer's `FetchSegments`), writes them under its own data_dir, attaches
    /// them, and starts serving — the cross-node peer-recovery primitive. `dict_fp` must equal
    /// the coordinator's frozen-dict fingerprint (the server re-checks it). Returns
    /// `(segments_attached, num_queries, up_to_seqno)` — the last being the snapshot's translog
    /// position `P` (ADR-039), from which the coordinator replays the source's tail (> P) to
    /// finish a no-quiesce recovery. The node must be durable + have adopted the dict.
    pub fn recover_from(
        &self,
        source_endpoint: &str,
        dict_fp: u64,
    ) -> Result<(u64, u64, u64), ShardError> {
        let req = proto::RecoverFromRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            source_endpoint: source_endpoint.to_string(),
            dict_fingerprint: dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        // Long-running server-side pull — no per-call deadline (keepalive-guarded), no retry.
        let client = self.client.clone();
        let reply = self.call(RpcMethod::RecoverFrom, CallKind::Unbounded, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                client
                    .recover_from(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        self.validate_ownership(
            self.shard_id,
            crate::ownership::PlacementGeneration(reply.placement_generation),
            reply.num_shards,
        )?;
        Ok((
            reply.segments_attached,
            reply.num_queries,
            reply.up_to_seqno,
        ))
    }

    /// Fence this remote node as the owner of its shard at `generation` (ADR-044, step 6b): the
    /// server stops accepting data-mutating writes (they return `failed_precondition`) while it
    /// keeps serving reads + the recovery RPCs — the brief write-quiesce a live handoff holds across
    /// the routing flip (serve-then-drop). Monotonic server-side (a stale lower-generation fence is
    /// a no-op). Returns the server's fence generation after the call. Inherent (not a [`Shard`]
    /// method): only the handoff orchestrator fences a specific old owner, addressed by endpoint.
    pub fn fence(&self, generation: u64) -> Result<u64, ShardError> {
        let req = proto::FenceRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            generation,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Fence, CallKind::Write, move || {
            let mut client = client.clone();
            async move { client.fence(req).await.map(tonic::Response::into_inner) }
        })?;
        Ok(reply.fenced_at_generation)
    }

    /// Lift this remote node's fence at `generation` (ADR-048): the CAS-guarded inverse of
    /// [`Self::fence`]. The server clears the fence only if it currently holds exactly
    /// `generation` (a stale unfence, or a newer handoff's higher-generation re-fence, is a
    /// no-op), then resumes accepting writes. Returns the server's fence generation after the
    /// call (0 ⇒ un-fenced). Called by the handoff orchestrator when a handoff aborts after
    /// fencing, so the source self-heals instead of staying permanently write-quiesced.
    pub fn unfence(&self, generation: u64) -> Result<u64, ShardError> {
        let req = proto::UnfenceRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            generation,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Unfence, CallKind::Write, move || {
            let mut client = client.clone();
            async move { client.unfence(req).await.map(tonic::Response::into_inner) }
        })?;
        Ok(reply.fenced_at_generation)
    }

    /// The NODE's slot inventory (ADR-096): every shard the server hosts with its GC-relevant
    /// state (fence generation, live count, unexpired leases), plus the node's dict/tag-dict
    /// fingerprints — the coordinator's GC sweep verifies node identity from the reply before
    /// classifying. Node-level (not per-slot): the request carries no `shard_id`.
    pub fn list_shards(&self) -> Result<proto::ListShardsReply, ShardError> {
        let client = self.client.clone();
        self.call(RpcMethod::ListShards, CallKind::Read, move || {
            let mut client = client.clone();
            async move {
                client
                    .list_shards(proto::Empty {})
                    .await
                    .map(tonic::Response::into_inner)
            }
        })
    }

    /// Drop THIS client's slot on the node (ADR-096): remove it from the slot map and reclaim its
    /// `shard_<id>/` dir. The server refuses unless the slot is fenced at exactly
    /// `expected_fence_generation` (> 0 — the coordinator arms an unfenced orphan via
    /// [`Self::fence`] first) and holds no unexpired retention lease; a divergent dict/tag space
    /// is refused like every guarded RPC. An absent slot replies `dropped = false` (idempotent).
    pub fn drop_shard(
        &self,
        expected_fence_generation: u64,
    ) -> Result<proto::DropShardReply, ShardError> {
        let req = proto::DropShardRequest {
            shard_id: self.shard_id,
            expected_fence_generation,
            dict_fingerprint: self.dict_fp,
            tag_dict_fingerprint: self.tag_dict_fp,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        self.call(RpcMethod::DropShard, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .drop_shard(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })
    }

    /// This slot's order-independent 128-bit live-set fingerprint + live count (ADR-097): the
    /// group move compares the frozen source's against a retained member's — equal (while both
    /// sides are quiescent) proves the member already holds exactly the source's live set, so
    /// its `O(corpus)` re-copy is skipped. Fingerprint-guarded; an old peer answers
    /// `Unimplemented` and the caller falls back to the proven re-copy.
    pub fn content_fingerprint(&self) -> Result<(u64, u64, u64), ShardError> {
        let req = proto::ContentFingerprintRequest {
            shard_id: self.shard_id,
            dict_fingerprint: self.dict_fp,
            tag_dict_fingerprint: self.tag_dict_fp,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::ContentFingerprint, CallKind::Read, move || {
            let mut client = client.clone();
            async move {
                client
                    .content_fingerprint(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        self.validate_ownership(
            self.shard_id,
            crate::ownership::PlacementGeneration(reply.placement_generation),
            reply.num_shards,
        )?;
        Ok((reply.fp_lo, reply.fp_hi, reply.live_count))
    }
}

fn grpc_deadline_status(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::DeadlineExceeded
        || (status.code() == tonic::Code::Cancelled && status.message().contains("Timeout expired"))
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

fn rpc_err(status: &tonic::Status) -> ShardError {
    if status.code() == tonic::Code::DeadlineExceeded {
        ShardError::DeadlineExceeded
    } else if status.code() == tonic::Code::NotFound {
        ShardError::SourceUnavailable(0)
    } else if status
        .message()
        .contains("placement configuration mismatch")
        || status.message().contains("ownership")
    {
        ShardError::OwnershipMismatch(crate::ownership::OwnershipError::PlacementDecisionMismatch)
    } else {
        ShardError::Remote(status.to_string())
    }
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

impl Shard for RemoteShard {
    fn percolate_filtered(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            // Ship the ALREADY-RESOLVED `TagId` groups (ADR-055); empty ⇒ unfiltered.
            filter: proto::tag_predicate_to_proto(pred),
            rank: None,
            shard_id: self.shard_id,
            ownership: None,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Percolate, CallKind::Read, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move { client.percolate(req).await.map(tonic::Response::into_inner) }
        })?;
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids, stats))
    }

    fn percolate_filtered_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.validate_ownership(current_position, context.generation(), context.num_shards())?;
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            rank: None,
            shard_id: self.shard_id,
            ownership: Some(proto::ownership_to_proto(context)),
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Percolate, CallKind::Read, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move { client.percolate(req).await.map(tonic::Response::into_inner) }
        })?;
        if !reply.ownership_applied {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::PlacementDecisionMismatch,
            ));
        }
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids, stats))
    }

    fn percolate_filtered_ranked(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            // The ALREADY-COMPILED spec (ADR-075): resolved `TagId` boosts + the priority
            // key, exactly like the filter groups — the server never re-resolves strings.
            rank: Some(proto::rank_spec_to_proto(spec)),
            shard_id: self.shard_id,
            ownership: None,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::PercolateRanked, CallKind::Read, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move { client.percolate(req).await.map(tonic::Response::into_inner) }
        })?;
        // Version-skew honesty: an older server ignores the `rank` field and leaves
        // `ranked` false — fail LOUD rather than fabricate scores or silently hand the
        // caller an unranked ordering it will present as ranked.
        if !reply.ranked || reply.scores.len() != reply.ids.len() {
            return Err(ShardError::Remote(format!(
                "shard did not score a ranked percolate (ranked={}, ids={}, scores={}): \
                 the server predates cluster ranking (ADR-075) — upgrade it or drop the \
                 rank block",
                reply.ranked,
                reply.ids.len(),
                reply.scores.len()
            )));
        }
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids.into_iter().zip(reply.scores).collect(), stats))
    }

    fn percolate_filtered_ranked_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.validate_ownership(current_position, context.generation(), context.num_shards())?;
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            rank: Some(proto::rank_spec_to_proto(spec)),
            shard_id: self.shard_id,
            ownership: Some(proto::ownership_to_proto(context)),
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::PercolateRanked, CallKind::Read, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move { client.percolate(req).await.map(tonic::Response::into_inner) }
        })?;
        if !reply.ownership_applied || !reply.ranked || reply.scores.len() != reply.ids.len() {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::PlacementDecisionMismatch,
            ));
        }
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids.into_iter().zip(reply.scores).collect(), stats))
    }

    fn percolate_top_k_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        self.validate_ownership(current_position, context.generation(), context.num_shards())?;
        let absolute = self.bounded_deadline(deadline)?;
        let base = proto::PercolateTopKRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            rank: Some(proto::rank_program_to_proto(program)),
            size: options.size as u32,
            track_total_hits_up_to: options.track_total_hits_up_to,
            remaining_micros: 0,
            shard_id: self.shard_id,
            ownership: Some(proto::ownership_to_proto(context)),
        };
        let client = self.client.clone();
        let reply = self.call_until(RpcMethod::PercolateTopK, absolute, move |remaining| {
            let mut client = client.clone();
            let mut body = base.clone();
            body.remaining_micros = remaining_micros(remaining);
            let mut request = tonic::Request::new(body);
            request.set_timeout(remaining);
            async move {
                client
                    .percolate_top_k(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        if !reply.bounded
            || !reply.ownership_applied
            || reply.requested_size != options.size as u32
            || reply.placement_generation != context.generation().get()
            || reply.num_shards != context.num_shards()
            || reply.hits.len() > options.size
        {
            return Err(ShardError::Protocol(
                "top-k reply failed bounded/ownership/configuration attestation".into(),
            ));
        }
        let total_hits = reply
            .total_hits
            .map(proto::total_hits_from_proto)
            .ok_or_else(|| ShardError::Protocol("top-k reply omitted total hits".into()))?;
        let rank_stats = reply
            .rank_stats
            .map(proto::rank_stats_from_proto)
            .ok_or_else(|| ShardError::Protocol("top-k reply omitted rank stats".into()))?;
        let result_bytes =
            u64::try_from(reverse_rusty_shard_proto::encoded_len(&reply)).unwrap_or(u64::MAX);
        Ok(ShardRankedMatch {
            hits: reply
                .hits
                .into_iter()
                .map(|hit| crate::rank::RankedHit {
                    logical_id: hit.logical_id,
                    score: hit.score,
                })
                .collect(),
            total_hits,
            stats: reply.stats.map(proto::stats_to_engine).unwrap_or_default(),
            rank_stats,
            result_bytes,
        })
    }

    fn fetch_matches(
        &self,
        logical_ids: &[u64],
        deadline: Option<Instant>,
    ) -> Result<Vec<FetchedMatch>, ShardError> {
        let absolute = self.bounded_deadline(deadline)?;
        let base = proto::FetchMatchesRequest {
            logical_ids: logical_ids.to_vec(),
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
            remaining_micros: 0,
        };
        let client = self.client.clone();
        let generation = self.placement_generation.get();
        let num_shards = self.num_shards;
        self.call_until(RpcMethod::FetchMatches, absolute, move |remaining| {
            let mut client = client.clone();
            let mut body = base.clone();
            body.remaining_micros = remaining_micros(remaining);
            let mut request = tonic::Request::new(body);
            request.set_timeout(remaining);
            async move {
                let mut stream = client.fetch_matches(request).await?.into_inner();
                let mut out = Vec::new();
                while let Some(row) = stream.message().await? {
                    if row.placement_generation != generation || row.num_shards != num_shards {
                        return Err(tonic::Status::failed_precondition(
                            "fetch_matches placement configuration mismatch",
                        ));
                    }
                    out.push(FetchedMatch {
                        logical_id: row.logical_id,
                        source: row.source,
                    });
                }
                Ok(out)
            }
        })
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        let client = self.client.clone();
        let shard_id = self.shard_id;
        let reply = self.call(RpcMethod::NumQueries, CallKind::Read, move || {
            let mut client = client.clone();
            async move {
                client
                    .num_queries(proto::ShardRef { shard_id })
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(reply.count as usize)
    }

    fn live_endpoints(&self) -> Vec<String> {
        // The GC keep-set contribution (ADR-096): the endpoint this client was connected with —
        // wherever live routing reaches through this shard is a node the sweep must not drop from.
        vec![self.endpoint.clone()]
    }

    fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        let client = self.client.clone();
        let shard_id = self.shard_id;
        let reply = self.call(RpcMethod::ClassCounts, CallKind::Read, move || {
            let mut client = client.clone();
            async move {
                client
                    .class_counts(proto::ShardRef { shard_id })
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        let c = reply.counts;
        // The wire keeps `counts` at exactly 4 (a pre-ADR-105 reader hard-errors on
        // any other length mid-rolling-upgrade); class H rides the ADDITIVE `hot`
        // field — proto3 default-0 from an older server, invisible to older readers.
        if c.len() != 4 {
            return Err(ShardError::Remote(format!(
                "class_counts: expected 4 entries, got {}",
                c.len()
            )));
        }
        Ok([c[0], c[1], c[2], c[3], reply.hot])
    }

    fn validate_ownership(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), ShardError> {
        if position != self.shard_id {
            return Err(crate::ownership::OwnershipError::LocalPositionMissing(position).into());
        }
        if generation != self.placement_generation {
            return Err(crate::ownership::OwnershipError::GenerationMismatch {
                expected: generation,
                actual: self.placement_generation,
            }
            .into());
        }
        if num_shards != self.num_shards {
            return Err(crate::ownership::OwnershipError::ShardCountMismatch {
                expected: num_shards,
                actual: self.num_shards,
            }
            .into());
        }
        Ok(())
    }

    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        refuse_wire_tag_ids(items)?;
        // Send raw DSL + raw tags, NOT the pre-extracted feature ids: the server re-compiles
        // read-only against its own frozen dict + resolves tags against its adopted frozen tag
        // space (dict-/tag-agnostic wire). The coordinator's `Extracted` was only for placement.
        let req = proto::IngestRequest {
            items: items
                .iter()
                .map(|q| proto::AddItem {
                    logical_id: q.logical,
                    dsl: q.dsl.clone(),
                    version: q.version,
                    tags: proto::tags_to_proto(&q.tags),
                    placement: Some(proto::placement_to_proto(&q.placement)),
                })
                .collect(),
            shard_id: self.shard_id,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Ingest, CallKind::Write, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                client
                    .ingest_extracted(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(IngestReport {
            ingested: reply.ingested as usize,
            rejected_parse: reply.rejected_parse as usize,
            rejected_class_d: reply.rejected_class_d as usize,
        })
    }

    fn insert_extracted_with_tags(
        &self,
        _ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        let req = proto::InsertRequest {
            item: Some(proto::AddItem {
                logical_id: logical,
                dsl: text.to_string(),
                version,
                tags: proto::tags_to_proto(tags),
                placement: Some(proto::placement_to_proto(
                    &crate::ownership::QueryPlacement::standalone(),
                )),
            }),
            shard_id: self.shard_id,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Insert, CallKind::Write, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                client
                    .insert_extracted(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(reply.present.then_some(reply.local_id))
    }

    fn insert_extracted_with_placement(
        &self,
        _ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<Option<u32>, ShardError> {
        placement.validate_for_shard(self.shard_id, self.placement_generation, self.num_shards)?;
        let req = proto::InsertRequest {
            item: Some(proto::AddItem {
                logical_id: logical,
                dsl: text.to_string(),
                version,
                tags: proto::tags_to_proto(tags),
                placement: Some(proto::placement_to_proto(placement)),
            }),
            shard_id: self.shard_id,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Insert, CallKind::Write, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                client
                    .insert_extracted(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(reply.present.then_some(reply.local_id))
    }

    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        let req = proto::DeleteRequest {
            logical_id: logical,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Delete, CallKind::Write, move || {
            let mut client = client.clone();
            async move { client.delete(req).await.map(tonic::Response::into_inner) }
        })?;
        Ok(reply.removed as usize)
    }

    fn flush(&self) -> Result<(), ShardError> {
        let client = self.client.clone();
        let shard_id = self.shard_id;
        let placement_generation = self.placement_generation.get();
        let num_shards = self.num_shards;
        self.call(RpcMethod::Flush, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .flush(proto::FlushRequest {
                        shard_id,
                        placement_generation,
                        num_shards,
                    })
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(())
    }

    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        // The remote node owns its own segment durability + translog position (server-side); a
        // recovering peer learns the snapshot's position from `FetchManifest.up_to_seqno`, not
        // from this client-side call. Flush so the remote memtable seals; report `LogPos(0)` as
        // a benign sentinel (the coordinator's gRPC recovery uses the server-reported position).
        self.flush()?;
        Ok(LogPos(0))
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        // Never `Ok(vec![])`: a silent empty registry would drop this shard's data on a
        // future durable-remote reopen. Surface that durability is remote-side here.
        Err(ShardError::Remote(
            "segment registry is unavailable for a remote shard (durable checkpoint is \
             local-only in this increment)"
                .into(),
        ))
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        Err(ShardError::Remote(
            "next_seg_id is unavailable for a remote shard".into(),
        ))
    }

    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        // Drain the source's `FetchTranslog` stream (ops > `from`) and decode each entry back
        // into a logical mutation. The coordinator replays these into the recovering target —
        // the no-quiesce catch-up (ADR-039). The tail is the small un-sealed delta.
        let req = proto::FetchTranslogRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            after_seqno: from.0,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        // A long server-stream drain — no per-call deadline (keepalive-guarded), no retry
        // (the catch-up loop is the coordinator's; re-streaming mid-recovery is unsafe).
        let client = self.client.clone();
        self.call(RpcMethod::Translog, CallKind::Unbounded, move || {
            let mut client = client.clone();
            async move {
                let mut stream = client.fetch_translog(req).await?.into_inner();
                let mut out = Vec::new();
                while let Some(entry) = stream.message().await? {
                    if let Some(pm) = proto::translog_entry_to_mutation(entry) {
                        out.push(pm);
                    }
                }
                Ok(out)
            }
        })
    }

    // ---- translog retention leases (ADR-040) ----
    fn acquire_retention_lease(&self) -> Result<(u64, LogPos), ShardError> {
        let req = proto::RetentionLeaseRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            op: 0,
            lease_id: 0,
            pos: 0,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::RetentionLease, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .retention_lease(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok((reply.lease_id, LogPos(reply.pos)))
    }

    fn renew_retention_lease(&self, lease: u64, to: LogPos) -> Result<(), ShardError> {
        let req = proto::RetentionLeaseRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            op: 1,
            lease_id: lease,
            pos: to.0,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        self.call(RpcMethod::RetentionLease, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .retention_lease(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(())
    }

    fn release_retention_lease(&self, lease: u64) -> Result<(), ShardError> {
        let req = proto::RetentionLeaseRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            op: 2,
            lease_id: lease,
            pos: 0,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        self.call(RpcMethod::RetentionLease, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .retention_lease(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn placed(tags: Vec<(String, String)>, tag_ids: Vec<crate::tagdict::TagId>) -> PlacedQuery {
        let norm = crate::normalize::Normalizer::default_vocab().expect("vocab");
        let mut dict = crate::dict::Dict::new();
        let mut lc = String::new();
        let ast = crate::dsl::parse("1994 upper deck").expect("parse");
        let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
        PlacedQuery {
            logical: 1,
            ex,
            dsl: "1994 upper deck".into(),
            version: 1,
            tags,
            tag_ids,
            rank: crate::rank::RankValues::default(),
            placement: crate::ownership::QueryPlacement::standalone(),
        }
    }

    #[test]
    fn wire_guard_passes_raw_tags_and_refuses_pre_resolved_ids() {
        // Raw (key,value) tags are the supported wire shape — no refusal.
        let raw = placed(vec![("category".into(), "cards".into())], Vec::new());
        assert!(refuse_wire_tag_ids(std::slice::from_ref(&raw)).is_ok());
        // Pre-resolved ids (the ADR-074 carry-through) must be refused loudly.
        let carried = placed(
            Vec::new(),
            vec![crate::tagdict::synthetic_tag_id("region", "emea")],
        );
        let err = refuse_wire_tag_ids(&[raw, carried])
            .expect_err("ids must not cross the process boundary");
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
}
