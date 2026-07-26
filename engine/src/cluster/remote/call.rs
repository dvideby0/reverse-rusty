use super::{
    block_on_in_context, block_on_timeout_in_context, coordinator_attestation_error,
    grpc_deadline_status, no_live_coordinator_lease_status, proto, ranked_rpc_err, rpc_err,
    run_with_retry, run_with_retry_until, Arc, CallKind, Duration, Future, Instant, RemoteShard,
    RpcMethod, RpcOutcome, Shard, ShardError, TransportMetrics,
};

impl RemoteShard {
    /// Drive an async RPC to completion from the synchronous [`Shard`] seam, safe regardless
    /// of the caller's thread context (see the module docs + ADR-047). Every RPC method below
    /// goes through this rather than `self.handle.block_on` directly, so a percolate or write
    /// issued from a tokio runtime worker re-enters via `block_in_place` instead of panicking.
    pub(super) fn block_on<F>(&self, fut: F) -> F::Output
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

    /// Reclaim this coordinator's lease after the shard process restarted.
    /// `DictFingerprint` is a claim-capable, read-only handshake: it can only
    /// succeed after the node restored/adopted its node space, and it never
    /// creates an empty shard slot.
    pub(super) fn reclaim_coordinator_lease(
        &self,
        deadline: Option<Instant>,
    ) -> Result<(), ShardError> {
        let (Some(expected_coordinator), Some(claim_client)) =
            (self.coordinator_id, self.claim_client.as_ref())
        else {
            return Err(ShardError::Remote(
                "remote shard has no coordinator claim capability".into(),
            ));
        };
        let timeout = match deadline {
            Some(deadline) => deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or(ShardError::DeadlineExceeded)?
                .min(self.transport.write_timeout),
            None => self.transport.write_timeout,
        };
        let mut claimant = claim_client.clone();
        let mut request = tonic::Request::new(proto::Empty {});
        request.set_timeout(timeout);
        let reply = block_on_timeout_in_context(&self.handle, timeout, async move {
            claimant.dict_fingerprint(request).await
        })
        .map_err(|_| {
            if deadline.is_some() {
                ShardError::DeadlineExceeded
            } else {
                ShardError::Remote("coordinator lease recovery timed out".into())
            }
        })?
        .map_err(|status| ShardError::Remote(format!("coordinator lease recovery: {status}")))?
        .into_inner();

        if reply.fingerprint != self.dict_fp {
            return Err(ShardError::DictMismatch {
                expected: self.dict_fp,
                actual: reply.fingerprint,
            });
        }
        if reply.tag_dict_fingerprint != self.tag_dict_fp
            || !reply.broad_replicate_all
            || reply.placement_generation != self.placement_generation.get()
            || reply.num_shards != self.num_shards
        {
            return Err(ShardError::Remote(
                "coordinator lease recovery attested a divergent shard configuration".into(),
            ));
        }
        if reply.coordinator_id != expected_coordinator {
            return Err(coordinator_attestation_error(
                &self.endpoint,
                expected_coordinator,
                reply.coordinator_id,
            ));
        }
        Ok(())
    }

    /// The single instrumented RPC seam (ADR-085): drive `mk`'s future with a per-call
    /// deadline (unary reads/writes) and bounded fail-loud retry of IDEMPOTENT reads on a
    /// transient error, recording the outcome + latency. `mk` is a FACTORY — a tonic call
    /// future is single-use, so each attempt rebuilds it from a cloned client + request. A
    /// timeout or exhausted retry surfaces as a loud [`ShardError`], never a dropped result,
    /// so the coordinator's fan-out still fails closed (a swallowed shard = false negative).
    pub(super) fn call<R, Fut, MkFut>(
        &self,
        method: RpcMethod,
        kind: CallKind,
        mk: MkFut,
    ) -> Result<R, ShardError>
    where
        MkFut: Fn() -> Fut + Send + Sync,
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
        let (mut result, mut attempts, mut timed_out) =
            self.block_on(run_with_retry(&mk, deadline, max_retries));
        if result
            .as_ref()
            .err()
            .is_some_and(no_live_coordinator_lease_status)
            && self.coordinator_id.is_some()
        {
            if let Err(error) = self.reclaim_coordinator_lease(None) {
                self.metrics
                    .record(method, RpcOutcome::Error, started.elapsed(), attempts);
                return Err(error);
            }
            let (retried, retry_attempts, retry_timed_out) =
                self.block_on(run_with_retry(&mk, deadline, max_retries));
            result = retried;
            attempts = attempts.saturating_add(retry_attempts).saturating_add(1);
            timed_out = retry_timed_out;
        }
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
    pub(super) fn call_until<R, Fut, MkFut>(
        &self,
        method: RpcMethod,
        deadline: Instant,
        mk: MkFut,
    ) -> Result<R, ShardError>
    where
        MkFut: Fn(Duration) -> Fut + Send + Sync,
        Fut: Future<Output = Result<R, tonic::Status>> + Send,
        R: Send,
    {
        let started = Instant::now();
        let (mut result, mut attempts, mut timed_out) = self.block_on(run_with_retry_until(
            &mk,
            deadline,
            self.transport.read_retries,
        ));
        if result
            .as_ref()
            .err()
            .is_some_and(no_live_coordinator_lease_status)
            && self.coordinator_id.is_some()
        {
            if let Err(error) = self.reclaim_coordinator_lease(Some(deadline)) {
                let outcome = if matches!(&error, ShardError::DeadlineExceeded) {
                    RpcOutcome::Timeout
                } else {
                    RpcOutcome::Error
                };
                self.metrics
                    .record(method, outcome, started.elapsed(), attempts);
                return Err(error);
            }
            let (retried, retry_attempts, retry_timed_out) = self.block_on(run_with_retry_until(
                &mk,
                deadline,
                self.transport.read_retries,
            ));
            result = retried;
            attempts = attempts.saturating_add(retry_attempts).saturating_add(1);
            timed_out = retry_timed_out;
        }
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
                ranked_rpc_err(&status)
            }
        })
    }

    pub(super) fn bounded_deadline(
        &self,
        deadline: Option<Instant>,
    ) -> Result<Instant, ShardError> {
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
