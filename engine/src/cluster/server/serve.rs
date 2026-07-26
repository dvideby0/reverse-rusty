use super::{
    server_tls_config, Arc, ClientSecurity, CoordinatorLeaseService, Duration, MeshAuthVerify,
    ServerSecurity, ShardError, ShardServer, ShardServiceServer, SocketAddr, Status,
    MAX_GRPC_RESULT_BYTES,
};

impl ShardServer {
    /// Install mesh security (ADR-071): a TLS identity to present and/or the
    /// expected cluster token, applied by every `serve*` method. Unset ⇒ the
    /// historical plaintext/open behavior, byte-identical.
    #[must_use]
    pub fn with_security(mut self, security: ServerSecurity) -> Self {
        self.security = security;
        self
    }

    /// Install the CLIENT half of the mesh security (ADR-071) — used when this node
    /// dials OUT (the `RecoverFrom` handler pulls segments + translog from the peer
    /// source). Without it a secured source would reject this node's pull; with it the
    /// internal dial rides the same TLS + token as every coordinator connection.
    #[must_use]
    pub fn with_client_security(mut self, security: ClientSecurity) -> Self {
        self.client_security = security;
        self
    }

    /// Also serve the standard `grpc.health.v1.Health` service on `addr` — a SEPARATE
    /// plaintext port for Kubernetes liveness/readiness probes (ADR-084). Liveness
    /// (`Check("")`) is SERVING once the gRPC server is up; readiness (`Check("ready")`)
    /// tracks dict-adoption — a `--pending` shard is live-but-not-ready until `AdoptDict`.
    /// Unset ⇒ no second listener, byte-identical to the historical single-port behavior.
    #[must_use]
    pub fn with_health_addr(mut self, addr: SocketAddr) -> Self {
        self.health_addr = Some(addr);
        self
    }

    /// Set the static exact encoded-result cap. It may be lowered to any
    /// positive byte count but never raised above tonic's 4 MiB default.
    pub fn with_max_grpc_result_bytes(mut self, bytes: usize) -> Result<Self, ShardError> {
        if !(1..=MAX_GRPC_RESULT_BYTES).contains(&bytes) {
            return Err(ShardError::Config(format!(
                "max gRPC result bytes must be within 1..={MAX_GRPC_RESULT_BYTES}, got {bytes}"
            )));
        }
        self.max_grpc_result_bytes = bytes;
        Ok(self)
    }

    /// Set the node-local maximum number of concurrently executing exhaustive
    /// shard streams. Admission never queues: requests above this bound receive
    /// gRPC `RESOURCE_EXHAUSTED` before a blocking worker is spawned.
    pub fn with_max_concurrent_exhaustive_streams(
        mut self,
        max_concurrent: usize,
    ) -> Result<Self, ShardError> {
        if max_concurrent == 0 || max_concurrent > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(ShardError::Config(format!(
                "max concurrent exhaustive shard streams must be within 1..={}, got \
                 {max_concurrent}",
                tokio::sync::Semaphore::MAX_PERMITS
            )));
        }
        self.exhaustive_permits = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        Ok(self)
    }

    /// Set the node-local wall-clock ceiling for one exhaustive shard stream.
    /// The request's remaining budget may be smaller, but never larger.
    pub fn with_max_exhaustive_stream_duration(
        mut self,
        max_duration: Duration,
    ) -> Result<Self, ShardError> {
        if max_duration.is_zero() {
            return Err(ShardError::Config(
                "max exhaustive shard-stream duration must be non-zero".into(),
            ));
        }
        self.max_exhaustive_stream_duration = max_duration;
        Ok(self)
    }

    pub(in crate::cluster::server) fn check_result_bytes(
        &self,
        encoded: usize,
    ) -> Result<(), Status> {
        if encoded > self.max_grpc_result_bytes {
            Err(Status::resource_exhausted(format!(
                "encoded result is {encoded} bytes; configured maximum is {}",
                self.max_grpc_result_bytes
            )))
        } else {
            Ok(())
        }
    }

    /// Build the tonic server (TLS applied when configured) + the token-verified
    /// service — one assembly shared by every `serve*` flavor so they cannot drift.
    #[allow(clippy::type_complexity)]
    fn secured_router(self) -> Result<tonic::transport::server::Router, tonic::transport::Error> {
        let security = self.security.clone();
        // Server-side HTTP/2 keepalive (ADR-085): PING idle/half-open CLIENT connections and
        // drop the dead ones, so a crashed coordinator/peer can't leak server resources.
        // Off any hot path; default-on via `ServerSecurity::default`.
        let mut builder = tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(security.keepalive_interval))
            .http2_keepalive_timeout(Some(security.keepalive_timeout));
        if let Some(tls) = &security.tls {
            builder = builder.tls_config(server_tls_config(tls))?;
        }
        // The verifier wraps the WHOLE service (pass-through with no token), so every
        // RPC — including a future one — is covered before its handler runs.
        let verify = MeshAuthVerify::with_coordinator_lease(
            security.token,
            Arc::clone(&self.coordinator_lease),
        );
        let coordinator_lease = Arc::clone(&self.coordinator_lease);
        let service = ShardServiceServer::with_interceptor(self, verify);
        Ok(builder.add_service(CoordinatorLeaseService::new(service, coordinator_lease)))
    }

    /// Serve `ShardService` on `addr` until the returned future completes. When a
    /// `--health-addr` was configured ([`with_health_addr`](Self::with_health_addr)), the
    /// plaintext health service runs concurrently on its own port and a watcher tracks
    /// readiness (dict-adoption); the two servers are joined fail-loud (ADR-084).
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        let Some(health_addr) = self.health_addr else {
            return self.secured_router()?.serve(addr).await;
        };
        // Capture a shared handle to the shard map BEFORE `secured_router` consumes `self`. The
        // watcher flips `Check("ready")` to SERVING once any slot adopts a dict — no RPC handler is
        // touched (the shared `Arc<RwLock<…>>` shard map is the seam).
        let reporter = super::super::health::HealthReporter::serving();
        let shards = Arc::clone(&self.shards);
        super::super::health::spawn_readiness_watcher(reporter.clone(), move || {
            shards
                .read()
                .is_ok_and(|m| m.values().any(|s| s.state.load_full().is_some()))
        });
        let data = self.secured_router()?.serve(addr);
        let health = super::super::health::serve_health(health_addr, reporter);
        tokio::try_join!(data, health).map(|_| ())
    }

    /// Serve with a graceful-shutdown `signal` future — used by tests to stop cleanly.
    pub async fn serve_with_shutdown<F>(
        self,
        addr: SocketAddr,
        signal: F,
    ) -> Result<(), tonic::transport::Error>
    where
        F: std::future::Future<Output = ()>,
    {
        self.secured_router()?
            .serve_with_shutdown(addr, signal)
            .await
    }

    /// Serve `ShardService` on an already-bound `incoming` listener (no rebind). Lets a
    /// caller bind the socket first and learn its port — an ephemeral `:0` for tests, or
    /// socket activation in production — without the bind→drop→rebind gap that re-binding
    /// by address would open.
    pub async fn serve_with_incoming(
        self,
        incoming: tonic::transport::server::TcpIncoming,
    ) -> Result<(), tonic::transport::Error> {
        self.secured_router()?.serve_with_incoming(incoming).await
    }
}
