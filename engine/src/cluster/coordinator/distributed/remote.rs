use super::{
    wrap_handoff, Arc, ClientSecurity, ClusterConfig, ClusterDurable, ClusterEngine, Dict,
    HandoffShard, HashRing, Normalizer, Shard, ShardError, TagDict, TransportMetrics,
};

impl ClusterEngine {
    /// Install per-position handoff handles built by a gRPC builder (ADR-043). Consumes + returns
    /// `self` so a builder can chain it after [`Self::from_parts`]; `handoffs` must be index-aligned
    /// with `shards` (one [`HandoffShard`] per position, sharing the boxed copy already in `shards`,
    /// both produced by [`wrap_handoff`]). The in-process/default path never calls this, so its
    /// `handoffs` stays empty and the cluster is byte-identical to pre-6a.
    pub(super) fn with_handoffs(mut self, handoffs: Vec<Arc<HandoffShard>>) -> Self {
        self.handoffs = handoffs;
        self
    }

    /// Install the live-handoff drain caps from `ClusterConfig` (ADR-044/048). The in-process
    /// default leaves the `from_parts` defaults (8 / 1024); the gRPC builders chain this after
    /// `from_parts` so a handoff on a remote cluster honors the configured caps (and a test can
    /// force the abort path with `handoff_final_drain_cap = 0`).
    pub(super) fn with_handoff_caps(mut self, drain_passes: usize, final_drain_cap: usize) -> Self {
        self.handoff_drain_passes = drain_passes;
        self.handoff_final_drain_cap = final_drain_cap;
        self
    }

    /// Retain the tokio runtime handle the cluster was connected on (ADR-048), so the autoscaler's
    /// `tick` can drive `execute_handoff` (which needs a handle for its sync→async `block_on`
    /// bridge). Only the gRPC builders call this; the in-process path leaves it `None`.
    pub(super) fn with_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.handle = Some(handle);
        self
    }

    /// Retain the mesh client security (ADR-071) the cluster was connected with, so every
    /// LATER internal connection (peer recovery, live handoff) rides the same TLS + token.
    /// Only the secure gRPC builders set it; the default stays empty (plaintext).
    pub(super) fn with_client_security(mut self, security: ClientSecurity) -> Self {
        self.client_security = security;
        self
    }

    /// Retain the optional identity already installed in every serving
    /// RemoteShard. Later recovery/handoff/GC connections preserve the same
    /// leased (`Some`) or compatibility (`None`) mode.
    pub(super) fn with_coordinator_id(mut self, coordinator_id: Option<u64>) -> Self {
        self.coordinator_id = coordinator_id;
        self
    }

    /// Install the SHARED transport-metrics collector (ADR-085) the gRPC builders also handed
    /// to each serving `RemoteShard`, so remote per-RPC stats aggregate on the engine (read via
    /// [`Self::transport_metrics`]). Replaces the empty one `from_parts` created. Only the gRPC
    /// builders call this; the in-process path keeps its all-zero collector.
    pub(super) fn with_transport_metrics(mut self, metrics: Arc<TransportMetrics>) -> Self {
        self.transport_metrics = metrics;
        self
    }

    /// The fence generation each shard position's backing is currently serving under (ADR-043) —
    /// introspection for the handoff state, index-aligned with positions. Empty on the
    /// in-process/default path (no position is handoff-wrapped). Stage 6b's `execute_handoff`
    /// advances a position's generation when it re-points it to a new owner; this is how a
    /// test/operator observes the live map.
    pub fn handoff_generations(&self) -> Vec<u64> {
        self.handoffs.iter().map(|h| h.generation()).collect()
    }

    /// Assemble a cluster whose K shards are REMOTE (gRPC) — one per `endpoints[i]`,
    /// connected on the given tokio `handle`. Placement + routing run here on the
    /// coordinator, while each server re-compiles DSL read-only against its copy of the
    /// frozen dict, so the ids line up only when the dicts match. To guarantee that, the
    /// coordinator **ships** its dict to each server at connect (ADR-034): an empty/pending
    /// server adopts it, a server already holding it no-ops, and a server holding *data*
    /// under a divergent dict refuses — surfaced as [`ShardError::DictMismatch`], so a
    /// divergent feature space fails loud instead of dropping matches silently (the ADR-029
    /// handshake, now backed by shipping). A data node therefore need not rebuild a
    /// byte-identical dict from the corpus out-of-band; only `norm` must still match the
    /// servers' (`default_vocab()` today — normalizer shipping is a later step, ADR-034).
    /// `endpoints.len()` must equal `config.num_shards`; endpoint `i` serves shard `i`.
    /// Load the corpus afterwards with [`Self::ingest`].
    pub fn connect_remote(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        Self::connect_remote_with_security(
            norm,
            dict,
            tag_dict,
            config,
            endpoints,
            handle,
            ClientSecurity::default(),
        )
    }

    /// [`connect_remote`](Self::connect_remote) over a secured mesh (ADR-071): TLS per the
    /// client config + the cluster token on every RPC, including the connect-time
    /// `AdoptDict` handshake and every LATER internal connection (peer recovery, handoff —
    /// the config is retained). A default (empty) config is byte-identical.
    pub fn connect_remote_with_security(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
        security: ClientSecurity,
    ) -> Result<Self, ShardError> {
        Self::connect_remote_with_security_mode(
            norm, dict, tag_dict, config, endpoints, handle, security, None,
        )
    }

    /// Assemble a remote cluster with an exclusive renewable coordinator
    /// lease, required before the exact exhaustive API can attest completion.
    /// `coordinator_id` must be generated once and reused for all retries of
    /// this logical coordinator.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_remote_exclusive(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
        coordinator_id: u64,
    ) -> Result<Self, ShardError> {
        Self::connect_remote_exclusive_with_security(
            norm,
            dict,
            tag_dict,
            config,
            endpoints,
            handle,
            coordinator_id,
            ClientSecurity::default(),
        )
    }

    /// Secured-mesh variant of [`Self::connect_remote_exclusive`].
    #[allow(clippy::too_many_arguments)]
    pub fn connect_remote_exclusive_with_security(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
        coordinator_id: u64,
        security: ClientSecurity,
    ) -> Result<Self, ShardError> {
        if coordinator_id == 0 {
            return Err(ShardError::Config(
                "exclusive remote coordinator id must be non-zero".into(),
            ));
        }
        Self::connect_remote_with_security_mode(
            norm,
            dict,
            tag_dict,
            config,
            endpoints,
            handle,
            security,
            Some(coordinator_id),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_remote_with_security_mode(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
        security: ClientSecurity,
        coordinator_id: Option<u64>,
    ) -> Result<Self, ShardError> {
        if endpoints.len() != config.num_shards {
            return Err(ShardError::Config(format!(
                "connect_remote needs exactly one endpoint per shard: got {} endpoints \
                 for {} shards",
                endpoints.len(),
                config.num_shards
            )));
        }
        if config.replication_factor > 1 {
            return Err(ShardError::Config(
                "connect_remote does not support replication_factor > 1; remote per-shard \
                 replication is clustering step 4b (ADR-036)"
                    .into(),
            ));
        }
        let ring = HashRing::new(config.num_shards, config.vnodes)?;
        // Cross-process shared-dict invariant: placement/routing ids line up only when every
        // server's frozen dict equals this coordinator's. SHIP it (ADR-034): serialize once,
        // then adopt per endpoint. An empty server adopts; a server already holding this dict
        // no-ops; a server holding data under a divergent dict refuses → DictMismatch (loud,
        // never a silent drop). Servers therefore needn't rebuild the dict from the corpus.
        let expected = dict.fingerprint();
        let dict_bytes = crate::storage::serialize_dict(&dict);
        // Ship the frozen tag space alongside the dict (ADR-055), so each server resolves ingested
        // tags against the same space the coordinator's filter `TagId`s came from.
        let expected_tag = tag_dict.fingerprint();
        let tag_dict_bytes = crate::storage::serialize_tagdict(&tag_dict);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(endpoints.len());
        // Wrap each remote position in a `HandoffShard` so it can be re-pointed at a new owner at
        // runtime (ADR-043); the typed handles are installed via `with_handoffs` below.
        let mut handoffs: Vec<Arc<HandoffShard>> = Vec::with_capacity(endpoints.len());
        // ONE shared transport-metrics collector (ADR-085): every serving RemoteShard records
        // into it and the engine reads it via `transport_metrics()` (installed below).
        let metrics = Arc::new(TransportMetrics::new());
        // CO-LOCATION (ADR-093 Stage 2): several positions may share one endpoint (fewer pods than
        // shards, expressed by repeating an endpoint in the list). `endpoints[i]` is still position
        // `i`'s endpoint (the len check holds), but the FIRST position on each distinct endpoint
        // ships+adopts the node dict; every LATER position on that node reuses it via a lightweight
        // `AddShard` (no dict re-ship / re-deserialize). Routing stays position-indexed, so
        // co-location is transparent to it.
        // INITIAL is EXACT here, not a placeholder: a remote cluster cannot bump the
        // placement generation (`set_vocab`/`resize` refuse handoff-wrapped and
        // non-local shards, and every builder below wraps positions in HandoffShard),
        // so the generation a data node persisted at adopt time is always INITIAL. If
        // a future increment lifts that refusal it must thread the real generation
        // through these builders — the failure until then is a loud connect-time
        // `adopt_dict` refusal, never a silent mismatch.
        let mut adopted: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (position, ep) in endpoints.iter().enumerate() {
            let shard_id = position as u32;
            let remote = if adopted.insert(ep.as_str()) {
                match coordinator_id {
                    Some(id) => {
                        crate::cluster::remote::RemoteShard::connect_and_adopt_with_security(
                            ep,
                            handle.clone(),
                            dict_bytes.clone(),
                            expected,
                            tag_dict_bytes.clone(),
                            expected_tag,
                            shard_id,
                            crate::ownership::PlacementGeneration::INITIAL,
                            config.num_shards as u32,
                            id,
                            &security,
                        )
                    }
                    None => crate::cluster::remote::RemoteShard::
                        connect_and_adopt_compatible_with_security(
                            ep,
                            handle.clone(),
                            dict_bytes.clone(),
                            expected,
                            tag_dict_bytes.clone(),
                            expected_tag,
                            shard_id,
                            crate::ownership::PlacementGeneration::INITIAL,
                            config.num_shards as u32,
                            &security,
                        ),
                }?
            } else {
                match coordinator_id {
                    Some(id) => {
                        crate::cluster::remote::RemoteShard::connect_and_add_shard_with_security(
                            ep,
                            handle.clone(),
                            expected,
                            expected_tag,
                            shard_id,
                            crate::ownership::PlacementGeneration::INITIAL,
                            config.num_shards as u32,
                            id,
                            &security,
                        )
                    }
                    None => crate::cluster::remote::RemoteShard::
                        connect_and_add_shard_compatible_with_security(
                            ep,
                            handle.clone(),
                            expected,
                            expected_tag,
                            shard_id,
                            crate::ownership::PlacementGeneration::INITIAL,
                            config.num_shards as u32,
                            &security,
                        ),
                }?
            }
            .with_metrics(Arc::clone(&metrics));
            let (boxed, h) = wrap_handoff(Box::new(remote), 0);
            shards.push(boxed);
            handoffs.push(h);
        }
        // A remote cluster is non-durable at the coordinator in this increment (the
        // coordinator-level durable log is the in-process story; cross-node durability
        // is a later step). Use the in-memory log so behavior is unchanged.
        let durable =
            ClusterDurable::in_memory(config.num_shards as u32, config.vnodes, dict.fingerprint());
        Ok(Self::from_parts(
            norm,
            dict,
            tag_dict,
            ring,
            shards,
            config.include_broad,
            config.replication_factor,
            config.per_shard.clone(),
            durable,
        )?
        .with_handoffs(handoffs)
        .with_handoff_caps(config.handoff_drain_passes, config.handoff_final_drain_cap)
        .with_handle(handle.clone())
        .with_client_security(security)
        .with_coordinator_id(coordinator_id)
        .with_transport_metrics(metrics))
    }
}
