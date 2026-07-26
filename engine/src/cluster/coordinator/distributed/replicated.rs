use super::{
    wrap_handoff, Arc, ClientSecurity, ClusterConfig, ClusterDurable, ClusterEngine, Dict,
    HandoffShard, HashRing, Normalizer, Shard, ShardError, ShardGroup, TagDict, TransportMetrics,
};

impl ClusterEngine {
    /// Assemble a cluster whose K shard POSITIONS are each a [`ReplicatedShard`](crate::cluster::replica::ReplicatedShard)
    /// over RF gRPC [`RemoteShard`]s (a primary + replicas), one [`ShardGroup`] per position. Ships +
    /// adopts the frozen dict on EVERY endpoint (ADR-034), then wraps position `i`'s RemoteShards
    /// into one composite boxed as the `i`-th shard — so the coordinator's placement / routing /
    /// merge is identical to a non-replicated remote cluster, while reads fail over to a replica and
    /// writes fan out (ADR-035). `groups.len()` must equal `config.num_shards`; a group with no
    /// replicas degenerates to a bare `RemoteShard` (identical to [`Self::connect_remote`]). Load the
    /// corpus afterwards with [`Self::ingest`].
    pub fn connect_replicated(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        Self::connect_replicated_with_security(
            norm,
            dict,
            tag_dict,
            config,
            groups,
            handle,
            ClientSecurity::default(),
        )
    }

    /// [`connect_replicated`](Self::connect_replicated) over a secured mesh (ADR-071) —
    /// the replicated analogue of
    /// [`connect_remote_with_security`](Self::connect_remote_with_security); the config is
    /// retained for later internal connections. A default (empty) config is byte-identical.
    pub fn connect_replicated_with_security(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
        security: ClientSecurity,
    ) -> Result<Self, ShardError> {
        Self::connect_replicated_with_security_mode(
            norm, dict, tag_dict, config, groups, handle, security, None,
        )
    }

    /// Replicated analogue of [`Self::connect_remote_exclusive`].
    #[allow(clippy::too_many_arguments)]
    pub fn connect_replicated_exclusive(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
        coordinator_id: u64,
    ) -> Result<Self, ShardError> {
        Self::connect_replicated_exclusive_with_security(
            norm,
            dict,
            tag_dict,
            config,
            groups,
            handle,
            coordinator_id,
            ClientSecurity::default(),
        )
    }

    /// Secured-mesh variant of [`Self::connect_replicated_exclusive`].
    #[allow(clippy::too_many_arguments)]
    pub fn connect_replicated_exclusive_with_security(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
        coordinator_id: u64,
        security: ClientSecurity,
    ) -> Result<Self, ShardError> {
        if coordinator_id == 0 {
            return Err(ShardError::Config(
                "exclusive remote coordinator id must be non-zero".into(),
            ));
        }
        Self::connect_replicated_with_security_mode(
            norm,
            dict,
            tag_dict,
            config,
            groups,
            handle,
            security,
            Some(coordinator_id),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_replicated_with_security_mode(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
        security: ClientSecurity,
        coordinator_id: Option<u64>,
    ) -> Result<Self, ShardError> {
        if groups.len() != config.num_shards {
            return Err(ShardError::Config(format!(
                "connect_replicated needs one ShardGroup per shard: got {} for {} shards",
                groups.len(),
                config.num_shards
            )));
        }
        let ring = HashRing::new(config.num_shards, config.vnodes)?;
        let expected = dict.fingerprint();
        let dict_bytes = crate::storage::serialize_dict(&dict);
        // Ship the frozen tag space alongside the dict on every endpoint (ADR-055).
        let expected_tag = tag_dict.fingerprint();
        let tag_dict_bytes = crate::storage::serialize_tagdict(&tag_dict);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(groups.len());
        // Each position (a bare remote or a ReplicatedShard group) is wrapped in a `HandoffShard`
        // so the whole group can be re-pointed at a new owner at runtime (ADR-043).
        let mut handoffs: Vec<Arc<HandoffShard>> = Vec::with_capacity(groups.len());
        // ONE shared transport-metrics collector (ADR-085); see `connect_remote_with_security`.
        let metrics = Arc::new(TransportMetrics::new());
        // CO-LOCATION (ADR-093 Stage 3): a primary and/or replicas of different positions may share
        // one endpoint (fewer pods than shards × RF). The FIRST connection to each distinct endpoint
        // ships+adopts the node dict; every LATER slot on that node reuses it via a lightweight
        // `AddShard` (no dict re-ship / re-deserialize). This set spans BOTH primaries and replicas
        // across all groups, so a node hosting e.g. pos-0's primary and pos-1's replica adopts once
        // and gains its second slot via `AddShard` (which keys on the node dict, not the shard-id).
        let mut adopted: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (position, g) in groups.iter().enumerate() {
            // A replica hosts the SAME global position (shard-id) as its primary (ADR-093).
            let shard_id = position as u32;
            let primary = if adopted.insert(g.primary.as_str()) {
                match coordinator_id {
                    Some(id) => {
                        crate::cluster::remote::RemoteShard::connect_and_adopt_with_security(
                            &g.primary,
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
                            &g.primary,
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
                            &g.primary,
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
                            &g.primary,
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
            let mut replicas: Vec<Box<dyn Shard>> = Vec::with_capacity(g.replicas.len());
            for ep in &g.replicas {
                let r = if adopted.insert(ep.as_str()) {
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
                replicas.push(Box::new(r) as Box<dyn Shard>);
            }
            let shard: Box<dyn Shard> = if replicas.is_empty() {
                Box::new(primary)
            } else {
                Box::new(crate::cluster::replica::ReplicatedShard::new(
                    Box::new(primary) as Box<dyn Shard>,
                    replicas,
                ))
            };
            let (boxed, h) = wrap_handoff(shard, 0);
            shards.push(boxed);
            handoffs.push(h);
        }
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
