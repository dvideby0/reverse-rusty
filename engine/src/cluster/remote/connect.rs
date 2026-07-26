use super::{
    block_on_in_context, connect_channel, coordinator_attestation_error, legacy_broad_layout_err,
    no_live_coordinator_lease_status, proto, rpc_err, Arc, ClientSecurity, Handle, RemoteShard,
    ShardError, TransportMetrics,
};

impl RemoteShard {
    /// Mint a non-zero process-boot-unique coordinator identity for the direct
    /// adoption APIs. Generate this **once**, retain it, and reuse it for every
    /// retry and every node/shard owned by the same coordinator. In particular,
    /// do not mint a replacement after a lost `AdoptDict` response: the server
    /// may already have committed the first identity.
    pub fn new_coordinator_id() -> u64 {
        super::super::security::fresh_coordinator_id()
    }

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
        Self::connect_with_identity(
            endpoint,
            handle,
            expected_fp,
            expected_tag_fp,
            shard_id,
            None,
            security,
        )
    }

    /// Coordinator-owned variant used for every later recovery, handoff, and
    /// GC connection made by a remote [`ClusterEngine`](super::ClusterEngine).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn connect_for_coordinator_with_security(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        expected_tag_fp: u64,
        shard_id: u32,
        coordinator_id: Option<u64>,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        Self::connect_with_identity(
            endpoint,
            handle,
            expected_fp,
            expected_tag_fp,
            shard_id,
            coordinator_id,
            security,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_with_identity(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        expected_tag_fp: u64,
        shard_id: u32,
        coordinator_id: Option<u64>,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        let client = connect_channel(endpoint, &handle, security, coordinator_id, false)?;
        let claim_client = coordinator_id
            .map(|id| connect_channel(endpoint, &handle, security, Some(id), true))
            .transpose()?;
        // Handshake before trusting the shard: clone the client for the probe RPC (a cheap
        // Channel bump, mirroring the per-call pattern below).
        let mut probe = client.clone();
        let probed = block_on_in_context(&handle, async move {
            probe.dict_fingerprint(proto::Empty {}).await
        });
        let reply = match probed {
            Ok(reply) => reply,
            Err(status) if no_live_coordinator_lease_status(&status) && claim_client.is_some() => {
                let mut claimant = claim_client
                    .as_ref()
                    .ok_or_else(|| {
                        ShardError::Remote("coordinator lease recovery client disappeared".into())
                    })?
                    .clone();
                block_on_in_context(&handle, async move {
                    claimant.dict_fingerprint(proto::Empty {}).await
                })
                .map_err(|status| rpc_err(&status))?
            }
            Err(status) => return Err(rpc_err(&status)),
        }
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
        if reply.compiler_semantics_version != crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION {
            return Err(ShardError::Remote(format!(
                "compiler semantics mismatch at connect: coordinator {} != server {}",
                crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
                reply.compiler_semantics_version
            )));
        }
        if reply.placement_generation == 0 || reply.num_shards == 0 {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::MissingGeneration,
            ));
        }
        if let Some(expected_coordinator) = coordinator_id {
            if reply.coordinator_id != expected_coordinator {
                return Err(coordinator_attestation_error(
                    endpoint,
                    expected_coordinator,
                    reply.coordinator_id,
                ));
            }
        }
        Ok(RemoteShard {
            client,
            claim_client,
            coordinator_id,
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
}
