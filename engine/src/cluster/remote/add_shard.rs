use super::{
    block_on_in_context, connect_channel, coordinator_attestation_error, legacy_broad_layout_err,
    probe_actual_dict_fingerprint, proto, Arc, ClientSecurity, Handle, RemoteShard, ShardError,
    TransportMetrics,
};

impl RemoteShard {
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
        coordinator_id: u64,
    ) -> Result<Self, ShardError> {
        Self::connect_and_add_shard_with_security(
            endpoint,
            handle,
            expected_fp,
            expected_tag_fp,
            shard_id,
            crate::ownership::PlacementGeneration::INITIAL,
            shard_id.saturating_add(1),
            coordinator_id,
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
        coordinator_id: u64,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        Self::connect_and_add_shard_with_identity(
            endpoint,
            handle,
            expected_fp,
            expected_tag_fp,
            shard_id,
            placement_generation,
            num_shards,
            Some(coordinator_id),
            security,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn connect_and_add_shard_compatible_with_security(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        expected_tag_fp: u64,
        shard_id: u32,
        placement_generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        Self::connect_and_add_shard_with_identity(
            endpoint,
            handle,
            expected_fp,
            expected_tag_fp,
            shard_id,
            placement_generation,
            num_shards,
            None,
            security,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_and_add_shard_with_identity(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        expected_tag_fp: u64,
        shard_id: u32,
        placement_generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
        coordinator_id: Option<u64>,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        // As in AdoptDict, retain the claim marker only on this handshake
        // client. The returned serving client carries the owner id but cannot
        // claim a freshly restarted process.
        let client = connect_channel(endpoint, &handle, security, coordinator_id, false)?;
        let claim_client = connect_channel(
            endpoint,
            &handle,
            security,
            coordinator_id,
            coordinator_id.is_some(),
        )?;
        let mut shipper = claim_client.clone();
        // No dict bytes — just NAME the slot and attest the node's fingerprints (ADR-093 Stage 2).
        let req = proto::AddShardRequest {
            shard_id,
            dict_fingerprint: expected_fp,
            tag_dict_fingerprint: expected_tag_fp,
            placement_generation: placement_generation.0,
            num_shards,
            compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
        };
        let (
            added,
            added_tag,
            added_replicate_all,
            added_generation,
            added_num_shards,
            added_coordinator,
            added_compiler_semantics,
        ) = match block_on_in_context(&handle, async move { shipper.add_shard(req).await }) {
            Ok(reply) => {
                let r = reply.into_inner();
                (
                    r.dict_fingerprint,
                    r.tag_dict_fingerprint,
                    r.broad_replicate_all,
                    r.placement_generation,
                    r.num_shards,
                    r.coordinator_id,
                    r.compiler_semantics_version,
                )
            }
            // The node's adopted dict differs from ours (or it adopted none). Read its actual
            // fingerprint so the mismatch is truthful, then fail loud (never a silent drop).
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                if let Some(actual) = probe_actual_dict_fingerprint(
                    endpoint,
                    &handle,
                    security,
                    &client,
                    coordinator_id,
                ) {
                    if actual != expected_fp {
                        return Err(ShardError::DictMismatch {
                            expected: expected_fp,
                            actual,
                        });
                    }
                }
                return Err(ShardError::Remote(format!("add_shard: {status}")));
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
        if added_compiler_semantics != crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION {
            return Err(ShardError::Remote(format!(
                "compiler semantics mismatch after add_shard: coordinator {} != server {}",
                crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
                added_compiler_semantics
            )));
        }
        if added_generation != placement_generation.0 || added_num_shards != num_shards {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::GenerationMismatch {
                    expected: placement_generation,
                    actual: crate::ownership::PlacementGeneration(added_generation),
                },
            ));
        }
        match coordinator_id {
            Some(expected) if added_coordinator != expected => {
                return Err(coordinator_attestation_error(
                    endpoint,
                    expected,
                    added_coordinator,
                ))
            }
            None if added_coordinator != 0 => {
                return Err(ShardError::Remote(format!(
                    "shard {endpoint} unexpectedly attested coordinator {added_coordinator} \
                     to an unleased compatibility handshake"
                )))
            }
            _ => {}
        }
        Ok(RemoteShard {
            client,
            claim_client: coordinator_id.map(|_| claim_client),
            coordinator_id,
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
}
