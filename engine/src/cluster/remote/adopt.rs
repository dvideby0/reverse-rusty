use super::{
    block_on_in_context, connect_channel, coordinator_attestation_error, legacy_broad_layout_err,
    probe_actual_dict_fingerprint, proto, Arc, ClientSecurity, Handle, RemoteShard, ShardError,
    TransportMetrics,
};

impl RemoteShard {
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
        coordinator_id: u64,
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
            coordinator_id,
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
        coordinator_id: u64,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        Self::connect_and_adopt_with_identity(
            endpoint,
            handle,
            dict_bytes,
            expected_fp,
            tag_dict_bytes,
            expected_tag_fp,
            shard_id,
            placement_generation,
            num_shards,
            Some(coordinator_id),
            security,
        )
    }

    /// Compatibility builder used by the historical distributed coordinator.
    /// It deliberately leaves the shard process unleased, so multiple
    /// compatibility coordinators keep their pre-ADR-114 behavior; such a
    /// coordinator is refused by the exact exhaustive API.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn connect_and_adopt_compatible_with_security(
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
        Self::connect_and_adopt_with_identity(
            endpoint,
            handle,
            dict_bytes,
            expected_fp,
            tag_dict_bytes,
            expected_tag_fp,
            shard_id,
            placement_generation,
            num_shards,
            None,
            security,
        )
    }

    /// Internal coordinator path used by recovery/handoff. An exclusive
    /// coordinator passes `Some(id)`; the compatibility path passes `None`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn connect_and_adopt_for_coordinator_with_security(
        endpoint: &str,
        handle: Handle,
        dict_bytes: Vec<u8>,
        expected_fp: u64,
        tag_dict_bytes: Vec<u8>,
        expected_tag_fp: u64,
        shard_id: u32,
        placement_generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
        coordinator_id: Option<u64>,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        Self::connect_and_adopt_with_identity(
            endpoint,
            handle,
            dict_bytes,
            expected_fp,
            tag_dict_bytes,
            expected_tag_fp,
            shard_id,
            placement_generation,
            num_shards,
            coordinator_id,
            security,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_and_adopt_with_identity(
        endpoint: &str,
        handle: Handle,
        dict_bytes: Vec<u8>,
        expected_fp: u64,
        tag_dict_bytes: Vec<u8>,
        expected_tag_fp: u64,
        shard_id: u32,
        placement_generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
        coordinator_id: Option<u64>,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        // The claim bit is a one-handshake capability, never a property of the
        // long-lived serving client. Retain a separate claim-only client so a
        // durable shard-process restart can recover through a read-only
        // fingerprint handshake; ordinary RPCs still cannot claim implicitly.
        let client = connect_channel(endpoint, &handle, security, coordinator_id, false)?;
        let claim_client = connect_channel(
            endpoint,
            &handle,
            security,
            coordinator_id,
            coordinator_id.is_some(),
        )?;
        let mut shipper = claim_client.clone();
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
            compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
        };
        let (
            adopted,
            adopted_tag,
            adopted_replicate_all,
            adopted_generation,
            adopted_num_shards,
            adopted_coordinator,
            adopted_compiler_semantics,
        ) = match block_on_in_context(&handle, async move { shipper.adopt_dict(req).await }) {
            Ok(reply) => {
                let r = reply.into_inner();
                (
                    r.fingerprint,
                    r.tag_dict_fingerprint,
                    r.broad_replicate_all,
                    r.placement_generation,
                    r.num_shards,
                    r.coordinator_id,
                    r.compiler_semantics_version,
                )
            }
            // The server holds data under a different dict and refused ours. Read its actual
            // fingerprint so the mismatch is truthful, then fail loud (never a silent drop).
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                // Keep this mismatch diagnostic non-claiming; the claim
                // capability is reserved for explicit ownership handshakes.
                // Probe through the ordinary owner-stamped client after the
                // handshake has established (or confirmed) the lease.
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
                return Err(ShardError::Remote(format!("adopt_dict: {status}")));
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
        if adopted_compiler_semantics != crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION {
            return Err(ShardError::Remote(format!(
                "compiler semantics mismatch after adopt: coordinator {} != server {}",
                crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
                adopted_compiler_semantics
            )));
        }
        if adopted_generation != placement_generation.0 || adopted_num_shards != num_shards {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::GenerationMismatch {
                    expected: placement_generation,
                    actual: crate::ownership::PlacementGeneration(adopted_generation),
                },
            ));
        }
        match coordinator_id {
            Some(expected) if adopted_coordinator != expected => {
                return Err(coordinator_attestation_error(
                    endpoint,
                    expected,
                    adopted_coordinator,
                ))
            }
            None if adopted_coordinator != 0 => {
                return Err(ShardError::Remote(format!(
                    "shard {endpoint} unexpectedly attested coordinator {adopted_coordinator} \
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
