use super::{
    finalized_empty_tag_dict, node_space_cell, read_adopted_space, restore_durable_slots,
    shard_dir, single_slot, sweep_dropped_trash, AdoptedSpace, Arc, ArcSwapOption, ClientSecurity,
    CoordinatorLease, Dict, EngineConfig, HashMap, LocalShard, Normalizer, PathBuf, RwLock,
    ServerSecurity, ServerState, Shard, ShardError, ShardServer, ShardSlot,
    DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS, DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION,
    DEFAULT_MAX_GRPC_RESULT_BYTES,
};

impl ShardServer {
    /// Build a server over a fresh `LocalShard` sharing the given frozen `norm`/`dict` —
    /// the pre-built path (the dict is already arranged to match the coordinator's).
    pub fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        // Pre-built path: starts with an empty tag space; a tagged deployment ships the real one
        // via `AdoptDict` (which rebuilds the shard over it). Empty + finalized so the read-only
        // tag-resolution invariant holds even before an adopt. The node hosts its sole slot at
        // shard-id 0 (ADR-093: the pre-built path is the 1:1 position-0 deployment).
        let tag_dict = Arc::new(finalized_empty_tag_dict());
        let shard = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tag_dict),
            config.clone(),
        );
        let node_dict = node_space_cell(Arc::clone(&dict), Arc::clone(&tag_dict));
        let shards = single_slot(ShardSlot::loaded(ServerState {
            dict,
            tag_dict,
            shard,
        }));
        ShardServer {
            norm,
            config,
            rank_profiles: Arc::new(crate::rank::RankProfiles::default()),
            data_dir: None,
            shards,
            node_dict,
            coordinator_lease: Arc::new(CoordinatorLease::new()),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
            exhaustive_permits: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS,
            )),
            max_exhaustive_stream_duration: DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION,
        }
    }

    /// Build a **pending** server: no dict yet, awaiting an `AdoptDict` from the coordinator
    /// (ADR-034). Reads return `failed_precondition` until a dict is adopted. This is how a
    /// data node starts in a real multi-node deploy — empty, then handed the frozen dict —
    /// instead of rebuilding a byte-identical dict from the whole corpus out-of-band.
    pub fn pending(norm: Arc<Normalizer>, config: EngineConfig) -> Self {
        ShardServer {
            norm,
            config,
            rank_profiles: Arc::new(crate::rank::RankProfiles::default()),
            data_dir: None,
            shards: Arc::new(RwLock::new(HashMap::new())),
            node_dict: Arc::new(ArcSwapOption::from(None)),
            coordinator_lease: Arc::new(CoordinatorLease::new()),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
            exhaustive_permits: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS,
            )),
            max_exhaustive_stream_duration: DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION,
        }
    }

    /// Open (or start) a durable data node at `data_dir` (ADR-072): if the node
    /// previously adopted a dict (persisted alongside its shard state by the durable
    /// `AdoptDict` path), **self-restore** — deserialize the persisted dict + tag
    /// space and reopen the shard from its checkpoint sidecar + translog tail
    /// (ADR-039 §6) — so a restarted container/process resumes serving without
    /// waiting for a coordinator. A fresh directory starts **pending** exactly like
    /// [`Self::pending_durable`]. This is what a deployable node should boot through;
    /// `pending_durable` remains the explicit always-start-empty constructor.
    pub fn open_durable(
        norm: Arc<Normalizer>,
        config: EngineConfig,
        data_dir: PathBuf,
    ) -> Result<Self, ShardError> {
        // Boot hygiene (ADR-096): reclaim any trash-renamed dropped-slot dir whose final delete
        // was interrupted. Best-effort — never fails boot (the ADR-078/079 posture) — and runs
        // BEFORE the adoption branch so a pending node's trash is swept too.
        sweep_dropped_trash(&data_dir);
        // The dict + tag space are ONE atomically-written blob (never desynced); absent
        // ⇒ a never-adopted durable node, which starts pending and adopts on connect.
        let Some((dict_bytes, tag_bytes, placement_generation, num_shards)) =
            read_adopted_space(&data_dir)?
        else {
            return Ok(Self::pending_durable(norm, config, data_dir));
        };
        let dict = Arc::new(crate::storage::deserialize_dict(&dict_bytes).map_err(|e| {
            ShardError::Log(format!(
                "deserializing persisted dict under {}: {e}",
                data_dir.display()
            ))
        })?);
        let tag_dict = Arc::new(
            crate::storage::deserialize_tagdict(&tag_bytes).map_err(|e| {
                ShardError::Log(format!(
                    "deserializing persisted tag dict under {}: {e}",
                    data_dir.display()
                ))
            })?,
        );
        // Restore every slot this node previously hosted from its `shard_<id>/` subdir (ADR-093).
        // Each `new_durable` self-restores via that subdir's checkpoint sidecar (segments attached +
        // translog tail replayed, fingerprint-checked). A fingerprint mismatch fails LOUD
        // (DictMismatch): the durable state was built under a dict that no longer matches the
        // persisted one (a corpus/coordinator change across the restart, ADR-034 divergence); the
        // remedy is to wipe this node's data dir and let the coordinator re-seed it.
        let node_dict = node_space_cell(Arc::clone(&dict), Arc::clone(&tag_dict));
        node_dict.store(Some(Arc::new(AdoptedSpace {
            dict: Arc::clone(&dict),
            tag_dict: Arc::clone(&tag_dict),
            placement_generation,
            num_shards,
        })));
        let slots = restore_durable_slots(&data_dir, &norm, &dict, &tag_dict, &config)?;
        for (&position, slot) in &slots {
            if let Some(state) = slot.state.load_full() {
                state
                    .shard
                    .validate_ownership(position, placement_generation, num_shards)?;
            }
        }
        Ok(ShardServer {
            norm,
            config,
            rank_profiles: Arc::new(crate::rank::RankProfiles::default()),
            data_dir: Some(data_dir),
            shards: Arc::new(RwLock::new(slots)),
            node_dict,
            coordinator_lease: Arc::new(CoordinatorLease::new()),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
            exhaustive_permits: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS,
            )),
            max_exhaustive_stream_duration: DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION,
        })
    }

    /// A **durable, pending** server (ADR-035/036): empty (awaiting `AdoptDict`) but rooted at
    /// `data_dir`, so once it adopts a dict its shard persists segments there. This is the real
    /// recovering/replica node — after adoption it can serve `FetchSegments` and accept
    /// `RecoverFrom`. The durable analogue of [`Self::pending`].
    pub fn pending_durable(norm: Arc<Normalizer>, config: EngineConfig, data_dir: PathBuf) -> Self {
        ShardServer {
            norm,
            config,
            rank_profiles: Arc::new(crate::rank::RankProfiles::default()),
            data_dir: Some(data_dir),
            shards: Arc::new(RwLock::new(HashMap::new())),
            node_dict: Arc::new(ArcSwapOption::from(None)),
            coordinator_lease: Arc::new(CoordinatorLease::new()),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
            exhaustive_permits: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS,
            )),
            max_exhaustive_stream_duration: DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION,
        }
    }

    /// A **durable, pre-built** server: build a segments-only durable shard over `dict` rooted
    /// at `data_dir`. The durable analogue of [`Self::new`]. Errors if the durable engine cannot
    /// be created (e.g. the dir is unwritable).
    pub fn new_durable(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
        data_dir: PathBuf,
    ) -> Result<Self, ShardError> {
        // The sole pre-built slot (shard-id 0) roots its segments at `data_dir/shard_000/` (ADR-093:
        // the per-shard subdir the coordinator's durable layout already uses), not the data_dir root.
        let mut sc = config.clone();
        sc.data_dir = Some(shard_dir(&data_dir, 0));
        let tag_dict = Arc::new(finalized_empty_tag_dict());
        let shard = LocalShard::new_durable(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tag_dict),
            sc,
        )?;
        let node_dict = node_space_cell(Arc::clone(&dict), Arc::clone(&tag_dict));
        let shards = single_slot(ShardSlot::loaded(ServerState {
            dict,
            tag_dict,
            shard,
        }));
        Ok(ShardServer {
            norm,
            config,
            rank_profiles: Arc::new(crate::rank::RankProfiles::default()),
            data_dir: Some(data_dir),
            shards,
            node_dict,
            coordinator_lease: Arc::new(CoordinatorLease::new()),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
            exhaustive_permits: Arc::new(tokio::sync::Semaphore::new(
                DEFAULT_MAX_CONCURRENT_EXHAUSTIVE_STREAMS,
            )),
            max_exhaustive_stream_duration: DEFAULT_MAX_EXHAUSTIVE_STREAM_DURATION,
        })
    }
}
