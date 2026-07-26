use super::{
    resolve_lease_ttl, translog, Arc, ArcSwap, Dict, Engine, EngineConfig, LocalShard, LogPos,
    Mutex, Normalizer, RetentionLeases, ShardError, TagDict,
};

impl LocalShard {
    /// Build a shard sharing the coordinator's frozen normalizer + dict. In-memory ⇒ a
    /// no-op [`NullClusterLog`](crate::cluster::clog::NullClusterLog) translog (byte-identical to
    /// pre-ADR-039) and no checkpoint sidecar.
    pub(crate) fn new(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        mut config: EngineConfig,
    ) -> Self {
        // Cluster shards are coordinator-gated storage: the COORDINATOR's placement is the SOLE
        // class-D gate (ADR-068/080), so a shard must ACCEPT whatever the coordinator places — else
        // a class-D the coordinator accepted (or replays / rebuilds as already-accepted) would be
        // silently dropped by the shard's own knob, a false negative (codex review). The operator's
        // front-door knob lives on the coordinator (`ClusterEngine::per_shard`), not here.
        config.accept_class_d = true;
        let retention_lease_ttl = resolve_lease_ttl(&config);
        // `tag_dict` is moved into the engine (the shard keeps no separate copy — the engine holds
        // the shared frozen tag space and does all read-only resolution against it).
        let engine = Engine::with_shared(Arc::clone(&norm), Arc::clone(&dict), tag_dict, config);
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog: translog::null(),
            retention: Mutex::new(RetentionLeases::default()),
            retention_lease_ttl,
            event_sink: Mutex::new(None),
            norm,
            dict,
            data_dir: None,
            pits: Mutex::new(crate::util::fast_map()),
        }
    }

    /// Build a DURABLE shard (ADR-032): an engine that persists compiled segments under
    /// `config.data_dir` with no WAL and no own manifest, plus a durable translog (ADR-039).
    /// **Self-restart (ADR-039 §6):** if a checkpoint sidecar is already present in the dir, this
    /// is a node restarting over its own prior data — attach its committed segments and replay the
    /// translog tail instead of starting fresh. Otherwise a fresh empty durable shard.
    pub(crate) fn new_durable(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        mut config: EngineConfig,
    ) -> Result<Self, ShardError> {
        // Coordinator-gated storage: the shard always accepts class-D (see `new`); the operator's
        // front-door knob lives on the coordinator. Forced before the self-restart path inherits it.
        config.accept_class_d = true;
        let dir = config.data_dir.clone().ok_or_else(|| {
            ShardError::Log("durable shard requires a data_dir for its translog".into())
        })?;
        if let Some(ckpt) = translog::read_sidecar(&dir)? {
            return Self::open_durable_self(norm, dict, tag_dict, config, &ckpt);
        }
        let retention_lease_ttl = resolve_lease_ttl(&config);
        let translog = translog::open_fresh(&dir, config.wal_sync_on_write)?;
        let engine = Engine::with_shared_segments_only(
            Arc::clone(&norm),
            Arc::clone(&dict),
            tag_dict,
            config,
        )
        .map_err(|e| ShardError::Log(format!("creating durable shard: {e}")))?;
        // Write the INITIAL (empty) checkpoint sidecar so a durable shard is
        // self-restartable from the moment it exists (ADR-072): a crash before the
        // first seal then takes the `open_durable_self` path above — open the
        // EXISTING translog and replay its whole tail — instead of this fresh path,
        // whose `open_fresh` resets the translog (which would drop acknowledged
        // live writes) and ignores bulk-written segments.
        translog::write_sidecar(
            &dir,
            &translog::ShardCheckpoint {
                next_seg_id: engine.next_seg_id(),
                local_checkpoint: 0,
                dict_fingerprint: dict.fingerprint(),
                segment_files: Vec::new(),
                compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
                source_file_name: engine.source_file_name().to_string(),
            },
        )?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        Ok(LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog,
            retention: Mutex::new(RetentionLeases::default()),
            retention_lease_ttl,
            event_sink: Mutex::new(None),
            norm,
            dict,
            data_dir: Some(dir),
            pits: Mutex::new(crate::util::fast_map()),
        })
    }

    /// Reopen a durable shard by attaching an EXPLICIT committed segment list (ADR-032) against
    /// the shared dict — attach-and-mmap, not re-ingest. `files`/`next_seg_id` come from the
    /// coordinator's `cluster_manifest.bin`; the attached segments are the durable base, and the
    /// translog starts FRESH (ADR-039) — the coordinator `ClusterLog` (in-process) or the
    /// peer-recovery tail repopulates it. (Distinct from `new_durable`'s sidecar-driven
    /// self-restart: this is the coordinator-managed attach.)
    pub(crate) fn open_segments(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
    ) -> Result<Self, ShardError> {
        Self::open_segments_inner(
            norm,
            dict,
            tag_dict,
            config,
            files,
            next_seg_id,
            "sources.dat",
            false,
        )
    }

    /// Attach current-semantics segments using the source sidecar selected by
    /// the coordinator commit document.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_segments_with_source_file(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
        source_file_name: &str,
    ) -> Result<Self, ShardError> {
        Self::open_segments_inner(
            norm,
            dict,
            tag_dict,
            config,
            files,
            next_seg_id,
            source_file_name,
            false,
        )
    }

    /// Attach legacy compiler materializations only for the coordinator's
    /// boot-time ADR-118 blue/green rebuild. This shard must not be published
    /// for reads unless the coordinator finishes that rebuild and commits its
    /// new manifest.
    pub(crate) fn open_segments_for_compiler_migration(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
    ) -> Result<Self, ShardError> {
        Self::open_segments_inner(
            norm,
            dict,
            tag_dict,
            config,
            files,
            next_seg_id,
            "sources.dat",
            true,
        )
    }

    /// Coordinator-only legacy attach using the source sidecar selected by the
    /// old manifest.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_segments_for_compiler_migration_with_source_file(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
        source_file_name: &str,
    ) -> Result<Self, ShardError> {
        Self::open_segments_inner(
            norm,
            dict,
            tag_dict,
            config,
            files,
            next_seg_id,
            source_file_name,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn open_segments_inner(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        mut config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
        source_file_name: &str,
        allow_legacy_compiler_semantics: bool,
    ) -> Result<Self, ShardError> {
        // Coordinator-gated storage: the reopened shard always accepts class-D, so a clog-tail or
        // peer-recovery replay of an already-accepted class-D write is stored, not re-rejected by a
        // shard built under a since-flipped knob (codex review). See `new` for the full rationale.
        config.accept_class_d = true;
        let dir = config.data_dir.clone();
        let retention_lease_ttl = resolve_lease_ttl(&config);
        let translog = match &dir {
            Some(d) => translog::open_fresh(d, config.wal_sync_on_write)?,
            None => translog::null(),
        };
        let engine = if allow_legacy_compiler_semantics {
            Engine::open_shared_segments_for_compiler_migration_with_source_file(
                Arc::clone(&norm),
                Arc::clone(&dict),
                tag_dict,
                config,
                files,
                next_seg_id,
                source_file_name,
            )
        } else {
            Engine::open_shared_segments_with_source_file(
                Arc::clone(&norm),
                Arc::clone(&dict),
                tag_dict,
                config,
                files,
                next_seg_id,
                source_file_name,
            )
        }
        .map_err(|e| ShardError::Log(format!("attaching shard segments: {e}")))?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        Ok(LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog,
            retention: Mutex::new(RetentionLeases::default()),
            retention_lease_ttl,
            event_sink: Mutex::new(None),
            norm,
            dict,
            data_dir: dir,
            pits: Mutex::new(crate::util::fast_map()),
        })
    }

    /// True when this local shard contains a live materialization from an older
    /// compiler semantics that must be rebuilt and re-placed before serving.
    pub(crate) fn needs_compiler_semantics_migration(&self) -> bool {
        self.lock().needs_compiler_semantics_migration()
    }

    /// Self-restart a durable shard from its checkpoint sidecar (ADR-039 §6): attach the committed
    /// segments (ops ≤ `local_checkpoint`), open the EXISTING translog (the on-disk tail is the
    /// authority — not reset), and replay the un-sealed tail (ops > `local_checkpoint`) into the
    /// engine. Fail-loud if the sidecar's dict fingerprint diverges (never attach segments built
    /// for a different feature space). Replay is engine-only (the ops are already in the translog).
    fn open_durable_self(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        ckpt: &translog::ShardCheckpoint,
    ) -> Result<Self, ShardError> {
        let dir = config
            .data_dir
            .clone()
            .ok_or_else(|| ShardError::Log("durable self-restart requires a data_dir".into()))?;
        if ckpt.dict_fingerprint != dict.fingerprint() {
            return Err(ShardError::DictMismatch {
                expected: dict.fingerprint(),
                actual: ckpt.dict_fingerprint,
            });
        }
        if ckpt.compiler_semantics_version < crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION {
            return Err(ShardError::Log(format!(
                "shard checkpoint uses legacy compiler semantics {} (current {}); a shard-local \
                 restart cannot safely replay its translog tail without coordinator-wide rebuild \
                 and re-placement",
                ckpt.compiler_semantics_version,
                crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION
            )));
        }
        let floor = LogPos(ckpt.local_checkpoint);
        let retention_lease_ttl = resolve_lease_ttl(&config);
        let translog = translog::open_existing(&dir, config.wal_sync_on_write, floor)?;
        // A shard-local restart cannot safely rewrite an older compiler plan:
        // restoring a lost clause/member boundary can change both placement
        // and visibility mode. The strict attach therefore refuses every
        // pre-current stamp and requires coordinator-wide rebuild/re-placement
        // (or recovery from a current peer) before this shard can serve.
        let engine = Engine::open_shared_segments_with_source_file(
            Arc::clone(&norm),
            Arc::clone(&dict),
            tag_dict,
            config,
            &ckpt.segment_files,
            ckpt.next_seg_id,
            &ckpt.source_file_name,
        )
        .map_err(|e| ShardError::Log(format!("attaching shard segments on self-restart: {e}")))?;
        let snapshot = ArcSwap::new(Arc::new(engine.snapshot()));
        let shard = LocalShard {
            engine: Mutex::new(engine),
            snapshot,
            translog,
            retention: Mutex::new(RetentionLeases::default()),
            retention_lease_ttl,
            event_sink: Mutex::new(None),
            norm,
            dict,
            data_dir: Some(dir),
            pits: Mutex::new(crate::util::fast_map()),
        };
        // Replay the un-sealed tail (ops > P) into the engine ONLY — the ops are already on disk
        // in the translog, so re-appending would duplicate them. Position-filtered, so it never
        // double-applies an op already baked into the attached segments.
        let tail = shard.translog.replay(floor)?.entries;
        for (_pos, m) in &tail {
            shard.apply_to_engine(m)?;
        }
        Ok(shard)
    }
}
