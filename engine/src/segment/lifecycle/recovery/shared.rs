use super::{
    fresh_segment_generations, seed_next_source_generation, Arc, BaseSegment, Dict, Engine,
    EngineConfig, MmapSegment, Normalizer, Segment, SourceCommitState, SourceStore, TagDict,
};

impl Engine {
    /// Reopen a **cluster-shard** engine (ADR-032) by attaching an EXPLICIT list of
    /// committed segment files against the SUPPLIED shared dict — no per-shard manifest,
    /// no dict deserialize, no WAL. The coordinator supplies `files` (relative `.seg`
    /// names under `config.data_dir/segments/`) and `next_seg_id` from its
    /// `cluster_manifest.bin`, having already fingerprint-checked the dict. This is
    /// attach-and-mmap, NOT re-ingest: the compiled segments ARE the materialized base.
    ///
    /// Fails LOUD (returns `Err`) on any missing or CRC-corrupt segment — deliberately
    /// unlike [`open`](Self::open), which skips corrupt segments and degrades. A skipped
    /// shard segment is a silent, shard-sized false negative, which the cluster's
    /// zero-false-negative contract forbids; the caller surfaces the error instead.
    pub fn open_shared_segments(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
    ) -> std::io::Result<Self> {
        Self::open_shared_segments_inner(
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

    /// Coordinator-selected source-sidecar attach seam. The source filename is
    /// validated by the cluster manifest reader before it reaches this method.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_shared_segments_with_source_file(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
        source_file_name: &str,
    ) -> std::io::Result<Self> {
        Self::open_shared_segments_inner(
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

    /// Coordinator-only legacy attach with an explicitly manifest-selected
    /// source sidecar.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_shared_segments_for_compiler_migration_with_source_file(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
        source_file_name: &str,
    ) -> std::io::Result<Self> {
        Self::open_shared_segments_inner(
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
    fn open_shared_segments_inner(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
        files: &[String],
        next_seg_id: u64,
        source_file_name: &str,
        allow_legacy_compiler_semantics: bool,
    ) -> std::io::Result<Self> {
        let dir = config.data_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "open_shared_segments requires config.data_dir",
            )
        })?;
        Self::init_segments_dir(dir)?;
        let seg_dir = dir.join("segments");
        let mut segments = Vec::with_capacity(files.len());
        for name in files {
            // Fail loud: a missing / CRC-corrupt committed segment is a false-negative risk.
            let mmap_seg = MmapSegment::open(&seg_dir.join(name))?;
            segments.push(Arc::new(BaseSegment::Mmap(mmap_seg)));
        }
        let query_store = Arc::new(SourceStore::open(
            &dir.join(source_file_name),
            config.retain_source,
        )?);
        let next_source_generation = seed_next_source_generation(&segments, &query_store)?;
        let live_phrase_segments = segments
            .iter()
            .filter(|segment| segment.has_phrase_predicates())
            .count();
        let segment_generations = fresh_segment_generations(segments.len());
        let committed_segment_generations = segment_generations.clone();
        let engine = Engine {
            config: Arc::new(config),
            norm,
            vocab: None,
            dict,
            // The cluster shard shares the coordinator's frozen tag space (ADR-049/055): the
            // attached segments already carry resolved `TagId`s, and this shared dict resolves any
            // later live-add / translog-replayed tags consistently. Empty ⇒ untagged cluster.
            tag_dict,
            segments,
            segment_generations,
            committed_segment_generations,
            memtable: Arc::new(Segment::new()),
            live_phrase_segments,
            rejected_parse: 0,
            rejected_class_d: 0,
            would_be_hot: 0,
            bodies_total: 0,
            dup_joined: 0,
            dup_sketch: None,
            observer: None,
            pending_events: Vec::new(),
            wal: None,
            next_seg_id,
            next_source_generation,
            wal_healthy: true,
            persistence_healthy: true,
            skipped_segments: 0,
            query_store,
            source_file_name: source_file_name.to_string(),
            source_commit_state: SourceCommitState::Ready,
            vocab_epoch: 0,
            owns_manifest: false,
        };
        if !allow_legacy_compiler_semantics && engine.needs_compiler_semantics_migration() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "legacy compiler semantics require an atomic source-driven rebuild and \
                 re-placement; reopen through ClusterEngine or recover this shard from a \
                 current peer",
            ));
        }
        Ok(engine)
    }
}
