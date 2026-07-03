//! `impl Engine` — construction: the public builders (`new`/`with_config`/
//! `with_shared`/`with_vocab`) and the cluster-shard segments-only builder
//! ([`with_shared_segments_only`](Engine::with_shared_segments_only)), plus the
//! private data-directory initializers they share. Reopen lives in
//! [`recovery`](super::recovery).

use crate::segment::{Engine, Segment};
use std::sync::Arc;

use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::storage::SourceStore;
use crate::tagdict::TagDict;
use crate::wal::Wal;

impl Engine {
    /// Create an engine with default configuration.
    pub fn new(norm: Normalizer) -> Self {
        Self::with_config(norm, EngineConfig::default())
    }

    /// Create an engine with explicit configuration. If `config.data_dir` is set,
    /// initializes the data directory and WAL.
    pub fn with_config(norm: Normalizer, config: EngineConfig) -> Self {
        Self::with_shared(
            Arc::new(norm),
            Arc::new(Dict::new()),
            Arc::new(TagDict::new()),
            config,
        )
    }

    /// Create an engine that SHARES a pre-built normalizer and dictionary (by
    /// `Arc`) instead of owning fresh ones. This is how a cluster shard is built:
    /// every shard shares the coordinator's one authoritative, already-finalized
    /// `Dict` so `FeatureId`s / `sig_key`s / hotness are globally consistent (see
    /// [`crate::cluster`]). The dict must be treated as frozen — shard ingest uses
    /// the read-only `*_extracted` paths so it is never `Arc::make_mut`'d (which
    /// would fork it and break cross-shard agreement).
    pub fn with_shared(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
    ) -> Self {
        let mut wal_healthy = true;
        // Diagnostics raised here predate any observer (it is attached after
        // construction via `set_observer`), so they are buffered and replayed on
        // attach rather than dropped — see `pending_events` / `set_observer`.
        let mut pending_events = Vec::new();
        let wal = if let Some(ref dir) = config.data_dir {
            match Self::init_data_dir(dir, config.wal_sync_on_write) {
                Ok(wal) => Some(wal),
                Err(e) => {
                    // A configured data_dir means durability was requested. If we
                    // cannot create it or open the WAL, do NOT silently run without
                    // durability: mark the engine unhealthy (surfaced via /_health)
                    // and emit a DurabilityFailure so ops can alert.
                    wal_healthy = false;
                    pending_events.push(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::WalInit,
                        detail: format!(
                            "failed to initialize data dir / WAL at {} — running WITHOUT \
                             durability (writes will not survive restart)",
                            dir.display()
                        ),
                        error: e.to_string(),
                    });
                    None
                }
            }
        } else {
            None
        };
        let query_store = Arc::new(SourceStore::empty(config.retain_source));
        Engine {
            config: Arc::new(config),
            norm,
            vocab: None,
            dict,
            tag_dict,
            segments: Vec::new(),
            memtable: Arc::new(Segment::new()),
            rejected_parse: 0,
            rejected_class_d: 0,
            would_be_hot: 0,
            bodies_total: 0,
            dup_joined: 0,
            dup_sketch: None,
            observer: None,
            pending_events,
            wal,
            next_seg_id: 1,
            wal_healthy,
            persistence_healthy: wal_healthy,
            skipped_segments: 0,
            query_store,
            vocab_epoch: 0,
            owns_manifest: true,
        }
    }

    /// Create the data directory (and its `segments` subdirectory) and open the
    /// WAL. Returns an error if any filesystem operation fails so callers can
    /// surface loss of durability instead of silently running without a WAL.
    fn init_data_dir(dir: &std::path::Path, wal_sync_on_write: bool) -> std::io::Result<Wal> {
        std::fs::create_dir_all(dir)?;
        std::fs::create_dir_all(dir.join("segments"))?;
        Wal::open(&dir.join("wal.log"), wal_sync_on_write)
    }

    /// Create the data directory and its `segments` subdirectory WITHOUT opening a WAL —
    /// the segments-only-durable path ([`with_shared_segments_only`](Self::with_shared_segments_only)).
    pub(in crate::segment) fn init_segments_dir(dir: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir.join("segments"))
    }

    /// Create a **cluster-shard** engine (ADR-032): shares the coordinator's frozen
    /// normalizer + dict (like [`with_shared`](Self::with_shared)) and persists sealed
    /// segments under `config.data_dir`, but runs WITHOUT a WAL and WITHOUT writing its
    /// own `manifest.bin`. The coordinator's `ClusterLog` is the durable tail and its
    /// `cluster_manifest.bin` is the sole segment registry + dict store, so a per-shard
    /// WAL would double-log the tail and a per-shard manifest would duplicate the shared
    /// dict. `config.data_dir` MUST be set; reopen via
    /// [`open_shared_segments`](Self::open_shared_segments).
    pub fn with_shared_segments_only(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: EngineConfig,
    ) -> std::io::Result<Self> {
        let dir = config.data_dir.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "with_shared_segments_only requires config.data_dir",
            )
        })?;
        Self::init_segments_dir(dir)?;
        let query_store = Arc::new(SourceStore::empty(config.retain_source));
        Ok(Engine {
            config: Arc::new(config),
            norm,
            vocab: None,
            dict,
            tag_dict,
            segments: Vec::new(),
            memtable: Arc::new(Segment::new()),
            rejected_parse: 0,
            rejected_class_d: 0,
            would_be_hot: 0,
            bodies_total: 0,
            dup_joined: 0,
            dup_sketch: None,
            observer: None,
            pending_events: Vec::new(),
            wal: None,
            next_seg_id: 1,
            wal_healthy: true,
            persistence_healthy: true,
            skipped_segments: 0,
            query_store,
            vocab_epoch: 0,
            owns_manifest: false,
        })
    }

    /// Create an engine from a [`Vocab`](crate::vocab::Vocab), which is
    /// converted to a Normalizer internally. The vocab is stored so it can
    /// be queried or serialized later.
    pub fn with_vocab(
        mut vocab: crate::vocab::Vocab,
        config: EngineConfig,
    ) -> Result<Self, crate::error::NormalizerError> {
        let mut norm = vocab.to_normalizer()?;
        // Self-heal stale-active aliases (codex R13): a persisted vocab can carry an Active
        // entry whose form the CURRENT classification can no longer express (e.g. a fused
        // grader after a punctuation refold) — demote it to a candidate rather than install an
        // alias that reports active and never matches. Expressibility is dict-independent
        // (a feature COUNT), so the fresh empty dict is fine here.
        if vocab
            .aliases_mut()
            .demote_unexpressible(&norm, &crate::dict::Dict::new())
            > 0
        {
            norm = vocab.to_normalizer()?;
        }
        let mut eng = Self::with_config(norm, config);
        // Install the vocab's equivalence groups so they apply to inserts from the start
        // (ADR-054), interning the active forms first for ID stability (ADR-060) — exactly
        // what `set_vocab` does at runtime. No groups ⇒ a no-op ⇒ byte-identical to before.
        let norm_arc = Arc::clone(&eng.norm);
        let dict = Arc::make_mut(&mut eng.dict);
        vocab.intern_equivalence_forms(&norm_arc, dict);
        let equiv = vocab.resolve_equivalences(&norm_arc, dict);
        dict.set_equivalences(equiv);
        eng.vocab = Some(Arc::new(vocab));
        Ok(eng)
    }
}
