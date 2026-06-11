//! `impl ClusterEngine` — construction from an initial corpus: `build` /
//! `build_with_tags` (the primary constructor: build + freeze the ONE dict + tag
//! space, create `K` shards, place every query) and `commit_durable_base` (write
//! the initial coordinator manifest + open an empty log for a durable cluster).

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::cluster::clog::{FileClusterLog, LogPos};
use crate::cluster::control::InMemoryControlPlane;
use crate::cluster::coordinator::{
    into_shard, placement_of, replica_dir, shard_dir, ClusterConfig, ClusterDurable, ClusterEngine,
    Target, CLUSTER_LOG_FILE, CLUSTER_MANIFEST_FILE,
};
use crate::cluster::ring::HashRing;
use crate::cluster::shard::{LocalShard, Shard, ShardError};
use crate::compile::{extract, Extracted};
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::segment::PlacedQuery;
use crate::tagdict::TagDict;

/// One accepted query carried through pass A → pass B of [`ClusterEngine::build_with_tags`]:
/// `(logical, extracted features, dsl, raw tags)` (ADR-055).
type ExtractedTagged = (u64, Extracted, String, Vec<(String, String)>);

impl ClusterEngine {
    /// Pass A of [`build`](Self::build) as a standalone step: build + freeze the ONE
    /// authoritative feature dict and tag space over a corpus, WITHOUT constructing
    /// shards. This is what a remote-cluster assembler needs before
    /// [`connect_remote`](Self::connect_remote) /
    /// [`connect_replicated`](Self::connect_replicated) — the coordinator-mode server
    /// (ADR-070) mints the same frozen space `build` would and ships it at connect
    /// (ADR-034/055). An empty corpus yields an empty (still finalized) space: every
    /// later term resolves to a deterministic synthetic id (ADR-046), so a fresh
    /// remote cluster needs no out-of-band dict at all.
    pub fn freeze_feature_space(
        norm: &Normalizer,
        queries: &[(u64, String)],
        tags: &[Vec<(String, String)>],
    ) -> (Dict, TagDict) {
        let mut dict = Dict::new();
        let mut tag_dict = TagDict::new();
        let mut lc = String::new();
        for (idx, (_logical, text)) in queries.iter().enumerate() {
            if let Ok(ast) = crate::dsl::parse(text) {
                let _ = extract(&ast, norm, &mut dict, &mut lc);
                for (k, v) in tags.get(idx).map_or(&[][..], Vec::as_slice) {
                    tag_dict.intern(k, v);
                }
            }
        }
        dict.finalize_mask();
        tag_dict.mark_finalized();
        (dict, tag_dict)
    }

    /// Build a cluster from an initial corpus. This is the primary constructor:
    /// it builds the ONE authoritative dict over the whole corpus (pass A), freezes
    /// it, creates `K` shards sharing it, then distributes each query to its
    /// placement shard(s) (pass B). One immutable base segment per shard.
    ///
    /// After this the dict is frozen: [`Self::add_query`] can only use vocabulary
    /// already present (it compiles read-only against the shared dict), which is
    /// the in-process limitation noted in the design (new-vocabulary adds need the
    /// deferred feature-model-epoch machinery).
    pub fn build(
        norm: Normalizer,
        config: &ClusterConfig,
        queries: &[(u64, String)],
    ) -> Result<Self, ShardError> {
        Self::build_with_tags(norm, config, queries, &[])
    }

    /// [`build`](Self::build) carrying per-query metadata tags (ADR-049/055). `tags` is parallel to
    /// `queries` (`tags[i]` describes `queries[i]`); an empty slice means no query is tagged. Pass A
    /// builds the one frozen `TagDict` over the corpus tags alongside the feature `Dict`, then shares
    /// it read-only into every shard, so a tagged write and a percolate filter resolve a given
    /// `(key,value)` to the same `TagId` everywhere. With no tags the tag space is empty (still
    /// finalized) and every path is byte-identical to the pre-tag `build`.
    pub fn build_with_tags(
        norm: Normalizer,
        config: &ClusterConfig,
        queries: &[(u64, String)],
        tags: &[Vec<(String, String)>],
    ) -> Result<Self, ShardError> {
        Self::build_inner(norm, None, config, queries, tags)
    }

    /// [`build`](Self::build) from a [`Vocab`](crate::vocab::Vocab) — the constructor that
    /// fully ACTIVATES the vocabulary (ADR-076), mirroring the single-node
    /// `Engine::with_vocab`: the normalizer is derived (with the unexpressible-alias
    /// self-heal), the vocab's equivalence/alias forms are interned into the fresh dict
    /// FIRST (pinning dense ids), and the equivalence map is installed BEFORE the corpus
    /// extracts — so every query expands through declared equivalences and registry
    /// aliases (single- AND multi-word; routing is P(T)-aware) from the first percolate.
    /// The vocab is installed on the engine (served by `GET /_vocab`) and persisted in
    /// the durable manifest at build time, so a crash before any later checkpoint still
    /// reopens with the vocabulary in effect.
    ///
    /// Building from a bare `Normalizer` instead leaves equivalence-driven features
    /// (declared equivalences, registry aliases) INERT — identical to a single-node
    /// engine built from a bare normalizer. Operators with a vocab file want THIS
    /// constructor (the coordinator-mode server uses it).
    pub fn build_with_vocab(
        vocab: crate::vocab::Vocab,
        config: &ClusterConfig,
        queries: &[(u64, String)],
    ) -> Result<Self, ShardError> {
        let mut vocab = vocab;
        let mut norm = vocab
            .to_normalizer()
            .map_err(|e| ShardError::Config(format!("building normalizer from vocab: {e}")))?;
        // Self-heal stale-active aliases (the codex-R13 install seam): an entry whose form
        // the classification can no longer express is demoted rather than installed inert.
        if vocab
            .aliases_mut()
            .demote_unexpressible(&norm, &Dict::new())
            > 0
        {
            norm = vocab
                .to_normalizer()
                .map_err(|e| ShardError::Config(format!("building normalizer from vocab: {e}")))?;
        }
        Self::build_inner(norm, Some(vocab), config, queries, &[])
    }

    fn build_inner(
        norm: Normalizer,
        vocab: Option<crate::vocab::Vocab>,
        config: &ClusterConfig,
        queries: &[(u64, String)],
        tags: &[Vec<(String, String)>],
    ) -> Result<Self, ShardError> {
        if config.num_shards == 0 {
            return Err(ShardError::Config(
                "cluster needs at least one shard".into(),
            ));
        }
        if config.replication_factor == 0 {
            return Err(ShardError::Config(
                "replication_factor must be ≥ 1 (1 = primary only)".into(),
            ));
        }
        // A multi-word-alias normalizer is supported on a cluster since ADR-076:
        // content routing is P(T)-aware (`route` derives targets from the maximal
        // positive view when multi-word aliases are active), so a nested alias
        // entity's shard is always probed — the ADR-061 single-node-only refusal
        // that used to live here is retired.
        let norm = Arc::new(norm);

        // Pass A — build the authoritative dict + tag space over the WHOLE corpus, then freeze both.
        // Each accepted query carries its raw tags forward so pass B can place them; the tags are
        // interned now so a corpus tag keeps a dense id (a post-build tag hashes to a synthetic id).
        let mut dict = Dict::new();
        // Install the vocabulary's equivalence machinery BEFORE extraction (ADR-076,
        // the `Engine::with_vocab` fresh-path order): intern every active
        // equivalence/alias form so it pins a dense id, then install the resolved map —
        // the corpus loop's `extract` then expands each query through it (an alias can
        // widen an anchor into an any-of, which placement fans accordingly). `None` ⇒
        // both are no-ops ⇒ byte-identical to the bare-normalizer build.
        if let Some(v) = &vocab {
            v.intern_equivalence_forms(&norm, &mut dict);
            let equiv = v.resolve_equivalences(&norm, &dict);
            dict.set_equivalences(equiv);
        }
        let mut tag_dict = TagDict::new();
        let mut lc = String::new();
        let mut extracted: Vec<ExtractedTagged> = Vec::with_capacity(queries.len());
        for (idx, (logical, text)) in queries.iter().enumerate() {
            if let Ok(ast) = crate::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                let qtags = tags.get(idx).cloned().unwrap_or_default();
                for (k, v) in &qtags {
                    tag_dict.intern(k, v);
                }
                extracted.push((*logical, ex, text.clone(), qtags));
            }
        }
        dict.finalize_mask();
        tag_dict.mark_finalized();
        let dict = Arc::new(dict);
        let tag_dict = Arc::new(tag_dict);

        let ring = HashRing::new(config.num_shards, config.vnodes)?;

        // Construct concrete local shards: `replication_factor` copies per position (copy 0 =
        // primary, copies 1.. = replicas). A durable cluster roots the primary at `shard_<i>/`
        // (the manifest-recorded copy) and each replica at `shard_<i>/replica_<r>/` (rebuilt
        // from the primary on `open`); an in-memory cluster uses plain in-RAM copies. `build`
        // only makes `LocalShard`s (remote shards arrive via `from_parts`), so pass-B ingest can
        // use the infallible inherent `ingest_local` path on every copy.
        let rf = config.replication_factor;
        let mut groups: Vec<Vec<LocalShard>> = Vec::with_capacity(config.num_shards);
        for s in 0..config.num_shards {
            let mut copies = Vec::with_capacity(rf);
            for r in 0..rf {
                let shard = if let Some(dir) = &config.data_dir {
                    let mut sc = config.per_shard.clone();
                    sc.data_dir = Some(if r == 0 {
                        shard_dir(dir, s)
                    } else {
                        replica_dir(dir, s, r)
                    });
                    LocalShard::new_durable(
                        Arc::clone(&norm),
                        Arc::clone(&dict),
                        Arc::clone(&tag_dict),
                        sc,
                    )?
                } else {
                    LocalShard::new(
                        Arc::clone(&norm),
                        Arc::clone(&dict),
                        Arc::clone(&tag_dict),
                        config.per_shard.clone(),
                    )
                };
                copies.push(shard);
            }
            groups.push(copies);
        }

        // Pass B — bucket by placement, then ingest one base segment per shard. For a
        // durable cluster each shard's `ingest_local` persists a compiled `.seg`; the
        // initial corpus becomes the committed base (the Aurora "segments are the
        // materialized view" base), recorded in the coordinator manifest below rather
        // than as a raw-DSL snapshot.
        let mut buckets: Vec<Vec<PlacedQuery>> =
            (0..config.num_shards).map(|_| Vec::new()).collect();
        for (logical, ex, text, qtags) in extracted {
            match placement_of(&dict, &ring, &ex) {
                Target::Reject => {}
                Target::Replicated => buckets[0].push(PlacedQuery {
                    logical,
                    ex,
                    dsl: text,
                    version: 1,
                    tags: qtags,
                    tag_ids: Vec::new(),
                }),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push(PlacedQuery {
                            logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version: 1,
                            tags: qtags.clone(),
                            tag_ids: Vec::new(),
                        });
                    }
                }
            }
        }
        // Ingest the same bucket into EVERY copy of the owning position (identical op stream
        // ⇒ all copies set-equal by construction).
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                for copy in &groups[s] {
                    copy.ingest_local(&bucket);
                }
            }
        }

        // Durability: commit the coordinator manifest (the atomic base = per-shard
        // segment registry + dict + ring + epoch 0) and open an empty log, or fall back
        // to an in-memory log. Construction fails loud on a durable-setup error (fresh
        // construction — nothing to lose yet); a shard whose segment write fell back to
        // in-memory makes `segment_filenames` error, aborting the build rather than
        // committing a registry that would lose it.
        // Durability: commit the manifest from the PRIMARIES (copy 0 of each position); this
        // borrow of `groups` ends before the positions are consumed into shards below.
        let durable = match &config.data_dir {
            Some(dir) => {
                let primaries: Vec<&LocalShard> = groups.iter().map(|g| &g[0]).collect();
                Self::commit_durable_base(
                    dir,
                    &dict,
                    &tag_dict,
                    &ring,
                    config,
                    &primaries,
                    vocab.as_ref(),
                )?
            }
            None => ClusterDurable::in_memory(
                config.num_shards as u32,
                config.vnodes,
                dict.fingerprint(),
            ),
        };

        // Assemble each position into a shard: a bare `LocalShard` at RF=1, else a
        // `ReplicatedShard` composite over the primary + replicas.
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(config.num_shards);
        for copies in groups {
            shards.push(into_shard(copies)?);
        }
        let mut engine = Self::from_parts(
            norm,
            dict,
            tag_dict,
            ring,
            shards,
            config.include_broad,
            config.replication_factor,
            config.per_shard.clone(),
            durable,
        )?;
        // Install the vocabulary on the engine (ADR-076): served by `GET /_vocab`,
        // merged-under by the learn paths, and re-persisted at every checkpoint.
        // (The durable manifest above already carries it.)
        if let Some(v) = vocab {
            engine.vocab = Some(Arc::new(v));
        }
        // Latch tags_present (ADR-055, `/_stats` introspection). For a build with interned corpus
        // tags `tag_dict` is also non-empty, but this also covers a build whose only tags would be
        // synthetic, keeping `has_tags` correct regardless.
        if tags.iter().any(|t| !t.is_empty()) {
            engine.tags_present.store(true, Ordering::Relaxed);
        }
        Ok(engine)
    }

    /// Commit the initial durable base for a freshly built cluster: collect each shard's
    /// segment registry + next-seg-id, write the coordinator manifest (epoch 0,
    /// snapshot_pos 0 — the atomic commit point), and open an empty log. The per-shard
    /// `.seg` files were already written by pass-B ingest; this records which ones are
    /// committed. Returns the durability bundle for [`from_parts`].
    fn commit_durable_base(
        dir: &Path,
        dict: &Dict,
        tag_dict: &TagDict,
        ring: &HashRing,
        config: &ClusterConfig,
        primaries: &[&LocalShard],
        vocab: Option<&crate::vocab::Vocab>,
    ) -> Result<ClusterDurable, ShardError> {
        std::fs::create_dir_all(dir)
            .map_err(|e| ShardError::Log(format!("creating cluster data dir: {e}")))?;
        // Only the PRIMARY of each position is committed to the manifest; replicas are not
        // catalogued (rebuilt from the primary via peer recovery on reopen — ADR-035).
        let mut segment_registry = Vec::with_capacity(primaries.len());
        let mut next_seg_ids = Vec::with_capacity(primaries.len());
        for p in primaries {
            segment_registry.push(p.segment_filenames()?);
            next_seg_ids.push(p.next_seg_id()?);
        }
        let manifest = crate::storage::ClusterManifest {
            epoch: 0,
            snapshot_pos: 0,
            dict_fingerprint: dict.fingerprint(),
            num_shards: ring.num_shards() as u32,
            vnodes: config.vnodes,
            include_broad: config.include_broad,
            segment_registry,
            next_seg_ids,
            dict_data: crate::storage::serialize_dict(dict),
            // A `build_with_vocab` cluster persists its vocabulary from the FIRST
            // commit (ADR-076) — a crash before any later checkpoint must still
            // reopen with the vocab in effect; serialization failure fails the
            // build loudly. A bare-normalizer build has none (a later `set_vocab`
            // → `checkpoint` lands it).
            vocab_data: match vocab {
                Some(v) => v
                    .to_json()
                    .map_err(|e| {
                        ShardError::Log(format!("serializing cluster vocab at build: {e}"))
                    })?
                    .into_bytes(),
                None => Vec::new(),
            },
            // The frozen per-query tag space (ADR-049/055) — persisted so a reopen resolves a
            // request filter to the SAME `TagId`s the stored segments carry. Empty + finalized for
            // an untagged cluster ⇒ a byte-identical empty blob (manifest v4 round-trips it).
            tag_dict_data: crate::storage::serialize_tagdict(tag_dict),
        };
        crate::storage::write_cluster_manifest(&manifest, &dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|e| ShardError::Log(format!("writing cluster manifest: {e}")))?;
        let log = FileClusterLog::open(
            &dir.join(CLUSTER_LOG_FILE),
            config.wal_sync_on_write,
            LogPos(0),
        )
        .map_err(|e| ShardError::Log(format!("opening cluster log: {e}")))?;
        Ok(ClusterDurable {
            log: Box::new(log),
            data_dir: Some(dir.to_path_buf()),
            epoch: 0,
            vnodes: config.vnodes,
            control: Box::new(InMemoryControlPlane::single_node(
                ring.num_shards() as u32,
                config.vnodes,
                dict.fingerprint(),
            )),
        })
    }
}
