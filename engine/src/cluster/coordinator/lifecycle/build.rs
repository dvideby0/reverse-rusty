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
        // (ADR-061 multi-word-alias refusal is enforced centrally in `from_parts`, the one assembly
        // seam this path routes through — see the guard there.)
        let norm = Arc::new(norm);

        // Pass A — build the authoritative dict + tag space over the WHOLE corpus, then freeze both.
        // Each accepted query carries its raw tags forward so pass B can place them; the tags are
        // interned now so a corpus tag keeps a dense id (a post-build tag hashes to a synthetic id).
        let mut dict = Dict::new();
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
                }),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push(PlacedQuery {
                            logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version: 1,
                            tags: qtags.clone(),
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
                Self::commit_durable_base(dir, &dict, &tag_dict, &ring, config, &primaries)?
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
        let engine = Self::from_parts(
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
        // Latch tags_present for the set_vocab guard (ADR-055). For a build with interned corpus
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
            // A freshly built cluster has no runtime vocabulary change yet; a
            // declared alias lands here on a later `set_vocab` → `checkpoint`.
            vocab_data: Vec::new(),
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
