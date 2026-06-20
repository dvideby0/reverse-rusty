//! `impl ClusterEngine` — the construction seam + crash recovery: `from_parts`
//! (the shared assembly point that materializes a `ClusterEngine` from pre-built
//! parts, used by both `build` and the distributed/gRPC builders) and `open`
//! (reattach a durable cluster's committed segments + replay the log tail).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::cluster::clog::{FileClusterLog, LogPos};
use crate::cluster::control::InMemoryControlPlane;
use crate::cluster::coordinator::{
    into_shard, replica_dir, shard_dir, ClusterConfig, ClusterDurable, ClusterEngine,
    CLUSTER_LOG_FILE, CLUSTER_MANIFEST_FILE,
};
use crate::cluster::ring::HashRing;
use crate::cluster::shard::{LocalShard, Shard, ShardError};
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::tagdict::TagDict;

impl ClusterEngine {
    /// Assemble a cluster from pre-built parts — the construction seam shared by
    /// [`Self::build`] (which supplies `LocalShard`s) and the distributed builder /
    /// gRPC integration test (which supply boxed `RemoteShard`s). `shards.len()` must
    /// equal `ring.num_shards()`.
    // The internal assembly seam genuinely takes many independent parts (feature
    // space, ring, shards, the broad toggle, the rebuild config, and the durability
    // bundle); grouping them further would only obscure the construction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        ring: HashRing,
        shards: Vec<Box<dyn Shard>>,
        include_broad: bool,
        replication_factor: usize,
        per_shard: crate::config::EngineConfig,
        durable: ClusterDurable,
    ) -> Result<Self, ShardError> {
        if shards.len() != ring.num_shards() {
            return Err(ShardError::Config(format!(
                "shard count {} must match the ring's shard count {}",
                shards.len(),
                ring.num_shards()
            )));
        }
        // Multi-word aliases are cluster-supported since ADR-076: `route` is P(T)-aware
        // (targets derived from the maximal positive view when multi-word aliases are
        // active), so a nested alias entity that lives only in `P(T)` still probes the
        // shard holding a query anchored on it — the ADR-061 single-node-only refusal
        // that guarded this assembly seam is retired (the shard-local two-view verifier
        // was already correct once the probe arrives). Cross-process callers
        // (`connect_remote`/`connect_replicated`) still arrange ONE normalizer
        // out-of-band — the same consistency every vocabulary feature already relies
        // on; ADR-076 records that trust model and keeps LIVE vocab changes on a
        // remote cluster refused (`set_vocab` non-local guard).
        Ok(ClusterEngine {
            norm,
            dict,
            tag_dict,
            // Untagged by default; the tagged write paths + `open` latch it (ADR-055).
            tags_present: AtomicBool::new(false),
            vocab: None,
            ring,
            shards,
            include_broad,
            replication_factor: replication_factor.max(1),
            per_shard,
            log: durable.log,
            epoch: AtomicU64::new(durable.epoch),
            vnodes: durable.vnodes,
            data_dir: durable.data_dir,
            control: durable.control,
            observer: Mutex::new(None),
            pending_events: Mutex::new(Vec::new()),
            pending_repair: Mutex::new(std::collections::BTreeMap::new()),
            // No position is handoff-wrapped by default; the gRPC builders install handles via
            // `with_handoffs`. Empty here ⇒ the in-process/default path is byte-identical (ADR-043).
            #[cfg(feature = "distributed")]
            handoffs: Vec::new(),
            // Handoff drain caps default here (the in-process path never hands off); the gRPC
            // builders override them from `ClusterConfig` via `with_handoff_caps` (ADR-044/048).
            #[cfg(feature = "distributed")]
            handoff_drain_passes: ClusterConfig::DEFAULT_HANDOFF_DRAIN_PASSES,
            #[cfg(feature = "distributed")]
            handoff_final_drain_cap: ClusterConfig::DEFAULT_HANDOFF_FINAL_DRAIN_CAP,
            // No runtime handle on the in-process path (it never hands off); the gRPC builders set
            // it via `with_handle` so the autoscaler can drive `execute_handoff` (ADR-048).
            #[cfg(feature = "distributed")]
            handle: None,
            // No mesh security on the in-process path; the secure gRPC builders set it via
            // `with_client_security` (ADR-071).
            #[cfg(feature = "distributed")]
            client_security: crate::cluster::security::ClientSecurity::default(),
        })
    }
    /// True if `data_dir` holds a committed cluster manifest — i.e. [`Self::open`]
    /// will reopen an existing durable cluster there; otherwise [`Self::build`] is the
    /// constructor. The boot-time predicate the coordinator-mode server branches on
    /// (ADR-070), exposed so callers need not string-match `open`'s error.
    pub fn cluster_exists(data_dir: &Path) -> bool {
        data_dir.join(CLUSTER_MANIFEST_FILE).exists()
    }

    /// Reopen a durable cluster from `data_dir` (built earlier with a `data_dir` set).
    /// Each shard **attaches-and-mmaps** its committed compiled segments (the
    /// `cluster_manifest.bin` registry) — NOT re-ingest — then the log tail strictly
    /// after the manifest's `snapshot_pos` is replayed through the same apply funnel as
    /// live writes (ADR-032). The frozen dict is restored from the manifest
    /// (fingerprint-checked — a mismatch is a loud [`ShardError::DictMismatch`], ADR-030
    /// parity) and the ring re-derived deterministically, so placement is byte-identical
    /// to the original → zero false negatives across the restart. `config` supplies the
    /// per-shard engine config + fsync policy (defaults if `None`).
    pub fn open(
        data_dir: impl Into<PathBuf>,
        norm: Normalizer,
        config: Option<&ClusterConfig>,
    ) -> Result<Self, ShardError> {
        let data_dir = data_dir.into();
        let manifest_path = data_dir.join(CLUSTER_MANIFEST_FILE);
        if !manifest_path.exists() {
            return Err(ShardError::Config(format!(
                "no cluster manifest at {}; use build() to create a durable cluster",
                manifest_path.display()
            )));
        }
        let manifest = crate::storage::read_cluster_manifest(&manifest_path)
            .map_err(|e| ShardError::Config(format!("reading cluster manifest: {e}")))?;
        // ADR-080 forward fence: a pre-ADR-080 durable cluster placed the broad lane (class C +
        // B-arity-2) on shard 0 ONLY. This binary evaluates broad on a rotating per-title
        // broad-eval shard, which would silently miss those queries whenever the chosen shard is
        // not 0 — a false negative. Refuse loudly rather than mis-route; the operator rebuilds the
        // cluster with this binary (which writes the v5 replicate-to-all layout).
        if !manifest.broad_replicate_all {
            return Err(ShardError::Config(format!(
                "cluster at {} predates ADR-080's replicate-to-all broad layout (its broad lane \
                 lives on shard 0 only); reopening it here would mis-route broad queries — rebuild \
                 the cluster with this binary",
                data_dir.display()
            )));
        }
        let dict = crate::storage::deserialize_dict(&manifest.dict_data)
            .map_err(|e| ShardError::Config(format!("deserializing cluster dict: {e}")))?;
        let mut dict = Arc::new(dict);
        // Fail loud if the restored dict's fingerprint disagrees with the manifest's —
        // the one false-negative path the fallible seam can't otherwise catch.
        let actual_fp = dict.fingerprint();
        if actual_fp != manifest.dict_fingerprint {
            return Err(ShardError::DictMismatch {
                expected: manifest.dict_fingerprint,
                actual: actual_fp,
            });
        }
        let num_shards = manifest.num_shards as usize;
        // Defensive: the registry + next-seg-id columns must agree with num_shards. A
        // malformed manifest must fail loud, never silently attach the wrong segments.
        if manifest.segment_registry.len() != num_shards
            || manifest.next_seg_ids.len() != num_shards
        {
            return Err(ShardError::Config(format!(
                "cluster manifest is inconsistent: num_shards={num_shards} but registry has \
                 {} shard list(s) and {} next-seg-id(s)",
                manifest.segment_registry.len(),
                manifest.next_seg_ids.len()
            )));
        }
        // ADR-046: if a runtime vocabulary change was committed, the manifest carries
        // the serialized vocab — rebuild the normalizer from IT (authoritative over the
        // caller-supplied one) so a declared alias survives the restart, and retain the
        // vocab so a later checkpoint re-persists it (else the next reopen would lose it).
        let mut restored_vocab = if manifest.vocab_data.is_empty() {
            None
        } else {
            let json = std::str::from_utf8(&manifest.vocab_data)
                .map_err(|e| ShardError::Config(format!("cluster vocab not utf-8: {e}")))?;
            let v = crate::vocab::Vocab::from_json(json)
                .map_err(|e| ShardError::Config(format!("deserializing cluster vocab: {e}")))?;
            Some(v)
        };
        let norm = match &restored_vocab {
            Some(v) => v.to_normalizer().map_err(|e| {
                ShardError::Config(format!("building normalizer from cluster vocab: {e}"))
            })?,
            None => norm,
        };
        // Self-heal stale-active aliases against the restored normalizer (codex R13, the same
        // demotion every other equivalence-install seam runs): a persisted vocab can carry an
        // Active entry the current classification can no longer express. Demotion can only
        // shrink the registered phrase set, so rebuild the normalizer when it fires (the
        // demoted state re-persists at the next checkpoint).
        let mut norm = norm;
        if let Some(v) = &mut restored_vocab {
            if v.aliases_mut().demote_unexpressible(&norm, &dict) > 0 {
                norm = v.to_normalizer().map_err(|e| {
                    ShardError::Config(format!("building normalizer from cluster vocab: {e}"))
                })?;
            }
        }
        let restored_vocab = restored_vocab.map(Arc::new);
        let norm = Arc::new(norm);
        // Re-install equivalence groups (ADR-054) on the recovered dict so a log-tail replay
        // and post-reopen incremental adds expand through them. The already-attached segments
        // carry their expansion baked in, so matching recovered queries needs no re-resolution;
        // this only re-equips the live compile path. No-op when the restored vocab declared none.
        if let Some(v) = &restored_vocab {
            let equiv = v.resolve_equivalences(&norm, &dict);
            if !equiv.is_empty() {
                Arc::make_mut(&mut dict).set_equivalences(equiv);
            }
        }
        // Restore the frozen tag space (ADR-049/055) like the dict, so a reopened cluster resolves a
        // request filter to the SAME `TagId`s its attached segments carry. An empty blob (a pre-v4
        // manifest / untagged cluster) deserializes to an empty tag dict — the back-compat path.
        let tag_dict = Arc::new(
            crate::storage::deserialize_tagdict(&manifest.tag_dict_data)
                .map_err(|e| ShardError::Config(format!("deserializing cluster tag dict: {e}")))?,
        );
        let ring = HashRing::new(num_shards, manifest.vnodes)?;

        let per_shard = config.map(|c| c.per_shard.clone()).unwrap_or_default();
        let fsync = config.is_some_and(|c| c.wal_sync_on_write);

        // Attach each shard's committed compiled segments (mmap) against the shared dict —
        // NOT re-ingest. Fails loud on a missing / CRC-corrupt segment (a skipped segment
        // is a silent shard-sized false negative).
        let rf = config.map_or(1, |c| c.replication_factor.max(1));
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(num_shards);
        for s in 0..num_shards {
            let primary_dir = shard_dir(&data_dir, s);
            let mut sc = per_shard.clone();
            sc.data_dir = Some(primary_dir.clone());
            let primary = LocalShard::open_segments(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                sc,
                &manifest.segment_registry[s],
                manifest.next_seg_ids[s],
            )?;
            // Re-seed replicas (rf-1) by peer recovery from the just-attached primary — replicas
            // are not in the manifest, so they are rebuilt from the durable primary on every open.
            // The log-tail replay below then feeds primary AND replicas through the composite.
            let mut copies: Vec<LocalShard> = Vec::with_capacity(rf);
            let mut recovered: Vec<LocalShard> = Vec::with_capacity(rf.saturating_sub(1));
            for r in 1..rf {
                // The high-water is irrelevant here: at open there are no concurrent writes,
                // so the primary's translog tail is empty and this peer_recover is a pure
                // segment copy; the coordinator-log replay below repopulates all copies.
                let (replica, _hwm) = crate::cluster::replica::peer_recover(
                    &norm,
                    &dict,
                    &tag_dict,
                    per_shard.clone(),
                    &primary,
                    &primary_dir,
                    &replica_dir(&data_dir, s, r),
                )?;
                recovered.push(replica);
            }
            copies.push(primary);
            copies.extend(recovered);
            shards.push(into_shard(copies)?);
        }

        let log = FileClusterLog::open(
            &data_dir.join(CLUSTER_LOG_FILE),
            fsync,
            LogPos(manifest.snapshot_pos),
        )
        .map_err(|e| ShardError::Log(format!("opening cluster log: {e}")))?;

        let durable = ClusterDurable {
            log: Box::new(log),
            data_dir: Some(data_dir.clone()),
            epoch: manifest.epoch,
            vnodes: manifest.vnodes,
            control: Box::new(InMemoryControlPlane::single_node(
                manifest.num_shards,
                manifest.vnodes,
                manifest.dict_fingerprint,
            )),
        };
        let mut engine = Self::from_parts(
            norm,
            dict,
            tag_dict,
            ring,
            shards,
            manifest.include_broad,
            rf,
            per_shard,
            durable,
        )?;
        // Retain the vocab restored from the manifest so a later checkpoint re-persists it.
        engine.vocab = restored_vocab;
        // Latch tags_present (ADR-055) from the restored tag space; the log-tail replay below
        // (`apply_add` → `note_tags`) additionally latches it for any un-checkpointed tagged add.
        if !engine.tag_dict.is_empty() {
            engine.tags_present.store(true, Ordering::Relaxed);
        }

        // The attached segments ARE the base (all entries ≤ snapshot_pos). Replay only the
        // log tail strictly after snapshot_pos, through the SAME apply funnel as live
        // writes — those entries are not in the attached segments, so no double-apply.
        let replay = engine.log.replay(LogPos(manifest.snapshot_pos))?;
        if replay.skipped_bytes > 0 {
            engine.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalTornTail,
                detail: format!(
                    "cluster log torn tail: {} trailing byte(s) skipped during recovery",
                    replay.skipped_bytes
                ),
                error: format!("{} bytes", replay.skipped_bytes),
            });
        }
        for (_pos, m) in replay.entries {
            engine.replay_apply(m)?;
        }
        Ok(engine)
    }
}
