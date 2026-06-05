//! `impl ClusterEngine` — durable checkpointing + introspection: `checkpoint`
//! (seal each shard, commit the coordinator manifest, truncate the captured log
//! prefix), `gc_orphan_segments` (best-effort cleanup of superseded `.seg` files),
//! and the `epoch` accessor.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::cluster::coordinator::{ClusterEngine, CLUSTER_MANIFEST_FILE};
use crate::cluster::shard::ShardError;
use crate::events::{DurabilityOp, EngineEvent};

impl ClusterEngine {
    /// Checkpoint the durable state: seal each shard (flush memtable + re-seal tombstoned
    /// base segments so the on-disk set is a clean materialization of the live state ≤
    /// `up_to`), then commit the coordinator manifest (the atomic commit point: new
    /// per-shard segment registry + log cursor + bumped epoch), then truncate the captured
    /// log prefix and GC orphaned segment files. A no-op on an in-memory cluster.
    ///
    /// Crash-safety: the manifest write is the single commit point (tmp + CRC + rename).
    /// A crash BEFORE it leaves the old (registry, cursor) authoritative — the freshly
    /// written `.seg` are orphans (not in the old registry) recovered via log replay, so
    /// no double-apply and no loss. A crash AFTER it (before truncation) loads the new
    /// segments and replays only the (now shorter) tail — also correct.
    pub fn checkpoint(&self) -> Result<(), ShardError> {
        let Some(dir) = self.data_dir.clone() else {
            return Ok(());
        };
        let up_to = self.log.last_pos()?;
        let new_epoch = self.epoch.load(Ordering::Relaxed) + 1;

        // 1. Seal each shard: memtable → base segment, then bake base-segment tombstones
        //    onto disk. After this every shard's on-disk segments reflect live state ≤ up_to.
        for s in &self.shards {
            s.seal_for_checkpoint()?;
        }

        // 2. Collect the per-shard segment registry + next-seg-ids. An error here (e.g. a
        //    segment write fell back to in-memory) aborts BEFORE the commit, leaving the
        //    old manifest authoritative — nothing is lost.
        let mut segment_registry = Vec::with_capacity(self.shards.len());
        let mut next_seg_ids = Vec::with_capacity(self.shards.len());
        for s in &self.shards {
            segment_registry.push(s.segment_filenames()?);
            next_seg_ids.push(s.next_seg_id()?);
        }

        // 3. Coordinator manifest = the atomic commit point (new base + new cursor).
        //    Persist the installed vocab (ADR-046) so a runtime alias survives reopen;
        //    serialization failure fails the checkpoint loudly rather than silently
        //    dropping the alias (which would be a false negative on the next open).
        let vocab_data = match &self.vocab {
            Some(v) => v
                .to_json()
                .map_err(|e| ShardError::Log(format!("serializing cluster vocab: {e}")))?
                .into_bytes(),
            None => Vec::new(),
        };
        let manifest = crate::storage::ClusterManifest {
            epoch: new_epoch,
            snapshot_pos: up_to.0,
            dict_fingerprint: self.dict.fingerprint(),
            num_shards: self.ring.num_shards() as u32,
            vnodes: self.vnodes,
            include_broad: self.include_broad,
            segment_registry: segment_registry.clone(),
            next_seg_ids,
            dict_data: crate::storage::serialize_dict(&self.dict),
            vocab_data,
            // The frozen per-query tag space (ADR-049/055) — re-persisted so the filter resolves to
            // the same `TagId`s on the next reopen. Empty + finalized for an untagged cluster.
            tag_dict_data: crate::storage::serialize_tagdict(&self.tag_dict),
        };
        crate::storage::write_cluster_manifest(&manifest, &dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|e| ShardError::Log(format!("writing cluster manifest: {e}")))?;

        // 4. Committed. Truncate the captured log prefix + GC orphaned segment files (both
        //    best-effort: a crash here just replays an already-captured tail / leaves
        //    orphan files that are ignored on open).
        self.epoch.store(new_epoch, Ordering::Relaxed);
        if let Err(e) = self.log.checkpoint(up_to) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalReset,
                detail: "cluster log truncation after checkpoint failed (benign: \
                         replayed on next open)"
                    .into(),
                error: e.to_string(),
            });
        }
        self.gc_orphan_segments(&dir, &segment_registry);
        Ok(())
    }

    /// Best-effort GC of segment files no longer in the committed registry (superseded by
    /// a re-seal/compaction during the just-committed checkpoint, or left by an earlier
    /// crashed checkpoint). An orphan left behind is benign — it is not in the manifest,
    /// so `open` never attaches it.
    fn gc_orphan_segments(&self, dir: &Path, registry: &[Vec<String>]) {
        for (s, files) in registry.iter().enumerate() {
            let keep: HashSet<&str> = files.iter().map(String::as_str).collect();
            let seg_dir = crate::cluster::coordinator::shard_dir(dir, s).join("segments");
            let Ok(rd) = std::fs::read_dir(&seg_dir) else {
                continue;
            };
            for entry in rd.flatten() {
                let path = entry.path();
                let is_seg = path
                    .extension()
                    .is_some_and(|e| e.eq_ignore_ascii_case("seg"));
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if !is_seg || keep.contains(name) {
                    continue;
                }
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::WalReset,
                        detail: format!(
                            "removing orphaned segment {name} after checkpoint failed \
                             (ignored on open)"
                        ),
                        error: e.to_string(),
                    }),
                }
            }
        }
    }

    /// The current checkpoint generation / log epoch (0 for an in-memory cluster).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }
}
