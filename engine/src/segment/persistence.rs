//! `impl Engine` — the durability layer: segment file naming, sealing a segment
//! to disk (mmap'd back), the all-or-nothing commit point (ADR-017), WAL
//! checkpoint/reset, and manifest + query-source persistence.

use super::{AcceptedSource, BaseSegment, Engine, IngestReport, Segment};
use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::{MmapSegment, StoredSource};

struct StagedSources {
    name: String,
    path: PathBuf,
}

#[cfg(test)]
std::thread_local! {
    static CRASH_AFTER_SOURCE_MANIFEST_COMMIT: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn inject_crash_after_source_manifest_commit() {
    CRASH_AFTER_SOURCE_MANIFEST_COMMIT.with(|armed| {
        assert!(
            !armed.replace(false),
            "injected crash after joint manifest commit"
        );
    });
}

impl Engine {
    /// Generate the next segment filename and increment the counter.
    fn next_segment_filename(&mut self) -> String {
        let name = format!("seg_{:06}.seg", self.next_seg_id);
        self.next_seg_id += 1;
        name
    }

    /// Seal a segment: if persistent, write to disk and mmap back; otherwise keep
    /// in memory. Pushes onto self.segments. Returns `true` if the seal is durable
    /// for the engine's mode — i.e. it was written to disk as an `Mmap` segment, OR
    /// the engine is in-memory (no `data_dir`, so there is nothing to persist).
    /// Returns `false` only when persistent-mode persistence failed and the segment
    /// fell back to in-memory: the data is served from RAM but is NOT on disk, so a
    /// caller about to advance/​truncate the WAL must not (see [`Engine::flush`]).
    pub(in crate::segment) fn seal_and_push(&mut self, seg: Segment) -> bool {
        let (base, persisted) = self.make_base_segment(seg);
        self.segments.push(Arc::new(base));
        self.refresh_phrase_capability();
        persisted
    }

    /// Convert a sealed Segment into a BaseSegment (mmap'd if persistent). The
    /// returned bool is the durability signal documented on [`Self::seal_and_push`]:
    /// `true` for a disk-backed `Mmap` (or for in-memory mode), `false` for a
    /// persistent-mode write/mmap failure that fell back to `Memory`.
    pub(in crate::segment) fn make_base_segment(&mut self, seg: Segment) -> (BaseSegment, bool) {
        let data_dir = self.config.data_dir.clone();
        if let Some(ref dir) = data_dir {
            let name = self.next_segment_filename();
            let seg_dir = dir.join("segments");
            let path = seg_dir.join(&name);
            match crate::storage::write_segment(&seg, &path) {
                Ok(()) => match MmapSegment::open(&path) {
                    Ok(mmap_seg) => return (BaseSegment::Mmap(mmap_seg), true),
                    Err(e) => {
                        self.persistence_healthy = false;
                        self.emit(crate::events::EngineEvent::DurabilityFailure {
                            op: crate::events::DurabilityOp::SegmentMmap,
                            detail: format!(
                                "segment mmap failed for {}, falling back to in-memory",
                                path.display()
                            ),
                            error: e.to_string(),
                        });
                    }
                },
                Err(e) => {
                    self.persistence_healthy = false;
                    self.emit(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::SegmentWrite,
                        detail: format!(
                            "segment write failed for {}, falling back to in-memory",
                            path.display()
                        ),
                        error: e.to_string(),
                    });
                }
            }
            // Fall back to in-memory if write/mmap fails — NOT durable (false).
            (BaseSegment::Memory(seg), false)
        } else {
            // In-memory mode: there is no disk to persist to, so the seal is as
            // durable as the engine gets (true) — callers must not treat it as a
            // persistence failure.
            (BaseSegment::Memory(seg), true)
        }
    }

    /// Persist a sealed segment to disk and mmap it back, propagating any I/O
    /// error instead of silently falling back to an in-memory segment (that
    /// silent fallback is the false-durability bug behind ADR-017). Returns the
    /// base segment plus its on-disk path, so a later commit failure can delete
    /// the orphaned file. In-memory mode (no `data_dir`) returns a `Memory` base
    /// and `None`.
    pub(in crate::segment) fn build_durable_base(
        &mut self,
        seg: Segment,
    ) -> std::io::Result<(BaseSegment, Option<PathBuf>)> {
        let Some(dir) = self.config.data_dir.clone() else {
            return Ok((BaseSegment::Memory(seg), None));
        };
        let path = dir.join("segments").join(self.next_segment_filename());
        if let Err(e) = crate::storage::write_segment(&seg, &path) {
            self.persistence_healthy = false;
            return Err(e);
        }
        match MmapSegment::open(&path) {
            Ok(mmap_seg) => Ok((BaseSegment::Mmap(mmap_seg), Some(path))),
            Err(e) => {
                self.persistence_healthy = false;
                self.best_effort_remove_segment(&path);
                Err(e)
            }
        }
    }

    /// Durably commit a freshly-built segment as a new base segment,
    /// all-or-nothing (ADR-017). Writes the segment file (fsync'd + atomic rename
    /// via `write_segment`), appends it in memory, then writes the manifest — the
    /// atomic commit point, which both references the new segment file and embeds
    /// the updated dict. If the segment or manifest write fails, the in-memory
    /// segment is dropped and the orphan file deleted, so nothing is committed
    /// (mirrors RocksDB's `IngestExternalFile`).
    ///
    /// `accepted` carries the source documents of queries that compiled. ADR-121
    /// writes a complete immutable source candidate (including these documents)
    /// before the manifest commit point. The manifest then selects the segment and
    /// source corpus together; only after that durable selection are the documents
    /// published into the live in-memory source store.
    pub(in crate::segment) fn commit_base_segment(
        &mut self,
        seg: Segment,
        accepted: Vec<AcceptedSource>,
        report: IngestReport,
    ) -> std::io::Result<IngestReport> {
        let (base, seg_path) = self.build_durable_base(seg)?;
        let pending_sources: Vec<(u64, StoredSource)> = accepted
            .into_iter()
            .map(|source| {
                (
                    source.logical,
                    StoredSource::with_generation(
                        source.text,
                        source.version,
                        source.source_generation,
                        source.tags,
                        source.tags_known,
                    ),
                )
            })
            .collect();
        let staged_sources = match self.stage_query_sources(&pending_sources) {
            Ok(staged) => staged,
            Err(e) => {
                if let Some(p) = seg_path {
                    self.best_effort_remove_segment(&p);
                }
                return Err(e);
            }
        };
        self.segments.push(Arc::new(base));
        self.refresh_phrase_capability();

        // The manifest write is the atomic commit point. It names both already-
        // durable artifacts. If it fails, roll the batch back entirely: drop the
        // in-memory segment and delete both orphans.
        let selected_source = staged_sources.as_ref().map_or_else(
            || self.source_file_name.clone(),
            |staged| staged.name.clone(),
        );
        if !self.save_manifest_selecting_source(&selected_source) {
            self.segments.pop();
            self.refresh_phrase_capability();
            if let Some(p) = seg_path {
                self.best_effort_remove_segment(&p);
            }
            if let Some(staged) = staged_sources {
                self.best_effort_remove_source(&staged.path);
            }
            return Err(std::io::Error::other(
                "manifest write failed during ingest; batch rolled back",
            ));
        }

        #[cfg(test)]
        inject_crash_after_source_manifest_commit();

        // Past the joint commit point — both match data and canonical sources are
        // durable. Record the selected generation before any fallible live
        // publication. Lazy mode publishes by remapping the complete candidate;
        // resident/in-memory mode applies the prepared documents directly.
        let staged_publishes_lazy = staged_sources.is_some() && self.query_store.is_lazy();
        if let Some(staged) = staged_sources {
            self.activate_staged_sources(staged);
        }
        if !staged_publishes_lazy {
            for (logical, source) in pending_sources {
                self.query_store.insert_stored(logical, source);
            }
        }

        self.emit(crate::events::EngineEvent::Ingest {
            ingested: report.ingested,
            rejected_parse: report.rejected_parse,
            rejected_class_d: report.rejected_class_d,
            base_segments_after: self.segments.len(),
        });
        Ok(report)
    }

    /// Write a WAL flush checkpoint (all prior WAL entries are in segments).
    pub(in crate::segment) fn checkpoint_wal(&mut self) {
        // Capture the error and release the `&mut self.wal` borrow before `emit`
        // (which needs `&self`); `.err()` drops the borrowed Result.
        let err = if let Some(ref mut wal) = self.wal {
            // Use the latest segment name as the checkpoint marker
            let name = format!("seg_{:06}.seg", self.next_seg_id - 1);
            wal.append_flush_checkpoint(&name).err()
        } else {
            None
        };
        if let Some(e) = err {
            self.emit(crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::WalCheckpoint,
                detail: "WAL flush checkpoint write failed".to_string(),
                error: e.to_string(),
            });
        }
    }

    /// Reset the WAL after a successful flush + manifest write. Only call when
    /// both the checkpoint and manifest have been persisted, so no data is lost.
    pub(in crate::segment) fn reset_wal_if_safe(&mut self) {
        let err = if let Some(ref mut wal) = self.wal {
            wal.reset().err()
        } else {
            None
        };
        if let Some(e) = err {
            self.emit(crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::WalReset,
                detail: "WAL reset failed".to_string(),
                error: e.to_string(),
            });
        }
    }

    /// Save the manifest file if persistence is enabled. Returns true if the
    /// write succeeded (or persistence is not enabled), false on failure.
    pub(in crate::segment) fn save_manifest_if_persistent(&mut self) -> bool {
        let selected_source = self.source_file_name.clone();
        self.save_manifest_selecting_source(&selected_source)
    }

    /// Write the standalone manifest selecting `source_file_name`. The caller
    /// must have fsync'd a non-legacy immutable sidecar before invoking this.
    pub(in crate::segment) fn save_manifest_selecting_source(
        &mut self,
        source_file_name: &str,
    ) -> bool {
        // Cluster shards (ADR-032) do not own a manifest: the coordinator's
        // `cluster_manifest.bin` is the sole segment registry + dict store. Segment
        // `.seg` files are still written (by `make_base_segment`); only the per-shard
        // manifest is suppressed. Reported as success so the flush/compaction paths
        // (which gate WAL reset on this) proceed normally.
        if !self.owns_manifest {
            return true;
        }
        if let Some(ref dir) = self.config.data_dir {
            let segment_files: Vec<String> = self
                .segments
                .iter()
                .filter_map(|s| {
                    if let BaseSegment::Mmap(m) = s.as_ref() {
                        m.path()
                            .file_name()
                            .and_then(|f| f.to_str())
                            .map(std::string::ToString::to_string)
                    } else {
                        None
                    }
                })
                .collect();
            // ADR-066: bake each dirty mmap segment's tombstones into the commit point.
            // The on-disk `.seg` alive flags are frozen at write time and live deletes
            // mutate only the in-RAM overlay, so without this the flush-time WAL reset
            // would drop the only durable record of a base-segment delete and the
            // deleted query would resurrect on reopen. The dead set is maintained
            // incrementally on the segment, so this is O(deletes), never a rescan.
            let segment_tombstones: Vec<(String, Vec<u8>)> = self
                .segments
                .iter()
                .filter_map(|s| {
                    let BaseSegment::Mmap(m) = s.as_ref() else {
                        return None;
                    };
                    let dead = m.dead_overlay();
                    if dead.is_empty() {
                        return None; // clean — no bitmap to record
                    }
                    let name = m.path().file_name().and_then(|f| f.to_str())?.to_string();
                    let mut bytes = Vec::with_capacity(dead.serialized_size());
                    // Serialization into a Vec cannot fail; if it ever did, recording
                    // no bitmap (resurrect-risk, a bounded false positive) is the
                    // conservative direction — never a wrong tombstone.
                    dead.serialize_into(&mut bytes).ok()?;
                    Some((name, bytes))
                })
                .collect();
            // The rollback fence (ADR-068): if ANY registered segment holds class-D
            // always-candidates, this commit writes manifest v4 so a pre-ADR-068
            // binary fails `Engine::open` loudly instead of skipping the v4 segment
            // as corrupt and silently serving without its queries (recovery's
            // corrupt-segment posture is skip + event, not abort). Registered
            // segments are mmap'd (flush writes the file, then attaches), so the
            // file's own version word is the source of truth.
            let class_d_fence = self.segments.iter().any(|s| match s.as_ref() {
                BaseSegment::Mmap(m) => m.carries_class_d_fence(),
                BaseSegment::Memory(seg) => seg
                    .classes()
                    .iter()
                    .any(|c| matches!(c, crate::compile::CostClass::D)),
            });
            // The hot-tier fence (ADR-105) mirrors the class-D fence: any registered
            // segment holding class-H entries makes this commit write manifest v5, so
            // a pre-ADR-105 binary — which never probes the hot index — refuses the
            // corpus loudly instead of silently serving without those queries.
            let hot_fence = self.segments.iter().any(|s| match s.as_ref() {
                BaseSegment::Mmap(m) => m.carries_hot_fence(),
                BaseSegment::Memory(seg) => seg.has_hot_entries(),
            });
            // Segment v8 carries the source/exact generation used by point reads.
            // Standalone recovery skips an unsupported segment, so propagate the
            // capability to manifest v6 and force an older binary to refuse the
            // corpus before it can silently serve a partial index.
            let source_generation_fence = self.segments.iter().any(|s| match s.as_ref() {
                BaseSegment::Mmap(m) => m.carries_source_generation_fence(),
                BaseSegment::Memory(seg) => seg.max_source_generation() != 0,
            });
            let manifest = crate::storage::Manifest {
                segment_files,
                class_d_fence,
                hot_fence,
                source_generation_fence,
                hot_anchor_theta: self.config.hot_anchor_threshold,
                next_seg_id: self.next_seg_id,
                dict_data: crate::storage::serialize_dict(&self.dict),
                tag_dict_data: crate::storage::serialize_tagdict(&self.tag_dict),
                rejected_parse: self.rejected_parse,
                rejected_class_d: self.rejected_class_d,
                // Everything appended through this seq is captured by this commit
                // (single-writer: every frame already appended is already applied).
                wal_seq_watermark: self.wal.as_ref().map_or(0, crate::wal::Wal::last_seq),
                segment_tombstones,
                source_file_name: source_file_name.to_string(),
            };
            let dir = dir.clone();
            if let Err(e) = crate::storage::write_manifest(&manifest, &dir.join("manifest.bin")) {
                self.persistence_healthy = false;
                self.emit(crate::events::EngineEvent::DurabilityFailure {
                    op: crate::events::DurabilityOp::ManifestWrite,
                    detail: "manifest write failed (atomic commit point); batch rolled back"
                        .to_string(),
                    error: e.to_string(),
                });
                return false;
            }
        }
        true
    }

    /// Persist the current source corpus as a new immutable sidecar and atomically
    /// select it with the current standalone segment registry.
    pub(in crate::segment) fn commit_sources_and_manifest(&mut self) -> bool {
        let Ok(staged) = self.stage_query_sources(&[]) else {
            return false;
        };
        let selected = staged
            .as_ref()
            .map_or_else(|| self.source_file_name.clone(), |s| s.name.clone());
        if !self.save_manifest_selecting_source(&selected) {
            if let Some(staged) = staged {
                self.best_effort_remove_source(&staged.path);
            }
            return false;
        }
        if let Some(staged) = staged {
            self.activate_staged_sources(staged);
        }
        true
    }

    /// Standalone commits must select a source snapshot with every registry
    /// change. Cluster shards delegate that atomic selection to their coordinator.
    pub(in crate::segment) fn commit_manifest_with_current_sources(&mut self) -> bool {
        if self.owns_manifest {
            self.commit_sources_and_manifest()
        } else {
            self.save_manifest_if_persistent()
        }
    }

    /// [`flush`](Engine::flush), guaranteeing the on-disk source store is persisted even
    /// when the memtable is empty — the cluster checkpoint seal's seam (ADR-074). `flush`
    /// persists sources whenever it seals a non-empty memtable, but a checkpoint on a
    /// CLEAN shard (empty memtable — e.g. right after a bulk build, or with only tombstone
    /// deletes since the last seal) early-returns past that save, leaving the selected
    /// source sidecar absent or stale. The cluster trims its translog at the checkpoint,
    /// so a reopen rebuilds `live_sources` from this file alone: an absent/stale file
    /// would omit bulk-loaded ids from (or resurrect tombstone-deleted ids into) the
    /// source set the vocabulary rebuild gathers — silent corpus loss/resurrection on
    /// the next `set_vocab`. A write failure degrades `persistence_healthy`, which the
    /// seal turns into a fail-closed abort before any translog trim.
    pub(crate) fn flush_and_persist_sources_for_checkpoint(&mut self) {
        let memtable_was_empty = self.memtable.is_empty();
        self.flush();
        if memtable_was_empty {
            self.save_query_sources();
        }
    }

    pub(in crate::segment) fn save_query_sources(&mut self) {
        // A standalone source-only publication still goes through the same joint
        // manifest commit. Cluster shards have no local manifest; their coordinator
        // already selects the shard sidecar in `cluster_manifest.bin`.
        if self.owns_manifest {
            self.commit_sources_and_manifest();
            return;
        }
        self.save_query_sources_in_place();
    }

    fn save_query_sources_in_place(&mut self) {
        let Some(dir) = self.config.data_dir.clone() else {
            return;
        };
        let path = dir.join(&self.source_file_name);
        if let Err(e) = self.query_store.write_to(&path) {
            self.persistence_healthy = false;
            self.emit(crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::SourceStoreWrite,
                detail: "query sources write failed (_source/explain may be stale)".to_string(),
                error: e.to_string(),
            });
            return;
        }
        // Lazy mode: re-map the freshly written file so reads hit it and the
        // in-memory overlay resets (reclaiming the post-flush deltas). Resident
        // mode keeps its in-RAM map as the source of truth (no re-map needed).
        if self.query_store.is_lazy() {
            match crate::storage::SourceStore::open(&path, false) {
                Ok(s) => self.query_store = Arc::new(s),
                Err(e) => {
                    self.persistence_healthy = false;
                    self.emit(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::SourceStoreRemap,
                        detail: "query sources re-map failed after write (lazy mode)".to_string(),
                        error: e.to_string(),
                    });
                }
            }
        }
    }

    /// Prepare a complete immutable standalone source corpus. `updates` are
    /// included in the file without becoming visible through `query_store`.
    fn stage_query_sources(
        &mut self,
        updates: &[(u64, StoredSource)],
    ) -> std::io::Result<Option<StagedSources>> {
        if !self.owns_manifest {
            return Ok(None);
        }
        let Some(dir) = self.config.data_dir.clone() else {
            return Ok(None);
        };
        let name = match self.next_source_file_name() {
            Ok(name) => name,
            Err(e) => {
                self.record_source_write_failure("allocating immutable source filename", &e);
                return Err(e);
            }
        };
        let path = dir.join(&name);
        if let Err(e) = self.query_store.write_to_with_updates(updates, &path) {
            self.record_source_write_failure("writing immutable query-source candidate", &e);
            return Err(e);
        }
        Ok(Some(StagedSources { name, path }))
    }

    fn next_source_file_name(&self) -> std::io::Result<String> {
        let current = self
            .source_file_name
            .strip_prefix("sources_g")
            .and_then(|rest| rest.strip_suffix(".dat"))
            .map(str::parse::<u64>)
            .transpose()
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "invalid immutable source filename {}",
                        self.source_file_name
                    ),
                )
            })?
            .unwrap_or(0);
        let next = current.checked_add(1).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "source-sidecar generation space exhausted",
            )
        })?;
        Ok(format!("sources_g{next:020}.dat"))
    }

    fn activate_staged_sources(&mut self, staged: StagedSources) {
        let old_name = std::mem::replace(&mut self.source_file_name, staged.name);
        let mut may_remove_old = true;
        // Lazy mode: re-map the manifest-selected immutable file so reads hit it
        // and the in-memory overlay resets. Resident mode already contains the
        // published documents and needs no re-map.
        if self.query_store.is_lazy() {
            match crate::storage::SourceStore::open(&staged.path, false) {
                Ok(store) => self.query_store = Arc::new(store),
                Err(e) => {
                    may_remove_old = false;
                    self.persistence_healthy = false;
                    self.emit(crate::events::EngineEvent::DurabilityFailure {
                        op: crate::events::DurabilityOp::SourceStoreRemap,
                        detail: format!(
                            "query sources re-map failed after joint commit; manifest selects {}",
                            staged.path.display()
                        ),
                        error: e.to_string(),
                    });
                }
            }
        }
        if may_remove_old && old_name != self.source_file_name {
            if let Some(dir) = self.config.data_dir.as_ref() {
                self.best_effort_remove_source(&dir.join(old_name));
            }
        }
    }

    fn record_source_write_failure(&mut self, detail: &str, error: &std::io::Error) {
        self.persistence_healthy = false;
        self.emit(crate::events::EngineEvent::DurabilityFailure {
            op: crate::events::DurabilityOp::SourceStoreWrite,
            detail: detail.to_string(),
            error: error.to_string(),
        });
    }

    fn best_effort_remove_source(&self, path: &std::path::Path) {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => self.emit(crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::SourceStoreWrite,
                detail: format!(
                    "failed to remove unreferenced source sidecar {}",
                    path.display()
                ),
                error: e.to_string(),
            }),
        }
    }

    /// Basename of the source sidecar this engine persists. Cluster durability
    /// records it in the coordinator manifest / shard checkpoint.
    pub(crate) fn source_file_name(&self) -> &str {
        &self.source_file_name
    }

    /// Collect paths of mmap'd segments (for cleanup during compaction).
    pub(in crate::segment) fn collect_mmap_paths(&self) -> Vec<PathBuf> {
        self.segments
            .iter()
            .filter_map(|s| {
                if let BaseSegment::Mmap(m) = s.as_ref() {
                    Some(m.path().to_path_buf())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Remove old segment files after compaction replaces them.
    pub(in crate::segment) fn cleanup_segment_files(&self, paths: &[PathBuf]) {
        for p in paths {
            self.best_effort_remove_segment(p);
        }
    }

    /// Best-effort removal of a segment file on a cleanup/rollback path.
    ///
    /// The caller's primary result already reflects the operation outcome, so a
    /// removal failure must not change control flow. But rather than silently
    /// discarding the error (which could leak orphan files unnoticed), we surface
    /// it through the observer as [`EngineEvent::SegmentCleanupFailed`]. A missing
    /// file is the expected, benign case and is not reported.
    pub(in crate::segment) fn best_effort_remove_segment(&self, path: &std::path::Path) {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => self.emit(crate::events::EngineEvent::SegmentCleanupFailed {
                path: path.to_path_buf(),
                error: e.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod source_commit_tests {
    use super::*;

    fn scratch_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("rr-source-commit-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    fn config(dir: &std::path::Path) -> crate::config::EngineConfig {
        crate::config::EngineConfig {
            data_dir: Some(dir.to_path_buf()),
            ..crate::config::EngineConfig::default()
        }
    }

    #[test]
    fn post_manifest_crash_recovers_source_explain_rebuild_checkpoint_and_backup() {
        let dir = scratch_dir("post-manifest");
        let backup = scratch_dir("post-manifest-backup-root").join("backup");
        let mut engine = Engine::with_config(
            crate::normalize::Normalizer::default_vocab().expect("normalizer"),
            config(&dir),
        );
        CRASH_AFTER_SOURCE_MANIFEST_COMMIT.with(|armed| armed.set(true));
        let crash = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rows = [(7, "1995 fleer".to_string())];
            let _ = engine.try_bulk_ingest(&rows);
        }));
        assert!(crash.is_err(), "the injected post-commit crash must fire");
        assert!(
            engine.get_query_source(7).is_none(),
            "the crash must precede live source publication"
        );
        drop(engine);

        let mut reopened = Engine::open(
            crate::normalize::Normalizer::default_vocab().expect("normalizer"),
            config(&dir),
        )
        .expect("reopen from joint commit");
        let snapshot = reopened.snapshot();
        assert_eq!(
            snapshot
                .get_query_document(7)
                .expect("GET document after recovery")
                .query(),
            "1995 fleer"
        );
        assert!(
            snapshot.explain_hit(7, "1995 Fleer card").is_some(),
            "explain must compile from the recovered canonical source"
        );
        let mut scratch = crate::segment::MatchScratch::new();
        let mut matches = Vec::new();
        snapshot.match_title("1995 Fleer card", &mut scratch, &mut matches, false);
        assert!(
            matches.contains(&7),
            "matching remains available after recovery"
        );

        reopened
            .set_vocab(crate::vocab::Vocab::default())
            .expect("install rebuild trigger");
        assert_eq!(
            reopened.recompile_stale_segments(),
            1,
            "source-driven rebuild must see the recovered document"
        );
        reopened.flush();
        reopened.backup_to(&backup).expect("backup joint commit");
        drop(reopened);

        for recovered_dir in [&dir, &backup] {
            let recovered = Engine::open(
                crate::normalize::Normalizer::default_vocab().expect("normalizer"),
                config(recovered_dir),
            )
            .expect("checkpoint/backup reopen");
            assert_eq!(recovered.get_query_source(7).as_deref(), Some("1995 fleer"));
            assert!(recovered.explain_hit(7, "1995 Fleer card").is_some());
        }

        let _ = std::fs::remove_dir_all(&dir);
        if let Some(root) = backup.parent() {
            let _ = std::fs::remove_dir_all(root);
        }
    }
}
