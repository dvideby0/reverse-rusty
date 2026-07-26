use super::{Arc, Engine, SourceCommitState, StagedSources, StoredSource};

impl Engine {
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

    pub(super) fn save_query_sources_in_place(&mut self) {
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
    pub(in crate::segment) fn stage_query_sources(
        &mut self,
        updates: &[(u64, StoredSource)],
    ) -> std::io::Result<Option<StagedSources>> {
        if !self.owns_manifest {
            return Ok(None);
        }
        let Some(dir) = self.config.data_dir.clone() else {
            return Ok(None);
        };
        if self.source_commit_state == SourceCommitState::IncompleteRecovery {
            let error = std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "refusing source commit from an incomplete recovery baseline; restart after \
                 repairing the manifest-selected source sidecar",
            );
            self.record_source_write_failure(
                "joint source/manifest commit refused from incomplete recovery",
                &error,
            );
            return Err(error);
        }
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

    pub(in crate::segment) fn discard_staged_sources(&self, staged: Option<StagedSources>) {
        if let Some(staged) = staged {
            self.best_effort_remove_source(&staged.path);
        }
    }

    pub(super) fn next_source_file_name(&self) -> std::io::Result<String> {
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

    pub(super) fn activate_staged_sources(&mut self, staged: StagedSources) {
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
                    self.source_commit_state = SourceCommitState::IncompleteRecovery;
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

    pub(super) fn record_source_write_failure(&mut self, detail: &str, error: &std::io::Error) {
        self.persistence_healthy = false;
        self.emit(crate::events::EngineEvent::DurabilityFailure {
            op: crate::events::DurabilityOp::SourceStoreWrite,
            detail: detail.to_string(),
            error: error.to_string(),
        });
    }

    pub(super) fn best_effort_remove_source(&self, path: &std::path::Path) {
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
}
