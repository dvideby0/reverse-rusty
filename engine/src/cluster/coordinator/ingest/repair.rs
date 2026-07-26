use super::{
    ClusterEngine, ClusterMutation, DurabilityOp, EngineEvent, PendingRepair, ResyncReport,
    ShardError,
};

impl ClusterEngine {
    /// Record a partial multi-shard apply (ADR-047): queue the failed shards for repair (keyed by
    /// logical id, so the latest mutation for an id wins), emit a `ClusterPartialApply` durability
    /// event, and build the honest [`ShardError::PartiallyApplied`] the caller returns. The
    /// mutation is already durably logged, so this is a liveness gap (a transient false-negative
    /// window on `failed`), not a lost write — [`Self::resync`] or reopen converges it.
    pub(super) fn note_partial(
        &self,
        mutation: ClusterMutation,
        logical: u64,
        applied: Vec<usize>,
        failed: Vec<usize>,
        first_err: Option<ShardError>,
    ) -> ShardError {
        let detail = first_err.map_or_else(|| "unknown shard error".to_string(), |e| e.to_string());
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                logical,
                PendingRepair {
                    mutation,
                    failed_shards: failed.clone(),
                },
            );
        self.emit(EngineEvent::DurabilityFailure {
            op: DurabilityOp::ClusterPartialApply,
            detail: format!("logical {logical}: applied on {applied:?}, failed on {failed:?}"),
            error: detail.clone(),
        });
        ShardError::PartiallyApplied {
            logical,
            applied,
            failed,
            detail,
        }
    }

    /// Drop any queued partial-apply entry for `logical` — a later full apply (or delete)
    /// supersedes it, so `resync` must not re-drive a stale mutation (e.g. resurrect a removed
    /// query). Cheap (an uncontended lock + a `BTreeMap` miss) on the default path, where the
    /// queue is always empty.
    pub(super) fn clear_pending(&self, logical: u64) {
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&logical);
    }

    /// Re-drive every queued partial-apply mutation (ADR-047) against its still-failed shards,
    /// converging a cluster left divergent by a mid-fan-out remote write failure WITHOUT a full
    /// reopen. Re-driving touches ONLY the failed shards — re-applying an Add there is a clean
    /// first insert (they never received it) and a Remove is idempotent — so already-converged
    /// shards are untouched. Idempotent and safe to call repeatedly: a still-unreachable shard
    /// stays queued. A no-op (empty report) on the in-process / RF=1 path, which never queues
    /// anything. The durable cluster log stays authoritative — a reopen replays it in order, so
    /// `resync` is a liveness optimization, not the correctness backstop.
    pub fn resync(&self) -> ResyncReport {
        // Exhaustive cross-shard reads take the exclusive side of the same
        // barrier. A repair re-drive mutates shard visibility just like a live
        // add/upsert/remove and must not slip between sequential shard reads.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Drain the queue, then re-drive OUTSIDE the lock (re-driving issues shard RPCs; holding
        // the lock across them would stall concurrent writes' note_partial/clear_pending).
        let pending: Vec<(u64, PendingRepair)> = {
            let mut guard = self
                .pending_repair
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *guard).into_iter().collect()
        };
        let mut repaired = 0usize;
        let mut still_pending = 0usize;
        for (logical, pr) in pending {
            // Serialize the whole per-id re-drive against same-id writers (the
            // same stripe scope the live paths hold), and skip our drained copy
            // when a concurrent writer queued fresher work for this id during
            // the drain — `note_partial` overwrites, so a live map entry is
            // strictly fresher than what we hold.
            let _logical_guard = self.logical_write_guard(logical);
            {
                let guard = self
                    .pending_repair
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if guard.contains_key(&logical) {
                    still_pending += 1;
                    continue;
                }
            }
            let mut still_failed = Vec::new();
            let mut first_err: Option<ShardError> = None;
            for &s in &pr.failed_shards {
                match crate::cluster::shard::apply_mutation(
                    self.shards[s].as_ref(),
                    &self.norm,
                    &self.dict,
                    &pr.mutation,
                    Some(s as u32),
                ) {
                    Ok(()) => {}
                    Err(e) => {
                        still_failed.push(s);
                        first_err.get_or_insert(e);
                    }
                }
            }
            if still_failed.is_empty() {
                repaired += 1;
                // A converged Remove has now deleted the row everywhere, so the
                // fail-closed reservation retained at the partial-apply point is
                // releasable — without this, the id would 409 every future
                // add_query until a coordinator reopen (review finding).
                if matches!(pr.mutation, ClusterMutation::Remove { .. }) {
                    self.remove_logical_id(logical);
                }
                continue;
            }
            still_pending += 1;
            let detail =
                first_err.map_or_else(|| "unknown shard error".to_string(), |e| e.to_string());
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ClusterPartialApply,
                detail: format!("resync: logical {logical} still failing on {still_failed:?}"),
                error: detail,
            });
            // Re-queue only the still-failed shards — but `or_insert`, so a fresher mutation a
            // concurrent write queued for this id during the drain is not clobbered.
            self.pending_repair
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(logical)
                .or_insert(PendingRepair {
                    mutation: pr.mutation,
                    failed_shards: still_failed,
                });
        }
        ResyncReport {
            repaired,
            still_pending,
        }
    }

    /// Number of mutations currently queued for partial-apply repair (ADR-047): 0 on a healthy
    /// cluster, and always 0 on the in-process / RF=1 path (whose writes never fail). A nonzero
    /// value means at least one shard is lagging — call [`Self::resync`] (or wait for the next
    /// autoscaler `tick`) to converge it. Introspection for operators + tests.
    #[must_use]
    pub fn pending_repairs(&self) -> usize {
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Replay one recovered mutation through the same `apply` funnel as live writes.
    pub(in crate::cluster::coordinator) fn replay_apply(
        &self,
        m: ClusterMutation,
    ) -> Result<(), ShardError> {
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => {
                if !self.insert_logical_id(logical) {
                    return Err(ShardError::DuplicateLogicalId(logical));
                }
                self.apply_add(logical, version, &dsl, &tags, &placement)?;
            }
            ClusterMutation::Remove { logical } => {
                self.apply_remove(logical)?;
                self.remove_logical_id(logical);
            }
            ClusterMutation::Upsert {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => {
                self.insert_logical_id(logical);
                self.apply_upsert(logical, version, &dsl, &tags, &placement)?;
            }
        }
        Ok(())
    }

    /// Seal every shard's memtable into an immutable base segment.
    pub fn flush(&self) -> Result<(), ShardError> {
        for s in &self.shards {
            s.flush()?;
        }
        self.compact_logical_ids();
        Ok(())
    }
}
