use super::{Arc, Engine, SegmentAddress};

impl Engine {
    /// Tombstone a query version in the MEMTABLE (update = insert_live new +
    /// tombstone old). `local_id` is a memtable-local id (as returned by
    /// `insert_live`).
    ///
    /// Returns `Err` if the tombstone could not be durably logged; in that case
    /// the in-memory tombstone is not applied (the entry stays alive) so the
    /// memtable never diverges from the WAL.
    pub fn tombstone(&mut self, local_id: u32) -> std::io::Result<()> {
        // WAL: memtable tombstones use seg_idx = u32::MAX as sentinel
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_tombstone(u32::MAX, local_id) {
                self.wal_healthy = false;
                return Err(e);
            }
        }
        Arc::make_mut(&mut self.memtable).tombstone(local_id);
        self.refresh_phrase_capability();
        Ok(())
    }

    /// Resolve a generation-bearing address for one live base-segment row.
    ///
    /// `logical_id` is the caller's stable expected identity. A stale positional
    /// pair that now names another row is rejected here instead of being minted
    /// into a token for the wrong query.
    ///
    /// The token must be retained from the time the row is resolved; a bare
    /// `(segment, local_id)` pair cannot safely be reconstructed after
    /// compaction because both numbers may have been reused for another query.
    pub fn segment_address(
        &self,
        seg_idx: usize,
        local_id: u32,
        logical_id: u64,
    ) -> Result<SegmentAddress, crate::error::TombstoneError> {
        let segment = self
            .segments
            .get(seg_idx)
            .ok_or(crate::error::TombstoneError::SegmentNotFound { segment: seg_idx })?;
        if local_id as usize >= segment.len() {
            return Err(crate::error::TombstoneError::LocalNotFound {
                segment: seg_idx,
                local_id,
            });
        }
        if !segment.is_alive(local_id) {
            return Err(crate::error::TombstoneError::AlreadyDeleted {
                segment: seg_idx,
                local_id,
            });
        }
        if segment.logical(local_id) != logical_id {
            return Err(crate::error::TombstoneError::StaleAddress {
                segment: seg_idx,
                local_id,
            });
        }
        let generation = self.segment_generations.get(seg_idx).ok_or(
            crate::error::TombstoneError::StaleAddress {
                segment: seg_idx,
                local_id,
            },
        )?;
        // A persistent standalone engine may be serving a coherent live
        // fallback/recompile layout whose manifest commit failed. Such a
        // generation has no replay-safe positional WAL ordinal, so fail at
        // address resolution as well as rechecking in `tombstone_in`.
        if self.owns_manifest
            && self.config.data_dir.is_some()
            && !self
                .committed_segment_generations
                .iter()
                .any(|committed| Arc::ptr_eq(committed, generation))
        {
            return Err(crate::error::TombstoneError::StaleAddress {
                segment: seg_idx,
                local_id,
            });
        }
        Ok(SegmentAddress {
            generation: Arc::clone(generation),
            segment: seg_idx,
            local_id,
            logical_id,
        })
    }

    /// Tombstone the live row identified by a generation-bearing physical
    /// address.
    ///
    /// Segment generation, row bounds, logical identity, and liveness are all
    /// validated before the WAL is touched. A stale address consumes no WAL
    /// sequence; callers must re-resolve the logical query after compaction.
    /// Valid addresses preserve WAL-first ordering: an append failure leaves
    /// the row alive.
    pub fn tombstone_in(
        &mut self,
        address: &SegmentAddress,
    ) -> Result<(), crate::error::TombstoneError> {
        let stale = || crate::error::TombstoneError::StaleAddress {
            segment: address.segment,
            local_id: address.local_id,
        };
        let seg_idx = self
            .segment_generations
            .iter()
            .position(|generation| Arc::ptr_eq(generation, &address.generation))
            .ok_or_else(stale)?;
        let segment = self.segments.get(seg_idx).ok_or_else(stale)?;
        if address.local_id as usize >= segment.len()
            || segment.logical(address.local_id) != address.logical_id
        {
            return Err(stale());
        }
        if !segment.is_alive(address.local_id) {
            return Err(crate::error::TombstoneError::AlreadyDeleted {
                segment: seg_idx,
                local_id: address.local_id,
            });
        }
        // Standalone WAL replay addresses the latest committed manifest list,
        // which can lag the coherent live layout after a failed flush or vocab
        // recompile. Resolve the generation in that durable list rather than
        // writing its live-vector ordinal. In-memory/cluster engines have no
        // standalone positional replay authority, so their current ordinal is
        // sufficient for the process-local mutation.
        let replay_seg_idx = if self.owns_manifest && self.config.data_dir.is_some() {
            self.committed_segment_generations
                .iter()
                .position(|generation| Arc::ptr_eq(generation, &address.generation))
                .ok_or_else(stale)?
        } else {
            seg_idx
        };
        let wal_seg_idx = u32::try_from(replay_seg_idx).map_err(|_| {
            crate::error::TombstoneError::SegmentIndexOverflow {
                segment: replay_seg_idx,
            }
        })?;
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_tombstone(wal_seg_idx, address.local_id) {
                self.wal_healthy = false;
                return Err(crate::error::TombstoneError::Wal(e));
            }
        }
        let segment = self.segments.get_mut(seg_idx).ok_or_else(stale)?;
        Arc::make_mut(segment).tombstone(address.local_id);
        self.refresh_phrase_capability();
        Ok(())
    }

    /// Delete all live entries with a given logical ID across all segments
    /// and the memtable. Uses the per-segment reverse index for O(segments)
    /// lookup instead of O(total_entries) full scan. Returns the number of
    /// entries tombstoned.
    ///
    /// Durability (ADR-066): the delete is logged as ONE address-free
    /// `DeleteByLogical` WAL frame *before* anything is applied — all-or-nothing
    /// (a WAL failure rejects the whole delete; the server returns HTTP 503 and a
    /// retry is idempotent). The frame carries the logical id, not `(seg_idx,
    /// local)` addresses, so a later compaction that renumbers the address space
    /// can never make a crash-recovery replay tombstone an unrelated query.
    pub fn delete_by_logical_id(&mut self, logical_id: u64) -> std::io::Result<usize> {
        if let Some(ref mut wal) = self.wal {
            if let Err(e) = wal.append_delete_logical(logical_id) {
                self.wal_healthy = false;
                return Err(e);
            }
        }
        Ok(self.apply_delete_by_logical(logical_id))
    }

    /// The shared apply funnel behind [`delete_by_logical_id`](Self::delete_by_logical_id)
    /// and its WAL replay: tombstone every live copy of `logical_id` in the base
    /// segments and the memtable, then drop the source text. No WAL involvement —
    /// the caller has already logged (live path) or is replaying (recovery). Live
    /// and replay running the same funnel is what makes replay deterministic:
    /// at the frame's position in the log, the recovered live set is exactly the
    /// live set the original call saw.
    pub(in crate::segment) fn apply_delete_by_logical(&mut self, logical_id: u64) -> usize {
        let mut count = 0usize;
        for seg in &mut self.segments {
            let locals: Vec<u32> = seg
                .locals_for_logical(logical_id)
                .iter()
                .copied()
                .filter(|&local| seg.is_alive(local))
                .collect();
            for local in locals {
                Arc::make_mut(seg).tombstone(local);
                count += 1;
            }
        }

        let mem_locals: Vec<u32> = self
            .memtable
            .locals_for_logical(logical_id)
            .iter()
            .copied()
            .filter(|&local| {
                self.memtable
                    .alive
                    .get(local as usize)
                    .copied()
                    .unwrap_or(false)
            })
            .collect();
        for local in mem_locals {
            Arc::make_mut(&mut self.memtable).tombstone(local);
            count += 1;
        }

        if count > 0 {
            self.query_store.remove(logical_id);
            self.refresh_phrase_capability();
        }
        count
    }
}
