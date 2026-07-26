#[cfg(feature = "distributed")]
use super::Instant;
use super::{
    extract_readonly, translog, Arc, ArcSwap, ClusterMutation, Engine, EngineSnapshot,
    IngestReport, LocalShard, PlacedQuery, PoisonError, ShardError,
};

impl LocalShard {
    /// Apply one logged mutation to the engine WITHOUT re-appending it to the translog — used by
    /// self-restart replay (ADR-039 §6), where the op is already durable in the translog. The
    /// translog-appending counterpart is the seam's `insert_extracted`/`delete_by_logical_id`.
    /// A malformed acknowledged frame fails restart loudly; silently skipping it
    /// would shrink the recovered shard.
    pub(super) fn apply_to_engine(&self, m: &ClusterMutation) -> Result<(), ShardError> {
        let mut eng = self.lock();
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => {
                let ast = crate::dsl::parse_for_recovery(dsl).map_err(|error| {
                    ShardError::Log(format!(
                        "parsing acknowledged shard add during self-restart: {error}"
                    ))
                })?;
                let mut lc = String::new();
                let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
                if eng
                    .insert_extracted_with_placement(&ex, *logical, *version, dsl, tags, placement)
                    .is_none()
                {
                    return Err(ShardError::Log(format!(
                        "acknowledged shard add {logical} was rejected during self-restart"
                    )));
                }
            }
            ClusterMutation::Remove { logical } => {
                eng.delete_by_logical_id(*logical).unwrap_or(0);
            }
            // Defensive: a per-shard translog never holds an Upsert frame today — the
            // coordinator decomposes a cluster upsert into per-shard delete + insert seam
            // calls, each re-logged as its own Remove/Add record (ADR-070). Replay one
            // anyway (same delete-then-insert semantics) rather than panic on a future
            // writer that logs it whole.
            ClusterMutation::Upsert {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => {
                let ast = crate::dsl::parse_for_recovery(dsl).map_err(|error| {
                    ShardError::Log(format!(
                        "parsing acknowledged shard upsert during self-restart: {error}"
                    ))
                })?;
                eng.delete_by_logical_id(*logical).unwrap_or(0);
                let mut lc = String::new();
                let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
                if eng
                    .insert_extracted_with_placement(&ex, *logical, *version, dsl, tags, placement)
                    .is_none()
                {
                    return Err(ShardError::Log(format!(
                        "acknowledged shard upsert {logical} was rejected during self-restart"
                    )));
                }
            }
        }
        Self::publish(&eng, &self.snapshot);
        Ok(())
    }

    /// Bulk-ingest, infallibly — the build path uses this directly on a concrete
    /// `LocalShard` (before boxing) so `ClusterEngine::build` stays infallible. The
    /// trait's `ingest_extracted` is the `Result`-wrapped view of the same work.
    pub(crate) fn ingest_local(&self, items: &[PlacedQuery]) -> IngestReport {
        let mut eng = self.lock();
        let report = eng.ingest_extracted(items);
        Self::publish(&eng, &self.snapshot);
        // Bulk ingest writes durable segments WITHOUT riding the translog, so the
        // checkpoint sidecar must learn about them or a self-restart would attach a
        // stale registry and silently lose the bulk (ADR-072). Refresh it here,
        // PRESERVING local_checkpoint — the un-sealed translog tail is unchanged, and
        // advancing it would skip replaying live ops (a false negative).
        self.refresh_sidecar_segments(&eng);
        report
    }

    /// Refresh the durable checkpoint sidecar's segment registry after an
    /// off-translog write (bulk ingest). Best-effort like the engine's degraded
    /// paths: the segments themselves are already durable, so a failed pointer
    /// update is surfaced as a [`DurabilityFailure`](crate::events::EngineEvent)
    /// (data-at-risk: a self-restart before the next successful seal would miss
    /// the bulk) rather than failing the infallible build-path ingest.
    fn refresh_sidecar_segments(&self, eng: &Engine) {
        let Some(dir) = &self.data_dir else { return };
        let emit_fail = |detail: String, error: String| {
            self.emit(&crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::ManifestWrite,
                detail,
                error,
            });
        };
        let prev = match translog::read_sidecar(dir) {
            Ok(c) => c.map_or(0, |c| c.local_checkpoint),
            Err(e) => {
                emit_fail(
                    "reading shard.ckpt to refresh after bulk ingest".into(),
                    e.to_string(),
                );
                return;
            }
        };
        let segment_files = match eng.segment_filenames() {
            Ok(f) => f,
            Err(e) => {
                emit_fail(
                    "collecting segment filenames after bulk ingest".into(),
                    e.to_string(),
                );
                return;
            }
        };
        if let Err(e) = translog::write_sidecar(
            dir,
            &translog::ShardCheckpoint {
                next_seg_id: eng.next_seg_id(),
                local_checkpoint: prev,
                dict_fingerprint: self.dict.fingerprint(),
                segment_files,
                compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
                source_file_name: eng.source_file_name().to_string(),
            },
        ) {
            emit_fail("writing shard.ckpt after bulk ingest".into(), e.to_string());
        }
    }

    /// Lock the engine, recovering the guard if a prior writer panicked: a poisoned
    /// shard mutex must not take down the whole cluster, and the engine state behind
    /// it is still self-consistent (writes are atomic at this layer).
    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, Engine> {
        self.engine.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Republish the lock-free read snapshot after a write.
    pub(super) fn publish(eng: &Engine, slot: &ArcSwap<EngineSnapshot>) {
        slot.store(Arc::new(eng.snapshot()));
    }

    /// The current lock-free read snapshot (an `Arc` clone — no engine lock). Private:
    /// the seam exposes `percolate`, not the snapshot, so a remote shard need not have
    /// one.
    pub(super) fn snapshot(&self) -> Arc<EngineSnapshot> {
        self.snapshot.load_full()
    }

    /// The current lock-free read snapshot, for the local node's `/_metrics` renderer (ADR-091):
    /// one consistent point-in-time view to read `metrics()` / `segment_infos()` / `class_counts()`
    /// from, off the engine write lock. Visible to the `ShardServer` (same crate) that hosts the
    /// metrics endpoint — which is `distributed`-only, so gate it to avoid a dead-code warning in
    /// the lean / server builds.
    #[cfg(feature = "distributed")]
    pub(crate) fn metrics_snapshot(&self) -> Arc<EngineSnapshot> {
        self.snapshot.load_full()
    }

    /// Whether this shard's engine has had every best-effort durability write succeed
    /// ([`Engine::persistence_healthy`]) — checked by the cluster's durable build
    /// before its manifest commit (codex retro-review, ADR-074).
    pub(crate) fn persistence_healthy(&self) -> bool {
        self.lock().persistence_healthy()
    }

    /// An order-independent 128-bit fingerprint over this shard's LIVE query multiset —
    /// `(logical_id, version, dsl, TagId*, typed priority, placement, raw source tags)`, the
    /// document-complete `live_sources_tagged` basis
    /// (memtable +
    /// segments, live copies only) — plus the live count (ADR-097). Two logically-equal copies
    /// fingerprint equal regardless of insertion order, flush boundaries, segment layout, or
    /// compaction history (byte-level segment CRCs cannot say this — equal op streams produce
    /// byte-divergent files by construction). Each entry is canonically encoded
    /// (LE scalars, length-prefixed dsl/tags), the encodings sorted (the multiset canon), then
    /// folded through two independently-seeded FNV-1a streams. Takes the engine lock (the same
    /// enumeration `set_vocab` trusts for completeness); called off the hot path — the ADR-094
    /// fence window, where the alternative is an `O(corpus)` network copy. `distributed`-only.
    /// Errs (fail-toward-copy) when the source enumeration does not cover the engine's live
    /// query count — a source-less / partial store (e.g. a legacy restore whose segments serve
    /// queries `sources.dat` no longer names; codex P1 on this ADR): fingerprinting the partial
    /// enumeration could make divergent shards compare equal and wrongly skip the healing
    /// re-copy, so the caller must fall back to it instead.
    #[cfg(feature = "distributed")]
    pub(crate) fn content_fingerprint128(&self) -> Result<(u64, u64, u64), ShardError> {
        const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        // A second, independent stream seed (the golden-ratio constant XOR'd in) so the two
        // 64-bit halves never collide in lockstep.
        const HI_SEED: u64 = FNV_OFFSET ^ 0x9e37_79b9_7f4a_7c15;
        fn fnv1a64_seeded(seed: u64, bytes: &[u8]) -> u64 {
            let mut h = seed;
            for &b in bytes {
                h ^= u64::from(b);
                h = h.wrapping_mul(0x0100_0000_01b3);
            }
            h
        }
        let (entries, live) = {
            let eng = self.lock();
            // One lock hold = one point-in-time: the enumeration and the live count must agree
            // about the same instant for the completeness cross-check below to mean anything.
            // `num_live_queries` (index-side, tombstone-aware) — NOT `num_queries`, which counts
            // physical entries including dead copies and would spuriously refuse after any
            // in-memtable delete.
            let entries = eng
                .live_source_documents_tagged()
                .map_err(ShardError::SourceUnavailable)?;
            (entries, eng.num_live_queries())
        };
        if entries.len() != live {
            return Err(ShardError::Config(format!(
                "content fingerprint refused: the source enumeration covers {} of {live} live \
                 queries (a source-less or partial store); a fingerprint over it could equate \
                 divergent shards — fall back to the re-copy",
                entries.len()
            )));
        }
        let mut encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(
                |(
                    logical,
                    dsl,
                    version,
                    _source_generation,
                    raw_tags,
                    tag_ids,
                    rank,
                    placement,
                )| {
                    let raw_tag_bytes: usize = raw_tags
                        .iter()
                        .map(|(key, value)| 8 + key.len() + value.len())
                        .sum();
                    let source_suffix_bytes = if raw_tags.is_empty() {
                        0
                    } else {
                        8 + 8 + raw_tag_bytes
                    };
                    let mut e = Vec::with_capacity(
                        8 + 4 + 8 + dsl.len() + 8 + 4 * tag_ids.len() + 8 + source_suffix_bytes,
                    );
                    e.extend_from_slice(&logical.to_le_bytes());
                    e.extend_from_slice(&version.to_le_bytes());
                    e.extend_from_slice(&(dsl.len() as u64).to_le_bytes());
                    e.extend_from_slice(dsl.as_bytes());
                    e.extend_from_slice(&(tag_ids.len() as u64).to_le_bytes());
                    for t in tag_ids {
                        e.extend_from_slice(&t.to_le_bytes());
                    }
                    // Preserve the pre-ADR-108 fingerprint for the overwhelmingly-common all-zero
                    // corpus while still making a typed-priority divergence force peer recovery.
                    if rank.priority != 0 {
                        e.extend_from_slice(&rank.priority.to_le_bytes());
                    }
                    if placement.mode() != crate::ownership::PlacementMode::Standalone {
                        e.extend_from_slice(&placement.generation().get().to_le_bytes());
                        e.extend_from_slice(&placement.num_shards().to_le_bytes());
                        e.push(placement.mode() as u8);
                        e.extend_from_slice(&(placement.positions().len() as u32).to_le_bytes());
                        for position in placement.positions() {
                            e.extend_from_slice(&position.to_le_bytes());
                        }
                    }
                    // Extend the old fingerprint encoding only for source-tagged documents.
                    // Untagged corpora keep their historical fingerprints across this upgrade,
                    // while a source-divergent tagged replica can no longer skip recovery.
                    if !raw_tags.is_empty() {
                        e.extend_from_slice(b"SRCTAGS\0");
                        e.extend_from_slice(&(raw_tags.len() as u64).to_le_bytes());
                        for (key, value) in raw_tags {
                            e.extend_from_slice(&(key.len() as u32).to_le_bytes());
                            e.extend_from_slice(key.as_bytes());
                            e.extend_from_slice(&(value.len() as u32).to_le_bytes());
                            e.extend_from_slice(value.as_bytes());
                        }
                    }
                    e
                },
            )
            .collect();
        // The multiset canon: sort the ENCODINGS (not the tuples), so equal live sets hash
        // equal without any Ord requirement on the entry fields.
        encoded.sort_unstable();
        let mut lo = FNV_OFFSET;
        let mut hi = HI_SEED;
        for e in &encoded {
            lo = fnv1a64_seeded(lo, e);
            hi = fnv1a64_seeded(hi, e);
        }
        Ok((lo, hi, encoded.len() as u64))
    }

    /// Whether any UNEXPIRED peer-recovery retention lease is held (ADR-096) — the `DropShard`
    /// guard: a slot pinned as an in-flight recovery's source is never destroyed. Reaps expired
    /// leases first (mirroring the seal path, so a crashed recovery's stale lease cannot block a
    /// GC drop forever), then reports the survivors. `distributed`-only (its sole caller is the
    /// gRPC `ShardServer`), gated to avoid a dead-code warning in the lean/server builds.
    #[cfg(feature = "distributed")]
    pub(crate) fn has_unexpired_retention_leases(&self) -> bool {
        let mut held = self
            .retention
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(ttl) = self.retention_lease_ttl {
            held.reap_expired(Instant::now(), ttl);
        }
        held.floor().is_some()
    }
}
