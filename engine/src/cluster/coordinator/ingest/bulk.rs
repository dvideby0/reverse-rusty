use super::{
    extract_readonly, ClusterEngine, DurabilityOp, EngineEvent, PlacedQuery, ShardError,
    TaggedEntry, Target,
};

impl ClusterEngine {
    /// Bulk-load queries into an already-built (frozen-dict) cluster — the load path
    /// for a cluster assembled via [`Self::from_parts`] (e.g. a remote cluster), and
    /// the distributed analog of `build`'s pass B. Buckets each query by placement
    /// (compiling read-only against the shared frozen dict) and ingests each bucket
    /// into its shard through the seam. Parse failures and class-D queries are skipped
    /// (mirroring `build`); a shard write error propagates. Requires a freshly assembled
    /// (empty) cluster: it errors with [`ShardError::Config`] if the cluster already holds
    /// queries, rather than silently re-indexing them as duplicates (use
    /// [`Self::add_query`] for incremental adds).
    pub fn ingest(&self, queries: &[(u64, String)]) -> Result<(), ShardError> {
        self.ingest_with_tags(queries, &[])
    }

    /// [`ingest`](Self::ingest) carrying per-query metadata tags (ADR-049/055) — the bulk-load
    /// counterpart to [`build_with_tags`](Self::build_with_tags), for a freshly assembled (e.g.
    /// remote) cluster. `tags` is parallel to `queries`; an empty slice means no query is tagged
    /// (byte-identical to `ingest`). Each shard resolves the raw tags read-only against the shared
    /// frozen tag space, so a later filtered percolate agrees on the `TagId`s.
    pub fn ingest_with_tags(
        &self,
        queries: &[(u64, String)],
        tags: &[Vec<(String, String)>],
    ) -> Result<(), ShardError> {
        // ADR-113: bulk load is a mutation like any other for the PIT-open
        // barrier — a pin fan interleaving mid-load would freeze half a corpus.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Initial bulk load is one exclusive logical-id admission boundary. A
        // concurrent incremental mutation cannot slip between the empty check,
        // directory install, and shard writes.
        let _logical_guards = self.logical_bulk_write_guards();
        // ingest re-indexes from scratch; on a populated cluster it would create duplicate
        // entries. Refuse loudly instead (the doc contract: a freshly assembled cluster).
        if self.num_queries()? > 0 {
            return Err(ShardError::Config(
                "ingest() requires an empty cluster; it re-indexes from scratch — use \
                 add_query for incremental adds"
                    .into(),
            ));
        }
        if tags.iter().any(|t| !t.is_empty()) {
            self.tags_present
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let entries: Vec<TaggedEntry> = queries
            .iter()
            .enumerate()
            .map(|(i, (l, t))| (*l, 1, t.clone(), tags.get(i).cloned().unwrap_or_default()))
            .collect();
        self.bucket_and_ingest(&entries)?;
        // These bulk adds bypassed the log (they go straight to base segments), so on a
        // durable cluster a checkpoint commits them into the coordinator manifest's
        // per-shard segment registry to survive reopen.
        if self.data_dir.is_some() {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Bucket a set of `(logical, version, dsl, tags)` queries by placement and bulk-ingest one
    /// base segment per shard — the load path for [`Self::ingest_with_tags`] (a freshly assembled,
    /// e.g. remote, cluster). Compiles read-only against the frozen dict, so placement is
    /// byte-identical to the original build. (Recovery no longer re-ingests; [`Self::open`]
    /// attaches each shard's committed segments instead — ADR-032.)
    fn bucket_and_ingest(&self, entries: &[TaggedEntry]) -> Result<(), ShardError> {
        let mut buckets: Vec<Vec<PlacedQuery>> =
            (0..self.ring.num_shards()).map(|_| Vec::new()).collect();
        let mut lc = String::new();
        let mut accepted_ids = Vec::with_capacity(entries.len());
        for (logical, version, text, qtags) in entries {
            let Ok(ast) = crate::dsl::parse(text) else {
                continue;
            };
            let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
            let target = self.placement(&ex);
            let placement =
                target.placement(self.placement_generation(), self.shards.len() as u32)?;
            if !matches!(&target, Target::Reject) {
                accepted_ids.push(*logical);
            }
            match target {
                Target::Reject => {}
                Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                    // The broad lane is replicated to every shard (ADR-080).
                    for bucket in &mut buckets {
                        bucket.push(PlacedQuery {
                            logical: *logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version: *version,
                            source_generation: None,
                            tags: qtags.clone(),
                            tag_ids: Vec::new(),
                            rank: crate::rank::RankValues::default(),
                            placement: placement.clone(),
                        });
                    }
                }
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push(PlacedQuery {
                            logical: *logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version: *version,
                            source_generation: None,
                            tags: qtags.clone(),
                            tag_ids: Vec::new(),
                            rank: crate::rank::RankValues::default(),
                            placement: placement.clone(),
                        });
                    }
                }
            }
        }
        super::super::logical_ids::sort_and_check_unique(&mut accepted_ids)?;
        // Reserve the complete semantic corpus BEFORE the first shard mutation.
        // If a remote bulk write fails part-way, retaining these reservations is
        // fail-closed: an incremental Add cannot coexist with a physical row that
        // may already have landed. Retrying ingest on the still-empty cluster may
        // replace this directory with the same corpus and continue.
        self.replace_logical_ids(accepted_ids)?;
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                if let Err(error) = self.shards[s].ingest_extracted(&bucket) {
                    // Unlike incremental writes, this initial base-segment fan-out
                    // has no per-logical repair record. Earlier shards may already
                    // hold their buckets, and a transport failure cannot prove the
                    // current shard applied nothing. Keep all id reservations but
                    // revoke the convergence attestation so exact exhaustive
                    // delivery cannot certify the ambiguous corpus (review finding).
                    self.mark_logical_ids_unconverged();
                    self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::ClusterPartialApply,
                        detail: format!(
                            "bulk ingest failed at shard {s}; cluster convergence is unattested"
                        ),
                        error: error.to_string(),
                    });
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}
