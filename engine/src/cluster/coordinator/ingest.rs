//! `impl ClusterEngine` — the write path: bulk `ingest`, incremental `add_query` /
//! `remove_query`, the shared `apply` / `replay_apply` funnel, placement bucketing, and `flush`.

use crate::cluster::clog::ClusterMutation;
use crate::cluster::shard::ShardError;
use crate::compile::{extract_readonly, Extracted};
use crate::events::{DurabilityOp, EngineEvent};

use super::{placement_of, AddOutcome, ClusterEngine, Target};

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
        // ingest re-indexes from scratch; on a populated cluster it would create duplicate
        // entries. Refuse loudly instead (the doc contract: a freshly assembled cluster).
        if self.num_queries()? > 0 {
            return Err(ShardError::Config(
                "ingest() requires an empty cluster; it re-indexes from scratch — use \
                 add_query for incremental adds"
                    .into(),
            ));
        }
        let entries: Vec<(u64, u32, String)> =
            queries.iter().map(|(l, t)| (*l, 1, t.clone())).collect();
        self.bucket_and_ingest(&entries)?;
        // These bulk adds bypassed the log (they go straight to base segments), so on a
        // durable cluster a checkpoint commits them into the coordinator manifest's
        // per-shard segment registry to survive reopen.
        if self.data_dir.is_some() {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Bucket a set of `(logical, version, dsl)` queries by placement and bulk-ingest one
    /// base segment per shard — the load path for [`Self::ingest`] (a freshly assembled,
    /// e.g. remote, cluster). Compiles read-only against the frozen dict, so placement is
    /// byte-identical to the original build. (Recovery no longer re-ingests; [`Self::open`]
    /// attaches each shard's committed segments instead — ADR-032.)
    fn bucket_and_ingest(&self, entries: &[(u64, u32, String)]) -> Result<(), ShardError> {
        let mut buckets: Vec<Vec<(u64, Extracted, String, u32)>> =
            (0..self.ring.num_shards()).map(|_| Vec::new()).collect();
        let mut lc = String::new();
        for (logical, version, text) in entries {
            let Ok(ast) = crate::dsl::parse(text) else {
                continue;
            };
            let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
            match self.placement(&ex) {
                Target::Reject => {}
                Target::Replicated => buckets[0].push((*logical, ex, text.clone(), *version)),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push((*logical, ex.clone(), text.clone(), *version));
                    }
                }
            }
        }
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.shards[s].ingest_extracted(&bucket)?;
            }
        }
        Ok(())
    }

    /// The placement decision for one compiled query — see the module-level table.
    /// Delegates to the free [`placement_of`] so `build` can bucket the corpus before
    /// the cluster value exists.
    fn placement(&self, ex: &Extracted) -> Target {
        placement_of(&self.dict, &self.ring, ex)
    }

    /// Add one query incrementally (lands in the target shard's memtable). Uses a
    /// read-only compile against the frozen shared dict, so vocabulary not seen at
    /// [`Self::build`] time is dropped (the deferred new-vocabulary limitation).
    ///
    /// WAL-first: the mutation is durably logged BEFORE it is applied to any shard, so a
    /// crash can never leave an acknowledged add that [`Self::open`] would lose. A log
    /// append failure rejects the add (shards untouched) and surfaces a
    /// [`DurabilityFailure`](EngineEvent::DurabilityFailure) — the cluster analogue of
    /// the engine's WAL-first write path (ADR-013).
    pub fn add_query(&self, id: u64, dsl: &str) -> Result<AddOutcome, ShardError> {
        // Reject malformed DSL up front: it carries no replayable mutation, so it must
        // never reach the log (a logged record must parse on replay).
        if let Err(e) = crate::dsl::parse(dsl) {
            return Ok(AddOutcome::RejectedParse(e));
        }
        let m = ClusterMutation::Add {
            logical: id,
            version: 1,
            dsl: dsl.to_string(),
        };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster add_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_add(id, 1, dsl)
    }

    /// Remove a query by logical id. Fans the (idempotent) delete out to every
    /// shard and sums the count — sidestepping any placement journal (a replicated
    /// or any-of query may live on several shards; a re-add may have moved it).
    /// WAL-first, like [`Self::add_query`].
    pub fn remove_query(&self, id: u64) -> Result<usize, ShardError> {
        let m = ClusterMutation::Remove { logical: id };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster remove_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_remove(id)
    }

    /// Apply an ADD to the shards — the state-machine `apply` for adds, shared by the live
    /// write path ([`Self::add_query`], after logging) and log replay ([`Self::open`]).
    /// Re-deriving placement here from the frozen dict makes live and replayed application
    /// byte-identical.
    fn apply_add(&self, id: u64, version: u32, dsl: &str) -> Result<AddOutcome, ShardError> {
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok(AddOutcome::RejectedParse(e)),
        };
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        let outcome = match self.placement(&ex) {
            // Class D is logged-but-unplaceable: a harmless no-op on replay (stored
            // nowhere), matching the caller-visible "rejected, stored nowhere".
            Target::Reject => return Ok(AddOutcome::RejectedClassD),
            Target::Replicated => {
                self.shards[0].insert_extracted(&ex, id, version, dsl)?;
                AddOutcome::Replicated
            }
            Target::Selective(shards) => {
                for &s in &shards {
                    self.shards[s].insert_extracted(&ex, id, version, dsl)?;
                }
                AddOutcome::Placed { shards }
            }
        };
        Ok(outcome)
    }

    /// Apply a REMOVE to the shards — the state-machine `apply` for removes. The shard
    /// memtable/segment liveness is the authority; there is no separate coordinator live
    /// set to keep in sync (the durable base is the per-shard segments — ADR-032).
    fn apply_remove(&self, id: u64) -> Result<usize, ShardError> {
        self.shards
            .iter()
            .map(|s| s.delete_by_logical_id(id))
            .sum::<Result<usize, _>>()
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
            } => {
                self.apply_add(logical, version, &dsl)?;
            }
            ClusterMutation::Remove { logical } => {
                self.apply_remove(logical)?;
            }
        }
        Ok(())
    }

    /// Seal every shard's memtable into an immutable base segment.
    pub fn flush(&self) -> Result<(), ShardError> {
        for s in &self.shards {
            s.flush()?;
        }
        Ok(())
    }
}
