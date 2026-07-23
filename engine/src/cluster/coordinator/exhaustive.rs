//! ADR-114 bounded exhaustive collection across ownership-disjoint shards.

use std::sync::{PoisonError, RwLockWriteGuard, TryLockError};
use std::time::{Duration, Instant};

use crate::cluster::shard::ShardError;
use crate::delivery::{
    ChunkSink, ChunkSinkError, DeliveryChecksum, ExhaustiveMatchResult, ExhaustiveSummary,
    MatchChunk, MAX_MATCH_CHUNK_SIZE,
};
use crate::rank::CompiledRankProgram;
use crate::result::QueryScope;
use crate::segment::MatchStats;

use super::ClusterEngine;

/// Terminal metadata for one exact cluster-wide exhaustive stream. The caller
/// may publish it only after every routed shard has succeeded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterExhaustiveMatch {
    pub summary: ExhaustiveSummary,
    pub stats: MatchStats,
    pub routed_shards: usize,
    pub placement_generation: crate::ownership::PlacementGeneration,
    pub num_shards: u32,
}

/// Rewrites each shard-local sequence into one contiguous job sequence while
/// independently accounting for the shard's delivered frames.
struct ResequencingSink<'a> {
    inner: &'a mut dyn ChunkSink,
    next_global: &'a mut u64,
    next_local: u64,
    max_members: usize,
    observed_total: u64,
    observed_checksum: DeliveryChecksum,
    protocol_error: Option<String>,
}

impl ResequencingSink<'_> {
    fn validate(&self, expected: ExhaustiveSummary) -> Result<(), ShardError> {
        if let Some(detail) = &self.protocol_error {
            return Err(ShardError::Protocol(detail.clone()));
        }
        if expected.chunk_count != self.next_local
            || expected.exact_total != self.observed_total
            || expected.checksum != self.observed_checksum
        {
            return Err(ShardError::Protocol(format!(
                "exhaustive shard summary disagrees with delivered chunks: \
                 summary(chunks={}, total={}, checksum={:?}), \
                 observed(chunks={}, total={}, checksum={:?})",
                expected.chunk_count,
                expected.exact_total,
                expected.checksum,
                self.next_local,
                self.observed_total,
                self.observed_checksum
            )));
        }
        Ok(())
    }
}

impl ChunkSink for ResequencingSink<'_> {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        if chunk.sequence != self.next_local {
            let detail = format!(
                "exhaustive shard chunk sequence {} arrived where {} was required",
                chunk.sequence, self.next_local
            );
            self.protocol_error = Some(detail.clone());
            return Err(ChunkSinkError::new(detail));
        }
        if chunk.matches.is_empty() || chunk.matches.len() > self.max_members {
            let detail = format!(
                "exhaustive shard chunk contains {} members; required 1..={}",
                chunk.matches.len(),
                self.max_members
            );
            self.protocol_error = Some(detail.clone());
            return Err(ChunkSinkError::new(detail));
        }

        let forwarded = MatchChunk {
            sequence: *self.next_global,
            // One additional bounded chunk is the price of resequencing
            // ownership-disjoint shard streams without a result-sized buffer.
            matches: chunk.matches.clone(),
        };
        self.inner.send_chunk(&forwarded)?;

        self.next_local = self.next_local.saturating_add(1);
        *self.next_global = self.next_global.saturating_add(1);
        self.observed_total = self
            .observed_total
            .saturating_add(chunk.matches.len() as u64);
        for member in &chunk.matches {
            self.observed_checksum.observe(*member);
        }
        Ok(())
    }

    fn check_cancelled(&mut self) -> Result<(), ChunkSinkError> {
        self.inner.check_cancelled()
    }
}

impl ClusterEngine {
    /// Stream the complete exact match set without materializing it.
    ///
    /// Routed shards run sequentially through one resequencing sink. This keeps
    /// memory `O(chunk_size)`, preserves downstream backpressure, and avoids a
    /// second fan-in queue. No shard or member order is part of the contract.
    #[allow(clippy::too_many_arguments)]
    pub fn try_percolate_filtered_all(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        query_scope: QueryScope,
        program: Option<&CompiledRankProgram>,
        chunk_size: usize,
        deadline: Option<Instant>,
        sink: &mut dyn ChunkSink,
    ) -> Result<ClusterExhaustiveMatch, ShardError> {
        if chunk_size == 0 || chunk_size > MAX_MATCH_CHUNK_SIZE {
            return Err(ShardError::Config(format!(
                "exhaustive chunk size {chunk_size} is outside 1..={MAX_MATCH_CHUNK_SIZE}"
            )));
        }
        // Every incremental shard mutation holds this barrier for its complete
        // fan-out. Taking the exclusive side gives direct library callers the
        // same coherent cross-shard view as the HTTP wrapper: an upsert cannot
        // move between owners while this sequential stream is being read.
        let _view = self.lock_exhaustive_view(sink, deadline)?;
        // A queued partial apply means different shards may hold different live
        // versions of one logical id. Both versions can be ownership-valid for
        // their respective placements, so summing the otherwise-disjoint shard
        // streams could certify a duplicate logical id as an exact completion.
        // Exhaustive delivery cannot repair that overlap in O(chunk) memory:
        // refuse before any provisional member escapes and require convergence.
        self.ensure_exhaustive_converged()?;
        check_deadline(deadline)?;

        let include_broad = query_scope == QueryScope::WithBroad;
        let pred = self.compile_tag_predicate(filter);
        let (targets, broad_eval_shard) = self.route(title);
        let ownership = crate::ownership::OwnershipContext::new(
            self.placement_generation(),
            self.shards.len() as u32,
            targets.iter().map(|&position| position as u32).collect(),
            include_broad.then_some(broad_eval_shard as u32),
        )?;

        let mut next_sequence = 0u64;
        let mut exact_total = 0u64;
        let mut checksum = DeliveryChecksum::default();
        let mut stats = MatchStats::default();
        for &position in &targets {
            // Server callers serialize writes for the full job. Rechecking at
            // every boundary also makes a direct library caller fail closed if
            // a concurrent remote mutation queues a repair mid-stream.
            self.ensure_exhaustive_converged()?;
            check_deadline(deadline)?;
            let mut adapter = ResequencingSink {
                inner: sink,
                next_global: &mut next_sequence,
                next_local: 0,
                max_members: chunk_size,
                observed_total: 0,
                observed_checksum: DeliveryChecksum::default(),
                protocol_error: None,
            };
            let part: ExhaustiveMatchResult = self.shards[position].percolate_all_owned(
                title,
                include_broad && position == broad_eval_shard,
                &pred,
                program,
                chunk_size,
                &ownership,
                position as u32,
                deadline,
                &mut adapter,
            )?;
            adapter.validate(part.summary)?;
            self.ensure_exhaustive_converged()?;
            exact_total = exact_total
                .checked_add(part.summary.exact_total)
                .ok_or_else(|| ShardError::Protocol("exhaustive total overflowed u64".into()))?;
            checksum.merge(part.summary.checksum);
            stats.merge(part.stats);
        }
        self.ensure_exhaustive_converged()?;
        check_deadline(deadline)?;
        stats.matches = u32::try_from(exact_total).unwrap_or(u32::MAX);

        Ok(ClusterExhaustiveMatch {
            summary: ExhaustiveSummary {
                exact_total,
                chunk_count: next_sequence,
                checksum,
            },
            stats,
            routed_shards: targets.len(),
            placement_generation: ownership.generation(),
            num_shards: ownership.num_shards(),
        })
    }

    fn ensure_exhaustive_converged(&self) -> Result<(), ShardError> {
        #[cfg(feature = "distributed")]
        if !self.handoffs.is_empty() && self.coordinator_id.is_none() {
            return Err(ShardError::Protocol(
                "exact exhaustive delivery over remote shards requires an exclusive \
                 coordinator lease; assemble the cluster with \
                 connect_remote_exclusive/connect_replicated_exclusive"
                    .into(),
            ));
        }
        // `pending_repair` covers incremental divergence witnessed by THIS
        // coordinator. The directory authority bit covers two states that have
        // no reconstructable repair journal: a fresh coordinator attached to
        // populated remote shards it cannot enumerate, and an initial bulk load
        // that failed after an ambiguous subset of shard writes. In either case,
        // exact cross-shard disjointness/completeness is unattested even when the
        // incremental repair map is empty. Only a fresh corpus rebuild restores
        // that authority.
        if !self.logical_ids_authoritative() {
            return Err(ShardError::Protocol(
                "exhaustive delivery requires authoritative coordinator convergence state; \
                 the coordinator either attached to populated shards without a live-id \
                 enumeration or an initial bulk ingest failed after ambiguous shard writes; \
                 rebuild fresh shard slots from the authoritative corpus before retrying"
                    .into(),
            ));
        }
        let pending = self.pending_repairs();
        if pending == 0 {
            Ok(())
        } else {
            Err(ShardError::Protocol(format!(
                "exhaustive delivery requires a converged cluster, but {pending} \
                 partial-apply repair(s) are pending; run resync or reopen before retrying"
            )))
        }
    }

    fn lock_exhaustive_view<'a>(
        &'a self,
        sink: &mut dyn ChunkSink,
        deadline: Option<Instant>,
    ) -> Result<RwLockWriteGuard<'a, ()>, ShardError> {
        const POLL: Duration = Duration::from_millis(10);
        loop {
            sink.check_cancelled().map_err(|error| {
                ShardError::Protocol(format!("exhaustive sink failed: {error}"))
            })?;
            check_deadline(deadline)?;
            match self.pit_open_barrier.try_write() {
                Ok(guard) => return Ok(guard),
                Err(TryLockError::Poisoned(error)) => return Ok(PoisonError::into_inner(error)),
                Err(TryLockError::WouldBlock) => {
                    let wait = deadline
                        .and_then(|at| at.checked_duration_since(Instant::now()))
                        .map_or(POLL, |remaining| remaining.min(POLL));
                    if wait.is_zero() {
                        return Err(ShardError::DeadlineExceeded);
                    }
                    std::thread::sleep(wait);
                }
            }
        }
    }
}

fn check_deadline(deadline: Option<Instant>) -> Result<(), ShardError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(ShardError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
