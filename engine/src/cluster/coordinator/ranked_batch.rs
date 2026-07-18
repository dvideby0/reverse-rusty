//! ADR-112 distributed bounded ranked title batching: route every title, fan
//! ONE `PercolateTopKBatch`-shaped call per involved shard, merge each title
//! exactly through the shared ADR-110 core, and fetch winner sources for the
//! whole batch under ONE global byte credit.
//!
//! Broad-lane note (the ADR-112 correctness argument): a shard receives
//! `include_broad = true` when ANY title in its sub-batch broad-evaluates
//! there, so its columnar broad pass evaluates broad candidates against every
//! sub-batch title — but per-title `PerTitleUniqueOwner` suppression at the
//! emission boundary keeps the emitted set identical to N single calls (a
//! title whose broad evaluator is elsewhere emits no broad rows here). Only
//! `MatchStats` may differ from N single calls; hits and totals cannot.

use std::time::Instant;

use crate::cluster::shard::{BatchTitleRequest, ShardBatchRankedMatch, ShardError};
use crate::rank::CompiledRankProgram;
use crate::result::{
    TopKAdmissionError, TopKOptions, TotalHits, MAX_RANKED_BATCH_HEAP_ROWS, MAX_RANKED_BATCH_TITLES,
};
use crate::segment::MatchStats;
use crate::util::{fast_map, FastMap};

use super::ranked::{
    check_deadline, merge_title_rows, validate_options, validate_part, ClusterRankedError,
    ClusterRankedHit, TitlePart,
};
use super::ClusterEngine;

/// One title's exact distributed result inside a batch.
#[derive(Clone, Debug)]
pub struct ClusterRankedTitle {
    pub hits: Vec<ClusterRankedHit>,
    pub total_hits: TotalHits,
    /// Shards this title routed to (its own fan-out, not the batch's).
    pub routed_shards: usize,
}

/// Exact bounded distributed batch result before optional source enrichment.
#[derive(Clone, Debug)]
pub struct ClusterBatchRankedMatch {
    /// Per-title results in request order.
    pub titles: Vec<ClusterRankedTitle>,
    pub stats: MatchStats,
    pub rank_stats: crate::rank::RankStats,
    /// Distinct shard calls fanned for the whole batch (each shard is called
    /// once with its whole sub-batch).
    pub fanned_shard_calls: usize,
    /// Total bounded rows received across shards and titles.
    pub shard_rows_received: usize,
    pub shard_result_bytes: u64,
    pub placement_generation: crate::ownership::PlacementGeneration,
    pub num_shards: u32,
}

impl ClusterEngine {
    /// Exact distributed top K for a batch of titles under ONE shared
    /// program/K/threshold/deadline. Each title's winners and honest total are
    /// exactly what [`Self::try_percolate_filtered_top_k`] would return for it;
    /// the batch buys the columnar amortization and the bounded one-call-per-
    /// shard wire, never a different answer.
    pub fn try_percolate_filtered_top_k_batch(
        &self,
        titles: &[impl AsRef<str> + Sync],
        filter: &[(String, Vec<String>)],
        options: TopKOptions,
        program: &CompiledRankProgram,
        deadline: Option<Instant>,
    ) -> Result<ClusterBatchRankedMatch, ClusterRankedError> {
        validate_options(options)?;
        if titles.len() > MAX_RANKED_BATCH_TITLES {
            return Err(ClusterRankedError::Admission(
                TopKAdmissionError::BatchTitlesTooLarge {
                    requested: titles.len(),
                    max: MAX_RANKED_BATCH_TITLES,
                },
            ));
        }
        let requested_rows = (options.size as u64).saturating_mul(titles.len() as u64);
        if requested_rows > MAX_RANKED_BATCH_HEAP_ROWS {
            return Err(ClusterRankedError::Admission(
                TopKAdmissionError::BatchHeapBudgetExceeded {
                    requested_rows,
                    max: MAX_RANKED_BATCH_HEAP_ROWS,
                },
            ));
        }
        check_deadline(deadline)?;

        let include_broad = options.query_scope == crate::result::QueryScope::WithBroad;
        let pred = self.compile_tag_predicate(filter);
        let generation = self.placement_generation();
        let num_shards = self.shards.len();

        // Route every title independently; group titles by shard so each shard
        // is called ONCE with its sub-batch + index-aligned contexts.
        let mut contexts = Vec::with_capacity(titles.len());
        let mut routed_counts = Vec::with_capacity(titles.len());
        let mut per_shard_titles: Vec<Vec<usize>> = (0..num_shards).map(|_| Vec::new()).collect();
        for (index, title) in titles.iter().enumerate() {
            let (targets, broad_eval_shard) = self.route(title.as_ref());
            let context = crate::ownership::OwnershipContext::new(
                generation,
                num_shards as u32,
                targets.iter().map(|&position| position as u32).collect(),
                include_broad.then_some(broad_eval_shard as u32),
            )?;
            for &position in &targets {
                per_shard_titles[position].push(index);
            }
            routed_counts.push(targets.len());
            contexts.push(context);
        }
        let active: Vec<(usize, Vec<usize>)> = per_shard_titles
            .into_iter()
            .enumerate()
            .filter(|(_, indices)| !indices.is_empty())
            .collect();

        let collect_one = |(position, title_indices): &(usize, Vec<usize>)| {
            let requests: Vec<BatchTitleRequest<'_>> = title_indices
                .iter()
                .map(|&index| BatchTitleRequest {
                    title: titles[index].as_ref(),
                    context: &contexts[index],
                })
                .collect();
            // ADR-080 bounded broad fan-out: this shard broad-evaluates only
            // if some sub-batch title selected it as the ONE broad evaluator.
            let shard_broad = include_broad
                && title_indices
                    .iter()
                    .any(|&index| contexts[index].broad_evaluator() == Some(*position as u32));
            self.shards[*position]
                .percolate_top_k_batch_owned(
                    &requests,
                    shard_broad,
                    &pred,
                    program,
                    options,
                    *position as u32,
                    deadline,
                )
                .map(|part| (*position, part))
                .map_err(ClusterRankedError::from)
        };
        let parts: Vec<(usize, ShardBatchRankedMatch)> = if active.len() <= 1 {
            active.iter().map(collect_one).collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            active
                .par_iter()
                .map(collect_one)
                .collect::<Result<_, _>>()?
        };
        check_deadline(deadline)?;

        // Validate every shard's per-title rows, then gather them per title.
        let mut stats = MatchStats::default();
        let mut rank_stats = crate::rank::RankStats::default();
        let mut result_bytes = 0u64;
        let mut per_title_parts: Vec<Vec<TitlePart<'_>>> =
            (0..titles.len()).map(|_| Vec::new()).collect();
        for ((position, title_indices), (_, part)) in active.iter().zip(parts.iter()) {
            if part.titles.len() != title_indices.len() {
                return Err(ClusterRankedError::InvalidShardReply {
                    position: *position,
                    detail: format!(
                        "batch reply carried {} title results for {} requested titles",
                        part.titles.len(),
                        title_indices.len()
                    ),
                });
            }
            stats.merge(part.stats);
            result_bytes = result_bytes.saturating_add(part.result_bytes);
            for (&title_index, title_result) in title_indices.iter().zip(part.titles.iter()) {
                validate_part(
                    *position,
                    options.size,
                    options.track_total_hits_up_to,
                    &title_result.hits,
                    &title_result.total_hits,
                )?;
                rank_stats.evaluations = rank_stats
                    .evaluations
                    .saturating_add(title_result.rank_stats.evaluations);
                rank_stats.heap_replacements = rank_stats
                    .heap_replacements
                    .saturating_add(title_result.rank_stats.heap_replacements);
                per_title_parts[title_index].push(TitlePart {
                    position: *position,
                    hits: &title_result.hits,
                    total: title_result.total_hits,
                });
            }
        }

        let mut out_titles = Vec::with_capacity(titles.len());
        let mut shard_rows_received = 0usize;
        for (index, title_parts) in per_title_parts.into_iter().enumerate() {
            let (hits, total_hits, rows) =
                merge_title_rows(options.size, options.track_total_hits_up_to, &title_parts)?;
            shard_rows_received += rows;
            out_titles.push(ClusterRankedTitle {
                hits,
                total_hits,
                routed_shards: routed_counts[index],
            });
        }
        check_deadline(deadline)?;
        Ok(ClusterBatchRankedMatch {
            titles: out_titles,
            stats,
            rank_stats,
            fanned_shard_calls: active.len(),
            shard_rows_received,
            shard_result_bytes: result_bytes,
            placement_generation: generation,
            num_shards: num_shards as u32,
        })
    }

    /// Fetch current source text for every winner of every batch title under
    /// ONE cumulative byte credit. A logical id may win for several titles
    /// (per-title disjointness is across shards *within* a title): it is
    /// fetched ONCE — sources are version-identical on every owner — and the
    /// credit is charged per DELIVERED occurrence, so the returned enrichment
    /// never exceeds `max_source_bytes` even under cross-title duplication.
    /// The returned outer vector is aligned with `ranked.titles`, each inner
    /// vector with that title's hits.
    pub fn fetch_ranked_sources_batch_bounded(
        &self,
        ranked: &ClusterBatchRankedMatch,
        max_source_bytes: usize,
        deadline: Option<Instant>,
    ) -> Result<Vec<Vec<String>>, ClusterRankedError> {
        check_deadline(deadline)?;
        if self.placement_generation() != ranked.placement_generation
            || self.shards.len() as u32 != ranked.num_shards
        {
            return Err(ClusterRankedError::InvalidShardReply {
                position: 0,
                detail: "placement generation or shard count changed between collect and fetch"
                    .into(),
            });
        }
        // Distinct winners, first-observed owner (any owner serves an
        // identical source), grouped per shard.
        let mut owner_of: FastMap<u64, usize> = fast_map();
        let mut groups: Vec<Vec<u64>> = (0..self.shards.len()).map(|_| Vec::new()).collect();
        for title in &ranked.titles {
            for hit in &title.hits {
                let position = hit.owner_position as usize;
                if position >= groups.len() {
                    return Err(ClusterRankedError::InvalidShardReply {
                        position,
                        detail: "winner names an out-of-range owner".into(),
                    });
                }
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    owner_of.entry(hit.logical_id)
                {
                    entry.insert(position);
                    groups[position].push(hit.logical_id);
                }
            }
        }
        let active: Vec<(usize, Vec<u64>)> = groups
            .into_iter()
            .enumerate()
            .filter(|(_, ids)| !ids.is_empty())
            .collect();

        // Sequential one-credit draining, exactly like the single-title fetch:
        // the remaining credit is handed to one owner group at a time.
        let mut sources: FastMap<u64, String> = fast_map();
        let mut remaining = max_source_bytes;
        for (position, ids) in &active {
            let rows = match self.shards[*position].fetch_matches(ids, remaining, deadline) {
                Err(ShardError::EnrichmentLimit { .. }) => {
                    return Err(ClusterRankedError::EnrichmentLimit {
                        limit: max_source_bytes,
                    });
                }
                other => other.map_err(ClusterRankedError::from)?,
            };
            if rows.len() != ids.len() {
                return Err(ClusterRankedError::InvalidShardReply {
                    position: *position,
                    detail: format!(
                        "source stream returned {} rows for {} requested winners",
                        rows.len(),
                        ids.len()
                    ),
                });
            }
            for (&expected_id, row) in ids.iter().zip(rows) {
                if row.logical_id != expected_id {
                    return Err(ClusterRankedError::InvalidShardReply {
                        position: *position,
                        detail: format!(
                            "source stream returned id {} where {expected_id} was requested",
                            row.logical_id
                        ),
                    });
                }
                let Some(next) = remaining.checked_sub(row.source.len()) else {
                    return Err(ClusterRankedError::EnrichmentLimit {
                        limit: max_source_bytes,
                    });
                };
                remaining = next;
                sources.insert(row.logical_id, row.source);
            }
        }
        check_deadline(deadline)?;

        // Distribute per (title, hit), charging the credit per DELIVERED
        // occurrence: a source fetched once but delivered for three titles
        // spends three times its bytes against the caller's bound.
        let mut delivered = 0usize;
        let mut out = Vec::with_capacity(ranked.titles.len());
        for title in &ranked.titles {
            let mut title_sources = Vec::with_capacity(title.hits.len());
            for hit in &title.hits {
                let source = sources.get(&hit.logical_id).ok_or_else(|| {
                    ClusterRankedError::InvalidShardReply {
                        position: hit.owner_position as usize,
                        detail: format!("source stream omitted winner {}", hit.logical_id),
                    }
                })?;
                delivered = delivered.saturating_add(source.len());
                if delivered > max_source_bytes {
                    return Err(ClusterRankedError::EnrichmentLimit {
                        limit: max_source_bytes,
                    });
                }
                title_sources.push(source.clone());
            }
            out.push(title_sources);
        }
        Ok(out)
    }
}
