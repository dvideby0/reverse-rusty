//! ADR-110 exact bounded distributed ranking and winner-only source fetch.
//!
//! ADR-109 gives every matched logical id exactly one routed emitting position.
//! Therefore each shard's local K is sufficient for the global K: if a global
//! winner fell below its owner's local K, that one shard alone would contain K
//! globally better hits, a contradiction. The coordinator validates the bounded
//! and disjoint shard rows before applying the shared total order.

use std::time::Instant;

use crate::cluster::shard::{FetchedMatch, ShardError, ShardRankedMatch};
use crate::rank::{CompiledRankProgram, RankProgramError, RankProgramSpec, RankStats};
use crate::result::{
    QueryScope, TopKAdmissionError, TopKOptions, TotalHits, TotalHitsRelation,
    DEFAULT_TRACK_TOTAL_HITS_UP_TO, MAX_TOP_K,
};
use crate::segment::MatchStats;
use crate::util::FastSet;

use super::ClusterEngine;

type FetchRequest = (usize, u64);
type FetchedGroup = (usize, Vec<FetchRequest>, Vec<FetchedMatch>);

/// One global winner plus the logical shard position that owns its phase-two
/// source fetch. Physical replica or handoff endpoints never enter ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRankedHit {
    pub logical_id: u64,
    pub score: i64,
    pub owner_position: u32,
}

/// Exact bounded distributed result before optional source/explain enrichment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRankedMatch {
    pub hits: Vec<ClusterRankedHit>,
    pub total_hits: TotalHits,
    pub stats: MatchStats,
    pub rank_stats: RankStats,
    pub routed_shards: usize,
    pub shard_rows_received: usize,
    /// Exact protobuf bytes received from remote shard replies; zero for an
    /// entirely in-process cluster.
    pub shard_result_bytes: u64,
    /// Phase-one placement identity. Phase two refuses a coordinator generation
    /// or shard-count drift rather than fetching winners from a new layout.
    pub placement_generation: crate::ownership::PlacementGeneration,
    pub num_shards: u32,
}

/// Fail-closed failures from distributed bounded ranking or winner fetch.
#[derive(Clone, Debug)]
pub enum ClusterRankedError {
    Admission(TopKAdmissionError),
    Shard(ShardError),
    DeadlineExceeded,
    InvalidShardReply { position: usize, detail: String },
    DuplicateLogicalId(u64),
}

impl std::fmt::Display for ClusterRankedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(f),
            Self::Shard(error) => error.fmt(f),
            Self::DeadlineExceeded => f.write_str("distributed ranked deadline exceeded"),
            Self::InvalidShardReply { position, detail } => {
                write!(f, "invalid bounded reply from shard {position}: {detail}")
            }
            Self::DuplicateLogicalId(logical) => write!(
                f,
                "ownership violation: logical id {logical} was emitted by multiple shards"
            ),
        }
    }
}

impl std::error::Error for ClusterRankedError {}

impl From<ShardError> for ClusterRankedError {
    fn from(value: ShardError) -> Self {
        match value {
            ShardError::DeadlineExceeded => Self::DeadlineExceeded,
            ShardError::Admission(error) => Self::Admission(error),
            other => Self::Shard(other),
        }
    }
}

impl From<crate::ownership::OwnershipError> for ClusterRankedError {
    fn from(value: crate::ownership::OwnershipError) -> Self {
        Self::Shard(ShardError::OwnershipMismatch(value))
    }
}

impl ClusterEngine {
    /// Compile the fixed typed ranking program once against the coordinator's
    /// authoritative frozen tag space, then fan the integer program to shards.
    pub fn compile_rank_program(
        &self,
        spec: &RankProgramSpec,
    ) -> Result<CompiledRankProgram, RankProgramError> {
        let use_priority = match spec.priority_field.as_deref() {
            None => false,
            Some("priority") => true,
            Some(field) => return Err(RankProgramError::UnsupportedField(field.to_string())),
        };
        let boosts = spec
            .boosts
            .iter()
            .map(|(key, value, weight)| (self.tag_dict.get_or_synthetic(key, value), *weight))
            .collect();
        Ok(CompiledRankProgram::new(use_priority, boosts))
    }

    /// Exact distributed top K. Every required routed shard must succeed; shard
    /// rows are bounded by K, ownership-disjoint, and ordered identically before
    /// the coordinator merges and truncates them.
    pub fn try_percolate_filtered_top_k(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        options: TopKOptions,
        program: &CompiledRankProgram,
        deadline: Option<Instant>,
    ) -> Result<ClusterRankedMatch, ClusterRankedError> {
        validate_options(options)?;
        check_deadline(deadline)?;

        let include_broad = options.query_scope == QueryScope::WithBroad;
        let pred = self.compile_tag_predicate(filter);
        let (targets, broad_eval_shard) = self.route(title);
        let ownership = crate::ownership::OwnershipContext::new(
            self.placement_generation(),
            self.shards.len() as u32,
            targets.iter().map(|&position| position as u32).collect(),
            include_broad.then_some(broad_eval_shard as u32),
        )?;

        let collect_one = |&position: &usize| {
            self.shards[position]
                .percolate_top_k_owned(
                    title,
                    include_broad && position == broad_eval_shard,
                    &pred,
                    program,
                    options,
                    &ownership,
                    position as u32,
                    deadline,
                )
                .map(|part| (position, part))
                .map_err(ClusterRankedError::from)
        };
        let parts: Vec<(usize, ShardRankedMatch)> = if targets.len() <= 1 {
            targets.iter().map(collect_one).collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(collect_one)
                .collect::<Result<_, _>>()?
        };
        check_deadline(deadline)?;

        let mut hits = Vec::with_capacity(options.size.saturating_mul(targets.len()));
        let mut seen = FastSet::default();
        seen.reserve(hits.capacity());
        let mut stats = MatchStats::default();
        let mut rank_stats = RankStats::default();
        let mut exact_sum = 0u64;
        let mut exact_overflow = false;
        let mut all_exact = true;
        let mut result_bytes = 0u64;

        for (position, part) in parts {
            validate_part(position, options.size, &part)?;
            all_exact &= part.total_hits.relation == TotalHitsRelation::Eq;
            match exact_sum.checked_add(part.total_hits.value) {
                Some(sum) => exact_sum = sum,
                None => exact_overflow = true,
            }
            stats.merge(part.stats);
            rank_stats.evaluations = rank_stats
                .evaluations
                .saturating_add(part.rank_stats.evaluations);
            rank_stats.heap_replacements = rank_stats
                .heap_replacements
                .saturating_add(part.rank_stats.heap_replacements);
            result_bytes = result_bytes.saturating_add(part.result_bytes);
            for hit in part.hits {
                if !seen.insert(hit.logical_id) {
                    return Err(ClusterRankedError::DuplicateLogicalId(hit.logical_id));
                }
                hits.push(ClusterRankedHit {
                    logical_id: hit.logical_id,
                    score: hit.score,
                    owner_position: position as u32,
                });
            }
        }

        let shard_rows_received = hits.len();
        let threshold = options.track_total_hits_up_to;
        let total_hits = if all_exact && !exact_overflow && exact_sum <= threshold {
            TotalHits::exact(exact_sum)
        } else {
            TotalHits::lower_bound(threshold)
        };
        hits.sort_unstable_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| a.logical_id.cmp(&b.logical_id))
        });
        hits.truncate(options.size);
        Ok(ClusterRankedMatch {
            hits,
            total_hits,
            stats,
            rank_stats,
            routed_shards: targets.len(),
            shard_rows_received,
            shard_result_bytes: result_bytes,
            placement_generation: ownership.generation(),
            num_shards: ownership.num_shards(),
        })
    }

    /// Fetch current source text only for the finalized global winners. The
    /// returned vector is aligned with `ranked.hits`. Every owner group is one
    /// batched shard call; any missing/malformed group invalidates the response.
    pub fn fetch_ranked_sources(
        &self,
        ranked: &ClusterRankedMatch,
        deadline: Option<Instant>,
    ) -> Result<Vec<String>, ClusterRankedError> {
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
        let mut groups: Vec<Vec<(usize, u64)>> =
            (0..self.shards.len()).map(|_| Vec::new()).collect();
        let mut seen = FastSet::default();
        seen.reserve(ranked.hits.len());
        for (index, hit) in ranked.hits.iter().enumerate() {
            let position = hit.owner_position as usize;
            if position >= groups.len() {
                return Err(ClusterRankedError::InvalidShardReply {
                    position,
                    detail: "winner names an out-of-range owner".into(),
                });
            }
            if !seen.insert(hit.logical_id) {
                return Err(ClusterRankedError::DuplicateLogicalId(hit.logical_id));
            }
            groups[position].push((index, hit.logical_id));
        }

        let active: Vec<(usize, Vec<(usize, u64)>)> = groups
            .into_iter()
            .enumerate()
            .filter(|(_, rows)| !rows.is_empty())
            .collect();
        let fetch_one = |(position, requested): &(usize, Vec<(usize, u64)>)| {
            let ids: Vec<u64> = requested.iter().map(|&(_, id)| id).collect();
            self.shards[*position]
                .fetch_matches(&ids, deadline)
                .map(|fetched| (*position, requested.clone(), fetched))
                .map_err(ClusterRankedError::from)
        };
        let fetched: Vec<FetchedGroup> = if active.len() <= 1 {
            active.iter().map(fetch_one).collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            active.par_iter().map(fetch_one).collect::<Result<_, _>>()?
        };
        check_deadline(deadline)?;

        let mut sources: Vec<Option<String>> = (0..ranked.hits.len()).map(|_| None).collect();
        for (position, requested, rows) in fetched {
            if rows.len() != requested.len() {
                return Err(ClusterRankedError::InvalidShardReply {
                    position,
                    detail: format!(
                        "source stream returned {} rows for {} requested winners",
                        rows.len(),
                        requested.len()
                    ),
                });
            }
            for ((index, expected_id), row) in requested.into_iter().zip(rows) {
                if row.logical_id != expected_id {
                    return Err(ClusterRankedError::InvalidShardReply {
                        position,
                        detail: format!(
                            "source stream returned id {} where {expected_id} was requested",
                            row.logical_id
                        ),
                    });
                }
                sources[index] = Some(row.source);
            }
        }
        sources
            .into_iter()
            .enumerate()
            .map(|(index, source)| {
                source.ok_or_else(|| ClusterRankedError::InvalidShardReply {
                    position: ranked.hits[index].owner_position as usize,
                    detail: format!(
                        "source stream omitted winner {}",
                        ranked.hits[index].logical_id
                    ),
                })
            })
            .collect()
    }

    /// Compile an explanation from a phase-two source using the coordinator's
    /// authoritative normalizer/dictionary. Explanation objects never cross gRPC.
    pub fn explain_ranked_source(
        &self,
        logical_id: u64,
        source: &str,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let mut lc = String::new();
        let compiled = crate::compile::compile_one_readonly(
            source,
            logical_id,
            &self.norm,
            &self.dict,
            &mut lc,
            self.per_shard.hot_anchor_threshold,
        )
        .ok()?;
        Some(crate::explain::explain_match_structured(
            &compiled, title, &self.norm, &self.dict,
        ))
    }
}

fn validate_options(options: TopKOptions) -> Result<(), ClusterRankedError> {
    if options.size > MAX_TOP_K {
        return Err(ClusterRankedError::Admission(
            TopKAdmissionError::SizeTooLarge {
                requested: options.size,
                max: MAX_TOP_K,
            },
        ));
    }
    if options.track_total_hits_up_to > DEFAULT_TRACK_TOTAL_HITS_UP_TO {
        return Err(ClusterRankedError::Admission(
            TopKAdmissionError::TotalHitsThresholdTooLarge {
                requested: options.track_total_hits_up_to,
                max: DEFAULT_TRACK_TOTAL_HITS_UP_TO,
            },
        ));
    }
    Ok(())
}

fn check_deadline(deadline: Option<Instant>) -> Result<(), ClusterRankedError> {
    if deadline.is_some_and(|at| Instant::now() >= at) {
        Err(ClusterRankedError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn validate_part(
    position: usize,
    requested_k: usize,
    part: &ShardRankedMatch,
) -> Result<(), ClusterRankedError> {
    if part.hits.len() > requested_k {
        return Err(ClusterRankedError::InvalidShardReply {
            position,
            detail: format!(
                "returned {} rows for requested K={requested_k}",
                part.hits.len()
            ),
        });
    }
    for pair in part.hits.windows(2) {
        let left = pair[0];
        let right = pair[1];
        let ordered = left.score > right.score
            || (left.score == right.score && left.logical_id < right.logical_id);
        if !ordered {
            return Err(ClusterRankedError::InvalidShardReply {
                position,
                detail: "rows are not strictly ordered by (score desc, logical_id asc)".into(),
            });
        }
    }
    Ok(())
}
