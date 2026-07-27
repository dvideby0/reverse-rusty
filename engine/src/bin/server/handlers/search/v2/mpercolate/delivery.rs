use super::{
    assemble_slots, BatchDelivered, BatchSpec, DeliveryError, HitSource, Instant, RankedHitBody,
    SlotDelivered,
};

pub(super) trait RankedBatchClusterRead {
    fn top_k_batch(
        &self,
        titles: &[String],
        filter: &[(String, Vec<String>)],
        options: reverse_rusty::TopKOptions,
        program: &reverse_rusty::CompiledRankProgram,
        deadline: Instant,
    ) -> Result<
        reverse_rusty::cluster::ClusterBatchRankedMatch,
        reverse_rusty::cluster::ClusterRankedError,
    >;

    fn fetch_sources_batch(
        &self,
        ranked: &reverse_rusty::cluster::ClusterBatchRankedMatch,
        enrichment_limit: usize,
        deadline: Instant,
    ) -> Result<Vec<Vec<String>>, reverse_rusty::cluster::ClusterRankedError>;
}

macro_rules! impl_ranked_batch_cluster_read {
    ($type:ty) => {
        impl RankedBatchClusterRead for $type {
            fn top_k_batch(
                &self,
                titles: &[String],
                filter: &[(String, Vec<String>)],
                options: reverse_rusty::TopKOptions,
                program: &reverse_rusty::CompiledRankProgram,
                deadline: Instant,
            ) -> Result<
                reverse_rusty::cluster::ClusterBatchRankedMatch,
                reverse_rusty::cluster::ClusterRankedError,
            > {
                self.try_percolate_filtered_top_k_batch(
                    titles,
                    filter,
                    options,
                    program,
                    Some(deadline),
                )
            }

            fn fetch_sources_batch(
                &self,
                ranked: &reverse_rusty::cluster::ClusterBatchRankedMatch,
                enrichment_limit: usize,
                deadline: Instant,
            ) -> Result<Vec<Vec<String>>, reverse_rusty::cluster::ClusterRankedError> {
                self.fetch_ranked_sources_batch_bounded(ranked, enrichment_limit, Some(deadline))
            }
        }
    };
}

impl_ranked_batch_cluster_read!(reverse_rusty::cluster::ClusterEngine);
impl_ranked_batch_cluster_read!(reverse_rusty::cluster::ClusterReadView<'_>);

/// Single-node kernel: the columnar batch entry + distinct-winner enrichment
/// under the fail-closed budget, charged per DELIVERED occurrence (the same
/// rule as the cluster batch fetch).
pub(super) fn local_batch_delivery(
    snap: &reverse_rusty::EngineSnapshot,
    program: &reverse_rusty::CompiledRankProgram,
    predicate: &reverse_rusty::exact::TagPredicate,
    spec: &BatchSpec<'_>,
) -> Result<BatchDelivered, DeliveryError<reverse_rusty::RankedMatchError>> {
    let BatchSpec {
        titles,
        options,
        include_source,
        enrichment_limit,
        deadline,
    } = *spec;
    let cfg = snap.config();
    let batch_opts = reverse_rusty::segment::BatchMatchOptions {
        include_broad: options.query_scope == reverse_rusty::QueryScope::WithBroad,
        broad_batch_size: cfg.broad_batch_size,
        broad_strategy: if cfg.broad_columnar {
            reverse_rusty::segment::BroadStrategy::Columnar
        } else {
            reverse_rusty::segment::BroadStrategy::Inline
        },
        broad_materialize: cfg.broad_materialize,
        broad_prefilter: cfg.broad_prefilter,
    };
    let ranked = snap
        .try_match_titles_batch_top_k(
            titles,
            batch_opts,
            options,
            program,
            predicate,
            Some(deadline),
        )
        .map_err(DeliveryError::Backend)?;
    let mut rank_stats = reverse_rusty::RankStats::default();
    let mut slots = Vec::with_capacity(ranked.titles.len());
    for title in &ranked.titles {
        rank_stats.evaluations = rank_stats
            .evaluations
            .saturating_add(title.rank_stats.evaluations);
        rank_stats.heap_replacements = rank_stats
            .heap_replacements
            .saturating_add(title.rank_stats.heap_replacements);
        slots.push((
            title
                .hits
                .iter()
                .map(|hit| (hit.logical_id, hit.score))
                .collect::<Vec<_>>(),
            title.total_hits,
            1usize,
        ));
    }
    let mut sources = std::collections::HashMap::new();
    let mut delivered = 0usize;
    if include_source {
        // The cluster batch-fetch rule, mirrored locally: each DISTINCT winner
        // is fetched once under the running credit, and every delivered
        // occurrence is then charged against the same limit (a source shared
        // by three slots spends three times its bytes).
        let mut fetch_remaining = enrichment_limit;
        for (rows, _, _) in &slots {
            for &(logical_id, _) in rows {
                if Instant::now() >= deadline {
                    return Err(DeliveryError::Deadline);
                }
                if sources.contains_key(&logical_id) {
                    continue;
                }
                let source = match snap.get_query_source_bounded(logical_id, fetch_remaining) {
                    Ok(Some(source)) => source,
                    Ok(None) => return Err(DeliveryError::SourceUnavailable(logical_id)),
                    Err(_over_credit) => return Err(DeliveryError::EnrichmentLimit),
                };
                fetch_remaining = fetch_remaining.saturating_sub(source.len());
                sources.insert(logical_id, source);
            }
        }
        for (rows, _, _) in &slots {
            for &(logical_id, _) in rows {
                let bytes = sources
                    .get(&logical_id)
                    .map(String::len)
                    .ok_or(DeliveryError::SourceUnavailable(logical_id))?;
                delivered = delivered.saturating_add(bytes);
                if delivered > enrichment_limit {
                    return Err(DeliveryError::EnrichmentLimit);
                }
            }
        }
    }
    let source_bytes = delivered;
    let slots = assemble_slots(slots, include_source, &sources, deadline)?;
    if Instant::now() >= deadline {
        return Err(DeliveryError::Deadline);
    }
    Ok(BatchDelivered {
        slots,
        rank_stats,
        source_bytes,
        shard_rows_received: 0,
        shard_result_bytes: 0,
    })
}

/// Coordinator kernel: the one-call-per-shard batch fan + the union winner
/// fetch under the same ONE credit.
pub(super) fn cluster_batch_delivery<C: RankedBatchClusterRead>(
    cluster: &C,
    program: &reverse_rusty::CompiledRankProgram,
    filter: &[(String, Vec<String>)],
    spec: &BatchSpec<'_>,
) -> Result<BatchDelivered, DeliveryError<reverse_rusty::cluster::ClusterRankedError>> {
    let BatchSpec {
        titles,
        options,
        include_source,
        enrichment_limit,
        deadline,
    } = *spec;
    let ranked = cluster
        .top_k_batch(titles, filter, options, program, deadline)
        .map_err(DeliveryError::Backend)?;
    let per_slot_sources = if include_source {
        cluster
            .fetch_sources_batch(&ranked, enrichment_limit, deadline)
            .map_err(|error| match error {
                reverse_rusty::cluster::ClusterRankedError::EnrichmentLimit { .. } => {
                    DeliveryError::EnrichmentLimit
                }
                other => DeliveryError::Backend(other),
            })?
    } else {
        Vec::new()
    };
    let source_bytes = per_slot_sources
        .iter()
        .flatten()
        .map(String::len)
        .fold(0usize, usize::saturating_add);
    let mut slots = Vec::with_capacity(ranked.titles.len());
    for (index, title) in ranked.titles.iter().enumerate() {
        if Instant::now() >= deadline {
            return Err(DeliveryError::Deadline);
        }
        let mut hits = Vec::with_capacity(title.hits.len());
        for (hit_index, hit) in title.hits.iter().enumerate() {
            let source = if include_source {
                Some(HitSource {
                    query: per_slot_sources
                        .get(index)
                        .and_then(|sources| sources.get(hit_index))
                        .cloned()
                        .ok_or(DeliveryError::SourceUnavailable(hit.logical_id))?,
                })
            } else {
                None
            };
            hits.push(RankedHitBody {
                _id: hit.logical_id,
                _score: hit.score,
                _source: source,
                _explanation: None,
            });
        }
        slots.push(SlotDelivered {
            hits,
            total_hits: title.total_hits,
            routed_shards: title.routed_shards,
        });
    }
    Ok(BatchDelivered {
        slots,
        rank_stats: ranked.rank_stats,
        source_bytes,
        shard_rows_received: ranked.shard_rows_received,
        shard_result_bytes: ranked.shard_result_bytes,
    })
}
