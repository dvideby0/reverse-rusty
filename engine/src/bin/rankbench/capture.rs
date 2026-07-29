use super::{
    fnv_extend, fnv_offset, BoundedCapture, Capture, ClusterCapture, ClusterEngine, Duration,
    Instant, MatchScratch, QueryScope, ScoredSource, TopKOptions,
};

pub(super) struct BatchCapture {
    pub(super) batch_size: usize,
    pub(super) k: usize,
    pub(super) titles: usize,
    pub(super) local_time: Duration,
    pub(super) cluster_time: Duration,
    pub(super) fanned_shard_calls: usize,
    pub(super) shard_rows_received: usize,
    pub(super) shard_result_bytes: u64,
    pub(super) fetch_bytes: usize,
    pub(super) fetch_time: Duration,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn capture_batch(
    snap: &reverse_rusty::EngineSnapshot,
    cluster: &ClusterEngine,
    titles: &[String],
    program: &reverse_rusty::CompiledRankProgram,
    cluster_program: &reverse_rusty::CompiledRankProgram,
    k: usize,
    batch_size: usize,
) -> BatchCapture {
    const THRESHOLD: u64 = 10_000;
    let slice_len = titles.len().min(batch_size);
    let batch_titles = &titles[..slice_len];
    let options = TopKOptions {
        search_after: None,
        size: k,
        track_total_hits_up_to: THRESHOLD,
        query_scope: QueryScope::WithBroad,
    };
    let pred = reverse_rusty::exact::TagPredicate::empty();
    let mut scratch = MatchScratch::new();

    let started = Instant::now();
    let local = snap
        .try_match_titles_batch_top_k(
            batch_titles,
            reverse_rusty::segment::BatchMatchOptions {
                include_broad: true,
                ..reverse_rusty::segment::BatchMatchOptions::default()
            },
            options,
            program,
            &pred,
            None,
        )
        .expect("local batch top k");
    let local_time = started.elapsed();

    let started = Instant::now();
    let distributed = cluster
        .try_percolate_filtered_top_k_batch(batch_titles, &[], options, cluster_program, None)
        .expect("cluster batch top k");
    let cluster_time = started.elapsed();

    for (i, title) in batch_titles.iter().enumerate() {
        let scalar = snap
            .try_match_title_top_k(title, options, program, &pred, &mut scratch, None)
            .expect("scalar bounded reference");
        let expected: Vec<(u64, i64)> = scalar
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        let local_rows: Vec<(u64, i64)> = local.titles[i]
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        assert_eq!(
            local_rows, expected,
            "local batch diverged at K={k} title={i}"
        );
        let cluster_rows: Vec<(u64, i64)> = distributed.titles[i]
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        assert_eq!(
            cluster_rows, expected,
            "cluster batch diverged at K={k} title={i}"
        );
        assert!(
            distributed.titles[i].hits.len() <= k,
            "per-title rows exceed K"
        );
    }

    let fetch_started = Instant::now();
    let sources = cluster
        .fetch_ranked_sources_batch_bounded(&distributed, 16 * 1024 * 1024, None)
        .expect("batch winner fetch");
    let fetch_time = fetch_started.elapsed();
    let fetch_bytes = sources
        .iter()
        .flatten()
        .map(String::len)
        .fold(0usize, usize::saturating_add);

    BatchCapture {
        batch_size,
        k,
        titles: slice_len,
        local_time,
        cluster_time,
        fanned_shard_calls: distributed.fanned_shard_calls,
        shard_rows_received: distributed.shard_rows_received,
        shard_result_bytes: distributed.shard_result_bytes,
        fetch_bytes,
        fetch_time,
    }
}

pub(super) fn capture_bounded(
    snap: &reverse_rusty::EngineSnapshot,
    cluster: &ClusterEngine,
    titles: &[String],
    compatibility_rank: Option<&reverse_rusty::CompiledRankSpec>,
    program: &reverse_rusty::CompiledRankProgram,
    cluster_program: &reverse_rusty::CompiledRankProgram,
    k: usize,
) -> BoundedCapture {
    const THRESHOLD: usize = 10_000;
    let mut scratch = MatchScratch::new();
    let mut oracle_scratch = MatchScratch::new();
    let mut oracle_ids = Vec::new();
    let mut retained = 0usize;
    let mut encoded_bytes = 0usize;
    let mut match_rank_time = Duration::ZERO;
    let mut evaluations = 0u64;
    let mut replacements = 0u64;
    let mut shard_rows_received = 0usize;
    let mut routed_shards = 0usize;
    let mut shard_result_bytes = 0u64;
    let mut collect_merge_time = Duration::ZERO;
    let mut fetch_bytes = 0usize;
    let mut fetch_time = Duration::ZERO;
    for title in titles {
        let expected = compatibility_rank.map(|rank| {
            snap.match_title(title, &mut oracle_scratch, &mut oracle_ids, true);
            let mut rows = snap.rank(&oracle_ids, rank);
            rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            rows.truncate(k);
            rows
        });

        let started = Instant::now();
        let actual = snap
            .try_match_title_top_k(
                title,
                TopKOptions {
                    search_after: None,
                    size: k,
                    track_total_hits_up_to: THRESHOLD as u64,
                    query_scope: QueryScope::WithBroad,
                },
                program,
                &reverse_rusty::exact::TagPredicate::empty(),
                &mut scratch,
                None,
            )
            .expect("bounded ranked match");
        match_rank_time += started.elapsed();
        let rows: Vec<(u64, i64)> = actual
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        if let Some(expected) = &expected {
            assert_eq!(&rows, expected, "bounded result diverged at K={k}");
        }
        retained = retained.saturating_add(rows.len());
        encoded_bytes = encoded_bytes.saturating_add(
            serde_json::to_vec(&rows)
                .expect("serialize bounded rows")
                .len(),
        );
        evaluations = evaluations.saturating_add(actual.rank_stats.evaluations);
        replacements = replacements.saturating_add(actual.rank_stats.heap_replacements);

        let cluster_started = Instant::now();
        let distributed = cluster
            .try_percolate_filtered_top_k(
                title,
                &[],
                TopKOptions {
                    search_after: None,
                    size: k,
                    track_total_hits_up_to: THRESHOLD as u64,
                    query_scope: QueryScope::WithBroad,
                },
                cluster_program,
                None,
            )
            .expect("distributed bounded ranked match");
        collect_merge_time += cluster_started.elapsed();
        let distributed_rows: Vec<(u64, i64)> = distributed
            .hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect();
        assert_eq!(
            distributed_rows, rows,
            "distributed result diverged from local at K={k}"
        );
        assert_eq!(distributed.total_hits, actual.total_hits);
        assert!(
            distributed.shard_rows_received <= k.saturating_mul(distributed.routed_shards),
            "rows_received exceeded K × routed_shards at K={k}"
        );
        shard_rows_received = shard_rows_received.saturating_add(distributed.shard_rows_received);
        routed_shards = routed_shards.saturating_add(distributed.routed_shards);
        shard_result_bytes = shard_result_bytes.saturating_add(distributed.shard_result_bytes);
        let fetch_started = Instant::now();
        let sources = cluster
            .fetch_ranked_sources(&distributed, None)
            .expect("winner fetch");
        fetch_time += fetch_started.elapsed();
        fetch_bytes = fetch_bytes.saturating_add(sources.iter().map(String::len).sum::<usize>());
    }

    // Structural payload bound: K heap rows + K heap-id entries + threshold+1
    // total-id entries. Hash-table bucket/control overhead is allocator-specific,
    // so report the portable payload bytes separately from the entry bound.
    let collector_bound_entries = k
        .saturating_mul(2)
        .saturating_add(THRESHOLD.saturating_add(1));
    let collector_payload_bytes = k
        .saturating_mul(std::mem::size_of::<(u64, i64)>())
        .saturating_add(k.saturating_mul(std::mem::size_of::<u64>()))
        .saturating_add(
            THRESHOLD
                .saturating_add(1)
                .saturating_mul(std::mem::size_of::<u64>()),
        );
    BoundedCapture {
        k,
        retained,
        encoded_bytes,
        match_rank_time,
        evaluations,
        replacements,
        collector_bound_entries,
        collector_payload_bytes,
        shard_rows_received,
        routed_shards,
        shard_result_bytes,
        collect_merge_time,
        fetch_bytes,
        fetch_time,
    }
}

pub(super) fn capture_local(
    snap: &reverse_rusty::EngineSnapshot,
    titles: &[String],
    rank: &reverse_rusty::CompiledRankSpec,
) -> Capture {
    let mut scratch = MatchScratch::new();
    let mut ids = Vec::new();
    let mut capture = Capture {
        match_counts: Vec::with_capacity(titles.len()),
        logical_emissions: 0,
        duplicate_emissions: 0,
        id_bytes: 0,
        score_bytes: 0,
        source_bytes: 0,
        rank_time: Duration::ZERO,
        checksum: fnv_offset(),
    };
    for title in titles {
        let stats = snap.match_title(title, &mut scratch, &mut ids, true);
        capture.match_counts.push(ids.len());
        capture.logical_emissions = capture
            .logical_emissions
            .saturating_add(stats.logical_emissions);
        capture.duplicate_emissions = capture
            .duplicate_emissions
            .saturating_add(stats.duplicate_emissions);
        let started = Instant::now();
        let scored = snap.rank(&ids, rank);
        capture.rank_time += started.elapsed();
        let rows: Vec<ScoredSource> = scored
            .iter()
            .map(|&(id, score)| ScoredSource {
                id,
                score,
                source: snap.get_query_source(id),
            })
            .collect();
        let id_json = serde_json::to_vec(&ids).expect("serialize ids");
        let score_json = serde_json::to_vec(&scored).expect("serialize scores");
        let source_json = serde_json::to_vec(&rows).expect("serialize sources");
        capture.id_bytes += id_json.len();
        capture.score_bytes += score_json.len();
        capture.source_bytes += source_json.len();
        capture.checksum = fnv_extend(capture.checksum, &id_json);
        capture.checksum = fnv_extend(capture.checksum, &score_json);
        capture.checksum = fnv_extend(capture.checksum, &source_json);
    }
    capture
}

pub(super) fn capture_cluster(cluster: &ClusterEngine, titles: &[String]) -> ClusterCapture {
    let mut fanouts = Vec::with_capacity(titles.len());
    let mut logical_emissions = 0u64;
    let mut duplicate_emissions = 0u64;
    let mut checksum = fnv_offset();
    for title in titles {
        let (ids, stats) = cluster
            .percolate_with_stats(title)
            .expect("cluster percolate");
        logical_emissions = logical_emissions.saturating_add(stats.logical_emissions);
        duplicate_emissions = duplicate_emissions.saturating_add(stats.duplicate_emissions);
        fanouts.push(cluster.shard_fanout(title).len());
        checksum = fnv_extend(
            checksum,
            &serde_json::to_vec(&ids).expect("serialize cluster ids"),
        );
    }
    fanouts.sort_unstable();
    ClusterCapture {
        logical_emissions,
        duplicate_emissions,
        fanouts,
        checksum,
    }
}
