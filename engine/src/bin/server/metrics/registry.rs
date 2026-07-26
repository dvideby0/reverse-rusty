use super::{
    Counter, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, PrometheusMetrics, Registry,
};

impl PrometheusMetrics {
    pub(crate) fn new() -> Self {
        let registry = Registry::new_custom(Some("reverse_rusty".to_string()), None)
            .expect("failed to create prometheus registry");

        // --- Engine gauges (refreshed on each /_metrics scrape) ---

        let total_queries = IntGauge::with_opts(Opts::new(
            "total_queries",
            "Total queries stored across all segments and memtable",
        ))
        .unwrap();

        let open_pits = IntGauge::with_opts(Opts::new(
            "open_pits",
            "Point-in-time snapshots currently pinned for cursor pagination (ADR-113)",
        ))
        .unwrap();

        let base_segments = IntGauge::with_opts(Opts::new(
            "base_segments",
            "Number of sealed immutable base segments",
        ))
        .unwrap();

        let memtable_entries = IntGauge::with_opts(Opts::new(
            "memtable_entries",
            "Entries currently in the mutable memtable",
        ))
        .unwrap();

        let dict_features = IntGauge::with_opts(Opts::new(
            "dict_features",
            "Distinct features in the shared dictionary",
        ))
        .unwrap();

        let memory_bytes = IntGaugeVec::new(
            Opts::new("memory_bytes", "Heap memory usage by component"),
            &["component"],
        )
        .unwrap();

        let wal_size_bytes = IntGauge::with_opts(Opts::new(
            "wal_size_bytes",
            "Current on-disk size of the write-ahead log in bytes",
        ))
        .unwrap();

        let wal_pending_entries = IntGauge::with_opts(Opts::new(
            "wal_pending_entries",
            "Un-checkpointed WAL entries (mutations not yet in a sealed segment)",
        ))
        .unwrap();

        let would_be_hot = IntGauge::with_opts(Opts::new(
            "would_be_hot",
            "Accepted compiles since process start that would reclassify to the hot tier \
             under the default hot-anchor threshold (Broad-Query Cost Program observe mode)",
        ))
        .unwrap();

        let dedup_bodies_total = IntGauge::with_opts(Opts::new(
            "dedup_bodies_total",
            "Accepted compiles since process start (canonical-body dedup Stage A)",
        ))
        .unwrap();
        let dedup_joined = IntGauge::with_opts(Opts::new(
            "dedup_joined",
            "Accepted compiles that joined an existing per-segment body group (dedup Stage A)",
        ))
        .unwrap();
        let dedup_distinct_bodies_est = IntGauge::with_opts(Opts::new(
            "dedup_distinct_bodies_est",
            "Linear-counting estimate of distinct canonical bodies seen since process start",
        ))
        .unwrap();

        // --- Event counters ---

        let flush_total =
            IntCounter::with_opts(Opts::new("flush_total", "Total number of memtable flushes"))
                .unwrap();

        let flush_entries_total = IntCounter::with_opts(Opts::new(
            "flush_entries_total",
            "Total entries flushed across all flushes",
        ))
        .unwrap();

        let ingest_total = IntCounter::with_opts(Opts::new(
            "ingest_total",
            "Total number of bulk ingest operations",
        ))
        .unwrap();

        let ingest_queries_total = IntCounter::with_opts(Opts::new(
            "ingest_queries_total",
            "Total queries ingested successfully",
        ))
        .unwrap();

        let ingest_rejected = IntCounterVec::new(
            Opts::new("ingest_rejected_total", "Queries rejected during ingest"),
            &["reason"],
        )
        .unwrap();

        let compaction_total = IntCounter::with_opts(Opts::new(
            "compaction_total",
            "Total number of compaction operations",
        ))
        .unwrap();

        let compaction_tombstones_reclaimed = IntCounter::with_opts(Opts::new(
            "compaction_tombstones_reclaimed_total",
            "Tombstones reclaimed by compaction",
        ))
        .unwrap();

        let segment_cleanup_failures_total = IntCounter::with_opts(Opts::new(
            "segment_cleanup_failures_total",
            "Segment files that failed best-effort removal (orphan/stale cleanup)",
        ))
        .unwrap();

        let durability_failures_total = IntCounterVec::new(
            Opts::new(
                "durability_failures_total",
                "Durability/persistence failures by operation (degraded durability — alertable)",
            ),
            &["op"],
        )
        .unwrap();

        let flush_time_seconds_total = Counter::with_opts(Opts::new(
            "flush_time_seconds_total",
            "Cumulative wall-clock seconds spent flushing the memtable into segments",
        ))
        .unwrap();

        let compaction_time_seconds_total = Counter::with_opts(Opts::new(
            "compaction_time_seconds_total",
            "Cumulative wall-clock seconds spent compacting base segments",
        ))
        .unwrap();

        // --- HTTP request metrics ---

        let http_requests_total = IntCounterVec::new(
            Opts::new(
                "http_requests_total",
                "Total HTTP requests by endpoint and status",
            ),
            &["endpoint", "status"],
        )
        .unwrap();

        let http_request_duration = HistogramVec::new(
            HistogramOpts::new(
                "http_request_duration_seconds",
                "HTTP request duration in seconds",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0,
            ]),
            &["endpoint"],
        )
        .unwrap();

        let in_flight_requests = IntGauge::with_opts(Opts::new(
            "in_flight_requests",
            "HTTP requests currently being processed",
        ))
        .unwrap();

        let auth_failures_total = IntCounterVec::new(
            Opts::new(
                "auth_failures_total",
                "Requests rejected by bearer-token auth, by reason (missing/invalid)",
            ),
            &["reason"],
        )
        .unwrap();

        // --- Match metrics ---

        let match_candidates_per_title = Histogram::with_opts(
            HistogramOpts::new(
                "match_candidates_per_title",
                "Candidate queries evaluated per title",
            )
            .buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
            ]),
        )
        .unwrap();

        let match_results_per_title = Histogram::with_opts(
            HistogramOpts::new("match_results_per_title", "Confirmed matches per title")
                .buckets(vec![0.0, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0]),
        )
        .unwrap();

        let slow_queries_total = IntCounter::with_opts(Opts::new(
            "slow_queries_total",
            "Searches exceeding the slow-query threshold",
        ))
        .unwrap();

        let match_cancellations_total = IntCounterVec::new(
            Opts::new(
                "match_cancellations_total",
                "Cooperatively cancelled match work (deadline expired mid-match), by endpoint",
            ),
            &["endpoint"],
        )
        .unwrap();
        let search_permits_in_use = IntGauge::with_opts(Opts::new(
            "search_permits_in_use",
            "Search-concurrency permits currently held (--max-concurrent-searches)",
        ))
        .unwrap();
        let ranked_search_permits_in_use = IntGauge::with_opts(Opts::new(
            "ranked_search_permits_in_use",
            "v2 ranked-search permits currently held",
        ))
        .unwrap();
        let ranked_requests_total = IntCounterVec::new(
            Opts::new(
                "ranked_requests_total",
                "v2 ranked-search requests by outcome and visibility scope",
            ),
            &["outcome", "scope"],
        )
        .unwrap();
        let rank_evaluations_total = IntCounter::with_opts(Opts::new(
            "rank_evaluations_total",
            "Logical-id score evaluations performed by bounded ranking",
        ))
        .unwrap();
        let rank_heap_replacements_total = IntCounter::with_opts(Opts::new(
            "rank_heap_replacements_total",
            "Competitive winner-heap replacements in bounded ranking",
        ))
        .unwrap();
        let rank_total_relation_total = IntCounterVec::new(
            Opts::new(
                "rank_total_relation_total",
                "v2 ranked-search total-hit relation outcomes",
            ),
            &["relation"],
        )
        .unwrap();
        let rank_admission_rejections_total = IntCounterVec::new(
            Opts::new(
                "rank_admission_rejections_total",
                "v2 ranked-search admission rejections by bounded reason",
            ),
            &["reason"],
        )
        .unwrap();
        let rank_source_bytes_total = IntCounter::with_opts(Opts::new(
            "rank_source_bytes_total",
            "Winner source bytes enriched after bounded ranking",
        ))
        .unwrap();
        let rank_true_match_lower_bound_total = IntCounter::with_opts(Opts::new(
            "rank_true_match_lower_bound_total",
            "Sum of exact or thresholded true-match lower bounds reported by v2",
        ))
        .unwrap();
        let rank_shard_rows_received_total = IntCounter::with_opts(Opts::new(
            "rank_shard_rows_received_total",
            "Bounded ranked rows received by coordinators from routed shards",
        ))
        .unwrap();
        let rank_shard_result_bytes_total = IntCounter::with_opts(Opts::new(
            "rank_shard_result_bytes_total",
            "Exact protobuf bytes received in distributed top-k shard replies",
        ))
        .unwrap();
        let rank_enrichment_rejections_total = IntCounter::with_opts(Opts::new(
            "rank_enrichment_rejections_total",
            "Ranked responses rejected by the static winner-enrichment byte limit",
        ))
        .unwrap();
        let exhaustive_chunks_total = IntCounter::with_opts(Opts::new(
            "percolate_stream_chunks_total",
            "Provisional exhaustive match chunks accepted by the job sink",
        ))
        .unwrap();
        let exhaustive_bytes_total = IntCounter::with_opts(Opts::new(
            "percolate_stream_bytes_total",
            "NDJSON bytes accepted by exhaustive job streams, including terminal frames",
        ))
        .unwrap();
        let exhaustive_backpressure_seconds_total = Counter::with_opts(Opts::new(
            "percolate_stream_backpressure_seconds_total",
            "Cumulative time exhaustive workers waited for bounded downstream capacity",
        ))
        .unwrap();
        let exhaustive_jobs = IntGaugeVec::new(
            Opts::new(
                "percolate_jobs",
                "Retained exhaustive jobs by current lifecycle state",
            ),
            &["state"],
        )
        .unwrap();
        let exhaustive_jobs_total = IntCounterVec::new(
            Opts::new(
                "percolate_jobs_total",
                "Exhaustive jobs reaching a terminal outcome",
            ),
            &["outcome"],
        )
        .unwrap();
        let exhaustive_permits_in_use = IntGauge::with_opts(Opts::new(
            "exhaustive_permits_in_use",
            "Dedicated exhaustive-job concurrency permits currently held",
        ))
        .unwrap();

        // --- Broad-lane batch metrics (POST /_mpercolate) ---

        let broad_batches_total = IntCounter::with_opts(Opts::new(
            "broad_batches_total",
            "Broad-lane sub-batches (title chunks) evaluated columnar",
        ))
        .unwrap();

        let broad_postings_scanned_total = IntCounter::with_opts(Opts::new(
            "broad_postings_scanned_total",
            "Broad posting entries scanned (the quantity batch evaluation amortizes)",
        ))
        .unwrap();

        let broad_queries_evaluated_total = IntCounter::with_opts(Opts::new(
            "broad_queries_evaluated_total",
            "Broad queries exact-checked via bitmap evaluation (non pure-anchor)",
        ))
        .unwrap();

        let broad_candidates_total = IntCounter::with_opts(Opts::new(
            "broad_candidates_total",
            "Broad-lane candidate queries retrieved across batches",
        ))
        .unwrap();

        let hot_batches_total = IntCounter::with_opts(Opts::new(
            "hot_batches_total",
            "Hot-tier columnar sub-batches processed (class H, ADR-105)",
        ))
        .unwrap();
        let hot_postings_scanned_total = IntCounter::with_opts(Opts::new(
            "hot_postings_scanned_total",
            "Hot-tier posting entries scanned",
        ))
        .unwrap();
        let hot_queries_evaluated_total = IntCounter::with_opts(Opts::new(
            "hot_queries_evaluated_total",
            "Hot-tier queries bitmap-evaluated by the columnar batch path",
        ))
        .unwrap();
        let hot_candidates_total = IntCounter::with_opts(Opts::new(
            "hot_candidates_total",
            "Hot-tier candidates retrieved",
        ))
        .unwrap();
        // --- Cluster gRPC transport metrics (ADR-085) ---

        let transport_rpc_calls = IntGaugeVec::new(
            Opts::new(
                "transport_rpc_calls",
                "Cluster gRPC RPC calls by method (cumulative; ADR-085)",
            ),
            &["method"],
        )
        .unwrap();
        let transport_rpc_errors = IntGaugeVec::new(
            Opts::new(
                "transport_rpc_errors",
                "Cluster gRPC RPC failures by method, including timeouts (cumulative)",
            ),
            &["method"],
        )
        .unwrap();
        let transport_rpc_timeouts = IntGaugeVec::new(
            Opts::new(
                "transport_rpc_timeouts",
                "Cluster gRPC RPC deadline-exceeded by method (cumulative)",
            ),
            &["method"],
        )
        .unwrap();
        let transport_rpc_retries = IntGaugeVec::new(
            Opts::new(
                "transport_rpc_retries",
                "Cluster gRPC idempotent-read retry attempts by method (cumulative)",
            ),
            &["method"],
        )
        .unwrap();
        let transport_rpc_latency_seconds = GaugeVec::new(
            Opts::new(
                "transport_rpc_latency_seconds",
                "Cumulative cluster gRPC RPC latency in seconds by method",
            ),
            &["method"],
        )
        .unwrap();

        // Per-shard stored-query count (ADR-091), labeled by shard ordinal.
        let cluster_shard_queries = IntGaugeVec::new(
            Opts::new(
                "cluster_shard_queries",
                "Stored queries per shard by ordinal (coordinator view; ADR-091)",
            ),
            &["shard"],
        )
        .unwrap();

        // Register all
        registry.register(Box::new(total_queries.clone())).unwrap();
        registry.register(Box::new(open_pits.clone())).unwrap();
        registry.register(Box::new(base_segments.clone())).unwrap();
        registry
            .register(Box::new(memtable_entries.clone()))
            .unwrap();
        registry.register(Box::new(dict_features.clone())).unwrap();
        registry.register(Box::new(memory_bytes.clone())).unwrap();
        registry.register(Box::new(flush_total.clone())).unwrap();
        registry
            .register(Box::new(flush_entries_total.clone()))
            .unwrap();
        registry.register(Box::new(ingest_total.clone())).unwrap();
        registry
            .register(Box::new(ingest_queries_total.clone()))
            .unwrap();
        registry
            .register(Box::new(ingest_rejected.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_tombstones_reclaimed.clone()))
            .unwrap();
        registry
            .register(Box::new(durability_failures_total.clone()))
            .unwrap();
        registry
            .register(Box::new(segment_cleanup_failures_total.clone()))
            .unwrap();
        registry
            .register(Box::new(http_requests_total.clone()))
            .unwrap();
        registry
            .register(Box::new(http_request_duration.clone()))
            .unwrap();
        registry
            .register(Box::new(match_candidates_per_title.clone()))
            .unwrap();
        registry
            .register(Box::new(match_results_per_title.clone()))
            .unwrap();
        registry
            .register(Box::new(broad_batches_total.clone()))
            .unwrap();
        registry
            .register(Box::new(broad_postings_scanned_total.clone()))
            .unwrap();
        registry
            .register(Box::new(broad_queries_evaluated_total.clone()))
            .unwrap();
        registry
            .register(Box::new(broad_candidates_total.clone()))
            .unwrap();
        registry
            .register(Box::new(hot_batches_total.clone()))
            .unwrap();
        registry
            .register(Box::new(hot_postings_scanned_total.clone()))
            .unwrap();
        registry
            .register(Box::new(hot_queries_evaluated_total.clone()))
            .unwrap();
        registry
            .register(Box::new(hot_candidates_total.clone()))
            .unwrap();
        registry
            .register(Box::new(slow_queries_total.clone()))
            .unwrap();
        registry
            .register(Box::new(match_cancellations_total.clone()))
            .unwrap();
        registry
            .register(Box::new(search_permits_in_use.clone()))
            .unwrap();
        registry
            .register(Box::new(ranked_search_permits_in_use.clone()))
            .unwrap();
        registry
            .register(Box::new(ranked_requests_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_evaluations_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_heap_replacements_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_total_relation_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_admission_rejections_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_source_bytes_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_true_match_lower_bound_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_shard_rows_received_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_shard_result_bytes_total.clone()))
            .unwrap();
        registry
            .register(Box::new(rank_enrichment_rejections_total.clone()))
            .unwrap();
        registry
            .register(Box::new(exhaustive_chunks_total.clone()))
            .unwrap();
        registry
            .register(Box::new(exhaustive_bytes_total.clone()))
            .unwrap();
        registry
            .register(Box::new(exhaustive_backpressure_seconds_total.clone()))
            .unwrap();
        registry
            .register(Box::new(exhaustive_jobs.clone()))
            .unwrap();
        registry
            .register(Box::new(exhaustive_jobs_total.clone()))
            .unwrap();
        registry
            .register(Box::new(exhaustive_permits_in_use.clone()))
            .unwrap();
        registry.register(Box::new(wal_size_bytes.clone())).unwrap();
        registry
            .register(Box::new(wal_pending_entries.clone()))
            .unwrap();
        registry.register(Box::new(would_be_hot.clone())).unwrap();
        registry
            .register(Box::new(dedup_bodies_total.clone()))
            .unwrap();
        registry.register(Box::new(dedup_joined.clone())).unwrap();
        registry
            .register(Box::new(dedup_distinct_bodies_est.clone()))
            .unwrap();
        registry
            .register(Box::new(flush_time_seconds_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_time_seconds_total.clone()))
            .unwrap();
        registry
            .register(Box::new(in_flight_requests.clone()))
            .unwrap();
        registry
            .register(Box::new(auth_failures_total.clone()))
            .unwrap();
        registry
            .register(Box::new(transport_rpc_calls.clone()))
            .unwrap();
        registry
            .register(Box::new(transport_rpc_errors.clone()))
            .unwrap();
        registry
            .register(Box::new(transport_rpc_timeouts.clone()))
            .unwrap();
        registry
            .register(Box::new(transport_rpc_retries.clone()))
            .unwrap();
        registry
            .register(Box::new(transport_rpc_latency_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(cluster_shard_queries.clone()))
            .unwrap();

        Self {
            registry,
            total_queries,
            open_pits,
            base_segments,
            memtable_entries,
            dict_features,
            memory_bytes,
            wal_size_bytes,
            wal_pending_entries,
            would_be_hot,
            dedup_bodies_total,
            dedup_joined,
            dedup_distinct_bodies_est,
            flush_total,
            flush_entries_total,
            ingest_total,
            ingest_queries_total,
            ingest_rejected,
            compaction_total,
            compaction_tombstones_reclaimed,
            segment_cleanup_failures_total,
            durability_failures_total,
            flush_time_seconds_total,
            compaction_time_seconds_total,
            http_requests_total,
            http_request_duration,
            in_flight_requests,
            auth_failures_total,
            match_candidates_per_title,
            match_results_per_title,
            broad_batches_total,
            broad_postings_scanned_total,
            broad_queries_evaluated_total,
            broad_candidates_total,
            hot_batches_total,
            hot_postings_scanned_total,
            hot_queries_evaluated_total,
            hot_candidates_total,
            slow_queries_total,
            match_cancellations_total,
            search_permits_in_use,
            ranked_search_permits_in_use,
            ranked_requests_total,
            rank_evaluations_total,
            rank_heap_replacements_total,
            rank_total_relation_total,
            rank_admission_rejections_total,
            rank_source_bytes_total,
            rank_true_match_lower_bound_total,
            rank_shard_rows_received_total,
            rank_shard_result_bytes_total,
            rank_enrichment_rejections_total,
            exhaustive_chunks_total,
            exhaustive_bytes_total,
            exhaustive_backpressure_seconds_total,
            exhaustive_jobs,
            exhaustive_jobs_total,
            exhaustive_permits_in_use,
            transport_rpc_calls,
            transport_rpc_errors,
            transport_rpc_timeouts,
            transport_rpc_retries,
            transport_rpc_latency_seconds,
            cluster_shard_queries,
        }
    }
}
