//! Prometheus metrics: the registry of all engine/HTTP/match gauges and counters,
//! plus the [`EngineEvent`] → counter bridge wired into the engine observer in `main`.
//! Gauges are refreshed from an `EngineMetrics` snapshot on each `/_metrics` scrape;
//! counters are incremented as events fire.

use prometheus::{
    Counter, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry,
};

use reverse_rusty::events::EngineEvent;

#[derive(Clone)]
pub(crate) struct PrometheusMetrics {
    pub(crate) registry: Registry,

    // Engine gauge metrics (scraped from EngineMetrics snapshot)
    pub(crate) total_queries: IntGauge,
    pub(crate) base_segments: IntGauge,
    pub(crate) memtable_entries: IntGauge,
    pub(crate) dict_features: IntGauge,
    pub(crate) memory_bytes: IntGaugeVec,
    pub(crate) wal_size_bytes: IntGauge,
    pub(crate) wal_pending_entries: IntGauge,
    pub(crate) would_be_hot: IntGauge,
    pub(crate) dedup_bodies_total: IntGauge,
    pub(crate) dedup_joined: IntGauge,
    pub(crate) dedup_distinct_bodies_est: IntGauge,

    // Cumulative counters (incremented via EngineEvent observer)
    pub(crate) flush_total: IntCounter,
    pub(crate) flush_entries_total: IntCounter,
    pub(crate) ingest_total: IntCounter,
    pub(crate) ingest_queries_total: IntCounter,
    pub(crate) ingest_rejected: IntCounterVec,
    pub(crate) compaction_total: IntCounter,
    pub(crate) compaction_tombstones_reclaimed: IntCounter,
    pub(crate) segment_cleanup_failures_total: IntCounter,
    /// Durability/persistence failures, labeled by `op` (e.g. `segment_write`,
    /// `manifest_write`, `wal_append`). Alert on this — a nonzero rate means
    /// durability is degraded. See `EngineEvent::DurabilityFailure`.
    pub(crate) durability_failures_total: IntCounterVec,
    pub(crate) flush_time_seconds_total: Counter,
    pub(crate) compaction_time_seconds_total: Counter,

    // Request metrics
    pub(crate) http_requests_total: IntCounterVec,
    pub(crate) http_request_duration: HistogramVec,
    pub(crate) in_flight_requests: IntGauge,
    /// Requests rejected by the bearer-token gate (ADR-062), labeled by reason
    /// (`missing` = no credentials presented, `invalid` = wrong token). A
    /// sustained rate means a misconfigured client — or someone probing.
    pub(crate) auth_failures_total: IntCounterVec,

    // Match metrics
    pub(crate) match_candidates_per_title: Histogram,
    pub(crate) match_results_per_title: Histogram,

    // Broad-lane batch metrics (POST /_mpercolate columnar evaluation, ADR-026).
    // Cumulative across requests; the amortization shows as broad_postings_scanned
    // rising far slower than broad_candidates as batch size grows.
    pub(crate) broad_batches_total: IntCounter,
    pub(crate) broad_postings_scanned_total: IntCounter,
    pub(crate) broad_queries_evaluated_total: IntCounter,
    pub(crate) broad_candidates_total: IntCounter,
    pub(crate) hot_batches_total: IntCounter,
    pub(crate) hot_postings_scanned_total: IntCounter,
    pub(crate) hot_queries_evaluated_total: IntCounter,
    pub(crate) hot_candidates_total: IntCounter,

    // Slow query counter
    pub(crate) slow_queries_total: IntCounter,
    /// Cooperative match cancellations (ADR-099), by endpoint — incremented inside the
    /// blocking closure when armed match work abandons itself at a deadline boundary,
    /// so it counts even after the handler already answered 408. The "work actually
    /// stopped" signal, distinct from `http_requests_total{status="408"}` (which also
    /// counts un-armed response-deadline timeouts).
    pub(crate) match_cancellations_total: IntCounterVec,
    /// Search permits currently held (ADR-099) — 0 permanently when
    /// `--max-concurrent-searches` is unset.
    pub(crate) search_permits_in_use: IntGauge,
    pub(crate) ranked_search_permits_in_use: IntGauge,
    pub(crate) ranked_requests_total: IntCounterVec,
    pub(crate) rank_evaluations_total: IntCounter,
    pub(crate) rank_heap_replacements_total: IntCounter,
    pub(crate) rank_total_relation_total: IntCounterVec,
    pub(crate) rank_admission_rejections_total: IntCounterVec,
    pub(crate) rank_source_bytes_total: IntCounter,
    pub(crate) rank_true_match_lower_bound_total: IntCounter,

    // Cluster gRPC transport metrics (ADR-085), set on each /_metrics scrape from the
    // coordinator's TransportMetrics snapshot; labeled by RPC `method`. Cumulative values in
    // gauges (the pull-on-scrape pattern of the engine gauges above). All-zero in single-node
    // mode and for an in-process cluster.
    pub(crate) transport_rpc_calls: IntGaugeVec,
    pub(crate) transport_rpc_errors: IntGaugeVec,
    pub(crate) transport_rpc_timeouts: IntGaugeVec,
    pub(crate) transport_rpc_retries: IntGaugeVec,
    pub(crate) transport_rpc_latency_seconds: GaugeVec,

    // Per-shard stored-query count, labeled by `shard` ordinal (ADR-091). Set on each cluster-mode
    // `/_metrics` scrape from `ClusterEngine::shard_query_counts`, so the coordinator exposes the
    // cluster-wide per-shard distribution without scraping each shard pod. Absent in single-node mode.
    pub(crate) cluster_shard_queries: IntGaugeVec,
}

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
            transport_rpc_calls,
            transport_rpc_errors,
            transport_rpc_timeouts,
            transport_rpc_retries,
            transport_rpc_latency_seconds,
            cluster_shard_queries,
        }
    }

    /// Update gauge metrics from an EngineMetrics snapshot.
    pub(crate) fn refresh_gauges(&self, m: &reverse_rusty::events::EngineMetrics) {
        self.total_queries.set(m.total_queries as i64);
        self.base_segments.set(m.base_segments as i64);
        self.memtable_entries.set(m.memtable_entries as i64);
        self.dict_features.set(m.dict_features as i64);
        self.memory_bytes
            .with_label_values(&["exact"])
            .set(m.exact_bytes as i64);
        self.memory_bytes
            .with_label_values(&["index"])
            .set(m.index_bytes as i64);
        self.memory_bytes
            .with_label_values(&["filter"])
            .set(m.filter_bytes as i64);
        self.wal_size_bytes.set(m.wal_size_bytes as i64);
        self.wal_pending_entries.set(m.wal_pending_entries as i64);
        self.would_be_hot.set(m.would_be_hot as i64);
        self.dedup_bodies_total.set(m.bodies_total as i64);
        self.dedup_joined.set(m.dup_joined as i64);
        self.dedup_distinct_bodies_est
            .set(m.distinct_bodies_est as i64);
    }

    /// Refresh the cluster gRPC transport gauges (ADR-085) from a coordinator snapshot.
    /// Called on each cluster-mode `/_metrics` scrape; a single-node server never calls it,
    /// and an in-process cluster's snapshot is all-zero, so the series simply read 0.
    pub(crate) fn observe_transport(
        &self,
        snap: &reverse_rusty::cluster::TransportMetricsSnapshot,
    ) {
        for m in &snap.methods {
            self.transport_rpc_calls
                .with_label_values(&[m.method])
                .set(m.calls as i64);
            self.transport_rpc_errors
                .with_label_values(&[m.method])
                .set(m.errors as i64);
            self.transport_rpc_timeouts
                .with_label_values(&[m.method])
                .set(m.timeouts as i64);
            self.transport_rpc_retries
                .with_label_values(&[m.method])
                .set(m.retries as i64);
            self.transport_rpc_latency_seconds
                .with_label_values(&[m.method])
                .set(m.latency_nanos_total as f64 / 1e9);
        }
    }

    /// Refresh the per-shard stored-query gauge (ADR-091) from `ClusterEngine::shard_query_counts`.
    /// Called on each cluster-mode `/_metrics` scrape; `counts[i]` is shard `i`'s query count.
    pub(crate) fn observe_shard_queries(&self, counts: &[usize]) {
        for (shard, count) in counts.iter().enumerate() {
            self.cluster_shard_queries
                .with_label_values(&[&shard.to_string()])
                .set(*count as i64);
        }
    }

    /// Handle an EngineEvent — increment counters. Called from the observer.
    pub(crate) fn observe_event(&self, event: &EngineEvent) {
        match event {
            EngineEvent::Flush {
                entries,
                duration_secs,
                ..
            } => {
                self.flush_total.inc();
                self.flush_entries_total.inc_by(*entries as u64);
                self.flush_time_seconds_total.inc_by(*duration_secs);
            }
            EngineEvent::Ingest {
                ingested,
                rejected_parse,
                rejected_class_d,
                ..
            } => {
                self.ingest_total.inc();
                self.ingest_queries_total.inc_by(*ingested as u64);
                if *rejected_parse > 0 {
                    self.ingest_rejected
                        .with_label_values(&["parse"])
                        .inc_by(*rejected_parse as u64);
                }
                if *rejected_class_d > 0 {
                    self.ingest_rejected
                        .with_label_values(&["class_d"])
                        .inc_by(*rejected_class_d as u64);
                }
            }
            EngineEvent::Compaction {
                report,
                duration_secs,
                ..
            } => {
                self.compaction_total.inc();
                self.compaction_tombstones_reclaimed
                    .inc_by(report.tombstones_reclaimed as u64);
                self.compaction_time_seconds_total.inc_by(*duration_secs);
            }
            EngineEvent::SegmentCleanupFailed { .. } => {
                self.segment_cleanup_failures_total.inc();
            }
            EngineEvent::DurabilityFailure { op, .. } => {
                self.durability_failures_total
                    .with_label_values(&[op.as_str()])
                    .inc();
            }
        }
    }
}
