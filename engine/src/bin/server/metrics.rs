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
    /// ADR-113: PITs currently pinning a snapshot for cursor pagination.
    pub(crate) open_pits: IntGauge,
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
    pub(crate) rank_shard_rows_received_total: IntCounter,
    pub(crate) rank_shard_result_bytes_total: IntCounter,
    pub(crate) rank_enrichment_rejections_total: IntCounter,
    // ADR-114 exhaustive background delivery.
    pub(crate) exhaustive_chunks_total: IntCounter,
    pub(crate) exhaustive_bytes_total: IntCounter,
    pub(crate) exhaustive_backpressure_seconds_total: Counter,
    pub(crate) exhaustive_jobs: IntGaugeVec,
    pub(crate) exhaustive_jobs_total: IntCounterVec,
    pub(crate) exhaustive_permits_in_use: IntGauge,

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

mod registry;

impl PrometheusMetrics {
    /// Update gauge metrics from an EngineMetrics snapshot.
    pub(crate) fn refresh_gauges(&self, m: &reverse_rusty::events::EngineMetrics) {
        self.total_queries.set(usize_gauge(m.total_queries));
        self.base_segments.set(usize_gauge(m.base_segments));
        self.memtable_entries.set(usize_gauge(m.memtable_entries));
        self.dict_features.set(usize_gauge(m.dict_features));
        self.memory_bytes
            .with_label_values(&["exact"])
            .set(usize_gauge(m.exact_bytes));
        self.memory_bytes
            .with_label_values(&["index"])
            .set(usize_gauge(m.index_bytes));
        self.memory_bytes
            .with_label_values(&["filter"])
            .set(usize_gauge(m.filter_bytes));
        self.wal_size_bytes.set(u64_gauge(m.wal_size_bytes));
        self.wal_pending_entries
            .set(u64_gauge(m.wal_pending_entries));
        self.would_be_hot.set(u64_gauge(m.would_be_hot));
        self.dedup_bodies_total.set(u64_gauge(m.bodies_total));
        self.dedup_joined.set(u64_gauge(m.dup_joined));
        self.dedup_distinct_bodies_est
            .set(u64_gauge(m.distinct_bodies_est));
    }

    /// Atomically refresh the coordinator gauges while the metrics handler
    /// retains shared stats admission. A failed collection never reaches here.
    pub(crate) fn refresh_cluster_gauges(
        &self,
        total_queries: usize,
        shard_queries: &[usize],
        transport: &reverse_rusty::cluster::TransportMetricsSnapshot,
    ) {
        self.total_queries.set(usize_gauge(total_queries));
        self.observe_shard_queries(shard_queries);
        self.observe_transport(transport);
    }

    /// Refresh the cluster gRPC transport gauges (ADR-085).
    fn observe_transport(&self, snap: &reverse_rusty::cluster::TransportMetricsSnapshot) {
        for m in &snap.methods {
            self.transport_rpc_calls
                .with_label_values(&[m.method])
                .set(u64_gauge(m.calls));
            self.transport_rpc_errors
                .with_label_values(&[m.method])
                .set(u64_gauge(m.errors));
            self.transport_rpc_timeouts
                .with_label_values(&[m.method])
                .set(u64_gauge(m.timeouts));
            self.transport_rpc_retries
                .with_label_values(&[m.method])
                .set(u64_gauge(m.retries));
            self.transport_rpc_latency_seconds
                .with_label_values(&[m.method])
                .set(m.latency_nanos_total as f64 / 1e9);
        }
    }

    /// Replace the per-position query series so a shrink cannot leave removed
    /// label values in later scrapes.
    fn observe_shard_queries(&self, counts: &[usize]) {
        self.cluster_shard_queries.reset();
        for (shard, count) in counts.iter().enumerate() {
            self.cluster_shard_queries
                .with_label_values(&[&shard.to_string()])
                .set(usize_gauge(*count));
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

fn usize_gauge(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn u64_gauge(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
