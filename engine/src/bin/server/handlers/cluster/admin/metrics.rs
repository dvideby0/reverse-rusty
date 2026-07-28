//! Coordinator implementation of the strict native metrics scrape contract.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::Response};
use tracing::{error, instrument, warn};

use reverse_rusty::cluster::{ClusterEngine, ShardError, TransportMetricsSnapshot};

use crate::handlers::admin::{encode_metrics, metrics_rejection, MetricsTransport};
use crate::state::ClusterAppState;

struct ClusterMetricsSnapshot {
    total_queries: usize,
    shard_queries: Vec<usize>,
    transport: TransportMetricsSnapshot,
}

/// `GET`/`HEAD /_metrics` — collect every serving-position count on one
/// blocking worker, then publish one internally consistent Prometheus scrape.
#[instrument(skip_all)]
pub(crate) async fn cluster_metrics(
    State(state): State<Arc<ClusterAppState>>,
    transport: MetricsTransport,
) -> Response {
    let (_duration, head) = transport.into_parts();
    let Ok(permit) = Arc::clone(&state.stats_permits).acquire_owned().await else {
        return metrics_rejection(
            &state.prom,
            StatusCode::SERVICE_UNAVAILABLE,
            "metrics_unavailable",
            "metrics collection admission is closed",
            head,
        );
    };

    let worker_state = Arc::clone(&state);
    let worker = tokio::task::spawn_blocking(move || {
        let cluster = worker_state.cluster.read();
        let snapshot = collect_cluster_metrics(&cluster);
        (permit, snapshot)
    });
    match worker.await {
        Ok((stats_permit, Ok(snapshot))) => {
            let _stats_permit = stats_permit;
            state.prom.refresh_cluster_gauges(
                snapshot.total_queries,
                &snapshot.shard_queries,
                &snapshot.transport,
            );
            encode_metrics(&state.prom, head)
        }
        Ok((_stats_permit, Err(source))) => {
            warn!(error = %source, "cluster metrics collection failed");
            metrics_rejection(
                &state.prom,
                StatusCode::SERVICE_UNAVAILABLE,
                "metrics_unavailable",
                "required shard metrics collection failed",
                head,
            )
        }
        Err(join_error) => {
            error!(error = %join_error, "cluster metrics worker failed");
            metrics_rejection(
                &state.prom,
                StatusCode::INTERNAL_SERVER_ERROR,
                "metrics_unavailable",
                "metrics collection worker failed",
                head,
            )
        }
    }
}

fn collect_cluster_metrics(cluster: &ClusterEngine) -> Result<ClusterMetricsSnapshot, ShardError> {
    let shard_queries = cluster.shard_query_counts()?;
    let total_queries = shard_queries
        .iter()
        .copied()
        .fold(0usize, usize::saturating_add);
    Ok(ClusterMetricsSnapshot {
        total_queries,
        shard_queries,
        transport: cluster.transport_metrics(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use reverse_rusty::{cluster::ClusterConfig, Normalizer};

    #[test]
    fn one_count_pass_builds_a_consistent_cluster_snapshot() {
        let config = ClusterConfig {
            num_shards: 3,
            ..Default::default()
        };
        let queries = vec![(7, "2024 acme keyboard".to_string())];
        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &config,
            &queries,
        )
        .expect("cluster");

        let snapshot = collect_cluster_metrics(&cluster).expect("metrics");
        assert_eq!(snapshot.shard_queries.len(), 3);
        assert_eq!(
            snapshot.total_queries,
            snapshot.shard_queries.iter().sum::<usize>()
        );
    }
}
