//! ADR-114 in-memory exhaustive-job orchestration.
//!
//! Boundary map:
//! - `lifecycle`: admission, worker execution, and terminal publication;
//! - `registry`: retained state, idempotency, pruning, status, and stream ownership;
//! - `stream`: bounded chunk delivery and the one terminal-transition gate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::Serialize;

use reverse_rusty::delivery::{DeliveryChecksum, ExhaustiveSummary};
use reverse_rusty::QueryScope;

use crate::metrics::PrometheusMetrics;

mod lifecycle;
mod registry;
mod stream;
#[cfg(test)]
mod tests;

use stream::CompletionState;
pub(crate) use stream::JobFrame;

#[derive(Clone, Copy)]
pub(crate) struct ExhaustiveJobConfig {
    pub(crate) threads: usize,
    pub(crate) max_concurrent: usize,
    pub(crate) chunk_size: usize,
    pub(crate) channel_depth: usize,
    pub(crate) max_timeout: Duration,
    pub(crate) max_retained: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobPhase {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl JobPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn terminal(self) -> bool {
        self != Self::Running
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JobExecutionError {
    pub(crate) error_type: &'static str,
    pub(crate) detail: String,
}

impl JobExecutionError {
    pub(crate) fn new(error_type: &'static str, detail: impl Into<String>) -> Self {
        Self {
            error_type,
            detail: detail.into(),
        }
    }

    pub(crate) fn generic(detail: impl Into<String>) -> Self {
        Self::new("exhaustive_job_failed", detail)
    }
}

impl From<String> for JobExecutionError {
    fn from(detail: String) -> Self {
        Self::generic(detail)
    }
}

#[derive(Clone, Serialize)]
pub(crate) struct JobView {
    pub(crate) job_id: String,
    pub(crate) event_id: String,
    pub(crate) state: JobPhase,
    pub(crate) query_scope: QueryScope,
    pub(crate) snapshot_generation: u64,
    pub(crate) created_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completed_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) exact_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chunk_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) checksum: Option<DeliveryChecksum>,
    #[serde(skip)]
    pub(crate) failure_type: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure: Option<String>,
}

struct JobState {
    phase: JobPhase,
    completed_unix_ms: Option<u64>,
    summary: Option<ExhaustiveSummary>,
    failure: Option<JobExecutionError>,
}

pub(crate) struct JobRecord {
    id: String,
    event_id: String,
    request_fingerprint: [u8; 32],
    query_scope: QueryScope,
    snapshot_generation: u64,
    created_unix_ms: u64,
    sequence: u64,
    state: Mutex<JobState>,
    phase: tokio::sync::watch::Sender<JobPhase>,
    cancel: Arc<AtomicBool>,
    completion: CompletionState,
    receiver: Mutex<Option<tokio::sync::mpsc::Receiver<JobFrame>>>,
}

impl JobRecord {
    fn view(&self) -> JobView {
        let state = self.state.lock();
        JobView {
            job_id: self.id.clone(),
            event_id: self.event_id.clone(),
            state: state.phase,
            query_scope: self.query_scope,
            snapshot_generation: self.snapshot_generation,
            created_unix_ms: self.created_unix_ms,
            completed_unix_ms: state.completed_unix_ms,
            exact_total: state.summary.map(|summary| summary.exact_total),
            chunk_count: state.summary.map(|summary| summary.chunk_count),
            checksum: state.summary.map(|summary| summary.checksum),
            failure_type: state.failure.as_ref().map(|error| error.error_type),
            failure: state.failure.as_ref().map(|error| error.detail.clone()),
        }
    }
}

#[derive(Default)]
struct Registry {
    jobs: HashMap<String, Arc<JobRecord>>,
    by_event: HashMap<String, String>,
    next_sequence: u64,
}

pub(crate) struct ExhaustiveJobs {
    config: ExhaustiveJobConfig,
    pool: rayon::ThreadPool,
    permits: Arc<tokio::sync::Semaphore>,
    registry: Mutex<Registry>,
    next_generation: AtomicU64,
    prom: PrometheusMetrics,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StartError {
    Busy,
    Capacity,
    EventConflict,
    InvalidTimeout,
}

pub(crate) struct StartOutcome {
    pub(crate) job: JobView,
    pub(crate) reused: bool,
}

pub(crate) struct DeleteOutcome {
    pub(crate) job: JobView,
    pub(crate) deleted: bool,
}

#[derive(Debug)]
pub(crate) enum StreamError {
    NotFound,
    AlreadyTaken,
}
