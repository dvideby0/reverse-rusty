//! Bounded retained-job registry and single-consumer stream ownership.
//!
//! The registry is the in-memory persistence boundary: event-id reuse,
//! retention pruning, status projection, stream claiming, and cancellation all
//! serialize through the one registry mutex.

use std::sync::Arc;

use super::stream::JobFrame;
use super::{ExhaustiveJobs, JobRecord, JobView, Registry, StartError, StreamError};

impl ExhaustiveJobs {
    pub(super) fn prune_for_capacity(&self, registry: &mut Registry) -> Result<(), StartError> {
        while registry.jobs.len() >= self.config.max_retained {
            let oldest = registry
                .jobs
                .values()
                .filter(|record| record.state.lock().phase.terminal())
                .min_by_key(|record| record.sequence)
                .cloned();
            let Some(oldest) = oldest else {
                return Err(StartError::Capacity);
            };
            let phase = oldest.state.lock().phase;
            registry.jobs.remove(&oldest.id);
            registry.by_event.remove(&oldest.event_id);
            self.prom
                .exhaustive_jobs
                .with_label_values(&[phase.label()])
                .dec();
        }
        Ok(())
    }

    pub(crate) fn status(&self, id: &str) -> Option<JobView> {
        self.registry
            .lock()
            .jobs
            .get(id)
            .map(|record| record.view())
    }

    pub(crate) fn take_stream(
        &self,
        id: &str,
    ) -> Result<tokio::sync::mpsc::Receiver<JobFrame>, StreamError> {
        let record = self
            .registry
            .lock()
            .jobs
            .get(id)
            .cloned()
            .ok_or(StreamError::NotFound)?;
        let receiver = record
            .receiver
            .lock()
            .take()
            .ok_or(StreamError::AlreadyTaken);
        receiver
    }

    pub(crate) fn cancel(&self, id: &str) -> Option<JobView> {
        let record = self.registry.lock().jobs.get(id).cloned()?;
        if !record.state.lock().phase.terminal() {
            record.completion.request_cancel();
        }
        Some(record.view())
    }

    /// Request cooperative cancellation of every retained running job. Shutdown
    /// calls this before taking engine/coordinator write locks so a worker
    /// blocked on an unclaimed bounded stream releases those locks promptly.
    pub(crate) fn cancel_all(&self) -> usize {
        let records: Vec<Arc<JobRecord>> = self.registry.lock().jobs.values().cloned().collect();
        let mut cancelled = 0;
        for record in records {
            if !record.state.lock().phase.terminal() && record.completion.request_cancel() {
                cancelled += 1;
            }
        }
        cancelled
    }
}
