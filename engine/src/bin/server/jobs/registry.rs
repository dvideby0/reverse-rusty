//! Bounded retained-job registry and single-consumer stream ownership.
//!
//! The registry is the in-memory persistence boundary: event-id reuse,
//! retention pruning, status projection, stream claiming, and cancellation all
//! serialize through the one registry mutex.

use std::sync::Arc;

use super::stream::JobFrame;
use super::{DeleteOutcome, ExhaustiveJobs, JobRecord, JobView, Registry, StartError, StreamError};

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

    #[cfg(test)]
    pub(crate) fn status(&self, id: &str) -> Option<JobView> {
        self.registry
            .lock()
            .jobs
            .get(id)
            .map(|record| record.view())
    }

    /// Return immediately for a terminal record or wait at most `timeout` for
    /// the retained record's one terminal publication. The record is cloned
    /// before waiting, so count-based registry pruning cannot turn an accepted
    /// status poll into a spurious not-found response.
    pub(crate) async fn wait_status(
        &self,
        id: &str,
        timeout: std::time::Duration,
    ) -> Option<JobView> {
        let record = self.registry.lock().jobs.get(id).cloned()?;
        let mut phase = record.phase.subscribe();
        let deadline = tokio::time::Instant::now().checked_add(timeout);
        loop {
            let view = record.view();
            if view.state.terminal() || timeout.is_zero() {
                return Some(view);
            }
            let Some(deadline) = deadline else {
                return Some(view);
            };
            match tokio::time::timeout_at(deadline, phase.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return Some(record.view()),
            }
        }
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

    #[cfg(test)]
    pub(crate) fn cancel(&self, id: &str) -> Option<JobView> {
        let record = self.registry.lock().jobs.get(id).cloned()?;
        if !record.state.lock().phase.terminal() {
            record.completion.request_cancel();
        }
        Some(record.view())
    }

    /// Cancel a running job or delete a terminal retained result.
    ///
    /// Terminal removal and event-id release are one registry-serialized
    /// transition, so a concurrent admission cannot reuse the event id while
    /// the old job is still addressable. A running record remains retained
    /// while cooperative cancellation is published and can be polled until it
    /// reaches a terminal phase; a later DELETE removes it.
    pub(crate) fn delete(&self, id: &str) -> Option<DeleteOutcome> {
        let mut registry = self.registry.lock();
        let record = registry.jobs.get(id).cloned()?;
        let job = record.view();
        if !job.state.terminal() {
            record.completion.request_cancel();
            return Some(DeleteOutcome {
                job,
                deleted: false,
            });
        }

        registry.jobs.remove(id);
        if registry.by_event.get(&record.event_id) == Some(&record.id) {
            registry.by_event.remove(&record.event_id);
        }
        self.prom
            .exhaustive_jobs
            .with_label_values(&[job.state.label()])
            .dec();
        Some(DeleteOutcome { job, deleted: true })
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
