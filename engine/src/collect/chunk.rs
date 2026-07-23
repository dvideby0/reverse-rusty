//! Fixed-capacity exhaustive collector (ADR-114).

use crate::delivery::{
    ChunkSink, ChunkSinkError, DeliveryChecksum, ExhaustiveMatch, ExhaustiveSummary, MatchChunk,
};
use crate::result::TotalHits;
use std::time::Instant;

use super::{CollectionSummary, MatchCollector, MatchSink};

pub(crate) struct ChunkCollector<'a, S: ChunkSink + ?Sized, D, F> {
    sink: &'a mut S,
    canonical: D,
    scorer: F,
    buffer: Vec<ExhaustiveMatch>,
    chunk_size: usize,
    current_source: usize,
    sequence: u64,
    exact_total: u64,
    physical_emissions: u64,
    checksum: DeliveryChecksum,
    error: Option<ChunkSinkError>,
    deadline: Option<Instant>,
    deadline_expired: bool,
    summary: Option<ExhaustiveSummary>,
}

impl<'a, S, D, F> ChunkCollector<'a, S, D, F>
where
    S: ChunkSink + ?Sized,
    D: FnMut(usize, u32, u64, &mut dyn FnMut() -> bool) -> bool,
    F: FnMut(u64, &mut dyn FnMut() -> bool) -> Option<i64>,
{
    pub(crate) fn new(
        sink: &'a mut S,
        chunk_size: usize,
        canonical: D,
        scorer: F,
        deadline: Option<Instant>,
    ) -> Self {
        Self {
            sink,
            canonical,
            scorer,
            buffer: Vec::with_capacity(chunk_size),
            chunk_size,
            current_source: 0,
            sequence: 0,
            exact_total: 0,
            physical_emissions: 0,
            checksum: DeliveryChecksum::default(),
            error: None,
            deadline,
            deadline_expired: false,
            summary: None,
        }
    }

    fn accept(&mut self, logical_id: u64) {
        if self.poll_stop() {
            return;
        }
        let score = {
            // Resolving newest-live rank metadata may itself walk a long legacy
            // reverse-index list. Give that scan the same cancellation/deadline
            // hook as canonical duplicate selection.
            let scorer = &mut self.scorer;
            let sink = &mut *self.sink;
            let error = &mut self.error;
            let deadline = self.deadline;
            let deadline_expired = &mut self.deadline_expired;
            let mut should_stop = || {
                if error.is_some() || *deadline_expired {
                    return true;
                }
                if deadline.is_some_and(|at| Instant::now() >= at) {
                    *deadline_expired = true;
                    return true;
                }
                if let Err(found) = sink.check_cancelled() {
                    *error = Some(found);
                    return true;
                }
                false
            };
            scorer(logical_id, &mut should_stop)
        };
        if self.error.is_some() || self.deadline_expired {
            return;
        }
        let member = ExhaustiveMatch { logical_id, score };
        self.exact_total = self.exact_total.saturating_add(1);
        self.checksum.observe(member);
        self.buffer.push(member);
        if self.buffer.len() == self.chunk_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() || self.error.is_some() {
            return;
        }
        // Move the one fixed-capacity allocation into the synchronous frame,
        // leaving only an allocation-free empty Vec behind. The sink borrows the
        // frame for the duration of `send_chunk`; afterward move the SAME buffer
        // back for the next chunk. Allocating a replacement here would turn a
        // result-sized stream into one heap allocation per emitted chunk.
        let matches = std::mem::take(&mut self.buffer);
        let chunk = MatchChunk {
            sequence: self.sequence,
            matches,
        };
        let sent = self.sink.send_chunk(&chunk);
        self.buffer = chunk.matches;
        self.buffer.clear();
        match sent {
            Ok(()) => {
                self.sequence = self.sequence.saturating_add(1);
            }
            Err(error) => {
                self.error = Some(error);
            }
        }
    }

    fn poll_stop(&mut self) -> bool {
        if self.error.is_some() || self.deadline_expired {
            return true;
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            self.deadline_expired = true;
            return true;
        }
        if let Err(error) = self.sink.check_cancelled() {
            self.error = Some(error);
            return true;
        }
        false
    }

    pub(crate) fn deadline_expired(&self) -> bool {
        self.deadline_expired
    }

    pub(crate) fn result(&self) -> Result<ExhaustiveSummary, ChunkSinkError> {
        match &self.error {
            Some(error) => Err(error.clone()),
            None => self.summary.ok_or_else(|| {
                ChunkSinkError::new("exhaustive collector did not reach terminal summary")
            }),
        }
    }
}

impl<S, D, F> MatchSink for ChunkCollector<'_, S, D, F>
where
    S: ChunkSink + ?Sized,
    D: FnMut(usize, u32, u64, &mut dyn FnMut() -> bool) -> bool,
    F: FnMut(u64, &mut dyn FnMut() -> bool) -> Option<i64>,
{
    fn on_match(&mut self, logical_id: u64) {
        if self.poll_stop() {
            return;
        }
        self.physical_emissions = self.physical_emissions.saturating_add(1);
        self.accept(logical_id);
    }

    fn should_stop(&mut self) -> bool {
        self.poll_stop()
    }

    fn begin_source(&mut self, source: usize) {
        self.current_source = source;
    }

    fn on_match_at(&mut self, logical_id: u64, local_id: u32) {
        if self.poll_stop() {
            return;
        }
        self.physical_emissions = self.physical_emissions.saturating_add(1);
        let accepted = {
            // The duplicate predicate may scan many legacy physical copies.
            // Give it a poll hook over the same sink/deadline state so that
            // scan cannot become one uninterruptible O(duplicates) region.
            let canonical = &mut self.canonical;
            let sink = &mut *self.sink;
            let error = &mut self.error;
            let deadline = self.deadline;
            let deadline_expired = &mut self.deadline_expired;
            let mut should_stop = || {
                if error.is_some() || *deadline_expired {
                    return true;
                }
                if deadline.is_some_and(|at| Instant::now() >= at) {
                    *deadline_expired = true;
                    return true;
                }
                if let Err(found) = sink.check_cancelled() {
                    *error = Some(found);
                    return true;
                }
                false
            };
            canonical(self.current_source, local_id, logical_id, &mut should_stop)
        };
        if accepted {
            self.accept(logical_id);
        }
    }
}

impl<S, D, F> MatchCollector for ChunkCollector<'_, S, D, F>
where
    S: ChunkSink + ?Sized,
    D: FnMut(usize, u32, u64, &mut dyn FnMut() -> bool) -> bool,
    F: FnMut(u64, &mut dyn FnMut() -> bool) -> Option<i64>,
{
    fn reset(&mut self) {
        self.buffer.clear();
        self.current_source = 0;
        self.sequence = 0;
        self.exact_total = 0;
        self.physical_emissions = 0;
        self.checksum = DeliveryChecksum::default();
        self.error = None;
        self.deadline_expired = false;
        self.summary = None;
    }

    fn finish(&mut self) -> CollectionSummary {
        // Poll on both sides of the final synchronous send. The pre-flush poll
        // avoids publishing after an already-visible cancellation/deadline;
        // the post-flush poll catches a deadline crossed, or cancellation made
        // visible, while a successful final `send_chunk` was blocked. Without
        // the latter there is no next candidate boundary and we could mint a
        // commit-capable summary for a cancelled stream.
        if !self.poll_stop() {
            self.flush();
        }
        if !self.poll_stop() {
            self.summary = Some(ExhaustiveSummary {
                exact_total: self.exact_total,
                chunk_count: self.sequence,
                checksum: self.checksum,
            });
        }
        CollectionSummary {
            retained: usize::try_from(self.exact_total).unwrap_or(usize::MAX),
            total_hits: TotalHits::exact(self.exact_total),
            logical_emissions: self.physical_emissions,
            duplicate_emissions: Some(self.physical_emissions.saturating_sub(self.exact_total)),
        }
    }

    fn abort(&mut self) {
        self.buffer.clear();
        self.summary = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RejectFirstChunk {
        calls: usize,
    }

    impl ChunkSink for RejectFirstChunk {
        fn send_chunk(&mut self, _chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
            self.calls += 1;
            Err(ChunkSinkError::new("injected sink failure"))
        }
    }

    #[test]
    fn sink_failure_stops_duplicate_lookup_immediately() {
        let mut sink = RejectFirstChunk::default();
        let mut canonical_calls = 0usize;
        {
            let mut collector = ChunkCollector::new(
                &mut sink,
                1,
                |_, _, _, _| {
                    canonical_calls += 1;
                    true
                },
                |_, _| None,
                None,
            );
            collector.on_match_at(1, 0);
            for local in 1..10_000 {
                collector.on_match_at(u64::from(local), local);
            }
            assert!(collector.should_stop());
            assert!(collector.result().is_err());
        }
        assert_eq!(sink.calls, 1);
        assert_eq!(canonical_calls, 1);
    }

    #[derive(Default)]
    struct CancelAfterFinalChunk {
        calls: usize,
        cancelled: bool,
    }

    impl ChunkSink for CancelAfterFinalChunk {
        fn send_chunk(&mut self, _chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
            self.calls += 1;
            self.cancelled = true;
            Ok(())
        }

        fn check_cancelled(&mut self) -> Result<(), ChunkSinkError> {
            if self.cancelled {
                Err(ChunkSinkError::new("cancelled during final send"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn final_successful_send_is_polled_before_summary() {
        let mut sink = CancelAfterFinalChunk::default();
        {
            let mut collector =
                ChunkCollector::new(&mut sink, 2, |_, _, _, _| true, |_, _| None, None);
            collector.on_match(7);
            collector.finish();
            assert!(
                collector.result().is_err(),
                "cancellation exposed by the final send must suppress completion"
            );
        }
        assert_eq!(sink.calls, 1, "the partial final chunk is sent once");
    }

    struct CancelThirdPoll {
        polls: usize,
    }

    impl ChunkSink for CancelThirdPoll {
        fn send_chunk(&mut self, _chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
            Ok(())
        }

        fn check_cancelled(&mut self) -> Result<(), ChunkSinkError> {
            self.polls += 1;
            if self.polls >= 3 {
                Err(ChunkSinkError::new("cancelled during rank lookup"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn ranked_metadata_scan_uses_the_collector_stop_hook() {
        let mut sink = CancelThirdPoll { polls: 0 };
        let mut scorer_called = false;
        {
            let mut collector = ChunkCollector::new(
                &mut sink,
                8,
                |_, _, _, _| true,
                |_, should_stop| {
                    scorer_called = true;
                    assert!(should_stop(), "third poll must expose cancellation");
                    Some(99)
                },
                None,
            );
            collector.on_match(7);
            assert!(collector.result().is_err());
            assert_eq!(collector.exact_total, 0);
            assert!(collector.buffer.is_empty());
        }
        assert!(scorer_called);
        assert_eq!(sink.polls, 3);
    }
}
