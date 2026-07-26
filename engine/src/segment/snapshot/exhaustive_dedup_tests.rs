use super::*;

struct CancelImmediately {
    polls: usize,
}

impl ChunkSink for CancelImmediately {
    fn send_chunk(
        &mut self,
        _chunk: &crate::delivery::MatchChunk,
    ) -> Result<(), crate::delivery::ChunkSinkError> {
        Ok(())
    }

    fn check_cancelled(&mut self) -> Result<(), crate::delivery::ChunkSinkError> {
        self.polls += 1;
        Err(crate::delivery::ChunkSinkError::new(
            "already cancelled before setup",
        ))
    }
}

#[test]
fn exhaustive_entry_polls_before_setup() {
    let engine = crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
    let snapshot = engine.snapshot();
    let mut sink = CancelImmediately { polls: 0 };
    let error = snapshot
        .try_match_title_chunks(
            "an alias-heavy or otherwise expensive title must not be normalized",
            ExhaustiveOptions::default(),
            None,
            &TagPredicate::empty(),
            &mut MatchScratch::new(),
            None,
            &mut sink,
        )
        .expect_err("pre-cancelled entry must fail before setup");
    assert!(matches!(error, ExhaustiveMatchError::Sink(_)));
    assert_eq!(sink.polls, 1);
}

#[test]
fn legacy_duplicate_scan_polls_cancellation_between_physical_copies() {
    let mut engine = crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
    for version in 0..2_048 {
        engine
            .try_insert_live("zzlegacyhay", 7, version)
            .expect("legacy duplicate");
    }
    engine.flush();
    engine
        .try_insert_live("zzmatchingneedle", 7, 2_048)
        .expect("current matching copy");
    let snapshot = engine.snapshot();
    assert_eq!(
        snapshot.segments[0].locals_for_logical(7).len(),
        2_048,
        "test must exercise a long reverse-index walk"
    );
    let current = snapshot.memtable.locals_for_logical(7)[0];
    let pred = TagPredicate::empty();
    let mut deduper = ExhaustiveDeduper::new(
        &snapshot,
        "zzmatchingneedle",
        &pred,
        true,
        crate::ownership::EmitAll,
    );
    let mut polls = 0usize;
    let accepted = deduper.is_first_matching(snapshot.segments.len(), current, 7, &mut || {
        polls += 1;
        polls >= 17
    });

    assert!(!accepted, "a cancelled walk must not emit its current copy");
    assert_eq!(
        polls, 17,
        "the walk must stop at the cancellation poll, not scan all duplicates"
    );
}

#[test]
fn ranked_metadata_scan_polls_cancellation_between_legacy_copies() {
    let mut engine = crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
    engine
        .try_insert_live("zzrankcancel", 7, 0)
        .expect("oldest live copy");
    for version in 1..=2_048 {
        let crate::segment::InsertOutcome::Inserted(local) = engine
            .try_insert_live("zzrankcancel", 7, version)
            .expect("newer legacy copy")
        else {
            panic!("selective test query was unexpectedly rejected");
        };
        engine.tombstone(local).expect("tombstone newer copy");
    }
    let snapshot = engine.snapshot();
    assert_eq!(
        snapshot.memtable.locals_for_logical(7).len(),
        2_049,
        "test must exercise a long newest-first metadata walk"
    );

    let mut polls = 0usize;
    let metadata = snapshot.rank_metadata_for_logical_with_poll(7, &mut || {
        polls += 1;
        polls >= 17
    });
    assert!(
        metadata.is_none(),
        "a cancelled metadata scan must not return an older score"
    );
    assert_eq!(
        polls, 17,
        "the walk must stop at the cancellation poll, not scan all copies"
    );
}
#[cfg(test)]
mod bounded_deadline_tests {
    use super::*;
    use crate::collect::MatchSink;
    use crate::ownership::EmissionPolicy;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    struct CancelOnCheck<'a> {
        checks: &'a AtomicUsize,
        cancel_at: usize,
    }

    impl DeadlineCheck for CancelOnCheck<'_> {
        const ARMED: bool = true;
        type Cancelled = MatchCancelled;

        fn check(self) -> Result<(), Self::Cancelled> {
            let current = self.checks.fetch_add(1, Ordering::Relaxed) + 1;
            if current >= self.cancel_at {
                Err(MatchCancelled)
            } else {
                Ok(())
            }
        }
    }

    #[derive(Clone, Copy)]
    struct CountEmissions<'a>(&'a AtomicUsize);

    impl EmissionPolicy for CountEmissions<'_> {
        fn should_emit(self, _placement: crate::ownership::QueryPlacementRef<'_>) -> bool {
            self.0.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    #[derive(Default)]
    struct StopAfterFirstMatch {
        matches: usize,
        stopped: bool,
    }

    impl MatchSink for StopAfterFirstMatch {
        fn on_match(&mut self, _logical_id: u64) {
            self.matches += 1;
            self.stopped = true;
        }

        fn should_stop(&mut self) -> bool {
            self.stopped
        }
    }

    #[test]
    fn collector_failure_precedes_a_simultaneous_deadline_poll() {
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        engine
            .try_insert_live("anchorw", 1, 1)
            .expect("insert matching row");
        let snapshot = engine.snapshot();
        let mut title_scratch = MatchScratch::new();
        snapshot.norm.match_features(
            "anchorw",
            &snapshot.dict,
            &mut title_scratch.lc,
            &mut title_scratch.norm,
            &mut title_scratch.feats,
        );
        let title = crate::exact::TitleView::single(0, &title_scratch.feats);
        let mut seen = vec![0; snapshot.memtable.len()];
        let mut collector = StopAfterFirstMatch::default();
        let pred = TagPredicate::empty();
        let mut stats = MatchStats::default();
        let checks = AtomicUsize::new(0);
        let mut deadline = DeadlinePoll::new(CancelOnCheck {
            checks: &checks,
            cancel_at: 1,
        });
        // The anchor probe and its posting consume two work units. The next
        // loop edge is therefore both the first deadline sample and the first
        // chance to observe the collector's already-recorded failure.
        deadline.remaining = 3;

        let result = snapshot.memtable.match_collect(
            &title,
            &snapshot.dict,
            1,
            &mut seen,
            &mut collector,
            crate::segment::ProbeLanes {
                include_broad: false,
                include_hot: true,
            },
            &pred,
            &mut stats,
            crate::ownership::EmitAll,
            &mut deadline,
        );

        assert_eq!(result, Ok(()));
        assert_eq!(collector.matches, 1);
        assert!(collector.stopped);
        assert_eq!(
            checks.load(Ordering::Relaxed),
            0,
            "an already-recorded collector failure must win before the clock poll"
        );
    }

    #[test]
    fn counter_deadline_stops_inside_one_body_group_and_clears_results() {
        const ROWS: u64 = 4_096;
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        for logical in 0..ROWS {
            engine
                .try_insert_live("anchorw", logical, 1)
                .expect("insert duplicate body");
        }
        let snapshot = engine.snapshot();
        assert!(snapshot.segments.is_empty());
        assert!(snapshot.memtable.has_dup_groups());

        let pred = TagPredicate::empty();
        let view = MatchView {
            norm: &snapshot.norm,
            dict: &snapshot.dict,
            segments: &snapshot.segments,
            memtable: &snapshot.memtable,
            has_phrase_predicates: snapshot.has_phrase_predicates,
            pred: &pred,
        };
        let checks = AtomicUsize::new(0);
        let emissions = AtomicUsize::new(0);
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let result = view.match_title_with_policy(
            "anchorw",
            &mut scratch,
            &mut out,
            true,
            CancelOnCheck {
                checks: &checks,
                // Entry + memtable boundary pass; the first in-segment sample
                // cancels deterministically without consulting wall time.
                cancel_at: 3,
            },
            CountEmissions(&emissions),
        );

        assert_eq!(result, Err(MatchCancelled));
        assert_eq!(checks.load(Ordering::Relaxed), 3);
        assert!(
            emissions.load(Ordering::Relaxed) < ROWS as usize,
            "the sampler must stop within the group instead of finishing the segment"
        );
        assert!(
            out.is_empty(),
            "the lowest-level abort must clear every pre-cancellation emission"
        );
    }

    #[test]
    fn ranked_scalar_metadata_walk_uses_the_active_sampler_and_aborts() {
        const LEGACY_COPIES: u32 = 2_048;
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        engine
            .try_insert_live("zzrankneedle", 7, 0)
            .expect("live matching copy");
        for version in 1..=LEGACY_COPIES {
            let query = format!("zzlegacyterm{version}");
            let crate::segment::InsertOutcome::Inserted(local) = engine
                .try_insert_live(&query, 7, version)
                .expect("newer legacy copy")
            else {
                panic!("selective test query was unexpectedly rejected");
            };
            engine.tombstone(local).expect("tombstone legacy copy");
        }
        let snapshot = engine.snapshot();
        assert_eq!(
            snapshot.memtable.locals_for_logical(7).len(),
            LEGACY_COPIES as usize + 1
        );
        assert!(
            !snapshot.memtable.has_dup_groups(),
            "unique legacy bodies keep cancellation inside rank metadata, not body emission"
        );

        let program = snapshot
            .compile_rank_program(&crate::rank::RankProgramSpec::default())
            .expect("rank program");
        let pred = TagPredicate::empty();
        let view = MatchView {
            norm: &snapshot.norm,
            dict: &snapshot.dict,
            segments: &snapshot.segments,
            memtable: &snapshot.memtable,
            has_phrase_predicates: snapshot.has_phrase_predicates,
            pred: &pred,
        };
        let checks = AtomicUsize::new(0);
        let mut collector =
            TopKCollector::new_polling(10, 100, None, snapshot.program_scorer_with_poll(&program));
        let mut scratch = MatchScratch::new();
        let result = view.match_title_collect(
            "zzrankneedle",
            &mut scratch,
            &mut collector,
            false,
            CancelOnCheck {
                checks: &checks,
                // Entry + memtable boundary pass. The next fixed-interval
                // sample must fire inside newest-live rank metadata.
                cancel_at: 3,
            },
            crate::ownership::EmitAll,
        );

        assert_eq!(result, Err(MatchCancelled));
        assert_eq!(checks.load(Ordering::Relaxed), 3);
        assert!(
            collector.winners().is_empty(),
            "a cancelled rank metadata walk must not leak a partial winner"
        );
        assert_eq!(collector.total_hits().value, 0);
    }
}
