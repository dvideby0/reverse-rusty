use super::*;
use crate::collect::BatchTopKCollector;
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::segment::{Engine, MatchCancelled};
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

#[test]
fn counter_deadline_stops_inside_one_columnar_body_group() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    let queries = (0..4_096)
        .map(|logical| (logical, "anchorw".to_string()))
        .collect::<Vec<_>>();
    assert_eq!(engine.build_from_queries(&queries).ingested, queries.len());
    let snapshot = engine.snapshot();
    assert_eq!(
        snapshot.class_counts()[2],
        queries.len() as u64,
        "the finalized high-frequency anchor must put every row in class C"
    );
    assert_eq!(snapshot.segments.len(), 1);
    assert!(snapshot.memtable.is_empty());
    assert!(
        matches!(
            snapshot.segments[0].as_ref(),
            BaseSegment::Memory(segment) if segment.has_dup_groups()
        ),
        "the columnar regression needs one shared-body broad base segment"
    );
    let pred = TagPredicate::empty();
    let view = MatchView {
        norm: &snapshot.norm,
        dict: &snapshot.dict,
        segments: &snapshot.segments,
        memtable: &snapshot.memtable,
        has_phrase_predicates: snapshot.has_phrase_predicates,
        pred: &pred,
    };
    let mut match_scratch = MatchScratch::new();
    let mut broad_scratch = BroadBatchScratch::new();
    let mut outs = vec![Vec::new()];
    let mut emissions = vec![0];
    let mut collector = AllBatchCollector::new(&mut outs, &mut emissions);
    let mut stats = MatchStats::default();
    let checks = AtomicUsize::new(0);

    let result = match_batch_chunk(
        &view,
        &["anchorw"],
        BatchMatchOptions {
            include_broad: true,
            broad_strategy: BroadStrategy::Columnar,
            ..BatchMatchOptions::default()
        },
        &mut match_scratch,
        &mut broad_scratch,
        &mut collector,
        &mut stats,
        CancelOnCheck {
            checks: &checks,
            // Phase-0 title + columnar memtable boundary pass; the first
            // sampled body-group interval cancels.
            cancel_at: 3,
        },
        EmitAll,
    );

    assert_eq!(result, Err(MatchCancelled));
    assert_eq!(checks.load(Ordering::Relaxed), 3);
    assert!(
        stats.broad_candidates > 0,
        "cancellation must happen after the columnar broad kernel reaches the body group"
    );
    assert!(outs[0].is_empty());
    assert_eq!(emissions[0], 0);
}

#[test]
fn ranked_columnar_metadata_walk_uses_the_active_sampler_and_aborts() {
    const LEGACY_COPIES: u32 = 2_048;
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    let mut queries = vec![(7, "anchorw".to_string())];
    queries.extend((100..1_100).map(|logical| (logical, format!("anchorw fillerterm{logical}"))));
    assert_eq!(engine.build_from_queries(&queries).ingested, queries.len());
    for version in 1..=LEGACY_COPIES {
        let query = format!("zzcolumnarlegacy{version}");
        let crate::segment::InsertOutcome::Inserted(local) = engine
            .try_insert_live(&query, 7, version)
            .expect("newer legacy copy")
        else {
            panic!("selective test query was unexpectedly rejected");
        };
        engine.tombstone(local).expect("tombstone legacy copy");
    }
    let snapshot = engine.snapshot();
    assert!(
        snapshot.class_counts()[2] > 0,
        "the matching anchor must reach the columnar class-C lane"
    );
    assert_eq!(
        snapshot.memtable.locals_for_logical(7).len(),
        LEGACY_COPIES as usize
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
    let title_features = [crate::rank::RankTitleFeatures::from_title("alpha")];
    let scorer = snapshot.program_scorer_with_poll(&program, &title_features);
    let mut collector = BatchTopKCollector::new_polling(1, 10, 100, &scorer);
    let mut match_scratch = MatchScratch::new();
    let mut broad_scratch = BroadBatchScratch::new();
    let mut stats = MatchStats::default();
    let checks = AtomicUsize::new(0);
    let result = match_batch_chunk(
        &view,
        &["anchorw"],
        BatchMatchOptions {
            include_broad: true,
            broad_strategy: BroadStrategy::Columnar,
            ..BatchMatchOptions::default()
        },
        &mut match_scratch,
        &mut broad_scratch,
        &mut collector,
        &mut stats,
        CancelOnCheck {
            checks: &checks,
            // Phase-0 title + columnar base boundary pass. The next
            // sample must fire in the rank metadata callback.
            cancel_at: 3,
        },
        EmitAll,
    );

    assert_eq!(result, Err(MatchCancelled));
    assert_eq!(checks.load(Ordering::Relaxed), 3);
    assert!(
        stats.broad_candidates > 0,
        "the regression must cancel after columnar candidate emission starts"
    );
    assert!(collector.slots()[0].winners().is_empty());
    assert_eq!(collector.slots()[0].total_hits().value, 0);
}
