//! Error-handling / API-hardening regression tests.
//!
//! These pin the production behavior added when the compile/ingest boundary was
//! hardened: parse failures and class-D rejections are counted *separately* and
//! never silently dropped, and `try_insert_live` surfaces parse errors as typed
//! `Err`s rather than folding them into a bare `None`.

use reverse_rusty::{
    Engine, IngestItemStatus, InsertOutcome, Normalizer, ParseErrorKind, WriteError,
};

fn engine() -> Engine {
    Engine::new(Normalizer::default_vocab().expect("built-in vocab"))
}

#[test]
fn build_report_splits_parse_and_class_d() {
    let mut eng = engine();
    let batch = vec![
        (1u64, "michael jordan".to_string()), // good        -> ingested
        (2u64, "(".to_string()),              // malformed   -> rejected_parse
        (3u64, "-auto".to_string()),          // only a NOT  -> rejected_class_d
    ];
    let report = eng.build_from_queries(&batch);

    assert_eq!(report.ingested, 1, "one good query should be indexed");
    assert_eq!(report.rejected_parse, 1, "the '(' should be a parse reject");
    assert_eq!(
        report.rejected_class_d, 1,
        "'-auto' has no anchor => class D"
    );

    // engine-level counters agree with the per-batch report
    assert_eq!(eng.rejected_parse(), 1);
    assert_eq!(eng.rejected_class_d(), 1);
    assert_eq!(eng.rejected(), 2, "total = parse + class-D");
    assert_eq!(eng.num_queries(), 1);

    // class_counts()[3] is the class-D count only, not parse failures
    assert_eq!(eng.class_counts()[3], 1);
}

#[test]
fn bulk_detailed_reports_per_item_outcomes() {
    // Regression for the bulk API (PRODUCTION-AUDIT P1-8): a batch that mixes a
    // good query, a parse failure, and a class-D rejection must report a per-item
    // outcome for each, in submission order — not just an aggregate count that
    // hides *which* items were dropped. This is what lets the HTTP /_bulk handler
    // return ES-style per-item statuses instead of marking every parsed item 201.
    let mut eng = engine();
    let batch = vec![
        (1u64, "michael jordan".to_string()), // good        -> Ingested
        (2u64, "(".to_string()),              // malformed   -> RejectedParse
        (3u64, "-auto".to_string()),          // only a NOT  -> RejectedClassD
    ];
    let (report, items) = eng
        .try_bulk_ingest_detailed(&batch)
        .expect("in-memory ingest is durable");

    // One outcome per input, index-aligned with submission order.
    assert_eq!(items.len(), batch.len());
    assert_eq!(items[0], IngestItemStatus::Ingested);
    assert_eq!(items[2], IngestItemStatus::RejectedClassD);

    // The parse rejection carries the typed diagnostic so the server can echo
    // the same detail the single-doc path returns.
    match &items[1] {
        IngestItemStatus::RejectedParse(pe) => assert_eq!(pe.kind, ParseErrorKind::UnclosedGroup),
        other => panic!("expected RejectedParse, got {other:?}"),
    }

    // The per-item view and the aggregate report never disagree.
    assert_eq!(
        (
            report.ingested,
            report.rejected_parse,
            report.rejected_class_d
        ),
        (1, 1, 1)
    );
    let ingested = items
        .iter()
        .filter(|s| matches!(s, IngestItemStatus::Ingested))
        .count();
    let parsed = items
        .iter()
        .filter(|s| matches!(s, IngestItemStatus::RejectedParse(_)))
        .count();
    let class_d = items
        .iter()
        .filter(|s| matches!(s, IngestItemStatus::RejectedClassD))
        .count();
    assert_eq!(
        (ingested, parsed, class_d),
        (
            report.ingested,
            report.rejected_parse,
            report.rejected_class_d
        ),
    );
}

#[test]
fn insert_live_now_counts_parse_failures() {
    // Regression: a parse failure in insert_live used to return None with NO
    // record at all, so rejected()/rejected_parse() under-counted.
    let mut eng = engine();
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    let before = eng.rejected_parse();
    assert_eq!(eng.insert_live("(", 99, 2), None);
    assert_eq!(
        eng.rejected_parse(),
        before + 1,
        "parse failure must be counted"
    );
}

#[test]
fn try_insert_live_surfaces_typed_error_without_counting() {
    let mut eng = engine();
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);

    // parse failure -> typed Err, and NOT counted (the caller owns it)
    let before_parse = eng.rejected_parse();
    match eng.try_insert_live("(", 99, 2).unwrap_err() {
        WriteError::Parse(pe) => assert_eq!(pe.kind, ParseErrorKind::UnclosedGroup),
        WriteError::Wal(e) => panic!("expected a parse error, got WAL error: {e}"),
    }
    assert_eq!(eng.rejected_parse(), before_parse);

    // class-D -> Ok(RejectedClassD), and IS counted
    let outcome = eng.try_insert_live("-auto", 100, 2).unwrap();
    assert_eq!(outcome, InsertOutcome::RejectedClassD);
    assert_eq!(eng.rejected_class_d(), 1);

    // good insert -> Ok(Inserted(_))
    let outcome = eng.try_insert_live("scottie pippen", 101, 2).unwrap();
    assert!(
        matches!(outcome, InsertOutcome::Inserted(_)),
        "expected Inserted, got {outcome:?}"
    );
}
