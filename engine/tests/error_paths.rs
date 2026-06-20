//! Error-handling / API-hardening regression tests.
//!
//! These pin the production behavior added when the compile/ingest boundary was
//! hardened: parse failures and class-D rejections are counted *separately* and
//! never silently dropped, and `try_insert_live` surfaces parse errors as typed
//! `Err`s rather than folding them into a bare `None`.

use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::MatchScratch;
use reverse_rusty::{
    Engine, IngestItemStatus, InsertOutcome, Normalizer, ParseErrorKind, WriteError,
};

fn engine() -> Engine {
    Engine::new(Normalizer::default_vocab().expect("built-in vocab"))
}

fn engine_with(config: EngineConfig) -> Engine {
    Engine::with_config(Normalizer::default_vocab().expect("built-in vocab"), config)
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

    // class_counts()[3] counts STORED class-D always-candidates (ADR-068),
    // symmetric with A/B/C — a REJECTED class-D query is only in
    // rejected_class_d(), no longer mirrored into the array.
    assert_eq!(eng.class_counts()[3], 0);
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

// ── A2: u16 count truncation guards (no silent false negatives) ──────────────

#[test]
fn validate_rejects_oversize_count_limits() {
    // The SoA exact store encodes per-query counts as u16; a limit above u16::MAX
    // (65535) would let an accepted query overflow the cast and silently truncate
    // the stored set (a false negative). validate() must reject the runtime-tunable
    // knobs above that ceiling, closing the /_settings path.
    let over = (u16::MAX as usize) + 1; // 65536

    let cfg = EngineConfig {
        max_anyof_group_size: 70_000,
        ..EngineConfig::default()
    };
    assert!(
        cfg.validate()
            .iter()
            .any(|p| p.contains("max_anyof_group_size")),
        "max_anyof_group_size = 70000 must be rejected, got {:?}",
        cfg.validate()
    );

    let cfg = EngineConfig {
        max_query_clauses: over,
        ..EngineConfig::default()
    };
    assert!(
        cfg.validate()
            .iter()
            .any(|p| p.contains("max_query_clauses")),
        "max_query_clauses above u16::MAX must be rejected"
    );

    let cfg = EngineConfig {
        max_tags: over,
        ..EngineConfig::default()
    };
    assert!(
        cfg.validate().iter().any(|p| p.contains("max_tags")),
        "max_tags above u16::MAX must be rejected"
    );

    // The exact ceiling (u16::MAX) is still valid, and the default config is clean.
    let cfg = EngineConfig {
        max_anyof_group_size: u16::MAX as usize,
        max_query_clauses: u16::MAX as usize,
        max_tags: u16::MAX as usize,
        ..EngineConfig::default()
    };
    assert!(
        cfg.validate().is_empty(),
        "the u16::MAX ceiling itself must validate, got {:?}",
        cfg.validate()
    );
    assert!(EngineConfig::default().validate().is_empty());
}

#[test]
fn large_anyof_group_within_limit_matches_high_index_member() {
    // With the any-of limit raised (but still <= u16::MAX, so validate() passes),
    // a large group must NOT be truncated: a title matching the LAST member still
    // matches. (A u16 cast truncation would drop high-index members silently.)
    let n = 500usize; // far above the default 64, well within u16::MAX
    let cfg = EngineConfig {
        max_anyof_group_size: 4_000,
        max_query_length: 1_000_000, // the joined group is long; don't hit QueryTooLong
        ..EngineConfig::default()
    };
    assert!(cfg.validate().is_empty());
    let mut eng = engine_with(cfg);

    let members: Vec<String> = (0..n).map(|i| format!("mbr{i:04}")).collect();
    let query = format!("anchorword ({})", members.join(","));
    let report = eng.build_from_queries(&[(1, query)]);
    assert_eq!(report.ingested, 1, "the large-group query must be stored");

    // A title carrying the anchor + the LAST any-of member must match.
    let snap = eng.snapshot();
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let title = format!("anchorword mbr{:04}", n - 1);
    snap.match_title(&title, &mut scratch, &mut out, true);
    assert!(
        out.contains(&1),
        "title with the high-index any-of member must match (no truncation)"
    );
}

#[test]
fn oversize_anyof_group_rejected_loudly_not_truncated() {
    // A group exceeding the configured limit is rejected at parse with a typed
    // AnyOfGroupTooLarge error — never silently truncated into the store. Combined
    // with validate() capping the knob at u16::MAX, no group can ever reach the
    // u16 group_len cast over-full.
    let cfg = EngineConfig {
        max_anyof_group_size: 8,
        ..EngineConfig::default()
    };
    let mut eng = engine_with(cfg);
    let members: Vec<String> = (0..9).map(|i| format!("m{i}")).collect(); // 9 > 8
    let query = format!("anchor ({})", members.join(","));

    match eng.try_insert_live(&query, 1, 1).unwrap_err() {
        WriteError::Parse(pe) => assert_eq!(pe.kind, ParseErrorKind::AnyOfGroupTooLarge),
        WriteError::Wal(e) => panic!("expected a parse error, got WAL error: {e}"),
    }
    assert_eq!(
        eng.num_queries(),
        0,
        "the over-large group must not be stored"
    );
}

#[test]
fn oversize_tag_set_rejected_loudly_not_truncated() {
    // The per-query tag count is a u16 column with no parse layer; a set above
    // max_tags must be rejected loudly (TooManyTags) rather than truncated, on
    // every live/build ingest path. A within-limit tagged query stays matchable.
    let cfg = EngineConfig {
        max_tags: 2,
        ..EngineConfig::default()
    };
    let mut eng = engine_with(cfg);

    let too_many: Vec<(String, String)> = vec![
        ("k".into(), "a".into()),
        ("k".into(), "b".into()),
        ("k".into(), "c".into()), // 3 > 2
    ];
    let within: Vec<(String, String)> = vec![("k".into(), "a".into()), ("k".into(), "b".into())];

    // Live insert: typed reject, nothing stored.
    let before = eng.num_queries();
    match eng
        .try_insert_live_with_tags("scottie pippen", 1, 1, &too_many)
        .unwrap_err()
    {
        WriteError::Parse(pe) => assert_eq!(pe.kind, ParseErrorKind::TooManyTags),
        WriteError::Wal(e) => panic!("expected a parse error, got WAL error: {e}"),
    }
    assert_eq!(
        eng.num_queries(),
        before,
        "the over-tagged query must not be stored"
    );

    // A within-limit tagged query is accepted and matchable.
    let outcome = eng
        .try_insert_live_with_tags("michael jordan", 2, 1, &within)
        .unwrap();
    assert!(matches!(outcome, InsertOutcome::Inserted(_)));
    let snap = eng.snapshot();
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    snap.match_title("michael jordan card", &mut scratch, &mut out, true);
    assert!(out.contains(&2), "within-limit tagged query must match");

    // Build path: the over-tagged item is reported as a parse reject, not stored.
    let mut eng2 = engine_with(EngineConfig {
        max_tags: 2,
        ..EngineConfig::default()
    });
    let (report, items) = eng2
        .try_bulk_ingest_detailed_with_tags(
            &[(10, "michael jordan".to_string())],
            std::slice::from_ref(&too_many),
        )
        .expect("in-memory ingest is durable");
    assert_eq!(report.ingested, 0);
    assert_eq!(report.rejected_parse, 1);
    match &items[0] {
        IngestItemStatus::RejectedParse(pe) => assert_eq!(pe.kind, ParseErrorKind::TooManyTags),
        other => panic!("expected RejectedParse(TooManyTags), got {other:?}"),
    }
    assert_eq!(eng2.num_queries(), 0);
}
