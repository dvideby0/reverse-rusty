use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::Engine;

/// Metrics snapshot should be consistent on a known corpus.
#[test]
fn metrics_consistent_with_known_corpus() {
    let cfg = GenConfig {
        num_queries: 5_000,
        num_titles: 100,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xAE7_21C5,
        num_players: 1_000,
        num_sets: 400,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    let report = eng.build_from_queries(&data.queries);

    assert!(
        report.ingested > 0,
        "need some ingested queries for this test"
    );

    let m = eng.metrics();
    assert_eq!(
        m.total_queries, report.ingested as usize,
        "metrics total_queries must equal ingested count"
    );
    assert_eq!(
        m.base_segments, 1,
        "one base segment after build_from_queries"
    );
    assert!(m.dict_features > 0, "dictionary should have features");
    assert!(m.exact_bytes > 0, "exact store should use memory");
    assert!(m.index_bytes > 0, "index should use memory");
    assert_eq!(m.rejected_parse as usize, report.rejected_parse);
    assert_eq!(m.rejected_class_d as usize, report.rejected_class_d);
}

// ─────────────────────────────────────────────────────────────────────────────
// Settings: the engine config rides in the lock-free snapshot (GET /_settings),
// and set_config swaps it copy-on-write so in-flight snapshots keep their view.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn settings_snapshot_reflects_set_config_and_is_immutable() {
    use reverse_rusty::config::EngineConfig;

    let mut eng = Engine::with_config(
        Normalizer::default_vocab().unwrap(),
        EngineConfig {
            max_segments: 8,
            ..EngineConfig::default()
        },
    );

    // GET /_settings reads the snapshot, so the snapshot must carry the config.
    let snap_before = eng.snapshot();
    assert_eq!(snap_before.config().max_segments, 8);

    // Change a dynamic knob via the public setter.
    let mut cfg = eng.config().clone();
    cfg.max_segments = 32;
    eng.set_config(cfg);

    // A fresh snapshot sees the new value; the engine agrees; the older snapshot
    // keeps its own view (copy-on-write via Arc).
    assert_eq!(eng.snapshot().config().max_segments, 32);
    assert_eq!(eng.config().max_segments, 32);
    assert_eq!(
        snap_before.config().max_segments,
        8,
        "an already-published snapshot must keep its own config view"
    );
}

/// The `EngineConfig` query-complexity limits must actually govern parsing on
/// the ingest paths (not just sit in the struct), and must be dynamic: raising a
/// limit at runtime makes a previously-rejected query acceptable. Regression
/// test for the wiring gap where these knobs were never read by the parser.
#[test]
fn configured_query_limits_are_enforced_at_ingest_and_are_dynamic() {
    use reverse_rusty::config::EngineConfig;
    use reverse_rusty::error::{ParseErrorKind, WriteError};
    use reverse_rusty::segment::InsertOutcome;

    // A max_query_clauses far below the compiled-in default. The query stays well
    // within the byte-length and any-of ceilings, so this isolates the clause
    // limit specifically.
    let mut eng = Engine::with_config(
        Normalizer::default_vocab().unwrap(),
        EngineConfig {
            max_query_clauses: 3,
            ..EngineConfig::default()
        },
    );

    // 4 clauses > configured max of 3 → rejected at the live-insert front door.
    let four = "alpha beta gamma delta";
    match eng.try_insert_live(four, 1, 1) {
        Err(WriteError::Parse(e)) => assert_eq!(e.kind, ParseErrorKind::TooManyClauses),
        other => panic!("expected a TooManyClauses parse rejection, got {other:?}"),
    }

    // Same rejection on the bulk path (counted in the IngestReport).
    let report = eng.bulk_ingest(&[(2, four.to_string())]);
    assert_eq!(report.ingested, 0);
    assert_eq!(report.rejected_parse, 1);

    // Raising the limit at runtime (the PUT /_settings path uses set_config) makes
    // the very same query acceptable — proving the knob governs parsing live.
    let mut cfg = eng.config().clone();
    cfg.max_query_clauses = 8;
    eng.set_config(cfg);
    assert!(
        matches!(
            eng.try_insert_live(four, 3, 1),
            Ok(InsertOutcome::Inserted(_))
        ),
        "raising max_query_clauses must let the same query through"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-segment introspection (segment_infos — backs GET /_cat/segments)
// ─────────────────────────────────────────────────────────────────────────────

/// `segment_infos()` reports one row per base segment plus the memtable, with
/// consistent per-row arithmetic, and a deletion surfaces as a hole. The engine
/// and its published snapshot must agree.
#[test]
fn segment_infos_reports_layout_and_holes() {
    use reverse_rusty::events::SegmentKind;

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    // First (sealed) base segment with a handful of anchorable queries.
    eng.build_from_queries(&[
        (1, "michael jordan 1994".to_string()),
        (2, "larry bird 1986".to_string()),
        (3, "magic johnson 1987".to_string()),
    ]);
    // Two live inserts land in the mutable memtable.
    let _ = eng.insert_live("kobe bryant 2000", 4, 1);
    let _ = eng.insert_live("tim duncan 1998", 5, 1);

    let infos = eng.segment_infos();
    assert!(!infos.is_empty(), "always at least the memtable row");

    // Ordinals are dense 0..n; per-row arithmetic holds; the final row is the memtable.
    for (i, s) in infos.iter().enumerate() {
        assert_eq!(s.ordinal, i, "ordinals must be dense and in order");
        assert_eq!(
            s.alive + s.deleted,
            s.entries,
            "alive + deleted must equal entries"
        );
        assert!((0.0..=1.0).contains(&s.holes_ratio));
    }
    let last = infos.last().expect("memtable row");
    assert_eq!(last.kind, SegmentKind::Memtable);
    assert_eq!(last.alive, 2, "two live inserts sit in the memtable");

    // Base rows account for the three bulk-built queries.
    let base_alive: usize = infos
        .iter()
        .filter(|s| s.kind != SegmentKind::Memtable)
        .map(|s| s.alive)
        .sum();
    assert_eq!(base_alive, 3);

    // Total entries across rows == the engine's reported query count.
    let total: usize = infos.iter().map(|s| s.entries).sum();
    assert_eq!(total, eng.num_queries());

    // The lock-free snapshot path returns the same view.
    let snap_infos = eng.snapshot().segment_infos();
    assert_eq!(snap_infos.len(), infos.len());
    for (a, b) in snap_infos.iter().zip(infos.iter()) {
        assert_eq!(a.ordinal, b.ordinal);
        assert_eq!(a.entries, b.entries);
        assert_eq!(a.alive, b.alive);
    }

    // Deleting a bulk-built query tombstones it in its base segment → a hole.
    let removed = eng.delete_by_logical_id(2).expect("delete ok");
    assert_eq!(removed, 1);
    let infos = eng.segment_infos();
    let holes: usize = infos
        .iter()
        .filter(|s| s.kind != SegmentKind::Memtable)
        .map(|s| s.deleted)
        .sum();
    assert_eq!(
        holes, 1,
        "the deleted query is now a hole in its base segment"
    );
    assert!(
        infos
            .iter()
            .any(|s| s.kind != SegmentKind::Memtable && s.holes_ratio > 0.0),
        "a base segment should report a non-zero holes ratio after deletion"
    );
}
