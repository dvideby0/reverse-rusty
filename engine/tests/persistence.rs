//! Persistence tests — segment round-trip, mmap matching, and WAL recovery.
//!
//! These tests verify:
//! 1. A segment serialized to disk and mmap'd back produces identical match results
//! 2. WAL recovery after simulated crash restores the memtable
//! 3. Compaction works correctly with mmap'd segments
//! 4. The full lifecycle: build → persist → close → reopen → match

use percolator::config::EngineConfig;
use percolator::segment::Engine;
use percolator::normalize::Normalizer;
use std::path::PathBuf;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("percolator_test_{}", name));
    // Clean up from previous runs
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn make_norm() -> Normalizer {
    Normalizer::default_vocab().unwrap()
}

fn sample_queries() -> Vec<(u64, String)> {
    vec![
        (1, "michael jordan 1986 fleer".into()),
        (2, "lebron james rookie".into()),
        (3, "kobe bryant psa 10".into()),
        (4, "mike trout 2011 topps update".into()),
        (5, "derek jeter bowman chrome refractor".into()),
        (6, "shohei ohtani rookie".into()),
        (7, "luka doncic prizm silver".into()),
        (8, "stephen curry select".into()),
        (9, "aaron judge topps chrome".into()),
        (10, "patrick mahomes prizm rookie".into()),
    ]
}

/// Helper: match a title and return sorted logical IDs.
fn match_ids(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = percolator::segment::MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort();
    out
}

#[test]
fn segment_round_trip() {
    // Build an engine in-memory, then write its segment, mmap it back, and
    // verify matches are identical.
    let dir = test_dir("round_trip");
    let norm = make_norm();
    let queries = sample_queries();

    // 1) Build in-memory engine
    let mut mem_engine = Engine::new(norm);
    mem_engine.build_from_queries(&queries);

    // 2) Build persistent engine with same queries
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut disk_engine = Engine::with_config(make_norm(), config);
    disk_engine.build_from_queries(&queries);

    // 3) Verify both produce the same matches
    let titles = [
        "1986 Fleer Michael Jordan Rookie Card #57 PSA 10",
        "LeBron James 2003 Topps Chrome Rookie RC",
        "Kobe Bryant 1996 Topps Chrome Refractor PSA 10",
        "Mike Trout 2011 Topps Update RC US175",
        "Random card that matches nothing specific",
    ];

    for title in &titles {
        let mem_result = match_ids(&mem_engine, title);
        let disk_result = match_ids(&disk_engine, title);
        assert_eq!(
            mem_result, disk_result,
            "Mismatch for title '{}': in-memory={:?} vs disk={:?}",
            title, mem_result, disk_result
        );
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persist_and_reopen() {
    // Build, close, reopen, and verify matches survive.
    let dir = test_dir("persist_reopen");
    let norm = make_norm();
    let queries = sample_queries();

    // 1) Build and persist
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(norm, config.clone());
    engine.build_from_queries(&queries);

    // Record expected matches
    let title = "1986 Fleer Michael Jordan Rookie Card #57 PSA 10";
    let expected = match_ids(&engine, title);
    drop(engine); // "close" the engine

    // 2) Reopen
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let actual = match_ids(&engine2, title);
    assert_eq!(expected, actual, "matches differ after reopen");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_recovery_inserts() {
    // Insert via insert_live (goes through WAL), then simulate crash + recovery.
    let dir = test_dir("wal_recovery");
    let norm = make_norm();
    let queries = sample_queries();

    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX, // never auto-flush
        ..EngineConfig::default()
    };

    // 1) Build base segment, then add live inserts (not flushed)
    let mut engine = Engine::with_config(norm, config.clone());
    engine.build_from_queries(&queries);

    // These go to the memtable + WAL but are NOT flushed to segments
    engine.insert_live("wander franco prospect", 100, 1);
    engine.insert_live("fernando tatis jr rookie", 101, 1);

    let title_wander = "Wander Franco 2019 Bowman Chrome Prospect";
    let title_tatis = "Fernando Tatis Jr 2019 Topps Chrome Rookie";
    let expected_wander = match_ids(&engine, title_wander);
    let expected_tatis = match_ids(&engine, title_tatis);

    drop(engine); // simulate crash

    // 2) Recover
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let actual_wander = match_ids(&engine2, title_wander);
    let actual_tatis = match_ids(&engine2, title_tatis);

    assert_eq!(expected_wander, actual_wander, "WAL recovery lost wander insert");
    assert_eq!(expected_tatis, actual_tatis, "WAL recovery lost tatis insert");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn compaction_with_mmap_segments() {
    // Multiple flushes create mmap'd segments, then compact and verify.
    let dir = test_dir("compact_mmap");
    let norm = make_norm();

    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };

    let mut engine = Engine::with_config(norm, config);

    // Build base segment
    let q1: Vec<(u64, String)> = vec![
        (1, "michael jordan 1986 fleer".into()),
        (2, "lebron james rookie".into()),
    ];
    engine.build_from_queries(&q1);

    // Bulk ingest a second segment
    let q2: Vec<(u64, String)> = vec![
        (3, "kobe bryant psa 10".into()),
        (4, "mike trout 2011 topps update".into()),
    ];
    engine.bulk_ingest(&q2);

    assert_eq!(engine.num_segments(), 3); // 2 base + memtable

    // Record matches before compaction
    let title = "1986 Fleer Michael Jordan Rookie Card PSA 10";
    let before = match_ids(&engine, title);

    // Compact
    engine.compact_all();
    assert_eq!(engine.num_segments(), 2); // 1 base + memtable

    // Verify matches unchanged
    let after = match_ids(&engine, title);
    assert_eq!(before, after, "compaction changed match results");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flush_creates_mmap_segment() {
    let dir = test_dir("flush_mmap");
    let norm = make_norm();

    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        ..EngineConfig::default()
    };

    let mut engine = Engine::with_config(norm, config);
    engine.insert_live("michael jordan 1986 fleer", 1, 1);
    engine.insert_live("lebron james rookie", 2, 1);

    // Manually flush
    engine.flush();
    assert_eq!(engine.num_segments(), 2); // 1 base (mmap'd) + memtable

    // Check that a .seg file exists
    let seg_dir = dir.join("segments");
    let seg_files: Vec<_> = std::fs::read_dir(&seg_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "seg"))
        .collect();
    assert!(!seg_files.is_empty(), "no .seg file created after flush");

    // Verify matching still works
    let title = "1986 Fleer Michael Jordan Rookie Card";
    let ids = match_ids(&engine, title);
    assert!(!ids.is_empty(), "no matches after flush to mmap");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn in_memory_backward_compat() {
    // Verify that engines without data_dir work exactly as before.
    let norm = make_norm();
    let queries = sample_queries();

    let mut engine = Engine::new(norm);
    engine.build_from_queries(&queries);

    let title = "1986 Fleer Michael Jordan Rookie Card #57 PSA 10";
    let ids = match_ids(&engine, title);
    // Should find at least query 1 (michael jordan 1986 fleer)
    assert!(ids.contains(&1), "backward compat: query 1 not found");
}
