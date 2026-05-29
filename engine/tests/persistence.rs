//! Persistence tests — segment round-trip, mmap matching, and WAL recovery.
//!
//! These tests verify:
//! 1. A segment serialized to disk and mmap'd back produces identical match results
//! 2. WAL recovery after simulated crash restores the memtable
//! 3. Compaction works correctly with mmap'd segments
//! 4. The full lifecycle: build → persist → close → reopen → match

use percolator::config::EngineConfig;
use percolator::normalize::Normalizer;
use percolator::segment::Engine;
use std::path::PathBuf;

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("percolator_test_{name}"));
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
    out.sort_unstable();
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
            "Mismatch for title '{title}': in-memory={mem_result:?} vs disk={disk_result:?}"
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

    assert_eq!(
        expected_wander, actual_wander,
        "WAL recovery lost wander insert"
    );
    assert_eq!(
        expected_tatis, actual_tatis,
        "WAL recovery lost tatis insert"
    );

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
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "seg"))
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

#[test]
fn wal_recovery_reports_corrupt_tail() {
    use percolator::wal::Wal;
    use std::io::Write;

    let dir = test_dir("wal_corrupt_tail");
    let wal_path = dir.join("wal.log");

    // Write a valid WAL with two inserts
    {
        let mut wal = Wal::open(&wal_path, false).unwrap();
        wal.append_insert(1, 1, "michael jordan card").unwrap();
        wal.append_insert(2, 1, "lebron james rookie").unwrap();
    }

    // Append garbage to simulate a torn write
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF])
            .unwrap();
    }

    // Recover and check that we get the valid entries + skipped bytes reported
    let recovery = Wal::recover(&wal_path).unwrap();
    assert_eq!(
        recovery.entries.len(),
        2,
        "should recover both valid entries"
    );
    assert!(
        recovery.skipped_bytes > 0,
        "should report skipped bytes from corrupt tail"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bulk_ingest_persists_sources_across_reopen() {
    // P1-15: bulk_ingest now persists both the segment AND the source text
    // (sources.dat) as part of its durable commit. Previously bulk bypassed
    // sources.dat entirely, so source text was lost on reopen.
    let dir = test_dir("bulk_sources_reopen");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&sample_queries());

    let batch = vec![
        (100u64, "wander franco prospect".to_string()),
        (101u64, "fernando tatis jr rookie".to_string()),
    ];
    let report = engine.bulk_ingest(&batch);
    assert_eq!(report.ingested, 2);
    assert_eq!(
        engine.get_query_source(100).as_deref(),
        Some("wander franco prospect")
    );

    let title = "Wander Franco 2019 Bowman Chrome Prospect";
    let expected = match_ids(&engine, title);
    assert!(
        expected.contains(&100),
        "bulk query should match before reopen"
    );
    drop(engine);

    // Reopen: both the match data AND the bulk source text must survive.
    let engine2 = Engine::open(make_norm(), config).unwrap();
    assert_eq!(
        match_ids(&engine2, title),
        expected,
        "bulk matches lost after reopen"
    );
    assert_eq!(
        engine2.get_query_source(100).as_deref(),
        Some("wander franco prospect"),
        "bulk-ingested source text lost after reopen (sources.dat not persisted)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn bulk_ingest_failure_is_all_or_nothing() {
    // P1-15: a persistence failure during bulk ingest must roll the batch back
    // entirely (no segment added, no source committed) and surface as an error,
    // instead of silently degrading to an in-memory segment.
    use std::os::unix::fs::PermissionsExt;

    let dir = test_dir("bulk_all_or_nothing");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config);
    engine.build_from_queries(&sample_queries());
    let segs_before = engine.num_segments();

    // Make the segments dir read-only so the next segment write fails.
    let seg_dir = dir.join("segments");
    let orig = std::fs::metadata(&seg_dir).unwrap().permissions();
    std::fs::set_permissions(&seg_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let batch = vec![(100u64, "wander franco prospect".to_string())];
    let failed = engine.try_bulk_ingest(&batch);

    // Restore perms BEFORE asserting so temp-dir cleanup always works.
    std::fs::set_permissions(&seg_dir, orig).unwrap();

    assert!(
        failed.is_err(),
        "bulk ingest into a read-only dir should fail"
    );
    assert_eq!(
        engine.num_segments(),
        segs_before,
        "a failed bulk ingest must not add a segment"
    );
    assert!(
        !engine.persistence_healthy,
        "persistence should be marked unhealthy after a write failure"
    );
    assert!(
        engine.get_query_source(100).is_none(),
        "a rolled-back batch must not commit source text"
    );

    // Once the dir is writable again, a fresh bulk ingest commits cleanly.
    let ok = engine.try_bulk_ingest(&batch);
    assert!(
        ok.is_ok(),
        "bulk ingest should succeed after the dir is writable"
    );
    assert_eq!(engine.num_segments(), segs_before + 1);
    assert_eq!(
        engine.get_query_source(100).as_deref(),
        Some("wander franco prospect")
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn metrics_account_for_resident_aux_components() {
    // Phase 0 (ADR-020): per-component resident accounting must cover the
    // structures the file-backed accounting ignores — dict, query_store,
    // logical_index, alive — and must report them for an mmap'd (reopened)
    // engine, where the SoA + candidate index are file-backed (0 resident heap).
    let dir = test_dir("resident_metrics");
    let queries = sample_queries();

    // Build persistent, drop, reopen so base segments load as MmapSegment.
    {
        let config = EngineConfig {
            data_dir: Some(dir.clone()),
            ..EngineConfig::default()
        };
        let mut eng = Engine::with_config(make_norm(), config);
        eng.build_from_queries(&queries);
    }
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let eng = Engine::open(make_norm(), config).expect("reopen");

    let m = eng.metrics();
    assert!(m.total_queries >= queries.len());
    assert!(m.dict_bytes > 0, "dict_bytes should be counted");
    assert!(
        m.query_store_bytes > 0,
        "query_store_bytes should be counted"
    );
    assert!(m.alive_bytes > 0, "alive_bytes should be counted");

    // For mmap'd segments the SoA + index are file-backed (paged), so they
    // contribute 0 resident heap — confirming the resident cost lives in the
    // auxiliary structures above.
    assert_eq!(
        m.exact_bytes, 0,
        "mmap exact SoA should report 0 resident heap"
    );
    assert_eq!(m.index_bytes, 0, "mmap index should report 0 resident heap");
    // ADR-020 Item 2: the reverse index is now file-backed for v2 segments, so
    // it too reports ~0 resident heap (the win this guards).
    assert_eq!(
        m.logical_index_bytes, 0,
        "v2 mmap logical index should be file-backed (0 resident heap)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn logical_index_v2_delete_after_reopen() {
    // ADR-020 Item 2: after reopen the base segment is a v2 mmap whose reverse
    // index is the binary-searched on-disk columns. Delete must still find every
    // local for a logical id, and the columns stay file-backed (0 resident).
    let dir = test_dir("li_v2_delete");
    let queries = sample_queries();
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut eng = Engine::with_config(make_norm(), cfg());
        eng.build_from_queries(&queries);
    }
    let mut eng = Engine::open(make_norm(), cfg()).expect("reopen");
    let title = "1986 Fleer Michael Jordan Rookie PSA 10";
    assert!(
        match_ids(&eng, title).contains(&1),
        "query 1 should match before delete"
    );
    let deleted = eng.delete_by_logical_id(1).expect("delete");
    assert!(
        deleted >= 1,
        "delete should tombstone at least one local for logical 1"
    );
    assert!(
        !match_ids(&eng, title).contains(&1),
        "query 1 must not match after delete"
    );
    // A different query is unaffected.
    assert!(match_ids(&eng, "LeBron James Rookie").contains(&2));
    assert_eq!(
        eng.metrics().logical_index_bytes,
        0,
        "v2 reverse index stays file-backed"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn logical_index_v1_backcompat_reconstruct() {
    // A pre-Item-2 (v1) segment has no column section; opening it must
    // reconstruct the reverse index from `logical_arr` and behave identically.
    // Simulate a v1 file by downgrading a freshly written v2 segment's header
    // (version → 1, logical_off → 0) and fixing the trailing CRC, then reopen.
    let dir = test_dir("li_v1_backcompat");
    let queries = sample_queries();
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };

    // Expected matches from a normal (v2) build.
    let title = "1986 Fleer Michael Jordan Rookie PSA 10";
    let expected = {
        let mut eng = Engine::with_config(make_norm(), cfg());
        eng.build_from_queries(&queries);
        match_ids(&eng, title)
    };

    // Downgrade every on-disk .seg to a v1-shaped header + CRC.
    let seg_dir = dir.join("segments");
    for entry in std::fs::read_dir(&seg_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("seg") {
            continue;
        }
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[4..8].copy_from_slice(&1u32.to_le_bytes()); // FORMAT_VERSION → 1
        bytes[56..64].copy_from_slice(&0u64.to_le_bytes()); // logical_index_off → 0
        let n = bytes.len();
        let crc = percolator::storage::crc32(&bytes[..n - 4]);
        bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();
    }

    // Reopen: the v1 path reconstructs the reverse index from logical_arr.
    let mut eng = Engine::open(make_norm(), cfg()).expect("reopen v1");
    assert_eq!(
        match_ids(&eng, title),
        expected,
        "v1-reconstructed segment must match identically to v2"
    );
    // The reverse index is owned (resident) for v1 — but flat, far below the old
    // per-logical Vec map (here just non-negative; the point is it's reconstructed).
    let _ = eng.metrics().logical_index_bytes;
    // Delete still finds the local via the reconstructed columns.
    assert!(eng.delete_by_logical_id(1).expect("delete") >= 1);
    assert!(!match_ids(&eng, title).contains(&1));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Hand-write a legacy v1 `sources.dat` (unordered records) for back-compat tests.
fn write_v1_sources(path: &std::path::Path, entries: &[(u64, &str)]) {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SRCS");
    buf.extend_from_slice(&1u32.to_le_bytes()); // version 1
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (id, text) in entries {
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(&(text.len() as u32).to_le_bytes());
        buf.extend_from_slice(text.as_bytes());
    }
    std::fs::write(path, buf).unwrap();
}

#[test]
fn lazy_sources_round_trip_and_reopen() {
    // retain_source = false: source text lives on disk (mmap'd v2), not resident.
    // Matching is unaffected (source text is never on the match path); _source
    // reads come from the file.
    let dir = test_dir("lazy_sources");
    let queries = sample_queries();
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        retain_source: false,
        ..EngineConfig::default()
    };

    {
        let mut eng = Engine::with_config(make_norm(), cfg());
        eng.build_from_queries(&queries);
        // _source resolves via the mmap base after the bulk commit re-map.
        assert_eq!(
            eng.get_query_source(1).as_deref(),
            Some("michael jordan 1986 fleer")
        );
        assert_eq!(
            eng.get_query_source(10).as_deref(),
            Some("patrick mahomes prizm rookie")
        );
        assert!(eng.get_query_source(999).is_none());

        // Resident source bytes are ~overlay-only (empty after flush), far below
        // holding all source text in RAM.
        let total_text: usize = queries.iter().map(|(_, s)| s.len()).sum();
        let m = eng.metrics();
        assert!(
            m.query_store_bytes < total_text,
            "lazy query_store_bytes {} should be < total source text {}",
            m.query_store_bytes,
            total_text
        );

        // Matches are identical to a retain_source=true engine.
        let mem = {
            let mut e = Engine::new(make_norm());
            e.build_from_queries(&queries);
            e
        };
        for title in [
            "1986 Fleer Michael Jordan PSA 10",
            "Luka Doncic Prizm Silver",
        ] {
            assert_eq!(
                match_ids(&eng, title),
                match_ids(&mem, title),
                "lazy-source engine must match identically for {title:?}"
            );
        }
    }

    // Reopen lazily; sources still readable from the mmap'd file.
    let eng = Engine::open(make_norm(), cfg()).expect("reopen");
    assert_eq!(
        eng.get_query_source(3).as_deref(),
        Some("kobe bryant psa 10")
    );
    assert_eq!(
        eng.get_query_source(7).as_deref(),
        Some("luka doncic prizm silver")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sources_v1_backcompat_and_migration() {
    use percolator::storage::SourceStore;
    let dir = test_dir("sources_v1");
    let path = dir.join("sources.dat");
    write_v1_sources(&path, &[(1, "alpha"), (2, "bravo"), (5, "echo")]);

    // Resident reads a v1 file directly.
    let r = SourceStore::open(&path, true).unwrap();
    assert_eq!(r.get(1).as_deref(), Some("alpha"));
    assert_eq!(r.get(5).as_deref(), Some("echo"));
    assert!(r.get(3).is_none());

    // Lazy migrates v1 → v2 on open, then reads from the mmap.
    let l = SourceStore::open(&path, false).unwrap();
    assert_eq!(l.get(2).as_deref(), Some("bravo"));
    assert_eq!(l.get(5).as_deref(), Some("echo"));
    assert!(l.get(99).is_none());

    // The file is now v2 — re-opening lazily still works.
    let l2 = SourceStore::open(&path, false).unwrap();
    assert_eq!(l2.get(1).as_deref(), Some("alpha"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn lazy_overlay_insert_and_tombstone() {
    use percolator::storage::SourceStore;
    let dir = test_dir("lazy_overlay");
    let path = dir.join("sources.dat");

    // Absent file → empty lazy store; mutations land in the overlay.
    let s = SourceStore::open(&path, false).unwrap();
    assert!(s.get(1).is_none());
    s.insert(1, "one".to_string());
    s.insert(2, "two".to_string());
    assert_eq!(s.get(1).as_deref(), Some("one"));
    s.remove(1); // overlay tombstone
    assert!(s.get(1).is_none());
    assert_eq!(s.get(2).as_deref(), Some("two"));

    // write_to persists only live entries; reopening reads them and the
    // tombstone is gone (id 1 absent, id 2 present).
    s.write_to(&path).unwrap();
    let s2 = SourceStore::open(&path, false).unwrap();
    assert_eq!(s2.get(2).as_deref(), Some("two"));
    assert!(s2.get(1).is_none());
    let _ = std::fs::remove_dir_all(&dir);
}
