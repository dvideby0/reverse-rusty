//! Query-source (`sources.dat`) persistence: bulk-ingest durability + roll-back,
//! lazy (mmap'd) sources, the v1→v2 back-compat migration, and overlay tombstones.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

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
    use reverse_rusty::storage::SourceStore;
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
    use reverse_rusty::storage::SourceStore;
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
