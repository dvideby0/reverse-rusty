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

/// Hand-write the original query-only v2 format (no metadata footer).
fn write_v2_sources(path: &std::path::Path, entries: &[(u64, &str)]) {
    let mut entries = entries.to_vec();
    entries.sort_unstable_by_key(|(id, _)| *id);
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SRCS");
    buf.extend_from_slice(&2u32.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    let mut blob = Vec::new();
    for (id, text) in entries {
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(text.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        blob.extend_from_slice(text.as_bytes());
    }
    buf.extend_from_slice(&blob);
    let crc = reverse_rusty::storage::crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
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

    // Lazy migrates v1 → extended v2 on open, then reads from the mmap.
    let l = SourceStore::open(&path, false).unwrap();
    assert_eq!(l.get(2).as_deref(), Some("bravo"));
    assert_eq!(l.get(5).as_deref(), Some("echo"));
    assert!(l.get(99).is_none());

    // The file is now extended v2 — re-opening lazily still works.
    let l2 = SourceStore::open(&path, false).unwrap();
    assert_eq!(l2.get(1).as_deref(), Some("alpha"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn original_v2_without_metadata_footer_stays_readable() {
    use reverse_rusty::storage::SourceStore;
    let dir = test_dir("sources_v2");
    let path = dir.join("sources.dat");
    write_v2_sources(&path, &[(5, "echo"), (1, "alpha"), (2, "bravo")]);

    let resident = SourceStore::open(&path, true).unwrap();
    assert_eq!(resident.get(1).as_deref(), Some("alpha"));
    let legacy = resident.get_document(2).expect("legacy document");
    assert_eq!(legacy.version(), 1);
    assert!(!legacy.tags_known());

    let lazy = SourceStore::open(&path, false).unwrap();
    assert_eq!(lazy.get(5).as_deref(), Some("echo"));
    assert!(!lazy
        .get_document(5)
        .expect("migrated document")
        .tags_known());
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 2);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_source_recovers_live_version_and_dense_tags() {
    let dir = test_dir("sources_v2_live_metadata");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_insert_live_with_tags(
                "topps chrome",
                7,
                42,
                &[("category".to_string(), "cards".to_string())],
            )
            .expect("tagged insert");
        engine.flush();
    }

    // Replace the current v8 segment with the equivalent pre-generation shape:
    // public Segment builders intentionally write generation zero, while the
    // initial durable engine above supplied the matching dict + tag dictionary
    // in the manifest. This models a real legacy segment paired with an original
    // query-only v2 source file; merely replacing v8's sidecar would correctly
    // be rejected as stale.
    let norm = make_norm();
    let mut dict = reverse_rusty::dict::Dict::new();
    let ast = reverse_rusty::dsl::parse("topps chrome").expect("legacy query");
    let mut lc = String::new();
    let ex = reverse_rusty::compile::extract(&ast, &norm, &mut dict, &mut lc);
    let mut legacy_segment = reverse_rusty::segment::Segment::new();
    legacy_segment
        .add_compiled(
            &ex,
            &[0],
            &dict,
            7,
            42,
            reverse_rusty::segment::CompileKnobs {
                accept_class_d: true,
                hot_anchor_threshold: 0,
                dedup_bodies: true,
            },
        )
        .expect("legacy segment row");
    reverse_rusty::storage::write_segment(
        &legacy_segment,
        &dir.join("segments").join("seg_000001.seg"),
    )
    .expect("write generation-zero segment");

    // The point read inherits version/tags only because BOTH durable domains
    // explicitly carry the legacy generation zero.
    write_v2_sources(&dir.join("sources.dat"), &[(7, "topps chrome")]);
    let engine = Engine::open(make_norm(), cfg()).expect("open legacy sources");
    let source = engine
        .snapshot()
        .get_query_document(7)
        .expect("reconstructed document");
    assert_eq!(source.version(), 42);
    assert!(source.tags_known());
    assert_eq!(
        source.tags(),
        [("category".to_string(), "cards".to_string())]
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn same_version_stale_source_sidecar_fails_loud_after_reopen() {
    let dir = test_dir("sources_same_version_generation");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let stale_sources = {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_upsert_live_with_tags(
                "1994 topps",
                7,
                1,
                &[("status".to_string(), "old".to_string())],
            )
            .expect("first write");
        engine.flush();
        let stale = std::fs::read(dir.join("sources.dat")).expect("first source sidecar");

        engine
            .try_upsert_live_with_tags(
                "1995 fleer",
                7,
                1,
                &[("status".to_string(), "new".to_string())],
            )
            .expect("same-version replacement");
        engine.flush();
        stale
    };

    // Model a failed/torn best-effort sidecar publication: the manifest and v8
    // exact row name the second accepted write, while sources.dat is still the
    // first write and carries the same caller-visible version.
    std::fs::write(dir.join("sources.dat"), stale_sources).expect("restore stale sidecar");
    let engine = Engine::open(make_norm(), cfg()).expect("reopen with stale source");
    let snapshot = engine.snapshot();
    assert!(snapshot.has_live_query(7), "the exact row remains live");
    assert!(
        snapshot.get_query_document(7).is_none(),
        "internal generation mismatch must be source unavailability, never stale _source"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn live_then_bulk_same_id_keeps_newer_source_across_reopen_and_rebuild() {
    let dir = test_dir("sources_live_bulk_reopen");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_insert_live_with_tags(
                "1994 topps",
                7,
                1,
                &[("status".to_string(), "old".to_string())],
            )
            .expect("live insert");
        let rows = [(7, "1995 fleer".to_string())];
        let tags = [vec![("status".to_string(), "new".to_string())]];
        let (report, _) = engine
            .try_bulk_ingest_detailed_with_tags(&rows, &tags)
            .expect("later bulk commit");
        assert_eq!(report.ingested, 1);
        assert_eq!(
            engine
                .snapshot()
                .get_query_document(7)
                .expect("newest live document")
                .query(),
            "1995 fleer"
        );
        // Crash/drop without flushing the older WAL-backed memtable row.
    }

    let mut reopened = Engine::open(make_norm(), cfg()).expect("reopen");
    let document = reopened
        .snapshot()
        .get_query_document(7)
        .expect("bulk source must still pair with the newer base exact row");
    assert_eq!(document.query(), "1995 fleer");
    assert_eq!(document.tags(), [("status".to_string(), "new".to_string())]);

    reopened
        .set_vocab(reverse_rusty::vocab::Vocab::default())
        .expect("coherent reopened corpus remains rebuildable");
    assert_eq!(reopened.recompile_stale_segments(), 1);
    assert!(match_ids(&reopened, "1995 fleer").contains(&7));
    assert!(
        !match_ids(&reopened, "1994 topps").contains(&7),
        "the older replayed source must not replace the bulk document during rebuild"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn missing_source_store_blocks_vocab_change_without_dropping_live_rows() {
    let dir = test_dir("sources_missing_vocab_guard");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_insert_live("1995 fleer", 7, 1)
            .expect("live insert");
        engine.flush();
    }
    std::fs::remove_file(dir.join("sources.dat")).expect("remove source store");

    let mut reopened = Engine::open(make_norm(), cfg()).expect("reopen");
    assert!(match_ids(&reopened, "1995 fleer").contains(&7));
    let epoch = reopened.vocab_epoch();
    let error = reopened
        .set_vocab(reverse_rusty::vocab::Vocab::default())
        .expect_err("a partial rebuild corpus must fail before changing the normalizer");
    assert!(
        error.to_string().contains("logical id 7"),
        "error must identify the uncovered live row: {error}"
    );
    assert_eq!(reopened.vocab_epoch(), epoch);
    assert!(!reopened.has_stale_segments());
    assert!(
        match_ids(&reopened, "1995 fleer").contains(&7),
        "a rejected vocabulary change must leave acknowledged matching state intact"
    );

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
