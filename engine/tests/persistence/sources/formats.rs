use super::*;

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
            Some("wireless mouse 1986 vertex")
        );
        assert_eq!(
            eng.get_query_source(10).as_deref(),
            Some("action camera contoso new")
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
        for title in ["1986 Vertex Wireless Mouse PRO", "USB Hub Contoso Silver"] {
            assert_eq!(
                match_ids(&eng, title),
                match_ids(&mem, title),
                "lazy-source engine must match identically for {title:?}"
            );
        }
    }

    // Reopen lazily; sources still readable from the mmap'd file.
    let mut eng = Engine::open(make_norm(), cfg()).expect("reopen");
    assert_eq!(
        eng.get_query_source(3).as_deref(),
        Some("noise cancelling headphones pro")
    );
    assert_eq!(
        eng.get_query_source(7).as_deref(),
        Some("usb hub contoso silver")
    );

    // A later WAL-backed flush selects and remaps a new immutable generation;
    // neither the old mmap base nor an overlay duplicate may hide the new row.
    let selected_before = committed_source_path(&dir);
    eng.try_insert_live("product omega preview", 100, 1)
        .expect("lazy live insert");
    eng.flush();
    let selected_after = committed_source_path(&dir);
    assert_ne!(selected_after, selected_before);
    assert_eq!(
        eng.get_query_source(100).as_deref(),
        Some("product omega preview")
    );
    drop(eng);
    let eng = Engine::open(make_norm(), cfg()).expect("reopen remapped lazy generation");
    assert_eq!(
        eng.get_query_source(100).as_deref(),
        Some("product omega preview")
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
                "acme chrome",
                7,
                42,
                &[("category".to_string(), "items".to_string())],
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
    let ast = reverse_rusty::dsl::parse("acme chrome").expect("legacy query");
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
    write_v2_sources(&dir.join("sources.dat"), &[(7, "acme chrome")]);
    let mut manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("read manifest");
    manifest.source_file_name = "sources.dat".to_string();
    manifest.source_generation_fence = false;
    reverse_rusty::storage::write_manifest(&manifest, &dir.join("manifest.bin"))
        .expect("select legacy source sidecar");
    let engine = Engine::open(make_norm(), cfg()).expect("open legacy sources");
    let source = engine
        .snapshot()
        .get_query_document(7)
        .expect("reconstructed document");
    assert_eq!(source.version(), 42);
    assert!(source.tags_known());
    assert_eq!(
        source.tags(),
        [("category".to_string(), "items".to_string())]
    );

    let _ = std::fs::remove_dir_all(&dir);
}
