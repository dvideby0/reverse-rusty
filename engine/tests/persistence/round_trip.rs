//! Segment round-trip, mmap matching, reopen lifecycle, tag-column-on-mmap,
//! in-memory backward-compat, and the v1/v2 logical-index reverse-index paths.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::Engine;
use reverse_rusty::vocab::Vocab;

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
fn tagged_queries_survive_reopen_and_filter_on_mmap() {
    // The .seg v3 tag column (ADR-049) must survive reopen: build two queries that match
    // the same title but carry different category tags, persist, reopen (now mmap-backed),
    // and confirm a tag filter narrows correctly against the mmap'd tag column.
    let dir = test_dir("tagged_reopen");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let queries = vec![
        (1u64, "topps chrome".to_string()),
        (2u64, "topps chrome".to_string()),
    ];
    let tags = vec![
        vec![("category".to_string(), "cards".to_string())],
        vec![("category".to_string(), "coins".to_string())],
    ];
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged durable build");
    drop(engine);

    // Reopen — the base segment is now mmap'd, so the tag column is read from the v3 .seg.
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let snap = engine2.snapshot();
    let title = "2020 topps chrome update";

    let mut s = reverse_rusty::segment::MatchScratch::new();
    let mut out = Vec::new();

    snap.match_title(title, &mut s, &mut out, true);
    out.sort_unstable();
    assert_eq!(
        out,
        vec![1, 2],
        "both queries match the title unfiltered after reopen"
    );

    let cards = snap.compile_tag_predicate(&[("category".to_string(), vec!["cards".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &cards);
    out.sort_unstable();
    assert_eq!(
        out,
        vec![1],
        "category=cards narrows to query 1 on the reopened mmap segment"
    );

    let coins = snap.compile_tag_predicate(&[("category".to_string(), vec!["coins".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &coins);
    out.sort_unstable();
    assert_eq!(out, vec![2], "category=coins narrows to query 2");

    // A value never ingested matches nothing (safe `terms` semantics).
    let none = snap.compile_tag_predicate(&[("category".to_string(), vec!["stamps".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &none);
    assert!(out.is_empty(), "an unseen filter value returns ∅");

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
        let crc = reverse_rusty::storage::crc32(&bytes[..n - 4]);
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

/// A single-node DURABLE engine with an active MULTI-WORD alias (ADR-061) survives reopen with zero
/// false negatives, INCLUDING a post-reopen live insert. The cluster durability suites can't cover
/// this (the cluster refuses multi-word aliases), and it exercises the trickiest `adopt_vocab`
/// branch: a RECOVERED engine must restore the equivalence map by resolving the multi-word entity
/// (`term:new_york`) AS-IS against the persisted dict (where it is already dense), so both the
/// baked-in existing queries AND a fresh `ny` insert key the same id the title side resolves.
#[test]
fn durable_reopen_preserves_multiword_alias() {
    let dir = test_dir("durable_multiword_alias");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut vocab = Vocab::new();
    vocab.import_solr_aliases(
        "ny => new york",
        &Normalizer::default_vocab().expect("vocab"),
        &reverse_rusty::dict::Dict::new(),
    );

    // Build durable with the alias active, store a `new york` query, flush, close.
    {
        let mut eng = Engine::with_vocab(vocab.clone(), config.clone()).expect("with_vocab");
        eng.build_from_queries(&[(1, "new york yankees".into())]);
        assert!(
            match_ids(&eng, "ny yankees").contains(&1),
            "pre-persist: ny title reaches the new york query"
        );
        eng.flush();
    }

    // Reopen (server path): open with the rebuilt normalizer, then adopt the vocab on the recovered
    // engine to restore the equivalence map.
    let mut eng = Engine::open(vocab.to_normalizer().expect("norm"), config).expect("reopen");
    eng.adopt_vocab(vocab)
        .expect("adopt_vocab on recovered engine");

    // Existing query keeps the alias across reopen.
    assert!(
        match_ids(&eng, "ny yankees").contains(&1),
        "FN: existing multi-word alias query lost its reach after durable reopen"
    );
    // A post-reopen live insert gains the alias too (recovered-engine resolve-as-is keys the map on
    // the same dense id the title side resolves).
    eng.try_insert_live("ny mets", 2, 1)
        .expect("post-reopen insert");
    assert!(
        match_ids(&eng, "new york mets").contains(&2),
        "FN: post-reopen ny insert did not gain the multi-word alias"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Codex R13 (P1): a query in the WAL TAIL (inserted after the last flush) must recover with its
/// alias expansion. `Engine::open` replays the tail BEFORE any vocab is installed, and the
/// equivalence map is transient (never persisted in the dict) — so the pre-fix open + adopt order
/// recompiled those queries unexpanded, and a recovered `new york mets` query no longer reached a
/// `ny mets` title (a recovery false negative). Covers BOTH healing paths: `open_with_vocab`
/// (equivalences installed before replay — the server's path) and the legacy `open` +
/// `adopt_vocab` (which now detects the hazard and escalates to a full recompile).
#[test]
fn wal_tail_recovers_alias_expansion() {
    for use_open_with_vocab in [true, false] {
        let dir = test_dir(&format!("wal_tail_alias_{use_open_with_vocab}"));
        let config = EngineConfig {
            data_dir: Some(dir.clone()),
            ..EngineConfig::default()
        };
        let mut vocab = Vocab::new();
        vocab.import_solr_aliases(
            "ny => new york",
            &Normalizer::default_vocab().expect("vocab"),
            &reverse_rusty::dict::Dict::new(),
        );

        {
            let mut eng = Engine::with_vocab(vocab.clone(), config.clone()).expect("with_vocab");
            eng.build_from_queries(&[(99, "seed".into())]); // flushed base state
                                                            // The WAL-tail query: inserted live, never flushed.
            eng.try_insert_live("new york mets", 1, 1).expect("insert");
            assert!(match_ids(&eng, "ny mets").contains(&1), "pre-crash sanity");
        } // drop without a flush — the query survives only in the WAL

        let eng = if use_open_with_vocab {
            Engine::open_with_vocab(vocab.clone(), config).expect("open_with_vocab")
        } else {
            let mut e = Engine::open(vocab.to_normalizer().expect("norm"), config).expect("open");
            e.adopt_vocab(vocab.clone()).expect("adopt_vocab");
            e
        };
        assert!(
            match_ids(&eng, "ny mets").contains(&1),
            "FN: the WAL-tail query lost its alias expansion on recovery \
             (open_with_vocab={use_open_with_vocab})"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
