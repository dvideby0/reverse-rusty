use super::*;

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

/// A `.seg` whose per-query exact column overruns its blob must fail loud on
/// `MmapSegment::open` — NOT panic with an out-of-bounds slice on the hot path or in
/// compaction's `to_memory_segment` (B1 / ADR-052 extended to intra-section
/// consistency). `checked_section_end` proves each section's own length, but not that
/// `req_off[i] + req_len[i]` lands inside `req_blob`; a writer bug, a torn write that
/// re-passes CRC, or tampering could leave it overrunning. We build a real durable
/// segment, then hand-corrupt `req_len[0]` to a huge value, recompute the trailing
/// whole-file CRC so the file passes the CRC gate, and assert `open` returns `Err`.
#[test]
fn corrupt_req_column_fails_loud_on_open() {
    use reverse_rusty::storage::{crc32, MmapSegment};

    let dir = test_dir("corrupt_req_column");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    // A query with a required (non-anchor) tail feature so `req_blob` is populated
    // and `req_off`/`req_len` point into it.
    let mut eng = Engine::with_config(make_norm(), config);
    eng.build_from_queries(&[(1u64, "michael jordan 1986 fleer".into())]);
    drop(eng); // seal + flush the base segment to disk

    // Find the written .seg.
    let seg_dir = dir.join("segments");
    let seg_path = std::fs::read_dir(&seg_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
        .expect("a base segment .seg file");

    // Sanity: it opens cleanly before corruption.
    MmapSegment::open(&seg_path).expect("pristine segment opens");

    let mut bytes = std::fs::read(&seg_path).unwrap();
    let rd_u32 = |b: &[u8], off: usize| -> usize {
        u32::from_le_bytes(b[off..off + 4].try_into().unwrap()) as usize
    };
    let rd_u64 = |b: &[u8], off: usize| -> usize {
        u64::from_le_bytes(b[off..off + 8].try_into().unwrap()) as usize
    };
    let align8 = |x: usize| (x + 7) & !7;

    // Header: num_queries @ 8..12, exact_section_off @ 16..24.
    let nq = rd_u32(&bytes, 8);
    let exact_off = rd_u64(&bytes, 16);

    // Walk the exact section to the start of the `req_len` (u16) data column. Layout
    // (see write::write_exact_section): req_mask(u64), forb_mask(u64), req_off(u32),
    // req_len(u16), ... . u64 arrays are [count:u32][pad:4][data]; u32/u16 arrays are
    // [count:u32][data][pad_to_8].
    let mut cursor = exact_off;
    // req_mask (u64 array)
    cursor += 8 + nq * 8;
    // forb_mask (u64 array)
    cursor += 8 + nq * 8;
    // req_off (u32 array): [count:u32][data: nq*4][pad_to_8]
    cursor = align8(cursor + 4 + nq * 4);
    // req_len (u16 array): cursor now points at its [count:u32] header.
    let req_len_count = rd_u32(&bytes, cursor);
    assert_eq!(req_len_count, nq, "req_len column has one entry per query");
    let req_len_data = cursor + 4;
    // Overrun req_blob: set req_len[0] to u16::MAX so req_off[0] + req_len[0] far
    // exceeds req_blob.len().
    bytes[req_len_data..req_len_data + 2].copy_from_slice(&u16::MAX.to_le_bytes());

    // Recompute the trailing whole-file CRC so the file still passes the CRC gate;
    // only the structural validation should now reject it.
    let n = bytes.len();
    let crc = crc32(&bytes[..n - 4]);
    bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&seg_path, &bytes).unwrap();

    // The corrupt-but-CRC-valid segment must fail loud, not panic.
    let err =
        MmapSegment::open(&seg_path).expect_err("open must reject a column that overruns its blob");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::InvalidData,
        "corrupt segment must fail with InvalidData, got: {err}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
