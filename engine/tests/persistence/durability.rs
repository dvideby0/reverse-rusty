//! Durability + fail-closed behaviour: write failures surface as `DurabilityFailure`
//! events, failed flush/compaction roll back and recover, and recovery diagnostics
//! raised during `Engine::open` are buffered until an observer attaches.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

#[test]
fn durability_failure_surfaces_as_event() {
    // A persistence failure must reach the observability stack as a
    // `DurabilityFailure` event (not just stderr), so an operator can alert.
    // We isolate a single failure: make only `segments/` read-only, so the
    // flush's segment write fails while the WAL/manifest writes (data-dir root)
    // still succeed.
    use reverse_rusty::events::{DurabilityOp, EngineEvent};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Mutex};

    let dir = test_dir("durability_event");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        auto_compact_on_flush: false,
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config);

    let captured: Arc<Mutex<Vec<(DurabilityOp, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    engine.set_observer(move |ev: &EngineEvent| {
        if let EngineEvent::DurabilityFailure { op, error, .. } = ev {
            sink.lock().unwrap().push((*op, error.clone()));
        }
    });

    // Make segments/ read-only up front, then queue a mutation (WAL + memtable
    // only — both succeed) and flush (the segment write fails).
    let seg_dir = dir.join("segments");
    let orig = std::fs::metadata(&seg_dir).unwrap().permissions();
    std::fs::set_permissions(&seg_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    engine.insert_live("michael jordan 1986 fleer", 1, 1);
    engine.flush();

    // Restore perms BEFORE asserting so temp-dir cleanup always works.
    std::fs::set_permissions(&seg_dir, orig).unwrap();

    let events = captured.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|(op, _)| *op == DurabilityOp::SegmentWrite),
        "a failed segment write must emit a SegmentWrite DurabilityFailure; got {events:?}"
    );
    assert!(
        events.iter().all(|(_, err)| !err.is_empty()),
        "each DurabilityFailure must carry the underlying error string"
    );
    assert!(
        !engine.persistence_healthy,
        "persistence must be marked unhealthy after the write failed"
    );
    // SegmentWrite means match data is at risk — operators should page on it.
    assert!(DurabilityOp::SegmentWrite.is_data_at_risk());

    drop(events);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failed_flush_retains_data_in_wal_and_recovers_on_reopen() {
    // ADR-051 (fail-closed flush): a flush that cannot durably write its segment
    // must NOT advance the WAL. The data falls back to an in-memory segment (still
    // served at runtime) AND stays in the WAL, so a reopen replays it. Before the
    // fix, the WAL was reset whenever the *manifest* write succeeded — and the
    // manifest excludes the in-memory fallback — so the acknowledged insert
    // silently vanished on restart.
    use std::os::unix::fs::PermissionsExt;

    let dir = test_dir("failed_flush_recovers");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX, // we flush explicitly
        auto_compact_on_flush: false,
        ..EngineConfig::default()
    };

    let t_jordan = "Michael Jordan 1986 Fleer Rookie PSA 10";
    let t_pippen = "Scottie Pippen 1988 Fleer";
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        // A durable base segment (writes a manifest), then a live insert that lives
        // only in the WAL + memtable until the (about-to-fail) flush.
        engine.build_from_queries(&[(1, "michael jordan 1986 fleer".into())]);
        engine.insert_live("scottie pippen 1988 fleer", 2, 1);
        assert_eq!(
            match_ids(&engine, t_pippen),
            vec![2],
            "pippen matches before flush"
        );

        // Make segments/ read-only so the flush's segment write fails.
        let seg_dir = dir.join("segments");
        let orig = std::fs::metadata(&seg_dir).unwrap().permissions();
        std::fs::set_permissions(&seg_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        engine.flush(); // segment write fails → in-memory fallback, WAL left intact
        std::fs::set_permissions(&seg_dir, orig).unwrap(); // restore before asserting

        assert!(
            !engine.persistence_healthy,
            "a failed flush must mark persistence unhealthy"
        );
        // The in-memory fallback still serves the query — no live false negative.
        assert_eq!(
            match_ids(&engine, t_pippen),
            vec![2],
            "the in-memory fallback segment still matches after the failed flush"
        );
        drop(engine); // simulate restart
    }

    // Reopen: the WAL must still hold the insert (it was never reset/checkpointed),
    // so recovery replays it. This is the durability guarantee the fix restores.
    let engine2 = Engine::open(make_norm(), config).expect("reopen after failed flush");
    assert_eq!(
        match_ids(&engine2, t_jordan),
        vec![1],
        "the durably-committed query survives reopen"
    );
    assert_eq!(
        match_ids(&engine2, t_pippen),
        vec![2],
        "the acknowledged insert survives a restart even though its flush failed"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failed_compaction_rolls_back_and_keeps_segments_on_disk() {
    // ADR-051 (fail-closed compaction): a compaction that cannot durably write its
    // merged segment must abort and roll back — the source segments stay on disk and
    // queryable. Before the fix, compaction deleted the old files and kept the merged
    // segment only in memory, so a restart found a manifest referencing deleted files
    // (or lost the merged data entirely).
    use std::os::unix::fs::PermissionsExt;

    let dir = test_dir("failed_compaction_rollback");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    // Two durable base segments.
    engine.build_from_queries(&[(1, "michael jordan 1986 fleer".into())]);
    engine.bulk_ingest(&[(2, "scottie pippen 1988 fleer".into())]);
    assert_eq!(
        engine.metrics().base_segments,
        2,
        "two base segments before compaction"
    );

    let t1 = "Michael Jordan 1986 Fleer";
    let t2 = "Scottie Pippen 1988 Fleer";
    assert_eq!(match_ids(&engine, t1), vec![1]);
    assert_eq!(match_ids(&engine, t2), vec![2]);

    let seg_dir = dir.join("segments");
    let count_seg_files = || {
        std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "seg"))
            .count()
    };
    assert_eq!(
        count_seg_files(),
        2,
        "two .seg files on disk before compaction"
    );

    // Make segments/ read-only so the merged-segment write fails.
    let orig = std::fs::metadata(&seg_dir).unwrap().permissions();
    std::fs::set_permissions(&seg_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let report = engine.compact_all();
    std::fs::set_permissions(&seg_dir, orig).unwrap(); // restore before asserting

    assert!(
        report.is_none(),
        "compaction must abort (return None) when it cannot durably commit"
    );
    assert!(
        !engine.persistence_healthy,
        "a failed compaction marks persistence unhealthy"
    );
    // Rolled back to the original two segments, both still queryable.
    assert_eq!(
        engine.metrics().base_segments,
        2,
        "compaction rolled back to the original 2 segments"
    );
    assert_eq!(
        match_ids(&engine, t1),
        vec![1],
        "query 1 intact after rollback"
    );
    assert_eq!(
        match_ids(&engine, t2),
        vec![2],
        "query 2 intact after rollback"
    );
    assert_eq!(
        count_seg_files(),
        2,
        "old segment files must NOT be deleted on a failed (rolled-back) compaction"
    );

    // Reopen cleanly: the manifest still references the two original files (the
    // failed compaction never rewrote it), so both queries recover.
    drop(engine);
    let engine2 = Engine::open(make_norm(), config).expect("reopen after rolled-back compaction");
    assert_eq!(match_ids(&engine2, t1), vec![1], "query 1 survives reopen");
    assert_eq!(match_ids(&engine2, t2), vec![2], "query 2 survives reopen");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn recovery_diagnostics_buffered_until_observer_attaches() {
    // Failures raised during `Engine::open` predate any observer. They must be
    // buffered and replayed when `set_observer` is called, so recovery
    // diagnostics (here: a corrupt segment skipped on reopen) still reach the
    // structured stack rather than being lost.
    use reverse_rusty::events::{DurabilityOp, EngineEvent};
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    let dir = test_dir("recovery_buffered_events");

    // 1) Build a persistent engine with one flushed base segment, then drop it.
    {
        let config = EngineConfig {
            data_dir: Some(dir.clone()),
            auto_compact_on_flush: false,
            ..EngineConfig::default()
        };
        let mut eng = Engine::with_config(make_norm(), config);
        eng.insert_live("michael jordan 1986 fleer", 1, 1);
        eng.flush();
        assert!(eng.num_segments() >= 1, "expected a flushed base segment");
    }

    // 2) Corrupt the on-disk .seg so MmapSegment::open fails on reopen.
    let seg_dir = dir.join("segments");
    let seg_file = std::fs::read_dir(&seg_dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "seg"))
        .expect("a .seg file should exist after flush");
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&seg_file)
            .unwrap();
        f.write_all(b"not a valid reverse-rusty segment").unwrap();
    }

    // 3) Reopen: the corrupt segment is skipped and the diagnostic is buffered
    //    (no observer attached yet).
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut eng =
        Engine::open(make_norm(), config).expect("open should succeed, skipping corrupt segment");
    assert!(
        eng.skipped_segments >= 1,
        "a corrupt segment should be skipped on recovery"
    );

    // 4) Attaching the observer must replay the buffered SegmentRecovery event.
    let captured: Arc<Mutex<Vec<DurabilityOp>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    eng.set_observer(move |ev: &EngineEvent| {
        if let EngineEvent::DurabilityFailure { op, .. } = ev {
            sink.lock().unwrap().push(*op);
        }
    });

    {
        let ops = captured.lock().unwrap();
        assert!(
            ops.contains(&DurabilityOp::SegmentRecovery),
            "set_observer must replay the buffered SegmentRecovery diagnostic; got {ops:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
