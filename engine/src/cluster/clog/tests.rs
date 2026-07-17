//! Unit tests for the cluster log: frame round-trips (incl. the ADR-070 `Upsert`),
//! cursor replay, torn-tail recovery, checkpoint truncation, write-fault surfacing,
//! and the `NullClusterLog` backend.

use super::*;

fn scratch_path(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "reverse_rusty_clog_{}_{}.log",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
    p
}

fn add(logical: u64, dsl: &str) -> ClusterMutation {
    ClusterMutation::Add {
        logical,
        version: 1,
        dsl: dsl.to_string(),
        tags: Vec::new(),
        placement: crate::ownership::QueryPlacement::standalone(),
    }
}

#[test]
fn append_then_replay_round_trips() {
    let path = scratch_path("roundtrip");
    {
        let log = FileClusterLog::open(&path, true, LogPos(0)).unwrap();
        assert_eq!(log.append(&add(1, "1994 upper deck")).unwrap(), LogPos(1));
        assert_eq!(
            log.append(&ClusterMutation::Remove { logical: 1 }).unwrap(),
            LogPos(2)
        );
        assert_eq!(log.append(&add(2, "topps chrome")).unwrap(), LogPos(3));
        assert_eq!(log.last_pos().unwrap(), LogPos(3));
    }
    // Reopen and replay from the start.
    let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
    let replay = log.replay(LogPos(0)).unwrap();
    assert_eq!(replay.skipped_bytes, 0);
    assert_eq!(replay.entries.len(), 3);
    assert_eq!(replay.entries[0], (LogPos(1), add(1, "1994 upper deck")));
    assert_eq!(
        replay.entries[1],
        (LogPos(2), ClusterMutation::Remove { logical: 1 })
    );
    // next_seq stays monotonic across reopen.
    assert_eq!(log.append(&add(3, "x")).unwrap(), LogPos(4));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn upsert_frame_round_trips_with_and_without_tags() {
    let path = scratch_path("upsert");
    let tagged = ClusterMutation::Upsert {
        logical: 7,
        version: 2,
        dsl: "psa 10 charizard".to_string(),
        tags: vec![("category".into(), "cards".into())],
        placement: crate::ownership::QueryPlacement::standalone(),
    };
    let untagged = ClusterMutation::Upsert {
        logical: 8,
        version: 1,
        dsl: "topps chrome".to_string(),
        tags: Vec::new(),
        placement: crate::ownership::QueryPlacement::standalone(),
    };
    {
        let log = FileClusterLog::open(&path, true, LogPos(0)).unwrap();
        log.append(&add(7, "old version")).unwrap();
        log.append(&tagged).unwrap();
        log.append(&untagged).unwrap();
    }
    // Reopen and replay: the mixed Add/Upsert stream survives byte-exact, in order.
    let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
    let replay = log.replay(LogPos(0)).unwrap();
    assert_eq!(replay.skipped_bytes, 0);
    assert_eq!(replay.entries.len(), 3);
    assert_eq!(replay.entries[1].1, tagged);
    assert_eq!(replay.entries[2].1, untagged);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn v4_round_trips_ownership_and_v3_is_refused() {
    let path = scratch_path("ownership_v4");
    let placement = crate::ownership::QueryPlacement::selective(
        crate::ownership::PlacementGeneration(9),
        16,
        vec![2, 7, 11],
    )
    .expect("placement");
    let mutation = ClusterMutation::Add {
        logical: 44,
        version: 5,
        dsl: "1994 upper deck".into(),
        tags: vec![("category".into(), "cards".into())],
        placement,
    };
    {
        let log = FileClusterLog::open(&path, true, LogPos(0)).expect("open v4");
        log.append(&mutation).expect("append");
        assert_eq!(
            log.replay(LogPos(0)).expect("replay").entries[0].1,
            mutation
        );
    }
    let bytes = std::fs::read(&path).expect("read log");
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().expect("version")),
        4
    );

    let mut legacy = bytes;
    legacy[4..8].copy_from_slice(&3u32.to_le_bytes());
    std::fs::write(&path, legacy).expect("write legacy header");
    let error = FileClusterLog::open(&path, false, LogPos(0))
        .err()
        .expect("v3 must fail");
    assert!(
        error.to_string().contains("predates ADR-109") && error.to_string().contains("rebuild"),
        "got: {error}"
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn replay_from_cursor_skips_captured_prefix() {
    let path = scratch_path("cursor");
    let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
    for i in 1..=5 {
        log.append(&add(i, "q")).unwrap();
    }
    let replay = log.replay(LogPos(3)).unwrap();
    let positions: Vec<u64> = replay.entries.iter().map(|(p, _)| p.0).collect();
    assert_eq!(positions, vec![4, 5]);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn torn_tail_is_dropped_not_fatal() {
    let path = scratch_path("torn");
    {
        let log = FileClusterLog::open(&path, true, LogPos(0)).unwrap();
        log.append(&add(1, "alpha")).unwrap();
        log.append(&add(2, "beta")).unwrap();
    }
    // Corrupt the tail by appending junk that can't frame a valid record.
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&[0xFF, 0xFF, 0xFF, 0x7F, 0xAA, 0xBB]).unwrap();
    }
    let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
    let replay = log.replay(LogPos(0)).unwrap();
    assert_eq!(replay.entries.len(), 2, "the two whole records survive");
    assert!(replay.skipped_bytes > 0, "torn tail counted");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn checkpoint_truncates_captured_records() {
    let path = scratch_path("checkpoint");
    let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
    for i in 1..=5 {
        log.append(&add(i, "q")).unwrap();
    }
    let size_before = std::fs::metadata(&path).unwrap().len();
    log.checkpoint(LogPos(3)).unwrap();
    let size_after = std::fs::metadata(&path).unwrap().len();
    assert!(size_after < size_before, "captured prefix dropped");
    // Only records after the cursor remain; new appends stay monotonic.
    let replay = log.replay(LogPos(0)).unwrap();
    let positions: Vec<u64> = replay.entries.iter().map(|(p, _)| p.0).collect();
    assert_eq!(positions, vec![4, 5]);
    assert_eq!(log.append(&add(6, "q")).unwrap(), LogPos(6));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn append_surfaces_write_errors() {
    let path = scratch_path("writefault");
    let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
    assert!(log.append(&add(1, "ok")).is_ok());
    log.break_writes_for_test();
    assert!(matches!(log.append(&add(2, "no")), Err(ShardError::Log(_))));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn null_log_assigns_positions_and_replays_empty() {
    let log = NullClusterLog::new();
    assert_eq!(log.append(&add(1, "q")).unwrap(), LogPos(1));
    assert_eq!(log.append(&add(2, "q")).unwrap(), LogPos(2));
    assert_eq!(log.last_pos().unwrap(), LogPos(2));
    assert_eq!(log.replay(LogPos(0)).unwrap().entries.len(), 0);
    log.checkpoint(LogPos(2)).unwrap();
}
