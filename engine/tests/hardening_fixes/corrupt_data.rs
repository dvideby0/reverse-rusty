//! Fix 2: corrupt data graceful handling (no panics).

use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

use crate::harness::{make_norm, sample_queries, test_dir};

#[test]
fn corrupt_wal_file_recovers_gracefully() {
    let dir = test_dir("corrupt_wal");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.insert_live("michael jordan 1986 fleer", 1, 1);
    engine.insert_live("lebron james rookie", 2, 1);
    engine.flush();

    // Append garbage to the WAL file (simulates torn write)
    let wal_path = dir.join("wal.log");
    if wal_path.exists() {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        f.write_all(&[0xFF; 37]).unwrap(); // corrupt trailing data
    }

    // Reopen should succeed — corrupt tail is skipped
    let reopened = Engine::open(make_norm(), config);
    assert!(
        reopened.is_ok(),
        "engine should open despite corrupt WAL tail"
    );
}

#[test]
fn corrupt_segment_file_skipped_on_open() {
    let dir = test_dir("corrupt_seg");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&sample_queries()[..5]);
    engine.flush();
    drop(engine);

    // Corrupt a segment file
    let seg_dir = dir.join("segments");
    if let Ok(entries) = std::fs::read_dir(&seg_dir) {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|e| e == "seg") {
                // Overwrite the middle of the file with garbage
                let data = std::fs::read(entry.path()).unwrap();
                if data.len() > 20 {
                    let mut corrupted = data;
                    for b in &mut corrupted[10..20] {
                        *b = 0xDE;
                    }
                    std::fs::write(entry.path(), &corrupted).unwrap();
                }
                break;
            }
        }
    }

    // Reopen should succeed — corrupt segment is skipped
    let reopened = Engine::open(make_norm(), config);
    assert!(
        reopened.is_ok(),
        "engine should open despite corrupt segment"
    );
    let engine = reopened.unwrap();
    assert!(
        engine.skipped_segments > 0,
        "should report skipped segments"
    );
}
