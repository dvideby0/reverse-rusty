//! Scenario B — kill mid memtable-flush (segment write + manifest commit). A low
//! `memtable_flush_threshold` makes inserts internally seal segments, so a kill in
//! the insert loop can tear a `.seg` write or the manifest atomic-rename commit
//! point. The invariant: the commit is all-or-nothing — either the new segment is
//! in the manifest, or its mutations are still in the un-reset WAL — so every acked
//! id is matchable either way (a torn `.seg` is skipped on open, but its WAL frames
//! recover).

use crate::harness::{
    crash_iters, full_reference, jitter_for, reopen_and_diff, spawn_and_kill, unique_dir, Corpus,
    Trigger,
};

#[test]
#[ignore = "crash-injection: spawns + SIGKILLs a real process; run via the check.sh crash lane or `cargo test --release --test crash_injection -- --ignored`"]
fn flush_acked_writes_survive_sigkill() {
    let corpus = Corpus::generate("flush", 0xC0DE_0002, 16_000, 500);
    let full = full_reference(&corpus);
    let iters = crash_iters();
    let mut exercised = 0usize;
    // fsync=false (the realistic SIGKILL setting): the flush commit point — the
    // segment write + manifest atomic-rename — is fsync'd by the manifest writer
    // regardless of `wal_sync_on_write`, so the WAL durability mode is not the lever
    // here (that is proved by the wal_append scenario in both modes); running one
    // mode keeps the slow device-flush count down.
    let fsync = false;
    for i in 0..iters {
        let dir = unique_dir("flush");
        let res = spawn_and_kill(
            "flush",
            &dir,
            &corpus.tsv,
            &[],
            fsync,
            Trigger::Acks(2_000),
            jitter_for(i),
        );
        assert!(
            res.killed,
            "[flush] writer finished before the kill — raise the corpus size"
        );
        exercised += reopen_and_diff(
            &dir,
            &corpus,
            &full,
            &res.acked,
            &res.tombed,
            fsync,
            &format!("flush/iter={i}"),
        );
    }
    assert!(
        exercised > 0,
        "flush: no iteration produced a match (degenerate corpus)"
    );
}
