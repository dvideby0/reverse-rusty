//! Scenario C — kill mid-compaction (merge + manifest swap + WAL reset). A low
//! flush threshold + `max_segments=2` + `auto_compact_on_flush` makes the insert
//! loop continuously seal and then MERGE segments, so a kill can tear the merge,
//! the manifest swap that replaces N old segments with the merged one, or the
//! subsequent `wal.reset()`. The invariant exercises the ADR-066 watermark
//! (`ensure_seq_after`): a kill after the new manifest but before the WAL reset must
//! not double-apply or resurrect on the next open. Every acked id stays matchable.

use crate::harness::{
    crash_iters, full_reference, jitter_for, reopen_and_diff, spawn_and_kill, unique_dir, Corpus,
    Trigger,
};

#[test]
#[ignore = "crash-injection: spawns + SIGKILLs a real process; run via the check.sh crash lane or `cargo test --release --test crash_injection -- --ignored`"]
fn compaction_acked_writes_survive_sigkill() {
    let corpus = Corpus::generate("compact", 0xC0DE_0003, 16_000, 500);
    let full = full_reference(&corpus);
    let iters = crash_iters();
    let mut exercised = 0usize;
    // fsync=false (the realistic SIGKILL setting): the compaction commit — the merged
    // segment write + the manifest swap + the WAL reset — is sequenced by the
    // manifest atomic-rename (fsync'd regardless of `wal_sync_on_write`), so the WAL
    // mode is not the lever here. A higher ack trigger so several flush→merge cycles
    // have fired before the kill.
    let fsync = false;
    for i in 0..iters {
        let dir = unique_dir("compact");
        let res = spawn_and_kill(
            "compact",
            &dir,
            &corpus.tsv,
            &[],
            fsync,
            Trigger::Acks(2_500),
            jitter_for(i),
        );
        assert!(
            res.killed,
            "[compact] writer finished before the kill — raise the corpus size"
        );
        exercised += reopen_and_diff(
            &dir,
            &corpus,
            &full,
            &res.acked,
            &res.tombed,
            fsync,
            &format!("compact/iter={i}"),
        );
    }
    assert!(
        exercised > 0,
        "compact: no iteration produced a match (degenerate corpus)"
    );
}
