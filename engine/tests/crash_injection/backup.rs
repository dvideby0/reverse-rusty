//! Scenario D — kill mid-backup. The writer seals a base segment, signals FLUSHED,
//! then loops `backup_to` to fresh dirs so the kill lands in a copy. The invariant:
//! `backup_to` only READS the source and writes to a separate dest, so a torn backup
//! must never corrupt the source — reopening the SOURCE recovers every acked id
//! intact. (The source's own durability is the guarantee under test here; the
//! partial dest's fail-loud `open` is covered by the persistence-suite backup tests.)

use std::time::Duration;

use crate::harness::{
    crash_iters, full_reference_prefix, reopen_and_diff, spawn_and_kill, unique_dir, Corpus,
    Trigger,
};

#[test]
#[ignore = "crash-injection: spawns + SIGKILLs a real process; run via the check.sh crash lane or `cargo test --release --test crash_injection -- --ignored`"]
fn backup_source_survives_sigkill_during_backup() {
    // The writer seals only the first LIMIT queries, so the FP reference is bounded to
    // that slice — a recovered id beyond it (which could never have been written) is a
    // false positive, not silently allowed (codex review).
    const LIMIT: usize = 6_000;
    let corpus = Corpus::generate("backup", 0xC0DE_0004, 16_000, 500);
    let full = full_reference_prefix(&corpus, LIMIT);
    let iters = crash_iters();
    let mut exercised = 0usize;
    for i in 0..iters {
        let src = unique_dir("backup_src");
        let dest_root = unique_dir("backup_dest");
        let extra = vec![
            "--limit".to_string(),
            LIMIT.to_string(),
            "--backup-dest".to_string(),
            dest_root.to_string_lossy().into_owned(),
        ];
        // Kill after FLUSHED + a jitter so a backup copy is genuinely in progress.
        let res = spawn_and_kill(
            "backup",
            &src,
            &corpus.tsv,
            &extra,
            false,
            Trigger::Flushed,
            Duration::from_micros(400 + (i as u64 % 6) * 350),
        );
        assert!(
            res.killed,
            "[backup] writer exited before the kill (it should loop backups forever)"
        );
        exercised += reopen_and_diff(
            &src,
            &corpus,
            &full,
            &res.acked,
            &res.tombed,
            false,
            &format!("backup/iter={i}"),
        );
    }
    assert!(
        exercised > 0,
        "backup: no iteration produced a match (degenerate corpus)"
    );
}
