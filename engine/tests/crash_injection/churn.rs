//! Scenario E — kill mid-churn (insert + interleaved self-delete). Crash-tests the
//! delete-recovery path (`DeleteByLogical`, ADR-066): every 7th query is durably
//! inserted and then deleted (`TOMB`), so on reopen its id must be ABSENT — a
//! resurrected delete is a false positive (the FP reference excludes the tombed
//! ids). The surviving acked queries must be present (zero FN). The no-flush config
//! keeps insert AND delete frames in the WAL tail, so reopen replays the pair.

use crate::harness::{
    crash_iters, full_reference, jitter_for, reopen_and_diff, spawn_and_kill, unique_dir, Corpus,
    Trigger,
};

#[test]
#[ignore = "crash-injection: spawns + SIGKILLs a real process; run via the check.sh crash lane or `cargo test --release --test crash_injection -- --ignored`"]
fn churn_acked_survive_and_deleted_stay_dead_under_sigkill() {
    // 16 prepended canaries (deleted via --delete-prefix) make a resurrected delete
    // observable through their self-matching titles.
    const CANARIES: usize = 16;
    let corpus = Corpus::generate_with_canaries("churn", 0xC0DE_0005, 16_000, 500, CANARIES);
    let full = full_reference(&corpus);
    let iters = crash_iters();
    let fsync = false;
    let delete_prefix = vec!["--delete-prefix".to_string(), CANARIES.to_string()];
    let mut exercised = 0usize;
    for i in 0..iters {
        let dir = unique_dir("churn");
        let res = spawn_and_kill(
            "churn",
            &dir,
            &corpus.tsv,
            &delete_prefix,
            fsync,
            Trigger::Acks(2_000),
            jitter_for(i),
        );
        assert!(
            res.killed,
            "[churn] writer finished before the kill — raise the corpus size"
        );
        assert!(
            !res.tombed.is_empty(),
            "[churn] no durable deletes observed before the kill — the resurrection check is vacuous"
        );
        exercised += reopen_and_diff(
            &dir,
            &corpus,
            &full,
            &res.acked,
            &res.tombed,
            fsync,
            &format!("churn/iter={i}"),
        );
    }
    assert!(
        exercised > 0,
        "churn: no iteration produced a match (degenerate corpus)"
    );
}
