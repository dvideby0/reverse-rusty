//! Scenario G — the multi-reopen watermark hazard (ADR-066 `ensure_seq_after`). A
//! delete appended AFTER a reopen — when that reopen followed a flush that set the
//! manifest's `wal_seq_watermark = N` and reset the WAL — must get a WAL seq > N, or
//! the NEXT reopen skips it (it looks already-captured by the checkpoint) and the
//! deleted query RESURRECTS. The single-reopen churn scenario cannot reach this: its
//! deletes share the inserts' un-reset WAL generation, so no watermark undercuts them.
//!
//! The real SIGKILL is load-bearing here. It leaves the post-reopen canary delete as a
//! bare, UNSEALED WAL tail frame, so the second reopen's replay-or-skip decision
//! actually exercises the seq-vs-watermark comparison — the exact state a clean exit
//! (which could seal the delete into a segment) or a simulation cannot produce.
//!
//! Flow per iteration: parent flushes a base (sets `N`, resets the WAL, canary on disk)
//! → worker reopens (#1, `ensure_seq_after` re-pins past `N`), deletes the canary
//! (`seq = N+1`), then churns throwaways until SIGKILLed → parent reopens (#2) and
//! asserts the canary stayed ABSENT (a broken re-pin resurrects it as a false positive
//! via its self-matching title).

use crate::harness::{
    build_base, crash_iters, full_reference, jitter_for, reopen_and_diff, spawn_and_kill,
    unique_dir, Corpus, Trigger,
};

#[test]
#[ignore = "crash-injection: spawns + SIGKILLs a real process; run via the check.sh crash lane or `cargo test --release --test crash_injection -- --ignored`"]
fn watermark_post_reopen_delete_survives_second_reopen_under_sigkill() {
    // ONE prepended canary (index 0, id 9_000_000) with a self-matching title — a
    // resurrected delete shows up as that canary matching its own title.
    const CANARIES: usize = 1;
    let corpus = Corpus::generate_with_canaries("watermark", 0xC0DE_0007, 12_000, 400, CANARIES);
    let full = full_reference(&corpus);
    let canary_id = corpus.queries[0].0;
    // Every base id EXCEPT the canary must survive (present); the canary must be ABSENT.
    let acked: Vec<u64> = corpus.queries[1..].iter().map(|(id, _)| *id).collect();
    let tombed = vec![canary_id];

    let iters = crash_iters();
    let fsync = false;
    let mut exercised = 0usize;
    for i in 0..iters {
        let dir = unique_dir("watermark");
        // Setup: flush the base so `manifest.wal_seq_watermark = N (> 0)` and the WAL is
        // reset, with the canary a flushed BASE-segment query.
        build_base(&dir, &corpus.queries);
        // `--limit 1` makes the worker's slice exactly the prepended canary, so it
        // deletes id 9_000_000 as its first post-reopen durable op.
        let res = spawn_and_kill(
            "watermark",
            &dir,
            &corpus.tsv,
            &["--limit".to_string(), "1".to_string()],
            fsync,
            Trigger::Tombs(1),
            jitter_for(i),
        );
        assert!(
            res.killed,
            "[watermark] writer finished before the kill — the throwaway churn should loop forever"
        );
        assert_eq!(
            res.tombed,
            vec![canary_id],
            "[watermark] the canary delete was not observed — the watermark probe is vacuous"
        );
        // Reopen #2 + diff: the canary delete (seq N+1 > N) MUST replay, so the canary
        // stays ABSENT. A broken `ensure_seq_after` re-uses seq <= N, the second reopen
        // SKIPS the delete, and the canary resurrects (a false positive).
        exercised += reopen_and_diff(
            &dir,
            &corpus,
            &full,
            &acked,
            &tombed,
            fsync,
            &format!("watermark/iter={i}"),
        );
    }
    assert!(
        exercised > 0,
        "watermark: no iteration produced a match (degenerate corpus)"
    );
}
