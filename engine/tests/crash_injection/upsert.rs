//! Scenario F — kill mid-upsert (atomic replace-by-id, ADR-067). The parent lays down
//! a FLUSHED base of the OLD query versions, the worker replaces each id via
//! `try_upsert_live` (one WAL frame: tombstone-old + insert-new), and is SIGKILLed
//! mid-loop. On reopen the ADR-067 contract holds per id: BOTH halves recover (the id
//! is its NEW version) or NEITHER (it keeps its OLD version) — never a half-state (a
//! VANISHED id, or a stale OLD version surviving alongside the new).
//!
//! ## Why the construction is race-immune
//! The crashwriter races AHEAD of the parent's recorded ACK stream through the stdout
//! pipe buffer — it durably upserts thousands of ids the parent never reads an ACK for
//! before the kill. So "not in the recorded acked set" does NOT mean "still old". The
//! check must not depend on the exact old/new cutoff. Each id `X` gets a unique stem
//! and two distinguishing tokens:
//! - `old_X  = "qstem{X} qold{X}"`   `new_X = "qstem{X} qnew{X}"`
//! - `both_X = "qstem{X} qold{X} qnew{X}"` — a title BOTH versions match.
//!
//! Whichever version survived the crash, `X` matches `both_X` (and no other id's tokens
//! appear in it), so `match(both_X) == {X}` for every id — a VANISH drops `X` (FN), a
//! corruption adds a stranger (FP), independent of which version won the race.
//!
//! For the ids the parent DID record an ACK for, the new version is DEFINITELY durable
//! (the ACK is a happens-after proof), so those get the stronger unambiguous check:
//! the new-only title matches and the old-only title does NOT (old tombstoned).

use std::collections::HashSet;
use std::io::Write;

use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};

use crate::harness::{build_base, crash_iters, jitter_for, spawn_and_kill, unique_dir, Trigger};

fn old_dsl(id: u64) -> String {
    format!("qstem{id:05} qold{id:05}")
}
fn new_dsl(id: u64) -> String {
    format!("qstem{id:05} qnew{id:05}")
}
fn both_title(id: u64) -> String {
    format!("qstem{id:05} qold{id:05} qnew{id:05}")
}

#[test]
#[ignore = "crash-injection: spawns + SIGKILLs a real process; run via the check.sh crash lane or `cargo test --release --test crash_injection -- --ignored`"]
fn upsert_acked_replace_whole_or_not_at_all_under_sigkill() {
    const N: u64 = 8_000;
    let old: Vec<(u64, String)> = (0..N).map(|x| (x, old_dsl(x))).collect();
    let new: Vec<(u64, String)> = (0..N).map(|x| (x, new_dsl(x))).collect();

    // The TSV the worker upserts FROM = the NEW versions, written once and reused.
    let tsv_dir = unique_dir("upsert_tsv");
    let tsv = tsv_dir.join("new.tsv");
    {
        let mut f = std::fs::File::create(&tsv).expect("create new.tsv");
        for (id, dsl) in &new {
            writeln!(f, "{id}\t{dsl}").expect("write tsv line");
        }
        f.sync_all().ok();
    }

    let iters = crash_iters();
    let fsync = false;
    let mut checked = 0usize;
    for i in 0..iters {
        // A FRESH flushed base each iteration (the prior iteration's worker mutated it).
        let dir = unique_dir("upsert");
        build_base(&dir, &old);

        let res = spawn_and_kill(
            "upsert",
            &dir,
            &tsv,
            &[],
            fsync,
            Trigger::Acks(1_500),
            jitter_for(i),
        );
        assert!(
            res.killed,
            "[upsert] writer finished before the kill — raise N or lower the Acks trigger"
        );
        let acked: HashSet<u64> = res.acked.iter().copied().collect();
        assert!(
            !acked.is_empty(),
            "[upsert] no durable upsert before the kill — the replace path is untested"
        );

        let cfg = EngineConfig {
            data_dir: Some(dir.clone()),
            wal_sync_on_write: fsync,
            ..EngineConfig::default()
        };
        let engine = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg)
            .unwrap_or_else(|e| panic!("[upsert/iter={i}] reopen failed: {e}"));
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let mut hits = |title: &str| -> HashSet<u64> {
            engine.match_title(title, &mut scratch, &mut out, /* include_broad */ true);
            out.iter().copied().collect()
        };

        for x in 0..N {
            // (1) Race-immune no-vanish + no-corruption: whichever version survived, X
            // matches its both-title and nothing else does.
            let both = hits(&both_title(x));
            assert!(
                both.contains(&x),
                "[upsert/iter={i}] id {x} VANISHED — neither old nor new recovered (ADR-067 half-state, cardinal sin)"
            );
            assert_eq!(
                both,
                HashSet::from([x]),
                "[upsert/iter={i}] id {x}'s both-title matched a stranger {both:?} (corruption)"
            );
            // (2) Race-immune no-DUPLICATE: an id is its old XOR its new version, never
            // BOTH live at once. The both-title above cannot see this — match_title dedups
            // a logical id to {x} however many physical copies survive — so probe the
            // version-distinguishing titles directly (old-only matches ONLY a live old
            // copy, new-only ONLY a live new copy; any stranger is already caught above).
            let old_live = hits(&old_dsl(x)).contains(&x);
            let new_live = hits(&new_dsl(x)).contains(&x);
            assert!(
                !(old_live && new_live),
                "[upsert/iter={i}] id {x} has BOTH its old and new versions live (non-atomic replace — tombstone-half lost)"
            );
            // (3) Stronger winner check for ids whose ACK the parent actually recorded —
            // the new version is DEFINITELY durable: new present, old gone.
            if acked.contains(&x) {
                assert!(
                    new_live,
                    "[upsert/iter={i}] acked id {x}'s NEW version missing after kill+reopen (FN)"
                );
                assert!(
                    !old_live,
                    "[upsert/iter={i}] acked id {x}'s OLD version survived the replace (stale half / non-atomic)"
                );
            }
            checked += 1;
        }
    }
    assert!(checked > 0, "upsert: no ids checked (degenerate)");
}
