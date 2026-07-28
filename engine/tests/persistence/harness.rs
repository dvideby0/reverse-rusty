//! Shared helpers for the persistence test suite: temp-dir setup, the sample
//! query corpus, and the serialize→mmap→match round-trip helper.

use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::Engine;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) fn test_dir(name: &str) -> PathBuf {
    // Per-invocation unique suffix: the persistence suite runs alongside the other
    // test binaries (cargo schedules them concurrently), and `backup_to` fails loud
    // on a pre-existing dest. A fixed path that relied on a best-effort `remove_dir_all`
    // succeeding raced under that load (stale subdir → `DestExists`), so derive a
    // collision-free directory from pid + a process-local counter instead.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "reverse_rusty_test_{name}_{}_{unique}",
        std::process::id()
    ));
    // Clean up any residue from a previous run that happened to collide.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub(crate) fn make_norm() -> Normalizer {
    Normalizer::default_vocab().unwrap()
}

pub(crate) fn sample_queries() -> Vec<(u64, String)> {
    vec![
        (1, "wireless mouse 1986 vertex".into()),
        (2, "mechanical keyboard new".into()),
        (3, "noise cancelling headphones pro".into()),
        (4, "air purifier 2011 acme update".into()),
        (5, "product kappa summit chrome premium".into()),
        (6, "shohei product alpha new".into()),
        (7, "luka doncic contoso silver".into()),
        (8, "stephen curry select".into()),
        (9, "aaron judge acme chrome".into()),
        (10, "action camera contoso new".into()),
    ]
}

/// Helper: match a title and return sorted logical IDs.
pub(crate) fn match_ids(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = reverse_rusty::segment::MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}
