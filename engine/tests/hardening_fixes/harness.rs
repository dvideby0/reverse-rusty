//! Shared helpers/harness for the hardening-fixes integration tests.

use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::path::PathBuf;

pub(crate) fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("reverse_rusty_hardening_{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub(crate) fn make_norm() -> Normalizer {
    Normalizer::default_vocab().unwrap()
}

pub(crate) fn match_ids(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

pub(crate) fn sample_queries() -> Vec<(u64, String)> {
    vec![
        (1, "michael jordan 1986 fleer".into()),
        (2, "lebron james rookie".into()),
        (3, "kobe bryant psa 10".into()),
        (4, "mike trout 2011 topps update".into()),
        (5, "derek jeter bowman chrome refractor".into()),
        (6, "shohei ohtani rookie".into()),
        (7, "luka doncic prizm silver".into()),
        (8, "stephen curry select".into()),
        (9, "aaron judge topps chrome".into()),
        (10, "patrick mahomes prizm rookie".into()),
        // Duplicate logical IDs (different versions of same query)
        (1, "michael jordan 1986 fleer rookie".into()),
        (2, "lebron james rookie card".into()),
    ]
}
