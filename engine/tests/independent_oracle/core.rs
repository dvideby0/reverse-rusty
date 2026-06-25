//! The high-volume default-vocab differential: generated (clean + messy) corpora, engine vs. the
//! independent reference, under the empty default vocabulary the in-tree oracle also runs.

use crate::harness::RefOracle;
use reverse_rusty::gen::{generate, messify_dataset, GenConfig, Rng};

fn cfg(seed: u64) -> GenConfig {
    GenConfig {
        num_queries: 40_000,
        num_titles: 4_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: 3_000,
        num_sets: 1_200,
    }
}

#[test]
fn default_vocab_clean_corpus() {
    let data = generate(&cfg(0x00AB_CDEF));
    let oracle = RefOracle::build_default(&data.queries);
    oracle.assert_matches(&data.titles, "default/clean");
}

#[test]
fn default_vocab_messy_corpus() {
    // Surface noise (case / diacritics / whitespace runs / punctuation / unicode junk) stresses the
    // reference's byte-clean + diacritic-fold + marker handling against the engine's.
    let mut data = generate(&cfg(0x5EED_1234));
    let mut rng = Rng::new(0x4D45_5353 ^ 0x5EED_1234); // "MESS" ^ seed
    messify_dataset(&mut rng, &mut data, 0.8, 0.5);
    let oracle = RefOracle::build_default(&data.queries);
    oracle.assert_matches(&data.titles, "default/messy");
}
