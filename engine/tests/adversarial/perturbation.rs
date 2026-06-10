//! Metamorphic set-identity: surface-only title edits must not move the match set.
//!
//! Under the phrase-free default vocab, the identity perturbations (case, foldable
//! diacritics, whitespace runs, Split-class punctuation around tokens, end-appended
//! junk) provably leave the title's feature set unchanged — so the FULL corpus match
//! set must be byte-identical, title by title. Unlike the differential oracle, the
//! ground truth here is the engine's own answer on the clean twin: no shared-code
//! blindness, and a divergence in EITHER direction (lost match = FN, new match = FP)
//! fails loudly.

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig, Rng};
use reverse_rusty::segment::MatchScratch;

#[test]
fn identity_perturbations_preserve_the_exact_match_set() {
    let cfg = GenConfig {
        num_queries: 25_000,
        num_titles: 2_500,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x3E7A_0001,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);
    let eng = engine_from(&data.queries);
    let mut s = MatchScratch::new();
    let mut rng = Rng::new(0x3E7A_0001);

    let mut total_matches = 0usize;
    for (ti, title) in data.titles.iter().enumerate() {
        let baseline = matched(&eng, &mut s, title);
        total_matches += baseline.len();

        // Each op alone (rotating start point so all ops see all kinds of titles), then
        // every op composed.
        for op in [ti % IDENTITY_OPS, (ti + 2) % IDENTITY_OPS] {
            let p = identity_perturb(&mut rng, title, op);
            let out = matched(&eng, &mut s, &p);
            assert_eq!(
                out, baseline,
                "MATCH-SET DRIFT under identity op#{op}:\n  clean:     `{title}`\n  perturbed: `{p}`"
            );
        }
        let all = identity_perturb_all(&mut rng, title);
        let out = matched(&eng, &mut s, &all);
        assert_eq!(
            out, baseline,
            "MATCH-SET DRIFT under composed identity ops:\n  clean:     `{title}`\n  perturbed: `{all}`"
        );
    }
    assert!(
        total_matches > 1_000,
        "degenerate corpus: only {total_matches} baseline matches"
    );
}
