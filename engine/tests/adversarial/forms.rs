//! Cross-form matrices: every declared-equivalent surface form must match every other
//! form, in both directions, under surface noise. This is the PRODUCT contract of
//! ADR-058 (punctuation folding), ADR-054 (equivalence expansion), and ADR-060/061
//! (alias governance + multi-word aliases) — asserted end-to-end through the engine
//! with no reference implementation to share code with.
//!
//! The R11 escape (`fix(normalize,alias): query-side whitespace runs`, ecb569f) is
//! pinned here as a permanent regression: a whitespace run inside a quoted phrase or
//! any-of member must not hide a multi-word alias from the query compiler, and a run
//! inside a title must not hide it from the positive view.

use crate::harness::*;
use reverse_rusty::gen::Rng;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::Vocab;
use reverse_rusty::EngineConfig;

/// ADR-058: with `'` and `-` declared Fold, every punctuation variant of a name is ONE
/// feature — so every variant-phrased query matches every variant-phrased title,
/// including case/diacritic/whitespace-perturbed ones.
#[test]
fn punctuation_fold_forms_all_cross_match() {
    let mut v = Vocab::new();
    v.fold_punctuation('\'');
    v.fold_punctuation('-');

    let forms = [
        "obrien", "o'brien", "o-brien", "O'Brien", "O-BRIEN", "öbrien",
    ];
    let queries: Vec<(u64, String)> = forms
        .iter()
        .enumerate()
        .map(|(i, f)| (i as u64 + 1, format!("{f} rookie")))
        .collect();
    let all_ids: Vec<u64> = queries.iter().map(|(id, _)| *id).collect();

    let mut eng = Engine::with_vocab(v, EngineConfig::default()).expect("with_vocab");
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    let mut rng = Rng::new(0xF07D_0001);

    for form in &forms {
        let title = format!("1994 {form} rookie card");
        assert_eq!(
            matched(&eng, &mut s, &title),
            all_ids,
            "FOLD MATRIX: title with form `{form}` must match every variant query"
        );
        // The same title under composed surface noise.
        let messy = identity_perturb_all(&mut rng, &title);
        assert_eq!(
            matched(&eng, &mut s, &messy),
            all_ids,
            "FOLD MATRIX (perturbed): `{messy}` must match every variant query"
        );
    }
}

/// ADR-054: a declared equivalence makes either form's query match either form's title —
/// including under surface noise (composing equivalence expansion with the cleaning
/// pipeline).
#[test]
fn equivalence_forms_cross_match_under_surface_noise() {
    let mut v = Vocab::new();
    v.add_equivalence(&["rc", "rookie"]);
    let mut queries: Vec<(u64, String)> =
        vec![(1, "1994 fleer rc".into()), (2, "1994 fleer rookie".into())];
    // Intern both forms widely enough that the dict knows them (mirrors the ADR-054 tests).
    for i in 0..10u64 {
        queries.push((100 + i, format!("rc filler{i}")));
        queries.push((200 + i, format!("rookie filler{i}")));
    }

    let mut eng = Engine::with_vocab(v, EngineConfig::default()).expect("with_vocab");
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();

    for title in [
        "1994 fleer rookie",
        "1994 fleer rc",
        "1994 FLEER  rookie!!",
        "1994 fleer rc ™ zzjunk00aa11bb",
        "1994 (fleer) RC,",
    ] {
        let out = matched(&eng, &mut s, title);
        assert!(
            out.binary_search(&1).is_ok() && out.binary_search(&2).is_ok(),
            "EQUIV MATRIX: both the rc-query and the rookie-query must match `{title}`, got {out:?}"
        );
    }
}

/// ADR-061 + the R11 regression: multi-word alias forms cross-match through quoted
/// phrases and any-of members even when whitespace RUNS corrupt the query surface, and
/// through the positive view when runs corrupt the title surface.
#[test]
fn multiword_alias_forms_cross_match_despite_whitespace_runs() {
    let queries: Vec<(u64, String)> = vec![
        (1, "ny mets".into()),
        (2, "new york yankees".into()),
        (3, "\"new  york\" knicks".into()), // run INSIDE a quoted phrase (R11)
        (4, "(new  york,gotham) rangers".into()), // run inside an any-of member (R11)
    ];
    let mut eng =
        Engine::new(reverse_rusty::normalize::Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    eng.import_alias_synonyms("ny => new york")
        .expect("import + apply the multi-word alias");
    let mut s = MatchScratch::new();

    let must_match: &[(u64, &str)] = &[
        (1, "new york mets"),    // alias forward: ny query → new york title
        (1, "NEW  YORK mets"),   // + case + title-side run (positive-view overlap scan)
        (2, "ny yankees"),       // alias reverse: new york query → ny title
        (3, "ny knicks"),        // R11: run-in-phrase query still reaches the alias
        (3, "new york knicks"),  // and still matches the literal form
        (3, "new  york knicks"), // run on BOTH sides
        (4, "ny rangers"),       // R11: run-in-any-of-member still reaches the alias
        (4, "gotham rangers"),   // the other member still works
        (4, "new york rangers"), // the literal multi-word member still works
    ];
    for (id, title) in must_match {
        let out = matched(&eng, &mut s, title);
        assert!(
            out.binary_search(id).is_ok(),
            "ALIAS MATRIX FN: query {id} must match `{title}`, got {out:?}"
        );
    }
}
