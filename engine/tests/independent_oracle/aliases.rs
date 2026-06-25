//! Populated-vocab differentials: graders + grade context (ADR-069) and multi-word phrases /
//! synonyms — the parts of the front end the default-vocab pass does not exercise. The engine
//! `Normalizer` and the reference `RefVocab` are built from the SAME generator constants
//! (`reverse_rusty::gen::{PLAYERS,BRANDS,BRAND_ALT,CARD_TERMS,GRADERS}`), so they encode one
//! vocabulary in each side's own type. (Alias-mode two-view + equivalence expansion get their own
//! pass once this grader/phrase pass is green.)

use crate::harness::RefOracle;
use reverse_rusty::dict::FeatureKind;
use reverse_rusty::gen::{generate, GenConfig, Rng};
use reverse_rusty::normalize::{Normalizer, NormalizerBuilder};
use reverse_rusty_ref_matcher::vocab::PhraseMode;
use reverse_rusty_ref_matcher::RefVocab;

/// The engine-side populated normalizer — identical in spirit to the in-tree oracle's `gen_vocab`
/// (multiword player/brand phrases, single-token brand + brand-alt + card-term synonyms, graders,
/// grade words).
fn gen_engine_norm() -> Normalizer {
    use reverse_rusty::gen::{BRANDS, BRAND_ALT, CARD_TERMS, GRADERS, PLAYERS};
    let mut b = NormalizerBuilder::new();
    for p in PLAYERS {
        let canon = format!("player:{}", p.replace(' ', "_"));
        let toks: Vec<&str> = p.split(' ').collect();
        b.add_phrase(&toks, &canon, FeatureKind::Player);
    }
    for brand in BRANDS {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        let toks: Vec<&str> = brand.split(' ').collect();
        if toks.len() > 1 {
            b.add_phrase(&toks, &canon, FeatureKind::Brand);
        } else {
            b.add_synonym(toks[0], &canon, FeatureKind::Brand);
        }
    }
    for (alt, brand) in BRAND_ALT.iter().zip(BRANDS.iter()) {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        b.add_synonym(alt, &canon, FeatureKind::Brand);
    }
    for ct in CARD_TERMS {
        b.add_synonym(ct, &format!("card_term:{ct}"), FeatureKind::Category);
    }
    for g in GRADERS {
        b.add_grader(g);
    }
    b.add_grade_word("gem");
    b.add_grade_word("mint");
    b.build().expect("gen vocab automaton")
}

/// The reference-side vocabulary built from the SAME generator constants — one source of truth,
/// expressed in `RefVocab`'s own type. Multiword brand/player phrases are `Collapse` (the engine's
/// `add_phrase` default).
fn gen_ref_vocab() -> RefVocab {
    use reverse_rusty::gen::{BRANDS, BRAND_ALT, CARD_TERMS, GRADERS, PLAYERS};
    let mut v = RefVocab::default_vocab();
    for p in PLAYERS {
        let canon = format!("player:{}", p.replace(' ', "_"));
        v = v.phrase(p, &canon, PhraseMode::Collapse);
    }
    for brand in BRANDS {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        if brand.split(' ').count() > 1 {
            v = v.phrase(brand, &canon, PhraseMode::Collapse);
        } else {
            v = v.synonym(brand, &canon);
        }
    }
    for (alt, brand) in BRAND_ALT.iter().zip(BRANDS.iter()) {
        let canon = format!("brand:{}", brand.replace(' ', "_"));
        v = v.synonym(alt, &canon);
    }
    for ct in CARD_TERMS {
        v = v.synonym(ct, &format!("card_term:{ct}"));
    }
    for g in GRADERS {
        v = v.grader(g);
    }
    v.grade_word("gem").grade_word("mint")
}

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
#[ignore = "probe: discovers the engine's alias canonical features; run with --ignored"]
fn probe_alias_features() {
    use reverse_rusty::normalize::{NormScratch, Side};
    use reverse_rusty::segment::Engine;
    let queries = vec![
        (1u64, "ny mets".to_string()),
        (2u64, "new york yankees".to_string()),
    ];
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    eng.import_alias_synonyms("ny => new york\nnyc => new york city")
        .expect("aliases");
    let vocab = eng.vocab().expect("vocab").clone();
    eprintln!("equivalences:           {:?}", vocab.equivalences());
    eprintln!(
        "effective_equiv_groups: {:?}",
        vocab.effective_equivalence_groups()
    );
    let norm = vocab.to_normalizer().expect("norm");
    let mut lc = String::new();
    let mut sc = NormScratch::new();
    let emit_names =
        |norm: &Normalizer, lc: &mut String, sc: &mut NormScratch, t: &str, side, fa| {
            let mut v = Vec::new();
            norm.emit(t, lc, sc, side, fa, &mut |n, _k| v.push(n.to_string()));
            v
        };
    for probe in [
        "new york",
        "ny",
        "new york city",
        "nyc",
        "york",
        "new york city yankees",
        "ny mets",
    ] {
        let q = emit_names(&norm, &mut lc, &mut sc, probe, Side::Query, false);
        let tn = emit_names(&norm, &mut lc, &mut sc, probe, Side::Title, false);
        let tp = emit_names(&norm, &mut lc, &mut sc, probe, Side::Title, true);
        eprintln!("{probe:?}\n  Q:    {q:?}\n  N(T): {tn:?}\n  T+:   {tp:?}");
    }
}

#[test]
fn populated_vocab_graders_and_phrases() {
    let data = generate(&cfg(0x1234_5678));
    let oracle =
        RefOracle::build_with_normalizer(&data.queries, gen_engine_norm(), gen_ref_vocab());
    oracle.assert_matches(&data.titles, "populated/graders+phrases");
}

/// The reference vocabulary mirroring `import_alias_synonyms("ny => new york\nnyc => new york
/// city")`: two `Alias`-mode multi-word phrases (entity `term:<tokens joined by _>`) + the
/// effective equivalence form-groups. (Canonical features confirmed by `probe_alias_features`.)
fn ny_ref_vocab() -> RefVocab {
    RefVocab::default_vocab()
        .phrase("new york", "term:new_york", PhraseMode::Alias)
        .phrase("new york city", "term:new_york_city", PhraseMode::Alias)
        .equivalence(&["new york", "ny"])
        .equivalence(&["new york city", "nyc"])
}

/// ADR-061 two-view differential: a query mix exercising bidirectional aliases, overlapping/nested
/// entities, forbidden-over-multi-word (the canonical `N(T)` view), component-token, and any-of —
/// engine vs. the independent reference, zero FN AND zero FP over every title. The engine is built
/// the same way the in-tree alias oracle builds it (`import_alias_synonyms`); the reference uses a
/// from-scratch two-view normalizer + equivalence expansion.
#[test]
fn multiword_alias_two_view_differential() {
    let queries: Vec<(u64, String)> = vec![
        (1, "ny mets".into()),
        (2, "new york yankees".into()),
        (3, "new york -mets".into()),
        (4, "foo -\"new york\"".into()), // THE WALL: forbidden multi-word vs canonical N(T)
        (5, "york".into()),              // component-token query (title side additive)
        (6, "new york city subway".into()),
        (7, "(ny,boston) finals".into()), // any-of with an alias form
        (8, "brooklyn".into()),
    ];
    let titles: Vec<String> = [
        "new york mets opening day",
        "ny yankees world series",
        "new york city subway map",
        "foo new york city skyline", // q4 MATCHES: canonical reads `new york city`, not `new york`
        "foo new york state",        // q4 rejected: literal `new york` present
        "boston finals run",
        "brooklyn bridge",
        "york peppermint pattie",
        "ny mets vs boston",
        "new york city",
        "new  york mets", // whitespace run: P(T) overlap scan still matches the alias
    ]
    .iter()
    .map(ToString::to_string)
    .collect();

    let oracle = RefOracle::build_with_alias_import(
        &queries,
        "ny => new york\nnyc => new york city",
        ny_ref_vocab(),
    );
    oracle.assert_matches(&titles, "alias/two-view");
}

/// A randomized at-scale alias corpus combining the overlapping forms (`ny` / `new york` /
/// `new york city` / `nyc` / component tokens) with fillers, negations (incl. forbidden phrases),
/// and any-of groups (incl. multi-word members) — so the two-view normalization, the overlap scan,
/// the canonical forbidden view, and equivalence widening all run over thousands of titles. Engine
/// vs. the independent reference, zero FN AND zero FP.
fn alias_scale_corpus(
    seed: u64,
    n_queries: usize,
    n_titles: usize,
) -> (Vec<(u64, String)>, Vec<String>) {
    let forms = [
        "ny",
        "new york",
        "new york city",
        "nyc",
        "york",
        "new",
        "city",
    ];
    let fillers = [
        "mets", "yankees", "subway", "finals", "series", "bridge", "boston", "rookie",
    ];
    let mut rng = Rng::new(seed);
    let pick = |rng: &mut Rng, xs: &[&str]| xs[rng.below(xs.len())].to_string();

    let mut queries = Vec::new();
    for i in 0..n_queries {
        let mut parts: Vec<String> = Vec::new();
        for _ in 0..=rng.below(2) {
            parts.push(pick(&mut rng, &forms));
        }
        if rng.frac() < 0.6 {
            parts.push(pick(&mut rng, &fillers));
        }
        if rng.frac() < 0.3 {
            let neg = if rng.frac() < 0.5 {
                pick(&mut rng, &forms)
            } else {
                pick(&mut rng, &fillers)
            };
            if neg.contains(' ') {
                parts.push(format!("-\"{neg}\""));
            } else {
                parts.push(format!("-{neg}"));
            }
        }
        if rng.frac() < 0.3 {
            parts.push(format!(
                "({},{})",
                pick(&mut rng, &forms),
                pick(&mut rng, &fillers)
            ));
        }
        queries.push((i as u64 + 1, parts.join(" ")));
    }

    let mut titles = Vec::new();
    for _ in 0..n_titles {
        let n = 2 + rng.below(5);
        let parts: Vec<String> = (0..n)
            .map(|_| {
                if rng.frac() < 0.5 {
                    pick(&mut rng, &forms)
                } else {
                    pick(&mut rng, &fillers)
                }
            })
            .collect();
        titles.push(parts.join(" "));
    }
    (queries, titles)
}

#[test]
fn multiword_alias_scale_differential() {
    let (queries, titles) = alias_scale_corpus(0x0A11_A5E2, 3_000, 1_500);
    let oracle = RefOracle::build_with_alias_import(
        &queries,
        "ny => new york\nnyc => new york city",
        ny_ref_vocab(),
    );
    oracle.assert_matches(&titles, "alias/scale");
}
