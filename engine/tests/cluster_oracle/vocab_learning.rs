//! Vocabulary learning through the cluster: declared aliases (ADR-046 mech 2), corpus-learned
//! synonyms (ADR-015), induced entity phrases (ADR-053), and declared equivalence expansion
//! (ADR-054) — each applied via the blue/green re-place rebuild and held to `cluster ≡ brute`
//! over the live set at every K (zero FN/FP).

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use std::collections::HashSet;

#[test]
fn declared_alias_makes_both_surface_forms_match() {
    // ADR-046 mechanism (2): after a declared alias (zzabbr ≡ zzcanon), a query
    // written with ONE surface form matches a title written with the OTHER —
    // cluster-wide, with zero false negatives. `set_vocab` re-mints the shared dict,
    // re-places every query under the new normalizer, and atomically swaps the
    // shards. The two tokens never appear in build_corpus, so before the alias they
    // are distinct (synthetic) features that do not cross-match.
    let (mut queries, titles) = build_corpus();
    let q_abbr = 8_000_001u64;
    let q_canon = 8_000_002u64;
    queries.push((q_abbr, "1994 fleer zzabbr".into()));
    queries.push((q_canon, "1994 fleer zzcanon".into()));

    // The declared alias as a Vocab — built manually (keeping this gate orthogonal
    // to the learning heuristic). Rebuilt per use: set_vocab and the oracle each
    // consume one.
    let make_vocab = || {
        let mut v = reverse_rusty::vocab::Vocab::new();
        v.add_synonym(
            "zzabbr",
            "term:zzcanon",
            reverse_rusty::dict::FeatureKind::Generic,
        );
        v
    };

    let title_abbr = "1994 fleer zzabbr psa 10";
    let title_canon = "1994 fleer zzcanon psa 10";

    for &k in &[1usize, 3, 8, 16] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

        // Before the alias the two forms are distinct: the canonical-form title does
        // not match the abbreviation query, and vice versa.
        assert!(
            !cluster.percolate(title_canon).unwrap().contains(&q_abbr),
            "K={k}: before alias, the canonical title must not match the abbreviation query"
        );
        assert!(
            !cluster.percolate(title_abbr).unwrap().contains(&q_canon),
            "K={k}: before alias, the abbreviation title must not match the canonical query"
        );

        // Declare the alias + rebuild the cluster under the new normalizer.
        let rebuilt = cluster.set_vocab(make_vocab()).expect("set_vocab");
        assert!(
            rebuilt > 100,
            "K={k}: set_vocab should rebuild the whole live corpus, not just the 2 added \
             queries (got {rebuilt})"
        );

        // After the alias, BOTH queries match BOTH surface forms (the headline; zero FN).
        for title in [title_abbr, title_canon] {
            let got = cluster.percolate(title).unwrap();
            assert!(
                got.contains(&q_abbr),
                "K={k}: {title:?} must match the abbreviation-form query after the alias"
            );
            assert!(
                got.contains(&q_canon),
                "K={k}: {title:?} must match the canonical-form query after the alias"
            );
        }

        // Differential equivalence STILL holds post-alias: cluster ≡ an independent,
        // alias-aware brute over the full live set — for the alias titles AND a sample
        // of corpus titles (proving the rebuild preserved base matching, zero FN/FP).
        let brute = Brute::build_with_vocab(&queries, make_vocab().to_normalizer().unwrap());
        let mut lc = String::new();
        let mut feats = Vec::new();
        for title in titles
            .iter()
            .map(String::as_str)
            .take(100)
            .chain([title_abbr, title_canon])
        {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            let truth = brute.matches(title, &mut lc, &mut feats);
            assert_eq!(
                got, truth,
                "K={k}: cluster disagrees with the alias-aware oracle for {title:?}"
            );
        }
    }
}

#[test]
fn learn_and_apply_absorbs_synonyms_from_anyof_groups() {
    // ADR-046 mechanism (2) auto-learning (ADR-015): the cluster learns a synonym from its
    // OWN corpus's any-of groups — `(rookie,rc)` seen ≥ min_count ⇒ `rc → rookie` — and
    // applies it. A query phrased with the abbreviation then matches a title written with
    // the canonical form (zero FN). The learned rule merges under the current vocabulary.
    let (mut queries, _titles) = build_corpus();
    let q_rc = 8_300_001u64;
    queries.push((q_rc, "1994 fleer rc".into())); // a query phrased with the abbreviation
                                                  // Plant ≥ min_count any-of groups so the learner discovers rc → rookie.
    for i in 0..4u64 {
        queries.push((8_300_100 + i, "(rookie,rc)".into()));
    }

    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let title_rookie = "1994 fleer rookie psa 10";
    // Before learning, "rc" and "rookie" are distinct features (the default vocab is empty).
    assert!(
        !cluster.percolate(title_rookie).unwrap().contains(&q_rc),
        "before learning, a rookie title must not match the rc-phrased query"
    );

    // Learn from the corpus's any-of groups (min_count = 2) and apply.
    let rebuilt = cluster.learn_and_apply(2).expect("learn_and_apply");
    assert!(rebuilt > 100, "learn_and_apply rebuilds the whole corpus");

    // After learning rc → rookie, both surface forms match the rc-phrased query (zero FN).
    assert!(
        cluster.percolate(title_rookie).unwrap().contains(&q_rc),
        "after learning rc→rookie, a rookie title matches the rc-phrased query"
    );
    assert!(
        cluster
            .percolate("1994 fleer rc psa 10")
            .unwrap()
            .contains(&q_rc),
        "the abbreviation form still matches after learning"
    );
    // The learned synonym is recorded + introspectable on the cluster.
    assert!(
        cluster
            .vocab()
            .is_some_and(|v| v.synonyms().iter().any(|s| s.token == "rc")),
        "the learned rc→rookie synonym is recorded in the cluster vocab"
    );
}

#[test]
fn learn_and_apply_with_corpus_phrases_preserves_zero_false_negatives() {
    // ADR-053: the cluster self-derives an ENTITY PHRASE from its OWN corpus via NPMI
    // (`corpus_phrases=true`) and applies it through the same blue/green re-place rebuild
    // as a declared alias. After the rebuild the cluster must STILL equal an independent,
    // phrase-aware brute over the full live set — for the planted titles AND a sample of
    // corpus titles (zero FN/FP), at every K. A phrase can move a query's anchor (hence its
    // shard), so this exercises re-placement under an induced feature.
    let (mut queries, titles) = build_corpus();
    let q_plant = 8_400_001u64;
    queries.push((q_plant, "1994 fleer zenith zonk".into())); // requires the adjacent pair
    for id in 8_400_100u64..8_400_140 {
        queries.push((id, "zenith zonk".into())); // plant a strong collocation
    }
    let plant_title = "1994 fleer zenith zonk psa 10";

    let learn_cfg = reverse_rusty::vocab::CorpusLearnConfig {
        corpus_phrases: true,
        npmi_min_count: 3,
        ..Default::default()
    };

    for &k in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

        // Before induction "zenith"/"zonk" are distinct synthetic features; gluing has
        // not happened, so the phrase-form title need not match yet — we only assert the
        // POST-induction equivalence below (the headline).
        let rebuilt = cluster
            .learn_and_apply_with(&learn_cfg)
            .expect("corpus-phrase learn_and_apply");
        assert!(
            rebuilt > 100,
            "K={k}: learn_and_apply rebuilds the whole corpus (got {rebuilt})"
        );

        // The induced phrase is recorded on the cluster vocab (non-vacuous induction).
        assert!(
            cluster.vocab().is_some_and(|v| v
                .phrases()
                .iter()
                .any(|p| p.tokens == vec!["zenith".to_string(), "zonk".to_string()])),
            "K={k}: the planted zenith/zonk phrase must be induced + recorded"
        );
        // The phrase-form query matches the phrase-bearing title after induction (zero FN).
        assert!(
            cluster.percolate(plant_title).unwrap().contains(&q_plant),
            "K={k}: the phrase-form query must match the phrase-bearing title after induction"
        );

        // Differential equivalence post-induction: cluster ≡ an independent phrase-aware
        // brute carrying the SAME learned normalizer.
        let learned = cluster.vocab().unwrap().clone();
        let brute = Brute::build_with_vocab(&queries, learned.to_normalizer().unwrap());
        let mut lc = String::new();
        let mut feats = Vec::new();
        for title in titles
            .iter()
            .map(String::as_str)
            .take(120)
            .chain([plant_title])
        {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            let truth = brute.matches(title, &mut lc, &mut feats);
            assert_eq!(
                got, truth,
                "K={k}: cluster disagrees with the phrase-aware oracle for {title:?}"
            );
        }
    }
}

#[test]
fn declared_equivalence_expands_across_shards_with_zero_false_negatives() {
    // ADR-054: a DECLARED equivalence {zzabbr, zzcanon} applied via set_vocab must make a
    // query phrased with one form match a title bearing the other, at every K, with zero FN.
    // Expansion turns the query's anchor into an any-of, so it fans to BOTH forms' shards
    // (re-placement under expansion) — and the cluster still equals an equivalence-aware brute.
    let (mut queries, titles) = build_corpus();
    let q_abbr = 8_500_001u64;
    let q_canon = 8_500_002u64;
    queries.push((q_abbr, "1994 fleer zzabbr".into()));
    queries.push((q_canon, "1994 fleer zzcanon".into()));
    // Intern both tokens widely so they resolve to real (non-synthetic) feature ids.
    for i in 0..30u64 {
        queries.push((8_500_100 + i, format!("zzabbr u{i}")));
        queries.push((8_500_200 + i, format!("zzcanon u{i}")));
    }

    let make_vocab = || {
        let mut v = reverse_rusty::vocab::Vocab::new();
        v.add_equivalence(&["zzabbr", "zzcanon"]);
        v
    };
    let title_abbr = "1994 fleer zzabbr psa 10";
    let title_canon = "1994 fleer zzcanon psa 10";

    for &k in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

        // Before the equivalence, the two forms are distinct.
        assert!(
            !cluster.percolate(title_canon).unwrap().contains(&q_abbr),
            "K={k}: before equiv, a canonical title must not match the abbreviation query"
        );

        let rebuilt = cluster
            .set_vocab(make_vocab())
            .expect("set_vocab equivalence");
        assert!(
            rebuilt > 100,
            "K={k}: set_vocab rebuilds the whole corpus (got {rebuilt})"
        );

        // After: BOTH queries match BOTH surface forms (expansion; zero FN).
        for title in [title_abbr, title_canon] {
            let got = cluster.percolate(title).unwrap();
            assert!(
                got.contains(&q_abbr),
                "K={k}: {title:?} must match the abbreviation-form query after the equivalence"
            );
            assert!(
                got.contains(&q_canon),
                "K={k}: {title:?} must match the canonical-form query after the equivalence"
            );
        }

        // Differential: cluster ≡ an independent equivalence-aware brute over the live set.
        let brute = Brute::build_with_equiv(&queries, vocab(), &make_vocab());
        let mut lc = String::new();
        let mut feats = Vec::new();
        for title in titles
            .iter()
            .map(String::as_str)
            .take(100)
            .chain([title_abbr, title_canon])
        {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            let truth = brute.matches(title, &mut lc, &mut feats);
            assert_eq!(
                got, truth,
                "K={k}: cluster disagrees with the equivalence-aware oracle for {title:?}"
            );
        }
    }
}
