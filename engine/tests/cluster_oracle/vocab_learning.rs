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

/// A multi-word alias form (`new york`) shared with the durable-reopen suite
/// ([`crate::vocab_reopen`]).
pub(crate) fn vocab_with_multiword_alias() -> reverse_rusty::vocab::Vocab {
    let mut v = reverse_rusty::vocab::Vocab::new();
    let n = reverse_rusty::normalize::Normalizer::default_vocab().unwrap();
    let d = reverse_rusty::dict::Dict::new();
    v.aliases_mut().add_classified(
        &["ny".into(), "new york".into()],
        reverse_rusty::vocab::AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &d,
    );
    v
}

#[test]
fn set_vocab_activates_multiword_alias_with_pt_aware_routing() {
    // ADR-076 (flips the ADR-061 refusal): `set_vocab` ACTIVATES a multi-word alias on a
    // cluster. Routing is P(T)-aware, so a title bearing the multi-word surface form
    // (`new york ...`) — whose canonical alias entity lives only in the positive superset
    // P(T), never in the leftmost-longest N(T) — still probes the shard holding a query
    // anchored on that entity. The cluster must agree with a single-node engine carrying
    // the same vocabulary (the ADR-061-proven dual-view ground truth) on both surface
    // forms AND on a corpus sample (no regression on alias-free titles).
    let (mut queries, titles) = build_corpus();
    // The probe query is expansion-widened (any-of of the group's forms) and the title's
    // canonical view carries the entity additively, so this test proves ACTIVATION +
    // expansion ≡ single-node; the routing-specific false-negative proof (a nested
    // entity that exists ONLY in P(T)) is `overlapping_aliases_match_nested_entity_...`.
    let q_alias = 9_600_001u64;
    queries.push((q_alias, "ny".into()));

    let title_direct = "ny psa 10";
    let title_full = "new york psa 10";

    for &k in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
        let rebuilt = cluster
            .set_vocab(vocab_with_multiword_alias())
            .expect("set_vocab must ACTIVATE a multi-word alias on a cluster (ADR-076)");
        assert!(rebuilt > 100, "K={k}: the rebuild covers the corpus");

        // Single-node ground truth under the SAME vocabulary.
        let mut reference = reverse_rusty::segment::Engine::with_vocab(
            vocab_with_multiword_alias(),
            reverse_rusty::config::EngineConfig::default(),
        )
        .expect("single-node engine with the multi-word vocab");
        reference.build_from_queries(&queries);
        let ref_snap = reference.snapshot();
        let mut s = reverse_rusty::segment::MatchScratch::new();
        let mut out = Vec::new();

        // The headline: the FULL surface form routes to the alias entity's shard and
        // matches the alias-anchored query (pre-ADR-076 this was the false negative).
        for title in [title_direct, title_full] {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert!(
                got.contains(&q_alias),
                "K={k}: {title:?} must match the alias-anchored query (P(T)-aware routing)"
            );
            ref_snap.match_title(title, &mut s, &mut out, true);
            let want: HashSet<u64> = out.iter().copied().collect();
            assert_eq!(got, want, "K={k}: cluster ≠ single-node for {title:?}");
        }
        // No regression on a corpus sample.
        for title in titles.iter().take(60) {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            ref_snap.match_title(title, &mut s, &mut out, true);
            let want: HashSet<u64> = out.iter().copied().collect();
            assert_eq!(
                got, want,
                "K={k}: cluster ≠ single-node for corpus {title:?}"
            );
        }
    }
}

#[test]
fn set_vocab_heals_unexpressible_alias_instead_of_refusing() {
    // Codex R14 (kept post-ADR-076): the install seam must judge the HEALED vocabulary. An
    // active group like `psa-10 ≡ new york` — classified while '-' split (`psa-10` = 2 tokens)
    // — whose first form later becomes unexpressible (`psa` grader + '-' fold ⇒ fused `psa10`)
    // is demoted at the install seam rather than installed as an alias that reports active and
    // silently never matches. The vocabulary is accepted and the demotion is observable.
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let mut v = reverse_rusty::vocab::Vocab::new();
    v.add_grader("psa");
    let n = reverse_rusty::normalize::Normalizer::default_vocab().unwrap();
    let d = reverse_rusty::dict::Dict::new();
    v.aliases_mut().add_classified(
        &["psa-10".into(), "new york".into()],
        reverse_rusty::vocab::AliasProvenance::DeclaredFile,
        1.0,
        &n,
        &d,
    );
    assert!(!v.aliases().active_alias_forms().is_empty(), "active");
    v.fold_punctuation('-'); // the mutation that makes `psa-10` unexpressible (fused grader)

    cluster
        .set_vocab(v)
        .expect("the healed (no remaining active multi-word) vocabulary must be accepted");
    let aliases = cluster.vocab().expect("vocab installed").alias_summary();
    assert_eq!(
        (aliases.active, aliases.candidate),
        (0, 1),
        "the unexpressible group demoted to a review candidate through the cluster seam"
    );
    assert!(
        cluster.percolate("1994 fleer psa 10").is_ok(),
        "the cluster remains usable after the healed vocabulary change"
    );
}

#[test]
fn build_with_vocab_activates_multiword_alias() {
    // ADR-076 (flips the ADR-061 construction refusal): a cluster built FROM A VOCAB
    // fully activates a multi-word alias at construction — the equivalence machinery
    // installs on the minted dict before extraction, P(T)-aware routing probes the
    // nested entity's shard, and BOTH surface forms match the alias-anchored query
    // with no set_vocab involved. (The coordinator-mode server's --vocab path.)
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster =
        ClusterEngine::build_with_vocab(vocab_with_multiword_alias(), &cfg, &[(1, "ny".into())])
            .expect("build_with_vocab must activate a multi-word alias (ADR-076)");
    assert!(
        cluster.vocab().is_some(),
        "the vocab is installed on the engine (GET /_vocab serves it)"
    );
    for title in ["ny psa 10", "new york psa 10"] {
        assert!(
            cluster.percolate(title).unwrap().contains(&1),
            "{title:?} must match the alias-anchored query on a built-with-vocab cluster"
        );
    }
}

#[test]
fn overlapping_aliases_match_nested_entity_titles_across_the_cluster() {
    // The dual-view divergence case end-to-end (ADR-076; reshaped from a review
    // finding): with two OVERLAPPING multi-word aliases active, a title bearing the
    // longer surface form emits the NESTED alias entity only in the positive superset
    // P(T) (the overlap pass) — never in the canonical N(T). The structural facts this
    // test also pins: an alias-group query is EXPANSION-WIDENED into an any-of
    // (ADR-060/061) that places SELECTIVELY on each member feature's shard, and the
    // nested title carries NEITHER member in its canonical view (`ny` is not a title
    // token; the inner entity is P(T)-only) — so pre-ADR-076 routing probed none of
    // the query's shards: a real false negative only P(T)-aware routing closes.
    // Cluster ≡ single-node under the same vocab at every K.
    let two_alias_vocab = || {
        let mut v = reverse_rusty::vocab::Vocab::new();
        let n = reverse_rusty::normalize::Normalizer::default_vocab().unwrap();
        let d = reverse_rusty::dict::Dict::new();
        v.aliases_mut().add_classified(
            &["nyc".into(), "new york city".into()],
            reverse_rusty::vocab::AliasProvenance::DeclaredFile,
            1.0,
            &n,
            &d,
        );
        v.aliases_mut().add_classified(
            &["ny".into(), "new york".into()],
            reverse_rusty::vocab::AliasProvenance::DeclaredFile,
            1.0,
            &n,
            &d,
        );
        v
    };
    let (mut queries, _titles) = build_corpus();
    let q_inner = 9_700_001u64;
    queries.push((q_inner, "ny".into())); // the INNER alias's query form
                                          // The nested-entity title: leftmost-longest takes `new york city` (the outer
                                          // alias); the inner `new york` entity lives only in P(T).
    let title_nested = "new york city psa 10";

    for &k in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
        cluster
            .set_vocab(two_alias_vocab())
            .expect("activate both overlapping multi-word aliases");

        // Single-node ground truth under the same vocab.
        let mut reference = reverse_rusty::segment::Engine::with_vocab(
            two_alias_vocab(),
            reverse_rusty::config::EngineConfig::default(),
        )
        .expect("single-node engine");
        reference.build_from_queries(&queries);
        let snap = reference.snapshot();
        let mut s = reverse_rusty::segment::MatchScratch::new();
        let mut out = Vec::new();

        let got: HashSet<u64> = cluster
            .percolate(title_nested)
            .unwrap()
            .into_iter()
            .collect();
        snap.match_title(title_nested, &mut s, &mut out, true);
        let want: HashSet<u64> = out.iter().copied().collect();
        assert!(
            got.contains(&q_inner),
            "K={k}: the nested-entity title must match the inner-alias query"
        );
        assert_eq!(
            got, want,
            "K={k}: cluster ≠ single-node for the nested title"
        );
    }

    // Pin the load-bearing preconditions. (1) The alias query places SELECTIVELY
    // (the any-of fans to member shards — NOT the always-probed replicated lane), so
    // routing must genuinely reach one of ITS shards for the title to match it.
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build");
    cluster.set_vocab(two_alias_vocab()).expect("set_vocab");
    let placed = cluster.add_query(9_700_002, "ny").expect("add alias query");
    assert!(
        matches!(placed, reverse_rusty::cluster::AddOutcome::Placed { .. }),
        "the expansion-widened alias query places selectively (got {placed:?})"
    );
    // (2) The nested entity is genuinely P(T)-only for the title (the routing-miss
    // precondition): under the active normalizer, P(T) ⊋ N(T) on this title.
    let norm = two_alias_vocab().to_normalizer().unwrap();
    let mut probe_dict = reverse_rusty::dict::Dict::new();
    let mut lc = String::new();
    for q in ["ny", "nyc"] {
        let ast = reverse_rusty::dsl::parse(q).unwrap();
        let _ = reverse_rusty::compile::extract(&ast, &norm, &mut probe_dict, &mut lc);
    }
    probe_dict.finalize_mask();
    let mut sc = reverse_rusty::normalize::NormScratch::new();
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    norm.match_features_dual(
        title_nested,
        &probe_dict,
        &mut lc,
        &mut sc,
        &mut neg,
        &mut pos,
    );
    assert!(
        pos.iter().any(|f| !neg.contains(f)),
        "the nested title must carry a P(T)-only feature (the inner alias entity) — \
         else this construction stopped exercising the routing fix (N={neg:?} P={pos:?})"
    );
}

#[test]
fn bare_normalizer_build_matches_single_node_semantics() {
    // The boundary, pinned: a cluster built from a BARE normalizer (no Vocab) leaves
    // equivalence-driven vocabulary (registry aliases) inert — exactly like a
    // single-node engine built from a bare normalizer. Cluster ≡ single-node holds on
    // both surface forms (the direct form matches; the multi-word form does not,
    // because no equivalence map was ever installed). Activation requires the
    // vocab-carrying constructors (`build_with_vocab` / `set_vocab` / a persisted
    // reopen) — documented in ADR-076, not silently divergent.
    use std::sync::Arc;
    let norm = vocab_with_multiword_alias().to_normalizer().unwrap();
    assert!(norm.has_multiword_aliases());
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(
        vocab_with_multiword_alias().to_normalizer().unwrap(),
        &cfg,
        &[(1, "ny".into())],
    )
    .expect("a bare multi-word normalizer is accepted (single-node parity)");

    let mut reference = reverse_rusty::segment::Engine::with_shared(
        Arc::new(norm),
        Arc::new(reverse_rusty::dict::Dict::new()),
        Arc::new(reverse_rusty::tagdict::TagDict::new()),
        reverse_rusty::config::EngineConfig::default(),
    );
    reference.bulk_ingest(&[(1u64, "ny".to_string())]);
    let snap = reference.snapshot();
    let mut s = reverse_rusty::segment::MatchScratch::new();
    let mut out = Vec::new();
    for title in ["ny psa 10", "new york psa 10"] {
        let got: std::collections::HashSet<u64> =
            cluster.percolate(title).unwrap().into_iter().collect();
        snap.match_title(title, &mut s, &mut out, true);
        let want: std::collections::HashSet<u64> = out.iter().copied().collect();
        assert_eq!(got, want, "cluster ≠ single-node for bare-norm {title:?}");
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
