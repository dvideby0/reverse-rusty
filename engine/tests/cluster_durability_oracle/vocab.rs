//! ADR-046 mechanism (2) + ADR-054: a runtime alias / equivalence survives a crash + reopen.

use crate::harness::*;

/// The declared alias `zzabbr ≡ zzcanon`, as a Vocab (rebuilt per use).
fn alias_vocab(token: &str, canonical: &str) -> reverse_rusty::vocab::Vocab {
    let mut v = reverse_rusty::vocab::Vocab::new();
    v.add_synonym(token, canonical, reverse_rusty::dict::FeatureKind::Generic);
    v
}

#[test]
fn declared_alias_survives_reopen() {
    // `set_vocab` on a DURABLE cluster rebuilds + checkpoints: the new manifest carries the
    // re-minted dict + the serialized vocab. After a crash + reopen the alias is still in
    // effect — both surface forms match, and reopened ≡ pre-crash ≡ an independent
    // alias-aware oracle. Zero false negatives across the restart.
    let (mut queries, titles) = build_corpus();
    let q_abbr = 8_100_001u64;
    let q_canon = 8_100_002u64;
    queries.push((q_abbr, "1994 fleer zzabbr".into()));
    queries.push((q_canon, "1994 fleer zzcanon".into()));
    let title_abbr = "1994 fleer zzabbr psa 10";
    let title_canon = "1994 fleer zzcanon psa 10";
    // Titles snapshotted + compared across the restart: the alias forms + a corpus sample.
    let check: Vec<String> = [title_abbr.to_string(), title_canon.to_string()]
        .into_iter()
        .chain(titles.iter().take(80).cloned())
        .collect();

    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("alias_k{k}"));

        // Build durable, declare the alias (rebuild + checkpoint), snapshot, then "crash".
        let pre_crash: Vec<HashSet<u64>> = {
            let mut cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k, dir.clone(), false), &queries)
                    .expect("durable cluster builds");
            cluster
                .set_vocab(alias_vocab("zzabbr", "term:zzcanon"))
                .expect("set_vocab");
            for t in [title_abbr, title_canon] {
                let got = cluster.percolate(t).expect("percolate");
                assert!(
                    got.contains(&q_abbr) && got.contains(&q_canon),
                    "k={k}: pre-crash both forms must match {t:?}"
                );
            }
            check
                .iter()
                .map(|t| {
                    cluster
                        .percolate(t)
                        .expect("percolate")
                        .into_iter()
                        .collect()
                })
                .collect()
        };

        // Reopen from disk alone — `open` restores the alias normalizer from the manifest's
        // persisted vocab (the passed `vocab()` is overridden by it).
        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");

        // Independent alias-aware oracle over the live set.
        let brute = Brute::build_with_vocab(
            &queries,
            alias_vocab("zzabbr", "term:zzcanon")
                .to_normalizer()
                .unwrap(),
        );
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();
        for (i, t) in check.iter().enumerate() {
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(got, pre_crash[i], "k={k}: reopened≠pre-crash {t:?}");
            let want = brute.matches(t, &mut lc, &mut feats);
            assert_eq!(got, want, "k={k}: reopened≠alias-aware oracle {t:?}");
        }
        // The alias is still in effect after the restart (zero FN).
        for t in [title_abbr, title_canon] {
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert!(
                got.contains(&q_abbr) && got.contains(&q_canon),
                "k={k}: after reopen both forms must still match {t:?}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn declared_equivalence_survives_reopen() {
    // ADR-054: a DECLARED equivalence applied via set_vocab on a DURABLE cluster is persisted
    // in the manifest's serialized vocab + baked into the sealed segments (as expansion). After
    // a crash + reopen the equivalence is still in effect — both surface forms match, and
    // reopened ≡ pre-crash ≡ an independent equivalence-aware oracle. Zero FN across the restart.
    let equiv_vocab = || {
        let mut v = reverse_rusty::vocab::Vocab::new();
        v.add_equivalence(&["zzabbr", "zzcanon"]);
        v
    };
    let (mut queries, titles) = build_corpus();
    let q_abbr = 8_600_001u64;
    let q_canon = 8_600_002u64;
    queries.push((q_abbr, "1994 fleer zzabbr".into()));
    queries.push((q_canon, "1994 fleer zzcanon".into()));
    for i in 0..30u64 {
        queries.push((8_600_100 + i, format!("zzabbr u{i}")));
        queries.push((8_600_200 + i, format!("zzcanon u{i}")));
    }
    let title_abbr = "1994 fleer zzabbr psa 10";
    let title_canon = "1994 fleer zzcanon psa 10";
    let check: Vec<String> = [title_abbr.to_string(), title_canon.to_string()]
        .into_iter()
        .chain(titles.iter().take(80).cloned())
        .collect();

    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("equiv_k{k}"));

        let pre_crash: Vec<HashSet<u64>> = {
            let mut cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k, dir.clone(), false), &queries)
                    .expect("durable cluster builds");
            cluster
                .set_vocab(equiv_vocab())
                .expect("set_vocab equivalence");
            for t in [title_abbr, title_canon] {
                let got = cluster.percolate(t).expect("percolate");
                assert!(
                    got.contains(&q_abbr) && got.contains(&q_canon),
                    "k={k}: pre-crash both forms must match {t:?}"
                );
            }
            check
                .iter()
                .map(|t| {
                    cluster
                        .percolate(t)
                        .expect("percolate")
                        .into_iter()
                        .collect()
                })
                .collect()
        };

        // Reopen from disk alone — `open` restores the vocab (incl. equivalences) from the
        // manifest and re-installs them on the recovered dict.
        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");

        let brute = Brute::build_with_equiv(&queries, vocab(), &equiv_vocab());
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();
        for (i, t) in check.iter().enumerate() {
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(got, pre_crash[i], "k={k}: reopened≠pre-crash {t:?}");
            let want = brute.matches(t, &mut lc, &mut feats);
            assert_eq!(got, want, "k={k}: reopened≠equivalence-aware oracle {t:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn declared_alias_rebind_survives_reopen() {
    // A SECOND set_vocab on a durable cluster (re-binding the alias to a different canonical)
    // takes effect and the LATEST binding survives reopen — exercising repeated durable
    // rebuilds, and that live_sources de-dups correctly across the accumulated state.
    let (mut queries, _titles) = build_corpus();
    let qid = 8_200_001u64;
    queries.push((qid, "1994 fleer zzabbr".into()));

    let dir = unique_dir("alias_rebind");
    let title_one = "1994 fleer zzone psa 10";
    let title_two = "1994 fleer zztwo psa 10";

    {
        let mut cluster =
            ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
                .expect("durable cluster builds");
        // First binding: zzabbr → zzone.
        cluster
            .set_vocab(alias_vocab("zzabbr", "term:zzone"))
            .expect("set_vocab 1");
        assert!(
            cluster.percolate(title_one).unwrap().contains(&qid),
            "after the first binding, the zzone title matches"
        );
        assert!(
            !cluster.percolate(title_two).unwrap().contains(&qid),
            "the zztwo title must not match the first binding"
        );
        // Re-bind: zzabbr → zztwo.
        cluster
            .set_vocab(alias_vocab("zzabbr", "term:zztwo"))
            .expect("set_vocab 2");
        assert!(
            cluster.percolate(title_two).unwrap().contains(&qid),
            "after the re-bind, the zztwo title matches"
        );
        assert!(
            !cluster.percolate(title_one).unwrap().contains(&qid),
            "the old binding (zzone) must no longer match"
        );
    }

    // Reopen: the LATEST binding (zzabbr → zztwo) is what persisted.
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert!(
        reopened.percolate(title_two).unwrap().contains(&qid),
        "after reopen, the latest binding (zztwo) is in effect"
    );
    assert!(
        !reopened.percolate(title_one).unwrap().contains(&qid),
        "after reopen, the superseded binding (zzone) is gone"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
