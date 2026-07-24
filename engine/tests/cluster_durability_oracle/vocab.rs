//! ADR-046 mechanism (2) + ADR-054: a runtime alias / equivalence survives a crash + reopen;
//! ADR-074: per-query tags survive a vocabulary rebuild, including across reopen boundaries.

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
fn tagged_set_vocab_carries_tags_across_checkpoint_reopen_and_rebuild() {
    // The ADR-074 durable gate, sequenced to hit the hardest path: a TAGGED durable cluster
    // (interned corpus tags + one post-freeze SYNTHETIC live tag) is checkpointed and REOPENED
    // FIRST — so the synthetic tag's raw string exists nowhere (the log was truncated; segments
    // hold only `TagId`s) — and only THEN gets the vocabulary change. The rebuild must gather
    // each query's stored ids from the reopened (mmap-backed) segments and carry them through
    // re-extraction + re-placement. A second reopen then proves the REBUILT segments durably
    // carry the tag columns.
    let (mut queries, titles) = build_corpus();
    // A tagged query in the ALIAS surface form: its extraction (hence possibly its shard)
    // changes under the vocab change — its tags must follow it.
    let q_alias = 9_400_001u64;
    queries.push((q_alias, "1994 fleer zzabbr".into()));
    let tags = tags_parallel(&queries);

    // "region" never appears in the corpus tag keys ⇒ guaranteed post-freeze synthetic.
    let live_id = 9_400_002u64;
    let live_dsl = "zzrareliveq";
    let live_tag = || vec![("region".to_string(), "emea".to_string())];
    let tag_of = |l: u64| {
        if l == live_id {
            live_tag()
        } else {
            tags_for(l)
        }
    };

    let dir = unique_dir("tagged_set_vocab");
    {
        let cluster = ClusterEngine::build_with_tags(
            vocab(),
            &durable_cfg(3, dir.clone(), false),
            &queries,
            &tags,
        )
        .expect("tagged durable build");
        cluster
            .add_query_with_tags(live_id, live_dsl, &live_tag())
            .expect("synthetic-tagged live add");
        cluster.checkpoint().expect("checkpoint");
    }

    // Reopen #1, then the vocabulary change on the reopened cluster.
    let mut reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    let rebuilt = reopened
        .set_vocab(alias_vocab("zzabbr", "term:zzcanon"))
        .expect("set_vocab on a reopened tagged cluster (ADR-074)");
    assert!(rebuilt > 100, "the rebuild covers the live corpus");

    // Alias-aware ground truth over the full live set, filtered via tag_of.
    let mut all = queries.clone();
    all.push((live_id, live_dsl.to_string()));
    let brute = Brute::build_with_vocab(
        &all,
        alias_vocab("zzabbr", "term:zzcanon")
            .to_normalizer()
            .unwrap(),
    );
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let title_canon = "1994 fleer zzcanon psa 10";
    let region = vec![("region".to_string(), vec!["emea".to_string()])];

    let check_cluster = |cluster: &ClusterEngine,
                         brute: &Brute,
                         blc: &mut String,
                         bfeats: &mut Vec<u32>,
                         phase: &str| {
        // The re-placed alias query keeps its interned tags; the live add keeps its
        // synthetic tag (the id with no recoverable string).
        let alias_cat = vec![("category".to_string(), vec![tag_of(q_alias)[0].1.clone()])];
        assert!(
            cluster
                .percolate_filtered(title_canon, &alias_cat)
                .unwrap()
                .contains(&q_alias),
            "{phase}: the re-placed alias query must keep its interned tags"
        );
        assert!(
            cluster
                .percolate_filtered(live_dsl, &region)
                .unwrap()
                .contains(&live_id),
            "{phase}: the post-freeze synthetic tag must survive"
        );
        // Differential sweep: filtered ≡ brute-with-tags, filtered ⊆ unfiltered.
        for (ti, title) in titles
            .iter()
            .map(String::as_str)
            .take(50)
            .chain([title_canon, live_dsl])
            .enumerate()
        {
            let truth = brute.matches(title, blc, bfeats);
            let unfiltered: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(
                unfiltered, truth,
                "{phase}: unfiltered ≠ alias-aware oracle (title {ti})"
            );
            for filter in filters_for(ti) {
                let got: HashSet<u64> = cluster
                    .percolate_filtered(title, &filter)
                    .unwrap()
                    .into_iter()
                    .collect();
                let want: HashSet<u64> = truth
                    .iter()
                    .copied()
                    .filter(|l| passes_filter(&tag_of(*l), &filter))
                    .collect();
                assert_eq!(
                    got, want,
                    "{phase}: filtered ≠ oracle (title {ti}, filter {filter:?})"
                );
                assert!(
                    got.is_subset(&unfiltered),
                    "{phase}: filter added a match (title {ti})"
                );
            }
        }
    };
    check_cluster(&reopened, &brute, &mut blc, &mut bfeats, "post-rebuild");
    drop(reopened);

    // Reopen #2: the rebuilt segments + manifest durably carry the tag columns.
    let reopened2 = ClusterEngine::open(dir.clone(), vocab(), None).expect("second reopen");
    check_cluster(&reopened2, &brute, &mut blc, &mut bfeats, "post-reopen");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn set_vocab_after_reopen_rebuilds_from_persisted_sources() {
    // Regression for the silent-corpus-loss bug the ADR-074 work surfaced: a durable shard
    // populated ONLY by bulk ingest (the build path) wrote durable segments but never
    // `sources.dat` — and a checkpoint on a clean shard early-returned past the sources
    // save too. After checkpoint + reopen, `live_sources` was EMPTY, so the next
    // `set_vocab` gathered nothing and rebuilt the cluster to zero queries (percolate
    // returned ∅ for everything). Matching itself never noticed (segments are the match
    // path), which is why no other oracle caught it. Untagged on purpose — the bug
    // predates tags.
    let queries = vec![
        (1u64, "1994 topps zzplayerone".to_string()),
        (2u64, "1995 fleer zzplayertwo".to_string()),
        (3u64, "1996 upper deck zzplayerthree".to_string()),
    ];
    let dir = unique_dir("reopen_then_set_vocab");
    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
            .expect("build");
        cluster.checkpoint().expect("checkpoint");
    }
    let mut reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    let rebuilt = reopened
        .set_vocab(alias_vocab("zzabbr", "term:zzcanon"))
        .expect("set_vocab after reopen");
    assert_eq!(
        rebuilt, 3,
        "the rebuild must gather the whole bulk-built corpus from persisted sources"
    );
    for (id, q) in &queries {
        assert!(
            reopened.percolate(q).unwrap().contains(id),
            "query {id} must survive reopen + set_vocab"
        );
    }
    // And the rebuilt state is itself durable.
    drop(reopened);
    let again = ClusterEngine::open(dir.clone(), vocab(), None).expect("second reopen");
    for (id, q) in &queries {
        assert!(
            again.percolate(q).unwrap().contains(id),
            "query {id} must survive the rebuilt cluster's reopen"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn set_vocab_after_delete_and_checkpoint_does_not_resurrect() {
    // The stale-sources sibling of the bug above: a DELETE removes the query from the live
    // source store in memory, but a checkpoint on a then-clean shard (the delete is a
    // tombstone, not a memtable entry) used to skip rewriting `sources.dat` — so a reopen
    // resurrected the deleted query's SOURCE, and the next `set_vocab` re-ingested it: a
    // deleted query matching again (a correctness violation worse than a false positive —
    // the caller deleted it). The checkpoint seal now persists the source store even when
    // the memtable is empty.
    let queries = vec![
        (1u64, "1994 topps zzplayerone".to_string()),
        (2u64, "1995 fleer zzplayertwo".to_string()),
    ];
    let dir = unique_dir("delete_then_set_vocab");
    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
            .expect("build");
        cluster.checkpoint().expect("first checkpoint");
        assert!(cluster.remove_query(2).expect("remove") >= 1);
        // The shard holding q2 has an EMPTY memtable here (a delete is a tombstone) — the
        // exact shape that used to skip the sources rewrite.
        cluster.checkpoint().expect("checkpoint after delete");
    }
    let mut reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert!(
        !reopened
            .percolate("1995 fleer zzplayertwo psa")
            .unwrap()
            .contains(&2),
        "deleted query must stay deleted after reopen"
    );
    let rebuilt = reopened
        .set_vocab(alias_vocab("zzabbr", "term:zzcanon"))
        .expect("set_vocab after delete + reopen");
    assert_eq!(rebuilt, 1, "only the live query rebuilds — no resurrection");
    assert!(
        !reopened
            .percolate("1995 fleer zzplayertwo psa")
            .unwrap()
            .contains(&2),
        "the vocabulary rebuild must NOT resurrect a deleted query"
    );
    assert!(
        reopened
            .percolate("1994 topps zzplayerone psa")
            .unwrap()
            .contains(&1),
        "the live query survives the rebuild"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn synthetic_only_tags_survive_set_vocab_after_reopen() {
    // Regression for the refusal-era blind spot ADR-074 closes structurally: a cluster whose
    // ONLY tags are post-freeze synthetic (untagged build ⇒ empty `tag_dict`) checkpoints and
    // reopens — and the `tags_present` latch restores from `tag_dict` emptiness alone, i.e.
    // FALSE. Under the old guard, set_vocab here would have passed the refusal and silently
    // dropped the tags (a filtered-read recall loss no oracle covered). With the carry-through,
    // the latch is no longer load-bearing: tags ride the stored `TagId`s regardless.
    let dir = unique_dir("synthonly_set_vocab");
    let seed = vec![(1u64, "1994 topps".to_string())];
    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &seed)
            .expect("untagged durable build");
        cluster
            .add_query_with_tags(
                100,
                "zzrarelivetag",
                &[("category".to_string(), "cards".to_string())],
            )
            .expect("synthetic-tagged live add");
        cluster.checkpoint().expect("checkpoint");
    }

    let mut reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    reopened
        .set_vocab(alias_vocab("zzabbr", "term:zzcanon"))
        .expect("set_vocab on a reopened synthetic-only-tagged cluster");
    let source = reopened
        .get_document(100)
        .expect("source lookup")
        .expect("synthetic-tagged source");
    assert_eq!(
        source.tags(),
        [("category".to_string(), "cards".to_string())],
        "raw read-back metadata must survive the same reopen + rebuild"
    );

    let cards = vec![("category".to_string(), vec!["cards".to_string()])];
    let coins = vec![("category".to_string(), vec!["coins".to_string()])];
    assert!(
        reopened
            .percolate_filtered("zzrarelivetag", &cards)
            .unwrap()
            .contains(&100),
        "a synthetic-only tag must survive reopen + set_vocab (the old guard's blind spot)"
    );
    assert!(
        !reopened
            .percolate_filtered("zzrarelivetag", &coins)
            .unwrap()
            .contains(&100),
        "the carried tag must still exclude under a non-matching filter"
    );
    let _ = std::fs::remove_dir_all(&dir);
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
