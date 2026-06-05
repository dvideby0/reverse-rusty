//! Checkpoint compacts the log; reopen still equals the oracle (incl. tagged reopen).

use crate::harness::*;

/// Per-query tags survive a durable reopen (ADR-049/055): a tagged cluster built with a `data_dir`,
/// with one LIVE tagged add, then checkpointed, must — after `open` from disk alone — answer filtered
/// percolations identically to the brute oracle. Exercises BOTH round-trips: the manifest's
/// `tag_dict_data` (the frozen tag space) and the cluster log's per-`Add` raw tags.
#[test]
fn tagged_cluster_survives_checkpoint_and_reopen() {
    let (queries, titles) = build_corpus();
    let tags = tags_parallel(&queries);
    let dir = unique_dir("tags_reopen");

    // The live add's id + tag (category=cards, a corpus value ⇒ a dense id shared with the corpus).
    let live_id = 9_000_001u64;
    let live_dsl = "zzrarelivetag";
    let live_tag = || vec![("category".to_string(), "cards".to_string())];

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
            .expect("live tagged add");
        cluster.checkpoint().expect("checkpoint");
    }

    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen from disk");

    // Brute oracle over the corpus + the live add; tag-of resolves the live add's tag.
    let mut all = queries.clone();
    all.push((live_id, live_dsl.to_string()));
    let tag_of = |l: u64| {
        if l == live_id {
            live_tag()
        } else {
            tags_for(l)
        }
    };
    let brute = Brute::build(&all);
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let mut sweep_titles = titles.clone();
    sweep_titles.push(live_dsl.to_string());
    let mut nonempty = 0usize;
    for (ti, title) in sweep_titles.iter().enumerate() {
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        let unfiltered: HashSet<u64> = reopened.percolate(title).unwrap().into_iter().collect();
        for filter in filters_for(ti) {
            let got: HashSet<u64> = reopened
                .percolate_filtered(title, &filter)
                .unwrap()
                .into_iter()
                .collect();
            let brute_filtered: HashSet<u64> = truth
                .iter()
                .copied()
                .filter(|l| passes_filter(&tag_of(*l), &filter))
                .collect();
            assert_eq!(
                got, brute_filtered,
                "reopened cluster filtered diverged from oracle (title {ti}, filter {filter:?})"
            );
            assert!(
                got.is_subset(&unfiltered),
                "filter added a match not in the unfiltered set after reopen"
            );
            if !got.is_empty() {
                nonempty += 1;
            }
        }
    }
    // The live tagged add specifically survives reopen and filters by its own tag.
    let cards = vec![("category".to_string(), vec!["cards".to_string()])];
    let coins = vec![("category".to_string(), vec!["coins".to_string()])];
    assert!(
        reopened
            .percolate_filtered(live_dsl, &cards)
            .unwrap()
            .contains(&live_id),
        "the live tagged add must survive reopen and pass its own (cards) filter"
    );
    assert!(
        !reopened
            .percolate_filtered(live_dsl, &coins)
            .unwrap()
            .contains(&live_id),
        "the live tagged add must NOT pass a different-category (coins) filter after reopen"
    );
    assert!(
        nonempty > 0,
        "degenerate: no filter ever matched after reopen"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_then_reopen_matches_oracle() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("checkpoint");

    let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable cluster builds");
    assert_eq!(cluster.epoch(), 0);
    apply_churn(&cluster, &added, &removed);

    let log_path = dir.join("cluster.log");
    let log_before = std::fs::metadata(&log_path).expect("log").len();
    cluster.checkpoint().expect("checkpoint");
    assert_eq!(cluster.epoch(), 1, "checkpoint bumps the epoch");
    let log_after = std::fs::metadata(&log_path).expect("log").len();
    assert!(
        log_after < log_before,
        "checkpoint truncated the log ({log_before} -> {log_after})"
    );

    // More churn after the checkpoint (lives only in the post-checkpoint log tail).
    let post_id = added.iter().map(|(id, _)| *id).max().unwrap_or(0) + 100;
    let post_dsl = queries
        .iter()
        .find(|(_, t)| t.contains("rareplayer") && !t.starts_with('('))
        .map(|(_, t)| t.clone())
        .expect("a class-A query");
    cluster.add_query(post_id, &post_dsl).expect("post add");
    drop(cluster);

    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert_eq!(reopened.epoch(), 1, "epoch persists across reopen");

    let mut live = final_live(&queries, &added, &removed);
    live.push((post_id, post_dsl));
    let brute = Brute::build(&live);
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for t in &titles {
        let want = brute.matches(t, &mut lc, &mut feats);
        let got: HashSet<u64> = reopened
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, want, "checkpoint reopen {t:?}");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
