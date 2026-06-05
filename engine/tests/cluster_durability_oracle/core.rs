//! Headline: rebuild-from-log ≡ pre-crash ≡ brute, across shards and broad.

use crate::harness::*;

#[test]
fn reopened_cluster_matches_oracle_across_shards_and_broad() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);

    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("headline_k{k}"));

        // Build durable, churn live mutations, snapshot pre-crash results, then "crash".
        let pre_crash: Vec<(Vec<u64>, Vec<u64>)> = {
            let cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k, dir.clone(), false), &queries)
                    .expect("durable cluster builds");
            apply_churn(&cluster, &added, &removed);
            titles
                .iter()
                .map(|t| {
                    (
                        cluster.percolate(t).expect("percolate"),
                        cluster.percolate_with_broad(t, false).expect("percolate"),
                    )
                })
                .collect()
            // drop(cluster) — no checkpoint: recovery replays the whole log tail.
        };

        // Reopen from disk alone.
        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
        let cc = reopened.class_counts().expect("class counts");
        assert!(cc[0] > 0 && cc[1] > 0 && cc[2] > 0, "k={k}: classes {cc:?}");

        // Independent oracle over the final live set.
        let brute = Brute::build(&final_live(&queries, &added, &removed));
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();

        for (i, t) in titles.iter().enumerate() {
            let want = brute.matches(t, &mut lc, &mut feats);
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(got, want, "k={k} reopened≠brute broad-on {t:?}");

            let pre_b: HashSet<u64> = pre_crash[i].0.iter().copied().collect();
            assert_eq!(got, pre_b, "k={k} reopened≠pre-crash broad-on {t:?}");

            let got_sel: HashSet<u64> = reopened
                .percolate_with_broad(t, false)
                .expect("percolate")
                .into_iter()
                .collect();
            let pre_s: HashSet<u64> = pre_crash[i].1.iter().copied().collect();
            assert_eq!(got_sel, pre_s, "k={k} reopened≠pre-crash broad-off {t:?}");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
