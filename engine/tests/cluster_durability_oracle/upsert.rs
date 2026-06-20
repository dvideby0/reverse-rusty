//! ADR-070: cluster upsert durability — a single-frame `Upsert` replays to the same
//! state it produced live, through BOTH recovery paths (log-tail replay with no
//! checkpoint, and checkpoint + tail), with the reopened cluster ≡ pre-crash ≡ an
//! independent brute oracle over the post-upsert corpus.

use crate::harness::*;

#[test]
fn upserts_survive_reopen_via_log_tail_and_checkpoint() {
    let (queries, titles) = build_corpus();
    let base = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;

    // The upsert script, exercising the distinct shapes:
    //  - replace an existing query with another in-vocabulary DSL (the anchor — hence
    //    the placement shard — can move);
    //  - replace an existing query with a NEW-term DSL (absorbed as synthetic ids,
    //    ADR-046 — the dynamic-vocab path under replace semantics);
    //  - create a fresh id via upsert (the created outcome);
    //  - re-upsert one id twice (only the last version may match).
    let (v1_id, _) = queries[10].clone();
    let (_, v1_new_dsl) = queries[20].clone();
    let (v2_id, _) = queries[11].clone();
    let v2_new_dsl = "zzupserted gem mint".to_string();
    let v3_id = base;
    let (_, v3_dsl) = queries[30].clone();
    let (v4_id, _) = queries[12].clone();
    let (_, v4_mid_dsl) = queries[40].clone();
    let (_, v4_final_dsl) = queries[50].clone();

    let upserts: Vec<(u64, String)> = vec![
        (v1_id, v1_new_dsl),
        (v2_id, v2_new_dsl),
        (v3_id, v3_dsl),
        (v4_id, v4_mid_dsl),
        (v4_id, v4_final_dsl),
    ];

    // The post-upsert corpus the independent oracle is built over: last write per id wins.
    let final_corpus: Vec<(u64, String)> = {
        let mut by_id: std::collections::BTreeMap<u64, String> = queries.iter().cloned().collect();
        for (id, dsl) in &upserts {
            by_id.insert(*id, dsl.clone());
        }
        by_id.into_iter().collect()
    };

    // Probe the original titles plus one that only the new-term replacement matches.
    let mut probe_titles = titles.clone();
    probe_titles.push("zzupserted gem mint psa 10".to_string());

    for &(k, checkpoint) in &[(3usize, false), (3usize, true), (8usize, false)] {
        let dir = unique_dir(&format!("upsert_k{k}_ckpt{checkpoint}"));

        let pre_crash: Vec<Vec<u64>> = {
            let cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k, dir.clone(), false), &queries)
                    .expect("durable cluster builds");
            for (id, dsl) in &upserts {
                let (_removed, outcome) = cluster.upsert_query(*id, dsl, 1).expect("upsert");
                assert!(
                    !matches!(
                        outcome,
                        reverse_rusty::cluster::AddOutcome::RejectedParse(_)
                            | reverse_rusty::cluster::AddOutcome::RejectedClassD
                    ),
                    "script upserts must be accepted"
                );
            }
            if checkpoint {
                cluster.checkpoint().expect("checkpoint");
            }
            probe_titles
                .iter()
                .map(|t| cluster.percolate(t).expect("percolate"))
                .collect()
            // drop(cluster): without a checkpoint the whole upsert tail replays on open;
            // with one, the upserts are baked into committed segments.
        };

        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
        let brute = Brute::build(&final_corpus);
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();

        for (i, t) in probe_titles.iter().enumerate() {
            let want = brute.matches(t, &mut lc, &mut feats);
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(got, want, "k={k} ckpt={checkpoint} reopened≠brute on {t:?}");
            let pre: HashSet<u64> = pre_crash[i].iter().copied().collect();
            assert_eq!(
                got, pre,
                "k={k} ckpt={checkpoint} reopened≠pre-crash on {t:?}"
            );
        }

        // The replaced-away forms must not have resurrected on replay.
        let (_, v1_old_dsl) = &queries[10];
        let old_hits = reopened.percolate(v1_old_dsl).expect("percolate");
        let brute_old = brute.matches(v1_old_dsl, &mut lc, &mut feats);
        assert_eq!(
            old_hits.into_iter().collect::<HashSet<u64>>(),
            brute_old,
            "k={k} ckpt={checkpoint}: replaced-away version diverges from oracle"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
