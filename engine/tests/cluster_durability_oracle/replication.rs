//! ADR-035: per-shard replication survives reopen (durable replication_factor > 1).

use crate::harness::*;

/// A durable cluster with replicas (RF = 2), churned then crashed, reopens to EXACTLY the
/// pre-crash and brute sets. `open` attaches each primary from the manifest, peer-recovers its
/// replicas from that primary, and the log-tail replay then feeds the primary AND its replicas
/// through the composite — so a reopened replicated cluster is correct end-to-end.
#[test]
fn durable_replicated_cluster_reopens_and_matches() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    for &k in &[1usize, 3] {
        let dir = unique_dir(&format!("replicated_k{k}"));
        let cfg = ClusterConfig {
            num_shards: k,
            replication_factor: 2,
            data_dir: Some(dir.clone()),
            ..Default::default()
        };
        let pre_crash: Vec<Vec<u64>> = {
            let cluster =
                ClusterEngine::build(vocab(), &cfg, &queries).expect("durable replicated builds");
            apply_churn(&cluster, &added, &removed);
            titles
                .iter()
                .map(|t| cluster.percolate(t).expect("percolate"))
                .collect()
            // drop(cluster) — crash; no checkpoint, so recovery replays the whole log tail.
        };

        // Reopen WITH replication_factor = 2 so `open` peer-recovers the replicas.
        let reopened =
            ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen replicated");
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
            assert_eq!(got, want, "k={k} rf=2 reopened≠brute {t:?}");
            let pre: HashSet<u64> = pre_crash[i].iter().copied().collect();
            assert_eq!(got, pre, "k={k} rf=2 reopened≠pre-crash {t:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Checkpoint at RF = 2 seals only the PRIMARIES (replicas are not in the manifest); the
/// cluster still reopens to the oracle set, with replicas peer-recovered from the sealed base.
#[test]
fn checkpoint_with_replicas_reopens_and_matches() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("ckpt_replicated");
    let cfg = ClusterConfig {
        num_shards: 3,
        replication_factor: 2,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    {
        let cluster =
            ClusterEngine::build(vocab(), &cfg, &queries).expect("durable replicated builds");
        apply_churn(&cluster, &added, &removed);
        cluster
            .checkpoint()
            .expect("checkpoint seals primaries only");
        assert_eq!(cluster.epoch(), 1, "checkpoint bumps the epoch");
    }
    let reopened =
        ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen replicated");
    let brute = Brute::build(&final_live(&queries, &added, &removed));
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for t in &titles {
        let want = brute.matches(t, &mut lc, &mut feats);
        let got: HashSet<u64> = reopened
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, want, "rf=2 checkpoint reopen {t:?}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
