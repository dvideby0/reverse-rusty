//! Cluster backup/restore (ADR-079, ADR-065 criterion 11).
//!
//! `ClusterEngine::backup_to` checkpoints, then snapshots the coordinator manifest +
//! per-shard segments + `sources.dat` + the coordinator log into a fresh dir. Restore
//! is `ClusterEngine::open` on the (relocated) backup. These tests prove the restore
//! ≡ pre-backup ≡ brute across shard counts and broad on/off, that the snapshot is
//! point-in-time (isolated from post-backup churn), that tagged corpora round-trip
//! (the tag space embedded in the manifest survives), that a restored backup is a
//! legitimate checkpoint root, and that the fail-loud paths hold.

use crate::harness::*;
use reverse_rusty::storage::{self, BackupError};

/// Build durable → churn → backup → open the backup ≡ pre-backup ≡ brute, K×broad.
#[test]
fn backup_then_open_matches_oracle_across_shards_and_broad() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);

    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("backup_src_k{k}"));
        let backup = unique_dir(&format!("backup_dest_k{k}"));

        let pre: Vec<(Vec<u64>, Vec<u64>)> = {
            let cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k, dir.clone(), false), &queries)
                    .expect("durable cluster builds");
            apply_churn(&cluster, &added, &removed);
            let snap = titles
                .iter()
                .map(|t| {
                    (
                        cluster.percolate(t).expect("percolate"),
                        cluster.percolate_with_broad(t, false).expect("percolate"),
                    )
                })
                .collect();
            cluster.backup_to(&backup).expect("cluster backup");
            snap
        };

        // Restore = open the BACKUP dir (a different path than the source).
        let restored = ClusterEngine::open(backup.clone(), vocab(), None).expect("open backup");
        let cc = restored.class_counts().expect("class counts");
        assert!(cc[0] > 0 && cc[1] > 0 && cc[2] > 0, "k={k}: classes {cc:?}");

        let brute = Brute::build(&final_live(&queries, &added, &removed));
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();
        for (i, t) in titles.iter().enumerate() {
            let want = brute.matches(t, &mut lc, &mut feats);
            let got: HashSet<u64> = restored
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(got, want, "k={k} restored≠brute broad-on {t:?}");

            let pre_b: HashSet<u64> = pre[i].0.iter().copied().collect();
            assert_eq!(got, pre_b, "k={k} restored≠pre-backup broad-on {t:?}");

            let got_sel: HashSet<u64> = restored
                .percolate_with_broad(t, false)
                .expect("percolate")
                .into_iter()
                .collect();
            let pre_s: HashSet<u64> = pre[i].1.iter().copied().collect();
            assert_eq!(got_sel, pre_s, "k={k} restored≠pre-backup broad-off {t:?}");
        }

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&backup);
    }
}

/// The backup is a point-in-time snapshot: churning the source AFTER the backup does
/// not change what the backup restores to.
#[test]
fn backup_isolated_from_post_backup_churn() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("backup_iso_src");
    let backup = unique_dir("backup_iso_dest");

    let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable cluster builds");
    apply_churn(&cluster, &added, &removed);
    let pre: Vec<Vec<u64>> = titles
        .iter()
        .map(|t| {
            let mut v = cluster.percolate(t).expect("percolate");
            v.sort_unstable();
            v
        })
        .collect();
    cluster.backup_to(&backup).expect("cluster backup");

    // Remove 50 of the original queries from the SOURCE after the backup.
    for (id, _) in queries.iter().take(50) {
        let _ = cluster.remove_query(*id);
    }

    let restored = ClusterEngine::open(backup.clone(), vocab(), None).expect("open backup");
    for (i, t) in titles.iter().enumerate() {
        let got: HashSet<u64> = restored
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        let want: HashSet<u64> = pre[i].iter().copied().collect();
        assert_eq!(
            got, want,
            "post-backup churn leaked into the snapshot for {t:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&backup);
}

/// A tagged corpus round-trips through backup/restore: the frozen tag space embedded
/// in the cluster manifest is copied verbatim, so filtered percolation on the restore
/// ≡ the tag-aware brute oracle.
#[test]
fn backup_tagged_corpus_filtered_matches_oracle() {
    let (queries, titles) = build_corpus();
    let tags = tags_parallel(&queries);

    let brute = Brute::build(&queries);
    let mut blc = String::new();
    let mut bfeats: Vec<u32> = Vec::new();
    let truth: Vec<HashSet<u64>> = titles
        .iter()
        .map(|t| brute.matches(t, &mut blc, &mut bfeats))
        .collect();

    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("backup_tag_src_k{k}"));
        let backup = unique_dir(&format!("backup_tag_dest_k{k}"));
        {
            let cfg = ClusterConfig {
                num_shards: k,
                include_broad: true,
                data_dir: Some(dir.clone()),
                ..ClusterConfig::default()
            };
            let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
                .expect("tagged durable build");
            cluster.backup_to(&backup).expect("cluster backup");
        }

        let restored = ClusterEngine::open(backup.clone(), vocab(), None).expect("open backup");
        for (ti, title) in titles.iter().enumerate() {
            for filter in filters_for(ti) {
                let got: HashSet<u64> = restored
                    .percolate_filtered(title, &filter)
                    .expect("filtered percolate")
                    .into_iter()
                    .collect();
                let brute_filtered: HashSet<u64> = truth[ti]
                    .iter()
                    .copied()
                    .filter(|l| passes_filter(&tags_for(*l), &filter))
                    .collect();
                assert_eq!(
                    got, brute_filtered,
                    "k={k} restored filtered≠brute (title {ti}, filter {filter:?})"
                );
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&backup);
    }
}

/// A restored backup is a legitimate durable root: open it, checkpoint (a fresh
/// epoch), drop, reopen — still ≡ brute.
#[test]
fn backup_then_open_then_checkpoint_is_a_valid_root() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("backup_idem_src");
    let backup = unique_dir("backup_idem_dest");

    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
            .expect("durable cluster builds");
        apply_churn(&cluster, &added, &removed);
        cluster.backup_to(&backup).expect("cluster backup");
    }
    {
        let restored = ClusterEngine::open(backup.clone(), vocab(), None).expect("open backup");
        restored
            .checkpoint()
            .expect("checkpoint the restored backup");
    }
    let reopened =
        ClusterEngine::open(backup.clone(), vocab(), None).expect("reopen after checkpoint");

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
        assert_eq!(got, want, "backup→open→checkpoint→reopen≠brute {t:?}");
    }

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&backup);
}

/// An in-memory cluster has nothing on disk → `ShardError::Config`, no dest created.
#[test]
fn backup_refuses_in_memory_cluster() {
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 3,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("in-memory build");
    let dest = unique_dir("backup_inmem_dest");
    match cluster.backup_to(&dest) {
        Err(ShardError::Config(_)) => {}
        other => panic!("expected Config error, got {other:?}"),
    }
    assert!(!dest.exists(), "no dest created for an in-memory cluster");
}

/// An existing dest is rejected as a 400-class `Config` error BEFORE `checkpoint()`
/// runs (codex P2): the epoch is unchanged, proving the bad request had no side
/// effect (no epoch bump / log truncation).
#[test]
fn backup_refuses_existing_dest_without_checkpointing() {
    let (queries, _titles) = build_corpus();
    let dir = unique_dir("backup_dest_exists_src");
    let backup = unique_dir("backup_dest_exists_dest");
    std::fs::create_dir_all(&backup).unwrap(); // pre-existing dest

    let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable cluster builds");
    let epoch_before = cluster.epoch();
    match cluster.backup_to(&backup) {
        Err(ShardError::Config(_)) => {}
        other => panic!("expected Config error for an existing dest, got {other:?}"),
    }
    assert_eq!(
        cluster.epoch(),
        epoch_before,
        "checkpoint must not run when the dest already exists"
    );

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&backup);
}

/// `verify_cluster_backup` catches a corrupted per-shard segment.
#[test]
fn cluster_backup_corrupt_segment_fails_verify() {
    let (queries, _titles) = build_corpus();
    let dir = unique_dir("backup_corrupt_src");
    let backup = unique_dir("backup_corrupt_dest");
    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
            .expect("durable cluster builds");
        cluster.backup_to(&backup).expect("cluster backup");
    }
    storage::verify_cluster_backup(&backup).expect("fresh cluster backup verifies");

    // Flip a byte in the first backed-up shard segment we find.
    let mut corrupted = false;
    for s in 0..3usize {
        let seg_dir = backup.join(format!("shard_{s:03}")).join("segments");
        let Ok(rd) = std::fs::read_dir(&seg_dir) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().and_then(|x| x.to_str()) == Some("seg") {
                let mut bytes = std::fs::read(&p).unwrap();
                let mid = bytes.len() / 2;
                bytes[mid] ^= 0xFF;
                std::fs::write(&p, bytes).unwrap();
                corrupted = true;
                break;
            }
        }
        if corrupted {
            break;
        }
    }
    assert!(
        corrupted,
        "expected at least one backed-up segment to corrupt"
    );
    match storage::verify_cluster_backup(&backup) {
        Err(BackupError::CorruptSegment { .. }) => {}
        other => panic!("expected CorruptSegment, got {other:?}"),
    }

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&backup);
}
