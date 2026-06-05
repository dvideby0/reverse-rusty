//! The durable and in-memory backends agree step-for-step (the seam holds), plus the
//! open-time guards & fail-loud paths.

use crate::harness::*;

// ---- the durable and in-memory backends agree step-for-step (the seam holds) ----

#[test]
fn durable_and_in_memory_backends_agree() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("differential");

    let durable = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable cluster builds");
    let in_mem = {
        let cfg = ClusterConfig {
            num_shards: 3,
            ..Default::default()
        };
        ClusterEngine::build(vocab(), &cfg, &queries).expect("in-memory cluster builds")
    };
    apply_churn(&durable, &added, &removed);
    apply_churn(&in_mem, &added, &removed);

    for t in &titles {
        let a: HashSet<u64> = durable
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        let b: HashSet<u64> = in_mem
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(a, b, "backend mismatch on {t:?}");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---- guards & fail-loud ----

#[test]
fn open_without_manifest_is_an_error() {
    let dir = unique_dir("nomanifest");
    std::fs::create_dir_all(&dir).expect("mkdir");
    match ClusterEngine::open(dir.clone(), vocab(), None) {
        Err(ShardError::Config(_)) => {}
        Err(other) => panic!("expected Config error, got {other:?}"),
        Ok(_) => panic!("expected Config error, opened a cluster from an empty dir"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ingest_on_a_populated_durable_cluster_is_rejected() {
    let (queries, _) = build_corpus();
    let dir = unique_dir("ingestguard");
    let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable cluster builds");
    match cluster.ingest(&queries) {
        Err(ShardError::Config(_)) => {}
        Err(other) => panic!("expected Config error, got {other:?}"),
        Ok(()) => panic!("expected Config error; ingest on a populated cluster must be rejected"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn corrupt_manifest_fails_loud_not_silently_empty() {
    let (queries, _) = build_corpus();
    let dir = unique_dir("corruptmanifest");
    {
        ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
            .expect("durable cluster builds");
    }
    // Flip a byte in the manifest — the trailing CRC must catch it.
    let mpath = dir.join("cluster_manifest.bin");
    let mut bytes = std::fs::read(&mpath).expect("read manifest");
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&mpath, &bytes).expect("write manifest");

    assert!(
        ClusterEngine::open(dir.clone(), vocab(), None).is_err(),
        "a corrupt manifest must fail loud, never silently open empty"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
