//! Durable alias-import retry behavior after a live rebuild has been swapped.

use crate::harness::*;

#[test]
fn identical_alias_retry_recommits_an_uncommitted_rebuild() {
    let dir = unique_dir("alias_retry_recommit");
    let cfg = durable_cfg(3, dir.clone(), false);
    let manifest_path = dir.join("cluster_manifest.bin");
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "package adapter".into())])
        .expect("durable cluster");
    let mut old_manifest = read_cluster_manifest(&manifest_path).expect("baseline manifest");

    let applied = cluster
        .import_alias_synonyms("package, pkg")
        .expect("initial alias import");
    assert!(applied.applied);
    assert!(cluster.percolate("pkg adapter").unwrap().contains(&1));

    // Restore a pre-import commit point while retaining the coherent live
    // rebuild. Keep the in-memory epoch to model a checkpoint that failed
    // before publishing its manifest.
    old_manifest.epoch = cluster.epoch();
    reverse_rusty::storage::write_cluster_manifest(&old_manifest, &manifest_path)
        .expect("restore old manifest");

    let retry = cluster
        .import_alias_synonyms("package, pkg")
        .expect("identical retry must finish the durable commit");
    assert!(!retry.applied, "the registry itself remains unchanged");
    assert_eq!(retry.recompiled, 0, "the retry must not rebuild again");
    let recommitted = read_cluster_manifest(&manifest_path).expect("recommitted manifest");
    assert_eq!(
        recommitted.placement_generation,
        cluster.placement_generation(),
        "the no-op acknowledgement must refer to the live rebuild's commit"
    );

    drop(cluster);
    let reopened = ClusterEngine::open(&dir, vocab(), None).expect("reopen committed retry");
    assert!(
        reopened.percolate("pkg adapter").unwrap().contains(&1),
        "an acknowledged retry must preserve the imported alias across restart"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn identical_alias_retry_does_not_overwrite_an_unreadable_manifest() {
    let dir = unique_dir("alias_retry_corrupt_manifest");
    let cfg = durable_cfg(3, dir.clone(), false);
    let manifest_path = dir.join("cluster_manifest.bin");
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "package adapter".into())])
        .expect("durable cluster");
    cluster
        .import_alias_synonyms("package, pkg")
        .expect("initial alias import");

    let mut corrupt = std::fs::read(&manifest_path).expect("read manifest bytes");
    let last = corrupt.last_mut().expect("manifest CRC");
    *last ^= 0xff;
    std::fs::write(&manifest_path, &corrupt).expect("corrupt manifest CRC");

    let error = cluster
        .import_alias_synonyms("package, pkg")
        .expect_err("retry must fail loud on an unreadable manifest");
    assert!(
        matches!(error, ShardError::Log(_)),
        "unexpected retry error: {error:?}"
    );
    assert_eq!(
        std::fs::read(&manifest_path).expect("manifest after retry"),
        corrupt,
        "a no-op retry must never overwrite an incompatible commit point"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn identical_alias_retry_does_not_overwrite_incompatible_persisted_vocab() {
    let dir = unique_dir("alias_retry_incompatible_vocab");
    let cfg = durable_cfg(3, dir.clone(), false);
    let manifest_path = dir.join("cluster_manifest.bin");
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "package adapter".into())])
        .expect("durable cluster");
    cluster
        .import_alias_synonyms("package, pkg")
        .expect("initial alias import");

    let mut incompatible = read_cluster_manifest(&manifest_path).expect("committed manifest");
    incompatible.vocab_data = br#"{"future_vocab_field":[]}"#.to_vec();
    reverse_rusty::storage::write_cluster_manifest(&incompatible, &manifest_path)
        .expect("write CRC-valid incompatible vocab");
    let incompatible_bytes = std::fs::read(&manifest_path).expect("incompatible manifest bytes");

    let error = cluster
        .import_alias_synonyms("package, pkg")
        .expect_err("retry must fail loud on incompatible persisted vocab");
    assert!(
        matches!(error, ShardError::Log(_)),
        "unexpected retry error: {error:?}"
    );
    assert_eq!(
        std::fs::read(&manifest_path).expect("manifest after retry"),
        incompatible_bytes,
        "a no-op retry must not overwrite a semantically incompatible commit point"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn identical_alias_retry_does_not_overwrite_a_newer_manifest() {
    let dir = unique_dir("alias_retry_newer_manifest");
    let cfg = durable_cfg(3, dir.clone(), false);
    let manifest_path = dir.join("cluster_manifest.bin");
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "package adapter".into())])
        .expect("durable cluster");
    cluster
        .import_alias_synonyms("package, pkg")
        .expect("initial alias import");

    let mut newer = read_cluster_manifest(&manifest_path).expect("committed manifest");
    newer.epoch += 1;
    newer.placement_generation = newer
        .placement_generation
        .next()
        .expect("newer placement generation");
    reverse_rusty::storage::write_cluster_manifest(&newer, &manifest_path)
        .expect("write newer manifest");
    let newer_bytes = std::fs::read(&manifest_path).expect("newer manifest bytes");

    let error = cluster
        .import_alias_synonyms("package, pkg")
        .expect_err("retry must fail loud on a newer manifest");
    assert!(
        matches!(error, ShardError::Log(_)),
        "unexpected retry error: {error:?}"
    );
    assert_eq!(
        std::fs::read(&manifest_path).expect("manifest after retry"),
        newer_bytes,
        "a no-op retry must never roll back a newer commit point"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn identical_alias_retry_rejects_same_generation_topology_drift() {
    let dir = unique_dir("alias_retry_topology_drift");
    let cfg = durable_cfg(3, dir.clone(), false);
    let manifest_path = dir.join("cluster_manifest.bin");
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &[(1, "package adapter".into())])
        .expect("durable cluster");
    cluster
        .import_alias_synonyms("package, pkg")
        .expect("initial alias import");

    let mut divergent = read_cluster_manifest(&manifest_path).expect("committed manifest");
    divergent.vnodes += 1;
    reverse_rusty::storage::write_cluster_manifest(&divergent, &manifest_path)
        .expect("write divergent manifest");
    let divergent_bytes = std::fs::read(&manifest_path).expect("divergent manifest bytes");

    let error = cluster
        .import_alias_synonyms("package, pkg")
        .expect_err("retry must fail loud on same-generation topology drift");
    assert!(
        matches!(error, ShardError::Log(_)),
        "unexpected retry error: {error:?}"
    );
    assert_eq!(
        std::fs::read(&manifest_path).expect("manifest after retry"),
        divergent_bytes,
        "a no-op retry must not acknowledge a divergent topology"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
