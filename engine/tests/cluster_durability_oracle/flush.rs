//! Bare cluster flushes fail loud when a local shard falls back to RAM.

use crate::harness::*;

#[cfg(unix)]
#[test]
fn cluster_flush_surfaces_a_local_shard_persistence_failure() {
    use std::os::unix::fs::PermissionsExt;

    let dir = unique_dir("flush_fail_loud");
    let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &[])
        .expect("durable cluster");
    cluster
        .add_query(7, "1994 acme")
        .expect("WAL-backed live add");

    let mut original = Vec::new();
    for shard in 0..3 {
        let segments = dir.join(format!("shard_{shard:03}/segments"));
        let permissions = std::fs::metadata(&segments)
            .expect("segments directory")
            .permissions();
        std::fs::set_permissions(&segments, std::fs::Permissions::from_mode(0o555))
            .expect("make segments read-only");
        original.push((segments, permissions));
    }

    let result = cluster.flush();
    for (segments, permissions) in original {
        std::fs::set_permissions(segments, permissions).expect("restore permissions");
    }

    let error = result.expect_err("the durable shard failure must cross the cluster seam");
    assert!(matches!(error, ShardError::Log(_)), "{error}");
    assert!(
        cluster.percolate("1994 acme").expect("read").contains(&7),
        "the failed durable flush must keep the in-memory fallback readable"
    );

    drop(cluster);
    let _ = std::fs::remove_dir_all(dir);
}
