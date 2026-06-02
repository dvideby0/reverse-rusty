//! Unit tests for the coordinator that need private-state access (e.g. the durable
//! `log` field), kept in-module rather than in the integration oracles.

use super::*;

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rr_clog_coord_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// WAL-first fail-closed: when the durable log append fails, the add is rejected with
/// `ShardError::Log` AND no shard is mutated (the query never becomes matchable). Needs
/// private `log` access, so it lives here rather than in the integration oracle.
#[test]
fn add_query_is_fail_closed_when_log_append_fails() {
    let dir = scratch_dir("failclosed");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    // Build over a seed corpus so the frozen dict knows these tokens.
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");
    let before = cluster.num_queries().expect("count");

    // Break the durable log, then attempt an add of an in-vocabulary query.
    cluster.log.break_writes_for_test();
    let res = cluster.add_query(2, "1995 fleer");
    assert!(
        matches!(res, Err(ShardError::Log(_))),
        "expected Log error, got {res:?}"
    );

    // No shard was mutated: count unchanged and id 2 is not matchable.
    assert_eq!(cluster.num_queries().expect("count"), before);
    let hits = cluster.percolate("1995 fleer").expect("percolate");
    assert!(
        !hits.contains(&2),
        "rejected add must not be matchable: {hits:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// On-disk fingerprint guard: a manifest whose stored `dict_fingerprint` disagrees with
/// the dict it carries must fail `open` loud with `ShardError::DictMismatch` (ADR-030
/// parity for persisted state), never silently opening a divergent feature space. The
/// manifest is rewritten through `write_cluster_manifest` so its trailing CRC stays valid,
/// which exercises the fingerprint check itself — not the CRC check the integration
/// oracle's `corrupt_manifest_*` test already covers.
#[test]
fn open_rejects_manifest_with_divergent_dict_fingerprint() {
    let dir = scratch_dir("fpmismatch");
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    // Flip only the stored fingerprint, then rewrite with a fresh (valid) CRC. The dict
    // bytes are untouched, so on open the dict's recomputed fingerprint won't match.
    let mpath = dir.join(CLUSTER_MANIFEST_FILE);
    let mut manifest = crate::storage::read_cluster_manifest(&mpath).expect("read manifest");
    manifest.dict_fingerprint ^= 0xDEAD_BEEF_DEAD_BEEF;
    crate::storage::write_cluster_manifest(&manifest, &mpath).expect("rewrite manifest");

    // ClusterEngine isn't Debug, so match explicitly rather than `{:?}`-printing the Ok arm.
    match ClusterEngine::open(dir.clone(), vocab(), None) {
        Err(ShardError::DictMismatch { .. }) => {}
        Err(other) => panic!("expected DictMismatch, got {other:?}"),
        Ok(_) => panic!("expected DictMismatch, but open() succeeded"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
