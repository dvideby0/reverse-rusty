use super::*;
use crate::storage::{
    write_cluster_manifest, write_manifest, ClusterManifest, Manifest, SourceStore,
};

/// Write a valid `sources.dat` (so the round-trip fixtures pass the new
/// sources validation; the rejection tests write garbage on purpose).
fn write_valid_sources(path: &Path) {
    let store = SourceStore::new_resident();
    store.insert(1, "a stored query".into());
    store.write_to(path).unwrap();
}

fn empty_manifest(files: Vec<String>) -> Manifest {
    Manifest {
        segment_files: files,
        class_d_fence: false,
        hot_fence: false,
        source_generation_fence: false,
        hot_anchor_theta: 0,
        next_seg_id: 1,
        dict_data: Vec::new(),
        tag_dict_data: Vec::new(),
        rejected_parse: 0,
        rejected_class_d: 0,
        wal_seq_watermark: 0,
        segment_tombstones: Vec::new(),
        source_file_name: SOURCES.to_string(),
    }
}

fn tmp_root(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rr-backup-unit-{tag}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn staging_entries(dest: &Path) -> Vec<PathBuf> {
    let prefix = format!(
        "{}.backup.tmp.",
        dest.file_name()
            .and_then(|name| name.to_str())
            .expect("UTF-8 test destination")
    );
    let parent = dest.parent().expect("test destination parent");
    std::fs::read_dir(parent)
        .expect("read test destination parent")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .collect()
}

#[test]
fn engine_backup_round_trips_files_and_verifies() {
    let root = tmp_root("engine-rt");
    let src = root.join("src");
    std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
    // An empty-corpus manifest (no segments) + a WAL + sources.dat.
    write_manifest(&empty_manifest(vec![]), &src.join(ENGINE_MANIFEST)).unwrap();
    std::fs::write(src.join(ENGINE_WAL), b"wal-bytes").unwrap();
    write_valid_sources(&src.join(SOURCES));

    let dest = root.join("dest");
    copy_engine_dir(&src, &dest).unwrap();

    // Files are present and byte-identical.
    assert_eq!(
        std::fs::read(src.join(ENGINE_WAL)).unwrap(),
        std::fs::read(dest.join(ENGINE_WAL)).unwrap()
    );
    assert_eq!(
        std::fs::read(src.join(ENGINE_MANIFEST)).unwrap(),
        std::fs::read(dest.join(ENGINE_MANIFEST)).unwrap()
    );
    verify_backup(&dest).unwrap();
    // No leftover staging dir.
    assert!(staging_entries(&dest).is_empty());
}

#[test]
fn engine_backup_copies_only_manifest_selected_source_generation() {
    let root = tmp_root("engine-selected-source");
    let src = root.join("src");
    std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
    let selected = "sources_g00000000000000000003.dat";
    let orphan = "sources_g00000000000000000002.dat";
    write_valid_sources(&src.join(selected));
    write_valid_sources(&src.join(orphan));
    let mut manifest = empty_manifest(vec![]);
    manifest.source_file_name = selected.to_string();
    write_manifest(&manifest, &src.join(ENGINE_MANIFEST)).unwrap();

    let dest = root.join("dest");
    copy_engine_dir(&src, &dest).unwrap();
    assert!(dest.join(selected).exists());
    assert!(
        !dest.join(orphan).exists(),
        "unselected sidecar is an orphan"
    );
    verify_backup(&dest).unwrap();

    std::fs::remove_file(dest.join(selected)).unwrap();
    assert!(
        verify_backup(&dest).is_err(),
        "a v7-selected source sidecar is mandatory"
    );
}

#[test]
fn engine_backup_refuses_existing_dest() {
    let root = tmp_root("engine-dest-exists");
    let src = root.join("src");
    std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
    write_manifest(&empty_manifest(vec![]), &src.join(ENGINE_MANIFEST)).unwrap();
    let dest = root.join("dest");
    std::fs::create_dir_all(&dest).unwrap();
    match copy_engine_dir(&src, &dest) {
        Err(BackupError::DestExists(_)) => {}
        other => panic!("expected DestExists, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn engine_backup_refuses_dangling_symlink_destination() {
    use std::os::unix::fs::symlink;

    let root = tmp_root("engine-dangling-symlink");
    let src = root.join("src");
    std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
    write_manifest(&empty_manifest(vec![]), &src.join(ENGINE_MANIFEST)).unwrap();
    let dest = root.join("dest");
    symlink(root.join("missing-target"), &dest).unwrap();

    match copy_engine_dir(&src, &dest) {
        Err(BackupError::DestExists(path)) => assert_eq!(path, dest),
        other => panic!("expected DestExists, got {other:?}"),
    }
    assert!(
        std::fs::symlink_metadata(&dest)
            .expect("destination symlink remains")
            .file_type()
            .is_symlink(),
        "backup must not replace a dangling destination symlink"
    );
}

#[test]
fn final_promotion_refuses_an_entry_created_after_staging() {
    let root = tmp_root("commit-race");
    let dest = root.join("dest");
    let staging = reserve_staging_dir(&dest).expect("reserve staging");
    std::fs::create_dir(&dest).expect("competing destination");

    match commit_staging(&staging, &dest) {
        Err(BackupError::DestExists(path)) => assert_eq!(path, dest),
        other => panic!("expected DestExists, got {other:?}"),
    }
    assert!(dest.is_dir(), "competing destination must remain");
    assert!(
        staging.is_dir(),
        "failed promotion retains its staging tree"
    );
    std::fs::remove_dir_all(staging).unwrap();
}

#[test]
fn staging_reservations_are_unique() {
    let root = tmp_root("unique-staging");
    let dest = root.join("dest");
    let first = reserve_staging_dir(&dest).expect("first staging");
    let second = reserve_staging_dir(&dest).expect("second staging");
    assert_ne!(first, second);
    std::fs::remove_dir_all(first).unwrap();
    std::fs::remove_dir_all(second).unwrap();
}

#[test]
fn engine_backup_without_manifest_is_valid_wal_only() {
    let root = tmp_root("engine-wal-only");
    let src = root.join("src");
    std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
    std::fs::write(src.join(ENGINE_WAL), b"wal-only").unwrap();
    let dest = root.join("dest");
    copy_engine_dir(&src, &dest).unwrap();
    assert!(dest.join(ENGINE_WAL).exists());
    assert!(!dest.join(ENGINE_MANIFEST).exists());
    verify_backup(&dest).unwrap();
}

#[test]
fn verify_detects_missing_and_corrupt_segment() {
    let root = tmp_root("verify-bad");
    let dir = root.join("backup");
    std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).unwrap();
    // Manifest references a segment that does not exist → MissingSegment.
    write_manifest(
        &empty_manifest(vec!["seg_000001.seg".into()]),
        &dir.join(ENGINE_MANIFEST),
    )
    .unwrap();
    match verify_backup(&dir) {
        Err(BackupError::MissingSegment(n)) => assert_eq!(n, "seg_000001.seg"),
        other => panic!("expected MissingSegment, got {other:?}"),
    }
    // Now create a garbage "segment" → CorruptSegment (fails MmapSegment::open).
    std::fs::write(
        dir.join(SEGMENTS_DIR).join("seg_000001.seg"),
        b"not a segment",
    )
    .unwrap();
    match verify_backup(&dir) {
        Err(BackupError::CorruptSegment { name, .. }) => assert_eq!(name, "seg_000001.seg"),
        other => panic!("expected CorruptSegment, got {other:?}"),
    }
}

#[test]
fn cluster_backup_round_trips_and_verifies() {
    let root = tmp_root("cluster-rt");
    let src = root.join("src");
    // Two shards, both with empty registries (empty corpus) + a coordinator log.
    for i in 0..2 {
        std::fs::create_dir_all(src.join(shard_dir_name(i)).join(SEGMENTS_DIR)).unwrap();
        write_valid_sources(&src.join(shard_dir_name(i)).join(SOURCES));
    }
    std::fs::write(src.join(CLUSTER_LOG), b"clog").unwrap();
    let manifest = ClusterManifest {
        epoch: 1,
        snapshot_pos: 0,
        dict_fingerprint: 0,
        num_shards: 2,
        vnodes: 64,
        include_broad: true,
        broad_replicate_all: true,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL,
        segment_registry: vec![vec![], vec![]],
        next_seg_ids: vec![1, 1],
        compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
        source_files: vec![SOURCES.into(), SOURCES.into()],
        dict_data: Vec::new(),
        vocab_data: Vec::new(),
        tag_dict_data: Vec::new(),
    };
    write_cluster_manifest(&manifest, &src.join(CLUSTER_MANIFEST)).unwrap();

    let dest = root.join("dest");
    copy_cluster_dir(&src, &dest).unwrap();
    assert!(dest.join(CLUSTER_LOG).exists());
    assert!(dest.join(CLUSTER_MANIFEST).exists());
    assert!(dest.join(shard_dir_name(1)).join(SOURCES).exists());
    verify_cluster_backup(&dest).unwrap();
}

#[test]
fn cluster_verify_requires_manifest() {
    let root = tmp_root("cluster-no-manifest");
    let dir = root.join("backup");
    std::fs::create_dir_all(&dir).unwrap();
    match verify_cluster_backup(&dir) {
        Err(BackupError::MissingManifest(_)) => {}
        other => panic!("expected MissingManifest, got {other:?}"),
    }
}

#[test]
fn verify_rejects_corrupt_sources() {
    // A corrupt sources.dat (open would fail loading it) must fail verify, not be
    // silently accepted (codex P1).
    let root = tmp_root("engine-corrupt-sources");
    let dir = root.join("backup");
    std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).unwrap();
    write_manifest(&empty_manifest(vec![]), &dir.join(ENGINE_MANIFEST)).unwrap();
    std::fs::write(dir.join(SOURCES), b"not a valid sources store").unwrap();
    assert!(
        verify_backup(&dir).is_err(),
        "corrupt sources must fail verify"
    );
}

#[test]
fn cluster_verify_rejects_corrupt_shard_sources() {
    // A corrupt per-shard sources.dat must fail verify (codex P1): otherwise the
    // endpoint acks a backup ClusterEngine::open later refuses.
    let root = tmp_root("cluster-corrupt-sources");
    let dir = root.join("backup");
    for i in 0..2 {
        std::fs::create_dir_all(dir.join(shard_dir_name(i)).join(SEGMENTS_DIR)).unwrap();
    }
    write_valid_sources(&dir.join(shard_dir_name(0)).join(SOURCES));
    std::fs::write(dir.join(shard_dir_name(1)).join(SOURCES), b"corrupt").unwrap();
    let manifest = ClusterManifest {
        epoch: 1,
        snapshot_pos: 0,
        dict_fingerprint: 0,
        num_shards: 2,
        vnodes: 64,
        include_broad: true,
        broad_replicate_all: true,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL,
        segment_registry: vec![vec![], vec![]],
        next_seg_ids: vec![1, 1],
        compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
        source_files: vec![SOURCES.into(), SOURCES.into()],
        dict_data: Vec::new(),
        vocab_data: Vec::new(),
        tag_dict_data: Vec::new(),
    };
    write_cluster_manifest(&manifest, &dir.join(CLUSTER_MANIFEST)).unwrap();
    assert!(
        verify_cluster_backup(&dir).is_err(),
        "corrupt shard sources must fail verify"
    );
}

#[test]
fn copy_verifies_before_commit_so_a_bad_source_leaves_no_dest() {
    // A manifest referencing a corrupt segment fails verification, which now runs
    // on the staging tree BEFORE the rename (codex P2) — so no dest is created and
    // a retry isn't blocked by a half-written backup.
    let root = tmp_root("verify-before-commit");
    let src = root.join("src");
    std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
    std::fs::write(src.join(SEGMENTS_DIR).join("seg_000001.seg"), b"garbage").unwrap();
    write_manifest(
        &empty_manifest(vec!["seg_000001.seg".into()]),
        &src.join(ENGINE_MANIFEST),
    )
    .unwrap();
    let dest = root.join("dest");
    match copy_engine_dir(&src, &dest) {
        Err(BackupError::CorruptSegment { .. }) => {}
        other => panic!("expected CorruptSegment, got {other:?}"),
    }
    assert!(
        !dest.exists(),
        "verify failure must not leave a dest behind"
    );
    assert!(
        staging_entries(&dest).is_empty(),
        "owned staging must be cleaned up"
    );
}
