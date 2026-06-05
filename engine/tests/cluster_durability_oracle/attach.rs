//! Step 3b: attach-and-mmap (segments are the base), not re-ingest — plus the
//! segment-level fail-loud guard.

use crate::harness::*;

/// The defining 3b property: after a checkpoint the per-shard COMPILED segments are the
/// committed base. Deleting the mutation log entirely (and there is no raw-DSL snapshot)
/// must still answer the full corpus on reopen — the answer can ONLY come from the
/// attached segments. Also asserts the on-disk layout (per-shard `.seg`, no per-shard
/// `wal.log`, no `cluster_snapshot_*.dat`).
#[test]
fn reopen_attaches_segments_with_no_log_or_snapshot() {
    let (queries, titles) = build_corpus();
    let dir = unique_dir("attach");
    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
            .expect("durable cluster builds");
        cluster
            .checkpoint()
            .expect("checkpoint commits the base into the registry");
    }

    // Layout: each shard has committed `.seg` files, no per-shard WAL, and there is no
    // raw-DSL snapshot file anywhere under the cluster dir.
    for s in 0..3 {
        let shard = dir.join(format!("shard_{s:03}"));
        let has_seg = std::fs::read_dir(shard.join("segments"))
            .expect("shard segments dir")
            .flatten()
            .any(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("seg"))
            });
        assert!(has_seg, "shard {s} has no committed .seg files");
        assert!(
            !shard.join("wal.log").exists(),
            "shard {s} unexpectedly has a per-shard wal.log"
        );
    }
    assert!(
        std::fs::read_dir(&dir)
            .expect("cluster dir")
            .flatten()
            .all(|e| !e
                .file_name()
                .to_string_lossy()
                .starts_with("cluster_snapshot")),
        "a raw-DSL cluster_snapshot_*.dat exists — 3b should have removed it"
    );

    // Delete the log: the attached segments alone must answer the full corpus.
    std::fs::remove_file(dir.join("cluster.log")).expect("remove log");
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen with no log");
    let brute = Brute::build(&queries);
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for t in &titles {
        let want = brute.matches(t, &mut lc, &mut feats);
        let got: HashSet<u64> = reopened
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, want, "attach-only (no log) reopen != brute {t:?}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The bug-catcher (the hole the design review surfaced): a `Remove` against a query that
/// lives in a BASE segment only tombstones the in-RAM alive overlay. If `checkpoint` did
/// not re-seal that segment, the deletion would be lost once its `Remove` is truncated
/// from the log, and the query would RESURRECT on reopen (a false positive). This must NOT
/// happen. Fails against a naive "flush the memtable only" checkpoint; passes once
/// checkpoint re-seals tombstoned base segments (ADR-032).
#[test]
fn checkpoint_after_removing_a_build_time_query_does_not_resurrect_it() {
    // A controlled corpus so we know `Q` (id 7) is a build-time query and a title matches it.
    let queries = vec![
        (1u64, "1994 topps".to_string()),
        (7u64, "rareplayer42 1994 topps".to_string()),
        (8u64, "rareplayer99 1995 fleer".to_string()),
    ];
    let title_q = "rareplayer42 1994 topps psa 10";
    let survivors = vec![
        (1u64, "1994 topps".to_string()),
        (8u64, "rareplayer99 1995 fleer".to_string()),
    ];

    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("baseremove_k{k}"));
        {
            let cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k, dir.clone(), false), &queries)
                    .expect("durable cluster builds");
            // Q is in a base segment (build ingests to base) and matches the title.
            assert!(
                cluster.percolate(title_q).expect("percolate").contains(&7),
                "k={k}: Q must match before removal"
            );
            cluster.remove_query(7).expect("remove Q");
            assert!(
                !cluster.percolate(title_q).expect("percolate").contains(&7),
                "k={k}: Q must be gone immediately after removal"
            );
            // Checkpoint truncates the Remove from the log; the re-seal must bake it in.
            cluster.checkpoint().expect("checkpoint");
        }

        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
        assert!(
            !reopened.percolate(title_q).expect("percolate").contains(&7),
            "k={k}: a removed build-time query RESURRECTED after checkpoint + reopen"
        );
        // And the result equals the independent oracle over the surviving set.
        let brute = Brute::build(&survivors);
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();
        let want = brute.matches(title_q, &mut lc, &mut feats);
        let got: HashSet<u64> = reopened
            .percolate(title_q)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got, want,
            "k={k}: reopened != brute after base-remove checkpoint"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// A crash mid-checkpoint can leave a freshly-flushed `.seg` the committed manifest never
/// recorded. Such an orphan must be IGNORED on open (it is not in the registry, so it is
/// never attached), the result must still equal the oracle, and a later checkpoint GCs it.
#[test]
fn orphan_segment_files_are_ignored_on_open_and_gced() {
    let (queries, titles) = build_corpus();
    let dir = unique_dir("orphan");
    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
            .expect("durable cluster builds");
        cluster.checkpoint().expect("checkpoint");
    }
    // Stray segment file not referenced by the committed manifest (a mid-checkpoint crash).
    let stray = dir
        .join("shard_000")
        .join("segments")
        .join("seg_999999.seg");
    std::fs::write(&stray, b"not a real segment").expect("write stray");

    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen ignores orphan");
    let brute = Brute::build(&queries);
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for t in &titles {
        let want = brute.matches(t, &mut lc, &mut feats);
        let got: HashSet<u64> = reopened
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, want, "orphan-present reopen != brute {t:?}");
    }
    // A checkpoint diff-GCs the orphan (it is not in the registry).
    reopened.checkpoint().expect("checkpoint");
    assert!(!stray.exists(), "orphan .seg was not GC'd by checkpoint");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A missing / CRC-corrupt committed segment must fail `open` LOUD — never silently drop
/// that shard's matches (a shard-sized false negative). This is the deliberate divergence
/// from `Engine::open`'s skip-and-degrade, matching the fail-loud posture of the
/// dict-fingerprint and manifest-CRC guards.
#[test]
fn corrupt_committed_segment_fails_loud_on_open() {
    let (queries, _) = build_corpus();
    let dir = unique_dir("segcorrupt");
    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
            .expect("durable cluster builds");
        cluster.checkpoint().expect("checkpoint");
    }
    // Flip a byte inside a committed segment — the segment's trailing CRC must catch it.
    let manifest = read_cluster_manifest(&dir.join("cluster_manifest.bin")).expect("manifest");
    let (sidx, fname) = manifest
        .segment_registry
        .iter()
        .enumerate()
        .find_map(|(i, files)| files.first().map(|f| (i, f.clone())))
        .expect("at least one committed segment");
    let seg_path = dir
        .join(format!("shard_{sidx:03}"))
        .join("segments")
        .join(&fname);
    let mut bytes = std::fs::read(&seg_path).expect("read segment");
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&seg_path, &bytes).expect("corrupt segment");

    assert!(
        ClusterEngine::open(dir.clone(), vocab(), None).is_err(),
        "a corrupt committed segment must fail open loud, not silently drop matches"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
