//! Cluster durability oracle — the CONTRACT verification for the externalized
//! coordinator log (ADR-031, clustering build-path step 3a).
//!
//! A `ClusterEngine` built with a `data_dir` must be rebuildable from its manifest +
//! base snapshot + mutation log alone. After a crash (drop without clean shutdown),
//! `ClusterEngine::open` must reconstruct a cluster that returns EXACTLY what the
//! pre-crash cluster returned AND exactly the independent brute-force oracle's set —
//! across shard counts {1, 3, 8}, broad on/off, live add/remove churn, and a checkpoint.
//!
//! The `Brute`, `vocab`, and `build_corpus` helpers are copied from
//! `tests/cluster_oracle.rs` (the same deliberate "shares nothing with the engine"
//! oracle), so a compile/index/exact bug cannot hide by being present on both sides.

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError};
use reverse_rusty::compile::extract;
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig, BRANDS};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::storage::read_cluster_manifest;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

/// Independent ground-truth matcher (copied from `tests/cluster_oracle.rs` — shares
/// nothing with the engine or cluster).
struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, reverse_rusty::compile::Extracted)>,
}

impl Brute {
    fn build(queries: &[(u64, String)]) -> Self {
        Self::build_with_vocab(queries, vocab())
    }

    /// Build the oracle with an EXPLICIT normalizer (e.g. one carrying a declared
    /// alias) so it independently applies the same alias the cluster was given.
    fn build_with_vocab(queries: &[(u64, String)], norm: Normalizer) -> Self {
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue; // mirror class-D rejection
                }
                qs.push((*logical, ex));
            }
        }
        dict.finalize_mask();
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    fn matches(&self, title: &str, lc: &mut String, feats: &mut Vec<u32>) -> HashSet<u64> {
        self.norm.match_features(title, &self.dict, lc, feats);
        let present = |f: u32| feats.binary_search(&f).is_ok();
        let mut out = HashSet::new();
        for (logical, ex) in &self.queries {
            if ex.required.iter().all(|&f| present(f))
                && !ex.forbidden.iter().any(|&f| present(f))
                && ex.anyof.iter().all(|g| g.iter().any(|&f| present(f)))
            {
                out.insert(*logical);
            }
        }
        out
    }
}

/// Build the test corpus (copied from `tests/cluster_oracle.rs`): a generated base
/// (class A + C) plus injected class-B any-of, class-B arity-2, and class-A coverage,
/// plus multi-entity titles. Returns `(queries, titles)`.
fn build_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 12_000,
        num_titles: 1_200,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0CEA_5ADE,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);
    let mut queries = data.queries;
    let mut titles = data.titles;
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;

    for i in 0..150u64 {
        queries.push((next_id, format!("(rareplayer{i},rareplayer{})", i + 1000)));
        next_id += 1;
    }
    for i in 0..100u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand}")));
        next_id += 1;
    }
    for i in 0..150u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand} rareplayer{i}")));
        next_id += 1;
    }
    for i in 0..200u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        let a = i % 150;
        titles.push(format!(
            "{year} {brand} rareplayer{a} rareplayer{} psa 10",
            a + 1000
        ));
    }

    (queries, titles)
}

static NEXT: AtomicU32 = AtomicU32::new(0);

fn unique_dir(tag: &str) -> PathBuf {
    let n = NEXT.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("rr_cluster_dur_{tag}_{}_{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn durable_cfg(num_shards: usize, dir: PathBuf, fsync: bool) -> ClusterConfig {
    ClusterConfig {
        num_shards,
        data_dir: Some(dir),
        wal_sync_on_write: fsync,
        ..Default::default()
    }
}

/// A fixed live-mutation churn derived from the corpus: re-add three existing query
/// shapes (any-of, class A, arity-2/broad) under fresh ids, then remove some originals
/// plus one just-added query (exercising add-then-remove). Returns `(added, removed)`.
fn churn(queries: &[(u64, String)]) -> (Vec<(u64, String)>, Vec<u64>) {
    let base = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let mut added = Vec::new();
    if let Some((_, t)) = queries.iter().find(|(_, t)| t.starts_with('(')) {
        added.push((base, t.clone())); // any-of (class B)
    }
    if let Some((_, t)) = queries
        .iter()
        .find(|(_, t)| t.contains("rareplayer") && !t.starts_with('('))
    {
        added.push((base + 1, t.clone())); // class A
    }
    if let Some((_, t)) = queries
        .iter()
        .find(|(_, t)| !t.contains("rareplayer") && !t.starts_with('('))
    {
        added.push((base + 2, t.clone())); // arity-2 / broad (replicated lane)
    }
    // Remove a handful of originals plus the just-added class-A query (add-then-remove).
    let mut removed: Vec<u64> = queries.iter().take(5).map(|(id, _)| *id).collect();
    removed.push(base + 1);
    (added, removed)
}

/// Apply the churn to a cluster (panicking on any error — these adds/removes are valid).
fn apply_churn(cluster: &ClusterEngine, added: &[(u64, String)], removed: &[u64]) {
    for (id, dsl) in added {
        cluster.add_query(*id, dsl).expect("add_query");
    }
    for id in removed {
        cluster.remove_query(*id).expect("remove_query");
    }
}

/// The final live query set after the churn — the input to the brute oracle.
fn final_live(
    queries: &[(u64, String)],
    added: &[(u64, String)],
    removed: &[u64],
) -> Vec<(u64, String)> {
    let dead: HashSet<u64> = removed.iter().copied().collect();
    let mut out: Vec<(u64, String)> = queries
        .iter()
        .filter(|(id, _)| !dead.contains(id))
        .cloned()
        .collect();
    for (id, dsl) in added {
        if !dead.contains(id) {
            out.push((*id, dsl.clone()));
        }
    }
    out
}

// ---- headline: rebuild-from-log ≡ pre-crash ≡ brute, across shards and broad ----

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

// ---- checkpoint compacts the log; reopen still equals the oracle ----

#[test]
fn checkpoint_then_reopen_matches_oracle() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("checkpoint");

    let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable cluster builds");
    assert_eq!(cluster.epoch(), 0);
    apply_churn(&cluster, &added, &removed);

    let log_path = dir.join("cluster.log");
    let log_before = std::fs::metadata(&log_path).expect("log").len();
    cluster.checkpoint().expect("checkpoint");
    assert_eq!(cluster.epoch(), 1, "checkpoint bumps the epoch");
    let log_after = std::fs::metadata(&log_path).expect("log").len();
    assert!(
        log_after < log_before,
        "checkpoint truncated the log ({log_before} -> {log_after})"
    );

    // More churn after the checkpoint (lives only in the post-checkpoint log tail).
    let post_id = added.iter().map(|(id, _)| *id).max().unwrap_or(0) + 100;
    let post_dsl = queries
        .iter()
        .find(|(_, t)| t.contains("rareplayer") && !t.starts_with('('))
        .map(|(_, t)| t.clone())
        .expect("a class-A query");
    cluster.add_query(post_id, &post_dsl).expect("post add");
    drop(cluster);

    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert_eq!(reopened.epoch(), 1, "epoch persists across reopen");

    let mut live = final_live(&queries, &added, &removed);
    live.push((post_id, post_dsl));
    let brute = Brute::build(&live);
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for t in &titles {
        let want = brute.matches(t, &mut lc, &mut feats);
        let got: HashSet<u64> = reopened
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, want, "checkpoint reopen {t:?}");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---- a torn log tail drops only the torn record; acknowledged writes survive ----

#[test]
fn torn_log_tail_recovers_acknowledged_writes() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("torn");

    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), true), &queries)
            .expect("durable cluster builds");
        apply_churn(&cluster, &added, &removed);
    }
    // Corrupt the tail: append junk that cannot frame a valid record.
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.join("cluster.log"))
            .expect("open log");
        f.write_all(&[0xFF, 0xFF, 0xFF, 0x7F, 0x01, 0x02, 0x03])
            .expect("corrupt");
    }

    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
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
        assert_eq!(got, want, "torn-tail reopen {t:?}");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

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

// ---- fsync policy is invisible to recovery ----

#[test]
fn fsync_policy_does_not_change_recovery() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    for &fsync in &[false, true] {
        let dir = unique_dir(&format!("fsync_{fsync}"));
        {
            let cluster =
                ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), fsync), &queries)
                    .expect("durable cluster builds");
            apply_churn(&cluster, &added, &removed);
        }
        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
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
            assert_eq!(got, want, "fsync={fsync} {t:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
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

// ---- step 3b: attach-and-mmap (segments are the base), not re-ingest ----

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

// ---- ADR-035: per-shard replication survives reopen (durable replication_factor > 1) ----

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

// ---- ADR-046 mechanism (2): a runtime alias survives a crash + reopen ----

/// The declared alias `zzabbr ≡ zzcanon`, as a Vocab (rebuilt per use).
fn alias_vocab(token: &str, canonical: &str) -> reverse_rusty::vocab::Vocab {
    let mut v = reverse_rusty::vocab::Vocab::new();
    v.add_synonym(token, canonical, reverse_rusty::dict::FeatureKind::Generic);
    v
}

#[test]
fn declared_alias_survives_reopen() {
    // `set_vocab` on a DURABLE cluster rebuilds + checkpoints: the new manifest carries the
    // re-minted dict + the serialized vocab. After a crash + reopen the alias is still in
    // effect — both surface forms match, and reopened ≡ pre-crash ≡ an independent
    // alias-aware oracle. Zero false negatives across the restart.
    let (mut queries, titles) = build_corpus();
    let q_abbr = 8_100_001u64;
    let q_canon = 8_100_002u64;
    queries.push((q_abbr, "1994 fleer zzabbr".into()));
    queries.push((q_canon, "1994 fleer zzcanon".into()));
    let title_abbr = "1994 fleer zzabbr psa 10";
    let title_canon = "1994 fleer zzcanon psa 10";
    // Titles snapshotted + compared across the restart: the alias forms + a corpus sample.
    let check: Vec<String> = [title_abbr.to_string(), title_canon.to_string()]
        .into_iter()
        .chain(titles.iter().take(80).cloned())
        .collect();

    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("alias_k{k}"));

        // Build durable, declare the alias (rebuild + checkpoint), snapshot, then "crash".
        let pre_crash: Vec<HashSet<u64>> = {
            let mut cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k, dir.clone(), false), &queries)
                    .expect("durable cluster builds");
            cluster
                .set_vocab(alias_vocab("zzabbr", "term:zzcanon"))
                .expect("set_vocab");
            for t in [title_abbr, title_canon] {
                let got = cluster.percolate(t).expect("percolate");
                assert!(
                    got.contains(&q_abbr) && got.contains(&q_canon),
                    "k={k}: pre-crash both forms must match {t:?}"
                );
            }
            check
                .iter()
                .map(|t| {
                    cluster
                        .percolate(t)
                        .expect("percolate")
                        .into_iter()
                        .collect()
                })
                .collect()
        };

        // Reopen from disk alone — `open` restores the alias normalizer from the manifest's
        // persisted vocab (the passed `vocab()` is overridden by it).
        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");

        // Independent alias-aware oracle over the live set.
        let brute = Brute::build_with_vocab(
            &queries,
            alias_vocab("zzabbr", "term:zzcanon")
                .to_normalizer()
                .unwrap(),
        );
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();
        for (i, t) in check.iter().enumerate() {
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(got, pre_crash[i], "k={k}: reopened≠pre-crash {t:?}");
            let want = brute.matches(t, &mut lc, &mut feats);
            assert_eq!(got, want, "k={k}: reopened≠alias-aware oracle {t:?}");
        }
        // The alias is still in effect after the restart (zero FN).
        for t in [title_abbr, title_canon] {
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert!(
                got.contains(&q_abbr) && got.contains(&q_canon),
                "k={k}: after reopen both forms must still match {t:?}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn declared_alias_rebind_survives_reopen() {
    // A SECOND set_vocab on a durable cluster (re-binding the alias to a different canonical)
    // takes effect and the LATEST binding survives reopen — exercising repeated durable
    // rebuilds, and that live_sources de-dups correctly across the accumulated state.
    let (mut queries, _titles) = build_corpus();
    let qid = 8_200_001u64;
    queries.push((qid, "1994 fleer zzabbr".into()));

    let dir = unique_dir("alias_rebind");
    let title_one = "1994 fleer zzone psa 10";
    let title_two = "1994 fleer zztwo psa 10";

    {
        let mut cluster =
            ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
                .expect("durable cluster builds");
        // First binding: zzabbr → zzone.
        cluster
            .set_vocab(alias_vocab("zzabbr", "term:zzone"))
            .expect("set_vocab 1");
        assert!(
            cluster.percolate(title_one).unwrap().contains(&qid),
            "after the first binding, the zzone title matches"
        );
        assert!(
            !cluster.percolate(title_two).unwrap().contains(&qid),
            "the zztwo title must not match the first binding"
        );
        // Re-bind: zzabbr → zztwo.
        cluster
            .set_vocab(alias_vocab("zzabbr", "term:zztwo"))
            .expect("set_vocab 2");
        assert!(
            cluster.percolate(title_two).unwrap().contains(&qid),
            "after the re-bind, the zztwo title matches"
        );
        assert!(
            !cluster.percolate(title_one).unwrap().contains(&qid),
            "the old binding (zzone) must no longer match"
        );
    }

    // Reopen: the LATEST binding (zzabbr → zztwo) is what persisted.
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert!(
        reopened.percolate(title_two).unwrap().contains(&qid),
        "after reopen, the latest binding (zztwo) is in effect"
    );
    assert!(
        !reopened.percolate(title_one).unwrap().contains(&qid),
        "after reopen, the superseded binding (zzone) is gone"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
