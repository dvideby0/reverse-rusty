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
        let norm = vocab();
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
