//! Shared harness for the cluster durability oracle (ADR-031/032).
//!
//! Durable-cluster builders, crash/reopen drivers, the rebuilt ≡ pre-crash ≡ brute
//! differential helpers, corpus generators, and the per-query-tag / filtered-percolation
//! helpers — all `pub(crate)` so the per-concern test modules can reach them via
//! `use crate::harness::*;`.
//!
//! The `Brute`, `vocab`, and `build_corpus` helpers are copied from
//! `tests/cluster_oracle.rs` (the same deliberate "shares nothing with the engine"
//! oracle), so a compile/index/exact bug cannot hide by being present on both sides.

pub(crate) use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError};
use reverse_rusty::compile::extract;
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig, BRANDS};
use reverse_rusty::normalize::Normalizer;
pub(crate) use reverse_rusty::storage::read_cluster_manifest;
pub(crate) use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

/// Independent ground-truth matcher (copied from `tests/cluster_oracle.rs` — shares
/// nothing with the engine or cluster).
pub(crate) struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, reverse_rusty::compile::Extracted)>,
}

impl Brute {
    pub(crate) fn build(queries: &[(u64, String)]) -> Self {
        Self::build_with_vocab(queries, vocab())
    }

    /// Build the oracle with an EXPLICIT normalizer (e.g. one carrying a declared
    /// alias) so it independently applies the same alias the cluster was given.
    pub(crate) fn build_with_vocab(queries: &[(u64, String)], norm: Normalizer) -> Self {
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

    /// Like `build` but KEEPS negation-only (class-D) queries as always-candidates — the
    /// ground truth for the durable cluster always-candidate lane (ADR-068/080). Only the
    /// effectively-empty query (no positives AND no negatives) is dropped.
    pub(crate) fn build_accepting_class_d(queries: &[(u64, String)]) -> Self {
        let norm = vocab();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() && ex.forbidden.is_empty() {
                    continue;
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

    /// Like `build_with_vocab` but also applies the vocab's equivalence groups (ADR-054) via
    /// expansion — independent ground truth that widens each query exactly as the cluster does.
    pub(crate) fn build_with_equiv(
        queries: &[(u64, String)],
        norm: Normalizer,
        vocab: &reverse_rusty::vocab::Vocab,
    ) -> Self {
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue;
                }
                qs.push((*logical, ex));
            }
        }
        dict.finalize_mask();
        let equiv = vocab.resolve_equivalences(&norm, &dict);
        for (_, ex) in &mut qs {
            ex.expand_equivalences(&equiv);
        }
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    pub(crate) fn matches(
        &self,
        title: &str,
        lc: &mut String,
        feats: &mut Vec<u32>,
    ) -> HashSet<u64> {
        let mut sc = reverse_rusty::normalize::NormScratch::new();
        self.norm
            .match_features(title, &self.dict, lc, &mut sc, feats);
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
pub(crate) fn build_corpus() -> (Vec<(u64, String)>, Vec<String>) {
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

// ---- per-query tags + filtered percolation (ADR-049/055), copied from `tests/cluster_oracle.rs` ----
pub(crate) const CATEGORIES: [&str; 6] = ["cards", "coins", "stamps", "comics", "toys", "art"];
pub(crate) const STATUSES: [&str; 3] = ["active", "inactive", "archived"];

pub(crate) fn tags_for(logical: u64) -> Vec<(String, String)> {
    let cat = CATEGORIES[(logical % CATEGORIES.len() as u64) as usize];
    let status = STATUSES[((logical / 7) % STATUSES.len() as u64) as usize];
    vec![
        ("category".to_string(), cat.to_string()),
        ("status".to_string(), status.to_string()),
    ]
}

pub(crate) fn tags_parallel(queries: &[(u64, String)]) -> Vec<Vec<(String, String)>> {
    queries.iter().map(|(l, _)| tags_for(*l)).collect()
}

pub(crate) fn passes_filter(qtags: &[(String, String)], filter: &[(String, Vec<String>)]) -> bool {
    filter.iter().all(|(k, vals)| {
        qtags
            .iter()
            .any(|(qk, qv)| qk == k && vals.iter().any(|v| v == qv))
    })
}

pub(crate) fn filters_for(i: usize) -> Vec<Vec<(String, Vec<String>)>> {
    let c1 = CATEGORIES[i % CATEGORIES.len()].to_string();
    let c2 = CATEGORIES[(i + 1) % CATEGORIES.len()].to_string();
    let st = STATUSES[i % STATUSES.len()].to_string();
    vec![
        vec![("category".to_string(), vec![c1.clone()])],
        vec![("category".to_string(), vec![c1.clone(), c2])],
        vec![
            ("category".to_string(), vec![c1]),
            ("status".to_string(), vec![st]),
        ],
        vec![("category".to_string(), vec!["never-ingested".to_string()])],
    ]
}

static NEXT: AtomicU32 = AtomicU32::new(0);

pub(crate) fn unique_dir(tag: &str) -> PathBuf {
    let n = NEXT.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("rr_cluster_dur_{tag}_{}_{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

pub(crate) fn durable_cfg(num_shards: usize, dir: PathBuf, fsync: bool) -> ClusterConfig {
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
pub(crate) fn churn(queries: &[(u64, String)]) -> (Vec<(u64, String)>, Vec<u64>) {
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
pub(crate) fn apply_churn(cluster: &ClusterEngine, added: &[(u64, String)], removed: &[u64]) {
    for (id, dsl) in added {
        cluster.add_query(*id, dsl).expect("add_query");
    }
    for id in removed {
        cluster.remove_query(*id).expect("remove_query");
    }
}

/// The final live query set after the churn — the input to the brute oracle.
pub(crate) fn final_live(
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
