//! Shared gRPC-oracle test harness: the brute-force ground truth, the corpus
//! builders, the frozen dict / tag-dict constructors, and the listen/teardown
//! helpers that every `cluster_grpc_oracle` test group relies on. All items are
//! `pub(crate)` so the per-concern modules reach them via `use crate::harness::*;`.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig, BRANDS};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::tagdict::TagDict;

pub(crate) fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

/// Independent ground-truth matcher (same structure as `cluster_oracle.rs::Brute`;
/// deliberately shares nothing with the engine or cluster).
pub(crate) struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    pub(crate) fn build(queries: &[(u64, String)]) -> Self {
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

/// A compact corpus (smaller than `cluster_oracle.rs`'s, since every probe is an RPC)
/// that still exercises class A / B-any-of / B-arity-2 / C and multi-shard fan-out.
pub(crate) fn build_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 4_000,
        num_titles: 300,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x9119_57A1,
        num_players: 900,
        num_sets: 400,
    };
    let data = generate(&cfg);
    let mut queries = data.queries;
    let mut titles = data.titles;
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;

    // class-B any-of: pure any-of of two rare players.
    for i in 0..120u64 {
        queries.push((next_id, format!("(rareplayer{i},rareplayer{})", i + 1000)));
        next_id += 1;
    }
    // class-B arity-2: all-hot required (year + brand) → replicated lane.
    for i in 0..80u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand}")));
        next_id += 1;
    }
    // class-A anchored on injected rare players, so multi-entity titles match.
    for i in 0..120u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand} rareplayer{i}")));
        next_id += 1;
    }
    // multi-entity titles: two rare players → fan out to two selective shards + lane 0.
    for i in 0..120u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        let a = i % 120;
        titles.push(format!(
            "{year} {brand} rareplayer{a} rareplayer{} psa 10",
            a + 1000
        ));
    }

    (queries, titles)
}

/// The brute oracle's match set for every title over a given query list.
pub(crate) fn build_oracle(queries: &[(u64, String)], titles: &[String]) -> Vec<HashSet<u64>> {
    let brute = Brute::build(queries);
    let mut lc = String::new();
    let mut feats = Vec::new();
    titles
        .iter()
        .map(|t| brute.matches(t, &mut lc, &mut feats))
        .collect()
}

/// A finalized, empty tag space (ADR-055) — the untagged-test analogue of the coordinator's frozen
/// tag dict. These gRPC equivalence tests carry no tags, so an empty (but finalized) tag space ships
/// to every server and resolves no filter; the dedicated filtered-percolate test builds a real one.
pub(crate) fn empty_tag_dict() -> Arc<TagDict> {
    let mut td = TagDict::new();
    td.mark_finalized();
    Arc::new(td)
}

// ---- per-query tags + filtered percolation (ADR-049/055), mirroring `tests/oracle.rs` ----
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

/// The coordinator's frozen tag space: every corpus tag interned, then finalized — mirrors what
/// `ClusterEngine::build_with_tags` builds, so the shipped tag dict resolves a stored tag and a
/// request filter to the same `TagId`.
pub(crate) fn frozen_tag_dict_over(tags: &[Vec<(String, String)>]) -> Arc<TagDict> {
    let mut td = TagDict::new();
    for qtags in tags {
        for (k, v) in qtags {
            td.intern(k, v);
        }
    }
    td.mark_finalized();
    Arc::new(td)
}

/// One authoritative frozen dict interned over `queries` (the coordinator's feature space).
pub(crate) fn frozen_dict_over(queries: &[(u64, String)], norm: &Normalizer) -> Arc<Dict> {
    let mut d = Dict::new();
    let mut lc = String::new();
    for (_id, text) in queries {
        if let Ok(ast) = reverse_rusty::dsl::parse(text) {
            let _ = extract(&ast, norm, &mut d, &mut lc);
        }
    }
    d.finalize_mask();
    Arc::new(d)
}

/// Block until `addr` accepts TCP (the gRPC server is listening) or time out.
pub(crate) fn wait_until_listening(addr: SocketAddr) {
    for _ in 0..300 {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("shard server at {addr} never started listening");
}

/// Build a small frozen dict from a fixed base plus `extra` DSL snippets (interned in
/// order against `norm`). Two dicts built with different `extra` have different
/// fingerprints — the divergence the handshake must catch.
pub(crate) fn frozen_dict_with(extra: &[&str], norm: &Normalizer) -> Arc<Dict> {
    let mut d = Dict::new();
    let mut lc = String::new();
    let base = ["1994 upper deck", "psa 10", "topps chrome"];
    for q in base.iter().copied().chain(extra.iter().copied()) {
        if let Ok(ast) = reverse_rusty::dsl::parse(q) {
            let _ = extract(&ast, norm, &mut d, &mut lc);
        }
    }
    d.finalize_mask();
    Arc::new(d)
}

/// Block until `addr` stops accepting TCP (the server has gone) or time out.
pub(crate) fn wait_until_not_listening(addr: SocketAddr) {
    for _ in 0..300 {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server at {addr} never stopped listening");
}

/// A unique, freshly-cleaned data dir for one durable shard server.
pub(crate) fn server_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rr_grpc_rep_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
