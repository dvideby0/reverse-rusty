//! Shared harness for the cluster scale soak: env-tunable sizing, the sentinel
//! corpus, and small percentile / dir-size helpers.
//!
//! Submodules reach these via `use crate::harness::*;` — the `pub(crate) use`
//! re-exports carry the engine types the test body needs.

pub(crate) use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
pub(crate) use reverse_rusty::config::EngineConfig;
pub(crate) use reverse_rusty::gen::{generate, GenConfig};
pub(crate) use reverse_rusty::normalize::Normalizer;
pub(crate) use reverse_rusty::segment::{Engine, MatchScratch};
pub(crate) use std::collections::HashSet;
pub(crate) use std::path::PathBuf;
pub(crate) use std::time::Instant;

/// Sentinel query ids sit far above the generated id space (0..num_queries)
/// and the live-add id space below.
pub(crate) const SENTINEL_ID_BASE: u64 = 600_000_000;
pub(crate) const LIVE_ADD_ID_BASE: u64 = 700_000_000;

pub(crate) fn make_norm() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Sizing for one soak run. Defaults are the canonical 20M / 50k / K=8
/// acceptance run (ADR-104); every knob is env-overridable so the harness can
/// be smoked at small scale (and rerun anywhere) without touching code.
pub(crate) struct SoakConfig {
    pub num_queries: usize,
    pub num_titles: usize,
    pub num_shards: usize,
    /// Fresh per-run directory for the durable cluster. Removed on success;
    /// left in place on failure so the artifacts can be inspected.
    pub data_dir: PathBuf,
}

impl SoakConfig {
    pub(crate) fn from_env() -> Self {
        let base = std::env::var("RR_CLUSTER_SOAK_DIR")
            .map_or_else(|_| std::env::temp_dir(), PathBuf::from);
        SoakConfig {
            num_queries: env_usize("RR_CLUSTER_SOAK_QUERIES", 20_000_000),
            num_titles: env_usize("RR_CLUSTER_SOAK_TITLES", 50_000),
            num_shards: env_usize("RR_CLUSTER_SOAK_SHARDS", 8),
            data_dir: base.join(format!("rr_cluster_soak_{}", std::process::id())),
        }
    }

    /// Absolute-FN sentinels planted in the build corpus (phase 0/5).
    pub(crate) fn num_sentinels(&self) -> usize {
        (self.num_queries / 10_000).max(200)
    }

    /// Live `add_query` count for the mutation phase (frozen-dict synthetic-ID path).
    pub(crate) fn num_adds(&self) -> usize {
        self.num_queries / 200
    }

    /// Live `upsert_query` count (taken from `queries[num_deletes()..]`).
    pub(crate) fn num_upserts(&self) -> usize {
        self.num_queries / 1_000
    }

    /// Live `remove_query` count (taken from `queries[..num_deletes()]`).
    pub(crate) fn num_deletes(&self) -> usize {
        self.num_queries / 100
    }
}

/// Sentinel pair `i`: a single rare required term, so the query is class A and
/// the title retrieves it iff the signature cover is lossless end-to-end.
/// Tokens are plain alphanumerics disjoint from the generator vocabulary so the
/// containment assertions are exact.
pub(crate) fn sentinel_query(i: usize) -> String {
    format!("sentineltok{i:06}")
}

pub(crate) fn sentinel_title(i: usize) -> String {
    format!("sentineltok{i:06} listing")
}

/// Live-add pair `i`: the token is absent from the frozen dict, so the add
/// compiles to a synthetic feature id (ADR-046) — the title must still
/// retrieve it.
pub(crate) fn live_add_query(i: usize) -> String {
    format!("liveq{i:06}tok")
}

pub(crate) fn live_add_title(i: usize) -> String {
    format!("liveq{i:06}tok item")
}

/// Percentile over a sorted slice (nearest-rank, the clusterbench convention).
pub(crate) fn pct(sorted: &[u32], p: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

/// Recursive on-disk size of a directory, for the capture log.
pub(crate) fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += dir_size_bytes(&p);
            } else if let Ok(md) = entry.metadata() {
                total += md.len();
            }
        }
    }
    total
}

pub(crate) fn fmt_mb(bytes: u64) -> String {
    format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
}
