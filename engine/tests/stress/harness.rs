//! Shared harness for the stress test suite: workload helpers, the brute-force
//! oracle, the event log/observer, and the metrics printer.
//!
//! Submodules reach these via `use crate::harness::*;`. The `pub(crate) use`
//! re-exports below mean a submodule glob-importing this file also picks up the
//! engine types + std imports the test bodies need.

use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::Dict;
use reverse_rusty::events::{EngineEvent, EngineMetrics};
use reverse_rusty::normalize::Normalizer;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

// Re-exports so `use crate::harness::*;` carries the imports the test bodies use.
pub(crate) use reverse_rusty::config::EngineConfig;
pub(crate) use reverse_rusty::gen::{generate, GenConfig};
pub(crate) use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
pub(crate) use std::collections::HashSet;
pub(crate) use std::sync::atomic::Ordering;
pub(crate) use std::time::Instant;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) fn make_norm() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

pub(crate) fn match_ids(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

pub(crate) fn match_ids_set(engine: &Engine, title: &str) -> HashSet<u64> {
    match_ids(engine, title).into_iter().collect()
}

pub(crate) struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    pub(crate) fn build(queries: &[(u64, String)]) -> Self {
        let norm = make_norm();
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

#[derive(Debug, Default)]
pub(crate) struct EventLog {
    pub(crate) flushes: AtomicUsize,
    pub(crate) ingests: AtomicUsize,
    pub(crate) compactions: AtomicUsize,
    pub(crate) entries: Mutex<Vec<String>>,
}

impl EventLog {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn observer(self: &Arc<Self>) -> impl Fn(&EngineEvent) + Send + Sync + 'static {
        let log = Arc::clone(self);
        move |event: &EngineEvent| {
            let msg = match event {
                EngineEvent::Flush {
                    entries,
                    base_segments_after,
                    ..
                } => {
                    log.flushes.fetch_add(1, Ordering::Relaxed);
                    format!("[FLUSH] entries={entries} segments_after={base_segments_after}")
                }
                EngineEvent::Ingest {
                    ingested,
                    rejected_parse,
                    rejected_class_d,
                    base_segments_after,
                } => {
                    log.ingests.fetch_add(1, Ordering::Relaxed);
                    format!(
                        "[INGEST] ingested={ingested} rejected_parse={rejected_parse} rejected_d={rejected_class_d} segments_after={base_segments_after}"
                    )
                }
                EngineEvent::Compaction {
                    report,
                    trigger,
                    base_segments_after,
                    ..
                } => {
                    log.compactions.fetch_add(1, Ordering::Relaxed);
                    format!(
                        "[COMPACT] merged={} before={} after={} reclaimed={} trigger={:?} segments_after={}",
                        report.segments_merged,
                        report.entries_before,
                        report.entries_after,
                        report.tombstones_reclaimed,
                        trigger,
                        base_segments_after
                    )
                }
                EngineEvent::SegmentCleanupFailed { path, error } => {
                    format!("[CLEANUP_FAIL] path={} error={error}", path.display())
                }
                EngineEvent::DurabilityFailure { op, detail, error } => {
                    format!(
                        "[DURABILITY_FAIL] op={} detail={detail} error={error}",
                        op.as_str()
                    )
                }
            };
            eprintln!("  EVENT: {msg}");
            log.entries.lock().unwrap().push(msg);
        }
    }

    pub(crate) fn dump_summary(&self, label: &str) {
        eprintln!(
            "  {} event summary: flushes={} ingests={} compactions={}",
            label,
            self.flushes.load(Ordering::Relaxed),
            self.ingests.load(Ordering::Relaxed),
            self.compactions.load(Ordering::Relaxed),
        );
    }
}

pub(crate) fn print_metrics(label: &str, m: &EngineMetrics) {
    eprintln!(
        "  [METRICS:{}] total_queries={} base_segments={} memtable={} dict_features={} stale={}",
        label,
        m.total_queries,
        m.base_segments,
        m.memtable_entries,
        m.dict_features,
        m.stale_segments
    );
    if !m.segment_sizes.is_empty() {
        eprintln!(
            "    segment_sizes={:?} holes={:?}",
            m.segment_sizes,
            m.segment_holes
                .iter()
                .map(|h| format!("{:.2}%", h * 100.0))
                .collect::<Vec<_>>()
        );
    }
    eprintln!(
        "    memory: exact={}KB index={}KB filter={}KB",
        m.exact_bytes / 1024,
        m.index_bytes / 1024,
        m.filter_bytes / 1024
    );
}
