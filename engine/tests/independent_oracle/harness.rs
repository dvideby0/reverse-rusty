//! Shared harness: build the engine and the independent reference from one description, then diff.
//!
//! The reference (`reverse-rusty-ref-matcher`) shares no front-end code with the engine; the only
//! thing fed to both sides is the same query DSL strings + the same vocabulary *data* (not logic),
//! which is exactly what makes the diff a check on the engine's front-end implementation.

#![allow(dead_code)] // some helpers are used only by later test modules

use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty_ref_matcher::{RefMatcher, RefVocab};
use std::collections::HashMap;
use std::collections::HashSet;

/// One side-by-side oracle: the real engine and the independent reference, built from the same
/// queries + vocabulary.
pub struct RefOracle {
    eng: Engine,
    reference: RefMatcher,
    /// logical id -> the query's DSL text, for divergence diagnostics.
    dsl: HashMap<u64, String>,
}

impl RefOracle {
    /// Build both sides with the empty default vocabulary (`Normalizer::default_vocab` /
    /// `RefVocab::default_vocab`).
    pub fn build_default(queries: &[(u64, String)]) -> Self {
        let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
        eng.build_from_queries(queries);
        let reference = RefMatcher::build(queries, RefVocab::default_vocab());
        Self::from_parts(eng, reference, queries)
    }

    /// Build both sides from a paired `(engine Normalizer, RefVocab)` description — the caller is
    /// responsible for the two expressing the SAME vocabulary in each side's own type. Used for the
    /// grader / phrase / synonym pass (no equivalence map needed).
    pub fn build_with_normalizer(
        queries: &[(u64, String)],
        norm: Normalizer,
        ref_vocab: RefVocab,
    ) -> Self {
        let mut eng = Engine::new(norm);
        eng.build_from_queries(queries);
        let reference = RefMatcher::build(queries, ref_vocab);
        Self::from_parts(eng, reference, queries)
    }

    /// Build the engine by importing Solr-style alias lines (`ny => new york`) and applying them
    /// live (ADR-060/061 — auto-activates declared multi-word aliases + recompiles), and the
    /// reference from the paired `RefVocab`. The same path the in-tree alias oracle uses.
    pub fn build_with_alias_import(
        queries: &[(u64, String)],
        solr: &str,
        ref_vocab: RefVocab,
    ) -> Self {
        let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
        eng.build_from_queries(queries);
        eng.import_alias_synonyms(solr)
            .expect("import + apply aliases");
        let reference = RefMatcher::build(queries, ref_vocab);
        Self::from_parts(eng, reference, queries)
    }

    fn from_parts(eng: Engine, reference: RefMatcher, queries: &[(u64, String)]) -> Self {
        let dsl = queries.iter().map(|(id, q)| (*id, q.clone())).collect();
        RefOracle {
            eng,
            reference,
            dsl,
        }
    }

    /// The engine's match set for one title.
    fn engine_matches(
        &self,
        title: &str,
        s: &mut MatchScratch,
        out: &mut Vec<u64>,
    ) -> HashSet<u64> {
        self.eng
            .match_title(title, s, out, /* include_broad = */ true);
        out.iter().copied().collect()
    }

    /// Diff every title and assert ZERO false negatives + ZERO false positives, printing the first
    /// few divergences (title + offending query DSL) on failure. `label` names the corpus.
    pub fn assert_matches(&self, titles: &[String], label: &str) {
        let report = self.diff(titles);
        report.assert_clean(label, &self.dsl);
    }

    /// Diff every title, returning the tallies + a bounded sample of divergences.
    pub fn diff(&self, titles: &[String]) -> DiffReport {
        let mut s = MatchScratch::new();
        let mut out = Vec::new();
        let mut report = DiffReport::default();

        for title in titles {
            let engine_set = self.engine_matches(title, &mut s, &mut out);
            let truth = self.reference.matches(title);

            report.total_truth += truth.len();
            report.total_engine += engine_set.len();

            // Exact counts are unbounded (cheap usize); the stored title/query samples are CAPPED —
            // a broad regression can diverge on millions of pairs, and cloning every title would OOM
            // before the counts are ever printed (only the first few samples are shown anyway).
            for &t in &truth {
                if !engine_set.contains(&t) {
                    report.false_neg += 1;
                    if report.sample_fn.len() < SAMPLE_CAP {
                        report.sample_fn.push((title.clone(), t));
                    }
                }
            }
            for &e in &engine_set {
                if !truth.contains(&e) {
                    report.false_pos += 1;
                    if report.sample_fp.len() < SAMPLE_CAP {
                        report.sample_fp.push((title.clone(), e));
                    }
                }
            }
        }
        report
    }
}

/// How many divergent (title, query) pairs to retain for diagnostics (> the 10 printed).
const SAMPLE_CAP: usize = 16;

#[derive(Default)]
pub struct DiffReport {
    pub total_truth: usize,
    pub total_engine: usize,
    pub false_neg: usize,
    pub false_pos: usize,
    sample_fn: Vec<(String, u64)>,
    sample_fp: Vec<(String, u64)>,
}

impl DiffReport {
    /// Assert zero FN / zero FP / non-degenerate, with rich diagnostics on failure.
    pub fn assert_clean(&self, label: &str, dsl: &HashMap<u64, String>) {
        eprintln!(
            "[{label}] truth={} engine={} false_neg={} false_pos={}",
            self.total_truth, self.total_engine, self.false_neg, self.false_pos
        );
        if self.false_neg != 0 {
            eprintln!(
                "--- sample FALSE NEGATIVES (reference matched, engine missed) [{label}] ---"
            );
            for (title, qid) in self.sample_fn.iter().take(10) {
                eprintln!("  q#{qid} {:?}\n    title: {title:?}", dsl.get(qid));
            }
        }
        if self.false_pos != 0 {
            eprintln!(
                "--- sample FALSE POSITIVES (engine matched, reference did not) [{label}] ---"
            );
            for (title, qid) in self.sample_fp.iter().take(10) {
                eprintln!("  q#{qid} {:?}\n    title: {title:?}", dsl.get(qid));
            }
        }
        assert_eq!(
            self.false_neg, 0,
            "[{label}] FALSE NEGATIVES — engine misses a spec match (cardinal sin)"
        );
        assert_eq!(
            self.false_pos, 0,
            "[{label}] false positives — engine matches what the spec forbids"
        );
        assert!(
            self.total_truth > 0,
            "[{label}] degenerate: reference found no matches at all"
        );
    }
}
