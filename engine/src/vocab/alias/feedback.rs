//! Match-feedback alias validation (ADR-103) — the aggregation core behind the opt-in
//! title→query feedback loop. Std-only data structures + math; the *instance* lives in the
//! server's `AppState` (single-node v1), fed post-match by the percolate handlers when
//! `alias_feedback_capture` is on, and read by the feedback report endpoint.
//!
//! Validation-of-candidates, not open-ended discovery: only registry `Candidate` pairs are
//! tracked (bounded by `alias_feedback_max_pairs`), each accumulating bounded behavioral
//! evidence — how many titles said form A vs form B, and a bottom-k sketch of WHICH queries
//! each side satisfied. High Jaccard overlap of the (degenerate-filtered) matched-query
//! populations is the research doc's technique-2 signal: titles that say `ud` and titles that
//! say `upper deck` satisfying the *same* queries is behavioral evidence of equivalence.

use serde::{Deserialize, Serialize};

use crate::corpus::tokenize;
use crate::vocab::{AliasRegistry, AliasStatus};

/// Behavioral evidence stamped onto a validated registry entry (ADR-103). A new
/// `serde(default)` optional field on `AliasEntry` — old vocab JSON deserializes to `None`
/// (the `number_context` field-addition precedent).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FeedbackEvidence {
    /// Estimated Jaccard overlap of the two forms' matched-query populations.
    pub overlap: f64,
    /// Titles that contained ONLY this pair's first (canonical-order) form.
    pub titles_a: u64,
    /// Titles that contained ONLY the second form.
    pub titles_b: u64,
    /// Surviving sampled queries (min of the two sides) the overlap was estimated from.
    pub queries_sampled: u64,
}

/// splitmix64 — the sketch hash: cheap, well-mixed, and deliberately NOT the engine's FNV-1a
/// (id assignment patterns in FNV's low bits would bias the bottom-k sample).
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Jaccard sample size — a const, not a knob: 256 gives an estimator with stderr ≈ 1/√256
/// ≈ 0.06 (plenty against a 0.5 threshold).
const SKETCH_K: usize = 256;

/// Raw sketch capacity: 4× oversampled so the report-time degenerate-evidence exclusion
/// filters BEFORE the sample truncates (codex review). Filtering an already-`SKETCH_K`-capped
/// sketch would let heavily form-referencing populations starve the survivors below
/// `min_queries` despite plenty of clean evidence just outside the cap; oversampling keeps the
/// filtered bottom-k exact up to 75% exclusion, and beyond that the survivors are still a
/// valid (smaller) bottom-k sample of the filtered population. ~16 KiB per side ⇒ ~8 MiB at
/// the default pair cap.
const SKETCH_RAW: usize = SKETCH_K * 4;

/// A bottom-k sketch over query ids: the K smallest `splitmix64(id)` values, deduped, sorted.
/// Order-independent by construction — the same id set yields the same sketch under any
/// request interleaving — exact below K, fixed memory at K.
#[derive(Debug, Clone, Default)]
pub(crate) struct BottomK {
    /// Sorted ascending by hash; ≤ `SKETCH_K` entries. Each carries the raw id so the report
    /// can resolve sampled ids back to query sources for the degenerate-evidence exclusion.
    entries: Vec<(u64, u64)>,
}

impl BottomK {
    pub(crate) fn insert(&mut self, id: u64) {
        let h = splitmix64(id);
        // Err(pos) ≥ the raw capacity means the hash sits above the current k-th smallest
        // (not sampled); Ok means the id is already sampled — both no-ops.
        if let Err(pos) = self.entries.binary_search(&(h, id)) {
            if pos < SKETCH_RAW {
                self.entries.insert(pos, (h, id));
                self.entries.truncate(SKETCH_RAW);
            }
        }
    }

    pub(crate) fn members(&self) -> &[(u64, u64)] {
        &self.entries
    }
}

/// Estimated Jaccard between two id populations from their (possibly filtered) bottom-k
/// member lists: a sorted-merge walks the union's smallest `SKETCH_K` hashes and counts how
/// many are present in both. Exact when both populations fit their sketches; zero-sample
/// sides estimate 0.0 (never NaN).
pub(crate) fn jaccard(left: &[(u64, u64)], right: &[(u64, u64)]) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let (mut li, mut ri, mut taken, mut shared) = (0usize, 0usize, 0usize, 0usize);
    while taken < SKETCH_K && (li < left.len() || ri < right.len()) {
        match (left.get(li), right.get(ri)) {
            (Some(&lv), Some(&rv)) => match lv.cmp(&rv) {
                std::cmp::Ordering::Equal => {
                    shared += 1;
                    li += 1;
                    ri += 1;
                }
                std::cmp::Ordering::Less => li += 1,
                std::cmp::Ordering::Greater => ri += 1,
            },
            (Some(_), None) => li += 1,
            (None, Some(_)) => ri += 1,
            (None, None) => break,
        }
        taken += 1;
    }
    if taken == 0 {
        0.0
    } else {
        shared as f64 / taken as f64
    }
}

/// True iff `needle` occurs as a CONTIGUOUS token run in `haystack` — token-level equality,
/// never substring (`ud` never matches inside `stud`); a multi-word form is an adjacent run.
pub(crate) fn contains_run(haystack: &[String], needle: &[String]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|w| w.iter().zip(needle).all(|(h, n)| h == n))
}

/// One tracked candidate pair's accumulating evidence.
#[derive(Debug, Clone)]
struct TrackedPair {
    /// The registry entry's canonical (sorted) forms — exactly 2 in v1.
    forms: [String; 2],
    /// Each form tokenized once at sync time (multi-word forms = a token run).
    tokens: [Vec<String>; 2],
    titles: [u64; 2],
    titles_both: u64,
    sketches: [BottomK; 2],
}

impl TrackedPair {
    fn new(forms: [String; 2]) -> Self {
        let tokens = [tokenize(&forms[0]), tokenize(&forms[1])];
        Self {
            forms,
            tokens,
            titles: [0, 0],
            titles_both: 0,
            sketches: [BottomK::default(), BottomK::default()],
        }
    }
}

/// One pair's rendered feedback report row.
#[derive(Debug, Clone, Serialize)]
pub struct PairFeedback {
    pub forms: Vec<String>,
    pub titles_a: u64,
    pub titles_b: u64,
    /// Titles containing BOTH forms — excluded from evidence (no discriminating signal).
    pub titles_both: u64,
    /// Sampled matched queries per side that SURVIVED the degenerate-evidence exclusion.
    pub sampled_a: u64,
    pub sampled_b: u64,
    /// Sampled queries dropped because their own text references either form.
    pub excluded: u64,
    pub overlap: f64,
    pub validated: bool,
}

/// The aggregator (ADR-103): tracked candidate pairs + their bounded evidence. Lives behind a
/// `Mutex` in the server state; every method is cheap (the capture path is O(tracked pairs ×
/// title tokens) with no allocation beyond sketch inserts).
#[derive(Debug, Default)]
pub struct AliasFeedback {
    pairs: Vec<TrackedPair>,
}

impl AliasFeedback {
    /// Re-derive the tracked universe from the registry: `Candidate` entries with exactly two
    /// forms, ordered (confidence desc, forms asc) and capped at `max_pairs` — deterministic.
    /// Evidence for pairs still tracked is retained (keyed by canonical forms); pairs whose
    /// status changed (activated / rejected) or that fell past the cap are dropped. Called on
    /// every snapshot publish — the vocab epoch is NOT a sufficient dirty signal, because the
    /// ADR-102 metadata-only install records candidates without bumping it.
    pub fn sync_tracked(&mut self, registry: &AliasRegistry, max_pairs: usize) {
        let mut wanted: Vec<(&[String], f64)> = registry
            .entries()
            .iter()
            .filter(|e| e.status == AliasStatus::Candidate && e.forms.len() == 2)
            .map(|e| (e.forms.as_slice(), e.confidence))
            .collect();
        wanted.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(b.0))
        });
        wanted.truncate(max_pairs);

        let mut next: Vec<TrackedPair> = Vec::with_capacity(wanted.len());
        for (forms, _) in wanted {
            let key = [forms[0].clone(), forms[1].clone()];
            if let Some(pos) = self.pairs.iter().position(|p| p.forms == key) {
                next.push(self.pairs.swap_remove(pos)); // retain accumulated evidence
            } else {
                next.push(TrackedPair::new(key));
            }
        }
        self.pairs = next;
    }

    /// Record one percolate outcome: classify the title against every tracked pair by
    /// contiguous token run, and file the matched query ids into the side the title exhibited.
    /// A title containing BOTH forms carries no discriminating signal — counted and excluded.
    pub fn observe(&mut self, title_tokens: &[String], matched: &[u64]) {
        for p in &mut self.pairs {
            let has = [
                contains_run(title_tokens, &p.tokens[0]),
                contains_run(title_tokens, &p.tokens[1]),
            ];
            match (has[0], has[1]) {
                (true, true) => p.titles_both += 1,
                (true, false) => {
                    p.titles[0] += 1;
                    for &id in matched {
                        p.sketches[0].insert(id);
                    }
                }
                (false, true) => {
                    p.titles[1] += 1;
                    for &id in matched {
                        p.sketches[1].insert(id);
                    }
                }
                (false, false) => {}
            }
        }
    }

    /// Wipe accumulated evidence (an explicit window boundary); tracked pairs re-derive on the
    /// next sync.
    pub fn reset(&mut self) {
        self.pairs.clear();
    }

    pub fn tracked_pairs(&self) -> usize {
        self.pairs.len()
    }

    /// Render the per-pair report. `lookup` resolves a sampled query id to its source text —
    /// the **degenerate-evidence exclusion** (the correctness core, ADR-103): a sampled query
    /// whose OWN text references either form is dropped before the overlap is computed. Why: a
    /// query *requiring* `ud` matches `ud`-titles and structurally cannot match `upper deck`
    /// titles pre-activation (mechanically depressing a true alias's overlap), while a query
    /// already bridging the pair through an active equivalence inflates it — the exclusion
    /// removes both distortions. Filtering preserves the bottom-k sample property (the k
    /// smallest hashes surviving a filter are a bottom-k sample of the filtered population).
    /// An unresolvable id (compacted away / source not retained) is excluded — conservative.
    pub fn report(
        &self,
        min_overlap: f64,
        min_titles: u64,
        min_queries: u64,
        mut lookup: impl FnMut(u64) -> Option<String>,
    ) -> Vec<PairFeedback> {
        self.pairs
            .iter()
            .map(|p| {
                let mut excluded = 0u64;
                let mut survivors: [Vec<(u64, u64)>; 2] = [Vec::new(), Vec::new()];
                for (sketch, surviving) in p.sketches.iter().zip(survivors.iter_mut()) {
                    for &(h, id) in sketch.members() {
                        let keep = match lookup(id) {
                            Some(text) => {
                                let toks = tokenize(&text);
                                !contains_run(&toks, &p.tokens[0])
                                    && !contains_run(&toks, &p.tokens[1])
                            }
                            None => false,
                        };
                        if keep {
                            // The filtered walk stays a bottom-k sample: members arrive
                            // hash-ascending, so the first SKETCH_K survivors are exactly the
                            // filtered population's bottom-k.
                            if surviving.len() < SKETCH_K {
                                surviving.push((h, id));
                            }
                        } else {
                            excluded += 1;
                        }
                    }
                }
                let overlap = jaccard(&survivors[0], &survivors[1]);
                let sampled_a = survivors[0].len() as u64;
                let sampled_b = survivors[1].len() as u64;
                let validated = overlap >= min_overlap
                    && p.titles[0] >= min_titles
                    && p.titles[1] >= min_titles
                    && sampled_a >= min_queries
                    && sampled_b >= min_queries;
                PairFeedback {
                    forms: p.forms.to_vec(),
                    titles_a: p.titles[0],
                    titles_b: p.titles[1],
                    titles_both: p.titles_both,
                    sampled_a,
                    sampled_b,
                    excluded,
                    overlap,
                    validated,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| (*x).to_string()).collect()
    }

    #[test]
    fn bottom_k_is_exact_below_k_bounded_and_permutation_invariant() {
        let mut a = BottomK::default();
        let mut b = BottomK::default();
        let ids: Vec<u64> = (0..3000).collect();
        for &id in &ids {
            a.insert(id);
        }
        for &id in ids.iter().rev() {
            b.insert(id);
            b.insert(id); // dedup: double insert changes nothing
        }
        assert_eq!(a.members(), b.members(), "order-independent");
        assert_eq!(
            a.members().len(),
            SKETCH_RAW,
            "bounded at the oversampled capacity"
        );

        let mut small = BottomK::default();
        for id in 0..10u64 {
            small.insert(id);
        }
        assert_eq!(small.members().len(), 10, "exact below K");
    }

    #[test]
    fn jaccard_estimates_identity_disjoint_and_partial() {
        let (mut a, mut b, mut half) = (BottomK::default(), BottomK::default(), BottomK::default());
        for id in 0..1000u64 {
            a.insert(id);
            b.insert(id + 10_000); // disjoint
            half.insert(if id % 2 == 0 { id } else { id + 10_000 }); // 50% shared with a
        }
        assert!((jaccard(a.members(), a.members()) - 1.0).abs() < 1e-9);
        assert!(jaccard(a.members(), b.members()).abs() < 1e-12);
        assert!(
            jaccard(a.members(), &[]).abs() < 1e-12,
            "zero-sample side is 0.0, never NaN"
        );
        // |A∩H| = 500, |A∪H| = 1500 ⇒ J = 1/3; the estimator should land near it.
        let est = jaccard(a.members(), half.members());
        assert!((est - 1.0 / 3.0).abs() < 0.1, "estimate {est} ≉ 1/3");
    }

    #[test]
    fn contains_run_is_token_exact_and_adjacent() {
        let hay = s(&["1994", "stud", "upper", "deck", "psa"]);
        assert!(contains_run(&hay, &s(&["upper", "deck"])));
        assert!(
            !contains_run(&hay, &s(&["ud"])),
            "ud ⊄ stud — token equality, never substring"
        );
        assert!(
            !contains_run(&hay, &s(&["upper", "psa"])),
            "must be adjacent"
        );
        assert!(!contains_run(&hay, &[]));
    }

    fn registry_with_candidates(pairs: &[(&str, &str, f64)]) -> AliasRegistry {
        use crate::dict::Dict;
        use crate::normalize::Normalizer;
        use crate::vocab::AliasProvenance;
        let n = Normalizer::default_vocab().expect("vocab");
        let d = Dict::new();
        let mut reg = AliasRegistry::default();
        for (a, b, conf) in pairs {
            reg.add_classified(
                &[(*a).to_string(), (*b).to_string()],
                AliasProvenance::LearnedDistributional,
                *conf,
                &n,
                &d,
            );
        }
        reg
    }

    #[test]
    fn sync_tracks_candidates_deterministically_and_retains_evidence() {
        let reg = registry_with_candidates(&[
            ("ud", "upperdeck", 0.9),
            ("rc", "rookie", 0.7),
            ("gem", "gemmint", 0.8),
        ]);
        let mut fb = AliasFeedback::default();
        fb.sync_tracked(&reg, 2);
        assert_eq!(fb.tracked_pairs(), 2, "capped (confidence desc)");
        // Accumulate evidence on the top pair, then re-sync: evidence retained.
        fb.observe(&s(&["ud", "fleer"]), &[1, 2, 3]);
        fb.sync_tracked(&reg, 2);
        let rep = fb.report(0.5, 1, 1, |_| None);
        let top = rep
            .iter()
            .find(|r| r.forms.contains(&"ud".to_string()))
            .unwrap();
        assert_eq!(
            top.titles_a + top.titles_b,
            1,
            "evidence survived the re-sync"
        );
        // Cap determinism: two identical syncs track the same pairs.
        let mut fb2 = AliasFeedback::default();
        fb2.sync_tracked(&reg, 2);
        let f1: Vec<Vec<String>> = fb
            .report(0.5, 1, 1, |_| None)
            .into_iter()
            .map(|r| r.forms)
            .collect();
        let f2: Vec<Vec<String>> = fb2
            .report(0.5, 1, 1, |_| None)
            .into_iter()
            .map(|r| r.forms)
            .collect();
        assert_eq!(f1, f2);
    }

    #[test]
    fn observe_classifies_sides_and_excludes_both_form_titles() {
        let reg = registry_with_candidates(&[("ud", "upperdeck", 0.9)]);
        let mut fb = AliasFeedback::default();
        fb.sync_tracked(&reg, 8);
        fb.observe(&s(&["ud", "fleer"]), &[1, 2]);
        fb.observe(&s(&["upperdeck", "fleer"]), &[1, 2]);
        fb.observe(&s(&["ud", "upperdeck"]), &[9]); // both forms ⇒ excluded
        fb.observe(&s(&["stud", "fleer"]), &[7]); // neither (token-exact) ⇒ ignored
        let rep = fb.report(0.5, 1, 1, |_| Some("fleer jordan".to_string()));
        let r = &rep[0];
        assert_eq!((r.titles_a, r.titles_b, r.titles_both), (1, 1, 1));
        assert!((r.overlap - 1.0).abs() < 1e-9, "identical populations");
        assert!(r.validated);
    }

    #[test]
    fn report_excludes_form_referencing_queries_and_unresolvable_ids() {
        let reg = registry_with_candidates(&[("ud", "upperdeck", 0.9)]);
        let mut fb = AliasFeedback::default();
        fb.sync_tracked(&reg, 8);
        // Both sides match queries {1, 2}: 1 is clean, 2 names the form `ud`.
        fb.observe(&s(&["ud", "fleer"]), &[1, 2]);
        fb.observe(&s(&["upperdeck", "fleer"]), &[1, 2]);
        let rep = fb.report(0.5, 1, 1, |id| match id {
            1 => Some("fleer jordan".to_string()),
            2 => Some("ud fleer".to_string()), // references a form ⇒ excluded
            _ => None,
        });
        let r = &rep[0];
        assert_eq!((r.sampled_a, r.sampled_b), (1, 1));
        assert_eq!(r.excluded, 2, "one per side");
        assert!(
            (r.overlap - 1.0).abs() < 1e-9,
            "survivors still overlap fully"
        );

        // A corpus of ONLY form-referencing queries yields zero surviving evidence.
        let mut fb = AliasFeedback::default();
        fb.sync_tracked(&reg, 8);
        fb.observe(&s(&["ud", "fleer"]), &[2]);
        fb.observe(&s(&["upperdeck", "fleer"]), &[2]);
        let rep = fb.report(0.5, 1, 1, |_| Some("ud fleer".to_string()));
        assert!(!rep[0].validated, "no surviving samples ⇒ never validated");
        assert!(rep[0].overlap.abs() < 1e-12);
    }

    /// The oversampling regression (codex): a population where 95% of matched queries are
    /// form-referencing must still validate — the exclusion filters the 4×-oversampled raw
    /// sketch, so plenty of clean samples survive past the SKETCH_K report cap. A sketch
    /// filtered AFTER a 256-cap would keep only ~13 clean samples here and starve
    /// `min_queries`.
    #[test]
    fn oversampled_sketch_survives_heavy_exclusion() {
        let reg = registry_with_candidates(&[("ud", "upperdeck", 0.9)]);
        let mut fb = AliasFeedback::default();
        fb.sync_tracked(&reg, 8);
        let ids: Vec<u64> = (0..2000).collect();
        for i in 0..60 {
            fb.observe(&s(&["ud", &format!("p{i}")]), &ids);
            fb.observe(&s(&["upperdeck", &format!("p{i}")]), &ids);
        }
        // 95% of queries reference a form (excluded); 5% are clean evidence.
        let rep = fb.report(0.5, 50, 20, |id| {
            Some(if id % 20 == 0 {
                "fleer jordan".to_string()
            } else {
                "ud fleer".to_string()
            })
        });
        let r = &rep[0];
        assert!(
            r.sampled_a >= 20 && r.sampled_b >= 20,
            "clean samples must survive heavy exclusion; got {r:?}"
        );
        assert!((r.overlap - 1.0).abs() < 1e-9, "identical populations");
        assert!(
            r.validated,
            "a true alias must not be starved by exclusion; got {r:?}"
        );
    }
}
