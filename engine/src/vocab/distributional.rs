//! Distributional alias discovery (ADR-102) — technique 1 of the corpus-feature-learning
//! research doc (§5): two tokens are equivalence candidates if they appear in highly similar
//! query *contexts* (same neighbor distributions). Medium precision by nature — it conflates
//! true substitutes (`rc`/`rookie`) with co-hyponyms (`psa`/`bgs`, which fill the same slot but
//! are NOT interchangeable) — so everything this module proposes is **review-first**: the
//! registry files every discovered pair as a `Candidate` under the `LearnedDistributional`
//! provenance, which `default_status_for` never auto-activates (not even variant-looking pairs).
//!
//! Signal design (lean, std-only, deterministic):
//! - **Corpus** = the stored queries' positive clauses (negated clauses are excluded — a
//!   forbidden term is not semantic context), tokenized by [`corpus::tokenize`] — the same
//!   granularity as the NPMI learner.
//! - **Optional phrase glue** (default on): [`corpus::learn_phrases`] + [`apply_phrases`] first,
//!   so `upper deck` becomes the unit `upper_deck` and the canonical token-vs-multi-word case
//!   (`ud` ≡ `upper deck`) is discoverable at all.
//! - **Context vector** = same-query co-occurrence over the eligible vocabulary (queries are
//!   3–8-token intents; whole-query co-occurrence IS the neighbor distribution at that length).
//! - **Similarity** = cosine over PPMI-weighted vectors, accumulated sparsely via an inverted
//!   index over context tokens (only token pairs sharing ≥1 PPMI-positive context are ever
//!   touched; PPMI zeroes hub contexts, which is what keeps the accumulation sparse).
//! - **The co-hyponym defense**: a `cooccurrence_rate` penalty. True substitutes are
//!   *paradigmatic* (they fill the same slot and rarely appear together in one query);
//!   co-hyponyms are *syntagmatic* (`(psa,bgs)` any-ofs, `jordan pippen` duals co-occur). A pair
//!   co-occurring in more than `max_cooccurrence_rate` of its rarer member's queries is dropped.
//!   Heuristic, hence review-first — the miss cost is a reviewer's minute, never a match.

use std::collections::HashMap;

use crate::corpus::{apply_phrases, learn_phrases, tokenize};
use crate::dsl::{self, Atom};

#[cfg(test)]
mod tests;

/// Knobs for one discovery run — passed per call (the `CorpusLearnConfig` precedent), not an
/// `EngineConfig` resident. Defaults are the ADR-102 starting points.
#[derive(Debug, Clone)]
pub struct DistributionalConfig {
    /// A token must appear in at least this many queries to be considered.
    pub min_token_freq: usize,
    /// Minimum PPMI-cosine similarity for a pair to be proposed.
    pub min_similarity: f64,
    /// Hard cap on proposed pairs (best-first).
    pub max_pairs: usize,
    /// Eligible-vocabulary cap: the top-N tokens by frequency (ties token-asc). Bounds both the
    /// accumulator key space (≤ N²/2) and the inverted-index work.
    pub max_vocab: usize,
    /// Drop a pair whose `cooc / min(freq_a, freq_b)` exceeds this (the co-hyponym defense).
    pub max_cooccurrence_rate: f64,
    /// Glue NPMI phrases first so multi-word units participate as single tokens.
    pub glue_phrases: bool,
    /// NPMI phrase-glue support threshold (only with `glue_phrases`).
    pub npmi_min_count: usize,
    /// NPMI phrase-glue score threshold (only with `glue_phrases`).
    pub npmi_tau: f64,
    /// Consider numeric-only tokens (`1994`, `10`) — off by default: years/grades are textbook
    /// co-hyponyms with near-identical contexts.
    pub include_numeric: bool,
}

impl Default for DistributionalConfig {
    fn default() -> Self {
        Self {
            min_token_freq: 5,
            min_similarity: 0.60,
            max_pairs: 100,
            max_vocab: 4096,
            max_cooccurrence_rate: 0.05,
            glue_phrases: true,
            // Deliberately HIGHER than the ADR-053 corpus-phrase default (3): there a junk
            // phrase is harmless (additive indexing); here a junk glued unit spawns whole
            // families of high-similarity noise pairs that crowd the max_pairs budget.
            npmi_min_count: 10,
            npmi_tau: 0.30,
            include_numeric: false,
        }
    }
}

/// One proposed equivalence pair, with the evidence that produced it. `forms` are raw surface
/// forms (a glued phrase is emitted space-joined, e.g. `upper deck`) — they resolve through the
/// live normalizer at registry-classification time, like every other alias source.
/// `Serialize` so the review endpoint can return proposals verbatim (serde is lean-core).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct DiscoveredPair {
    pub forms: Vec<String>,
    pub similarity: f64,
    pub cooccurrence_rate: f64,
}

/// True iff the token is numeric-only (digits/dots — `1994`, `10.5`).
fn numeric_only(tok: &str) -> bool {
    !tok.is_empty() && tok.chars().all(|c| c.is_ascii_digit() || c == '.')
}

/// One query's context bag: the deduped tokens of its positive clauses.
fn context_bags(queries: &[(u64, String)], cfg: &DistributionalConfig) -> Vec<Vec<String>> {
    let mut bags: Vec<Vec<String>> = Vec::with_capacity(queries.len());
    for (_id, text) in queries {
        let Ok(ast) = dsl::parse(text) else {
            continue;
        };
        let mut bag: Vec<String> = Vec::new();
        for clause in &ast.clauses {
            if clause.negated {
                continue;
            }
            match &clause.atom {
                Atom::Term(t) | Atom::Phrase(t) => bag.extend(tokenize(t)),
                Atom::AnyOf(members) => {
                    for m in members {
                        bag.extend(tokenize(m));
                    }
                }
            }
        }
        if !bag.is_empty() {
            bags.push(bag);
        }
    }
    if cfg.glue_phrases {
        let phrases = learn_phrases(&bags, cfg.npmi_min_count, cfg.npmi_tau);
        bags = apply_phrases(&bags, &phrases);
    }
    // Presence semantics per query: dedup within each bag (sorted for determinism).
    for bag in &mut bags {
        bag.sort();
        bag.dedup();
    }
    bags
}

/// Discover distributional alias candidates over a query corpus. Deterministic: output is
/// sorted (similarity desc, forms asc) and capped at `cfg.max_pairs`.
pub fn discover_pairs(
    queries: &[(u64, String)],
    cfg: &DistributionalConfig,
) -> Vec<DiscoveredPair> {
    let bags = context_bags(queries, cfg);
    let n_bags = bags.len();
    if n_bags == 0 {
        return Vec::new();
    }

    // ---- eligible vocabulary: top max_vocab by (freq desc, token asc) --------------------
    let mut freq: HashMap<&str, usize> = HashMap::new();
    for bag in &bags {
        for tok in bag {
            *freq.entry(tok.as_str()).or_insert(0) += 1;
        }
    }
    let mut eligible: Vec<(&str, usize)> = freq
        .iter()
        .filter(|(t, &c)| c >= cfg.min_token_freq && (cfg.include_numeric || !numeric_only(t)))
        .map(|(&t, &c)| (t, c))
        .collect();
    eligible.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    eligible.truncate(cfg.max_vocab);
    // Dense ids in sorted-token order so every downstream ordering is reproducible.
    let mut vocab: Vec<&str> = eligible.iter().map(|&(t, _)| t).collect();
    vocab.sort_unstable();
    let ids: HashMap<&str, u32> = vocab
        .iter()
        .enumerate()
        .map(|(i, &t)| (t, i as u32))
        .collect();
    let tok_freq: Vec<usize> = vocab.iter().map(|&t| freq[t]).collect();

    // ---- same-query co-occurrence counts over the eligible vocab -------------------------
    let mut cooc: HashMap<(u32, u32), u32> = HashMap::new();
    for bag in &bags {
        let mut present: Vec<u32> = bag
            .iter()
            .filter_map(|t| ids.get(t.as_str()).copied())
            .collect();
        present.sort_unstable(); // bags are deduped; ids are unique per token
        for i in 0..present.len() {
            for j in (i + 1)..present.len() {
                *cooc.entry((present[i], present[j])).or_insert(0) += 1;
            }
        }
    }

    // ---- PPMI weights + per-token sparse vectors ------------------------------------------
    // PMI(a,b) = ln( p(a,b) / (p(a)·p(b)) ) with p(·) over query bags; PPMI clamps at 0, which
    // zeroes hub contexts (a token co-occurring with everything has PMI ≈ 0) — the sparsifier
    // the inverted-index accumulation below relies on.
    let n = n_bags as f64;
    let mut vectors: Vec<Vec<(u32, f64)>> = vec![Vec::new(); vocab.len()];
    for (&(a, b), &c) in &cooc {
        let pmi =
            ((f64::from(c) * n) / (tok_freq[a as usize] as f64 * tok_freq[b as usize] as f64)).ln();
        if pmi > 0.0 && pmi.is_finite() {
            vectors[a as usize].push((b, pmi));
            vectors[b as usize].push((a, pmi));
        }
    }
    // Determinism is load-bearing here, not cosmetic: entries arrive in HashMap iteration
    // order (random per instance), and float addition is non-associative — an unsorted
    // accumulation order would perturb norms/dots in the last ulp and flip near-tie output
    // ordering between two identical runs. Sorting each vector fixes every summation order
    // (norms below; the by_context lists and the dot accumulation both inherit it).
    for v in &mut vectors {
        v.sort_unstable_by_key(|&(id, _)| id);
    }
    let norms: Vec<f64> = vectors
        .iter()
        .map(|v| v.iter().map(|&(_, w)| w * w).sum::<f64>().sqrt())
        .collect();

    // ---- inverted index over context tokens → sparse dot products ------------------------
    // For each context token c, the list of tokens whose vector has a PPMI-positive entry at c;
    // every pair in that list shares context c, so accumulate the product. Only pairs sharing
    // ≥1 positive context are ever touched.
    let mut by_context: Vec<Vec<(u32, f64)>> = vec![Vec::new(); vocab.len()];
    for (t, vec) in vectors.iter().enumerate() {
        for &(c, w) in vec {
            by_context[c as usize].push((t as u32, w));
        }
    }
    let mut dots: HashMap<(u32, u32), f64> = HashMap::new();
    for list in &by_context {
        for i in 0..list.len() {
            for j in (i + 1)..list.len() {
                let (a, wa) = list[i];
                let (b, wb) = list[j];
                let key = if a < b { (a, b) } else { (b, a) };
                *dots.entry(key).or_insert(0.0) += wa * wb;
            }
        }
    }

    // ---- score, filter, emit ---------------------------------------------------------------
    let mut out: Vec<DiscoveredPair> = Vec::new();
    for (&(a, b), &dot) in &dots {
        let denom = norms[a as usize] * norms[b as usize];
        if denom <= 0.0 {
            continue;
        }
        let sim = dot / denom;
        if !sim.is_finite() || sim < cfg.min_similarity {
            continue;
        }
        let pair_cooc = cooc.get(&(a, b)).copied().unwrap_or(0);
        let rarer = tok_freq[a as usize].min(tok_freq[b as usize]) as f64;
        let rate = if rarer > 0.0 {
            f64::from(pair_cooc) / rarer
        } else {
            0.0
        };
        if rate > cfg.max_cooccurrence_rate {
            continue; // syntagmatic — a co-hyponym/companion pair, not a substitute
        }
        // Shared-token filter: two forms sharing a literal token (`zzud ctxp0` vs
        // `zzud ctxp5`, or `zzud ctxp0` vs `zzupperdeck ctxp0`) are members of one phrase
        // family — glue noise or a variant of the same unit, never the abbreviation-style
        // equivalence this discoverer exists for (a real pair like `ud`/`upper deck` shares
        // nothing). Kills whole junk families a spurious glue would otherwise spawn.
        let ta = vocab[a as usize];
        let tb = vocab[b as usize];
        if ta.split('_').any(|t| tb.split('_').any(|u| u == t)) {
            continue;
        }
        // Glued phrase units surface as space-joined forms.
        let forms = vec![ta.replace('_', " "), tb.replace('_', " ")];
        out.push(DiscoveredPair {
            forms,
            similarity: sim,
            cooccurrence_rate: rate,
        });
    }
    out.sort_by(|x, y| {
        y.similarity
            .partial_cmp(&x.similarity)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.forms.cmp(&y.forms))
    });
    out.truncate(cfg.max_pairs);
    out
}
