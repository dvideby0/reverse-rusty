//! Explain / debug tooling — first-class, not bolt-on.
//!
//! Design: docs/design/matching.md §6
//! Invariant: Reads the same data the matcher uses — no shadow structures
//! Hot path: no — diagnostic only, not called during normal matching
//!
//! Renders a compiled query and, for a (title, query) pair, why the query was
//! (or wasn't) a candidate and the exact pass/fail reason.

use crate::compile::{is_hot, CompiledQuery};
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::util::sig_key;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ExplainDetail {
    pub title_features: Vec<String>,
    pub candidate: bool,
    pub matched: bool,
    pub cost_class: String,
    pub required: Vec<String>,
    pub forbidden: Vec<String>,
    pub anyof_groups: Vec<Vec<String>>,
    pub failures: Vec<String>,
}

pub fn explain_compiled(cq: &CompiledQuery, dict: &Dict) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "logical_id={} version={} class={:?}\n",
        cq.logical_id, cq.version, cq.cost_class
    ));
    s.push_str("  REQUIRED: ");
    s.push_str(&names(&cq.extracted.required, dict));
    s.push('\n');
    if !cq.extracted.anyof.is_empty() {
        for (i, g) in cq.extracted.anyof.iter().enumerate() {
            s.push_str(&format!("  ANY_OF[{i}]: {}\n", names(g, dict)));
        }
    }
    s.push_str("  FORBIDDEN: ");
    s.push_str(&names(&cq.extracted.forbidden, dict));
    s.push('\n');
    s.push_str("  signatures (main): ");
    for sg in &cq.main_sigs {
        s.push_str(&format!("{sg:#018x} "));
    }
    s.push('\n');
    if !cq.broad_sigs.is_empty() {
        s.push_str("  signatures (broad lane): ");
        for sg in &cq.broad_sigs {
            s.push_str(&format!("{sg:#018x} "));
        }
        s.push('\n');
    }
    // anchor commentary
    if let Some(&r1) = cq.extracted.required.iter().min_by_key(|&&f| dict.freq(f)) {
        s.push_str(&format!(
            "  rarest required feature: {} (freq={}, hot={})\n",
            dict.name(r1),
            dict.freq(r1),
            is_hot(dict, r1)
        ));
    }
    s
}

/// Explain a single title against a single compiled query.
pub fn explain_match(cq: &CompiledQuery, title: &str, norm: &Normalizer, dict: &Dict) -> String {
    let mut lc = String::new();
    // Two title views (ADR-061): `pos` (overlapping superset `P(T)`) drives retrieval + required +
    // any-of; `neg` (canonical `N(T)`) drives forbidden — matching the real verifier so explain
    // can't disagree with the matcher under an active multi-word alias. No alias ⇒ pos == neg.
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    norm.match_features_dual(title, dict, &mut lc, &mut neg, &mut pos);

    let mut s = String::new();
    s.push_str(&format!("title: {title:?}\n"));
    s.push_str(&format!("  title features: {}\n", names(&pos, dict)));

    // would any signature retrieve this query? (retrieval is from the positive superset)
    let mut title_sigs = std::collections::HashSet::new();
    for &f in &pos {
        title_sigs.insert(sig_key(&[f]));
    }
    for &h in &pos {
        if is_hot(dict, h) {
            for &o in &pos {
                if o != h {
                    let (a, b) = if h < o { (h, o) } else { (o, h) };
                    title_sigs.insert(sig_key(&[a, b]));
                }
            }
        }
    }
    let retrieved = cq.main_sigs.iter().any(|s| title_sigs.contains(s))
        || cq.broad_sigs.iter().any(|s| title_sigs.contains(s));
    s.push_str(&format!(
        "  candidate? {retrieved} (title generates a signature in this query's cover)\n"
    ));

    // exact reasons: positive checks vs P(T), forbidden vs N(T) (ADR-061)
    let in_pos = |f: u32| pos.binary_search(&f).is_ok();
    let in_neg = |f: u32| neg.binary_search(&f).is_ok();
    let mut fail = Vec::new();
    for &f in &cq.extracted.required {
        if !in_pos(f) {
            fail.push(format!("missing required {}", dict.name(f)));
        }
    }
    for &f in &cq.extracted.forbidden {
        if in_neg(f) {
            fail.push(format!("present forbidden {}", dict.name(f)));
        }
    }
    for (i, g) in cq.extracted.anyof.iter().enumerate() {
        if !g.iter().any(|&f| in_pos(f)) {
            fail.push(format!("any_of[{i}] unsatisfied"));
        }
    }
    if fail.is_empty() {
        s.push_str("  exact match: PASS\n");
    } else {
        s.push_str("  exact match: FAIL\n");
        for r in fail {
            s.push_str(&format!("    - {r}\n"));
        }
    }
    s
}

/// Structured explain — same logic as `explain_match` but returns a
/// serializable struct for API responses.
pub fn explain_match_structured(
    cq: &CompiledQuery,
    title: &str,
    norm: &Normalizer,
    dict: &Dict,
) -> ExplainDetail {
    let mut lc = String::new();
    // Two title views (ADR-061), matching the verifier: positive superset `pos` for retrieval +
    // required + any-of, canonical `neg` for forbidden. No active multi-word alias ⇒ pos == neg.
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    norm.match_features_dual(title, dict, &mut lc, &mut neg, &mut pos);

    let title_features: Vec<String> = pos.iter().map(|&id| dict.name(id).to_string()).collect();

    let mut title_sigs = std::collections::HashSet::new();
    for &f in &pos {
        title_sigs.insert(sig_key(&[f]));
    }
    for &h in &pos {
        if is_hot(dict, h) {
            for &o in &pos {
                if o != h {
                    let (a, b) = if h < o { (h, o) } else { (o, h) };
                    title_sigs.insert(sig_key(&[a, b]));
                }
            }
        }
    }
    let candidate = cq.main_sigs.iter().any(|s| title_sigs.contains(s))
        || cq.broad_sigs.iter().any(|s| title_sigs.contains(s));

    let in_pos = |f: u32| pos.binary_search(&f).is_ok();
    let in_neg = |f: u32| neg.binary_search(&f).is_ok();
    let mut failures = Vec::new();
    for &f in &cq.extracted.required {
        if !in_pos(f) {
            failures.push(format!("missing required {}", dict.name(f)));
        }
    }
    for &f in &cq.extracted.forbidden {
        if in_neg(f) {
            failures.push(format!("present forbidden {}", dict.name(f)));
        }
    }
    for (i, g) in cq.extracted.anyof.iter().enumerate() {
        if !g.iter().any(|&f| in_pos(f)) {
            failures.push(format!("any_of[{i}] unsatisfied"));
        }
    }

    ExplainDetail {
        title_features,
        candidate,
        matched: failures.is_empty(),
        cost_class: format!("{:?}", cq.cost_class),
        required: cq
            .extracted
            .required
            .iter()
            .map(|&id| dict.name(id).to_string())
            .collect(),
        forbidden: cq
            .extracted
            .forbidden
            .iter()
            .map(|&id| dict.name(id).to_string())
            .collect(),
        anyof_groups: cq
            .extracted
            .anyof
            .iter()
            .map(|g| g.iter().map(|&id| dict.name(id).to_string()).collect())
            .collect(),
        failures,
    }
}

fn names(ids: &[u32], dict: &Dict) -> String {
    if ids.is_empty() {
        return "(none)".into();
    }
    ids.iter()
        .map(|&id| dict.name(id).to_string())
        .collect::<Vec<_>>()
        .join(", ")
}
