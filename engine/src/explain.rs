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
        s.push_str(&format!("{:#018x} ", sg));
    }
    s.push('\n');
    if !cq.broad_sigs.is_empty() {
        s.push_str("  signatures (broad lane): ");
        for sg in &cq.broad_sigs {
            s.push_str(&format!("{:#018x} ", sg));
        }
        s.push('\n');
    }
    // anchor commentary
    if let Some(&r1) = cq
        .extracted
        .required
        .iter()
        .min_by_key(|&&f| dict.freq(f))
    {
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
pub fn explain_match(
    cq: &CompiledQuery,
    title: &str,
    norm: &Normalizer,
    dict: &Dict,
) -> String {
    let mut lc = String::new();
    let mut feats = Vec::new();
    norm.match_features(title, dict, &mut lc, &mut feats);

    let mut s = String::new();
    s.push_str(&format!("title: {:?}\n", title));
    s.push_str(&format!("  title features: {}\n", names(&feats, dict)));

    // would any signature retrieve this query?
    let mut title_sigs = std::collections::HashSet::new();
    for &f in &feats {
        title_sigs.insert(sig_key(&[f]));
    }
    for &h in &feats {
        if is_hot(dict, h) {
            for &o in &feats {
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
        "  candidate? {} (title generates a signature in this query's cover)\n",
        retrieved
    ));

    // exact reasons
    let present = |f: u32| feats.binary_search(&f).is_ok();
    let mut fail = Vec::new();
    for &f in &cq.extracted.required {
        if !present(f) {
            fail.push(format!("missing required {}", dict.name(f)));
        }
    }
    for &f in &cq.extracted.forbidden {
        if present(f) {
            fail.push(format!("present forbidden {}", dict.name(f)));
        }
    }
    for (i, g) in cq.extracted.anyof.iter().enumerate() {
        if !g.iter().any(|&f| present(f)) {
            fail.push(format!("any_of[{i}] unsatisfied"));
        }
    }
    if fail.is_empty() {
        s.push_str("  exact match: PASS\n");
    } else {
        s.push_str("  exact match: FAIL\n");
        for r in fail {
            s.push_str(&format!("    - {}\n", r));
        }
    }
    s
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
