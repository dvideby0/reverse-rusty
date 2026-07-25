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
pub struct ExplainAnyOfMember {
    /// AND across requirements; OR across names inside one requirement.
    pub requirements: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainAnyOfPredicate {
    /// OR across members.
    pub members: Vec<ExplainAnyOfMember>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainPhraseArc {
    pub start: u32,
    pub end: u32,
    pub alternatives: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainPhrase {
    pub positions: u32,
    pub arcs: Vec<ExplainPhraseArc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainDetail {
    pub title_features: Vec<String>,
    pub candidate: bool,
    pub matched: bool,
    pub cost_class: String,
    pub required: Vec<String>,
    pub forbidden: Vec<String>,
    /// Compact simple groups and the lossless proxy groups used to retrieve
    /// compound predicates.
    pub anyof_groups: Vec<Vec<String>>,
    /// Exact member-preserving predicates for compound positive groups.
    pub anyof_member_predicates: Vec<ExplainAnyOfPredicate>,
    /// Whole negative conjunctions; the query rejects only when every feature
    /// in one member is present.
    pub forbidden_conjunctions: Vec<Vec<String>>,
    /// Required/forbidden analyzed token graphs for quoted clauses.
    pub required_phrases: Vec<ExplainPhrase>,
    pub forbidden_phrases: Vec<ExplainPhrase>,
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
    for (i, predicate) in cq.extracted.anyof_predicates.iter().enumerate() {
        s.push_str(&format!(
            "  ANY_OF_PREDICATE[{i}]: {}\n",
            predicate_name(predicate, dict)
        ));
    }
    s.push_str("  FORBIDDEN: ");
    s.push_str(&names(&cq.extracted.forbidden, dict));
    s.push('\n');
    for (i, conjunction) in cq.extracted.forbidden_conjunctions.iter().enumerate() {
        s.push_str(&format!(
            "  FORBIDDEN_MEMBER[{i}]: ALL OF ({})\n",
            names(conjunction, dict)
        ));
    }
    for (i, phrase) in cq.extracted.required_phrases.iter().enumerate() {
        s.push_str(&format!(
            "  REQUIRED_PHRASE[{i}]: {}\n",
            phrase_name(phrase, dict)
        ));
    }
    for (i, phrase) in cq.extracted.forbidden_phrases.iter().enumerate() {
        s.push_str(&format!(
            "  FORBIDDEN_PHRASE[{i}]: {}\n",
            phrase_name(phrase, dict)
        ));
    }
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
    if !cq.hot_sigs.is_empty() {
        s.push_str("  signatures (hot tier): ");
        for sg in &cq.hot_sigs {
            s.push_str(&format!("{sg:#018x} "));
        }
        s.push('\n');
    }
    if cq.cost_class == crate::compile::CostClass::H {
        s.push_str(
            "  class H: θ-hot anchor — hot tier (ADR-105): probed on every request \
             (always visible), evaluated columnar on the batch path\n",
        );
    }
    if cq.cost_class == crate::compile::CostClass::D {
        s.push_str(
            "  class D: negation-only — the broad signature above is the UNIVERSAL key \
             (always-candidate when stored under the accept_class_d lane, ADR-068)\n",
        );
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
    let mut sc = crate::normalize::NormScratch::new();
    // ADR-061 semantic views plus ADR-120's candidate-only probe: `pos` (overlapping flat
    // `P(T)`) drives required + any-of, `neg` (canonical `N(T)`) drives forbidden, and `probe`
    // drives retrieval. No alias or positioned labels ⇒ all applicable views coincide.
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    let mut probe = Vec::new();
    let (mut neg_arcs, mut pos_arcs) = (Vec::new(), Vec::new());
    let (positions, _complete) = norm.match_phrase_views(
        title,
        dict,
        &mut lc,
        &mut sc,
        &mut neg,
        &mut pos,
        &mut probe,
        &mut neg_arcs,
        &mut pos_arcs,
    );

    let mut s = String::new();
    s.push_str(&format!("title: {title:?}\n"));
    s.push_str(&format!("  title features: {}\n", names(&pos, dict)));

    // would any signature retrieve this query? (retrieval is from the positive superset)
    let mut title_sigs = std::collections::HashSet::new();
    for &f in &probe {
        title_sigs.insert(sig_key(&[f]));
    }
    for &h in &probe {
        if is_hot(dict, h) {
            for &o in &probe {
                if o != h {
                    let (a, b) = if h < o { (h, o) } else { (o, h) };
                    title_sigs.insert(sig_key(&[a, b]));
                }
            }
        }
    }
    // Every title implicitly generates the UNIVERSAL signature (ADR-068) — the broad
    // matcher probes it once per segment, which is how a stored class-D
    // always-candidate is retrieved. Mirror it here so explain can't report
    // `candidate: false` for a query the matcher reaches.
    title_sigs.insert(crate::util::universal_sig());
    let retrieved = cq.main_sigs.iter().any(|s| title_sigs.contains(s))
        || cq.broad_sigs.iter().any(|s| title_sigs.contains(s))
        || cq.hot_sigs.iter().any(|s| title_sigs.contains(s));
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
    for (i, predicate) in cq.extracted.anyof_predicates.iter().enumerate() {
        if !predicate.members.iter().any(|member| {
            member
                .requirements
                .iter()
                .all(|requirement| requirement.iter().any(|&feature| in_pos(feature)))
        }) {
            fail.push(format!("any_of_predicate[{i}] unsatisfied"));
        }
    }
    for (i, conjunction) in cq.extracted.forbidden_conjunctions.iter().enumerate() {
        if conjunction.iter().all(|&feature| in_neg(feature)) {
            fail.push(format!("forbidden_member[{i}] fully present"));
        }
    }
    for (i, phrase) in cq.extracted.required_phrases.iter().enumerate() {
        if !crate::normalize::phrase_graph_matches(phrase, positions, &pos_arcs) {
            fail.push(format!("required_phrase[{i}] not contiguous"));
        }
    }
    for (i, phrase) in cq.extracted.forbidden_phrases.iter().enumerate() {
        if crate::normalize::phrase_graph_matches(phrase, positions, &neg_arcs) {
            fail.push(format!("forbidden_phrase[{i}] present"));
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
    let mut sc = crate::normalize::NormScratch::new();
    // ADR-061 semantic views plus ADR-120's candidate-only retrieval probe, matching the verifier.
    // `pos` drives required + any-of, `neg` drives forbidden, and `probe` drives signatures.
    let (mut neg, mut pos) = (Vec::new(), Vec::new());
    let mut probe = Vec::new();
    let (mut neg_arcs, mut pos_arcs) = (Vec::new(), Vec::new());
    let (positions, _complete) = norm.match_phrase_views(
        title,
        dict,
        &mut lc,
        &mut sc,
        &mut neg,
        &mut pos,
        &mut probe,
        &mut neg_arcs,
        &mut pos_arcs,
    );

    let title_features: Vec<String> = pos.iter().map(|&id| dict.name(id).to_string()).collect();

    let mut title_sigs = std::collections::HashSet::new();
    for &f in &probe {
        title_sigs.insert(sig_key(&[f]));
    }
    for &h in &probe {
        if is_hot(dict, h) {
            for &o in &probe {
                if o != h {
                    let (a, b) = if h < o { (h, o) } else { (o, h) };
                    title_sigs.insert(sig_key(&[a, b]));
                }
            }
        }
    }
    // Every title implicitly generates the UNIVERSAL signature (ADR-068) — see
    // `explain_match`.
    title_sigs.insert(crate::util::universal_sig());
    let candidate = cq.main_sigs.iter().any(|s| title_sigs.contains(s))
        || cq.broad_sigs.iter().any(|s| title_sigs.contains(s))
        || cq.hot_sigs.iter().any(|s| title_sigs.contains(s));

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
    for (i, predicate) in cq.extracted.anyof_predicates.iter().enumerate() {
        if !predicate.members.iter().any(|member| {
            member
                .requirements
                .iter()
                .all(|requirement| requirement.iter().any(|&feature| in_pos(feature)))
        }) {
            failures.push(format!("any_of_predicate[{i}] unsatisfied"));
        }
    }
    for (i, conjunction) in cq.extracted.forbidden_conjunctions.iter().enumerate() {
        if conjunction.iter().all(|&feature| in_neg(feature)) {
            failures.push(format!("forbidden_member[{i}] fully present"));
        }
    }
    for (i, phrase) in cq.extracted.required_phrases.iter().enumerate() {
        if !crate::normalize::phrase_graph_matches(phrase, positions, &pos_arcs) {
            failures.push(format!("required_phrase[{i}] not contiguous"));
        }
    }
    for (i, phrase) in cq.extracted.forbidden_phrases.iter().enumerate() {
        if crate::normalize::phrase_graph_matches(phrase, positions, &neg_arcs) {
            failures.push(format!("forbidden_phrase[{i}] present"));
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
        anyof_member_predicates: cq
            .extracted
            .anyof_predicates
            .iter()
            .map(|predicate| ExplainAnyOfPredicate {
                members: predicate
                    .members
                    .iter()
                    .map(|member| ExplainAnyOfMember {
                        requirements: member
                            .requirements
                            .iter()
                            .map(|requirement| {
                                requirement
                                    .iter()
                                    .map(|&id| dict.name(id).to_string())
                                    .collect()
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
        forbidden_conjunctions: cq
            .extracted
            .forbidden_conjunctions
            .iter()
            .map(|member| member.iter().map(|&id| dict.name(id).to_string()).collect())
            .collect(),
        required_phrases: cq
            .extracted
            .required_phrases
            .iter()
            .map(|phrase| explain_phrase(phrase, dict))
            .collect(),
        forbidden_phrases: cq
            .extracted
            .forbidden_phrases
            .iter()
            .map(|phrase| explain_phrase(phrase, dict))
            .collect(),
        failures,
    }
}

fn explain_phrase(phrase: &crate::normalize::PhraseGraph, dict: &Dict) -> ExplainPhrase {
    ExplainPhrase {
        positions: phrase.positions,
        arcs: phrase
            .arcs
            .iter()
            .map(|arc| ExplainPhraseArc {
                start: arc.start,
                end: arc.end,
                alternatives: arc
                    .alternatives
                    .iter()
                    .map(|&feature| dict.name(feature).to_string())
                    .collect(),
            })
            .collect(),
    }
}

fn phrase_name(phrase: &crate::normalize::PhraseGraph, dict: &Dict) -> String {
    let explained = explain_phrase(phrase, dict);
    explained
        .arcs
        .iter()
        .map(|arc| format!("{}->{}({})", arc.start, arc.end, arc.alternatives.join("|")))
        .collect::<Vec<_>>()
        .join(" ")
}

fn predicate_name(predicate: &crate::compile::AnyOfPredicate, dict: &Dict) -> String {
    predicate
        .members
        .iter()
        .map(|member| {
            member
                .requirements
                .iter()
                .map(|alternatives| format!("({})", names(alternatives, dict)))
                .collect::<Vec<_>>()
                .join(" AND ")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
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
