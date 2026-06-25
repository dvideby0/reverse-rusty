//! Naive (no-automaton) phrase matching for the reference.
//!
//! The engine uses a daachorse double-array Aho-Corasick automaton; the reference deliberately
//! uses linear substring scans instead. A test oracle optimizes for correctness + independence,
//! not speed, and a second, structurally different implementation of leftmost-longest /
//! overlapping selection is more likely to expose an integration bug than reusing the same
//! automaton would be. Reproduces the selection semantics of `core.rs::emit` (phase 1) and
//! `core/alias_overlap.rs` (`select_phrases` / `scan_overlapping`).

use crate::vocab::RefPhrase;

/// Every byte offset at which `needle` occurs in `haystack` (overlapping occurrences included).
fn find_all(haystack: &str, needle: &str) -> Vec<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return Vec::new();
    }
    let hb = haystack.as_bytes();
    let nb = needle.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i + nb.len() <= hb.len() {
        if &hb[i..i + nb.len()] == nb {
            out.push(i);
        }
        i += 1;
    }
    out
}

/// A phrase match must start at the beginning or just after a space, and end at the end or just
/// before a space — the word-boundary check `core.rs` / `alias_overlap.rs` apply.
fn boundary_ok(bytes: &[u8], s: usize, e: usize) -> bool {
    (s == 0 || bytes[s - 1] == b' ') && (e == bytes.len() || bytes[e] == b' ')
}

/// The cleaned, space-joined match string for a phrase (`["upper","deck"]` -> `"upper deck"`).
fn joined(p: &RefPhrase) -> String {
    p.tokens.join(" ")
}

/// Collapse whitespace runs (and strip a leading space), as `AliasOverlap::collect_into` does
/// before its overlapping scan. Returns the input unchanged when there is no run.
#[must_use]
pub fn collapse_ws_runs(s: &str) -> String {
    if !s.as_bytes().windows(2).any(|w| w == b"  ") {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut prev_space = true; // also strips a leading space
    for c in s.chars() {
        if c == ' ' {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(c);
            prev_space = false;
        }
    }
    out
}

/// Boundary-aware leftmost-longest non-overlapping phrase selection over `phrases`, returning
/// `(byte_start, byte_end, phrase_index)` in start order. Mirrors `AliasOverlap::select_phrases`
/// (and the legacy leftmost-longest pass in the non-pathological case): collect every
/// boundary-valid occurrence, prefer the smallest start then the longest match, drop later
/// candidates overlapping an accepted span.
#[must_use]
pub fn select_leftmost_longest(lc: &str, phrases: &[RefPhrase]) -> Vec<(usize, usize, usize)> {
    let bytes = lc.as_bytes();
    let mut cands: Vec<(usize, usize, usize)> = Vec::new();
    for (idx, p) in phrases.iter().enumerate() {
        let j = joined(p);
        if j.is_empty() {
            continue;
        }
        for s in find_all(lc, &j) {
            let e = s + j.len();
            if boundary_ok(bytes, s, e) {
                cands.push((s, e, idx));
            }
        }
    }
    // smallest start wins; ties prefer the longest match.
    cands.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    let mut end = 0usize;
    cands.retain(|&(s, e, _)| {
        if s >= end {
            end = e;
            true
        } else {
            false
        }
    });
    cands
}

/// Every boundary-valid OVERLAPPING phrase occurrence's index (whitespace runs collapsed first),
/// for building the positive view `P(T)`. Mirrors `AliasOverlap::collect_into` +
/// `scan_overlapping`, but over ALL phrases (the engine builds the overlap automaton over every
/// phrase, alias and non-alias, once an alias is active — the codex-R6 FN fix).
#[must_use]
pub fn scan_overlapping(lc: &str, phrases: &[RefPhrase]) -> Vec<usize> {
    let collapsed = collapse_ws_runs(lc);
    let bytes = collapsed.as_bytes();
    let mut out = Vec::new();
    for (idx, p) in phrases.iter().enumerate() {
        let j = joined(p);
        if j.is_empty() {
            continue;
        }
        for s in find_all(&collapsed, &j) {
            let e = s + j.len();
            if boundary_ok(bytes, s, e) {
                out.push(idx);
            }
        }
    }
    out
}
