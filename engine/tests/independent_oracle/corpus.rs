//! Env-gated real-corpus differential. When `RR_ORACLE_CORPUS` points at a JSONL file of real saved
//! searches + real listing titles, the engine is diffed against the independent reference over it.
//! Skipped (passing) when the variable is unset, so CI and the public repo never see real data — the
//! corpus is user-supplied and stays entirely outside the tree.
//!
//! JSONL schema (one JSON object per line, two shapes; other keys ignored):
//!   {"query": "<dsl>"}     a saved search (queries are numbered in file order; an optional
//!                          informational "id" is not required and is ignored for matching)
//!   {"title": "<raw>"}     a listing title
//!
//! Runs under the empty default vocabulary (the front-end check that needs no domain config — it
//! still exercises parsing, normalization, diacritic folding, number typing, and the markers over
//! real strings). A populated-vocab real-corpus run is a future extension.

use crate::harness::RefOracle;

#[test]
fn real_corpus_differential() {
    let Ok(path) = std::env::var("RR_ORACLE_CORPUS") else {
        eprintln!("RR_ORACLE_CORPUS unset — skipping the real-corpus oracle");
        return;
    };

    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("RR_ORACLE_CORPUS={path:?} could not be read: {e}"));

    let mut queries: Vec<(u64, String)> = Vec::new();
    let mut titles: Vec<String> = Vec::new();
    for (lineno, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("{path}:{} not valid JSON: {e}", lineno + 1));
        if let Some(q) = v.get("query").and_then(serde_json::Value::as_str) {
            // Queries are numbered in file order; the engine and reference get the same ids.
            queries.push((queries.len() as u64 + 1, q.to_string()));
        } else if let Some(t) = v.get("title").and_then(serde_json::Value::as_str) {
            titles.push(t.to_string());
        }
    }

    assert!(
        !queries.is_empty(),
        "RR_ORACLE_CORPUS has no {{\"query\":...}} lines"
    );
    assert!(
        !titles.is_empty(),
        "RR_ORACLE_CORPUS has no {{\"title\":...}} lines"
    );
    eprintln!(
        "real-corpus: {} saved searches x {} titles from {path}",
        queries.len(),
        titles.len()
    );

    let oracle = RefOracle::build_default(&queries);
    oracle.assert_matches(&titles, "real-corpus");
}
