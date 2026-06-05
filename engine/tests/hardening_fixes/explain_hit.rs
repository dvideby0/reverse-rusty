//! Engine::explain_hit — read-only explain via search API.

use reverse_rusty::segment::Engine;

use crate::harness::{make_norm, match_ids};

#[test]
fn explain_hit_returns_structured_detail_for_matched_query() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    let queries = vec![
        (1u64, "michael jordan 1986 fleer".to_string()),
        (2u64, "kobe bryant psa 10".to_string()),
    ];
    engine.build_from_queries(&queries);

    let title = "michael jordan 1986 fleer rookie card";
    let ids = match_ids(&engine, title);
    assert!(ids.contains(&1), "query 1 should match");

    let detail = engine.explain_hit(1, title);
    assert!(
        detail.is_some(),
        "explain_hit should return detail for stored query"
    );
    let detail = detail.unwrap();
    assert!(detail.candidate, "matched query must be a candidate");
    assert!(detail.matched, "matched query must pass exact verification");
    assert!(
        detail.failures.is_empty(),
        "no failures for a passing match"
    );
    assert!(
        !detail.title_features.is_empty(),
        "should extract title features"
    );
    assert!(
        !detail.required.is_empty(),
        "compiled query should have required features"
    );
}

#[test]
fn explain_hit_shows_failure_for_non_matching_title() {
    let norm = make_norm();
    let mut engine = Engine::new(norm);

    engine.build_from_queries(&[(1u64, "michael jordan 1986 fleer".to_string())]);

    let title = "kobe bryant 1996 topps chrome";
    let ids = match_ids(&engine, title);
    assert!(!ids.contains(&1), "query 1 should not match this title");

    let detail = engine.explain_hit(1, title);
    assert!(detail.is_some());
    let detail = detail.unwrap();
    assert!(!detail.matched, "should not pass exact verification");
    assert!(!detail.failures.is_empty(), "should report failure reasons");
}

#[test]
fn explain_hit_returns_none_for_unknown_id() {
    let norm = make_norm();
    let engine = Engine::new(norm);
    assert!(engine.explain_hit(999, "anything").is_none());
}

#[test]
fn explain_hit_uses_dual_view_for_multiword_alias() {
    // ADR-061: explain must use the same two title views as the matcher. With nested aliases
    // (new york ⊂ new york city), a `new york` query matches a `new york city` title via the
    // positive superset P(T); explain must AGREE (candidate + matched), not report a miss from the
    // canonical N(T) (which lacks the overlap-only `term:new_york`). This fails on the pre-fix
    // single-view explain.
    let mut engine = Engine::new(reverse_rusty::normalize::Normalizer::default_vocab().unwrap());
    engine.build_from_queries(&[(1u64, "new york yankees".to_string())]);
    engine
        .import_alias_synonyms("ny => new york\nnyc => new york city")
        .expect("apply multi-word aliases");

    let title = "new york city yankees";
    assert!(
        match_ids(&engine, title).contains(&1),
        "the matcher hits via the positive superset P(T)"
    );

    let detail = engine.explain_hit(1, title).expect("explain detail");
    assert!(
        detail.candidate,
        "explain must agree the title is a candidate (retrieval from P(T))"
    );
    assert!(
        detail.matched,
        "explain must agree the query matched (positive checks vs P(T))"
    );
    assert!(
        detail.failures.is_empty(),
        "explain must report no failures: {:?}",
        detail.failures
    );
}
