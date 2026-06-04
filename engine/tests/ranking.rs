//! Engine-level ranking tests (ADR-059): the post-match `EngineSnapshot::rank`
//! scorer + `compile_rank_spec`, including newest-live-copy tag precedence and the
//! recall guard (ranking reorders the matched set, never changes its membership).
//!
//! The HTTP-surface behavior (response `_score`, `from`/`size` pagination, per-slot
//! truncation, byte-identical unranked path) is covered by the co-located handler
//! tests in `src/bin/server/handlers/search.rs`. The pure scorer arithmetic is unit
//! tested in `src/rank.rs`.

use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::{EngineSnapshot, Normalizer, RankSpec};

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocab")
}

fn tag(k: &str, v: &str) -> (String, String) {
    (k.to_string(), v.to_string())
}

fn boost(k: &str, v: &str, w: i64) -> (String, String, i64) {
    (k.to_string(), v.to_string(), w)
}

/// Match a title, returning the matched logical ids (sorted ascending, as the
/// engine dedups them).
fn matched(snap: &EngineSnapshot, title: &str) -> Vec<u64> {
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    snap.match_title(title, &mut s, &mut out, true);
    out.sort_unstable();
    out
}

/// Score + order ids exactly as the REST handler does: (score desc, id asc).
fn ranked_ids(snap: &EngineSnapshot, ids: &[u64], spec: &RankSpec) -> Vec<(u64, i64)> {
    let compiled = snap.compile_rank_spec(spec);
    let mut scored = snap.rank(ids, &compiled);
    scored.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    scored
}

#[test]
fn ranks_by_priority_then_boost_additively() {
    let mut eng = Engine::new(norm());
    // Three queries that all match the title below, with distinct priority + tier tags.
    eng.insert_live_with_tags(
        "topps chrome",
        1,
        1,
        &[tag("priority", "10"), tag("tier", "gold")],
    );
    eng.insert_live_with_tags("topps chrome", 2, 1, &[tag("priority", "50")]);
    eng.insert_live_with_tags("topps chrome", 3, 1, &[tag("tier", "gold")]);
    let snap = eng.snapshot();
    let ids = matched(&snap, "2020 topps chrome update");
    assert_eq!(ids, vec![1, 2, 3], "all three queries match the title");

    // priority only: 2 (50) > 1 (10) > 3 (0).
    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![],
    };
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(2, 50), (1, 10), (3, 0)]
    );

    // boost tier=gold by 100, no priority: 1 & 3 tie at 100 (id asc breaks the tie), 2 = 0.
    let spec = RankSpec {
        priority_key: None,
        boosts: vec![boost("tier", "gold", 100)],
    };
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(1, 100), (3, 100), (2, 0)]
    );

    // additive priority + boost: 1 = 10+100, 3 = 0+100, 2 = 50 → order 1, 3, 2.
    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![boost("tier", "gold", 100)],
    };
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(1, 110), (3, 100), (2, 50)]
    );
}

#[test]
fn ranking_never_changes_the_matched_set() {
    // The recall guard: ranking only reorders the already-final id set — it may
    // never add or drop a match. Compare the ranked id SET to the raw matched set.
    let mut eng = Engine::new(norm());
    eng.insert_live_with_tags("topps chrome", 1, 1, &[tag("priority", "10")]);
    eng.insert_live_with_tags("topps chrome", 2, 1, &[]); // untagged → priority 0
    eng.insert_live_with_tags("topps chrome", 3, 1, &[tag("priority", "999")]);
    let snap = eng.snapshot();
    let ids = matched(&snap, "2020 topps chrome update");

    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![boost("tier", "gold", 100)],
    };
    let mut got: Vec<u64> = ranked_ids(&snap, &ids, &spec)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    got.sort_unstable();
    assert_eq!(got, ids, "ranking preserves the exact matched set");
}

#[test]
fn rank_uses_newest_live_copy_tags() {
    // An update is "insert new version + tombstone old", but the low-level insert
    // does NOT tombstone, so after a flush the logical id is alive in BOTH a base
    // segment (v1, priority 1) and the memtable (v2, priority 9). `tags_for_logical`
    // must pick the NEWEST live copy (memtable), so the score is 9, not 1.
    let mut eng = Engine::new(norm());
    eng.insert_live_with_tags("topps chrome", 1, 1, &[tag("priority", "1")]);
    eng.flush(); // bake v1 into a base segment (still alive — no tombstone)
    eng.insert_live_with_tags("topps chrome", 1, 2, &[tag("priority", "9")]);
    let snap = eng.snapshot();
    let ids = matched(&snap, "2020 topps chrome update");
    assert_eq!(ids, vec![1], "the logical id dedups to a single hit");

    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![],
    };
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(1, 9)],
        "the memtable (newest) copy's priority wins over the base copy"
    );
}

#[test]
fn rank_scores_unknown_id_zero() {
    let mut eng = Engine::new(norm());
    eng.insert_live_with_tags("topps chrome", 1, 1, &[tag("priority", "5")]);
    let snap = eng.snapshot();
    let spec = snap.compile_rank_spec(&RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![],
    });
    // A logical id that was never inserted has no live tags → score 0 (never panics).
    assert_eq!(snap.rank(&[999], &spec), vec![(999, 0)]);
}
