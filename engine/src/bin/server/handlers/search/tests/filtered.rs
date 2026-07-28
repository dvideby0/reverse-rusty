#![allow(clippy::used_underscore_binding)]

use super::*;

#[tokio::test]
async fn mpercolate_ranks_by_priority_and_truncates_to_size() {
    let state = tagged_state();
    let req: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 acme chrome update"}],
        "rank": {"priority_key": "priority"},
        "size": 2
    }))
    .expect("valid body");
    let resp = mpercolate(State(state), Json(req)).await.expect("ok").0;
    let item = &resp.responses[0];
    assert_eq!(item.hits.total, 3, "total is the untruncated match count");
    let ids: Vec<u64> = item.hits.hits.iter().map(|h| h._id).collect();
    assert_eq!(ids, vec![2, 1], "size=2 → top two by priority (50, 10)");
    assert_eq!(item.hits.hits[0]._score, Some(50));
    assert_eq!(item.hits.hits[1]._score, Some(10));
}

#[tokio::test]
async fn mpercolate_from_offsets_into_ranked_hits() {
    let state = tagged_state();
    let req: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 acme chrome update"}],
        "rank": {"priority_key": "priority"},
        "from": 1,
        "size": 10
    }))
    .expect("valid body");
    let resp = mpercolate(State(state), Json(req)).await.expect("ok").0;
    let ids: Vec<u64> = resp.responses[0].hits.hits.iter().map(|h| h._id).collect();
    // ranked order is [2, 1, 3]; from=1 drops the first → [1, 3].
    assert_eq!(ids, vec![1, 3]);
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn ranking_preserves_the_matched_set_and_score_is_opt_in() {
    let state = tagged_state();
    let ranked: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 acme chrome update"}],
        "rank": {"priority_key": "priority", "boosts": [{"key": "tier", "value": "gold", "boost": 100}]},
        "size": 100
    }))
    .expect("valid body");
    let unranked: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 acme chrome update"}],
        "size": 100
    }))
    .expect("valid body");
    let r = mpercolate(State(Arc::clone(&state)), Json(ranked))
        .await
        .expect("ok")
        .0;
    let u = mpercolate(State(state), Json(unranked))
        .await
        .expect("ok")
        .0;

    let mut rset: Vec<u64> = r.responses[0].hits.hits.iter().map(|h| h._id).collect();
    let mut uset: Vec<u64> = u.responses[0].hits.hits.iter().map(|h| h._id).collect();
    rset.sort_unstable();
    uset.sort_unstable();
    assert_eq!(
        rset, uset,
        "ranking must not add or drop a match (recall guard)"
    );

    assert!(
        u.responses[0].hits.hits.iter().all(|h| h._score.is_none()),
        "unranked hits carry no _score (byte-identical response)"
    );
    assert!(
        r.responses[0].hits.hits.iter().all(|h| h._score.is_some()),
        "ranked hits all carry a _score"
    );
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn search_single_doc_ranks_additively_with_boost() {
    let state = tagged_state();
    let req: SearchBody = serde_json::from_value(serde_json::json!({
        "document": {"title": "2020 acme chrome update"},
        "rank": {"priority_key": "priority", "boosts": [{"key": "tier", "value": "gold", "boost": 100}]}
    }))
    .expect("valid body");
    let resp = search(State(state), Json(req)).await.expect("ok").0;
    let ids: Vec<u64> = resp.hits.hits.iter().map(|h| h._id).collect();
    // additive: 1 = 10+100, 3 = 0+100, 2 = 50 → [1, 3, 2].
    assert_eq!(ids, vec![1, 3, 2]);
    assert_eq!(resp.hits.hits[0]._score, Some(110));
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn search_multi_doc_truncates_per_slot_by_size() {
    let state = tagged_state();
    let req: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 acme chrome update"}],
        "size": 1,
        "rank": {"priority_key": "priority"}
    }))
    .expect("valid body");
    let resp = search(State(state), Json(req)).await.expect("ok").0;
    let slots = resp.slots.expect("multi-doc response has slots");
    assert_eq!(
        slots[0].total, 3,
        "per-slot total preserves the untruncated count"
    );
    assert_eq!(
        slots[0].hits.len(),
        1,
        "per-slot hits truncated to size=1 (ADR-059)"
    );
    assert_eq!(
        slots[0].hits[0]._id, 2,
        "the surviving hit is the top by priority"
    );
}
