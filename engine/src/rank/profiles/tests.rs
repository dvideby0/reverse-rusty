use super::*;

#[test]
fn title_features_are_allocation_free_counts() {
    assert_eq!(
        RankTitleFeatures::from_title("2024 North-Star Mouse"),
        RankTitleFeatures {
            tokens: 4,
            bytes: 21,
            digits: 4,
        }
    );
}

#[test]
fn loads_and_scores_linear_and_tree_profiles() {
    let profiles = RankProfiles::from_json_slice(
        br#"{
          "version": 1,
          "profiles": {
            "linear_v1": {
              "kind": "linear",
              "intercept": 7,
              "weights": [
                {"feature": "query_positive_terms", "weight": 10},
                {"feature": "unmatched_title_tokens", "weight": -2}
              ]
            },
            "ltr_v1": {
              "kind": "tree_ensemble",
              "trees": [{
                "nodes": [
                  {"kind": "split", "feature": "positive_coverage_milli",
                   "threshold": 500, "left": 1, "right": 2},
                  {"kind": "leaf", "value": -20},
                  {"kind": "leaf", "value": 30}
                ]
              }]
            }
          }
        }"#,
    )
    .expect("valid registry");
    let features = RankFeatureView::new(
        RankQueryFeatures {
            positive_terms: 3,
            ..RankQueryFeatures::default()
        },
        RankTitleFeatures {
            tokens: 4,
            ..RankTitleFeatures::default()
        },
    );
    assert_eq!(
        profiles
            .get("linear_v1")
            .expect("linear")
            .program
            .relevance_score(features),
        35
    );
    assert_eq!(
        profiles
            .get("ltr_v1")
            .expect("tree")
            .program
            .relevance_score(features),
        30
    );
}

#[test]
fn rejects_cycles_static_override_and_wrong_fingerprint() {
    let cycle = RankProfiles::from_json_slice(
        br#"{"version":1,"profiles":{"ltr_v1":{"kind":"tree_ensemble","trees":[{
          "nodes":[{"kind":"split","feature":"title_tokens","threshold":1,"left":0,"right":1},
                   {"kind":"leaf","value":1}]
        }]}}}"#,
    );
    assert!(cycle.is_err());

    let override_static = RankProfiles::from_json_slice(
        br#"{"version":1,"profiles":{"static_v1":{
          "kind":"linear","weights":[]
        }}}"#,
    );
    assert!(override_static.is_err());

    let wrong_fingerprint = RankProfiles::from_json_slice(
        br#"{"version":1,"profiles":{"linear_v1":{
          "kind":"linear",
          "expected_fingerprint":"fnv1a64:0000000000000000",
          "weights":[]
        }}}"#,
    );
    assert!(wrong_fingerprint.is_err());

    let shared_node = RankProfiles::from_json_slice(
        br#"{"version":1,"profiles":{"ltr_v1":{"kind":"tree_ensemble","trees":[{
          "nodes":[{"kind":"split","feature":"title_tokens","threshold":1,"left":1,"right":1},
                   {"kind":"leaf","value":1}]
        }]}}}"#,
    );
    assert!(shared_node.is_err());
}

#[test]
fn rejects_duplicate_profile_names_in_strict_json() {
    let duplicate = RankProfiles::from_json_slice(
        br#"{
          "version": 1,
          "profiles": {
            "linear_v1": {"kind": "linear", "intercept": 1, "weights": []},
            "linear_v1": {"kind": "linear", "intercept": 2, "weights": []}
          }
        }"#,
    )
    .expect_err("duplicate profile names must fail startup");
    assert!(duplicate
        .to_string()
        .contains("duplicate ranking profile name `linear_v1`"));
}

#[test]
fn deployed_example_is_valid_and_fingerprint_pinned() {
    let profiles = RankProfiles::from_json_slice(include_bytes!(
        "../../../../deploy/ranking-profiles.example.json"
    ))
    .expect("example ranking profiles");
    assert!(profiles.contains("linear_v1"));
    assert!(profiles.contains("ltr_v1"));
}
