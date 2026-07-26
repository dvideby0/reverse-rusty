use super::*;

#[test]
fn scalar_tag_values_coerce_canonically() {
    // Numbers and bools coerce to their canonical JSON text (the ES keyword
    // behavior); strings pass through. Both the `tags` object and ES-style
    // sibling fields take the same rule.
    let mut tags = tags_of(&serde_json::json!({
        "query": "q",
        "tags": {"priority": 7, "active": true, "tier": "gold"},
        "category": 42.5,
    }))
    .expect("scalars must coerce, not error");
    tags.sort();
    assert_eq!(
        tags,
        vec![
            ("active".to_string(), "true".to_string()),
            ("category".to_string(), "42.5".to_string()),
            ("priority".to_string(), "7".to_string()),
            ("tier".to_string(), "gold".to_string()),
        ]
    );
}

#[test]
fn null_tag_values_are_skipped_not_errors() {
    // An explicit null is the ES "no value" — the key carries no tag, top-level
    // or as an array element; `"tags": null` means no tags at all.
    let tags = tags_of(&serde_json::json!({
        "query": "q",
        "tags": {"status": null},
        "colors": ["red", null, 3],
    }))
    .expect("null is skip, not an error");
    assert_eq!(
        tags,
        vec![
            ("colors".to_string(), "red".to_string()),
            ("colors".to_string(), "3".to_string()),
        ]
    );
    assert_eq!(
        tags_of(&serde_json::json!({"query": "q", "tags": null})).expect("tags:null is no tags"),
        vec![]
    );
}

#[test]
fn empty_tag_keys_fail_loud() {
    // An empty KEY rejects (codex retro-review, ADR-075 family): an empty
    // `priority_key` means "no priority term" (the gRPC wire cannot express it),
    // so an empty-key tag would be reachable by SOME ranking paths and not others.
    // Both intake shapes — the `tags` object and an ES-style sibling field.
    let err = tags_of(&serde_json::json!({"query": "q", "tags": {"": "v"}}))
        .expect_err("an empty tag key in `tags` must reject");
    assert!(err.contains("non-empty"), "names the rule (got: {err})");
    assert!(
        tags_of(&serde_json::json!({"query": "q", "": "v"})).is_err(),
        "an empty sibling-field key must reject too"
    );
}

#[test]
fn typed_priority_is_strict_mirrored_and_conflict_checked() {
    let object = serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": "-50"},
        "tags": {"tenant": "acme"}
    });
    let (tags, rank) = super::super::extract_ranked_ingest(object.as_object().expect("object"))
        .expect("typed priority");
    assert_eq!(rank, Some(reverse_rusty::RankValues { priority: -50 }));
    assert!(tags.contains(&("priority".to_string(), "-50".to_string())));

    let matching = serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": 50},
        "tags": {"priority": "50"}
    });
    assert!(super::super::extract_ranked_ingest(matching.as_object().expect("object")).is_ok());

    let conflict = serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": 50},
        "tags": {"priority": "49"}
    });
    let (kind, _) = super::super::extract_ranked_ingest(conflict.as_object().expect("object"))
        .expect_err("conflict");
    assert_eq!(kind, "invalid_rank_value");
}

#[test]
fn typed_priority_rejects_non_integer_json_and_overflow() {
    for value in [
        serde_json::json!(1.5),
        serde_json::json!(true),
        serde_json::Value::Null,
        serde_json::json!([]),
        serde_json::json!({}),
        serde_json::json!("9223372036854775808"),
    ] {
        let object = serde_json::json!({
            "query": "topps chrome",
            "rank_fields": {"priority": value}
        });
        let (kind, _) = super::super::extract_ranked_ingest(object.as_object().expect("object"))
            .expect_err("invalid typed rank");
        assert_eq!(kind, "invalid_rank_value");
    }
}
