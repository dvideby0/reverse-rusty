use super::RESERVED_INGEST_FIELDS;

type RankedIngest = (Vec<(String, String)>, Option<reverse_rusty::RankValues>);
type RankedIngestError = (&'static str, String);

fn parse_priority_value(value: &serde_json::Value) -> Result<i64, String> {
    match value {
        serde_json::Value::Number(number) => number.as_i64().ok_or_else(|| {
            "rank_fields.priority must be an integer JSON value fitting signed i64".to_string()
        }),
        serde_json::Value::String(value) => value.parse::<i64>().map_err(|_| {
            "rank_fields.priority string must be signed decimal fitting i64".to_string()
        }),
        other => Err(format!(
            "rank_fields.priority must be an integer or signed decimal string (got {})",
            json_type_name(other)
        )),
    }
}

/// Parse strict typed rank metadata and mirror it into the canonical legacy tag.
/// The returned optional value distinguishes an explicit typed zero from absence.
pub(crate) fn extract_ranked_ingest(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<RankedIngest, RankedIngestError> {
    let mut tags = extract_ingest_tags(obj).map_err(|reason| ("invalid_tag_value", reason))?;
    let Some(raw_fields) = obj.get("rank_fields") else {
        return Ok((tags, None));
    };
    let fields = raw_fields.as_object().ok_or_else(|| {
        (
            "invalid_rank_value",
            format!(
                "rank_fields must be an object (got {})",
                json_type_name(raw_fields)
            ),
        )
    })?;
    if let Some(field) = fields.keys().find(|key| key.as_str() != "priority") {
        return Err((
            "unsupported_rank_field",
            format!("unsupported rank field `{field}`; only `priority` is available"),
        ));
    }
    let Some(raw_priority) = fields.get("priority") else {
        return Ok((tags, None));
    };
    let priority =
        parse_priority_value(raw_priority).map_err(|reason| ("invalid_rank_value", reason))?;
    let legacy: Vec<&str> = tags
        .iter()
        .filter_map(|(key, value)| (key == "priority").then_some(value.as_str()))
        .collect();
    match legacy.as_slice() {
        [] => tags.push(("priority".to_string(), priority.to_string())),
        [value] if value.parse::<i64>().ok() == Some(priority) => {}
        [_] => {
            return Err((
                "invalid_rank_value",
                "rank_fields.priority conflicts with legacy tags.priority".to_string(),
            ));
        }
        _ => {
            return Err((
                "invalid_rank_value",
                "typed priority requires at most one legacy tags.priority value".to_string(),
            ));
        }
    }
    Ok((tags, Some(reverse_rusty::RankValues { priority })))
}

/// Canonical scalar coercion shared by tag ingest and the filter parsers (ADR-073,
/// closing ADR-064 item 4): a string is itself; a number or bool coerces to its
/// canonical JSON text (`7` → `"7"`, `true` → `"true"`) — the ES keyword behavior.
/// One function serves BOTH sides, so an ingested value and a filter value can never
/// disagree about the coerced form (`7.0` coerces to `"7.0"` on both, exactly as in
/// ES). Returns `None` for null/array/object — no canonical scalar form; the caller
/// decides skip vs reject.
pub(crate) fn coerce_tag_scalar(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The JSON type of a value, for error messages.
pub(crate) fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a bool",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}

/// Extract per-query metadata tags from an ingest body's top-level fields (`PUT /_doc` or a
/// `/_bulk` source line), ES-style (ADR-049). Tags come from a canonical `tags` object
/// **and** any other non-reserved top-level field (ES stores percolator metadata as
/// siblings of `query`). Scalar values coerce canonically ([`coerce_tag_scalar`]); an
/// explicit `null` — top-level or array element — contributes no tag (the ES null
/// semantics: an explicit "no value"); an object or nested array is a hard error
/// (ADR-073: a silently dropped value left the query unreachable by any filter on that
/// key, corrupting filtered percolation invisibly).
pub(crate) fn extract_ingest_tags(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<(String, String)>, String> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut push_kv = |key: &str, v: &serde_json::Value| -> Result<(), String> {
        // An empty tag KEY rejects loudly (the ADR-073 family): an empty `priority_key`
        // means "no priority term" (ADR-075 — the gRPC wire cannot express it), so an
        // empty-key tag could never be consistently reachable by ranking or filtering;
        // accepting it would store a tag only SOME paths can see. Engine-side replay
        // (WAL/cluster log) is untouched — previously stored tags still load.
        if key.is_empty() {
            return Err("tag keys must be non-empty".to_string());
        }
        match v {
            // Explicit "no value" — the key carries no tag (ES null semantics).
            serde_json::Value::Null => Ok(()),
            serde_json::Value::Array(arr) => {
                for (i, e) in arr.iter().enumerate() {
                    if e.is_null() {
                        continue;
                    }
                    match coerce_tag_scalar(e) {
                        Some(s) => out.push((key.to_string(), s)),
                        None => {
                            return Err(format!(
                                "tag {key}[{i}] must be a string, number, bool or null \
                                 (got {})",
                                json_type_name(e)
                            ))
                        }
                    }
                }
                Ok(())
            }
            _ => match coerce_tag_scalar(v) {
                Some(s) => {
                    out.push((key.to_string(), s));
                    Ok(())
                }
                None => Err(format!(
                    "tag '{key}' must be a string, number, bool, null or an array of \
                     those (got {})",
                    json_type_name(v)
                )),
            },
        }
    };
    // canonical `tags` object (an explicit `"tags": null` means "no tags")
    match obj.get("tags") {
        Some(serde_json::Value::Object(tags)) => {
            for (k, v) in tags {
                push_kv(k, v)?;
            }
        }
        Some(serde_json::Value::Null) | None => {}
        Some(other) => {
            return Err(format!(
                "`tags` must be an object of key → value(s) (got {})",
                json_type_name(other)
            ))
        }
    }
    // ES-style sibling fields
    for (k, v) in obj {
        if !RESERVED_INGEST_FIELDS.contains(&k.as_str()) {
            push_kv(k, v)?;
        }
    }
    Ok(out)
}
