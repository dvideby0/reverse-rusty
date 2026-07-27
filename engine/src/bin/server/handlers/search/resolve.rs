//! Request resolution for the percolate endpoints: normalize BOTH the native RR
//! envelope (`document`/`documents` + `filter`) and the ES `bool`/`terms`/`percolate`
//! envelope (`query`) into a uniform `(titles, single, FilterSpec)` triple (ADR-049).
//! Any unsupported ES query node is a hard error — an unsupported filter never
//! silently widens the result set.

use crate::handlers::doc::{coerce_tag_scalar, json_type_name};

use super::DocBody;

/// A request filter: a conjunction of `(key, [values])` groups (ADR-049).
pub(crate) type FilterSpec = Vec<(String, Vec<String>)>;

/// Coerce one filter value through the SAME canonical scalar rule as tag ingest
/// (ADR-073): a number or bool filter value matches the tag its ingest twin
/// produced (`{"priority": 7}` ingested ⇒ `{"priority": 7}` filterable). `null`
/// and structured values are hard errors — a predicate with no canonical scalar
/// form is unanswerable, and silently dropping it would widen the result set.
fn coerce_filter_value(ctx: &str, v: &serde_json::Value) -> Result<String, String> {
    coerce_tag_scalar(v).ok_or_else(|| {
        format!(
            "{ctx} must be a string, number or bool (got {})",
            json_type_name(v)
        )
    })
}

/// Parse the ES `bool.filter` clause list into a [`FilterSpec`]. Each clause is a
/// `{"terms": {key: [values]}}` or `{"term": {key: value}}`; any other clause type is a
/// hard error (so an unsupported filter never silently widens the result set). Accepts a
/// single clause object or an array of them.
fn parse_es_filter(filter: &serde_json::Value, strict: bool) -> Result<FilterSpec, String> {
    let clauses: Vec<&serde_json::Value> = match filter {
        serde_json::Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    let mut spec = FilterSpec::new();
    for clause in clauses {
        let obj = clause
            .as_object()
            .ok_or_else(|| "filter clause must be an object".to_string())?;
        // ES parity + this module's contract: a clause object holds exactly ONE
        // query. Pre-ADR-073 a clause like `{"terms": {...}, "term": {...}}` took
        // the first branch and silently DROPPED the sibling predicate — the
        // widening direction (codex-class review catch).
        if obj.len() != 1 {
            return Err(
                "filter clause must contain exactly one `terms` or `term` query".to_string(),
            );
        }
        if let Some(terms) = obj.get("terms").and_then(|t| t.as_object()) {
            if strict && terms.len() != 1 {
                return Err("`terms` clause must name exactly one field".to_string());
            }
            if terms.is_empty() {
                return Err("`terms` clause must name at least one field".to_string());
            }
            for (k, v) in terms {
                let vals = match v {
                    serde_json::Value::Array(a) => a
                        .iter()
                        .enumerate()
                        .map(|(i, e)| coerce_filter_value(&format!("terms[{k}][{i}]"), e))
                        .collect::<Result<_, _>>()?,
                    other if !strict => {
                        vec![coerce_filter_value(&format!("terms[{k}]"), other)?]
                    }
                    _ => return Err(format!("terms[{k}] must be an array")),
                };
                spec.push((k.clone(), vals));
            }
        } else if let Some(term) = obj.get("term").and_then(|t| t.as_object()) {
            if strict && term.len() != 1 {
                return Err("`term` clause must name exactly one field".to_string());
            }
            if term.is_empty() {
                return Err("`term` clause must name at least one field".to_string());
            }
            for (k, v) in term {
                let val = coerce_filter_value(&format!("term[{k}]"), v)?;
                spec.push((k.clone(), vec![val]));
            }
        } else {
            return Err(
                "unsupported filter clause: only `terms` and `term` are supported".to_string(),
            );
        }
    }
    Ok(spec)
}

/// Parse a native filter block — an object `{key: value|[values], ...}` — into a
/// [`FilterSpec`].
fn parse_native_filter(filter: &serde_json::Value) -> Result<FilterSpec, String> {
    let obj = filter
        .as_object()
        .ok_or_else(|| "`filter` must be an object of key → value(s)".to_string())?;
    let mut spec = FilterSpec::new();
    for (k, v) in obj {
        let vals = match v {
            serde_json::Value::Array(a) => a
                .iter()
                .enumerate()
                .map(|(i, e)| coerce_filter_value(&format!("filter[{k}][{i}]"), e))
                .collect::<Result<_, _>>()?,
            other => vec![coerce_filter_value(&format!("filter[{k}]"), other)?],
        };
        spec.push((k.clone(), vals));
    }
    Ok(spec)
}

/// The percolate documents + tag filter resolved from a request, normalizing BOTH the
/// native RR shape (`document`/`documents` + `filter`) and the ES `bool`/`terms`/`percolate`
/// envelope (`query.bool.must.percolate` + `query.bool.filter`). Returns the titles, whether
/// the request was single-document (drives the response shape), and the filter spec. Any
/// unsupported ES query node is a hard error (never silently ignored).
pub(crate) fn resolve_percolate(
    document: Option<DocBody>,
    documents: Option<Vec<DocBody>>,
    native_filter: Option<serde_json::Value>,
    es_query: Option<serde_json::Value>,
) -> Result<(Vec<String>, bool, FilterSpec), String> {
    resolve_percolate_with_mode(document, documents, native_filter, es_query, false)
}

/// Strict compatibility resolver. `GET|POST /_search` (ADR-126) and
/// `POST /_mpercolate` (ADR-135) opt into these ES/OS subset checks; internal
/// v2/job request lowering retains its separately validated established envelope.
pub(crate) fn resolve_percolate_strict(
    document: Option<DocBody>,
    documents: Option<Vec<DocBody>>,
    native_filter: Option<serde_json::Value>,
    es_query: Option<serde_json::Value>,
) -> Result<(Vec<String>, bool, FilterSpec), String> {
    resolve_percolate_with_mode(document, documents, native_filter, es_query, true)
}

fn resolve_percolate_with_mode(
    document: Option<DocBody>,
    documents: Option<Vec<DocBody>>,
    native_filter: Option<serde_json::Value>,
    es_query: Option<serde_json::Value>,
    strict: bool,
) -> Result<(Vec<String>, bool, FilterSpec), String> {
    if let Some(q) = es_query {
        if strict && (document.is_some() || documents.is_some() || native_filter.is_some()) {
            return Err(
                "`query` cannot be combined with native `document`, `documents`, or `filter`"
                    .to_string(),
            );
        }
        return resolve_es_query(&q, strict);
    }
    let mut filter = FilterSpec::new();
    if let Some(f) = native_filter {
        filter = parse_native_filter(&f)?;
    }
    match (document, documents) {
        (Some(_), Some(_)) if strict => {
            Err("request must contain exactly one of `document` or `documents`".to_string())
        }
        (Some(d), _) => Ok((vec![d.title], true, filter)),
        (None, Some(ds)) => Ok((ds.into_iter().map(|d| d.title).collect(), false, filter)),
        (None, None) => Err("request must include 'document' or 'documents'".to_string()),
    }
}

/// Resolve the ES percolate envelope: `{query:{bool:{must:{percolate:{document(s)}}, filter:[…]}}}`
/// or the bare `{query:{percolate:{document(s)}}}`. Only the percolate + bool.filter(terms/term)
/// subset is supported.
fn resolve_es_query(
    query: &serde_json::Value,
    strict: bool,
) -> Result<(Vec<String>, bool, FilterSpec), String> {
    let obj = query
        .as_object()
        .ok_or_else(|| "`query` must be an object".to_string())?;
    if strict && obj.len() != 1 {
        return Err("`query` must contain exactly one `percolate` or `bool` clause".to_string());
    }
    let (percolate, filter) = if let Some(b) = obj.get("bool") {
        let b = b
            .as_object()
            .ok_or_else(|| "`query.bool` must be an object".to_string())?;
        if strict && b.keys().any(|key| key != "must" && key != "filter") {
            return Err("`query.bool` supports only `must` and `filter`".to_string());
        }
        // must → the percolate clause (single object or a one-element array)
        let must = b
            .get("must")
            .ok_or_else(|| "`query.bool` must contain a `must` percolate clause".to_string())?;
        let must_clause = match must {
            serde_json::Value::Array(a) if a.len() == 1 => &a[0],
            serde_json::Value::Array(_) => {
                return Err("only a single `percolate` clause is supported in `must`".to_string())
            }
            obj => obj,
        };
        let must_obj = must_clause
            .as_object()
            .ok_or_else(|| "`query.bool.must` must be an object".to_string())?;
        if strict && must_obj.len() != 1 {
            return Err(
                "`query.bool.must` must contain exactly one `percolate` clause".to_string(),
            );
        }
        let percolate = must_obj
            .get("percolate")
            .ok_or_else(|| "`query.bool.must` must be a `percolate` clause".to_string())?;
        let filter = match b.get("filter") {
            Some(f) => parse_es_filter(f, strict)?,
            None => FilterSpec::new(),
        };
        (percolate, filter)
    } else if let Some(p) = obj.get("percolate") {
        (p, FilterSpec::new())
    } else {
        return Err("`query` must be a `percolate` or `bool` percolate clause".to_string());
    };
    let (titles, single) = extract_percolate_docs(percolate, strict)?;
    Ok((titles, single, filter))
}

/// Pull the document(s) out of an ES `percolate` clause (`{field, document}` or
/// `{field, documents}`). Reverse Rusty's sole stored-query field is named `query`.
fn extract_percolate_docs(
    percolate: &serde_json::Value,
    strict: bool,
) -> Result<(Vec<String>, bool), String> {
    let p = percolate
        .as_object()
        .ok_or_else(|| "`percolate` must be an object".to_string())?;
    if strict
        && p.keys()
            .any(|key| key != "field" && key != "document" && key != "documents")
    {
        return Err("`percolate` supports only `field`, `document`, and `documents`".to_string());
    }
    if strict {
        match p.get("field").and_then(serde_json::Value::as_str) {
            Some("query") => {}
            Some(other) => {
                return Err(format!("`percolate.field` must be `query` (got `{other}`)"))
            }
            None => return Err("`percolate.field` must be the string `query`".to_string()),
        }
    }
    let title_of = |doc: &serde_json::Value| -> Result<String, String> {
        let object = doc
            .as_object()
            .ok_or_else(|| "percolate document must be an object".to_string())?;
        if strict && object.len() != 1 {
            return Err("percolate document must contain only a string `title`".to_string());
        }
        object
            .get("title")
            .and_then(|t| t.as_str())
            .map(str::to_string)
            .ok_or_else(|| "percolate document must have a string `title`".to_string())
    };
    match (p.get("document"), p.get("documents")) {
        (Some(doc), None) => Ok((vec![title_of(doc)?], true)),
        (None, Some(docs)) => {
            let docs = docs
                .as_array()
                .ok_or_else(|| "`percolate.documents` must be an array".to_string())?;
            Ok((docs.iter().map(title_of).collect::<Result<_, _>>()?, false))
        }
        (Some(doc), Some(_)) if !strict => Ok((vec![title_of(doc)?], true)),
        (Some(_), Some(_)) | (None, None) => {
            Err("`percolate` must contain exactly one of `document` or `documents`".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compatibility_strictness_does_not_change_shared_percolate_resolution() {
        let query = serde_json::json!({
            "percolate": {
                "field": "legacy_name",
                "document": {"title": "topps chrome", "sku": "ABC-1"},
                "legacy_option": true
            },
            "legacy_sibling": {}
        });

        let (titles, single, _) =
            resolve_percolate(None, None, None, Some(query.clone())).expect("legacy resolver");
        assert_eq!(titles, vec!["topps chrome"]);
        assert!(single);
        assert!(
            resolve_percolate_strict(None, None, None, Some(query)).is_err(),
            "compatibility search must reject the same unsupported siblings"
        );
    }
}
