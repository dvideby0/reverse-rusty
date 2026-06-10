//! Request resolution for the percolate endpoints: normalize BOTH the native RR
//! envelope (`document`/`documents` + `filter`) and the ES `bool`/`terms`/`percolate`
//! envelope (`query`) into a uniform `(titles, single, FilterSpec)` triple (ADR-049).
//! Any unsupported ES query node is a hard error — an unsupported filter never
//! silently widens the result set.

use super::DocBody;

/// A request filter: a conjunction of `(key, [values])` groups (ADR-049).
pub(crate) type FilterSpec = Vec<(String, Vec<String>)>;

/// Parse the ES `bool.filter` clause list into a [`FilterSpec`]. Each clause is a
/// `{"terms": {key: [values]}}` or `{"term": {key: value}}`; any other clause type is a
/// hard error (so an unsupported filter never silently widens the result set). Accepts a
/// single clause object or an array of them.
fn parse_es_filter(filter: &serde_json::Value) -> Result<FilterSpec, String> {
    let clauses: Vec<&serde_json::Value> = match filter {
        serde_json::Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    let mut spec = FilterSpec::new();
    for clause in clauses {
        let obj = clause
            .as_object()
            .ok_or_else(|| "filter clause must be an object".to_string())?;
        if let Some(terms) = obj.get("terms").and_then(|t| t.as_object()) {
            for (k, v) in terms {
                let vals = match v {
                    serde_json::Value::Array(a) => a
                        .iter()
                        .filter_map(|e| e.as_str().map(str::to_string))
                        .collect(),
                    serde_json::Value::String(s) => vec![s.clone()],
                    _ => return Err(format!("terms[{k}] must be a string or array of strings")),
                };
                spec.push((k.clone(), vals));
            }
        } else if let Some(term) = obj.get("term").and_then(|t| t.as_object()) {
            for (k, v) in term {
                let val = v
                    .as_str()
                    .ok_or_else(|| format!("term[{k}] must be a string"))?;
                spec.push((k.clone(), vec![val.to_string()]));
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
            serde_json::Value::String(s) => vec![s.clone()],
            serde_json::Value::Array(a) => a
                .iter()
                .filter_map(|e| e.as_str().map(str::to_string))
                .collect(),
            _ => return Err(format!("filter[{k}] must be a string or array of strings")),
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
    if let Some(q) = es_query {
        return resolve_es_query(&q);
    }
    let mut filter = FilterSpec::new();
    if let Some(f) = native_filter {
        filter = parse_native_filter(&f)?;
    }
    match (document, documents) {
        (Some(d), _) => Ok((vec![d.title], true, filter)),
        (None, Some(ds)) => Ok((ds.into_iter().map(|d| d.title).collect(), false, filter)),
        (None, None) => Err("request must include 'document' or 'documents'".to_string()),
    }
}

/// Resolve the ES percolate envelope: `{query:{bool:{must:{percolate:{document(s)}}, filter:[…]}}}`
/// or the bare `{query:{percolate:{document(s)}}}`. Only the percolate + bool.filter(terms/term)
/// subset is supported.
fn resolve_es_query(query: &serde_json::Value) -> Result<(Vec<String>, bool, FilterSpec), String> {
    let obj = query
        .as_object()
        .ok_or_else(|| "`query` must be an object".to_string())?;
    let (percolate, filter) = if let Some(b) = obj.get("bool") {
        let b = b
            .as_object()
            .ok_or_else(|| "`query.bool` must be an object".to_string())?;
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
        let percolate = must_clause
            .get("percolate")
            .ok_or_else(|| "`query.bool.must` must be a `percolate` clause".to_string())?;
        let filter = match b.get("filter") {
            Some(f) => parse_es_filter(f)?,
            None => FilterSpec::new(),
        };
        (percolate, filter)
    } else if let Some(p) = obj.get("percolate") {
        (p, FilterSpec::new())
    } else {
        return Err("`query` must be a `percolate` or `bool` percolate clause".to_string());
    };
    let (titles, single) = extract_percolate_docs(percolate)?;
    Ok((titles, single, filter))
}

/// Pull the document(s) out of an ES `percolate` clause (`{field, document}` or
/// `{field, documents}`); `field` is accepted but ignored (RR has one query field).
fn extract_percolate_docs(percolate: &serde_json::Value) -> Result<(Vec<String>, bool), String> {
    let p = percolate
        .as_object()
        .ok_or_else(|| "`percolate` must be an object".to_string())?;
    let title_of = |doc: &serde_json::Value| -> Result<String, String> {
        doc.get("title")
            .and_then(|t| t.as_str())
            .map(str::to_string)
            .ok_or_else(|| "percolate document must have a string `title`".to_string())
    };
    if let Some(doc) = p.get("document") {
        Ok((vec![title_of(doc)?], true))
    } else if let Some(docs) = p.get("documents").and_then(|d| d.as_array()) {
        Ok((docs.iter().map(title_of).collect::<Result<_, _>>()?, false))
    } else {
        Err("`percolate` must contain `document` or `documents`".to_string())
    }
}
