use serde::Deserialize;

use super::super::{
    extract_ranked_ingest, BulkItemError, Bytes, HeaderMap, RefreshPolicy, QUERY_INDEX,
};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BulkParams {
    refresh: Option<RefreshPolicy>,
    require_alias: Option<bool>,
}

impl BulkParams {
    fn validate(self) -> Result<(), BulkRequestError> {
        // Every acknowledged Reverse Rusty mutation is published before the
        // response, so all three validated policies receive the stronger
        // immediate-visibility guarantee.
        let _ = self.refresh;
        if self.require_alias.unwrap_or(false) {
            return Err(BulkRequestError::validation(
                "`require_alias=true` is unsupported because Reverse Rusty exposes one implicit \
                 `queries` index and no index aliases",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BulkActionKind {
    Index,
    Create,
}

impl BulkActionKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Index => "index",
            Self::Create => "create",
        }
    }
}

pub(crate) struct BulkSource {
    pub(crate) query: String,
    pub(crate) version: u32,
    pub(crate) tags: Vec<(String, String)>,
    pub(crate) rank: Option<reverse_rusty::RankValues>,
}

pub(crate) struct ParsedBulkItem {
    pub(crate) action: BulkActionKind,
    pub(crate) id: u64,
    pub(crate) source: Result<BulkSource, BulkItemError>,
}

pub(crate) struct BulkRequestError {
    pub(crate) status: axum::http::StatusCode,
    pub(crate) error_type: &'static str,
    pub(crate) reason: String,
}

impl BulkRequestError {
    fn validation(reason: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::BAD_REQUEST,
            error_type: "validation_error",
            reason: reason.into(),
        }
    }

    fn media_type(reason: impl Into<String>) -> Self {
        Self {
            status: axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_type: "unsupported_media_type",
            reason: reason.into(),
        }
    }
}

fn item_error(error_type: &'static str, reason: impl Into<String>) -> BulkItemError {
    BulkItemError {
        error_type,
        reason: reason.into(),
    }
}

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn validate_content_type(headers: &HeaderMap) -> Result<(), BulkRequestError> {
    let raw = headers
        .get(axum::http::header::CONTENT_TYPE)
        .ok_or_else(|| {
            BulkRequestError::media_type(
                "POST /_bulk requires Content-Type application/x-ndjson or application/json",
            )
        })?;
    let value = raw.to_str().map_err(|_| {
        BulkRequestError::media_type("POST /_bulk received a non-UTF-8 Content-Type header")
    })?;
    let media_type = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if media_type == "application/x-ndjson" || media_type == "application/json" {
        Ok(())
    } else {
        Err(BulkRequestError::media_type(
            "Content-Type must be application/x-ndjson or application/json",
        ))
    }
}

fn parse_id(value: Option<&serde_json::Value>, line: usize) -> Result<u64, BulkRequestError> {
    let value = value.ok_or_else(|| {
        BulkRequestError::validation(format!("bulk action on line {line} requires `_id`"))
    })?;
    match value {
        serde_json::Value::Number(number) => number.as_u64().ok_or_else(|| {
            BulkRequestError::validation(format!(
                "bulk action `_id` on line {line} must be an unsigned 64-bit integer"
            ))
        }),
        serde_json::Value::String(value) => value.parse::<u64>().map_err(|_| {
            BulkRequestError::validation(format!(
                "bulk action `_id` on line {line} must be a decimal unsigned 64-bit integer"
            ))
        }),
        _ => Err(BulkRequestError::validation(format!(
            "bulk action `_id` on line {line} must be an integer or decimal string"
        ))),
    }
}

fn validate_action_metadata(
    metadata: &serde_json::Map<String, serde_json::Value>,
    line: usize,
) -> Result<u64, BulkRequestError> {
    if let Some(field) = metadata.keys().find(|field| {
        !matches!(
            field.as_str(),
            "_id" | "_index" | "require_alias" | "_require_alias"
        )
    }) {
        return Err(BulkRequestError::validation(format!(
            "unsupported bulk action metadata field `{field}` on line {line}"
        )));
    }
    if let Some(index) = metadata.get("_index") {
        if index.as_str() != Some(QUERY_INDEX) {
            return Err(BulkRequestError::validation(format!(
                "bulk action `_index` on line {line} must be `{QUERY_INDEX}`"
            )));
        }
    }
    if metadata.contains_key("require_alias") && metadata.contains_key("_require_alias") {
        return Err(BulkRequestError::validation(format!(
            "`require_alias` and `_require_alias` are aliases; specify at most one on line {line}"
        )));
    }
    if let Some(require_alias) = metadata
        .get("require_alias")
        .or_else(|| metadata.get("_require_alias"))
    {
        match require_alias.as_bool() {
            Some(false) => {}
            Some(true) => {
                return Err(BulkRequestError::validation(format!(
                    "bulk action on line {line} cannot require an index alias; Reverse Rusty \
                     exposes only the implicit `{QUERY_INDEX}` index"
                )));
            }
            None => {
                return Err(BulkRequestError::validation(format!(
                    "bulk action alias requirement on line {line} must be a boolean"
                )));
            }
        }
    }
    parse_id(metadata.get("_id"), line)
}

fn parse_version(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<u32, BulkItemError> {
    let Some(value) = object.get("version") else {
        return Ok(1);
    };
    let version = value.as_u64().ok_or_else(|| {
        item_error(
            "invalid_version",
            "source `version` must be an unsigned 32-bit integer",
        )
    })?;
    u32::try_from(version).map_err(|_| {
        item_error(
            "invalid_version",
            "source `version` must fit an unsigned 32-bit integer",
        )
    })
}

fn parse_source(line: &[u8], line_number: usize) -> Result<BulkSource, BulkItemError> {
    let value: serde_json::Value = serde_json::from_slice(line).map_err(|error| {
        item_error(
            "document_parsing_exception",
            format!("invalid source JSON on line {line_number}: {error}"),
        )
    })?;
    let object = value.as_object().ok_or_else(|| {
        item_error(
            "document_parsing_exception",
            format!("bulk source on line {line_number} must be a JSON object"),
        )
    })?;
    let query = object
        .get("query")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            item_error(
                "document_parsing_exception",
                format!("bulk source on line {line_number} requires a string `query` field"),
            )
        })?
        .to_string();
    let version = parse_version(object)?;
    let (tags, rank) = extract_ranked_ingest(object)
        .map_err(|(error_type, reason)| item_error(error_type, reason))?;
    Ok(BulkSource {
        query,
        version,
        tags,
        rank,
    })
}

pub(crate) fn parse_bulk_request(
    headers: &HeaderMap,
    bytes: &Bytes,
    params: BulkParams,
) -> Result<Vec<ParsedBulkItem>, BulkRequestError> {
    validate_content_type(headers)?;
    params.validate()?;
    if bytes.is_empty() {
        return Err(BulkRequestError::validation(
            "bulk request body must contain at least one action/source pair",
        ));
    }
    if !bytes.ends_with(b"\n") {
        return Err(BulkRequestError::validation(
            "bulk request body must end with a newline",
        ));
    }

    let mut lines: Vec<&[u8]> = bytes.split(|byte| *byte == b'\n').collect();
    let _terminal = lines.pop();
    if let Some((offset, _)) = lines
        .iter()
        .enumerate()
        .find(|(_, line)| trim_ascii(line).is_empty())
    {
        return Err(BulkRequestError::validation(format!(
            "blank NDJSON line {} is not allowed",
            offset + 1
        )));
    }

    let mut items = Vec::new();
    let mut cursor = 0usize;
    while cursor < lines.len() {
        let action_line_number = cursor + 1;
        let action_value: serde_json::Value =
            serde_json::from_slice(lines[cursor]).map_err(|error| {
                BulkRequestError::validation(format!(
                    "invalid bulk action JSON on line {action_line_number}: {error}"
                ))
            })?;
        cursor += 1;
        let action_object = action_value.as_object().ok_or_else(|| {
            BulkRequestError::validation(format!(
                "bulk action on line {action_line_number} must be a JSON object"
            ))
        })?;
        if action_object.len() != 1 {
            return Err(BulkRequestError::validation(format!(
                "bulk action on line {action_line_number} must contain exactly one operation"
            )));
        }
        let (operation, metadata_value) = action_object
            .iter()
            .next()
            .ok_or_else(|| BulkRequestError::validation("bulk action cannot be empty"))?;
        let action = match operation.as_str() {
            "index" => BulkActionKind::Index,
            "create" => BulkActionKind::Create,
            other => {
                return Err(BulkRequestError::validation(format!(
                    "unsupported bulk operation `{other}` on line {action_line_number}; \
                     supported operations are `index` and `create`"
                )));
            }
        };
        let metadata = metadata_value.as_object().ok_or_else(|| {
            BulkRequestError::validation(format!(
                "bulk action metadata on line {action_line_number} must be a JSON object"
            ))
        })?;
        let id = validate_action_metadata(metadata, action_line_number)?;
        if cursor >= lines.len() {
            return Err(BulkRequestError::validation(format!(
                "bulk `{}` action on line {action_line_number} is missing its source line",
                action.name()
            )));
        }
        let source_line_number = cursor + 1;
        let source = parse_source(lines[cursor], source_line_number);
        cursor += 1;
        items.push(ParsedBulkItem { action, id, source });
    }
    Ok(items)
}
