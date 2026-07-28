//! Shared parsing and rendering for compact-and-aligned-text (CAT) tables.
//!
//! Endpoint modules own their schemas and any endpoint-specific controls. This
//! module owns the common Elasticsearch/OpenSearch mechanics: text versus JSON,
//! verbose headers, column selection (including aliases and simple wildcards),
//! schema help, stable multi-column sorting, alignment, and ordered JSON keys.

use std::cmp::Ordering;

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{ser::SerializeMap, Serialize, Serializer};

#[derive(Clone, Copy)]
pub(crate) enum CatAlignment {
    Left,
    Right,
}

#[derive(Clone, Copy)]
pub(crate) struct CatColumn {
    name: &'static str,
    aliases: &'static [&'static str],
    description: &'static str,
    alignment: CatAlignment,
}

impl CatColumn {
    pub(crate) const fn new(
        name: &'static str,
        aliases: &'static [&'static str],
        description: &'static str,
        alignment: CatAlignment,
    ) -> Self {
        Self {
            name,
            aliases,
            description,
            alignment,
        }
    }

    fn matches(self, selector: &str) -> bool {
        wildcard_matches(selector, self.name)
            || self
                .aliases
                .iter()
                .any(|alias| wildcard_matches(selector, alias))
    }

    fn is_named(self, selector: &str) -> bool {
        selector == self.name || self.aliases.contains(&selector)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CatFormat {
    Text,
    Json,
}

#[derive(Clone, Copy)]
struct SortSpec {
    column: usize,
    descending: bool,
}

pub(crate) struct CatRequest {
    format: CatFormat,
    verbose: bool,
    columns: Vec<usize>,
    help: bool,
    sort: Vec<SortSpec>,
}

impl CatRequest {
    pub(crate) const fn is_help(&self) -> bool {
        self.help
    }
}

#[derive(Clone)]
enum CatSortValue {
    Text(String),
    Unsigned(u64),
    Decimal(f64),
    Boolean(bool),
}

impl CatSortValue {
    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Text(left), Self::Text(right)) => left.cmp(right),
            (Self::Unsigned(left), Self::Unsigned(right)) => left.cmp(right),
            (Self::Decimal(left), Self::Decimal(right)) => {
                left.partial_cmp(right).unwrap_or(Ordering::Equal)
            }
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            // A schema column always uses one sort type. Keep this total if an
            // internal caller accidentally mixes types instead of panicking.
            _ => self.kind_rank().cmp(&other.kind_rank()),
        }
    }

    const fn kind_rank(&self) -> u8 {
        match self {
            Self::Text(_) => 0,
            Self::Unsigned(_) => 1,
            Self::Decimal(_) => 2,
            Self::Boolean(_) => 3,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CatCell {
    display: String,
    sort: CatSortValue,
}

impl CatCell {
    pub(crate) fn text(value: impl Into<String>) -> Self {
        let display = value.into();
        Self {
            sort: CatSortValue::Text(display.clone()),
            display,
        }
    }

    pub(crate) fn unsigned(value: impl Into<u64>) -> Self {
        let value = value.into();
        Self {
            display: value.to_string(),
            sort: CatSortValue::Unsigned(value),
        }
    }

    pub(crate) fn unsigned_display(display: impl Into<String>, value: u64) -> Self {
        Self {
            display: display.into(),
            sort: CatSortValue::Unsigned(value),
        }
    }

    pub(crate) fn decimal(display: impl Into<String>, value: f64) -> Self {
        Self {
            display: display.into(),
            sort: CatSortValue::Decimal(value),
        }
    }

    pub(crate) fn boolean(value: bool) -> Self {
        Self {
            display: value.to_string(),
            sort: CatSortValue::Boolean(value),
        }
    }
}

#[derive(Clone)]
pub(crate) struct CatRow {
    cells: Vec<CatCell>,
}

impl CatRow {
    pub(crate) fn new<const N: usize>(cells: [CatCell; N]) -> Self {
        Self {
            cells: Vec::from(cells),
        }
    }

    fn field(&self, column: usize) -> &str {
        &self.cells[column].display
    }

    fn compare(&self, other: &Self, column: usize) -> Ordering {
        self.cells[column].sort.compare(&other.cells[column].sort)
    }
}

struct JsonRow<'a> {
    row: &'a CatRow,
    columns: &'a [usize],
    schema: &'static [CatColumn],
}

impl Serialize for JsonRow<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for &column in self.columns {
            map.serialize_entry(self.schema[column].name, self.row.field(column))?;
        }
        map.end()
    }
}

pub(crate) fn resolve_request(
    endpoint: &'static str,
    schema: &'static [CatColumn],
    format: Option<&str>,
    verbose: Option<&str>,
    columns: Option<&str>,
    help: Option<&str>,
    sort: Option<&str>,
) -> Result<CatRequest, String> {
    let format = match format {
        None => CatFormat::Text,
        Some("json") => CatFormat::Json,
        Some(other) => {
            return Err(format!(
                "unsupported {endpoint} format `{other}`; supported: json"
            ))
        }
    };
    let verbose_enabled = parse_flag(endpoint, "v", verbose)?;
    let help_enabled = parse_flag(endpoint, "help", help)?;
    if help_enabled && (verbose.is_some() || columns.is_some() || sort.is_some()) {
        return Err(format!(
            "{endpoint} help cannot be combined with v, h, or s"
        ));
    }
    Ok(CatRequest {
        format,
        verbose: verbose_enabled,
        columns: parse_columns(endpoint, schema, columns)?,
        help: help_enabled,
        sort: parse_sort(endpoint, schema, sort)?,
    })
}

fn parse_flag(
    endpoint: &'static str,
    name: &'static str,
    raw: Option<&str>,
) -> Result<bool, String> {
    match raw {
        None | Some("false") => Ok(false),
        Some("" | "true") => Ok(true),
        Some(value) => Err(format!(
            "{endpoint} parameter `{name}` must be true, false, or a bare flag; got `{value}`"
        )),
    }
}

fn parse_columns(
    endpoint: &'static str,
    schema: &'static [CatColumn],
    raw: Option<&str>,
) -> Result<Vec<usize>, String> {
    let Some(raw) = raw else {
        return Ok((0..schema.len()).collect());
    };
    if raw.is_empty() {
        return Err(format!(
            "{endpoint} parameter `h` must select at least one column"
        ));
    }
    let mut columns = Vec::new();
    for selector in raw.split(',') {
        let matched: Vec<usize> = schema
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.matches(selector).then_some(index))
            .collect();
        if matched.is_empty() {
            return Err(format!("unknown {endpoint} column selector `{selector}`"));
        }
        for column in matched {
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
    }
    Ok(columns)
}

fn parse_sort(
    endpoint: &'static str,
    schema: &'static [CatColumn],
    raw: Option<&str>,
) -> Result<Vec<SortSpec>, String> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.is_empty() {
        return Err(format!(
            "{endpoint} parameter `s` must name at least one column"
        ));
    }
    raw.split(',')
        .map(|item| {
            let (selector, descending) = match item.rsplit_once(':') {
                Some((selector, "asc")) => (selector, false),
                Some((selector, "desc")) => (selector, true),
                Some((_, direction)) => {
                    return Err(format!("unknown {endpoint} sort direction `{direction}`"))
                }
                None => (item, false),
            };
            let column = schema
                .iter()
                .position(|column| column.is_named(selector))
                .ok_or_else(|| format!("unknown {endpoint} sort column `{selector}`"))?;
            Ok(SortSpec { column, descending })
        })
        .collect()
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let (mut pattern_index, mut value_index) = (0usize, 0usize);
    let (mut star, mut retry_value) = (None, 0usize);
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    while value_index < value.len() {
        if pattern_index < pattern.len() && pattern[pattern_index] == value[value_index] {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star = Some(pattern_index);
            pattern_index += 1;
            retry_value = value_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            retry_value += 1;
            value_index = retry_value;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

pub(crate) fn render_rows(
    rows: &mut [CatRow],
    request: &CatRequest,
    schema: &'static [CatColumn],
) -> Response {
    debug_assert!(rows.iter().all(|row| row.cells.len() == schema.len()));
    sort_rows(rows, &request.sort);
    match request.format {
        CatFormat::Text => {
            text_response(render_text(rows, &request.columns, request.verbose, schema))
        }
        CatFormat::Json => json_response(rows, &request.columns, schema),
    }
}

fn sort_rows(rows: &mut [CatRow], sort: &[SortSpec]) {
    rows.sort_by(|left, right| {
        for spec in sort {
            let ordering = left.compare(right, spec.column);
            let ordering = if spec.descending {
                ordering.reverse()
            } else {
                ordering
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

fn render_text(
    rows: &[CatRow],
    columns: &[usize],
    verbose: bool,
    schema: &'static [CatColumn],
) -> String {
    let widths: Vec<usize> = columns
        .iter()
        .map(|&column| {
            rows.iter()
                .map(|row| row.field(column).len())
                .chain(verbose.then_some(schema[column].name.len()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut output = String::new();
    if verbose {
        push_text_row(
            &mut output,
            &columns
                .iter()
                .map(|&column| schema[column].name)
                .collect::<Vec<_>>(),
            columns,
            &widths,
            schema,
        );
    }
    for row in rows {
        push_text_row(
            &mut output,
            &columns
                .iter()
                .map(|&column| row.field(column))
                .collect::<Vec<_>>(),
            columns,
            &widths,
            schema,
        );
    }
    output
}

fn push_text_row(
    output: &mut String,
    fields: &[&str],
    columns: &[usize],
    widths: &[usize],
    schema: &'static [CatColumn],
) {
    for (index, ((field, &column), width)) in fields.iter().zip(columns).zip(widths).enumerate() {
        if index > 0 {
            output.push(' ');
        }
        match schema[column].alignment {
            CatAlignment::Right => output.push_str(&format!("{field:>width$}")),
            CatAlignment::Left if index + 1 < fields.len() => {
                output.push_str(&format!("{field:<width$}"));
            }
            CatAlignment::Left => output.push_str(field),
        }
    }
    output.push('\n');
}

fn json_response(rows: &[CatRow], columns: &[usize], schema: &'static [CatColumn]) -> Response {
    let values: Vec<JsonRow<'_>> = rows
        .iter()
        .map(|row| JsonRow {
            row,
            columns,
            schema,
        })
        .collect();
    Json(values).into_response()
}

pub(crate) fn render_help(request: &CatRequest, schema: &'static [CatColumn]) -> Response {
    match request.format {
        CatFormat::Text => {
            let name_width = schema
                .iter()
                .map(|column| column.name.len())
                .max()
                .unwrap_or(0)
                .max(8);
            let alias_width = schema
                .iter()
                .map(|column| column.aliases.join(",").len())
                .max()
                .unwrap_or(0)
                .max(8);
            let mut output = String::new();
            for column in schema {
                output.push_str(&format!(
                    "{:<name_width$} | {:<alias_width$} | {}\n",
                    column.name,
                    column.aliases.join(","),
                    column.description
                ));
            }
            text_response(output)
        }
        CatFormat::Json => {
            let rows: Vec<serde_json::Value> = schema
                .iter()
                .map(|column| {
                    serde_json::json!({
                        "name": column.name,
                        "aliases": column.aliases,
                        "description": column.description,
                    })
                })
                .collect();
            Json(rows).into_response()
        }
    }
}

fn text_response(body: String) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::wildcard_matches;

    #[test]
    fn column_wildcards_are_exact() {
        assert!(wildcard_matches("*", "metric"));
        assert!(wildcard_matches("met*", "metric"));
        assert!(wildcard_matches("*tric", "metric"));
        assert!(!wildcard_matches("val*", "metric"));
    }
}
