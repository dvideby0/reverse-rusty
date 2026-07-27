//! CAT stats query parsing and two-column table rendering.

use std::cmp::Ordering;

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatStatsParams {
    format: Option<String>,
    v: Option<String>,
    h: Option<String>,
    help: Option<String>,
    s: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CatFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CatColumn {
    Metric,
    Value,
}

impl CatColumn {
    const ALL: [Self; 2] = [Self::Metric, Self::Value];

    const fn name(self) -> &'static str {
        match self {
            Self::Metric => "metric",
            Self::Value => "value",
        }
    }

    const fn aliases(self) -> &'static [&'static str] {
        match self {
            Self::Metric => &["m"],
            Self::Value => &["v"],
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::Metric => "native Reverse Rusty statistic name",
            Self::Value => "statistic value (byte fields use raw bytes)",
        }
    }

    fn matches(self, selector: &str) -> bool {
        wildcard_matches(selector, self.name())
            || self
                .aliases()
                .iter()
                .any(|alias| wildcard_matches(selector, alias))
    }
}

#[derive(Clone, Copy)]
struct SortSpec {
    column: CatColumn,
    descending: bool,
}

pub(super) struct CatRequest {
    format: CatFormat,
    verbose: bool,
    columns: Vec<CatColumn>,
    help: bool,
    sort: Vec<SortSpec>,
}

impl CatRequest {
    pub(super) const fn is_help(&self) -> bool {
        self.help
    }
}

#[derive(Clone)]
pub(super) struct CatRow {
    metric: String,
    value: String,
}

impl CatRow {
    pub(super) fn new(metric: impl Into<String>, value: String) -> Self {
        Self {
            metric: metric.into(),
            value,
        }
    }

    fn field(&self, column: CatColumn) -> &str {
        match column {
            CatColumn::Metric => &self.metric,
            CatColumn::Value => &self.value,
        }
    }
}

struct JsonRow<'a> {
    row: &'a CatRow,
    columns: &'a [CatColumn],
}

impl Serialize for JsonRow<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for &column in self.columns {
            map.serialize_entry(column.name(), self.row.field(column))?;
        }
        map.end()
    }
}

impl CatStatsParams {
    pub(super) fn resolve(self) -> Result<CatRequest, String> {
        let format = match self.format.as_deref() {
            None => CatFormat::Text,
            Some("json") => CatFormat::Json,
            Some(other) => {
                return Err(format!(
                    "unsupported CAT stats format `{other}`; supported: json"
                ))
            }
        };
        let verbose = parse_flag("v", self.v.as_deref())?;
        let help = parse_flag("help", self.help.as_deref())?;
        if help && (self.v.is_some() || self.h.is_some() || self.s.is_some()) {
            return Err("CAT stats help cannot be combined with v, h, or s".to_string());
        }
        let columns = parse_columns(self.h.as_deref())?;
        let sort = parse_sort(self.s.as_deref())?;
        Ok(CatRequest {
            format,
            verbose,
            columns,
            help,
            sort,
        })
    }
}

fn parse_flag(name: &'static str, raw: Option<&str>) -> Result<bool, String> {
    match raw {
        None | Some("false") => Ok(false),
        Some("" | "true") => Ok(true),
        Some(value) => Err(format!(
            "CAT stats parameter `{name}` must be true, false, or a bare flag; got `{value}`"
        )),
    }
}

fn parse_columns(raw: Option<&str>) -> Result<Vec<CatColumn>, String> {
    let Some(raw) = raw else {
        return Ok(CatColumn::ALL.to_vec());
    };
    if raw.is_empty() {
        return Err("CAT stats parameter `h` must select at least one column".to_string());
    }
    let mut columns = Vec::new();
    for selector in raw.split(',') {
        let matched: Vec<CatColumn> = CatColumn::ALL
            .into_iter()
            .filter(|column| column.matches(selector))
            .collect();
        if matched.is_empty() {
            return Err(format!("unknown CAT stats column selector `{selector}`"));
        }
        for column in matched {
            if !columns.contains(&column) {
                columns.push(column);
            }
        }
    }
    Ok(columns)
}

fn parse_sort(raw: Option<&str>) -> Result<Vec<SortSpec>, String> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.is_empty() {
        return Err("CAT stats parameter `s` must name at least one column".to_string());
    }
    raw.split(',')
        .map(|item| {
            let (selector, descending) = match item.rsplit_once(':') {
                Some((selector, "asc")) => (selector, false),
                Some((selector, "desc")) => (selector, true),
                Some((_, direction)) => {
                    return Err(format!("unknown CAT stats sort direction `{direction}`"))
                }
                None => (item, false),
            };
            let column = CatColumn::ALL
                .into_iter()
                .find(|column| selector == column.name() || column.aliases().contains(&selector))
                .ok_or_else(|| format!("unknown CAT stats sort column `{selector}`"))?;
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

pub(super) fn render_rows(rows: &mut [CatRow], request: &CatRequest) -> Response {
    sort_rows(rows, &request.sort);
    match request.format {
        CatFormat::Text => text_response(render_text(rows, &request.columns, request.verbose)),
        CatFormat::Json => json_response(rows, &request.columns),
    }
}

fn sort_rows(rows: &mut [CatRow], sort: &[SortSpec]) {
    rows.sort_by(|left, right| {
        for spec in sort {
            let ordering = left.field(spec.column).cmp(right.field(spec.column));
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

fn render_text(rows: &[CatRow], columns: &[CatColumn], verbose: bool) -> String {
    let widths: Vec<usize> = columns
        .iter()
        .map(|&column| {
            rows.iter()
                .map(|row| row.field(column).len())
                .chain(verbose.then_some(column.name().len()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut output = String::new();
    if verbose {
        push_text_row(
            &mut output,
            columns
                .iter()
                .map(|column| column.name())
                .collect::<Vec<_>>()
                .as_slice(),
            &widths,
        );
    }
    for row in rows {
        push_text_row(
            &mut output,
            columns
                .iter()
                .map(|&column| row.field(column))
                .collect::<Vec<_>>()
                .as_slice(),
            &widths,
        );
    }
    output
}

fn push_text_row(output: &mut String, fields: &[&str], widths: &[usize]) {
    for (index, (field, width)) in fields.iter().zip(widths).enumerate() {
        if index > 0 {
            output.push(' ');
        }
        if index + 1 == fields.len() {
            output.push_str(field);
        } else {
            output.push_str(&format!("{field:<width$}"));
        }
    }
    output.push('\n');
}

fn json_response(rows: &[CatRow], columns: &[CatColumn]) -> Response {
    let values: Vec<JsonRow<'_>> = rows.iter().map(|row| JsonRow { row, columns }).collect();
    Json(values).into_response()
}

pub(super) fn render_help(request: &CatRequest) -> Response {
    match request.format {
        CatFormat::Text => {
            let mut output = String::new();
            for column in CatColumn::ALL {
                output.push_str(&format!(
                    "{:<8} | {:<8} | {}\n",
                    column.name(),
                    column.aliases().join(","),
                    column.description()
                ));
            }
            text_response(output)
        }
        CatFormat::Json => {
            let rows: Vec<serde_json::Value> = CatColumn::ALL
                .into_iter()
                .map(|column| {
                    serde_json::json!({
                        "name": column.name(),
                        "aliases": column.aliases(),
                        "description": column.description(),
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
