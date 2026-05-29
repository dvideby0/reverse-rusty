//! Query loader — read `(u64, String)` query pairs from CSV and JSONL files.
//!
//! Formats:
//!   **CSV** — first row is a header (`id,query`), subsequent rows are `<u64>,<query_dsl>`.
//!     Commas inside a quoted field are handled (standard RFC-4180 quoting).
//!   **JSONL** — one JSON object per line: `{"id": 123, "query": "pokemon base set"}`.
//!
//! Auto-detection: if the file extension is `.csv` or `.tsv` we use the CSV parser;
//! `.jsonl` or `.ndjson` use the JSONL parser. Otherwise we peek at the first
//! non-empty line — if it starts with `{` we assume JSONL, else CSV.
//!
//! Both parsers stream line-by-line and collect per-line errors without aborting,
//! so a single bad row doesn't reject the whole file.

use std::io::{self, BufRead};
use std::path::Path;

/// A single load error tied to a line number.
#[derive(Debug, Clone)]
pub struct LoadError {
    pub line: usize,
    pub message: String,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

/// Result of loading a query file.
#[derive(Debug)]
pub struct LoadResult {
    pub queries: Vec<(u64, String)>,
    pub errors: Vec<LoadError>,
}

/// Load queries from a file, auto-detecting format from extension or content.
pub fn load_file(path: &Path) -> io::Result<LoadResult> {
    let file = std::fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let lines: Vec<String> = reader.lines().collect::<io::Result<Vec<_>>>()?;

    let format = detect_format(path, &lines);
    match format {
        Format::Csv => Ok(parse_csv(&lines)),
        Format::Jsonl => Ok(parse_jsonl(&lines)),
    }
}

/// Load queries from a string, auto-detecting format.
pub fn load_str(content: &str, hint: Option<&str>) -> LoadResult {
    let lines: Vec<String> = content.lines().map(String::from).collect();
    let format = match hint {
        Some(h) => format_from_ext(h).unwrap_or_else(|| detect_from_content(&lines)),
        None => detect_from_content(&lines),
    };
    match format {
        Format::Csv => parse_csv(&lines),
        Format::Jsonl => parse_jsonl(&lines),
    }
}

// ---------------------------------------------------------------------------
// Format detection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Csv,
    Jsonl,
}

fn detect_format(path: &Path, lines: &[String]) -> Format {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        if let Some(f) = format_from_ext(ext) {
            return f;
        }
    }
    detect_from_content(lines)
}

fn format_from_ext(ext: &str) -> Option<Format> {
    match ext.to_ascii_lowercase().as_str() {
        "csv" | "tsv" => Some(Format::Csv),
        "jsonl" | "ndjson" => Some(Format::Jsonl),
        _ => None,
    }
}

fn detect_from_content(lines: &[String]) -> Format {
    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        return if trimmed.starts_with('{') {
            Format::Jsonl
        } else {
            Format::Csv
        };
    }
    Format::Csv // default for empty files
}

// ---------------------------------------------------------------------------
// CSV parser
// ---------------------------------------------------------------------------

fn parse_csv(lines: &[String]) -> LoadResult {
    let mut queries = Vec::new();
    let mut errors = Vec::new();

    let mut iter = lines.iter().enumerate();

    // Skip header if it looks like one (first field is non-numeric).
    if let Some((_, first_line)) = iter.next() {
        let trimmed = first_line.trim();
        if !trimmed.is_empty() {
            // If the first field parses as u64 it's data, not a header.
            let first_field = csv_first_field(trimmed);
            if first_field.parse::<u64>().is_ok() {
                // It's data — parse it.
                match parse_csv_line(trimmed) {
                    Ok(pair) => queries.push(pair),
                    Err(msg) => errors.push(LoadError { line: 1, message: msg }),
                }
            }
            // else: it's a header row, skip it
        }
    }

    for (idx, line) in iter {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let line_num = idx + 1; // 1-indexed
        match parse_csv_line(trimmed) {
            Ok(pair) => queries.push(pair),
            Err(msg) => errors.push(LoadError { line: line_num, message: msg }),
        }
    }

    LoadResult { queries, errors }
}

/// Extract the first CSV field (before the first unquoted comma).
fn csv_first_field(line: &str) -> &str {
    if let Some(rest) = line.strip_prefix('"') {
        // Quoted field — find closing quote.
        if let Some(end) = rest.find('"') {
            return &rest[..end];
        }
    }
    line.split(',').next().unwrap_or(line)
}

/// Parse one CSV line into (id, query).
/// Supports: `123,pokemon base set` and `123,"pokemon, base set"` (quoted commas).
fn parse_csv_line(line: &str) -> Result<(u64, String), String> {
    let comma = line.find(',').ok_or_else(|| "no comma separator found".to_string())?;
    let id_str = line[..comma].trim();
    let id: u64 = id_str.parse().map_err(|e| format!("invalid id '{}': {}", id_str, e))?;

    let rest = line[comma + 1..].trim();
    let query = if rest.starts_with('"') && rest.ends_with('"') && rest.len() >= 2 {
        // Strip surrounding quotes, unescape doubled quotes.
        rest[1..rest.len() - 1].replace("\"\"", "\"")
    } else {
        rest.to_string()
    };

    if query.is_empty() {
        return Err("empty query".to_string());
    }

    Ok((id, query))
}

// ---------------------------------------------------------------------------
// JSONL parser
// ---------------------------------------------------------------------------

fn parse_jsonl(lines: &[String]) -> LoadResult {
    let mut queries = Vec::new();
    let mut errors = Vec::new();

    for (idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let line_num = idx + 1;
        match parse_jsonl_line(trimmed) {
            Ok(pair) => queries.push(pair),
            Err(msg) => errors.push(LoadError { line: line_num, message: msg }),
        }
    }

    LoadResult { queries, errors }
}

fn parse_jsonl_line(line: &str) -> Result<(u64, String), String> {
    let val: serde_json::Value =
        serde_json::from_str(line).map_err(|e| format!("invalid JSON: {}", e))?;

    let obj = val.as_object().ok_or("expected JSON object")?;

    let id = obj
        .get("id")
        .ok_or("missing 'id' field")?
        .as_u64()
        .ok_or("'id' must be a u64")?;

    let query = obj
        .get("query")
        .ok_or("missing 'query' field")?
        .as_str()
        .ok_or("'query' must be a string")?;

    if query.is_empty() {
        return Err("empty query".to_string());
    }

    Ok((id, query.to_string()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_with_header() {
        let input = "id,query\n1,pokemon base set\n2,charizard holo\n";
        let r = load_str(input, Some("csv"));
        assert_eq!(r.queries.len(), 2);
        assert_eq!(r.queries[0], (1, "pokemon base set".into()));
        assert_eq!(r.queries[1], (2, "charizard holo".into()));
        assert!(r.errors.is_empty());
    }

    #[test]
    fn csv_without_header() {
        let input = "1,pokemon base set\n2,charizard holo\n";
        let r = load_str(input, Some("csv"));
        assert_eq!(r.queries.len(), 2);
    }

    #[test]
    fn csv_quoted_field() {
        let input = "id,query\n1,\"pokemon, base set\"\n";
        let r = load_str(input, Some("csv"));
        assert_eq!(r.queries.len(), 1);
        assert_eq!(r.queries[0].1, "pokemon, base set");
    }

    #[test]
    fn csv_error_lines() {
        let input = "id,query\nnot_a_number,hello\n2,good query\n3,\n";
        let r = load_str(input, Some("csv"));
        assert_eq!(r.queries.len(), 1);
        assert_eq!(r.errors.len(), 2); // bad id + empty query
    }

    #[test]
    fn jsonl_basic() {
        let input = r#"{"id": 1, "query": "pokemon base set"}
{"id": 2, "query": "charizard holo"}
"#;
        let r = load_str(input, Some("jsonl"));
        assert_eq!(r.queries.len(), 2);
        assert_eq!(r.queries[0], (1, "pokemon base set".into()));
        assert!(r.errors.is_empty());
    }

    #[test]
    fn jsonl_error_lines() {
        let input = "{bad json}\n{\"id\": 1, \"query\": \"good\"}\n";
        let r = load_str(input, Some("jsonl"));
        assert_eq!(r.queries.len(), 1);
        assert_eq!(r.errors.len(), 1);
    }

    #[test]
    fn auto_detect_jsonl() {
        let input = r#"{"id": 1, "query": "test"}"#;
        let r = load_str(input, None);
        assert_eq!(r.queries.len(), 1);
    }

    #[test]
    fn auto_detect_csv() {
        let input = "id,query\n1,test\n";
        let r = load_str(input, None);
        assert_eq!(r.queries.len(), 1);
    }
}
