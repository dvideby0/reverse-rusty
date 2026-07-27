//! Shared `/_search` body/query-string control validation. ES/OS-compatible
//! controls may be sent in either location, but never both: rejecting ambiguity
//! keeps the effective request visible to clients and operators.

use std::time::Duration;

use serde::Deserialize;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchParams {
    from: Option<usize>,
    size: Option<usize>,
    explain: Option<bool>,
    profile: Option<bool>,
    #[serde(rename = "_source")]
    source: Option<bool>,
    timeout: Option<String>,
}

pub(crate) struct SearchControlInput {
    pub(crate) from: Option<usize>,
    pub(crate) size: Option<usize>,
    pub(crate) explain: Option<bool>,
    pub(crate) profile: Option<bool>,
    pub(crate) source: Option<bool>,
    pub(crate) include_source: Option<bool>,
    pub(crate) timeout: Option<String>,
    pub(crate) timeout_ms: Option<u64>,
}

pub(crate) struct SearchFeatures {
    pub(crate) explain: bool,
    pub(crate) profile: bool,
    pub(crate) include_source: bool,
}

pub(crate) struct SearchControls {
    pub(crate) from: usize,
    pub(crate) size: usize,
    pub(crate) features: SearchFeatures,
    pub(crate) timeout: Duration,
    pub(crate) explicit_timeout: bool,
}

fn one_location<T>(body: Option<T>, query: Option<T>, name: &str) -> Result<Option<T>, String> {
    match (body, query) {
        (Some(_), Some(_)) => Err(format!(
            "`{name}` must be specified in either the request body or query string, not both"
        )),
        (body, query) => Ok(body.or(query)),
    }
}

pub(crate) fn parse_named_time_value(control: &str, raw: &str) -> Result<Duration, String> {
    const UNITS: [(&str, u64); 7] = [
        ("nanos", 1),
        ("micros", 1_000),
        ("ms", 1_000_000),
        ("s", 1_000_000_000),
        ("m", 60 * 1_000_000_000),
        ("h", 60 * 60 * 1_000_000_000),
        ("d", 24 * 60 * 60 * 1_000_000_000),
    ];
    for (suffix, nanos) in UNITS {
        if let Some(number) = raw.strip_suffix(suffix) {
            let value = number.parse::<u64>().map_err(|_| {
                format!(
                    "`{control}` must be a non-negative integer followed by \
                     nanos, micros, ms, s, m, h, or d (got `{raw}`)"
                )
            })?;
            let total = value
                .checked_mul(nanos)
                .ok_or_else(|| format!("`{control}` is too large"))?;
            return Ok(Duration::from_nanos(total));
        }
    }
    Err(format!(
        "`{control}` must include a unit: nanos, micros, ms, s, m, h, or d (got `{raw}`)"
    ))
}

pub(crate) fn parse_time_value(raw: &str) -> Result<Duration, String> {
    parse_named_time_value("timeout", raw)
}

pub(crate) fn resolve_search_controls(
    input: SearchControlInput,
    params: SearchParams,
    default_include_source: bool,
) -> Result<SearchControls, String> {
    let from = one_location(input.from, params.from, "from")?.unwrap_or(0);
    let size = one_location(input.size, params.size, "size")?.unwrap_or(1000);
    let explain = one_location(input.explain, params.explain, "explain")?.unwrap_or(false);
    let profile = one_location(input.profile, params.profile, "profile")?.unwrap_or(false);

    let body_source = one_location(input.include_source, input.source, "include_source/_source")?;
    let include_source =
        one_location(body_source, params.source, "_source")?.unwrap_or(default_include_source);

    let es_timeout = one_location(input.timeout, params.timeout, "timeout")?;
    if input.timeout_ms.is_some() && es_timeout.is_some() {
        return Err(
            "`timeout_ms` and `timeout` are aliases; specify exactly one of them".to_string(),
        );
    }
    let explicit_timeout = input.timeout_ms.is_some() || es_timeout.is_some();
    let timeout = match (input.timeout_ms, es_timeout) {
        (Some(ms), None) => Duration::from_millis(ms),
        (None, Some(raw)) => parse_time_value(&raw)?,
        (None, None) => Duration::from_secs(30),
        (Some(_), Some(_)) => unreachable!("conflict rejected above"),
    };

    Ok(SearchControls {
        from,
        size,
        features: SearchFeatures {
            explain,
            profile,
            include_source,
        },
        timeout,
        explicit_timeout,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_es_timeout_units_and_rejects_ambiguity() {
        assert_eq!(
            parse_time_value("2s").expect("seconds"),
            Duration::from_secs(2)
        );
        assert_eq!(
            parse_time_value("250ms").expect("milliseconds"),
            Duration::from_millis(250)
        );
        assert!(parse_named_time_value("keep_alive", "soon")
            .expect_err("invalid keep alive")
            .contains("`keep_alive`"));
        assert!(parse_time_value("30").is_err());
        assert!(parse_time_value("18446744073709551615d").is_err());

        let result = resolve_search_controls(
            SearchControlInput {
                from: Some(1),
                size: None,
                explain: None,
                profile: None,
                source: None,
                include_source: None,
                timeout: None,
                timeout_ms: None,
            },
            SearchParams {
                from: Some(2),
                ..SearchParams::default()
            },
            true,
        );
        assert!(result.is_err());
    }
}
