//! Declarative mesh-origin validation for node registration.

use axum::http::Uri;

const MAX_NODE_ADDR_BYTES: usize = 2 * 1024;

pub(super) fn validate_node_addr(addr: String) -> Result<String, String> {
    if addr.is_empty() {
        return Err("`addr` must not be empty".to_string());
    }
    if addr.len() > MAX_NODE_ADDR_BYTES {
        return Err(format!(
            "`addr` exceeds the {MAX_NODE_ADDR_BYTES}-byte endpoint limit"
        ));
    }
    if addr.trim() != addr {
        return Err("`addr` must not contain leading or trailing whitespace".to_string());
    }
    if addr.contains('#') {
        return Err("`addr` must not contain a fragment".to_string());
    }
    let uri: Uri = addr
        .parse()
        .map_err(|source| format!("`addr` is not a valid endpoint URI: {source}"))?;
    if !matches!(uri.scheme_str(), Some("http" | "https")) {
        return Err("`addr` must use an `http` or `https` scheme".to_string());
    }
    let authority = uri
        .authority()
        .ok_or_else(|| "`addr` must include a host authority".to_string())?;
    if authority.as_str().contains('@') {
        return Err("`addr` must not contain embedded user credentials".to_string());
    }
    let host = authority.host();
    if host.is_empty() {
        return Err("`addr` must include a host authority".to_string());
    }
    let port_suffix = authority
        .as_str()
        .strip_prefix(host)
        .ok_or_else(|| "`addr` has an invalid authority".to_string())?;
    if !port_suffix.is_empty() {
        let raw_port = port_suffix
            .strip_prefix(':')
            .ok_or_else(|| "`addr` has an invalid port".to_string())?;
        let port = raw_port
            .parse::<u16>()
            .map_err(|_| "`addr` port must be an integer from 1 through 65535".to_string())?;
        if port == 0 {
            return Err("`addr` port must be an integer from 1 through 65535".to_string());
        }
    }
    if uri.path() != "/" || uri.query().is_some() {
        return Err("`addr` must be an endpoint origin without a path or query".to_string());
    }
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::validate_node_addr;

    #[test]
    fn accepts_default_ports_explicit_ports_and_ipv6_origins() {
        for addr in [
            "http://mesh.example.test",
            "https://mesh.example.test:443",
            "http://[::1]:50051",
        ] {
            assert_eq!(validate_node_addr(addr.to_string()).as_deref(), Ok(addr));
        }
    }

    #[test]
    fn rejects_zero_port_credentials_and_fragments() {
        for addr in [
            "http://127.0.0.1:0",
            "http://user:pass@127.0.0.1:50057",
            "http://127.0.0.1:50057#mesh",
        ] {
            assert!(validate_node_addr(addr.to_string()).is_err(), "{addr}");
        }
    }
}
