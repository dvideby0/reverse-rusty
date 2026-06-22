//! `controlserver` — a deployable reverse-rusty **cluster-manager** node: serves the openraft
//! `ControlService` over gRPC (ADR-038, clustering step 5b). Builds only with
//! `--features distributed`.
//!
//! Run (3-node example):
//! ```text
//! controlserver 0 127.0.0.1:50061 --peer 1=http://127.0.0.1:50062 --peer 2=http://127.0.0.1:50063 --bootstrap
//! controlserver 1 127.0.0.1:50062 --peer 0=http://127.0.0.1:50061 --peer 2=http://127.0.0.1:50063
//! controlserver 2 127.0.0.1:50063 --peer 0=http://127.0.0.1:50061 --peer 1=http://127.0.0.1:50062
//! ```
//! The `--bootstrap` node forms the initial cluster from itself + every `--peer` once the peers are
//! listening; the others just serve and join. This is the manager-side analogue of `shardserver`
//! (the data path stays on `ShardService`); consensus holds only the cluster-state document.
//!
//! When the bootstrap node binds a wildcard address (`0.0.0.0:port`, the usual containerized
//! case) it must also pass `--advertise-url <URL>` — the routable address peers dial it on
//! (e.g. `--advertise-url https://control0:50061`); otherwise it would commit the unreachable
//! `0.0.0.0` URL into Raft membership and every peer→bootstrapper RPC would fail (ADR-082).
//!
//! This URL is committed at the FIRST bootstrap only: `Raft::initialize` is idempotent, so an
//! already-bootstrapped durable node (`--data-dir`) keeps its persisted membership on restart.
//! To change the advertised URL on an existing deployment, reset its control-plane data volume
//! (idle at v1, ADR-081) so the next start re-bootstraps with the new URL.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    resolve_mesh_token, start_grpc_node_with_security, ClientSecurity, ControlServer,
    ServerSecurity, TlsClientConfig, TlsServerIdentity,
};

/// Ring/model params the genesis document is seeded with. A real deployment derives these from the
/// cluster config; for this building-block bin they are fixed defaults (overridable via flags).
const DEFAULT_SHARDS: u32 = 8;
const DEFAULT_VNODES: u32 = 128;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let bootstrap = args.iter().any(|a| a == "--bootstrap");
    let mut node_id: Option<u64> = None;
    let mut bind: Option<String> = None;
    let mut peers: Vec<(u64, String)> = Vec::new();
    // The URL peers dial to reach THIS node — committed into Raft membership at bootstrap.
    // Distinct from the bind address: a node binds `0.0.0.0:port` (every interface) but must
    // advertise a routable host (e.g. `https://control0:50061`). Defaults to the bind address
    // only when that is already routable (not a wildcard) — see the bootstrap block below.
    let mut advertise_url: Option<String> = None;
    let mut shards = DEFAULT_SHARDS;
    let mut vnodes = DEFAULT_VNODES;
    let mut fingerprint: u64 = 0;
    let mut data_dir: Option<PathBuf> = None;
    let mut tls_cert: Option<PathBuf> = None;
    let mut tls_key: Option<PathBuf> = None;
    let mut tls_ca: Option<PathBuf> = None;
    let mut tls_domain: Option<String> = None;
    let mut token_flag: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                if let Some(v) = args.get(i + 1) {
                    data_dir = Some(PathBuf::from(v));
                }
                i += 1;
            }
            "--tls-cert" => {
                tls_cert = args.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--tls-key" => {
                tls_key = args.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--tls-ca" => {
                tls_ca = args.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--tls-domain" => {
                tls_domain = args.get(i + 1).cloned();
                i += 1;
            }
            "--cluster-token" => {
                token_flag = args.get(i + 1).cloned();
                i += 1;
            }
            "--advertise-url" => {
                advertise_url = args.get(i + 1).cloned();
                i += 1;
            }
            "--peer" => {
                // `--peer ID=http://host:port`
                if let Some(spec) = args.get(i + 1) {
                    if let Some((id, addr)) = spec.split_once('=') {
                        peers.push((id.parse()?, addr.to_string()));
                    }
                }
                i += 1;
            }
            "--shards" => {
                if let Some(v) = args.get(i + 1) {
                    shards = v.parse()?;
                }
                i += 1;
            }
            "--vnodes" => {
                if let Some(v) = args.get(i + 1) {
                    vnodes = v.parse()?;
                }
                i += 1;
            }
            "--fingerprint" => {
                if let Some(v) = args.get(i + 1) {
                    fingerprint = v.parse()?;
                }
                i += 1;
            }
            "--bootstrap" => {}
            // Positionals: node_id then bind addr.
            a if !a.starts_with("--") && node_id.is_none() => node_id = Some(a.parse()?),
            a if !a.starts_with("--") && bind.is_none() => bind = Some(a.to_string()),
            _ => {}
        }
        i += 1;
    }

    let node_id = node_id.ok_or(
        "usage: controlserver <NODE_ID> <BIND_ADDR> [--peer ID=URL ...] [--advertise-url URL] \
         [--bootstrap]",
    )?;
    let bind = bind.ok_or("missing BIND_ADDR")?;
    let addr: SocketAddr = bind.parse()?;

    // Mesh security (ADR-071). A manager node is BOTH a server (its ControlService) and a
    // client (the Raft RPCs it sends its peers), so it takes both halves: the identity it
    // presents (--tls-cert/--tls-key) and the CA it verifies peers against (--tls-ca,
    // with --tls-domain when peer URLs are raw IPs). One mesh token covers both directions.
    let token = resolve_mesh_token(token_flag, std::env::var("RR_CLUSTER_TOKEN"))?;
    let server_tls = match (tls_cert, tls_key) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some(TlsServerIdentity {
            cert_pem: std::fs::read(&cert)
                .map_err(|e| format!("reading --tls-cert {}: {e}", cert.display()))?,
            key_pem: std::fs::read(&key)
                .map_err(|e| format!("reading --tls-key {}: {e}", key.display()))?,
        }),
        _ => return Err("--tls-cert and --tls-key must be provided together".into()),
    };
    let client_tls = match tls_ca {
        None => None,
        Some(ca) => Some(TlsClientConfig {
            ca_pem: std::fs::read(&ca)
                .map_err(|e| format!("reading --tls-ca {}: {e}", ca.display()))?,
            domain: tls_domain,
        }),
    };
    if token.is_some() && server_tls.is_none() {
        eprintln!(
            "WARNING: --cluster-token without TLS — the mesh secret crosses the wire in              cleartext; configure --tls-cert/--tls-key (ADR-071)"
        );
    }

    let rt = tokio::runtime::Runtime::new()?;
    // `--data-dir` makes this manager node DURABLE (ADR-041): it persists its Raft log/vote/
    // committed/snapshot and resumes its committed cluster-state document on restart. Without it the
    // node keeps the in-memory store (ADR-038) and starts fresh each launch.
    // `Arc` so the gRPC `ControlServer` (which serves the client-facing `ClientControl` op, ADR-083)
    // and the bootstrap `initialize` below both hold the same plane.
    let plane = Arc::new(start_grpc_node_with_security(
        node_id,
        shards,
        vnodes,
        fingerprint,
        rt.handle(),
        data_dir.as_deref(),
        ClientSecurity {
            tls: client_tls,
            token: token.clone(),
        },
    )?);
    if let Some(dir) = &data_dir {
        println!(
            "controlserver: node {node_id} DURABLE (raft state under {})",
            dir.display()
        );
    }
    let serves_tls = server_tls.is_some();
    let server = ControlServer::new(Arc::clone(&plane)).with_security(ServerSecurity {
        tls: server_tls,
        token,
    });
    let serve = rt.spawn(server.serve(addr));

    if bootstrap {
        // Let the peers' listeners come up, then form the initial cluster from all members.
        // The self-URL scheme must match the transport peers dial back on: with a TLS
        // identity configured this node serves https, so registering an http:// URL would
        // fail every peer→bootstrapper Raft RPC at the handshake (review finding).
        // Resolve the routable self-URL committed into Raft membership; fail loud on a wildcard
        // bind with no --advertise-url (peers could not dial it). See `bootstrap_self_url`.
        let self_url = bootstrap_self_url(advertise_url.as_deref(), &addr, &bind, serves_tls)?;
        if let Some(url) = &advertise_url {
            // A scheme that disagrees with the TLS identity guarantees a peer handshake failure.
            if url.starts_with("https://") != serves_tls {
                let scheme = if serves_tls { "https" } else { "http" };
                eprintln!(
                    "WARNING: --advertise-url {url} scheme disagrees with this node's TLS \
                     identity (serving {scheme}); peers may fail the Raft handshake"
                );
            }
        }
        std::thread::sleep(Duration::from_secs(2));
        let mut members: Vec<(u64, String)> = vec![(node_id, self_url)];
        members.extend(peers.iter().cloned());
        plane.initialize(&members)?;
        println!(
            "controlserver: node {node_id} BOOTSTRAPPED a {}-node control plane on {addr}",
            members.len()
        );
    } else {
        println!("controlserver: node {node_id} serving ControlService on {addr} (joining)");
    }

    // Serve until killed.
    rt.block_on(serve)??;
    Ok(())
}

/// Resolve the routable self-URL a bootstrap node commits into Raft membership (ADR-082).
///
/// Precedence: an explicit `--advertise-url` wins (used verbatim). Otherwise the bind address
/// becomes the self-URL — but ONLY when it is already routable. A wildcard bind (`0.0.0.0` / `::`,
/// the usual containerized case) is not dialable by peers, so refuse it rather than commit an
/// unreachable address that would fail every peer→bootstrapper RPC. Pure + `Result` so the policy
/// is unit-tested without standing up a node.
fn bootstrap_self_url(
    advertise_url: Option<&str>,
    addr: &SocketAddr,
    bind: &str,
    serves_tls: bool,
) -> Result<String, String> {
    let scheme = if serves_tls { "https" } else { "http" };
    match advertise_url {
        Some(url) => Ok(url.to_string()),
        None if addr.ip().is_unspecified() => Err(format!(
            "--bootstrap on a wildcard bind ({bind}) needs --advertise-url: peers cannot dial \
             {scheme}://{bind}. Pass --advertise-url {scheme}://<reachable-host>:{} (the address \
             peers use to reach this node).",
            addr.port()
        )),
        None => Ok(format!("{scheme}://{bind}")),
    }
}

#[cfg(test)]
mod tests {
    use super::bootstrap_self_url;
    use std::net::SocketAddr;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn explicit_advertise_url_wins_verbatim() {
        // Even with a wildcard bind, an explicit advertise URL is honored as-is.
        let got = bootstrap_self_url(
            Some("https://control0:50061"),
            &addr("0.0.0.0:50061"),
            "0.0.0.0:50061",
            true,
        )
        .unwrap();
        assert_eq!(got, "https://control0:50061");
    }

    #[test]
    fn routable_bind_derives_self_url_by_tls_scheme() {
        assert_eq!(
            bootstrap_self_url(None, &addr("127.0.0.1:50061"), "127.0.0.1:50061", false).unwrap(),
            "http://127.0.0.1:50061"
        );
        assert_eq!(
            bootstrap_self_url(None, &addr("10.0.0.5:50061"), "10.0.0.5:50061", true).unwrap(),
            "https://10.0.0.5:50061"
        );
    }

    #[test]
    fn wildcard_bind_without_advertise_url_is_refused() {
        // The bug ADR-082 fixes: committing https://0.0.0.0:port into Raft membership.
        for bind in ["0.0.0.0:50061", "[::]:50061"] {
            let err = bootstrap_self_url(None, &addr(bind), bind, true).unwrap_err();
            assert!(err.contains("--advertise-url"), "got: {err}");
            assert!(err.contains(bind), "error names the offending bind: {err}");
        }
    }
}
