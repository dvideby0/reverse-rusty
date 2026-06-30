//! `shardserver` — a deployable reverse-rusty shard node: serves `ShardService` over
//! gRPC. Builds only with `--features distributed`.
//!
//! Run: `cargo run --release --bin shardserver --features distributed -- [ADDR] [--pending]`
//! (ADDR defaults to 127.0.0.1:50051).
//!
//! `--health-addr <ADDR>` (ADR-084) additionally serves the standard `grpc.health.v1.Health`
//! service on a SEPARATE plaintext port for Kubernetes probes: liveness (`Check("")`) is
//! SERVING once the gRPC server is up; readiness (`Check("ready")`) is NOT_SERVING until the
//! node adopts a dict (a `--pending` shard is live-but-not-ready until `AdoptDict`).
//!
//! `--metrics-addr <ADDR>` (ADR-091) additionally serves this shard's Prometheus `/_metrics` on a
//! SEPARATE plaintext port for scraping — per-shard stored-query count, memory, compaction backlog,
//! and cost-class distribution. Same posture as `--health-addr`: plaintext, pod-local, never the
//! TLS + token mesh data port. Unset ⇒ no listener.
//!
//! This is the single-node server building block. By default it stands up ONE shard over a
//! self-contained synthetic corpus so the node serves something matchable. With `--pending`
//! it starts **dict-less** — serving nothing until a coordinator ships its frozen dict via
//! `AdoptDict` and then places queries (ADR-034); this is the real multi-node flow, where a
//! data node need not rebuild a byte-identical dict from the corpus out-of-band. A client can
//! point a `RemoteShard` at either (a K=1 cluster via `ClusterEngine::connect_remote`, which
//! ships the dict) and percolate.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use reverse_rusty::cluster::{
    resolve_mesh_token, serve_metrics, ClientSecurity, ServerSecurity, ShardServer,
    TlsClientConfig, TlsServerIdentity,
};
use reverse_rusty::compile::extract;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let pending = args.iter().any(|a| a == "--pending");
    // `--data-dir <path>` makes the node DURABLE: its shard persists segments there, so it can
    // serve `FetchSegments` and be a recovering replica (ADR-035/036). Parse it explicitly so its
    // value is not mistaken for the positional ADDR.
    let mut data_dir: Option<PathBuf> = None;
    let mut addr_arg: Option<String> = None;
    let mut tls_cert: Option<PathBuf> = None;
    let mut tls_key: Option<PathBuf> = None;
    let mut tls_ca: Option<PathBuf> = None;
    let mut tls_domain: Option<String> = None;
    let mut token_flag: Option<String> = None;
    // Optional SEPARATE plaintext port for the gRPC health service (k8s probes, ADR-084).
    let mut health_addr: Option<SocketAddr> = None;
    // Optional SEPARATE plaintext port for the Prometheus `/_metrics` endpoint (ADR-091).
    let mut metrics_addr: Option<SocketAddr> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                data_dir = args.get(i + 1).map(PathBuf::from);
                i += 1;
            }
            "--health-addr" => {
                if let Some(v) = args.get(i + 1) {
                    health_addr = Some(v.parse().map_err(|e| format!("--health-addr {v}: {e}"))?);
                }
                i += 1;
            }
            "--metrics-addr" => {
                if let Some(v) = args.get(i + 1) {
                    metrics_addr = Some(v.parse().map_err(|e| format!("--metrics-addr {v}: {e}"))?);
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
            // First positional arg = ADDR; later positionals are ignored.
            a if !a.starts_with("--") && addr_arg.is_none() => {
                addr_arg = Some(a.to_string());
            }
            _ => {}
        }
        i += 1;
    }
    let addr: SocketAddr = addr_arg.as_deref().unwrap_or("127.0.0.1:50051").parse()?;
    let (security, client_security) =
        resolve_security(tls_cert, tls_key, tls_ca, tls_domain, token_flag)?;

    let norm = Arc::new(Normalizer::default_vocab()?);
    let rt = tokio::runtime::Runtime::new()?;

    if pending {
        // Dict-less: serve nothing until a coordinator ships its frozen dict (AdoptDict) and then
        // places queries — the real multi-node flow, no out-of-band dict (ADR-034). With
        // `--data-dir` it is also durable: a recovering/replica node (ADR-035/036).
        let server = match &data_dir {
            // open_durable self-restores a previously adopted node (ADR-072); a fresh
            // dir starts pending exactly as before.
            Some(dir) => ShardServer::open_durable(norm, EngineConfig::default(), dir.clone())?,
            None => ShardServer::pending(norm, EngineConfig::default()),
        };
        let state = if server.is_serving() {
            "RESUMED from durable state".to_string()
        } else {
            let durable = if data_dir.is_some() { ", DURABLE" } else { "" };
            format!("PENDING{durable} — awaiting AdoptDict")
        };
        if let Some(ha) = health_addr {
            println!("shardserver: health (grpc.health.v1) on {ha} (plaintext, k8s probes)");
        }
        println!("shardserver: serving ShardService on {addr} ({state})");
        run(
            server,
            security,
            client_security,
            health_addr,
            metrics_addr,
            addr,
            &rt,
        )?;
        return Ok(());
    }

    // Self-contained demo: a deterministic corpus so the node serves matchable data.
    let cfg = GenConfig {
        num_queries: 2_000,
        num_titles: 0,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5EED_5417,
        num_players: 600,
        num_sets: 300,
    };
    let queries = generate(&cfg).queries;

    // Pass A: build the authoritative frozen dict over the corpus (same shape as
    // ClusterEngine::build's pass A), then hand it to the server.
    let mut dict = Dict::new();
    let mut lc = String::new();
    for (_id, text) in &queries {
        if let Ok(ast) = reverse_rusty::dsl::parse(text) {
            let _ = extract(&ast, &norm, &mut dict, &mut lc);
        }
    }
    dict.finalize_mask();

    let server = match &data_dir {
        Some(dir) => ShardServer::new_durable(
            Arc::clone(&norm),
            Arc::new(dict),
            EngineConfig::default(),
            dir.clone(),
        )?,
        None => ShardServer::new(Arc::clone(&norm), Arc::new(dict), EngineConfig::default()),
    };
    server.ingest_dsl(&queries);
    if let Some(ha) = health_addr {
        println!("shardserver: health (grpc.health.v1) on {ha} (plaintext, k8s probes)");
    }
    println!(
        "shardserver: serving ShardService on {addr} ({} queries loaded)",
        queries.len()
    );
    run(
        server,
        security,
        client_security,
        health_addr,
        metrics_addr,
        addr,
        &rt,
    )?;
    Ok(())
}

/// Spawn the optional plaintext Prometheus `/_metrics` listener (ADR-091) — captured BEFORE `serve`
/// consumes the server — then serve `ShardService` (with mesh security + the optional health port)
/// until exit. A `--metrics-addr` bind failure is fatal (an explicit observability request should not
/// start silently); the std listener thread serves for the process lifetime.
fn run(
    server: ShardServer,
    security: ServerSecurity,
    client_security: ClientSecurity,
    health_addr: Option<SocketAddr>,
    metrics_addr: Option<SocketAddr>,
    addr: SocketAddr,
    rt: &tokio::runtime::Runtime,
) -> Result<(), Box<dyn std::error::Error>> {
    let _metrics = match metrics_addr {
        Some(maddr) => {
            let src = server.metrics_source();
            println!("shardserver: metrics (prometheus /_metrics) on {maddr} (plaintext)");
            Some(
                serve_metrics(maddr, move || src.render())
                    .map_err(|e| format!("--metrics-addr {maddr}: {e}"))?,
            )
        }
        None => None,
    };
    rt.block_on(configure(server, security, client_security, health_addr).serve(addr))?;
    Ok(())
}

/// Apply mesh security (ADR-071) + the optional plaintext health port (ADR-084) to a
/// built server — shared by the pending and demo serve paths so they cannot drift.
fn configure(
    server: ShardServer,
    security: ServerSecurity,
    client_security: ClientSecurity,
    health_addr: Option<SocketAddr>,
) -> ShardServer {
    let server = server
        .with_security(security)
        .with_client_security(client_security);
    match health_addr {
        Some(addr) => server.with_health_addr(addr),
        None => server,
    }
}

/// Resolve the node's mesh security (ADR-071) from `--tls-cert`/`--tls-key` +
/// `--cluster-token`/`RR_CLUSTER_TOKEN` — fail-loud on a half-configured TLS identity
/// or a malformed token; warn loud when a token is configured without TLS (the secret
/// would cross the wire in cleartext).
fn resolve_security(
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    tls_domain: Option<String>,
    token_flag: Option<String>,
) -> Result<(ServerSecurity, ClientSecurity), Box<dyn std::error::Error>> {
    let tls = match (tls_cert, tls_key) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some(TlsServerIdentity {
            cert_pem: std::fs::read(&cert)
                .map_err(|e| format!("reading --tls-cert {}: {e}", cert.display()))?,
            key_pem: std::fs::read(&key)
                .map_err(|e| format!("reading --tls-key {}: {e}", key.display()))?,
        }),
        _ => return Err("--tls-cert and --tls-key must be provided together".into()),
    };
    // The CLIENT half (ADR-071/072): the CA this node verifies a peer SOURCE against
    // when its `RecoverFrom` handler dials out (the handoff / peer-recovery pull).
    let client_tls = match tls_ca {
        None => None,
        Some(ca) => Some(TlsClientConfig {
            ca_pem: std::fs::read(&ca)
                .map_err(|e| format!("reading --tls-ca {}: {e}", ca.display()))?,
            domain: tls_domain,
        }),
    };
    let token = resolve_mesh_token(token_flag, std::env::var("RR_CLUSTER_TOKEN"))?;
    if token.is_some() && tls.is_none() {
        eprintln!(
            "WARNING: --cluster-token without TLS — the mesh secret crosses the wire in \
             cleartext; configure --tls-cert/--tls-key (ADR-071)"
        );
    }
    Ok((
        ServerSecurity {
            tls,
            token: token.clone(),
            ..Default::default()
        },
        ClientSecurity {
            tls: client_tls,
            token,
            ..Default::default()
        },
    ))
}
