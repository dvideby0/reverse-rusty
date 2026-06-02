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

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use reverse_rusty::cluster::{start_grpc_node, ControlServer};

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
    let mut shards = DEFAULT_SHARDS;
    let mut vnodes = DEFAULT_VNODES;
    let mut fingerprint: u64 = 0;
    let mut data_dir: Option<PathBuf> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--data-dir" => {
                if let Some(v) = args.get(i + 1) {
                    data_dir = Some(PathBuf::from(v));
                }
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

    let node_id = node_id
        .ok_or("usage: controlserver <NODE_ID> <BIND_ADDR> [--peer ID=URL ...] [--bootstrap]")?;
    let bind = bind.ok_or("missing BIND_ADDR")?;
    let addr: SocketAddr = bind.parse()?;
    let self_url = format!("http://{bind}");

    let rt = tokio::runtime::Runtime::new()?;
    // `--data-dir` makes this manager node DURABLE (ADR-041): it persists its Raft log/vote/
    // committed/snapshot and resumes its committed cluster-state document on restart. Without it the
    // node keeps the in-memory store (ADR-038) and starts fresh each launch.
    let plane = start_grpc_node(
        node_id,
        shards,
        vnodes,
        fingerprint,
        rt.handle(),
        data_dir.as_deref(),
    )?;
    if let Some(dir) = &data_dir {
        println!(
            "controlserver: node {node_id} DURABLE (raft state under {})",
            dir.display()
        );
    }
    let server = ControlServer::new(plane.raft());
    let serve = rt.spawn(server.serve(addr));

    if bootstrap {
        // Let the peers' listeners come up, then form the initial cluster from all members.
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
