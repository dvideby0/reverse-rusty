//! `shardserver` — a deployable reverse-rusty shard node: serves `ShardService` over
//! gRPC. Builds only with `--features distributed`.
//!
//! Run: `cargo run --release --bin shardserver --features distributed -- [ADDR] [--pending]`
//! (ADDR defaults to 127.0.0.1:50051).
//!
//! This is the single-node server building block. By default it stands up ONE shard over a
//! self-contained synthetic corpus so the node serves something matchable. With `--pending`
//! it starts **dict-less** — serving nothing until a coordinator ships its frozen dict via
//! `AdoptDict` and then places queries (ADR-034); this is the real multi-node flow, where a
//! data node need not rebuild a byte-identical dict from the corpus out-of-band. A client can
//! point a `RemoteShard` at either (a K=1 cluster via `ClusterEngine::connect_remote`, which
//! ships the dict) and percolate.

use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::ShardServer;
use reverse_rusty::compile::extract;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let pending = args.iter().any(|a| a == "--pending");
    let addr: SocketAddr = args
        .iter()
        .find(|a| !a.starts_with("--"))
        .map_or("127.0.0.1:50051", String::as_str)
        .parse()?;

    let norm = Arc::new(Normalizer::default_vocab()?);
    let rt = tokio::runtime::Runtime::new()?;

    if pending {
        // Dict-less: serve nothing until a coordinator ships its frozen dict (AdoptDict) and
        // then places queries — the real multi-node flow, no out-of-band dict (ADR-034).
        let server = ShardServer::pending(norm, EngineConfig::default());
        println!("shardserver: serving ShardService on {addr} (PENDING — awaiting AdoptDict)");
        rt.block_on(server.serve(addr))?;
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

    let server = ShardServer::new(Arc::clone(&norm), Arc::new(dict), EngineConfig::default());
    server.ingest_dsl(&queries);
    println!(
        "shardserver: serving ShardService on {addr} ({} queries loaded)",
        queries.len()
    );
    rt.block_on(server.serve(addr))?;
    Ok(())
}
