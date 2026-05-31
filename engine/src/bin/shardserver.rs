//! `shardserver` — a deployable reverse-rusty shard node: serves `ShardService` over
//! gRPC. Builds only with `--features distributed`.
//!
//! Run: `cargo run --release --bin shardserver --features distributed -- [ADDR]`
//! (ADDR defaults to 127.0.0.1:50051).
//!
//! This is the single-node server building block. It stands up ONE shard over a
//! self-contained synthetic corpus so the node serves something matchable; real-data
//! loading and the multi-node coordinator that wires many of these together are later
//! build-path steps (ADR-029). A client can already point a `RemoteShard` at it (a
//! K=1 cluster via `ClusterEngine::connect_remote`) and percolate.

use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::ShardServer;
use reverse_rusty::compile::extract;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:50051".to_string())
        .parse()?;

    // A self-contained, deterministic corpus so the node serves matchable data.
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
    let norm = Normalizer::default_vocab()?;
    let mut dict = Dict::new();
    let mut lc = String::new();
    for (_id, text) in &queries {
        if let Ok(ast) = reverse_rusty::dsl::parse(text) {
            let _ = extract(&ast, &norm, &mut dict, &mut lc);
        }
    }
    dict.finalize_mask();

    let server = ShardServer::new(Arc::new(norm), Arc::new(dict), EngineConfig::default());
    server.ingest_dsl(&queries);

    let rt = tokio::runtime::Runtime::new()?;
    println!(
        "shardserver: serving ShardService on {addr} ({} queries loaded)",
        queries.len()
    );
    rt.block_on(server.serve(addr))?;
    Ok(())
}
