//! gRPC differential oracle — the CONTRACT verification for the distributed shard
//! transport (build behind `--features distributed`).
//!
//! Stands up K real `ShardServer`s on localhost, assembles a `ClusterEngine` whose
//! shards are gRPC `RemoteShard`s, loads the corpus over the wire (IngestExtracted),
//! and asserts the gRPC-backed cluster returns EXACTLY the independent brute-force
//! oracle's set AND the single-node engine's set — broad on and off. This proves the
//! seam + transport + the sync→async (`block_on`) bridge preserve the zero
//! false-negative contract across a process boundary (here, same-process sockets; the
//! servers share the SAME frozen `Arc<Dict>`/`Arc<Normalizer>`, which is how the
//! cross-process dict-identity requirement is satisfied in-test — see ADR-029).
//!
//! Whole file is gated; the default `cargo test` skips it.
#![cfg(feature = "distributed")]

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardServer};
use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig, BRANDS};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

/// Independent ground-truth matcher (same structure as `cluster_oracle.rs::Brute`;
/// deliberately shares nothing with the engine or cluster).
struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    fn build(queries: &[(u64, String)]) -> Self {
        let norm = vocab();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue; // mirror class-D rejection
                }
                qs.push((*logical, ex));
            }
        }
        dict.finalize_mask();
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    fn matches(&self, title: &str, lc: &mut String, feats: &mut Vec<u32>) -> HashSet<u64> {
        self.norm.match_features(title, &self.dict, lc, feats);
        let present = |f: u32| feats.binary_search(&f).is_ok();
        let mut out = HashSet::new();
        for (logical, ex) in &self.queries {
            if ex.required.iter().all(|&f| present(f))
                && !ex.forbidden.iter().any(|&f| present(f))
                && ex.anyof.iter().all(|g| g.iter().any(|&f| present(f)))
            {
                out.insert(*logical);
            }
        }
        out
    }
}

/// A compact corpus (smaller than `cluster_oracle.rs`'s, since every probe is an RPC)
/// that still exercises class A / B-any-of / B-arity-2 / C and multi-shard fan-out.
fn build_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 4_000,
        num_titles: 300,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x9119_57A1,
        num_players: 900,
        num_sets: 400,
    };
    let data = generate(&cfg);
    let mut queries = data.queries;
    let mut titles = data.titles;
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;

    // class-B any-of: pure any-of of two rare players.
    for i in 0..120u64 {
        queries.push((next_id, format!("(rareplayer{i},rareplayer{})", i + 1000)));
        next_id += 1;
    }
    // class-B arity-2: all-hot required (year + brand) → replicated lane.
    for i in 0..80u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand}")));
        next_id += 1;
    }
    // class-A anchored on injected rare players, so multi-entity titles match.
    for i in 0..120u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand} rareplayer{i}")));
        next_id += 1;
    }
    // multi-entity titles: two rare players → fan out to two selective shards + lane 0.
    for i in 0..120u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        let a = i % 120;
        titles.push(format!(
            "{year} {brand} rareplayer{a} rareplayer{} psa 10",
            a + 1000
        ));
    }

    (queries, titles)
}

/// Bind an ephemeral localhost port and return its address. (Probe-then-serve: the
/// brief window before the server re-binds is tolerated for a localhost test.)
fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
}

/// Block until `addr` accepts TCP (the gRPC server is listening) or time out.
fn wait_until_listening(addr: SocketAddr) {
    for _ in 0..300 {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("shard server at {addr} never started listening");
}

#[test]
fn grpc_cluster_matches_single_node_and_oracle() {
    let (queries, titles) = build_corpus();

    // Independent expected sets: brute-force oracle + single-node engine, broad on/off.
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut ref_broad: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut ref_selective: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut oracle: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut total_truth = 0usize;
    for title in &titles {
        reference.match_title(title, &mut s, &mut out, true);
        ref_broad.push(out.iter().copied().collect());
        reference.match_title(title, &mut s, &mut out, false);
        ref_selective.push(out.iter().copied().collect());
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        oracle.push(truth);
    }
    assert!(total_truth > 0, "degenerate corpus: no matches at all");

    // ONE authoritative frozen feature space, shared into every server (this is how
    // the cross-process dict-identity requirement is met in-test) AND used by the
    // coordinator for placement/routing.
    let norm = Arc::new(vocab());
    let dict = {
        let mut d = Dict::new();
        let mut lc = String::new();
        for (_id, text) in &queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let _ = extract(&ast, &norm, &mut d, &mut lc);
            }
        }
        d.finalize_mask();
        Arc::new(d)
    };

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    // Stand up K real gRPC shard servers over the SHARED frozen dict/norm.
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    for _ in 0..k {
        let addr = free_addr();
        let server = ShardServer::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        rt.spawn(server.serve(addr));
        addrs.push(addr);
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    // Assemble the gRPC-backed cluster and load the corpus OVER THE WIRE.
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // Every placement branch is exercised (A, B, C all present), counted over gRPC.
    let cc = cluster.class_counts().expect("class_counts over gRPC");
    assert!(cc[0] > 0, "no class-A queries: {cc:?}");
    assert!(cc[1] > 0, "no class-B queries: {cc:?}");
    assert!(cc[2] > 0, "no class-C (broad) queries: {cc:?}");

    // The differential contract, over gRPC, for every title.
    // TODO(ADR-029): this asserts matched-ID *sets* only — it does NOT verify the 11
    // round-tripped `MatchStats` fields, so a transposition in `cluster/proto.rs`'s wire
    // map would go undetected. Add a stats round-trip assertion (cheap, high-value).
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate over gRPC")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "gRPC cluster vs brute-force oracle on {title:?}"
        );
        assert_eq!(
            got, ref_broad[i],
            "gRPC cluster vs single-node on {title:?}"
        );

        let got_sel: HashSet<u64> = cluster
            .percolate_with_broad(title, false)
            .expect("percolate (broad off) over gRPC")
            .into_iter()
            .collect();
        assert_eq!(
            got_sel, ref_selective[i],
            "gRPC cluster broad=off vs single-node selective on {title:?}"
        );
    }

    // Exercise the live-write RPCs end-to-end: add a class-A query, find it, remove it.
    let qid = 7_777_001u64;
    let placed = cluster
        .add_query(qid, "1994 upper deck rareplayer0")
        .expect("add_query over gRPC");
    assert!(
        matches!(placed, reverse_rusty::cluster::AddOutcome::Placed { .. }),
        "expected class-A Placed, got {placed:?}"
    );
    let live_title = "1994 upper deck rareplayer0 psa 10";
    assert!(
        cluster
            .percolate(live_title)
            .expect("percolate live")
            .contains(&qid),
        "a gRPC live-added query must match"
    );
    let removed = cluster.remove_query(qid).expect("remove_query over gRPC");
    assert!(
        removed >= 1,
        "remove should tombstone the holding shard, got {removed}"
    );
    assert!(
        !cluster
            .percolate(live_title)
            .expect("percolate after remove")
            .contains(&qid),
        "a removed query must no longer match over gRPC"
    );
}
