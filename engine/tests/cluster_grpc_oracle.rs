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

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reverse_rusty::cluster::{
    AddOutcome, AutoscaleConfig, ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole,
    ScalingAction, ShardError, ShardGroup, ShardServer,
};
use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::events::{DurabilityOp, EngineEvent};
use reverse_rusty::gen::{generate, GenConfig, BRANDS};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use tonic::transport::server::TcpIncoming;

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

/// The brute oracle's match set for every title over a given query list.
fn build_oracle(queries: &[(u64, String)], titles: &[String]) -> Vec<HashSet<u64>> {
    let brute = Brute::build(queries);
    let mut lc = String::new();
    let mut feats = Vec::new();
    titles
        .iter()
        .map(|t| brute.matches(t, &mut lc, &mut feats))
        .collect()
}

/// One authoritative frozen dict interned over `queries` (the coordinator's feature space).
fn frozen_dict_over(queries: &[(u64, String)], norm: &Normalizer) -> Arc<Dict> {
    let mut d = Dict::new();
    let mut lc = String::new();
    for (_id, text) in queries {
        if let Ok(ast) = reverse_rusty::dsl::parse(text) {
            let _ = extract(&ast, norm, &mut d, &mut lc);
        }
    }
    d.finalize_mask();
    Arc::new(d)
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

    // Stand up K real gRPC shard servers over the SHARED frozen dict/norm. Each binds its
    // ephemeral port ONCE (via `TcpIncoming`) and serves on that same socket — no
    // bind→drop→rebind window for another process to steal the port (the old CI flake).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        // `TcpIncoming::bind` -> `TcpListener::from_std` registers with the reactor, so it
        // must run inside the runtime context; scope the guard so the later `connect_remote`
        // (which `block_on`s) still runs OUTSIDE it, as before.
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                EngineConfig::default(),
            );
            rt.spawn(server.serve_with_incoming(incoming));
        }
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

    // A local (in-process) cluster over the SAME corpus + config: identical placement and
    // routing, so its merged `MatchStats` must equal the gRPC cluster's for every title. A
    // transposition in `cluster/proto.rs`'s wire map shows up as a stats mismatch here (the
    // proto.rs unit test catches it directly; this is the end-to-end backstop).
    let local = ClusterEngine::build(vocab(), &cfg, &queries).expect("build local cluster");

    // The differential contract, over gRPC, for every title — matched ids AND the
    // round-tripped MatchStats.
    for (i, title) in titles.iter().enumerate() {
        let (ids, grpc_stats) = cluster
            .percolate_with_stats(title)
            .expect("percolate over gRPC");
        let got: HashSet<u64> = ids.into_iter().collect();
        assert_eq!(
            got, oracle[i],
            "gRPC cluster vs brute-force oracle on {title:?}"
        );
        assert_eq!(
            got, ref_broad[i],
            "gRPC cluster vs single-node on {title:?}"
        );

        let (_, local_stats) = local
            .percolate_with_stats(title)
            .expect("percolate local cluster");
        assert_eq!(
            grpc_stats, local_stats,
            "gRPC vs local-cluster MatchStats (wire round-trip) on {title:?}"
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

/// Guard: the `RemoteShard` sync→async `block_on` bridge ([`remote.rs`] lines 9-14) must be
/// drivable from a rayon fan-out without a nested-runtime panic. `percolate_inner`
/// parallelizes the per-shard probes with rayon `par_iter` when a title routes to >1 shard,
/// so each `RemoteShard::percolate` runs `Handle::block_on` on a *rayon worker* thread —
/// which is safe precisely because rayon workers are NOT tokio runtime threads. This is the
/// only place that arrangement is asserted to be load-bearing: a future refactor that drives
/// the fan-out from inside an async context (or onto tokio's own threads) would re-introduce
/// the nested-runtime panic and fail loudly HERE, even if the broader oracle were weakened.
#[test]
fn remote_fanout_block_on_does_not_panic_on_rayon_workers() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    // ONE authoritative frozen feature space, shared into every server + the coordinator.
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    // Stand up K real gRPC shard servers over the shared dict/norm (same pattern as the
    // oracle above: bind inside the runtime, connect — which `block_on`s — outside it).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                EngineConfig::default(),
            );
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // The guard only bites on titles that route to >1 shard (the rayon-parallel branch); a
    // single-target title takes the sequential branch and never parks `block_on` on a worker.
    // Drive every multi-shard title and assert (a) no panic and (b) the brute-oracle set.
    let mut covered = 0usize;
    for (i, title) in titles.iter().enumerate() {
        if cluster.shard_fanout(title).len() < 2 {
            continue;
        }
        covered += 1;
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("multi-shard percolate must not panic on the block_on bridge")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "rayon-fanout block_on bridge vs brute oracle on {title:?}"
        );
    }
    assert!(
        covered > 0,
        "guard needs >=1 title routing to >=2 shards (the rayon-parallel `par_iter` branch)"
    );
}

/// Guard (ADR-047): the `block_on` bridge must ALSO be safe on the SEQUENTIAL single-target
/// path — a title routing to only shard 0 (`fanout == 1`) skips the rayon `par_iter` branch and
/// runs `RemoteShard::percolate` directly on the CALLER's thread. If that caller is a tokio
/// runtime worker (a future async coordinator probing `percolate` from an axum/tonic handler),
/// a naive `Handle::block_on` panics with the nested-runtime error. `block_on_in_context`
/// detects the multi-thread worker and re-enters via `block_in_place` instead. This is exactly
/// the case the rayon guard above SKIPS (`fanout < 2`), so it is asserted here: a single-target
/// percolate is driven from INSIDE a spawned task on the runtime and must not panic + must equal
/// the brute-oracle set.
#[test]
fn remote_single_target_percolate_safe_from_tokio_worker() {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                EngineConfig::default(),
            );
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // Single-target titles route to shard 0 only — the sequential (non-rayon) probe path.
    let single: Vec<usize> = titles
        .iter()
        .enumerate()
        .filter(|(_, t)| cluster.shard_fanout(t).len() == 1)
        .map(|(i, _)| i)
        .collect();
    assert!(
        !single.is_empty(),
        "guard needs >=1 single-target title (the sequential block_on path)"
    );

    // Drive each single-target percolate from INSIDE a tokio worker: `spawn` runs the closure
    // on a multi-thread worker, so `RemoteShard::percolate`'s `block_on` executes in a runtime
    // context — the exact arrangement that panics without `block_in_place`. A panic surfaces as
    // a `JoinError` (the first `expect`), so the guard bites even though the seam is sync.
    let cluster = Arc::new(cluster);
    for i in single {
        let title = titles[i].clone();
        let c = Arc::clone(&cluster);
        let got: HashSet<u64> = rt
            .block_on(async move { tokio::task::spawn(async move { c.percolate(&title) }).await })
            .expect("spawned percolate task must not panic on the block_on bridge")
            .expect("single-target percolate over gRPC")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "single-target block_on bridge (from tokio worker) vs brute oracle on title #{i}"
        );
    }
}

/// Partial-apply DETECTION over the real wire (ADR-047): when a selective add's target shard
/// server is down, the fan-out write must surface as [`ShardError::PartiallyApplied`] (NOT a
/// swallowed error or a silent half-write), emit a `ClusterPartialApply` durability event, and
/// queue the failed shard for repair. (Convergence — `resync` re-driving once the shard is back —
/// is proven deterministically by the in-process `partial_apply_is_detected_then_resync_converges`
/// unit test; reconnect-to-a-restarted-server is out of scope for this wire-level detection test.)
#[test]
fn grpc_partial_apply_is_detected_and_queued() {
    let (queries, _titles) = build_corpus();

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let k = 3usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            let server = ShardServer::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                EngineConfig::default(),
            );
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    // Capture durability events so we can assert the partial-apply event fires over the wire.
    let events: Arc<Mutex<Vec<EngineEvent>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = Arc::clone(&events);
        cluster.set_observer(Arc::new(move |ev: &EngineEvent| {
            sink.lock().unwrap().push(ev.clone());
        }));
    }

    // A single out-of-dict required term ⇒ a synthetic (freq-0, never-hot) feature ⇒ class A ⇒
    // selective placement on exactly one shard. Discover that shard via a healthy add, then free
    // the id so the failing case re-uses the same placement.
    let dsl = "zznovelaterm";
    let target = match cluster
        .add_query(900_001, dsl)
        .expect("healthy selective add over gRPC")
    {
        AddOutcome::Placed { shards } => {
            assert_eq!(
                shards.len(),
                1,
                "a synthetic single-term query must be single-shard selective: {shards:?}"
            );
            shards[0]
        }
        other => panic!("expected selective Placed, got {other:?}"),
    };
    cluster.remove_query(900_001).expect("remove probe query");

    // FENCE the target shard's server so it REJECTS writes (`failed_precondition`) while staying
    // connected — a deterministic transient write failure. (Aborting the serve task would NOT do
    // it: tonic's per-connection handler tasks outlive the accept loop, so the cluster's existing
    // HTTP/2 connection keeps serving.) A separate client flips the server-side fence flag, which
    // every client to that server then observes.
    let fencer = reverse_rusty::cluster::RemoteShard::connect(
        endpoints[target].clone(),
        rt.handle().clone(),
        dict.fingerprint(),
    )
    .expect("connect fencer to target server");
    fencer.fence(1).expect("fence target server");

    match cluster.add_query(900_002, dsl) {
        Err(ShardError::PartiallyApplied {
            logical,
            applied,
            failed,
            ..
        }) => {
            assert_eq!(logical, 900_002);
            assert_eq!(
                failed,
                vec![target],
                "the downed shard must be the one reported failed"
            );
            assert!(
                applied.is_empty(),
                "a single-target add applies nowhere when its shard is down: {applied:?}"
            );
        }
        other => {
            panic!("expected PartiallyApplied after the target shard went down, got {other:?}")
        }
    }
    assert_eq!(
        cluster.pending_repairs(),
        1,
        "the failed mutation must be queued for repair"
    );
    assert!(
        events.lock().unwrap().iter().any(|e| matches!(
            e,
            EngineEvent::DurabilityFailure {
                op: DurabilityOp::ClusterPartialApply,
                ..
            }
        )),
        "a ClusterPartialApply durability event must be emitted over the wire too"
    );
}

/// Dict shipping (ADR-034): the shard servers start **pending** (dict-less) — NOT pre-built
/// over the corpus — and the coordinator SHIPS its authoritative frozen dict to each at
/// connect. The dict-shipped cluster must still return exactly the brute oracle's and the
/// single-node engine's sets (broad on/off). This proves a data node no longer needs the
/// corpus / out-of-band dict matching: only `norm` (`default_vocab()`) is arranged out-of-band.
#[test]
fn grpc_cluster_with_dict_shipping() {
    let (queries, titles) = build_corpus();

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

    // The coordinator owns the ONE authoritative frozen dict (built over the corpus). The
    // shard servers do NOT — they start dict-less and receive it via AdoptDict.
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

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(k);
    {
        let _enter = rt.enter();
        for _ in 0..k {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            // PENDING: no dict. Only `norm` is shared out-of-band (default_vocab); the dict
            // arrives over the wire during connect_remote.
            let server = ShardServer::pending(Arc::clone(&norm), EngineConfig::default());
            rt.spawn(server.serve_with_incoming(incoming));
        }
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }
    let endpoints: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    // connect_remote SHIPS the dict to each pending server (the behavior under test), then
    // the corpus loads over the wire and compiles against the adopted dict.
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        &endpoints,
        rt.handle(),
    )
    .expect("connect remote cluster ships the dict to pending servers");
    cluster.ingest(&queries).expect("ingest corpus over gRPC");

    let cc = cluster.class_counts().expect("class_counts over gRPC");
    assert!(
        cc[0] > 0 && cc[1] > 0 && cc[2] > 0,
        "every placement class must be exercised: {cc:?}"
    );

    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate over gRPC")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "dict-shipped cluster vs brute oracle on {title:?}"
        );
        assert_eq!(
            got, ref_broad[i],
            "dict-shipped cluster vs single-node on {title:?}"
        );

        let got_sel: HashSet<u64> = cluster
            .percolate_with_broad(title, false)
            .expect("percolate (broad off) over gRPC")
            .into_iter()
            .collect();
        assert_eq!(
            got_sel, ref_selective[i],
            "dict-shipped cluster broad=off vs single-node selective on {title:?}"
        );
    }
}

/// Build a small frozen dict from a fixed base plus `extra` DSL snippets (interned in
/// order against `norm`). Two dicts built with different `extra` have different
/// fingerprints — the divergence the handshake must catch.
fn frozen_dict_with(extra: &[&str], norm: &Normalizer) -> Arc<Dict> {
    let mut d = Dict::new();
    let mut lc = String::new();
    let base = ["1994 upper deck", "psa 10", "topps chrome"];
    for q in base.iter().copied().chain(extra.iter().copied()) {
        if let Ok(ast) = reverse_rusty::dsl::parse(q) {
            let _ = extract(&ast, norm, &mut d, &mut lc);
        }
    }
    d.finalize_mask();
    Arc::new(d)
}

/// Dict shipping + the divergence guard (ADR-034/029): connecting to a server that already
/// holds DATA under a divergent dict MUST fail loud with `DictMismatch`, not silently drop
/// matches. Shipping *adopts* onto an EMPTY server (the happy path the test above covers), so
/// the guard fires only once a server has committed to a feature space — here the server is
/// populated under `dict_server` while the coordinator ships `dict_coord`. The server refuses
/// the adopt (`FailedPrecondition`) and the client surfaces it as `DictMismatch`.
#[test]
fn grpc_connect_rejects_divergent_dict() {
    let norm = Arc::new(vocab());
    let dict_server = frozen_dict_with(&[], &norm);
    let dict_coord = frozen_dict_with(&["1995 fleer ultra"], &norm);
    assert_ne!(
        dict_server.fingerprint(),
        dict_coord.fingerprint(),
        "test setup: the two dicts must differ for the handshake to have anything to catch"
    );

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let addr = {
        // Bind in-context (see the main test), then drop the guard so `connect_remote`
        // below `block_on`s outside the runtime context.
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("local_addr");
        let server = ShardServer::new(
            Arc::clone(&norm),
            Arc::clone(&dict_server),
            EngineConfig::default(),
        );
        // Load data so the shard is NON-EMPTY under dict_server. Shipping would happily adopt
        // onto an empty server; the divergence guard only fires once data depends on a dict.
        server.ingest_dsl(&[(1u64, "1994 upper deck".to_string())]);
        rt.spawn(server.serve_with_incoming(incoming));
        addr
    };
    wait_until_listening(addr);

    let cfg = ClusterConfig {
        num_shards: 1,
        ..ClusterConfig::default()
    };
    // `ClusterEngine` is not `Debug`, so match rather than `expect_err` (which would print
    // the unexpected `Ok`).
    match ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict_coord),
        &cfg,
        &[format!("http://{addr}")],
        rt.handle(),
    ) {
        Err(ShardError::DictMismatch { .. }) => {} // the handshake fired — correct.
        Err(other) => panic!("expected DictMismatch, got a different error: {other:?}"),
        Ok(_) => panic!("connect SUCCEEDED against a divergent dict — the silent-FN guard failed"),
    }
}

/// Block until `addr` stops accepting TCP (the server has gone) or time out.
fn wait_until_not_listening(addr: SocketAddr) {
    for _ in 0..300 {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server at {addr} never stopped listening");
}

/// A unique, freshly-cleaned data dir for one durable shard server.
fn server_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rr_grpc_rep_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Per-shard replication + peer recovery over gRPC (ADR-035/036). Stands up K positions × RF=2
/// **durable** shard servers, builds the cluster via `connect_replicated`, and proves three
/// things end-to-end across the wire:
///   1. the replicated gRPC cluster ≡ the independent brute oracle;
///   2. stopping a primary still serves correct reads via its replica (FAILOVER — and, since
///      ingest fanned out to the replica, this also proves the write fan-out reached it);
///   3. a fresh node PEER-RECOVERS a position's sealed segments from a live peer (FetchSegments
///      over the wire) and then serves that position correctly inside a cluster.
#[test]
fn grpc_replicated_failover_and_peer_recovery() {
    let (queries, titles) = build_corpus();

    // Independent ground truth (brute oracle), broad on.
    let brute = Brute::build(&queries);
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut oracle: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    for t in &titles {
        oracle.push(brute.matches(t, &mut blc, &mut bfeats));
    }

    // ONE authoritative frozen dict at the coordinator; the servers start dict-less.
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
    let rf = 2usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    // Per position: a primary + (rf-1) replica `pending_durable` servers, each with its own dir.
    let mut groups: Vec<ShardGroup> = Vec::with_capacity(k);
    let mut primary_handles = Vec::with_capacity(k);
    let mut all_addrs: Vec<Vec<SocketAddr>> = Vec::with_capacity(k);
    let mut dirs: Vec<PathBuf> = Vec::new();
    {
        let _enter = rt.enter();
        for p in 0..k {
            let mut pos_addrs: Vec<SocketAddr> = Vec::with_capacity(rf);
            let mut replica_eps: Vec<String> = Vec::new();
            let mut primary_jh = None;
            for c in 0..rf {
                let dir = server_dir(&format!("{p}_{c}"));
                let incoming =
                    TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
                let addr = incoming.local_addr().expect("local_addr");
                let server = ShardServer::pending_durable(
                    Arc::clone(&norm),
                    EngineConfig::default(),
                    dir.clone(),
                );
                let jh = rt.spawn(server.serve_with_incoming(incoming));
                if c == 0 {
                    primary_jh = Some(jh); // keep the primary handle so we can stop it (failover)
                }
                // Replica handles are dropped — dropping a JoinHandle does NOT stop the task.
                pos_addrs.push(addr);
                if c > 0 {
                    replica_eps.push(format!("http://{addr}"));
                }
                dirs.push(dir);
            }
            groups.push(ShardGroup {
                primary: format!("http://{}", pos_addrs[0]),
                replicas: replica_eps,
            });
            primary_handles.push(primary_jh.expect("primary spawned"));
            all_addrs.push(pos_addrs);
        }
    }
    for addrs in &all_addrs {
        for &a in addrs {
            wait_until_listening(a);
        }
    }

    // Assemble the replicated gRPC cluster and load the corpus over the wire (the coordinator
    // ships the dict to every endpoint; ingest fans each bucket to the primary AND its replica).
    let cluster = ClusterEngine::connect_replicated(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        &groups,
        rt.handle(),
    )
    .expect("connect replicated cluster");
    cluster
        .ingest(&queries)
        .expect("ingest over gRPC (fans to primary + replica)");

    let cc = cluster.class_counts().expect("class_counts over gRPC");
    assert!(
        cc[0] > 0 && cc[1] > 0 && cc[2] > 0,
        "every placement class must be exercised: {cc:?}"
    );

    // (1) The replicated gRPC cluster ≡ the brute oracle.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "replicated gRPC cluster vs brute on {title:?}"
        );
    }

    // (2) FAILOVER: stop position 0's primary. Every title probes position 0 (the replicated
    // broad lane), so every read must now fail over to position 0's replica and still match.
    primary_handles[0].abort();
    wait_until_not_listening(all_addrs[0][0]);
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after primary stop")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "failover read vs brute on {title:?}");
    }

    // (3) PEER RECOVERY: a fresh durable node pulls position 1's sealed segments from its live
    // primary, then serves position 1 correctly inside a verify cluster.
    let fresh_dir = server_dir("fresh");
    let fresh_addr = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("local_addr");
        let server = ShardServer::pending_durable(
            Arc::clone(&norm),
            EngineConfig::default(),
            fresh_dir.clone(),
        );
        rt.spawn(server.serve_with_incoming(incoming));
        addr
    };
    dirs.push(fresh_dir);
    wait_until_listening(fresh_addr);

    let src_ep = format!("http://{}", all_addrs[1][0]); // position 1's live primary (durable)
    let tgt_ep = format!("http://{fresh_addr}");
    let (recovered_n, _hwm) = cluster
        .peer_recover_replica(&src_ep, &tgt_ep, rt.handle())
        .expect("peer recovery over gRPC");

    // Parity: the recovered node holds exactly position 1's query count.
    let pos1_count = cluster.shard_query_counts().expect("counts")[1];
    assert_eq!(
        recovered_n as usize, pos1_count,
        "recovered node query count {recovered_n} != source position's {pos1_count}"
    );

    // The recovered node serves position 1 correctly *inside a cluster*: a connect_remote (RF=1)
    // cluster with position 0 served by its still-live replica (its primary was stopped),
    // position 1 by the RECOVERED node, position 2 by its primary — must equal the brute oracle.
    let verify_eps = vec![
        format!("http://{}", all_addrs[0][1]), // pos 0 replica (primary was stopped)
        tgt_ep,                                // pos 1 recovered node
        format!("http://{}", all_addrs[2][0]), // pos 2 primary
    ];
    let verify = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        &verify_eps,
        rt.handle(),
    )
    .expect("verify cluster over the recovered node");
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = verify
            .percolate(title)
            .expect("verify percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "recovered-node cluster vs brute on {title:?}"
        );
    }

    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// No-quiesce peer recovery (ADR-039, clustering step 5c) — the headline. A durable SOURCE node
/// is recovered onto a fresh TARGET by streaming its sealed segments at snapshot position `P`,
/// and the writes that land AFTER `P` are replayed from the per-shard TRANSLOG TAIL
/// (`FetchTranslog`) rather than lost — so peer recovery need NOT quiesce writes (closing
/// ADR-036's documented gap). Deterministic by ordering (snapshot → write → tail catch-up),
/// which exercises the exact path a concurrent recovery uses for writes during the copy window;
/// the pre-catch-up staleness assertion proves the writes truly post-date the snapshot. The
/// recovered node converges to BOTH the live source AND an independent brute oracle over the
/// final live set — zero false negatives across the wire.
#[test]
fn grpc_peer_recovery_without_quiescing() {
    let (queries, titles) = build_corpus();
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    // The snapshot ground truth, and the set of query ids that actually match ≥1 title (so the
    // post-snapshot mutations provably move title results — many corpus queries match nothing).
    let oracle_corpus = build_oracle(&queries, &titles);
    let matched: Vec<u64> = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        let mut v: Vec<u64> = s.into_iter().collect();
        v.sort_unstable();
        v
    };
    assert!(
        matched.len() >= 20,
        "corpus must match ≥20 distinct queries to mutate; got {}",
        matched.len()
    );

    // Mutations applied AFTER the snapshot: REMOVE 10 title-matching queries (their ids vanish
    // from results) and ADD 10 copies of OTHER title-matching queries (new ids appear in results).
    let removed_ids: Vec<u64> = matched.iter().take(10).copied().collect();
    let additions: Vec<(u64, String)> = matched
        .iter()
        .skip(10)
        .take(10)
        .map(|id| {
            let nid = next_id;
            next_id += 1;
            (nid, by_id[id].clone())
        })
        .collect();
    let removed_set: HashSet<u64> = removed_ids.iter().copied().collect();
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .filter(|(id, _)| !removed_set.contains(id))
        .cloned()
        .chain(additions.iter().cloned())
        .collect();

    // The final ground truth MUST differ from the snapshot, else the tail never mattered.
    let oracle_final = build_oracle(&final_live, &titles);
    assert!(
        oracle_corpus != oracle_final,
        "test setup: the post-snapshot mutations must change some title results"
    );

    // ONE authoritative frozen dict (over the corpus; the additions reuse corpus DSLs, so their
    // vocab is already present).
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // A single durable SOURCE node + a fresh durable TARGET node (both pending → adopt the dict).
    let src_dir = server_dir("nq_src");
    let tgt_dir = server_dir("nq_tgt");
    let (src_addr, tgt_addr) = {
        let _enter = rt.enter();
        let si = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind src");
        let sa = si.local_addr().expect("src addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                src_dir.clone(),
            )
            .serve_with_incoming(si),
        );
        let ti = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tgt");
        let ta = ti.local_addr().expect("tgt addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                tgt_dir.clone(),
            )
            .serve_with_incoming(ti),
        );
        (sa, ta)
    };
    wait_until_listening(src_addr);
    wait_until_listening(tgt_addr);
    let src_ep = format!("http://{src_addr}");
    let tgt_ep = format!("http://{tgt_addr}");

    // Coordinator over the source; load the corpus (→ source segments; the translog stays empty,
    // since bulk ingest writes a base segment directly).
    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        std::slice::from_ref(&src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");

    // (1) SNAPSHOT: recover the fresh target from the source. The bulk copy is at position P; the
    // initial tail is empty (no post-snapshot writes yet), so hwm == P.
    let (_n, hwm) = cluster
        .peer_recover_replica(&src_ep, &tgt_ep, rt.handle())
        .expect("peer recovery");

    // A verify cluster over the recovered target alone, to read its state directly.
    let verify = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        std::slice::from_ref(&tgt_ep),
        rt.handle(),
    )
    .expect("connect verify cluster");

    // Pre-catch-up the target reflects the SNAPSHOT (the corpus) — proving the subsequent writes
    // truly post-date it (else this would equal the final state and the test would be trivial).
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = verify
            .percolate(title)
            .expect("verify pre-catch-up")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_corpus[i],
            "target must equal the snapshot pre-catch-up on {title:?}"
        );
    }

    // (2) WRITES land on the source AFTER the snapshot (into its translog tail, > P).
    for id in &removed_ids {
        cluster.remove_query(*id).expect("remove on source");
    }
    for (id, dsl) in &additions {
        cluster.add_query(*id, dsl).expect("add on source");
    }

    // (3) TAIL CATCH-UP: replay the source's translog tail (> hwm) into the target — no segment
    // re-copy, no quiesce. Loop to a fixed point (writes are done, so it converges at once).
    let mut cursor = hwm;
    loop {
        let next = cluster
            .catch_up_recovered_replica(&src_ep, &tgt_ep, cursor, rt.handle())
            .expect("catch up tail");
        if next == cursor {
            break;
        }
        cursor = next;
    }

    // (4) The recovered target now equals the live source AND the independent brute oracle over
    // the FINAL live set, on every title — zero false negatives after a no-quiesce recovery.
    for (i, title) in titles.iter().enumerate() {
        let tgt: HashSet<u64> = verify
            .percolate(title)
            .expect("verify post-catch-up")
            .into_iter()
            .collect();
        let src: HashSet<u64> = cluster
            .percolate(title)
            .expect("source percolate")
            .into_iter()
            .collect();
        assert_eq!(
            tgt, oracle_final[i],
            "recovered target vs brute(final) on {title:?}"
        );
        assert_eq!(
            src, oracle_final[i],
            "live source vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&tgt_dir);
}

/// ADR-040 (retention + finalize): peer recovery under SUSTAINED, concurrent writes. Unlike
/// `grpc_peer_recovery_without_quiescing` (ordered snapshot → write → catch-up), here a writer
/// thread streams adds onto the source CONCURRENTLY with the recovery. The recovery holds a
/// translog RETENTION LEASE across its segment copy + convergence loop, so the racing writes are
/// neither trimmed by the copy's seal nor lost; its bounded loop drains what it can while writes
/// continue. After the writer finishes (no further seals), a final lease-free catch-up — now
/// race-free — converges the target to BOTH the live source AND the brute oracle over the final
/// live set: zero false negatives across the wire with writes never paused.
#[test]
fn grpc_peer_recovery_converges_under_sustained_writes() {
    let (queries, titles) = build_corpus();
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    let oracle_corpus = build_oracle(&queries, &titles);
    let matched: Vec<u64> = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        let mut v: Vec<u64> = s.into_iter().collect();
        v.sort_unstable();
        v
    };
    assert!(
        matched.len() >= 20,
        "need ≥20 matching queries; got {}",
        matched.len()
    );

    // The writer's known add sequence: 20 copies of matching DSLs with fresh ids → a deterministic
    // final live set. A pure stream of adds (no removes) keeps it a clean firehose racing the copy.
    let additions: Vec<(u64, String)> = matched
        .iter()
        .take(20)
        .map(|id| {
            let nid = next_id;
            next_id += 1;
            (nid, by_id[id].clone())
        })
        .collect();
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain(additions.iter().cloned())
        .collect();
    let oracle_final = build_oracle(&final_live, &titles);
    assert!(
        oracle_corpus != oracle_final,
        "test setup: the concurrent adds must change some title results"
    );

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let src_dir = server_dir("sw_src");
    let tgt_dir = server_dir("sw_tgt");
    let (src_addr, tgt_addr) = {
        let _enter = rt.enter();
        let si = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind src");
        let sa = si.local_addr().expect("src addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                src_dir.clone(),
            )
            .serve_with_incoming(si),
        );
        let ti = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tgt");
        let ta = ti.local_addr().expect("tgt addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                tgt_dir.clone(),
            )
            .serve_with_incoming(ti),
        );
        (sa, ta)
    };
    wait_until_listening(src_addr);
    wait_until_listening(tgt_addr);
    let src_ep = format!("http://{src_addr}");
    let tgt_ep = format!("http://{tgt_addr}");

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        std::slice::from_ref(&src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");

    // Recover the target CONCURRENTLY with a writer streaming the additions onto the source.
    let hwm = std::thread::scope(|s| {
        let cluster_ref = &cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                cluster_ref.add_query(*id, dsl).expect("writer add");
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        // The recovery runs while the writer is mid-stream — its retention lease keeps the racing
        // writes safe and its convergence loop drains what it can.
        let (_n, hwm) = cluster
            .peer_recover_replica(&src_ep, &tgt_ep, rt.handle())
            .expect("peer recovery under writes");
        writer.join().expect("writer thread");
        hwm
    });

    // Writer done + no further seals ⇒ a final lease-free catch-up is race-free; drain to a fixed
    // point (covers any residual the recovery's bounded loop did not reach while writes raced).
    let mut cursor = hwm;
    loop {
        let next = cluster
            .catch_up_recovered_replica(&src_ep, &tgt_ep, cursor, rt.handle())
            .expect("final catch up");
        if next == cursor {
            break;
        }
        cursor = next;
    }

    let verify = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        std::slice::from_ref(&tgt_ep),
        rt.handle(),
    )
    .expect("connect verify cluster");

    for (i, title) in titles.iter().enumerate() {
        let tgt: HashSet<u64> = verify
            .percolate(title)
            .expect("verify")
            .into_iter()
            .collect();
        let src: HashSet<u64> = cluster
            .percolate(title)
            .expect("source")
            .into_iter()
            .collect();
        assert_eq!(
            tgt, oracle_final[i],
            "recovered target vs brute(final) on {title:?}"
        );
        assert_eq!(
            src, oracle_final[i],
            "live source vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&tgt_dir);
}

/// ADR-044 (live handoff): MOVE a shard from one owner to another while a writer streams adds —
/// no match dropped, reads never paused. Where `grpc_peer_recovery_converges_under_sustained_writes`
/// recovers a *replica* and verifies it out-of-band, this drives the full handoff through
/// `execute_handoff` (peer-recover → FENCE the source → drain to convergence → FLIP routing), then
/// asserts the SAME cluster — its position now re-pointed at the new owner — matches the brute
/// oracle over the final live set. The writer's adds (drained from the source, or landing on the
/// target after the flip, or briefly rejected mid-fence and retried) all converge onto the new owner.
#[test]
fn grpc_live_handoff_under_sustained_writes() {
    let (queries, titles) = build_corpus();
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    let oracle_corpus = build_oracle(&queries, &titles);
    let matched: Vec<u64> = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        let mut v: Vec<u64> = s.into_iter().collect();
        v.sort_unstable();
        v
    };
    assert!(
        matched.len() >= 20,
        "need ≥20 matching queries; got {}",
        matched.len()
    );

    // 20 adds of matching DSLs with fresh ids → a deterministic final live set (a clean firehose).
    let additions: Vec<(u64, String)> = matched
        .iter()
        .take(20)
        .map(|id| {
            let nid = next_id;
            next_id += 1;
            (nid, by_id[id].clone())
        })
        .collect();
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain(additions.iter().cloned())
        .collect();
    let oracle_final = build_oracle(&final_live, &titles);
    assert!(
        oracle_corpus != oracle_final,
        "test setup: the concurrent adds must change some title results"
    );

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let src_dir = server_dir("ho_src");
    let tgt_dir = server_dir("ho_tgt");
    let (src_addr, tgt_addr) = {
        let _enter = rt.enter();
        let si = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind src");
        let sa = si.local_addr().expect("src addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                src_dir.clone(),
            )
            .serve_with_incoming(si),
        );
        let ti = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tgt");
        let ta = ti.local_addr().expect("tgt addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                tgt_dir.clone(),
            )
            .serve_with_incoming(ti),
        );
        (sa, ta)
    };
    wait_until_listening(src_addr);
    wait_until_listening(tgt_addr);
    let src_ep = format!("http://{src_addr}");
    let tgt_ep = format!("http://{tgt_addr}");

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        std::slice::from_ref(&src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");
    assert_eq!(
        cluster.handoff_generations(),
        vec![0],
        "position 0 starts at generation 0 (the source owner)"
    );

    // Run the handoff CONCURRENTLY with a writer streaming the additions through the cluster. The
    // add routes to position 0's CURRENT backing — the source pre-flip, the target post-flip — and is
    // briefly REJECTED in the fence→flip window (the source is fenced, routing not yet flipped); the
    // writer retries until it lands, so no add is lost.
    std::thread::scope(|s| {
        let cluster_ref = &cluster;
        let adds = &additions;
        let writer = s.spawn(move || {
            for (id, dsl) in adds {
                loop {
                    match cluster_ref.add_query(*id, dsl) {
                        Ok(_) => break,
                        // The brief fence→flip window: the fenced source rejects the write
                        // (failed_precondition → ShardError::Remote). Retry — after the flip it lands
                        // on the new owner.
                        Err(ShardError::Remote(_)) => std::thread::sleep(Duration::from_millis(2)),
                        Err(e) => panic!("unexpected writer error: {e}"),
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        });
        cluster
            .execute_handoff(0, &src_ep, &tgt_ep, rt.handle())
            .expect("execute handoff");
        writer.join().expect("writer thread");
    });

    // The flip happened: position 0's generation bumped, and the cluster now routes to the new owner.
    assert_eq!(
        cluster.handoff_generations(),
        vec![1],
        "the handoff bumped position 0's generation"
    );

    // Over EVERY title the cluster (now serving from the new owner) matches the brute oracle over the
    // final live set — zero false negatives across a live data move under concurrent writes.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after handoff")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_final[i],
            "post-handoff cluster vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&tgt_dir);
}

/// ADR-048 (auto-unfence-on-abort): a handoff that aborts AFTER fencing must LIFT the source's
/// fence so it resumes serving writes — otherwise the source is left permanently write-quiesced
/// and needs a manual restart (the gap ADR-044 left). We force the abort deterministically with
/// `handoff_final_drain_cap = 0`: the post-fence drain runs zero passes, so it never "converges"
/// and `execute_handoff` aborts fail-closed (after fencing). The source must then accept a write
/// again, and the cluster — routing unchanged (no flip) — must still match the brute oracle over
/// the live set: zero false negatives despite the failed move.
#[test]
fn grpc_handoff_abort_unfences_source() {
    let (queries, titles) = build_corpus();
    let next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    let by_id: HashMap<u64, String> = queries.iter().map(|(id, d)| (*id, d.clone())).collect();

    // Pick one matching query to re-add under a fresh id AFTER the abort — the write that proves
    // the source unfenced (a still-fenced source would reject it with failed_precondition).
    let oracle_corpus = build_oracle(&queries, &titles);
    let a_match = {
        let mut s: HashSet<u64> = HashSet::new();
        for set in &oracle_corpus {
            s.extend(set);
        }
        *s.iter()
            .min()
            .expect("need ≥1 matching query in the corpus")
    };
    let addition = (next_id, by_id[&a_match].clone());
    let final_live: Vec<(u64, String)> = queries
        .iter()
        .cloned()
        .chain(std::iter::once(addition.clone()))
        .collect();
    let oracle_final = build_oracle(&final_live, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    // Force the post-fence drain to abort immediately: 0 passes ⇒ never converges ⇒ fail-closed.
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        handoff_final_drain_cap: 0,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let src_dir = server_dir("ho_abort_src");
    let tgt_dir = server_dir("ho_abort_tgt");
    let (src_addr, tgt_addr) = {
        let _enter = rt.enter();
        let si = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind src");
        let sa = si.local_addr().expect("src addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                src_dir.clone(),
            )
            .serve_with_incoming(si),
        );
        let ti = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind tgt");
        let ta = ti.local_addr().expect("tgt addr");
        rt.spawn(
            ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                tgt_dir.clone(),
            )
            .serve_with_incoming(ti),
        );
        (sa, ta)
    };
    wait_until_listening(src_addr);
    wait_until_listening(tgt_addr);
    let src_ep = format!("http://{src_addr}");
    let tgt_ep = format!("http://{tgt_addr}");

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        std::slice::from_ref(&src_ep),
        rt.handle(),
    )
    .expect("connect source cluster");
    cluster.ingest(&queries).expect("ingest corpus");

    // The handoff fences the source, then the 0-pass post-fence drain forces a fail-closed abort.
    let err = cluster
        .execute_handoff(0, &src_ep, &tgt_ep, rt.handle())
        .expect_err("handoff must abort with final_drain_cap = 0");
    assert!(
        matches!(err, ShardError::Remote(_)),
        "the abort surfaces as a remote error, got {err:?}"
    );
    // No flip happened: routing is unchanged, position 0 still at generation 0.
    assert_eq!(
        cluster.handoff_generations(),
        vec![0],
        "an aborted handoff must NOT flip routing"
    );

    // The crux (ADR-048): the source AUTO-UNFENCED, so a write lands again. A still-fenced source
    // would reject it with failed_precondition → ShardError::Remote — so `Ok` here IS the proof.
    cluster
        .add_query(addition.0, &addition.1)
        .expect("source must accept writes after the aborted handoff unfenced it");

    // And the cluster still matches the brute oracle over the final live set — the failed move
    // dropped nothing (zero false negatives).
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate after aborted handoff")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle_final[i],
            "post-abort cluster vs brute(final) on {title:?}"
        );
    }

    let _ = std::fs::remove_dir_all(&src_dir);
    let _ = std::fs::remove_dir_all(&tgt_dir);
}

/// ADR-048 (autoscaler-driven handoff): the policy's advisory `Handoff` (ADR-045) is wired to
/// `execute_handoff` and DRIVEN by `tick`. Over a REAL gRPC cluster this exercises the driver's
/// resolution path end-to-end and proves it never breaks matching. The registered nodes carry no
/// endpoint (`addr = None`), so a recommended move can't be physically performed: the driver
/// surfaces a "missing endpoint" event and SKIPS fail-safe — routing is untouched (no flip) and
/// `percolate` stays equal to brute force (zero false negatives). (The happy-path move itself is
/// proven by `grpc_live_handoff_under_sustained_writes`, a direct `execute_handoff`; driving it
/// cleanly from `tick` additionally needs the control-plane node→endpoint map to match the shard
/// endpoints — deployment-model maturity that is Tier-3 residue, see ADR-048.)
#[test]
fn grpc_autoscaler_tick_drives_handoff_resolution_and_preserves_matching() {
    // Several shards so the corpus spreads across nodes; the exact count only needs to be enough
    // that a rebalance lands shards on ≥2 nodes (asserted below).
    const NS: usize = 6;

    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: NS,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // Spin up NS endpoint-less shard servers (the shards' data lives here; the control-plane node
    // addresses are deliberately `None`, see below).
    let dirs: Vec<PathBuf> = (0..NS).map(|i| server_dir(&format!("as_ho_{i}"))).collect();
    let eps: Vec<String> = {
        let _enter = rt.enter();
        let mut eps = Vec::with_capacity(NS);
        for dir in &dirs {
            let inc = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
            let addr = inc.local_addr().expect("addr");
            rt.spawn(
                ShardServer::pending_durable(
                    Arc::clone(&norm),
                    EngineConfig::default(),
                    dir.clone(),
                )
                .serve_with_incoming(inc),
            );
            eps.push(format!("http://{addr}"));
        }
        eps
    };
    for ep in &eps {
        wait_until_listening(ep.trim_start_matches("http://").parse().unwrap());
    }

    let cluster = ClusterEngine::connect_remote(
        Arc::clone(&norm),
        Arc::clone(&dict),
        &cfg,
        &eps,
        rt.handle(),
    )
    .expect("connect cluster");
    cluster.ingest(&queries).expect("ingest");

    // Capture the driver's "missing endpoint" skip event — proof the wiring was reached.
    let saw_skip = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&saw_skip);
    cluster.set_observer(Arc::new(move |ev: &EngineEvent| {
        if let EngineEvent::DurabilityFailure {
            op: DurabilityOp::ReplicaDesync,
            detail,
            ..
        } = ev
        {
            if detail.contains("autoscaler") {
                flag.store(true, std::sync::atomic::Ordering::Release);
            }
        }
    }));

    // Register endpoint-less data nodes and spread the shards across them.
    for id in 1..=3 {
        cluster
            .register_node(NodeDescriptor {
                id: NodeId(id),
                addr: None,
                role: NodeRole::Data,
            })
            .expect("register");
    }
    cluster.rebalance(1).expect("rebalance");

    // Reconcile membership to the ACTUAL assigned set: deregister every member that owns no
    // primary (incl. the genesis node 0). With members == the set of owning nodes, the skew tick
    // fires NO membership rebalance — so the ordering guard does not suppress the handoff, and the
    // driver is genuinely exercised. (HRW need not place every registered node, so this reconcile
    // is what makes the test robust rather than depending on a particular placement.)
    let assigned: std::collections::BTreeSet<u64> = cluster
        .control_state()
        .expect("state")
        .assignments
        .iter()
        .map(|a| a.primary.0)
        .collect();
    for n in cluster.control_state().expect("state").nodes {
        if !assigned.contains(&n.id.0) {
            cluster
                .deregister_node(n.id)
                .expect("deregister unassigned");
        }
    }

    // Derive a skew threshold just under the observed max/mean so node-skew fires deterministically.
    let state = cluster.control_state().expect("state");
    let counts = cluster.shard_query_counts().expect("counts");
    let mut node_load: std::collections::BTreeMap<u64, usize> = std::collections::BTreeMap::new();
    for a in &state.assignments {
        if let Some(&c) = counts.get(a.position as usize) {
            *node_load.entry(a.primary.0).or_default() += c;
        }
    }
    assert!(
        node_load.len() >= 2,
        "the shards must land on ≥2 nodes for node-skew to apply: {node_load:?}"
    );
    let total: usize = node_load.values().sum();
    let mean = total as f64 / node_load.len() as f64;
    let max = *node_load.values().max().unwrap() as f64;
    assert!(
        max > mean,
        "shards must distribute unevenly to skew: {node_load:?}"
    );
    let skew = (max / mean - 0.01).max(1.0 + f64::EPSILON);
    let acfg = AutoscaleConfig {
        enabled: true,
        target_replication_factor: 1,
        max_node_load_skew: skew,
        split_corpus_threshold: 0,
    };

    let decision = cluster.tick(&acfg).expect("tick");
    assert!(
        decision
            .actions
            .iter()
            .any(|a| matches!(a, ScalingAction::Handoff { .. })),
        "skew over threshold recommends a handoff: {decision:?}"
    );
    // The driver reached the resolution path and skipped fail-safe (the target has no endpoint).
    assert!(
        saw_skip.load(std::sync::atomic::Ordering::Acquire),
        "the driver must surface a missing-endpoint event when it can't resolve the move"
    );
    // No flip: a skipped move never re-points a position.
    assert_eq!(
        cluster.handoff_generations(),
        vec![0; NS],
        "a skipped handoff must not flip any position's routing"
    );

    // Zero false negatives: matching is unchanged across the autoscaler tick.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got, oracle[i],
            "cluster vs brute on {title:?} after an autoscaler tick"
        );
    }

    for dir in &dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}
