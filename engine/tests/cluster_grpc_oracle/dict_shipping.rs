//! Dict shipping (ADR-034/029): shard servers start **pending** (dict-less) and the
//! coordinator SHIPS its frozen dict via `AdoptDict` at connect — the dict-shipped cluster
//! must still equal the brute oracle, and connecting to a server already populated under a
//! DIVERGENT dict must fail loud with `DictMismatch` (never a silent false negative).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError, ShardServer};
use reverse_rusty::compile::extract;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::segment::{Engine, MatchScratch};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

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
        empty_tag_dict(),
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
        empty_tag_dict(),
        &cfg,
        &[format!("http://{addr}")],
        rt.handle(),
    ) {
        Err(ShardError::DictMismatch { .. }) => {} // the handshake fired — correct.
        Err(other) => panic!("expected DictMismatch, got a different error: {other:?}"),
        Ok(_) => panic!("connect SUCCEEDED against a divergent dict — the silent-FN guard failed"),
    }
}

/// ADR-077 (criterion 9): the recovery/lease/fence handshakes verify the TAG-dict
/// fingerprint exactly like the feature dict's — a divergent tag space would silently
/// mis-filter (segments carry resolved `TagId`s; translog tags re-resolve against the
/// receiver's space), so every guarded RPC refuses it loudly. Three layers probed:
/// the bare `connect` handshake (the probe reply now attests the tag space — a wrong
/// expectation, or a pre-ADR-077 server's zero, fails the connect), then raw
/// wrong-tag-fingerprint `Fence`/`Unfence`/`RetentionLease`/`FetchTranslog` RPCs
/// against a live server (each `failed_precondition`, naming the tag space), with the
/// correct-fingerprint fence accepted as the control. The durable pair
/// (`FetchSegments`/`RecoverFrom`) runs the identical two-line check, and its
/// happy path is exercised by every existing peer-recovery oracle now that
/// `RemoteShard` presents the tag fingerprint on all guarded RPCs.
#[test]
fn grpc_recovery_handshakes_reject_divergent_tag_dict() {
    use reverse_rusty_shard_proto as raw;

    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let dict_fp = dict.fingerprint();
    let tag_fp = empty_tag_dict().fingerprint(); // ShardServer::new's finalized empty space
    let wrong_tag_fp = tag_fp ^ 0xBEEF_CAFE;

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let addr = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("local_addr");
        let server = ShardServer::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        server.ingest_dsl(&[(1u64, "1994 upper deck".to_string())]);
        rt.spawn(server.serve_with_incoming(incoming));
        addr
    };
    wait_until_listening(addr);
    let endpoint = format!("http://{addr}");

    // 1. The bare connect handshake: a wrong tag expectation fails LOUD. (The same
    // arm covers a pre-ADR-077 server: its probe reply leaves the field 0, which can
    // never equal a real fingerprint.)
    match reverse_rusty::cluster::RemoteShard::connect(
        &endpoint,
        rt.handle().clone(),
        dict_fp,
        wrong_tag_fp,
        0,
    ) {
        Err(e) => assert!(
            e.to_string().contains("tag-dict"),
            "the connect refusal names the tag space (got: {e})"
        ),
        Ok(_) => panic!("connect SUCCEEDED against a divergent tag dict"),
    }
    // Control: the correct pair connects.
    reverse_rusty::cluster::RemoteShard::connect(
        &endpoint,
        rt.handle().clone(),
        dict_fp,
        tag_fp,
        0,
    )
    .expect("connect with the matching tag fingerprint");

    // 2. Raw RPCs with the RIGHT dict fingerprint but a WRONG tag fingerprint: each
    // guarded handler refuses with failed_precondition naming the tag space.
    rt.block_on(async {
        let mut client = raw::shard_service_client::ShardServiceClient::connect(endpoint.clone())
            .await
            .expect("raw client connect");

        let fence_err = client
            .fence(raw::FenceRequest {
                generation: 1,
                dict_fingerprint: dict_fp,
                tag_dict_fingerprint: wrong_tag_fp,
                shard_id: 0,
                placement_generation: 1,
                num_shards: 1,
            })
            .await
            .expect_err("Fence must refuse a divergent tag dict");
        assert_eq!(fence_err.code(), tonic::Code::FailedPrecondition);
        assert!(fence_err.message().contains("tag-dict"), "{fence_err}");

        let unfence_err = client
            .unfence(raw::UnfenceRequest {
                generation: 1,
                dict_fingerprint: dict_fp,
                tag_dict_fingerprint: wrong_tag_fp,
                shard_id: 0,
                placement_generation: 1,
                num_shards: 1,
            })
            .await
            .expect_err("Unfence must refuse a divergent tag dict");
        assert_eq!(unfence_err.code(), tonic::Code::FailedPrecondition);

        let lease_err = client
            .retention_lease(raw::RetentionLeaseRequest {
                op: 0,
                lease_id: 0,
                pos: 0,
                dict_fingerprint: dict_fp,
                tag_dict_fingerprint: wrong_tag_fp,
                shard_id: 0,
                placement_generation: 1,
                num_shards: 1,
            })
            .await
            .expect_err("RetentionLease must refuse a divergent tag dict");
        assert_eq!(lease_err.code(), tonic::Code::FailedPrecondition);

        let translog_err = client
            .fetch_translog(raw::FetchTranslogRequest {
                after_seqno: 0,
                dict_fingerprint: dict_fp,
                tag_dict_fingerprint: wrong_tag_fp,
                shard_id: 0,
                placement_generation: 1,
                num_shards: 1,
            })
            .await
            .expect_err("FetchTranslog must refuse a divergent tag dict");
        assert_eq!(translog_err.code(), tonic::Code::FailedPrecondition);

        // Control: the correct pair is accepted (the fence actually applies).
        let ok = client
            .fence(raw::FenceRequest {
                generation: 1,
                dict_fingerprint: dict_fp,
                tag_dict_fingerprint: tag_fp,
                shard_id: 0,
                placement_generation: 1,
                num_shards: 1,
            })
            .await
            .expect("fence with the matching tag fingerprint")
            .into_inner();
        assert_eq!(ok.fenced_at_generation, 1);
    });
}
