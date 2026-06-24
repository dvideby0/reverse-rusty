//! ADR-071: the secured mesh — TLS + cluster-token on the shard transport. The
//! positive path proves a fully secured cluster is correctness-identical (≡ brute,
//! live writes over the secured link); the negative paths prove the gate fails LOUD
//! (a wrong/missing token or a plaintext client is an error, never an empty result —
//! the zero-false-negative posture extends to the security layer).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use reverse_rusty::cluster::{
    ClientSecurity, ClusterConfig, ClusterEngine, ServerSecurity, ShardServer, TlsClientConfig,
    TlsServerIdentity,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{generate, GenConfig};
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

const MESH_TOKEN: &[u8] = b"test-mesh-secret-1";

/// One self-signed identity for `localhost` (rcgen, in-test — no key material in the
/// repo). Self-signed ⇒ the leaf doubles as the client's CA.
fn test_identity() -> (Vec<u8>, Vec<u8>) {
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("self-signed");
    (
        cert.cert.pem().into_bytes(),
        cert.key_pair.serialize_pem().into_bytes(),
    )
}

fn server_security(cert_pem: &[u8], key_pem: &[u8]) -> ServerSecurity {
    ServerSecurity {
        tls: Some(TlsServerIdentity {
            cert_pem: cert_pem.to_vec(),
            key_pem: key_pem.to_vec(),
        }),
        token: Some(MESH_TOKEN.to_vec()),
        ..Default::default()
    }
}

fn client_security(ca_pem: &[u8], token: Option<&[u8]>) -> ClientSecurity {
    ClientSecurity {
        tls: Some(TlsClientConfig {
            ca_pem: ca_pem.to_vec(),
            domain: None, // endpoints are https://localhost — the SAN matches directly
        }),
        token: token.map(<[u8]>::to_vec),
        ..Default::default()
    }
}

/// A compact corpus for the secured path (every probe is a TLS RPC — keep it lean
/// while still exercising selective + any-of + broad placement).
fn small_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 800,
        num_titles: 100,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5EC2_2E71,
        num_players: 300,
        num_sets: 150,
    };
    let data = generate(&cfg);
    (data.queries, data.titles)
}

/// Stand up `k` SECURED shard servers (TLS + token) on ephemeral localhost ports,
/// returning their `https://localhost:<port>` endpoints.
fn spawn_secured_servers(
    rt: &tokio::runtime::Runtime,
    k: usize,
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Vec<String> {
    let norm = Arc::new(vocab());
    let mut endpoints = Vec::with_capacity(k);
    let _enter = rt.enter();
    for _ in 0..k {
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("local_addr");
        // Pending (dict-less) servers: the secured coordinator ships the dict through
        // the SAME secured link, so the AdoptDict handshake itself proves the gate.
        let server = ShardServer::pending(Arc::clone(&norm), EngineConfig::default())
            .with_security(server_security(cert_pem, key_pem));
        rt.spawn(server.serve_with_incoming(incoming));
        endpoints.push(format!("https://localhost:{}", addr.port()));
    }
    endpoints
}

#[test]
fn grpc_secured_cluster_matches_oracle_and_serves_live_writes() {
    let (queries, titles) = small_corpus();
    let oracle = build_oracle(&queries, &titles);

    let (cert_pem, key_pem) = test_identity();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let endpoints = spawn_secured_servers(&rt, 2, &cert_pem, &key_pem);

    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 2,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote_with_security(
        Arc::clone(&norm),
        dict,
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
        client_security(&cert_pem, Some(MESH_TOKEN)),
    )
    .expect("secured connect + dict shipping");
    cluster.ingest(&queries).expect("secured bulk ingest");

    // Secured cluster ≡ brute over every title (zero FN/FP across the TLS+token link).
    let mut matched = 0usize;
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .expect("secured percolate")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "secured grpc ≠ brute on {title:?}");
        matched += got.len();
    }
    assert!(matched > 0, "degenerate corpus: no matches at all");

    // Live write + read over the secured link.
    let next = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;
    cluster
        .add_query(next, "zzsecured gem mint")
        .expect("secured live add");
    assert!(cluster
        .percolate("zzsecured gem mint psa 10")
        .expect("secured percolate")
        .contains(&next));
}

#[test]
fn grpc_wrong_or_missing_token_is_rejected_loud() {
    let (cert_pem, key_pem) = test_identity();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let endpoints = spawn_secured_servers(&rt, 1, &cert_pem, &key_pem);

    let norm = Arc::new(vocab());
    let queries = vec![(1u64, "1994 topps".to_string())];
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        ..ClusterConfig::default()
    };

    // Wrong token: the AdoptDict handshake must be rejected (UNAUTHENTICATED → a loud
    // ShardError), never silently served.
    let Err(err) = ClusterEngine::connect_remote_with_security(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &endpoints,
        rt.handle(),
        client_security(&cert_pem, Some(b"wrong-token")),
    ) else {
        panic!("a wrong mesh token must fail the connect");
    };
    assert!(
        err.to_string().contains("adopt_dict"),
        "the rejection should surface at the first RPC: {err}"
    );

    // Missing token: same loud rejection.
    assert!(
        ClusterEngine::connect_remote_with_security(
            Arc::clone(&norm),
            dict,
            empty_tag_dict(),
            &cfg,
            &endpoints,
            rt.handle(),
            client_security(&cert_pem, None),
        )
        .is_err(),
        "a missing mesh token must fail the connect"
    );
}

#[test]
fn grpc_plaintext_client_to_tls_server_fails_loud() {
    let (cert_pem, key_pem) = test_identity();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let endpoints = spawn_secured_servers(&rt, 1, &cert_pem, &key_pem);
    // Same port, but a plaintext http:// endpoint and no TLS config.
    let plaintext = endpoints[0].replace("https://", "http://");

    let norm = Arc::new(vocab());
    let queries = vec![(1u64, "1994 topps".to_string())];
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        ..ClusterConfig::default()
    };
    assert!(
        ClusterEngine::connect_remote_with_security(
            norm,
            dict,
            empty_tag_dict(),
            &cfg,
            &[plaintext],
            rt.handle(),
            ClientSecurity {
                tls: None,
                token: Some(MESH_TOKEN.to_vec()),
                ..Default::default()
            },
        )
        .is_err(),
        "a plaintext client against a TLS server must fail loud"
    );
}

/// The `RecoverFrom` handler's OUTBOUND dial rides the mesh security too (the review
/// catch): a fully secured source + target pair completes a peer recovery — the
/// target pulls the source's segments over TLS with the token — and the recovered
/// target answers percolates ≡ brute. Without the target's client half configured,
/// the secured source rejects the pull (asserted as the negative leg).
#[test]
fn grpc_secured_peer_recovery_pulls_through_the_mesh() {
    let (queries, titles) = small_corpus();
    let oracle = build_oracle(&queries, &titles);

    let (cert_pem, key_pem) = test_identity();
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);

    // Durable secured SOURCE + two durable pending TARGETS (one with the client half,
    // one without). All serve TLS + demand the token.
    let src_dir = server_dir("sec_src");
    let tgt_dir = server_dir("sec_tgt");
    let bad_dir = server_dir("sec_bad");
    let (src_ep, tgt_ep, bad_ep) = {
        let _enter = rt.enter();
        let mut eps = Vec::new();
        for (dir, with_client) in [(&src_dir, true), (&tgt_dir, true), (&bad_dir, false)] {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).expect("bind");
            let addr = incoming.local_addr().expect("local_addr");
            let mut server = reverse_rusty::cluster::ShardServer::pending_durable(
                Arc::clone(&norm),
                EngineConfig::default(),
                (*dir).clone(),
            )
            .with_security(server_security(&cert_pem, &key_pem));
            if with_client {
                server = server.with_client_security(client_security(&cert_pem, Some(MESH_TOKEN)));
            }
            rt.spawn(server.serve_with_incoming(incoming));
            eps.push(format!("https://localhost:{}", addr.port()));
        }
        (eps[0].clone(), eps[1].clone(), eps[2].clone())
    };

    // Coordinator over the secured source; load the corpus (source segments).
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::connect_remote_with_security(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&src_ep),
        rt.handle(),
        client_security(&cert_pem, Some(MESH_TOKEN)),
    )
    .expect("connect secured source");
    cluster.ingest(&queries).expect("ingest over the mesh");

    // The target WITHOUT a client half cannot pull from the secured source: the
    // recovery fails loud (UNAUTHENTICATED at the source), never a silent empty shard.
    assert!(
        cluster
            .peer_recover_replica(&src_ep, &bad_ep, rt.handle())
            .is_err(),
        "a target without the mesh client half must fail the pull loudly"
    );

    // The properly configured target completes the recovery THROUGH the mesh.
    let (n, _hwm) = cluster
        .peer_recover_replica(&src_ep, &tgt_ep, rt.handle())
        .expect("secured peer recovery");
    assert!(n > 0, "the recovered target holds the corpus");

    // The recovered target answers ≡ brute (read through its own secured cluster).
    let verify = ClusterEngine::connect_remote_with_security(
        Arc::clone(&norm),
        dict,
        empty_tag_dict(),
        &cfg,
        std::slice::from_ref(&tgt_ep),
        rt.handle(),
        client_security(&cert_pem, Some(MESH_TOKEN)),
    )
    .expect("connect recovered target");
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = verify
            .percolate(title)
            .expect("percolate recovered target")
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "recovered-over-mesh ≠ brute on {title:?}");
    }
}
