//! Control-plane oracle for the **openraft backend** — the acceptance gate for clustering
//! step 5b-1 (ADR-038), behind `--features distributed`.
//!
//! ADR-037's `cluster_control_plane_oracle.rs` proves the in-memory backend. This file proves
//! the real consensus backend behind the SAME `trait ControlPlane`:
//!   * a 3-node openraft cluster (genuine elections + log replication + quorum commit, run in
//!     one process over a direct-dispatch network) converges, under the ADR-037 document script
//!     driven through the public trait, to the SAME committed document the in-memory backend
//!     reaches — voters / nodes / assignments / model (NOT the epoch: openraft commits its own
//!     Blank/Membership entries, so the semantic-transition counter is not comparable; see
//!     `control_raft.rs`),
//!   * a `propose` on a FOLLOWER returns `ControlError::ForwardToLeader` (the variant ADR-037
//!     baked into the seam so this backend changed no call site),
//!   * `change_membership` routes to `Raft::change_membership` and the committed voter set follows.
//!
//! This is the openraft analogue of the in-memory two-backend differential; it lives in its own
//! `distributed`-gated file because a faithful Raft proof is inherently multi-node (a lone node
//! cannot satisfy a voter-set change), unlike the single-handle in-memory differential.

#![cfg(feature = "distributed")]

use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use reverse_rusty::cluster::{
    durable_single_node, in_process_cluster, start_grpc_node, ClusterState, ClusterStateChange,
    ControlError, ControlPlane, ControlServer, InMemoryControlPlane, NodeDescriptor, NodeId,
    NodeRole, RaftControlPlane, ShardAssignment,
};
use tokio::runtime::Runtime;
use tonic::transport::server::TcpIncoming;

const NUM_SHARDS: u32 = 4;
const VNODES: u32 = 128;
const GENESIS_FP: u64 = 0xFEED;
const MODEL_FP: u64 = 0xBEEF;

/// A cluster member, built identically for both backends so their documents are comparable.
fn node(id: u64, role: NodeRole) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
        role,
    }
}

/// The ADR-037 document script (placement + model changes), driven through the public trait.
/// Both backends run it; the raft cluster already has voters {0,1,2} from bootstrap, so only the
/// in-memory backend needs the explicit `change_membership` to reach the same voter set.
fn run_doc_script(cp: &dyn ControlPlane) {
    cp.propose(ClusterStateChange::AddNode(node(1, NodeRole::Manager)))
        .expect("add node 1");
    cp.propose(ClusterStateChange::AddNode(node(2, NodeRole::Data)))
        .expect("add node 2");
    cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
        position: 0,
        primary: NodeId(1),
        replicas: vec![NodeId(2)],
    }))
    .expect("assign shard 0");
    cp.propose(ClusterStateChange::BumpModelVersion {
        dict_fingerprint: MODEL_FP,
    })
    .expect("bump model");
}

/// The in-memory backend's final document under the equivalent script (its genesis voters are
/// {0}, so it reaches {0,1,2} via the explicit change_membership).
fn in_memory_reference() -> ClusterState {
    let cp = InMemoryControlPlane::single_node(NUM_SHARDS, VNODES, GENESIS_FP);
    cp.change_membership(vec![NodeId(0), NodeId(1), NodeId(2)])
        .expect("set voters");
    run_doc_script(&cp);
    (*cp.cluster_state().expect("read in-memory state")).clone()
}

/// Index of the elected leader (== node id, since `in_process_cluster([0,1,2], …)` returns the
/// planes in id order). Polls until a leader is known.
fn leader_index(planes: &[RaftControlPlane]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(leader) = planes.first().and_then(RaftControlPlane::current_leader) {
            return leader as usize;
        }
        assert!(Instant::now() < deadline, "no leader elected");
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Poll until `plane` (the leader) has APPLIED the bootstrap membership entry. `initialize`
/// returns once the entry is appended, and a leader is *known* the moment the election is won —
/// but openraft refuses `change_membership` while the effective membership (here the bootstrap
/// entry at log index 0) is still uncommitted, so a reconfiguration issued straight after the
/// election races the bootstrap commit on slow runners. Apply trails commit, so
/// applied ≥ the effective membership's log index ⇒ that membership is committed.
fn wait_initial_membership_committed(plane: &RaftControlPlane) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let metrics = plane.raft().metrics().borrow().clone();
        let membership = metrics.membership_config.log_id().as_ref().map(|l| l.index);
        let applied = metrics.last_applied.as_ref().map(|l| l.index);
        if let (Some(m), Some(a)) = (membership, applied) {
            if a >= m {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "bootstrap membership never committed: membership log {membership:?}, applied {applied:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Poll until every node's committed document is identical (replication caught up), then return it.
fn wait_converged(planes: &[RaftControlPlane]) -> ClusterState {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let states: Vec<ClusterState> = planes.iter().map(RaftControlPlane::local_state).collect();
        if states.windows(2).all(|w| w[0] == w[1]) {
            return states[0].clone();
        }
        assert!(
            Instant::now() < deadline,
            "nodes did not converge: {states:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Assert two documents agree on everything the differential cares about — NOT the epoch (openraft
/// commits its own Blank/Membership entries, so the raft node's semantic-transition count differs).
fn assert_docs_agree(got: &ClusterState, want: &ClusterState) {
    assert_eq!(got.voters, want.voters, "voters");
    assert_eq!(got.nodes, want.nodes, "nodes");
    assert_eq!(got.assignments, want.assignments, "assignments");
    assert_eq!(got.num_shards, want.num_shards, "num_shards");
    assert_eq!(got.vnodes, want.vnodes, "vnodes");
    assert_eq!(
        got.dict_fingerprint, want.dict_fingerprint,
        "dict_fingerprint"
    );
    assert_eq!(got.model_version, want.model_version, "model_version");
}

/// A 3-node openraft cluster, driven through `trait ControlPlane`, converges to the same committed
/// document the in-memory backend reaches under the equivalent script.
#[test]
fn raft_three_node_converges_to_in_memory_document() {
    let rt = Runtime::new().expect("tokio runtime");
    let planes = in_process_cluster(&[0, 1, 2], NUM_SHARDS, VNODES, GENESIS_FP, rt.handle())
        .expect("bootstrap 3-node raft control plane");

    // Drive the document script on the leader, through the public trait.
    let leader = leader_index(&planes);
    run_doc_script(&planes[leader]);

    // All three nodes converge to an identical committed document...
    let converged = wait_converged(&planes);
    // ...and that document agrees with the in-memory backend (modulo epoch).
    assert_docs_agree(&converged, &in_memory_reference());

    // Sanity: bootstrap established the full voter set via consensus.
    assert_eq!(
        converged.voters,
        vec![NodeId(0), NodeId(1), NodeId(2)],
        "all three nodes are voters"
    );
}

/// A `propose` on a follower fails with `ForwardToLeader` (and does not mutate state) — the
/// fail-closed routing the seam promised, now served by a real Raft follower.
#[test]
fn propose_on_follower_forwards_to_leader() {
    let rt = Runtime::new().expect("tokio runtime");
    let planes = in_process_cluster(&[0, 1, 2], NUM_SHARDS, VNODES, GENESIS_FP, rt.handle())
        .expect("bootstrap 3-node raft control plane");

    let leader = leader_index(&planes);
    let follower = (leader + 1) % planes.len();

    let result = planes[follower].propose(ClusterStateChange::AddNode(node(9, NodeRole::Data)));
    assert!(
        matches!(result, Err(ControlError::ForwardToLeader { .. })),
        "a follower propose must forward to the leader, got {result:?}"
    );
}

/// `ControlPlane::change_membership` routes to `Raft::change_membership`; the committed voter set
/// follows. Removes a non-leader voter (keeping the leader a voter avoids a step-down race).
#[test]
fn change_membership_routes_to_raft() {
    let rt = Runtime::new().expect("tokio runtime");
    let planes = in_process_cluster(&[0, 1, 2], NUM_SHARDS, VNODES, GENESIS_FP, rt.handle())
        .expect("bootstrap 3-node raft control plane");

    let leader = leader_index(&planes);
    // A known leader ≠ a committed bootstrap membership; openraft rejects a reconfiguration
    // while the previous membership entry is uncommitted, so wait out the bootstrap commit.
    wait_initial_membership_committed(&planes[leader]);
    let drop_follower = (leader + 1) % planes.len();
    let target: Vec<NodeId> = (0..planes.len())
        .filter(|&i| i != drop_follower)
        .map(|i| NodeId(i as u64))
        .collect();

    planes[leader]
        .change_membership(target.clone())
        .expect("change membership on the leader");

    // The leader's committed voter set is exactly the requested set.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut voters = planes[leader].local_state().voters;
        voters.sort_unstable();
        if voters == target {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "voter set did not converge to {target:?}, got {voters:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// 5b-2 — the same backend over the cross-process gRPC ControlService.
// ---------------------------------------------------------------------------

/// Poll until a TCP connection to `addr` succeeds (the gRPC server is accepting), or panic.
fn wait_until_listening(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "server at {addr} never listened");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Poll the `watch` nodes until one reports an elected leader satisfying `accept`, or panic.
fn poll_leader(planes: &[RaftControlPlane], watch: &[usize], accept: impl Fn(u64) -> bool) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for &i in watch {
            if let Some(l) = planes[i].current_leader() {
                if accept(l) {
                    return l;
                }
            }
        }
        assert!(Instant::now() < deadline, "no acceptable leader elected");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A 3-node openraft control plane over real gRPC `ControlService` servers on localhost: elects a
/// leader and commits a document change, then SURVIVES the leader being killed — a new leader is
/// elected from the remaining quorum, the committed document persists, and a fresh write commits.
/// The multi-process analogue of the in-process convergence test (ADR-038 step 5b-2).
#[test]
fn grpc_three_node_survives_leader_failure() {
    let rt = Runtime::new().expect("tokio runtime");
    let ids = [0u64, 1, 2];

    // Bind three ephemeral localhost ports inside the runtime (reactor registration).
    let mut addrs: Vec<SocketAddr> = Vec::new();
    let mut incomings: Vec<TcpIncoming> = Vec::new();
    {
        let _enter = rt.enter();
        for _ in &ids {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            incomings.push(incoming);
        }
    }
    let urls: Vec<String> = addrs.iter().map(|a| format!("http://{a}")).collect();

    // Build a manager node per id (gRPC network factory) and serve each over its ControlService.
    let mut planes: Vec<RaftControlPlane> = Vec::new();
    for &id in &ids {
        planes.push(
            // In-memory store here (None): this test exercises elections/failover, not durability
            // (the durable restart path is covered by `durable_node_recovers_*` below).
            start_grpc_node(id, NUM_SHARDS, VNODES, GENESIS_FP, rt.handle(), None)
                .expect("start grpc manager node"),
        );
    }
    let mut servers = Vec::new();
    for incoming in incomings {
        let i = servers.len();
        let server = ControlServer::new(planes[i].raft());
        servers.push(rt.spawn(server.serve_with_incoming(incoming)));
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }

    // Bootstrap from node 0 with all members, then wait for an elected leader.
    let members: Vec<(u64, String)> = ids
        .iter()
        .map(|&id| (id, urls[id as usize].clone()))
        .collect();
    planes[0].initialize(&members).expect("bootstrap cluster");
    let all: Vec<usize> = (0..ids.len()).collect();
    let leader = poll_leader(&planes, &all, |_| true) as usize;

    // Commit a document change through the leader (over real gRPC replication).
    planes[leader]
        .propose(ClusterStateChange::AddNode(node(1, NodeRole::Manager)))
        .expect("commit add node 1");

    // KILL the leader: stop its server (unreachable) AND shut down its Raft core (stops heartbeats).
    servers[leader].abort();
    let _ = rt.block_on(planes[leader].raft().shutdown());

    // A new leader is elected from the surviving quorum (2 of 3).
    let survivors: Vec<usize> = all.iter().copied().filter(|&i| i != leader).collect();
    let old = leader as u64;
    let new_leader = poll_leader(&planes, &survivors, move |l| l != old) as usize;
    assert_ne!(new_leader, leader, "a new leader replaced the dead one");

    // The committed document SURVIVED the failover...
    let doc = planes[new_leader]
        .cluster_state()
        .expect("read committed state from the new leader");
    assert!(
        doc.nodes.iter().any(|n| n.id == NodeId(1)),
        "the pre-failover commit (node 1) survived"
    );

    // ...and the cluster is LIVE: a fresh write commits on the new leader.
    planes[new_leader]
        .propose(ClusterStateChange::AddNode(node(2, NodeRole::Data)))
        .expect("post-failover write commits");
    let doc2 = planes[new_leader]
        .cluster_state()
        .expect("read state after post-failover write");
    assert!(
        doc2.nodes.iter().any(|n| n.id == NodeId(1))
            && doc2.nodes.iter().any(|n| n.id == NodeId(2)),
        "both the pre- and post-failover commits are present: {:?}",
        doc2.nodes
    );
}

/// ADR-041 (durable Raft log + restart): a durable single-node control plane commits a document,
/// the node is cleanly stopped (its hard state is on disk — vote/log/committed/snapshot fsync'd per
/// write), then a FRESH node is rebuilt from the SAME data dir. It resumes its Raft state, replays
/// its committed log, and serves the committed document; a new write still commits. Single-node so
/// it is its own leader — deterministic, with no multi-node restart race.
#[test]
fn durable_node_recovers_committed_document_after_restart() {
    let rt = Runtime::new().expect("tokio runtime");
    let dir = std::env::temp_dir().join(format!("rr_raft_restart_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // Build → commit the document script → capture the committed doc → cleanly stop.
    let committed = {
        let node = durable_single_node(7, &dir, NUM_SHARDS, VNODES, GENESIS_FP, rt.handle())
            .expect("durable node");
        run_doc_script(&node); // add nodes 1,2 + assign shard 0 + bump model fingerprint
        let doc = node.cluster_state().expect("read committed doc");
        assert!(
            doc.nodes.iter().any(|n| n.id == NodeId(1))
                && doc.nodes.iter().any(|n| n.id == NodeId(2)),
            "pre-restart membership committed"
        );
        assert_eq!(
            doc.dict_fingerprint, MODEL_FP,
            "pre-restart model committed"
        );
        assert!(
            doc.assignments
                .iter()
                .any(|a| a.position == 0 && a.primary == NodeId(1)),
            "pre-restart assignment committed"
        );
        node.shutdown(); // release the durable files before reopening the same dir
        doc
    };

    // Restart from the SAME dir: a fresh node rebuilds its committed state from disk.
    let reopened = durable_single_node(7, &dir, NUM_SHARDS, VNODES, GENESIS_FP, rt.handle())
        .expect("restart durable node");
    let recovered = reopened.cluster_state().expect("read recovered doc");
    assert_eq!(
        recovered.nodes, committed.nodes,
        "membership survived the restart"
    );
    assert_eq!(
        recovered.assignments, committed.assignments,
        "the shard→node assignment survived the restart"
    );
    assert_eq!(
        recovered.dict_fingerprint, committed.dict_fingerprint,
        "the model fingerprint survived the restart"
    );

    // ...and the cluster is LIVE after restart: a fresh write commits on top of the recovered state.
    reopened
        .propose(ClusterStateChange::AddNode(node(3, NodeRole::Data)))
        .expect("post-restart write commits");
    let after = reopened
        .cluster_state()
        .expect("read after post-restart write");
    assert!(
        after.nodes.iter().any(|n| n.id == NodeId(3)),
        "the post-restart write is present on the recovered node"
    );

    reopened.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// ADR-071: the SECURED control plane — three manager nodes whose `ControlService`s
/// present a TLS identity and demand the mesh token, whose Raft network clients
/// verify that identity and attach the token. Real elections + replication + quorum
/// commit over the secured links: the cluster bootstraps, elects, commits a document
/// change, and every node converges to it. (The negative paths — wrong token,
/// plaintext client — are proven on the shard transport in
/// `cluster_grpc_oracle::security`; the interceptors are the same shared types.)
#[test]
fn grpc_secured_control_plane_elects_and_commits() {
    use reverse_rusty::cluster::{
        start_grpc_node_with_security, ClientSecurity, ServerSecurity, TlsClientConfig,
        TlsServerIdentity,
    };

    let rt = Runtime::new().expect("tokio runtime");
    let ids = [0u64, 1, 2];

    // One self-signed localhost identity shared by the three nodes (in-test rcgen —
    // no key material in the repo); self-signed ⇒ the leaf doubles as the CA.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed cert");
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.key_pair.serialize_pem().into_bytes();
    let token = b"control-mesh-secret".to_vec();

    let mut addrs: Vec<SocketAddr> = Vec::new();
    let mut incomings: Vec<TcpIncoming> = Vec::new();
    {
        let _enter = rt.enter();
        for _ in &ids {
            let incoming =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(incoming.local_addr().expect("local_addr"));
            incomings.push(incoming);
        }
    }
    // Peer URLs are https://localhost so the certificate SAN matches.
    let urls: Vec<String> = addrs
        .iter()
        .map(|a| format!("https://localhost:{}", a.port()))
        .collect();

    let client_security = ClientSecurity {
        tls: Some(TlsClientConfig {
            ca_pem: cert_pem.clone(),
            domain: None,
        }),
        token: Some(token.clone()),
        ..Default::default()
    };
    let mut planes: Vec<RaftControlPlane> = Vec::new();
    for &id in &ids {
        planes.push(
            start_grpc_node_with_security(
                id,
                NUM_SHARDS,
                VNODES,
                GENESIS_FP,
                rt.handle(),
                None,
                client_security.clone(),
            )
            .expect("start secured grpc manager node"),
        );
    }
    let mut servers = Vec::new();
    for incoming in incomings {
        let i = servers.len();
        let server = ControlServer::new(planes[i].raft()).with_security(ServerSecurity {
            tls: Some(TlsServerIdentity {
                cert_pem: cert_pem.clone(),
                key_pem: key_pem.clone(),
            }),
            token: Some(token.clone()),
            ..Default::default()
        });
        servers.push(rt.spawn(server.serve_with_incoming(incoming)));
    }
    for &addr in &addrs {
        wait_until_listening(addr);
    }

    let members: Vec<(u64, String)> = ids
        .iter()
        .map(|&id| (id, urls[id as usize].clone()))
        .collect();
    planes[0]
        .initialize(&members)
        .expect("bootstrap the secured cluster");
    let all: Vec<usize> = (0..ids.len()).collect();
    let leader = poll_leader(&planes, &all, |_| true) as usize;

    // A document change commits via quorum replication over the secured links.
    planes[leader]
        .propose(ClusterStateChange::AddNode(node(7, NodeRole::Data)))
        .expect("commit over the secured control plane");
    let doc = planes[leader]
        .cluster_state()
        .expect("read the committed document");
    assert!(
        doc.nodes.iter().any(|n| n.id == NodeId(7)),
        "the secured-quorum commit landed: {:?}",
        doc.nodes
    );
}

/// Control-server health endpoint (ADR-084): a single manager node serves the standard
/// `grpc.health.v1.Health` service on a SEPARATE plaintext `--health-addr` port (the
/// deployable `serve()` two-port path). Liveness (`Check("")`) is SERVING once the server is
/// up; readiness (`Check("ready")`) is NOT_SERVING until the node sees a leader, then flips to
/// SERVING once it bootstraps a single-voter cluster and elects itself — exercising the
/// raft-metrics readiness predicate the shard test cannot.
#[test]
fn grpc_control_server_health_liveness_and_readiness() {
    use reverse_rusty_shard_proto as raw;

    use raw::health::health_check_response::ServingStatus;
    use raw::health::health_client::HealthClient;
    use raw::health::HealthCheckRequest;

    let rt = Runtime::new().expect("tokio runtime");

    // Two distinct free ports (data + health), released for serve() to bind by address.
    let (data_addr, health_addr) = {
        let d = std::net::TcpListener::bind("127.0.0.1:0").expect("bind data port");
        let h = std::net::TcpListener::bind("127.0.0.1:0").expect("bind health port");
        (
            d.local_addr().expect("data addr"),
            h.local_addr().expect("health addr"),
        )
    };

    // Build a single manager node (in-memory store) and serve it over its data + health ports.
    let plane = {
        let _enter = rt.enter();
        start_grpc_node(0, NUM_SHARDS, VNODES, GENESIS_FP, rt.handle(), None)
            .expect("start grpc manager node")
    };
    let server = ControlServer::new(plane.raft()).with_health_addr(health_addr);
    rt.spawn(server.serve(data_addr));
    wait_until_listening(data_addr);
    wait_until_listening(health_addr);

    // Before bootstrap: liveness up, but no leader ⇒ NOT ready.
    rt.block_on(async {
        let mut hc = HealthClient::connect(format!("http://{health_addr}"))
            .await
            .expect("connect health client");
        assert_eq!(
            hc.check(HealthCheckRequest {
                service: String::new()
            })
            .await
            .expect("check overall")
            .into_inner()
            .status(),
            ServingStatus::Serving,
            "liveness must be SERVING once the control server is up"
        );
        assert_eq!(
            hc.check(HealthCheckRequest {
                service: "ready".to_string()
            })
            .await
            .expect("check ready before")
            .into_inner()
            .status(),
            ServingStatus::NotServing,
            "a control node with no elected leader is not ready"
        );
    });

    // Bootstrap a single-voter cluster: node 0 elects itself leader.
    plane
        .initialize(&[(0, format!("http://{data_addr}"))])
        .expect("bootstrap single-node control plane");

    // Readiness flips to SERVING once a leader is known (the 250ms watcher).
    let became_ready = rt.block_on(async {
        let mut hc = HealthClient::connect(format!("http://{health_addr}"))
            .await
            .expect("reconnect health client");
        for _ in 0..60 {
            let status = hc
                .check(HealthCheckRequest {
                    service: "ready".to_string(),
                })
                .await
                .expect("check ready after")
                .into_inner()
                .status();
            if status == ServingStatus::Serving {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    });
    assert!(
        became_ready,
        "control readiness must flip to SERVING once a leader is elected"
    );
    // Keep the plane (the raft core owner) alive until the assertions complete.
    drop(plane);
}
