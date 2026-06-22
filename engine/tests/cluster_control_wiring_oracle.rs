//! Control-plane wiring oracle (ADR-083, feature `distributed`): a coordinator attached to a durable
//! openraft quorum via [`RemoteControlPlane`] reads + proposes over the gRPC `ClientControl` RPC.
//!
//! Proves: (1) the `RemoteControlPlane` round-trips the whole `ControlPlane` trait against a real
//! `ControlServer` (genesis read, version, propose, leader); (2) injected into a `ClusterEngine`, an
//! admin op (`register_node`) commits THROUGH the quorum, and `percolate` is byte-identical
//! before/after — the control plane is off the matching hot path, so it cannot affect recall;
//! (3) a follower transparently forwards a read/propose to the leader (the `ForwardToLeader`
//! redirect the client follows). The acceptance gate for the ADR-083 seam.
#![cfg(feature = "distributed")]

use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reverse_rusty::cluster::{
    start_grpc_node, ClientSecurity, ClusterConfig, ClusterEngine, ClusterStateChange,
    ControlPlane, ControlServer, NodeDescriptor, NodeId, NodeRole, RaftControlPlane,
    RemoteControlPlane,
};
use reverse_rusty::normalize::Normalizer;
use tokio::runtime::Runtime;
use tonic::transport::server::TcpIncoming;

const NUM_SHARDS: u32 = 3;
const VNODES: u32 = 128;
const GENESIS_FP: u64 = 0xC0DE_F00D;

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

fn small_corpus() -> Vec<(u64, String)> {
    vec![
        (1, "1990 topps chrome".to_string()),
        (2, "1986 fleer".to_string()),
        (3, "psa 10".to_string()),
        (4, "1994 upper deck rookie".to_string()),
    ]
}

fn data_node(id: u64, octet: u64) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://10.0.0.{octet}:50051")),
        role: NodeRole::Data,
    }
}

fn wait_until_listening(addr: SocketAddr) {
    for _ in 0..300 {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(100)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("control server at {addr} never started listening");
}

/// Poll until `planes[0]` reports an elected leader, returning its id (fail-closed on timeout).
fn wait_for_leader(planes: &[Arc<RaftControlPlane>]) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(leader) = planes[0].current_leader() {
            return leader;
        }
        assert!(Instant::now() < deadline, "no control-plane leader elected");
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Stand up `ids.len()` real `ControlServer`s on localhost (client plane attached), bootstrap from
/// node 0 with the REAL addresses (so a `ForwardToLeader` carries a dialable URL), and wait for a
/// leader. Returns the planes + their endpoint URLs (index-aligned with `ids`).
fn stand_up_quorum(rt: &Runtime, ids: &[u64]) -> (Vec<Arc<RaftControlPlane>>, Vec<String>) {
    let mut incomings = Vec::new();
    let mut addrs = Vec::new();
    {
        let _enter = rt.enter();
        for _ in ids {
            let inc =
                TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
            addrs.push(inc.local_addr().expect("local_addr"));
            incomings.push(inc);
        }
    }
    let planes: Vec<Arc<RaftControlPlane>> = ids
        .iter()
        .map(|&id| {
            Arc::new(
                start_grpc_node(id, NUM_SHARDS, VNODES, GENESIS_FP, rt.handle(), None)
                    .expect("start grpc manager node"),
            )
        })
        .collect();
    for (i, inc) in incomings.into_iter().enumerate() {
        let server = ControlServer::new(planes[i].raft()).with_client_plane(Arc::clone(&planes[i]));
        rt.spawn(server.serve_with_incoming(inc));
    }
    for a in &addrs {
        wait_until_listening(*a);
    }
    let members: Vec<(u64, String)> = ids
        .iter()
        .map(|&id| (id, format!("http://{}", addrs[id as usize])))
        .collect();
    planes[0].initialize(&members).expect("bootstrap quorum");
    wait_for_leader(&planes);
    let endpoints = addrs.iter().map(|a| format!("http://{a}")).collect();
    (planes, endpoints)
}

fn sorted_percolate(cluster: &ClusterEngine, title: &str) -> Vec<u64> {
    let mut v = cluster.percolate(title).expect("percolate");
    v.sort_unstable();
    v
}

#[test]
fn remote_control_plane_round_trips_and_drives_coordinator() {
    let rt = Runtime::new().expect("tokio runtime");
    let (planes, endpoints) = stand_up_quorum(&rt, &[0]);

    let rcp = RemoteControlPlane::connect(
        &endpoints[0],
        rt.handle().clone(),
        ClientSecurity::default(),
    )
    .expect("connect remote control plane");

    // Round-trip the trait against the real server: genesis read, leader, version.
    let genesis = rcp.cluster_state().expect("cluster_state over the wire");
    assert_eq!(genesis.num_shards, NUM_SHARDS);
    assert_eq!(genesis.dict_fingerprint, GENESIS_FP);
    assert_eq!(rcp.leader().expect("leader"), Some(NodeId(0)));
    let v0 = rcp.version().expect("version");

    // Propose through the wire: the committed document reflects it and the version advances.
    rcp.propose(ClusterStateChange::AddNode(data_node(2, 2)))
        .expect("propose over the wire");
    let v1 = rcp.version().expect("version after propose");
    assert!(v1 > v0, "a committed proposal advances the version");
    assert!(
        rcp.cluster_state()
            .unwrap()
            .nodes
            .iter()
            .any(|n| n.id == NodeId(2)),
        "the proposed node is in the committed document"
    );

    // Inject into a coordinator: an admin op commits THROUGH the quorum, and percolate is
    // byte-identical before/after — the control plane is off the matching hot path (zero FN).
    let cfg = ClusterConfig {
        num_shards: NUM_SHARDS as usize,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let queries = small_corpus();
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries)
        .expect("build cluster")
        .with_control_plane(Box::new(rcp));

    let titles = [
        "1990 topps chrome psa 10",
        "1986 fleer",
        "1994 upper deck rookie psa 10",
    ];
    let before: Vec<Vec<u64>> = titles
        .iter()
        .map(|t| sorted_percolate(&cluster, t))
        .collect();

    cluster
        .register_node(data_node(5, 5))
        .expect("register_node through the remote quorum");
    assert!(
        cluster
            .control_state()
            .unwrap()
            .nodes
            .iter()
            .any(|n| n.id == NodeId(5)),
        "register_node committed through the remote control plane"
    );

    let after: Vec<Vec<u64>> = titles
        .iter()
        .map(|t| sorted_percolate(&cluster, t))
        .collect();
    assert_eq!(
        before, after,
        "percolate must be byte-identical across a control-plane op (off the hot path)"
    );

    planes[0].shutdown();
}

#[test]
fn remote_control_plane_follows_forward_to_leader() {
    let rt = Runtime::new().expect("tokio runtime");
    let ids = [0u64, 1, 2];
    let (planes, endpoints) = stand_up_quorum(&rt, &ids);
    let leader = wait_for_leader(&planes);
    let follower = *ids
        .iter()
        .find(|&&id| id != leader)
        .expect("a follower exists") as usize;

    // Connect to a FOLLOWER. A read (ensure_linearizable) and a propose (client_write) both return
    // ForwardToLeader there; the client redials the named leader and retries — so they still succeed.
    let rcp = RemoteControlPlane::connect(
        &endpoints[follower],
        rt.handle().clone(),
        ClientSecurity::default(),
    )
    .expect("connect to follower");

    let st = rcp
        .cluster_state()
        .expect("a read via a follower forwards to the leader");
    assert_eq!(st.num_shards, NUM_SHARDS);

    rcp.propose(ClusterStateChange::AddNode(data_node(8, 8)))
        .expect("a propose via a follower forwards to the leader");

    // The leader's committed document reflects the forwarded proposal.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if planes[leader as usize]
            .local_state()
            .nodes
            .iter()
            .any(|n| n.id == NodeId(8))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "forwarded proposal never reached the leader's committed document"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    for p in &planes {
        p.shutdown();
    }
}
