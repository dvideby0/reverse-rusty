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
/// leader. Returns the planes + their endpoint URLs + the serve-task handles, all index-aligned with
/// `ids`. Hold a handle to `abort()` a node's server mid-test (dropping it does NOT stop the task).
fn stand_up_quorum(
    rt: &Runtime,
    ids: &[u64],
) -> (
    Vec<Arc<RaftControlPlane>>,
    Vec<String>,
    Vec<tokio::task::JoinHandle<()>>,
) {
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
    let mut handles = Vec::with_capacity(ids.len());
    for (i, inc) in incomings.into_iter().enumerate() {
        let server = ControlServer::new(planes[i].raft()).with_client_plane(Arc::clone(&planes[i]));
        handles.push(rt.spawn(async move {
            let _ = server.serve_with_incoming(inc).await;
        }));
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
    (planes, endpoints, handles)
}

fn sorted_percolate(cluster: &ClusterEngine, title: &str) -> Vec<u64> {
    let mut v = cluster.percolate(title).expect("percolate");
    v.sort_unstable();
    v
}

#[test]
fn remote_control_plane_round_trips_and_drives_coordinator() {
    let rt = Runtime::new().expect("tokio runtime");
    let (planes, endpoints, _handles) = stand_up_quorum(&rt, &[0]);

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
    let (planes, endpoints, _handles) = stand_up_quorum(&rt, &ids);
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

/// ADR-086 multi-control-endpoint failover: the coordinator's control client is given the WHOLE
/// quorum endpoint list and tries them in order. (a) a dead leading endpoint is skipped at connect
/// time (a read AND a propose still succeed via the survivor); (b) when the primary's NODE dies
/// mid-session a call fails over to the surviving quorum leader — the primary is a throwaway node on
/// its OWN runtime, dropped to kill it connection-and-all (aborting a serve task leaves the
/// established connection alive, cf. `cluster_grpc_oracle::transport`); (c) all endpoints down fails
/// loud (never a stale read). The main quorum leader stays up throughout, so the test is deterministic.
#[test]
fn remote_control_plane_fails_over_across_endpoints() {
    const DEAD: &str = "http://127.0.0.1:1";

    let rt = Runtime::new().expect("tokio runtime");
    let ids = [0u64, 1, 2];
    let (planes, endpoints, _handles) = stand_up_quorum(&rt, &ids);
    let leader = wait_for_leader(&planes) as usize;

    // (a) Connect-time failover: a dead leading endpoint is skipped; the survivor answers a read AND
    // a propose (committed via the leader — by forwarding if the survivor is a follower).
    let list_a = vec![DEAD.to_string(), endpoints[leader].clone()];
    let rcp_a = RemoteControlPlane::connect_failover(
        &list_a,
        rt.handle().clone(),
        ClientSecurity::default(),
    )
    .expect("connect_failover skips the dead leading endpoint");
    assert_eq!(
        rcp_a
            .cluster_state()
            .expect("read via the survivor")
            .num_shards,
        NUM_SHARDS
    );
    rcp_a
        .propose(ClusterStateChange::AddNode(data_node(7, 7)))
        .expect("propose via the survivor");
    assert!(
        planes[leader]
            .local_state()
            .nodes
            .iter()
            .any(|n| n.id == NodeId(7)),
        "the connect-failover propose committed through the leader"
    );

    // (b) Per-call failover: the primary is a throwaway control node on its OWN runtime. Dropping that
    // runtime kills it connection-and-all, so a subsequent call MUST fail over to the surviving quorum
    // leader (and the propose can ONLY have committed there — the victim is dead).
    let victim_rt = Runtime::new().expect("victim runtime");
    let victim_plane = Arc::new(
        start_grpc_node(99, NUM_SHARDS, VNODES, GENESIS_FP, victim_rt.handle(), None)
            .expect("start victim node"),
    );
    let victim_ep = {
        let _enter = victim_rt.enter();
        let inc = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind victim");
        let addr = inc.local_addr().expect("victim addr");
        let server =
            ControlServer::new(victim_plane.raft()).with_client_plane(Arc::clone(&victim_plane));
        victim_rt.spawn(async move {
            let _ = server.serve_with_incoming(inc).await;
        });
        format!("http://{addr}")
    };
    wait_until_listening(victim_ep.trim_start_matches("http://").parse().unwrap());
    victim_plane
        .initialize(&[(99, victim_ep.clone())])
        .expect("bootstrap victim single-node");
    wait_for_leader(&[Arc::clone(&victim_plane)]);

    let list_b = vec![victim_ep.clone(), endpoints[leader].clone()];
    let rcp_b = RemoteControlPlane::connect_failover(
        &list_b,
        rt.handle().clone(),
        ClientSecurity::default(),
    )
    .expect("connect_failover to the victim primary");
    // While the victim is up it answers from its own single-node genesis (the primary path).
    assert_eq!(
        rcp_b
            .cluster_state()
            .expect("read via the victim")
            .num_shards,
        NUM_SHARDS
    );

    victim_plane.shutdown(); // stop the victim raft while its runtime is still alive
    drop(victim_rt); // then kill its server + connection (the reliable kill)
    wait_until_stopped(victim_ep.trim_start_matches("http://"));

    // The call now fails over to the surviving quorum leader: a read AND a propose succeed there.
    assert_eq!(
        rcp_b
            .cluster_state()
            .expect("a read fails over from the dead victim to the surviving leader")
            .num_shards,
        NUM_SHARDS
    );
    rcp_b
        .propose(ClusterStateChange::AddNode(data_node(9, 9)))
        .expect("a propose fails over to the surviving leader");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if planes[leader]
            .local_state()
            .nodes
            .iter()
            .any(|n| n.id == NodeId(9))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "failed-over proposal never reached the surviving leader's committed document"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    // (c) All endpoints down ⇒ fail loud at connect (never a swallowed stale read).
    let all_dead = vec![DEAD.to_string(), "http://127.0.0.1:2".to_string()];
    assert!(
        RemoteControlPlane::connect_failover(
            &all_dead,
            rt.handle().clone(),
            ClientSecurity::default()
        )
        .is_err(),
        "connect_failover with all endpoints down must fail loud"
    );

    for p in &planes {
        p.shutdown();
    }
}

/// Block until `addr` (a `host:port`) stops accepting TCP, or time out — the inverse of
/// [`wait_until_listening`], so an aborted server is provably unreachable before we assert failover.
fn wait_until_stopped(addr: &str) {
    let sa: SocketAddr = addr.parse().expect("parse addr");
    for _ in 0..300 {
        if TcpStream::connect_timeout(&sa, Duration::from_millis(100)).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("server at {addr} never stopped listening");
}
