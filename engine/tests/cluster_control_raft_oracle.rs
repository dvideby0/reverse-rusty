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
    in_process_cluster, start_grpc_node, ClusterState, ClusterStateChange, ControlError,
    ControlPlane, ControlServer, InMemoryControlPlane, NodeDescriptor, NodeId, NodeRole,
    RaftControlPlane, ShardAssignment,
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
            start_grpc_node(id, NUM_SHARDS, VNODES, GENESIS_FP, rt.handle())
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
