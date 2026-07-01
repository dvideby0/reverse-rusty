//! Shared fixture for the RF>1 group-reconcile oracle (ADR-094): abortable durable servers, the
//! HRW mirror at rf, the replicated seed/resolve helpers, and the packed / single-position cluster
//! builders — split from `reconcile_replicated.rs` to keep it under the file-size goal.

use std::collections::{BTreeSet, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ClusterState, NodeDescriptor, NodeId, NodeRole, ShardAssignment,
    ShardGroup, ShardServer,
};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

use crate::harness::*;

/// A durable server + its serve-task handle (abortable — the node-loss lever) + data dir.
pub(crate) struct Server {
    pub(crate) addr: SocketAddr,
    pub(crate) ep: String,
    pub(crate) jh: JoinHandle<Result<(), tonic::transport::Error>>,
    pub(crate) dir: PathBuf,
}

pub(crate) fn spin_durable(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<Normalizer>,
    tag: &str,
) -> Server {
    let dir = server_dir(tag);
    let (addr, jh) = {
        let _enter = rt.enter();
        let inc = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind ephemeral port");
        let addr = inc.local_addr().expect("local_addr");
        let srv =
            ShardServer::pending_durable(Arc::clone(norm), EngineConfig::default(), dir.clone());
        let jh = rt.spawn(srv.serve_with_incoming(inc));
        (addr, jh)
    };
    wait_until_listening(addr);
    Server {
        addr,
        ep: format!("http://{addr}"),
        jh,
        dir,
    }
}

pub(crate) fn spin_n_durable(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<Normalizer>,
    tag: &str,
    n: usize,
) -> Vec<Server> {
    (0..n)
        .map(|i| spin_durable(rt, norm, &format!("{tag}_{i}")))
        .collect()
}

pub(crate) fn teardown(servers: &[Server]) {
    for s in servers {
        let _ = std::fs::remove_dir_all(&s.dir);
    }
}

/// Mirror `allocator::hrw_weight` (stable rendezvous hash) so the tests can compute the desired
/// GROUP per position and seed a deliberately different one — deterministic regardless of which way
/// the hash falls. Coupled to the allocator on purpose (a placement change asserts loudly).
pub(crate) fn hrw_weight(position: u32, node: u64) -> u64 {
    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&position.to_le_bytes());
    bytes[4..12].copy_from_slice(&node.to_le_bytes());
    reverse_rusty::util::fnv1a64(&bytes)
}

/// The HRW-desired `(primary, replicas)` for `position` over `nodes` at `rf` — exactly
/// `allocator::plan_assignments` (rank by weight desc, tie → lower id, take rf distinct).
pub(crate) fn hrw_group(position: u32, nodes: &[u64], rf: usize) -> (u64, Vec<u64>) {
    let mut ranked: Vec<(u64, u64)> = nodes
        .iter()
        .map(|&n| (hrw_weight(position, n), n))
        .collect();
    ranked.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    let chosen: Vec<u64> = ranked
        .into_iter()
        .take(rf.min(nodes.len()).max(1))
        .map(|(_, n)| n)
        .collect();
    (chosen[0], chosen[1..].to_vec())
}

/// Register `servers[i]` as data node `i + 1`, then commit the position-preserving replicated map:
/// `plan[pos] = (primary index, replica indexes)` into `servers` — where the data physically lives.
pub(crate) fn seed_group_map(
    cluster: &ClusterEngine,
    servers: &[Server],
    plan: &[(usize, Vec<usize>)],
) {
    for (i, s) in servers.iter().enumerate() {
        cluster
            .register_node(NodeDescriptor {
                id: NodeId((i + 1) as u64),
                addr: Some(s.ep.clone()),
                role: NodeRole::Data,
            })
            .expect("register node");
    }
    for (pos, (pi, ris)) in plan.iter().enumerate() {
        cluster
            .reassign_shard(ShardAssignment {
                position: pos as u32,
                primary: NodeId((pi + 1) as u64),
                replicas: ris.iter().map(|&r| NodeId((r + 1) as u64)).collect(),
            })
            .expect("seed committed replicated assignment");
    }
}

/// The committed `(primary, replica-set)` for `position` as raw node ids.
pub(crate) fn group_of(state: &ClusterState, position: u32) -> (u64, BTreeSet<u64>) {
    let a = state
        .assignments
        .iter()
        .find(|a| a.position == position)
        .expect("an assignment for every position");
    (a.primary.0, a.replicas.iter().map(|n| n.0).collect())
}

/// Resolve the committed map into `connect_replicated` groups — what a resolve-only RF>1
/// coordinator restart (`--route-by-assignments`) does on boot.
pub(crate) fn resolved_groups(state: &ClusterState) -> Vec<ShardGroup> {
    let addr_of = |id: NodeId| -> String {
        state
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.addr.clone())
            .expect("every committed member has a registered addr")
    };
    (0..state.num_shards)
        .map(|pos| {
            let a = state
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .expect("an assignment for every position");
            ShardGroup {
                primary: addr_of(a.primary),
                replicas: a.replicas.iter().map(|r| addr_of(*r)).collect(),
            }
        })
        .collect()
}

/// Drain any fence-window writes queued for partial-apply repair.
pub(crate) fn converge_repairs(cluster: &ClusterEngine) {
    for _ in 0..50 {
        if cluster.pending_repairs() == 0 {
            break;
        }
        let _ = cluster.resync();
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(cluster.pending_repairs(), 0, "fence-window writes converge");
}

pub(crate) fn assert_matches_oracle(
    cluster: &ClusterEngine,
    titles: &[String],
    oracle: &[HashSet<u64>],
    ctx: &str,
) {
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster
            .percolate(title)
            .unwrap_or_else(|e| panic!("{ctx}: percolate {title:?}: {e}"))
            .into_iter()
            .collect();
        assert_eq!(got, oracle[i], "{ctx}: vs brute on {title:?}");
    }
}

/// The packed RF=2 fixture: K positions, every primary on node A (index 0), every replica on node B
/// (index 1), node C (index 2) empty — a deliberately non-HRW replicated map, position-preserving
/// (the data lives exactly where the committed map says). Returns the cluster + servers.
pub(crate) fn build_packed_rf2(
    rt: &tokio::runtime::Runtime,
    norm: &Arc<Normalizer>,
    dict: &Arc<reverse_rusty::dict::Dict>,
    cfg: &ClusterConfig,
    queries: &[(u64, String)],
    tag: &str,
) -> (ClusterEngine, Vec<Server>) {
    let k = cfg.num_shards;
    let servers = spin_n_durable(rt, norm, tag, 3);
    let groups: Vec<ShardGroup> = (0..k)
        .map(|_| ShardGroup {
            primary: servers[0].ep.clone(),
            replicas: vec![servers[1].ep.clone()],
        })
        .collect();
    let cluster = ClusterEngine::connect_replicated(
        Arc::clone(norm),
        Arc::clone(dict),
        empty_tag_dict(),
        cfg,
        &groups,
        rt.handle(),
    )
    .expect("connect packed RF=2 cluster (all primaries on A, all replicas on B)");
    cluster.ingest(queries).expect("ingest corpus over gRPC");
    let plan: Vec<(usize, Vec<usize>)> = (0..k).map(|_| (0usize, vec![1usize])).collect();
    seed_group_map(&cluster, &servers, &plan);
    (cluster, servers)
}

/// The K=1 two/three-node fixture for the single-shape group-move legs: committed {A;[B]}, data
/// position-preserving, node C spun for the replica-only leg. Returns (cluster, servers, titles,
/// oracle, queries).
pub(crate) type OneShape = (
    ClusterEngine,
    Vec<Server>,
    Vec<String>,
    Vec<HashSet<u64>>,
    Vec<(u64, String)>,
);
pub(crate) fn one_position_rf2(tag: &str, rt: &tokio::runtime::Runtime, cap0: bool) -> OneShape {
    let (queries, titles) = build_corpus();
    let oracle = build_oracle(&queries, &titles);
    let norm = Arc::new(vocab());
    let dict = frozen_dict_over(&queries, &norm);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        replication_factor: 2,
        handoff_final_drain_cap: if cap0 {
            0
        } else {
            ClusterConfig::default().handoff_final_drain_cap
        },
        ..ClusterConfig::default()
    };
    let servers = spin_n_durable(rt, &norm, tag, 3);
    let groups = vec![ShardGroup {
        primary: servers[0].ep.clone(),
        replicas: vec![servers[1].ep.clone()],
    }];
    let cluster = ClusterEngine::connect_replicated(
        Arc::clone(&norm),
        Arc::clone(&dict),
        empty_tag_dict(),
        &cfg,
        &groups,
        rt.handle(),
    )
    .expect("connect {A;[B]} cluster");
    cluster.ingest(&queries).expect("ingest");
    seed_group_map(&cluster, &servers, &[(0usize, vec![1usize])]);
    (cluster, servers, titles, oracle, queries)
}
