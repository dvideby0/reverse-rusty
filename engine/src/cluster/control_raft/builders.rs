//! Node builders: the openraft `Config`, the generic `build_node`, and the public entry points
//! (`start_grpc_node`, `in_process_cluster`, `durable_single_node`) + the leader-wait helper.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use openraft::network::RaftNetworkFactory;
use openraft::{Config, Raft};
use tokio::runtime::Handle;

use crate::cluster::control::{single_node_state, ClusterState, ControlError};

use super::log_store::LogStore;
use super::network::{GrpcControlNetworkFactory, InProcFactory, Registry};
use super::state_machine::StateMachine;
use super::{RaftControlPlane, TypeConfig};

fn control_config() -> Result<Config, ControlError> {
    Config {
        cluster_name: "reverse-rusty-control".to_string(),
        heartbeat_interval: 150,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    }
    .validate()
    .map_err(|e| ControlError::Backend(format!("invalid raft config: {e}")))
}

/// Build a [`RaftControlPlane`] over an explicit network factory + genesis document. Shared by
/// [`in_process_cluster`] (in-process registry network) and the gRPC manager node (step 5b-2).
/// `dir` selects the store backend: `None` ⇒ in-memory (byte-identical to ADR-038); `Some` ⇒
/// durable under that dir (ADR-041), so the node resumes its Raft hard state + committed document
/// on restart. `fsync` is the durability policy for the durable backend (ignored when in-memory).
fn build_node<N>(
    node_id: u64,
    genesis: ClusterState,
    network: N,
    handle: &Handle,
    dir: Option<&Path>,
    fsync: bool,
) -> Result<RaftControlPlane, ControlError>
where
    N: RaftNetworkFactory<TypeConfig>,
{
    let config = Arc::new(control_config()?);
    let (log, sm) = match dir {
        Some(d) => (
            LogStore::open(d, fsync)?,
            StateMachine::open(d, genesis, fsync)?,
        ),
        None => (LogStore::in_memory(), StateMachine::in_memory(genesis)),
    };
    let raft = handle
        .block_on(Raft::new(node_id, config, network, log, sm.clone()))
        .map_err(|e| ControlError::Backend(format!("Raft::new({node_id}): {e}")))?;
    Ok(RaftControlPlane {
        raft,
        sm,
        handle: handle.clone(),
    })
}

/// Build a cluster-manager node that talks to its peers over the gRPC `ControlService` (ADR-038
/// step 5b-2). Seeds the [`single_node_state`] genesis (every manager starts from the same
/// document); the caller serves a [`ControlServer`](crate::cluster::control_server::ControlServer) over
/// `node.raft()` and, on exactly ONE node, calls [`RaftControlPlane::initialize`] with the manager
/// addresses once all peers are listening. Returns the node handle (not yet serving). `data_dir`
/// makes the node **durable** (ADR-041) — it resumes its Raft state + committed document after a
/// restart and rejoins the quorum; `None` keeps the (ADR-038) in-memory store.
pub fn start_grpc_node(
    node_id: u64,
    num_shards: u32,
    vnodes: u32,
    dict_fingerprint: u64,
    handle: &Handle,
    data_dir: Option<&Path>,
) -> Result<RaftControlPlane, ControlError> {
    let genesis = single_node_state(num_shards, vnodes, dict_fingerprint);
    // Durable manager nodes fsync their hard state (election safety + no committed-data loss).
    build_node(
        node_id,
        genesis,
        GrpcControlNetworkFactory,
        handle,
        data_dir,
        true,
    )
}

/// Build an in-process multi-node control-plane cluster: `ids.len()` real [`Raft`] nodes wired by
/// a direct-dispatch registry network, all seeded with the [`single_node_state`] genesis
/// (`num_shards`/`vnodes`/`dict_fingerprint`) so the committed document is comparable to
/// [`InMemoryControlPlane::single_node`](crate::cluster::control::InMemoryControlPlane::single_node). Node
/// `ids[0]` bootstraps the cluster; the call blocks until a leader is elected.
///
/// This runs genuine elections + log replication + quorum commit in ONE process — the acceptance
/// vehicle for the openraft backend (ADR-038 step 5b-1). It is `distributed`-gated and intended for
/// the oracle / single-process embedding; multi-process deployment uses the gRPC `ControlService`
/// (step 5b-2).
pub fn in_process_cluster(
    ids: &[u64],
    num_shards: u32,
    vnodes: u32,
    dict_fingerprint: u64,
    handle: &Handle,
) -> Result<Vec<RaftControlPlane>, ControlError> {
    let Some(&first_id) = ids.first() else {
        return Err(ControlError::Backend(
            "in_process_cluster needs at least one node".into(),
        ));
    };
    let registry: Registry = Arc::new(Mutex::new(BTreeMap::new()));
    let genesis = single_node_state(num_shards, vnodes, dict_fingerprint);

    let mut planes = Vec::with_capacity(ids.len());
    for &id in ids {
        let factory = InProcFactory {
            registry: Arc::clone(&registry),
        };
        let plane = build_node(id, genesis.clone(), factory, handle, None, false)?;
        // Register the handle so peers can reach it. Raft tolerates the brief window before all
        // handles are present (uninitialized nodes do not campaign, so no RPCs fly until bootstrap).
        registry
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, plane.raft());
        planes.push(plane);
    }

    // Bootstrap from the first node with ALL members as genesis voters (in-process addresses are
    // placeholders — the registry routes by id, not address).
    let members: Vec<(u64, String)> = ids
        .iter()
        .map(|&id| (id, format!("inproc://{id}")))
        .collect();
    if let Some(first) = planes.first() {
        first.initialize(&members)?;
    }
    wait_for_leader(&planes, first_id, Duration::from_secs(10))?;
    Ok(planes)
}

/// Build a SINGLE durable in-process control-plane node (its own leader) rooted at `dir` — the
/// restart-recovery vehicle for ADR-041 (and a single-manager durable embedding). It reuses the
/// in-process registry network (a one-node cluster), persists its Raft state under `dir`, and is
/// idempotent across a restart: calling it again over the SAME `dir` rebuilds the node from disk
/// (`initialize` returns `NotAllowed`, ignored), re-elects itself, and replays its committed log so
/// the committed cluster-state document survives. `distributed`-gated.
pub fn durable_single_node(
    node_id: u64,
    dir: &Path,
    num_shards: u32,
    vnodes: u32,
    dict_fingerprint: u64,
    handle: &Handle,
) -> Result<RaftControlPlane, ControlError> {
    let registry: Registry = Arc::new(Mutex::new(BTreeMap::new()));
    let genesis = single_node_state(num_shards, vnodes, dict_fingerprint);
    let factory = InProcFactory {
        registry: Arc::clone(&registry),
    };
    // Durable: fsync the hard state (this is the deployment path, not the fast oracle path).
    let node = build_node(node_id, genesis, factory, handle, Some(dir), true)?;
    registry
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(node_id, node.raft());
    // First build forms the one-node cluster; a restart finds it already initialized (NotAllowed,
    // ignored) and re-elects from the persisted vote/log.
    node.initialize(&[(node_id, format!("inproc://{node_id}"))])?;
    wait_for_leader(
        std::slice::from_ref(&node),
        node_id,
        Duration::from_secs(10),
    )?;
    Ok(node)
}

/// Poll until some node reports an elected leader, or `timeout` elapses (fail-closed — a silent
/// "no leader" would hang the caller). Returns the elected leader id.
fn wait_for_leader(
    planes: &[RaftControlPlane],
    _bootstrap_id: u64,
    timeout: Duration,
) -> Result<u64, ControlError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(leader) = planes.first().and_then(RaftControlPlane::current_leader) {
            return Ok(leader);
        }
        if Instant::now() >= deadline {
            return Err(ControlError::Backend(
                "no control-plane leader elected within timeout".into(),
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
