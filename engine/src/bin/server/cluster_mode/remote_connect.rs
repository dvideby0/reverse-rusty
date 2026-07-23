//! Remote-coordinator assembly (ADR-070/083/086): connect a coordinator over `shardserver`
//! endpoints, optionally attach a durable control-plane quorum (with multi-endpoint failover), and —
//! under `--route-by-assignments` — make the committed quorum document the topology source of truth
//! (seed position-preserving → resolve → guard). Split out of `cluster_mode.rs` so both files stay
//! within the module-size budget.

use std::sync::Arc;
use std::sync::OnceLock;

use tracing::{info, warn};

use reverse_rusty::cluster::{
    ClientSecurity, ClusterConfig, ClusterEngine, ControlPlane, RemoteControlPlane, RemoteShard,
    ShardEndpoints, ShardError, ShardGroup,
};
use reverse_rusty::normalize::Normalizer;

/// Parse the CLI `--shard-endpoint` groups (`primary[,replica,...]`) into `ShardGroup`s, rejecting
/// an empty primary.
fn parse_groups(remote_groups: &[String]) -> Result<Vec<ShardGroup>, ShardError> {
    let groups: Vec<ShardGroup> = remote_groups
        .iter()
        .map(|g| {
            let mut parts = g.split(',').map(str::trim).map(str::to_string);
            let primary = parts.next().unwrap_or_default();
            ShardGroup {
                primary,
                replicas: parts.collect(),
            }
        })
        .collect();
    if groups.iter().any(|g| g.primary.is_empty()) {
        return Err(ShardError::Config(
            "every --shard-endpoint needs a primary endpoint (got an empty one)".into(),
        ));
    }
    Ok(groups)
}

fn process_coordinator_id() -> u64 {
    static ID: OnceLock<u64> = OnceLock::new();
    *ID.get_or_init(RemoteShard::new_coordinator_id)
}

/// Connect + validate the durable control-plane quorum (ADR-083) with multi-endpoint failover
/// (ADR-086). `None` when no `--control-endpoint` was given (the in-memory backend stays). Validates
/// the committed ring (`num_shards`/`vnodes`) BEFORE the caller routes by it; a dict-fingerprint
/// mismatch is a warning (routing is by the coordinator's own ring). A transient "no leader yet"
/// read fails loud here — `restart: unless-stopped` retries, like the shard connect race.
fn connect_control_plane(
    control_endpoints: &[String],
    cfg: &ClusterConfig,
    dict_fp: u64,
    handle: &tokio::runtime::Handle,
    security: &ClientSecurity,
) -> Result<Option<RemoteControlPlane>, ShardError> {
    if control_endpoints.is_empty() {
        return Ok(None);
    }
    info!(endpoints = ?control_endpoints, "attaching coordinator to durable control-plane quorum");
    let rcp =
        RemoteControlPlane::connect_failover(control_endpoints, handle.clone(), security.clone())
            .map_err(|e| ShardError::ControlPlane(format!("connect control plane: {e}")))?;
    let doc = rcp.cluster_state().map_err(|e| {
        ShardError::ControlPlane(format!(
            "read control-plane state (is a quorum leader up?): {e}"
        ))
    })?;
    if doc.num_shards != cfg.num_shards as u32 || doc.vnodes != cfg.vnodes {
        return Err(ShardError::ControlPlane(format!(
            "control-plane quorum ring (num_shards={}, vnodes={}) does not match this coordinator \
             (num_shards={}, vnodes={}); seed controlserver with --shards {} --vnodes {}",
            doc.num_shards, doc.vnodes, cfg.num_shards, cfg.vnodes, cfg.num_shards, cfg.vnodes
        )));
    }
    if doc.dict_fingerprint != dict_fp {
        warn!(
            "control-plane quorum feature-model fingerprint {} differs from this coordinator's {} \
             (seed controlserver with --fingerprint {}); routing is unaffected (the coordinator \
             routes by its own ring), but the cluster-state model label is stale",
            doc.dict_fingerprint, dict_fp, dict_fp
        );
    }
    Ok(Some(rcp))
}

fn groups_to_endpoints(groups: &[ShardGroup]) -> Vec<ShardEndpoints> {
    groups
        .iter()
        .map(|g| (g.primary.clone(), g.replicas.clone()))
        .collect()
}

fn endpoints_to_groups(eps: Vec<ShardEndpoints>) -> Vec<ShardGroup> {
    eps.into_iter()
        .map(|(primary, replicas)| ShardGroup { primary, replicas })
        .collect()
}

/// Decide the shard build groups under `--route-by-assignments` (ADR-086). The committed quorum is
/// the source of truth, so we **read it first**: a genesis (unseeded) quorum is seeded
/// position-preservingly from `--shard-endpoint`; a populated quorum is used as-is. When
/// `--shard-endpoint` is also given, a **guard** requires the committed map to be position-preserving
/// (equal to the CLI order) — a difference is a non-data-moving `rebalance` and routing it would be a
/// false negative. With no CLI endpoints (resolve-only boot) the committed map is trusted. Without the
/// flag, the CLI groups are used unchanged (today's path, byte-identical).
///
/// Reading before seeding is load-bearing: seeding first would overwrite a rebalanced map back to the
/// CLI order and silently DEFEAT the guard.
fn build_groups(
    route_by_assignments: bool,
    cli_groups: Vec<ShardGroup>,
    control: Option<&RemoteControlPlane>,
    cfg: &ClusterConfig,
) -> Result<Vec<ShardGroup>, ShardError> {
    if !route_by_assignments {
        return Ok(cli_groups);
    }
    let Some(rcp) = control else {
        return Err(ShardError::Config(
            "--route-by-assignments requires --control-endpoint (the committed quorum is the \
             topology source of truth)"
                .into(),
        ));
    };
    // The read-first seed/resolve/guard decision is the lean `route_topology` (unit-tested vs
    // `InMemoryControlPlane`); this is the `ShardGroup` adapter over it.
    let cli_eps = groups_to_endpoints(&cli_groups);
    let resolved = reverse_rusty::cluster::route_topology(rcp, cfg.num_shards as u32, &cli_eps)?;
    info!(
        num_shards = cfg.num_shards,
        resolve_only = cli_eps.is_empty(),
        "routing by committed shard→node assignments (ADR-086)"
    );
    Ok(endpoints_to_groups(resolved))
}

/// Connect a coordinator over remote `shardserver` endpoints: mint + freeze the feature space over
/// the load corpus (pass A of `build`, so a restart re-mints the identical dict and the ADR-034
/// fingerprint handshake holds), attach the control-plane quorum FIRST (so routing-by-assignments can
/// read/seed the committed document before choosing endpoints), connect (the dict + tag space ship at
/// connect), then bulk-load an empty cluster.
#[allow(clippy::too_many_arguments)]
pub(crate) fn connect_remote_cluster(
    remote_groups: &[String],
    cfg: &ClusterConfig,
    norm: Normalizer,
    queries: &[(u64, String)],
    handle: &tokio::runtime::Handle,
    security: ClientSecurity,
    control_endpoints: &[String],
    route_by_assignments: bool,
) -> Result<ClusterEngine, ShardError> {
    let cli_groups = parse_groups(remote_groups)?;

    let (dict, tag_dict) = ClusterEngine::freeze_feature_space(&norm, queries, &[]);
    let norm = Arc::new(norm);
    let dict = Arc::new(dict);
    let dict_fp = dict.fingerprint();
    let tag_dict = Arc::new(tag_dict);

    // Attach the durable control-plane quorum BEFORE building shards (ADR-083/086): when routing by
    // assignments we must read/seed the committed document to know which endpoints to connect. The
    // control plane is off the matching hot path, so this never affects a percolate's result.
    let control = connect_control_plane(control_endpoints, cfg, dict_fp, handle, &security)?;
    let groups = build_groups(route_by_assignments, cli_groups, control.as_ref(), cfg)?;

    let plain = groups.iter().all(|g| g.replicas.is_empty());
    let coordinator_id = process_coordinator_id();
    let cluster = if plain && cfg.replication_factor == 1 {
        let endpoints: Vec<String> = groups.into_iter().map(|g| g.primary).collect();
        ClusterEngine::connect_remote_exclusive_with_security(
            norm,
            dict,
            tag_dict,
            cfg,
            &endpoints,
            handle,
            coordinator_id,
            security,
        )?
    } else {
        ClusterEngine::connect_replicated_exclusive_with_security(
            norm,
            dict,
            tag_dict,
            cfg,
            &groups,
            handle,
            coordinator_id,
            security,
        )?
    };

    let cluster = match control {
        Some(rcp) => cluster.with_control_plane(Box::new(rcp)),
        None => cluster,
    };

    if !queries.is_empty() {
        match cluster.num_queries()? {
            0 => cluster.ingest(queries)?,
            n => warn!(
                existing = n,
                "skipping --load-file: the remote cluster is already populated"
            ),
        }
    }
    Ok(cluster)
}
