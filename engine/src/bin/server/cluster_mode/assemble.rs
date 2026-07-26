#[cfg(feature = "distributed")]
use super::remote_connect;
use super::{info, warn, ClusterConfig, ClusterEngine, Normalizer, PathBuf, ShardError};

/// The mesh client-security pieces as plain bytes (ADR-071) — typed
/// `ClientSecurity` is built inside the distributed-gated connect path, so the
/// default (non-distributed) build never names the gated types.
pub(super) struct MeshClientParts {
    pub(super) ca: Option<Vec<u8>>,
    /// Consumed only by the distributed-gated connect path, hence the gated allowance.
    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    pub(super) domain: Option<String>,
    pub(super) token: Option<Vec<u8>>,
    /// Transport-resilience overrides (ADR-085) as plain values; the typed `MeshTransport`
    /// is built inside the distributed-gated path. `None` ⇒ the MeshTransport default. All
    /// consumed only there, hence the gated dead-code allowances.
    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    pub(super) connect_timeout_secs: Option<u64>,
    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    pub(super) read_timeout_secs: Option<u64>,
    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    pub(super) write_timeout_secs: Option<u64>,
    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    pub(super) keepalive_secs: Option<u64>,
    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    pub(super) read_retries: Option<u32>,
}

/// Assemble the `ClusterEngine` for the chosen backend: reopen an existing durable
/// in-process cluster, build a fresh one, or (under the `distributed` feature)
/// connect remote shard endpoints, ship the frozen feature space, and bulk-load.
#[allow(clippy::too_many_arguments)]
pub(super) fn assemble_cluster(
    in_process: bool,
    remote_groups: &[String],
    data_dir: Option<PathBuf>,
    cfg: &ClusterConfig,
    norm: Normalizer,
    vocab: Option<reverse_rusty::vocab::Vocab>,
    queries: &[(u64, String)],
    handle: &tokio::runtime::Handle,
    mesh: MeshClientParts,
    control_endpoints: &[String],
    route_by_assignments: bool,
) -> Result<ClusterEngine, ShardError> {
    if in_process {
        // only the remote path connects on the runtime / consults the quorum
        let _ = (handle, mesh, control_endpoints, route_by_assignments);
        if let Some(dir) = data_dir.filter(|d| ClusterEngine::cluster_exists(d)) {
            info!(data_dir = ?dir, "reopening durable cluster from manifest");
            // The manifest's persisted vocab is authoritative on a reopen (it matches
            // the committed segments); the file-supplied one only derived `norm`.
            let mut cluster = ClusterEngine::open(dir, norm, Some(cfg))?;
            if let Some(v) = vocab {
                if cluster.vocab().is_some() {
                    info!(
                        "--vocab-file ignored on reopen: the manifest's persisted \
                         vocabulary is authoritative (change it via PUT /_vocab)"
                    );
                } else if cluster.num_queries()? == 0 {
                    // A bare manifest (no persisted vocab) + an EMPTY corpus: activate
                    // the file vocab so this reopen behaves exactly like a fresh
                    // `build_with_vocab` — `set_vocab` installs the equivalence/alias
                    // machinery and its own durable checkpoint persists the vocab,
                    // BEFORE any --load-file ingest below (codex: this path used to
                    // ingest with the rules silently inert and the next reopen lost
                    // the file's vocabulary entirely).
                    info!("activating the vocab file on the empty reopened cluster");
                    cluster.set_vocab(v)?;
                } else {
                    warn!(
                        "--vocab-file NOT applied: this reopened cluster is populated \
                         and its manifest carries no vocabulary, so the file's \
                         equivalence/alias rules stay inactive (only its \
                         normalizer-level rules derived `norm`). Apply it explicitly \
                         via PUT /_vocab (a full blue/green rebuild)."
                    );
                }
            }
            if !queries.is_empty() {
                match cluster.num_queries()? {
                    0 => cluster.ingest(queries)?,
                    n => warn!(
                        existing = n,
                        "skipping --load-file: the reopened cluster is already populated"
                    ),
                }
            }
            return Ok(cluster);
        }
        // A vocab FILE must fully activate (ADR-076): `build_with_vocab` installs the
        // equivalence/alias machinery on the minted dict (a bare-normalizer build
        // would leave declared equivalences + registry aliases silently inert) and
        // persists the vocab in the manifest from the first durable commit.
        return match vocab {
            Some(v) => ClusterEngine::build_with_vocab(v, cfg, queries),
            None => ClusterEngine::build(norm, cfg, queries),
        };
    }
    // Remote shard servers run the STOCK normalizer (`shardserver` has no vocab flag)
    // and `AdoptDict` ships only the frozen dict — NO mechanism ships a normalizer
    // across processes. ANY vocab file would therefore split the feature space:
    // equivalence-driven rules would be silently inert, and even normalizer-level
    // rules (synonyms/phrases/punctuation/number-context) would have the coordinator
    // extracting queries and routing under a normalizer the shards' title side does
    // not run — cross-process query/title normalizer divergence, silent cross-form
    // false negatives (codex review broadened this from the equivalence-only check).
    // ADR-076 records the refusal. This build ships no mechanism to install or update a
    // normalizer/vocabulary consistently across remote shard processes.
    if vocab.is_some() {
        return Err(ShardError::Config(
            "a --vocab-file cannot apply to a REMOTE cluster (ADR-076): remote shard \
             servers run the stock normalizer and are not shipped vocabulary, so \
             queries and titles would normalize differently across processes (silent \
             false negatives — even for plain synonyms/phrases/punctuation). Remove \
             the vocab file or run the cluster in-process (--shards K)."
                .into(),
        ));
    }

    #[cfg(feature = "distributed")]
    {
        // Transport-resilience overrides (ADR-085): start from the always-on defaults and
        // apply any operator flags.
        let mut transport = reverse_rusty::cluster::MeshTransport::default();
        if let Some(s) = mesh.connect_timeout_secs {
            transport.connect_timeout = std::time::Duration::from_secs(s);
        }
        if let Some(s) = mesh.read_timeout_secs {
            transport.read_timeout = std::time::Duration::from_secs(s);
        }
        if let Some(s) = mesh.write_timeout_secs {
            transport.write_timeout = std::time::Duration::from_secs(s);
        }
        if let Some(s) = mesh.keepalive_secs {
            transport.keepalive_interval = std::time::Duration::from_secs(s);
        }
        if let Some(n) = mesh.read_retries {
            transport.read_retries = n;
        }
        let security = reverse_rusty::cluster::ClientSecurity {
            tls: mesh
                .ca
                .map(|ca_pem| reverse_rusty::cluster::TlsClientConfig {
                    ca_pem,
                    domain: mesh.domain,
                }),
            token: mesh.token,
            transport,
        };
        remote_connect::connect_remote_cluster(
            remote_groups,
            cfg,
            norm,
            queries,
            handle,
            security,
            control_endpoints,
            route_by_assignments,
        )
    }
    #[cfg(not(feature = "distributed"))]
    {
        let _ = (remote_groups, handle);
        Err(ShardError::Config(
            "--shard-endpoint requires a server built with --features distributed \
             (the gRPC RemoteShard transport is compiled out of this binary)"
                .into(),
        ))
    }
}
