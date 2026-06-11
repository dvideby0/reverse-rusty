//! `ShardServer` — serves the gRPC `ShardService` over ONE in-process `LocalShard`.
//!
//! Construct it over the SAME frozen `Arc<Dict>` / `Arc<Normalizer>` the coordinator
//! uses for placement. The write path carries raw DSL (not pre-extracted feature
//! ids), so the server re-compiles read-only against ITS copy of that dict — a
//! dict-agnostic wire that fails loud on mismatch rather than corrupting matches.
//! Placement + routing stay the coordinator's job; the server is a dumb executor of
//! `percolate` / `ingest` / `insert` / `delete` / `flush`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tonic::Status;

use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::segment::PlacedQuery;
use crate::tagdict::TagDict;

use super::proto::shard_service_server::ShardServiceServer;
use super::security::{ClientSecurity, MeshAuthVerify, ServerSecurity, TlsServerIdentity};
use super::shard::{LocalShard, ShardError};

mod service;

#[cfg(test)]
mod tests;

/// The adopted dict, persisted by a durable `AdoptDict` so a restarted node can
/// self-restore without a coordinator (ADR-072). Written atomically (tmp + rename).
const ADOPTED_DICT_FILE: &str = "dict.bin";
/// The adopted tag space (ADR-055), persisted alongside the dict.
const ADOPTED_TAGDICT_FILE: &str = "tagdict.bin";

/// Persist the adopted (already fingerprint-verified) dict + tag-space bytes under
/// `dir` — write-to-tmp + atomic rename, so a crash mid-write leaves either the old
/// file or the new one, never a torn blob.
fn persist_adopted_space(dir: &Path, dict_bytes: &[u8], tag_bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    for (name, bytes) in [
        (ADOPTED_DICT_FILE, dict_bytes),
        (ADOPTED_TAGDICT_FILE, tag_bytes),
    ] {
        let tmp = dir.join(format!("{name}.tmp"));
        std::fs::write(&tmp, bytes)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, dir.join(name))?;
    }
    Ok(())
}

struct ServerState {
    dict: Arc<Dict>,
    /// The frozen per-query tag space (ADR-049/055), shipped by the coordinator via `AdoptDict`
    /// alongside the dict. Held so the server resolves ingested tags read-only against the same
    /// space the coordinator's filter `TagId`s came from. Empty until adopted (a pre-built `new`
    /// server starts empty; the coordinator's adopt installs the real one).
    tag_dict: Arc<TagDict>,
    shard: LocalShard,
}

/// A gRPC server wrapping ONE in-process shard.
///
/// The (dict, shard) pair is **swappable**: a server may start *pending* (dict-less) via
/// [`ShardServer::pending`] and adopt the coordinator's frozen dict through the `AdoptDict`
/// RPC, so a data node need not rebuild a byte-identical dict from the corpus out-of-band
/// (ADR-034). `norm` + `config` are fixed for the server's life (the normalizer must still
/// match the coordinator's — `default_vocab()` today; see ADR-034 scope note).
pub struct ShardServer {
    norm: Arc<Normalizer>,
    config: EngineConfig,
    /// `Some` ⇒ a **durable** node: its shard persists segments under this dir (ADR-035), so
    /// the node can serve `FetchSegments` (stream its segments to a recovering peer) and accept
    /// `RecoverFrom` (pull a peer's segments + attach). `None` ⇒ in-memory (today's default).
    /// When set, `AdoptDict` builds a durable (segments-only) shard rather than an in-memory one.
    data_dir: Option<PathBuf>,
    /// `None` until a dict is adopted; reads against a pending server return
    /// `failed_precondition`.
    state: ArcSwapOption<ServerState>,
    /// The fence generation (ADR-044, step 6b): `0` ⇒ not fenced; `> 0` ⇒ this node has been
    /// demoted as the owner of its shard at that generation, so data-mutating writes
    /// (`insert`/`delete`/`ingest`) return `failed_precondition`. Reads + the recovery RPCs stay
    /// served (serve-then-drop). Set monotonically by the `Fence` RPC (a stale lower-gen Fence
    /// never un-fences). A live handoff fences the old owner, drains its tail to the new owner, then
    /// flips routing — the fence holds a brief write-quiesce across that flip.
    fenced_at_generation: AtomicU64,
    /// Mesh security (ADR-071): TLS identity + expected cluster token, applied by the
    /// `serve*` methods. Default (none) ⇒ the historical plaintext/open behavior.
    security: ServerSecurity,
    /// The CLIENT half of the mesh security (ADR-071) — what THIS node presents when it
    /// dials OUT (the `RecoverFrom` handler's pull from a peer source). Default (none) ⇒
    /// plaintext, the historical behavior.
    client_security: ClientSecurity,
}

impl ShardServer {
    /// Build a server over a fresh `LocalShard` sharing the given frozen `norm`/`dict` —
    /// the pre-built path (the dict is already arranged to match the coordinator's).
    pub fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        // Pre-built path: starts with an empty tag space; a tagged deployment ships the real one
        // via `AdoptDict` (which rebuilds the shard over it). Empty + finalized so the read-only
        // tag-resolution invariant holds even before an adopt.
        let tag_dict = Arc::new(finalized_empty_tag_dict());
        let shard = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tag_dict),
            config.clone(),
        );
        let state = ArcSwapOption::from(Some(Arc::new(ServerState {
            dict,
            tag_dict,
            shard,
        })));
        ShardServer {
            norm,
            config,
            data_dir: None,
            state,
            fenced_at_generation: AtomicU64::new(0),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
        }
    }

    /// Build a **pending** server: no dict yet, awaiting an `AdoptDict` from the coordinator
    /// (ADR-034). Reads return `failed_precondition` until a dict is adopted. This is how a
    /// data node starts in a real multi-node deploy — empty, then handed the frozen dict —
    /// instead of rebuilding a byte-identical dict from the whole corpus out-of-band.
    pub fn pending(norm: Arc<Normalizer>, config: EngineConfig) -> Self {
        ShardServer {
            norm,
            config,
            data_dir: None,
            state: ArcSwapOption::from(None),
            fenced_at_generation: AtomicU64::new(0),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
        }
    }

    /// Open (or start) a durable data node at `data_dir` (ADR-072): if the node
    /// previously adopted a dict (persisted alongside its shard state by the durable
    /// `AdoptDict` path), **self-restore** — deserialize the persisted dict + tag
    /// space and reopen the shard from its checkpoint sidecar + translog tail
    /// (ADR-039 §6) — so a restarted container/process resumes serving without
    /// waiting for a coordinator. A fresh directory starts **pending** exactly like
    /// [`Self::pending_durable`]. This is what a deployable node should boot through;
    /// `pending_durable` remains the explicit always-start-empty constructor.
    pub fn open_durable(
        norm: Arc<Normalizer>,
        config: EngineConfig,
        data_dir: PathBuf,
    ) -> Result<Self, ShardError> {
        let dict_path = data_dir.join(ADOPTED_DICT_FILE);
        let tag_path = data_dir.join(ADOPTED_TAGDICT_FILE);
        if !dict_path.exists() {
            return Ok(Self::pending_durable(norm, config, data_dir));
        }
        let dict_bytes = std::fs::read(&dict_path)
            .map_err(|e| ShardError::Log(format!("reading {}: {e}", dict_path.display())))?;
        let dict = Arc::new(crate::storage::deserialize_dict(&dict_bytes).map_err(|e| {
            ShardError::Log(format!(
                "deserializing persisted dict {}: {e}",
                dict_path.display()
            ))
        })?);
        // The tag space ships (and persists) atomically with the dict; an absent file
        // means a pre-ADR-072 node — treat as the empty (finalized) space.
        let tag_dict = Arc::new(if tag_path.exists() {
            let bytes = std::fs::read(&tag_path)
                .map_err(|e| ShardError::Log(format!("reading {}: {e}", tag_path.display())))?;
            crate::storage::deserialize_tagdict(&bytes).map_err(|e| {
                ShardError::Log(format!(
                    "deserializing persisted tag dict {}: {e}",
                    tag_path.display()
                ))
            })?
        } else {
            finalized_empty_tag_dict()
        });
        let mut sc = config.clone();
        sc.data_dir = Some(data_dir.clone());
        // `new_durable` self-restores via the checkpoint sidecar when one exists
        // (segments attached + translog tail replayed, fingerprint-checked).
        let shard = LocalShard::new_durable(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tag_dict),
            sc,
        )?;
        let state = ArcSwapOption::from(Some(Arc::new(ServerState {
            dict,
            tag_dict,
            shard,
        })));
        Ok(ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            state,
            fenced_at_generation: AtomicU64::new(0),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
        })
    }

    /// A **durable, pending** server (ADR-035/036): empty (awaiting `AdoptDict`) but rooted at
    /// `data_dir`, so once it adopts a dict its shard persists segments there. This is the real
    /// recovering/replica node — after adoption it can serve `FetchSegments` and accept
    /// `RecoverFrom`. The durable analogue of [`Self::pending`].
    pub fn pending_durable(norm: Arc<Normalizer>, config: EngineConfig, data_dir: PathBuf) -> Self {
        ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            state: ArcSwapOption::from(None),
            fenced_at_generation: AtomicU64::new(0),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
        }
    }

    /// A **durable, pre-built** server: build a segments-only durable shard over `dict` rooted
    /// at `data_dir`. The durable analogue of [`Self::new`]. Errors if the durable engine cannot
    /// be created (e.g. the dir is unwritable).
    pub fn new_durable(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
        data_dir: PathBuf,
    ) -> Result<Self, ShardError> {
        let mut sc = config.clone();
        sc.data_dir = Some(data_dir.clone());
        let tag_dict = Arc::new(finalized_empty_tag_dict());
        let shard = LocalShard::new_durable(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tag_dict),
            sc,
        )?;
        let state = ArcSwapOption::from(Some(Arc::new(ServerState {
            dict,
            tag_dict,
            shard,
        })));
        Ok(ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            state,
            fenced_at_generation: AtomicU64::new(0),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
        })
    }

    /// Whether this server currently holds an adopted/restored state (false ⇒ pending,
    /// awaiting `AdoptDict`). Introspection for the deployable bin's startup banner.
    pub fn is_serving(&self) -> bool {
        self.state.load_full().is_some()
    }

    /// The adopted state, or `failed_precondition` if the server is still pending.
    fn loaded(&self) -> Result<Arc<ServerState>, Status> {
        self.state
            .load_full()
            .ok_or_else(|| Status::failed_precondition("shard has not adopted a dict yet"))
    }

    /// Reject a data-mutating write if this node has been fenced (demoted by a live handoff,
    /// ADR-044). Called by `insert`/`delete`/`ingest` only — reads + the recovery RPCs deliberately
    /// do NOT call it, so the demoted owner keeps serving them until the coordinator stops routing
    /// to it (serve-then-drop), and an in-flight read never hits the fence.
    fn check_not_fenced(&self) -> Result<(), Status> {
        let gen = self.fenced_at_generation.load(Ordering::Acquire);
        if gen > 0 {
            return Err(Status::failed_precondition(format!(
                "shard is fenced at generation {gen} (demoted by a handoff); writes are rejected"
            )));
        }
        Ok(())
    }

    /// Compile + bulk-load raw `(id, DSL)` queries into this shard before serving —
    /// the server-side preload for standing up a populated node. Read-only against the
    /// adopted frozen dict; parse failures are skipped (like `build`/`ingest`). No-op on a
    /// pending (not-yet-adopted) server.
    pub fn ingest_dsl(&self, items: &[(u64, String)]) {
        let Some(st) = self.state.load_full() else {
            return;
        };
        let mut lc = String::new();
        let extracted: Vec<PlacedQuery> = items
            .iter()
            .filter_map(|(logical, dsl)| {
                let ast = crate::dsl::parse(dsl).ok()?;
                let ex = extract_readonly(&ast, &self.norm, &st.dict, &mut lc);
                Some(PlacedQuery {
                    logical: *logical,
                    ex,
                    dsl: dsl.clone(),
                    version: 1,
                    tags: Vec::new(),
                })
            })
            .collect();
        st.shard.ingest_local(&extracted);
    }

    /// Install mesh security (ADR-071): a TLS identity to present and/or the
    /// expected cluster token, applied by every `serve*` method. Unset ⇒ the
    /// historical plaintext/open behavior, byte-identical.
    #[must_use]
    pub fn with_security(mut self, security: ServerSecurity) -> Self {
        self.security = security;
        self
    }

    /// Install the CLIENT half of the mesh security (ADR-071) — used when this node
    /// dials OUT (the `RecoverFrom` handler pulls segments + translog from the peer
    /// source). Without it a secured source would reject this node's pull; with it the
    /// internal dial rides the same TLS + token as every coordinator connection.
    #[must_use]
    pub fn with_client_security(mut self, security: ClientSecurity) -> Self {
        self.client_security = security;
        self
    }

    /// Build the tonic server (TLS applied when configured) + the token-verified
    /// service — one assembly shared by every `serve*` flavor so they cannot drift.
    #[allow(clippy::type_complexity)]
    fn secured_router(self) -> Result<tonic::transport::server::Router, tonic::transport::Error> {
        let security = self.security.clone();
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = &security.tls {
            builder = builder.tls_config(server_tls_config(tls))?;
        }
        // The verifier wraps the WHOLE service (pass-through with no token), so every
        // RPC — including a future one — is covered before its handler runs.
        let verify = MeshAuthVerify::new(security.token);
        Ok(builder.add_service(ShardServiceServer::with_interceptor(self, verify)))
    }

    /// Serve `ShardService` on `addr` until the returned future completes.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        self.secured_router()?.serve(addr).await
    }

    /// Serve with a graceful-shutdown `signal` future — used by tests to stop cleanly.
    pub async fn serve_with_shutdown<F>(
        self,
        addr: SocketAddr,
        signal: F,
    ) -> Result<(), tonic::transport::Error>
    where
        F: std::future::Future<Output = ()>,
    {
        self.secured_router()?
            .serve_with_shutdown(addr, signal)
            .await
    }

    /// Serve `ShardService` on an already-bound `incoming` listener (no rebind). Lets a
    /// caller bind the socket first and learn its port — an ephemeral `:0` for tests, or
    /// socket activation in production — without the bind→drop→rebind gap that re-binding
    /// by address would open.
    pub async fn serve_with_incoming(
        self,
        incoming: tonic::transport::server::TcpIncoming,
    ) -> Result<(), tonic::transport::Error> {
        self.secured_router()?.serve_with_incoming(incoming).await
    }
}

/// Build the tonic `ServerTlsConfig` from an operator identity — shared with
/// [`ControlServer`](super::control_server::ControlServer) via the same shapes.
pub(crate) fn server_tls_config(tls: &TlsServerIdentity) -> tonic::transport::ServerTlsConfig {
    tonic::transport::ServerTlsConfig::new().identity(tonic::transport::Identity::from_pem(
        &tls.cert_pem,
        &tls.key_pem,
    ))
}

/// Compile one raw query read-only against the shared frozen dict (parse failure →
/// `None`, counted by the caller as a rejected-parse).
fn compile_item(norm: &Normalizer, dict: &Dict, dsl: &str, lc: &mut String) -> Option<Extracted> {
    let ast = crate::dsl::parse(dsl).ok()?;
    Some(extract_readonly(&ast, norm, dict, lc))
}

/// An empty but FINALIZED tag space — the placeholder a pre-built / pending server holds until the
/// coordinator's `AdoptDict` installs the real one (ADR-055). Finalized so the engine's read-only
/// tag-resolution invariant (`debug_assert!(is_finalized())`) holds even before an adopt.
fn finalized_empty_tag_dict() -> TagDict {
    let mut td = TagDict::new();
    td.mark_finalized();
    td
}
