//! `RemoteShard` — a [`Shard`] backed by a gRPC `ShardService` client.
//!
//! Implements the SYNC [`Shard`] trait by blocking on its async tonic client via a
//! [`tokio::runtime::Handle`], confining all async to this type so the coordinator,
//! `LocalShard`, and the oracle stay synchronous. A failed RPC surfaces as
//! [`ShardError::Remote`] — never a swallowed empty result, which would shrink a
//! percolate's union into a false negative.
//!
//! All RPCs are driven through [`block_on_in_context`], which keeps the sync→async bridge
//! safe regardless of the CALLER's thread context (the seam is sync, but a coordinator may
//! probe a shard from a rayon worker, a plain thread, OR — for a future async coordinator
//! server — a tokio runtime worker). The naive `Handle::block_on` panics with a
//! nested-runtime error when called on a runtime worker, so the bridge dispatches on the
//! caller's context: off any runtime (rayon fan-out / the in-process build path) it is a
//! plain `block_on`; on a multi-thread runtime worker it wraps `block_on` in
//! `task::block_in_place` (the documented re-entry pattern); on a current-thread runtime it
//! offloads to a scoped non-runtime thread. The cost — a parked worker per in-flight RPC —
//! is the latency of distribution itself; an async fan-out is the documented later
//! optimization (ADR-029). See ADR-047 for the thread-context contract.

use std::future::Future;

use tokio::runtime::{Handle, RuntimeFlavor};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::compile::Extracted;
use crate::exact::TagPredicate;
use crate::segment::{IngestReport, MatchStats, PlacedQuery};

use super::clog::{ClusterMutation, LogPos};
use super::proto;
use super::proto::shard_service_client::ShardServiceClient;
use super::security::{configure_endpoint, ClientSecurity, MeshAuthInject};
use super::shard::{Shard, ShardError};

/// The mesh-aware client channel (ADR-071): every RPC flows through the
/// [`MeshAuthInject`] interceptor, which attaches the cluster token when one is
/// configured and is a no-op otherwise — so the secured and plaintext paths share
/// ONE client type and no RPC call site changes.
pub(crate) type MeshChannel = InterceptedService<Channel, MeshAuthInject>;

/// Async mesh connect (ADR-071): configure the endpoint (TLS when the security
/// config carries it), eagerly connect, wrap with the token interceptor. The
/// async core under [`connect_channel`], and the dial the server-side `RecoverFrom`
/// handler uses for its OUTBOUND peer connection — one path, so an internal dial
/// can never silently skip the mesh security.
pub(crate) async fn connect_mesh(
    endpoint: &str,
    security: &ClientSecurity,
) -> Result<ShardServiceClient<MeshChannel>, ShardError> {
    let ep = configure_endpoint(endpoint, security.tls.as_ref())?;
    let channel = ep
        .connect()
        .await
        .map_err(|e| ShardError::Remote(format!("connect: {e}")))?;
    let inject = MeshAuthInject::new(security.token.as_deref())?;
    Ok(ShardServiceClient::with_interceptor(channel, inject))
}

/// One shard living behind a gRPC `ShardService`.
pub struct RemoteShard {
    client: ShardServiceClient<MeshChannel>,
    handle: Handle,
    /// The coordinator's frozen-dict fingerprint (verified equal to the server's at connect).
    /// Carried so dict-guarded RPCs (e.g. `FetchTranslog`) can present it.
    dict_fp: u64,
}

/// Connect the mesh channel: configure the endpoint (TLS when the security config
/// carries it), eagerly connect on `handle` (a bad endpoint/handshake fails here,
/// not on the first RPC), and wrap it with the token-injecting interceptor.
fn connect_channel(
    endpoint: &str,
    handle: &Handle,
    security: &ClientSecurity,
) -> Result<ShardServiceClient<MeshChannel>, ShardError> {
    block_on_in_context(handle, connect_mesh(endpoint, security))
}

impl RemoteShard {
    /// Connect to a `ShardService` at `endpoint` (e.g. `"http://127.0.0.1:50051"`),
    /// driving the async connect on `handle`, then verify the server's frozen-dict
    /// fingerprint equals `expected_fp` (the coordinator's
    /// [`crate::dict::Dict::fingerprint`]). A mismatch returns [`ShardError::DictMismatch`]
    /// — a divergent dict would otherwise drop matches silently across the wire (ADR-029).
    pub fn connect(endpoint: &str, handle: Handle, expected_fp: u64) -> Result<Self, ShardError> {
        Self::connect_with_security(endpoint, handle, expected_fp, &ClientSecurity::default())
    }

    /// [`connect`](Self::connect) over a secured mesh link (ADR-071): TLS per the
    /// client config, the mesh token attached to every RPC. A default (empty)
    /// security config is byte-identical to the plaintext path.
    pub fn connect_with_security(
        endpoint: &str,
        handle: Handle,
        expected_fp: u64,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        let client = connect_channel(endpoint, &handle, security)?;
        // Handshake before trusting the shard: clone the client for the probe RPC (a cheap
        // Channel bump, mirroring the per-call pattern below).
        let mut probe = client.clone();
        let actual_fp = block_on_in_context(&handle, async move {
            probe.dict_fingerprint(proto::Empty {}).await
        })
        .map_err(rpc_err)?
        .into_inner()
        .fingerprint;
        if actual_fp != expected_fp {
            return Err(ShardError::DictMismatch {
                expected: expected_fp,
                actual: actual_fp,
            });
        }
        Ok(RemoteShard {
            client,
            handle,
            dict_fp: expected_fp,
        })
    }

    /// Connect, then **ship** the coordinator's frozen dict to the server (`AdoptDict`,
    /// ADR-034) before trusting it — so a data node need not have rebuilt a byte-identical
    /// dict from the corpus out-of-band. `dict_bytes` is `crate::storage::serialize_dict` of
    /// the coordinator's dict; `expected_fp` is its [`crate::dict::Dict::fingerprint`].
    ///
    /// The server adopts onto an empty shard and no-ops if it already holds this dict; the
    /// returned fingerprint then *is* the handshake (it must equal `expected_fp`). If the
    /// server holds data under a **different** dict it refuses (`FailedPrecondition`), which
    /// we surface as [`ShardError::DictMismatch`] (reading back its actual fingerprint) — a
    /// divergent populated server fails loud instead of dropping matches silently.
    pub fn connect_and_adopt(
        endpoint: &str,
        handle: Handle,
        dict_bytes: Vec<u8>,
        expected_fp: u64,
        tag_dict_bytes: Vec<u8>,
        expected_tag_fp: u64,
    ) -> Result<Self, ShardError> {
        Self::connect_and_adopt_with_security(
            endpoint,
            handle,
            dict_bytes,
            expected_fp,
            tag_dict_bytes,
            expected_tag_fp,
            &ClientSecurity::default(),
        )
    }

    /// [`connect_and_adopt`](Self::connect_and_adopt) over a secured mesh link
    /// (ADR-071). A default (empty) security config is byte-identical.
    #[allow(clippy::too_many_arguments)]
    pub fn connect_and_adopt_with_security(
        endpoint: &str,
        handle: Handle,
        dict_bytes: Vec<u8>,
        expected_fp: u64,
        tag_dict_bytes: Vec<u8>,
        expected_tag_fp: u64,
        security: &ClientSecurity,
    ) -> Result<Self, ShardError> {
        let client = connect_channel(endpoint, &handle, security)?;
        let mut shipper = client.clone();
        // Ship the dict AND the frozen tag space (ADR-049/055) in one atomic adopt — never a window
        // where the server has the dict but not the tag space.
        let req = proto::AdoptDictRequest {
            dict: dict_bytes,
            fingerprint: expected_fp,
            tag_dict: tag_dict_bytes,
            tag_dict_fingerprint: expected_tag_fp,
        };
        let (adopted, adopted_tag) =
            match block_on_in_context(&handle, async move { shipper.adopt_dict(req).await }) {
                Ok(reply) => {
                    let r = reply.into_inner();
                    (r.fingerprint, r.tag_dict_fingerprint)
                }
                // The server holds data under a different dict and refused ours. Read its actual
                // fingerprint so the mismatch is truthful, then fail loud (never a silent drop).
                Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                    let mut probe = client.clone();
                    let actual = block_on_in_context(&handle, async move {
                        probe.dict_fingerprint(proto::Empty {}).await
                    })
                    .map_or(0, |r| r.into_inner().fingerprint);
                    return Err(ShardError::DictMismatch {
                        expected: expected_fp,
                        actual,
                    });
                }
                Err(status) => return Err(ShardError::Remote(format!("adopt_dict: {status}"))),
            };
        // On success the server echoes the fingerprints it now serves — this equality IS the
        // dict-identity handshake, so no separate round-trip is needed. The tag-dict fingerprint is
        // checked the same way: a divergent tag space would mis-filter reads (ADR-055).
        if adopted != expected_fp {
            return Err(ShardError::DictMismatch {
                expected: expected_fp,
                actual: adopted,
            });
        }
        if adopted_tag != expected_tag_fp {
            return Err(ShardError::Remote(format!(
                "tag-dict fingerprint mismatch after adopt: coordinator {expected_tag_fp:#018x} != \
                 server {adopted_tag:#018x} (the shipped tag space did not round-trip)"
            )));
        }
        Ok(RemoteShard {
            client,
            handle,
            dict_fp: expected_fp,
        })
    }

    /// Drive an async RPC to completion from the synchronous [`Shard`] seam, safe regardless
    /// of the caller's thread context (see the module docs + ADR-047). Every RPC method below
    /// goes through this rather than `self.handle.block_on` directly, so a percolate or write
    /// issued from a tokio runtime worker re-enters via `block_in_place` instead of panicking.
    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        block_on_in_context(&self.handle, fut)
    }

    /// Drive this remote node's `RecoverFrom` RPC (ADR-036): it pulls `source_endpoint`'s sealed
    /// segments (via that peer's `FetchSegments`), writes them under its own data_dir, attaches
    /// them, and starts serving — the cross-node peer-recovery primitive. `dict_fp` must equal
    /// the coordinator's frozen-dict fingerprint (the server re-checks it). Returns
    /// `(segments_attached, num_queries, up_to_seqno)` — the last being the snapshot's translog
    /// position `P` (ADR-039), from which the coordinator replays the source's tail (> P) to
    /// finish a no-quiesce recovery. The node must be durable + have adopted the dict.
    pub fn recover_from(
        &self,
        source_endpoint: &str,
        dict_fp: u64,
    ) -> Result<(u64, u64, u64), ShardError> {
        let mut client = self.client.clone();
        let req = proto::RecoverFromRequest {
            source_endpoint: source_endpoint.to_string(),
            dict_fingerprint: dict_fp,
        };
        let reply = self
            .block_on(async move { client.recover_from(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok((
            reply.segments_attached,
            reply.num_queries,
            reply.up_to_seqno,
        ))
    }

    /// Fence this remote node as the owner of its shard at `generation` (ADR-044, step 6b): the
    /// server stops accepting data-mutating writes (they return `failed_precondition`) while it
    /// keeps serving reads + the recovery RPCs — the brief write-quiesce a live handoff holds across
    /// the routing flip (serve-then-drop). Monotonic server-side (a stale lower-generation fence is
    /// a no-op). Returns the server's fence generation after the call. Inherent (not a [`Shard`]
    /// method): only the handoff orchestrator fences a specific old owner, addressed by endpoint.
    pub fn fence(&self, generation: u64) -> Result<u64, ShardError> {
        let mut client = self.client.clone();
        let req = proto::FenceRequest {
            generation,
            dict_fingerprint: self.dict_fp,
        };
        let reply = self
            .block_on(async move { client.fence(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(reply.fenced_at_generation)
    }

    /// Lift this remote node's fence at `generation` (ADR-048): the CAS-guarded inverse of
    /// [`Self::fence`]. The server clears the fence only if it currently holds exactly
    /// `generation` (a stale unfence, or a newer handoff's higher-generation re-fence, is a
    /// no-op), then resumes accepting writes. Returns the server's fence generation after the
    /// call (0 ⇒ un-fenced). Called by the handoff orchestrator when a handoff aborts after
    /// fencing, so the source self-heals instead of staying permanently write-quiesced.
    pub fn unfence(&self, generation: u64) -> Result<u64, ShardError> {
        let mut client = self.client.clone();
        let req = proto::UnfenceRequest {
            generation,
            dict_fingerprint: self.dict_fp,
        };
        let reply = self
            .block_on(async move { client.unfence(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(reply.fenced_at_generation)
    }
}

/// Drive `fut` on `handle` from a SYNCHRONOUS caller, dispatching on the caller's tokio
/// context so the bridge never panics with the nested-runtime error (ADR-047):
/// - **off any runtime** (a rayon fan-out worker, the in-process build path, a plain thread):
///   a plain [`Handle::block_on`] — the fast path, unchanged from before.
/// - **on a multi-thread runtime worker**: [`tokio::task::block_in_place`] around `block_on`,
///   the documented way to re-enter a multi-thread scheduler's async context without starving
///   it (`Runtime::new()` / tonic / axum are all multi-thread).
/// - **on a current-thread runtime**: `block_in_place` is unavailable there, so the drive is
///   offloaded to a scoped helper thread — not a runtime worker, so `block_on` is safe on it.
///
/// `Handle::try_current` only DETECTS the caller's context/flavor; the future is always driven
/// on the passed `handle` (the shard's runtime), which may or may not be the current one.
fn block_on_in_context<F>(handle: &Handle, fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    match Handle::try_current() {
        Err(_) => handle.block_on(fut),
        Ok(current) => match current.runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(fut)),
            // Current-thread (or any non-multi-thread) runtime: can't park the only worker, so
            // drive on a scoped non-runtime thread, forwarding any panic from the future intact.
            _ => std::thread::scope(|s| {
                s.spawn(|| handle.block_on(fut))
                    .join()
                    .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            }),
        },
    }
}

fn rpc_err<E: std::fmt::Display>(e: E) -> ShardError {
    ShardError::Remote(e.to_string())
}

impl Shard for RemoteShard {
    fn percolate_filtered(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let mut client = self.client.clone();
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            // Ship the ALREADY-RESOLVED `TagId` groups (ADR-055); empty ⇒ unfiltered.
            filter: proto::tag_predicate_to_proto(pred),
            rank: None,
        };
        let reply = self
            .block_on(async move { client.percolate(req).await })
            .map_err(rpc_err)?
            .into_inner();
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids, stats))
    }

    fn percolate_filtered_ranked(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        let mut client = self.client.clone();
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            // The ALREADY-COMPILED spec (ADR-075): resolved `TagId` boosts + the priority
            // key, exactly like the filter groups — the server never re-resolves strings.
            rank: Some(proto::rank_spec_to_proto(spec)),
        };
        let reply = self
            .block_on(async move { client.percolate(req).await })
            .map_err(rpc_err)?
            .into_inner();
        // Version-skew honesty: an older server ignores the `rank` field and leaves
        // `ranked` false — fail LOUD rather than fabricate scores or silently hand the
        // caller an unranked ordering it will present as ranked.
        if !reply.ranked || reply.scores.len() != reply.ids.len() {
            return Err(ShardError::Remote(format!(
                "shard did not score a ranked percolate (ranked={}, ids={}, scores={}): \
                 the server predates cluster ranking (ADR-075) — upgrade it or drop the \
                 rank block",
                reply.ranked,
                reply.ids.len(),
                reply.scores.len()
            )));
        }
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids.into_iter().zip(reply.scores).collect(), stats))
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        let mut client = self.client.clone();
        let reply = self
            .block_on(async move { client.num_queries(proto::Empty {}).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(reply.count as usize)
    }

    fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        let mut client = self.client.clone();
        let reply = self
            .block_on(async move { client.class_counts(proto::Empty {}).await })
            .map_err(rpc_err)?
            .into_inner();
        let c = reply.counts;
        if c.len() != 4 {
            return Err(ShardError::Remote(format!(
                "class_counts: expected 4 entries, got {}",
                c.len()
            )));
        }
        Ok([c[0], c[1], c[2], c[3]])
    }

    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        refuse_wire_tag_ids(items)?;
        let mut client = self.client.clone();
        // Send raw DSL + raw tags, NOT the pre-extracted feature ids: the server re-compiles
        // read-only against its own frozen dict + resolves tags against its adopted frozen tag
        // space (dict-/tag-agnostic wire). The coordinator's `Extracted` was only for placement.
        let req = proto::IngestRequest {
            items: items
                .iter()
                .map(|q| proto::AddItem {
                    logical_id: q.logical,
                    dsl: q.dsl.clone(),
                    version: q.version,
                    tags: proto::tags_to_proto(&q.tags),
                })
                .collect(),
        };
        let reply = self
            .block_on(async move { client.ingest_extracted(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(IngestReport {
            ingested: reply.ingested as usize,
            rejected_parse: reply.rejected_parse as usize,
            rejected_class_d: reply.rejected_class_d as usize,
        })
    }

    fn insert_extracted_with_tags(
        &self,
        _ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        let mut client = self.client.clone();
        let req = proto::InsertRequest {
            item: Some(proto::AddItem {
                logical_id: logical,
                dsl: text.to_string(),
                version,
                tags: proto::tags_to_proto(tags),
            }),
        };
        let reply = self
            .block_on(async move { client.insert_extracted(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(reply.present.then_some(reply.local_id))
    }

    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        let mut client = self.client.clone();
        let req = proto::DeleteRequest {
            logical_id: logical,
        };
        let reply = self
            .block_on(async move { client.delete(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(reply.removed as usize)
    }

    fn flush(&self) -> Result<(), ShardError> {
        let mut client = self.client.clone();
        self.block_on(async move { client.flush(proto::FlushRequest {}).await })
            .map_err(rpc_err)?;
        Ok(())
    }

    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        // The remote node owns its own segment durability + translog position (server-side); a
        // recovering peer learns the snapshot's position from `FetchManifest.up_to_seqno`, not
        // from this client-side call. Flush so the remote memtable seals; report `LogPos(0)` as
        // a benign sentinel (the coordinator's gRPC recovery uses the server-reported position).
        self.flush()?;
        Ok(LogPos(0))
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        // Never `Ok(vec![])`: a silent empty registry would drop this shard's data on a
        // future durable-remote reopen. Surface that durability is remote-side here.
        Err(ShardError::Remote(
            "segment registry is unavailable for a remote shard (durable checkpoint is \
             local-only in this increment)"
                .into(),
        ))
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        Err(ShardError::Remote(
            "next_seg_id is unavailable for a remote shard".into(),
        ))
    }

    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        // Drain the source's `FetchTranslog` stream (ops > `from`) and decode each entry back
        // into a logical mutation. The coordinator replays these into the recovering target —
        // the no-quiesce catch-up (ADR-039). The tail is the small un-sealed delta.
        let mut client = self.client.clone();
        let req = proto::FetchTranslogRequest {
            after_seqno: from.0,
            dict_fingerprint: self.dict_fp,
        };
        self.block_on(async move {
            let mut stream = client
                .fetch_translog(req)
                .await
                .map_err(rpc_err)?
                .into_inner();
            let mut out = Vec::new();
            while let Some(entry) = stream.message().await.map_err(rpc_err)? {
                if let Some(pm) = proto::translog_entry_to_mutation(entry) {
                    out.push(pm);
                }
            }
            Ok(out)
        })
    }

    // ---- translog retention leases (ADR-040) ----
    fn acquire_retention_lease(&self) -> Result<(u64, LogPos), ShardError> {
        let mut client = self.client.clone();
        let req = proto::RetentionLeaseRequest {
            op: 0,
            lease_id: 0,
            pos: 0,
            dict_fingerprint: self.dict_fp,
        };
        let reply = self
            .block_on(async move { client.retention_lease(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok((reply.lease_id, LogPos(reply.pos)))
    }

    fn renew_retention_lease(&self, lease: u64, to: LogPos) -> Result<(), ShardError> {
        let mut client = self.client.clone();
        let req = proto::RetentionLeaseRequest {
            op: 1,
            lease_id: lease,
            pos: to.0,
            dict_fingerprint: self.dict_fp,
        };
        self.block_on(async move { client.retention_lease(req).await })
            .map_err(rpc_err)?;
        Ok(())
    }

    fn release_retention_lease(&self, lease: u64) -> Result<(), ShardError> {
        let mut client = self.client.clone();
        let req = proto::RetentionLeaseRequest {
            op: 2,
            lease_id: lease,
            pos: 0,
            dict_fingerprint: self.dict_fp,
        };
        self.block_on(async move { client.retention_lease(req).await })
            .map_err(rpc_err)?;
        Ok(())
    }
}

/// Fail-loud guard (ADR-074): pre-resolved `tag_ids` — the tagged vocabulary rebuild's
/// carry-through — cannot cross the dict-agnostic wire. The proto ships raw `(key,value)`
/// tags only, and a synthetic `TagId` has no recoverable string to send; silently dropping
/// the ids would lose the query's tags (a filtered-read recall loss). `set_vocab` refuses a
/// non-local cluster before ever building such a bucket, so this is defense in depth at the
/// transport seam, not a reachable path.
fn refuse_wire_tag_ids(items: &[PlacedQuery]) -> Result<(), ShardError> {
    if items.iter().any(|q| !q.tag_ids.is_empty()) {
        return Err(ShardError::Config(
            "pre-resolved tag ids cannot cross the process boundary: the gRPC wire ships raw \
             (key,value) tags only (a synthetic TagId has no recoverable string) — the tagged \
             vocabulary rebuild is in-process only (ADR-074)"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placed(tags: Vec<(String, String)>, tag_ids: Vec<crate::tagdict::TagId>) -> PlacedQuery {
        let norm = crate::normalize::Normalizer::default_vocab().expect("vocab");
        let mut dict = crate::dict::Dict::new();
        let mut lc = String::new();
        let ast = crate::dsl::parse("1994 upper deck").expect("parse");
        let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
        PlacedQuery {
            logical: 1,
            ex,
            dsl: "1994 upper deck".into(),
            version: 1,
            tags,
            tag_ids,
        }
    }

    #[test]
    fn wire_guard_passes_raw_tags_and_refuses_pre_resolved_ids() {
        // Raw (key,value) tags are the supported wire shape — no refusal.
        let raw = placed(vec![("category".into(), "cards".into())], Vec::new());
        assert!(refuse_wire_tag_ids(std::slice::from_ref(&raw)).is_ok());
        // Pre-resolved ids (the ADR-074 carry-through) must be refused loudly.
        let carried = placed(
            Vec::new(),
            vec![crate::tagdict::synthetic_tag_id("region", "emea")],
        );
        let err = refuse_wire_tag_ids(&[raw, carried])
            .expect_err("ids must not cross the process boundary");
        assert!(
            format!("{err}").contains("process boundary"),
            "the refusal names the boundary: {err}"
        );
    }
}
