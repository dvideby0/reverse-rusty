//! `RemoteShard` — a [`Shard`] backed by a gRPC `ShardService` client.
//!
//! Implements the SYNC [`Shard`] trait by blocking on its async tonic client via a
//! [`tokio::runtime::Handle`], confining all async to this type so the coordinator,
//! `LocalShard`, and the oracle stay synchronous. A failed RPC surfaces as
//! [`ShardError::Remote`] — never a swallowed empty result, which would shrink a
//! percolate's union into a false negative.
//!
//! `block_on` is safe here because rayon worker threads (where `percolate_inner` fans
//! out) are NOT tokio runtime threads, so parking one on `block_on` cannot panic with
//! a nested-runtime error; the RPC's I/O is driven by the separate tokio pool the
//! `Handle` belongs to. The cost — a parked worker per in-flight RPC — is the latency
//! of distribution itself; an async fan-out is the documented later optimization
//! (ADR-029).

use tokio::runtime::Handle;
use tonic::transport::Channel;

use crate::compile::Extracted;
use crate::segment::{IngestReport, MatchStats};

use super::clog::{ClusterMutation, LogPos};
use super::proto;
use super::proto::shard_service_client::ShardServiceClient;
use super::shard::{Shard, ShardError};

/// One shard living behind a gRPC `ShardService`.
pub struct RemoteShard {
    client: ShardServiceClient<Channel>,
    handle: Handle,
    /// The coordinator's frozen-dict fingerprint (verified equal to the server's at connect).
    /// Carried so dict-guarded RPCs (e.g. `FetchTranslog`) can present it.
    dict_fp: u64,
}

impl RemoteShard {
    /// Connect to a `ShardService` at `endpoint` (e.g. `"http://127.0.0.1:50051"`),
    /// driving the async connect on `handle`, then verify the server's frozen-dict
    /// fingerprint equals `expected_fp` (the coordinator's
    /// [`crate::dict::Dict::fingerprint`]). A mismatch returns [`ShardError::DictMismatch`]
    /// — a divergent dict would otherwise drop matches silently across the wire (ADR-029).
    pub fn connect(endpoint: String, handle: Handle, expected_fp: u64) -> Result<Self, ShardError> {
        let client = handle
            .block_on(ShardServiceClient::connect(endpoint))
            .map_err(|e| ShardError::Remote(format!("connect: {e}")))?;
        // Handshake before trusting the shard: clone the client for the probe RPC (a cheap
        // Channel bump, mirroring the per-call pattern below).
        let mut probe = client.clone();
        let actual_fp = handle
            .block_on(async move { probe.dict_fingerprint(proto::Empty {}).await })
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
        endpoint: String,
        handle: Handle,
        dict_bytes: Vec<u8>,
        expected_fp: u64,
    ) -> Result<Self, ShardError> {
        let client = handle
            .block_on(ShardServiceClient::connect(endpoint))
            .map_err(|e| ShardError::Remote(format!("connect: {e}")))?;
        let mut shipper = client.clone();
        let req = proto::AdoptDictRequest {
            dict: dict_bytes,
            fingerprint: expected_fp,
        };
        let adopted = match handle.block_on(async move { shipper.adopt_dict(req).await }) {
            Ok(reply) => reply.into_inner().fingerprint,
            // The server holds data under a different dict and refused ours. Read its actual
            // fingerprint so the mismatch is truthful, then fail loud (never a silent drop).
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                let mut probe = client.clone();
                let actual = handle
                    .block_on(async move { probe.dict_fingerprint(proto::Empty {}).await })
                    .map_or(0, |r| r.into_inner().fingerprint);
                return Err(ShardError::DictMismatch {
                    expected: expected_fp,
                    actual,
                });
            }
            Err(status) => return Err(ShardError::Remote(format!("adopt_dict: {status}"))),
        };
        // On success the server echoes the fingerprint it now serves — this equality IS the
        // dict-identity handshake, so no separate `dict_fingerprint` round-trip is needed.
        if adopted != expected_fp {
            return Err(ShardError::DictMismatch {
                expected: expected_fp,
                actual: adopted,
            });
        }
        Ok(RemoteShard {
            client,
            handle,
            dict_fp: expected_fp,
        })
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
            .handle
            .block_on(async move { client.recover_from(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok((
            reply.segments_attached,
            reply.num_queries,
            reply.up_to_seqno,
        ))
    }
}

fn rpc_err<E: std::fmt::Display>(e: E) -> ShardError {
    ShardError::Remote(e.to_string())
}

impl Shard for RemoteShard {
    fn percolate(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let mut client = self.client.clone();
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
        };
        let reply = self
            .handle
            .block_on(async move { client.percolate(req).await })
            .map_err(rpc_err)?
            .into_inner();
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids, stats))
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        let mut client = self.client.clone();
        let reply = self
            .handle
            .block_on(async move { client.num_queries(proto::Empty {}).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(reply.count as usize)
    }

    fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        let mut client = self.client.clone();
        let reply = self
            .handle
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

    fn ingest_extracted(
        &self,
        items: &[(u64, Extracted, String, u32)],
    ) -> Result<IngestReport, ShardError> {
        let mut client = self.client.clone();
        // Send raw DSL (the `String` in each tuple), NOT the pre-extracted feature ids:
        // the server re-compiles read-only against its own frozen dict (dict-agnostic
        // wire). The coordinator's `Extracted` was only needed for placement.
        let req = proto::IngestRequest {
            items: items
                .iter()
                .map(|(logical, _ex, dsl, version)| proto::AddItem {
                    logical_id: *logical,
                    dsl: dsl.clone(),
                    version: *version,
                })
                .collect(),
        };
        let reply = self
            .handle
            .block_on(async move { client.ingest_extracted(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(IngestReport {
            ingested: reply.ingested as usize,
            rejected_parse: reply.rejected_parse as usize,
            rejected_class_d: reply.rejected_class_d as usize,
        })
    }

    fn insert_extracted(
        &self,
        _ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
    ) -> Result<Option<u32>, ShardError> {
        let mut client = self.client.clone();
        let req = proto::InsertRequest {
            item: Some(proto::AddItem {
                logical_id: logical,
                dsl: text.to_string(),
                version,
            }),
        };
        let reply = self
            .handle
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
            .handle
            .block_on(async move { client.delete(req).await })
            .map_err(rpc_err)?
            .into_inner();
        Ok(reply.removed as usize)
    }

    fn flush(&self) -> Result<(), ShardError> {
        let mut client = self.client.clone();
        self.handle
            .block_on(async move { client.flush(proto::FlushRequest {}).await })
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
        self.handle.block_on(async move {
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
}
