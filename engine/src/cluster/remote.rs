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

use super::proto;
use super::proto::shard_service_client::ShardServiceClient;
use super::shard::{Shard, ShardError};

/// One shard living behind a gRPC `ShardService`.
pub struct RemoteShard {
    client: ShardServiceClient<Channel>,
    handle: Handle,
}

impl RemoteShard {
    /// Connect to a `ShardService` at `endpoint` (e.g. `"http://127.0.0.1:50051"`),
    /// driving the async connect on `handle`.
    pub fn connect(endpoint: String, handle: Handle) -> Result<Self, ShardError> {
        let client = handle
            .block_on(ShardServiceClient::connect(endpoint))
            .map_err(|e| ShardError::Remote(format!("connect: {e}")))?;
        Ok(RemoteShard { client, handle })
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
}
