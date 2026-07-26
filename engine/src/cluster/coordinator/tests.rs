//! Unit tests for the coordinator that need private-state access (e.g. the durable
//! `log` field), kept in-module rather than in the integration oracles.

use super::*;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::cluster::clog::{ClusterLog, ClusterMutation, ClusterReplay, LogPos};
use crate::delivery::{ChunkSink, ChunkSinkError, MatchChunk};
use crate::events::DurabilityOp;
use crate::exact::TagPredicate;
use crate::segment::{IngestReport, MatchStats, PlacedQuery};

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rr_clog_coord_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn downgrade_cluster_manifest_to_v6(path: &std::path::Path) {
    let manifest = crate::storage::read_cluster_manifest(path).expect("read v7 manifest");
    let mut bytes = std::fs::read(path).expect("read manifest bytes");
    let suffix = 8 + manifest
        .source_files
        .iter()
        .map(|name| 4 + name.len())
        .sum::<usize>();
    let content_len = bytes.len().checked_sub(4 + suffix).expect("v7 suffix fits");
    bytes.truncate(content_len);
    bytes[4..8].copy_from_slice(&6u32.to_le_bytes());
    let crc = crate::storage::crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(path, bytes).expect("write v6 manifest");
}

#[derive(Default)]
struct FirstAppendGate {
    calls: AtomicU64,
    entered: (std::sync::Mutex<bool>, std::sync::Condvar),
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
}

impl FirstAppendGate {
    fn wait_until_entered(&self) {
        let (lock, ready) = &self.entered;
        let entered = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (entered, _) = ready
            .wait_timeout_while(entered, Duration::from_secs(5), |entered| !*entered)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(*entered, "first coordinator-log append never started");
    }

    fn release_first(&self) {
        let (lock, release) = &self.release;
        *lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        release.notify_all();
    }
}

struct FailFirstAppendLog {
    gate: Arc<FirstAppendGate>,
}

impl ClusterLog for FailFirstAppendLog {
    fn append(&self, _mutation: &ClusterMutation) -> Result<LogPos, ShardError> {
        let call = self.gate.calls.fetch_add(1, Ordering::SeqCst);
        if call != 0 {
            return Ok(LogPos(call));
        }

        let (entered_lock, entered) = &self.gate.entered;
        *entered_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        entered.notify_all();

        let (release_lock, release) = &self.gate.release;
        let released = release_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(
            release
                .wait_while(released, |released| !*released)
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        Err(ShardError::Log("injected first-append failure".into()))
    }

    fn replay(&self, _from: LogPos) -> Result<ClusterReplay, ShardError> {
        Ok(ClusterReplay {
            entries: Vec::new(),
            skipped_bytes: 0,
        })
    }

    fn last_pos(&self) -> Result<LogPos, ShardError> {
        Ok(LogPos(
            self.gate.calls.load(Ordering::SeqCst).saturating_sub(1),
        ))
    }

    fn checkpoint(&self, _up_to: LogPos) -> Result<(), ShardError> {
        Ok(())
    }
}

/// A `LocalShard` wrapper whose WRITES (`insert`/`delete`/`ingest`) can be toggled to fail —
/// simulating a transient remote shard outage — while reads and everything else delegate. Lets a
/// `from_parts` cluster drive the partial-apply detection + `resync` repair (ADR-047)
/// deterministically with NO network: the in-process build path's writes are infallible, so this
/// fault injection is the only way to reach the remote-failure machinery from the lean core. One
/// shared `Arc<AtomicBool>` toggles every shard at once.
struct ToggleFailShard {
    inner: LocalShard,
    fail_writes: Arc<AtomicBool>,
    /// `Some(p)` mimics the gRPC server seam, which validates placement
    /// coverage for its own position on every insert (`validate_for_shard`) —
    /// the check an in-process `LocalShard` cannot run (it does not know its
    /// position). Lets in-process tests reproduce remote-only refusals.
    position: Option<u32>,
}

impl ToggleFailShard {
    fn new(inner: LocalShard, fail_writes: Arc<AtomicBool>) -> Self {
        ToggleFailShard {
            inner,
            fail_writes,
            position: None,
        }
    }

    fn with_position(inner: LocalShard, fail_writes: Arc<AtomicBool>, position: u32) -> Self {
        ToggleFailShard {
            inner,
            fail_writes,
            position: Some(position),
        }
    }
    fn write_err(&self) -> Option<ShardError> {
        self.fail_writes
            .load(Ordering::Acquire)
            .then(|| ShardError::Remote("injected transient write failure".into()))
    }
}

impl Shard for ToggleFailShard {
    fn percolate_filtered(
        &self,
        t: &str,
        b: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.inner.percolate_filtered(t, b, pred)
    }
    fn percolate_filtered_owned(
        &self,
        t: &str,
        b: bool,
        pred: &TagPredicate,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.inner
            .percolate_filtered_owned(t, b, pred, context, current_position)
    }
    fn percolate_filtered_ranked(
        &self,
        t: &str,
        b: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.inner.percolate_filtered_ranked(t, b, pred, spec)
    }
    fn percolate_filtered_ranked_owned(
        &self,
        t: &str,
        b: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.inner
            .percolate_filtered_ranked_owned(t, b, pred, spec, context, current_position)
    }
    fn percolate_all_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: Option<&crate::rank::CompiledRankProgram>,
        chunk_size: usize,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<std::time::Instant>,
        sink: &mut dyn ChunkSink,
    ) -> Result<crate::delivery::ExhaustiveMatchResult, ShardError> {
        self.inner.percolate_all_owned(
            title,
            include_broad,
            pred,
            program,
            chunk_size,
            context,
            current_position,
            deadline,
            sink,
        )
    }
    fn num_queries(&self) -> Result<usize, ShardError> {
        self.inner.num_queries()
    }
    fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        self.inner.class_counts()
    }
    fn validate_ownership(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), ShardError> {
        self.inner
            .validate_ownership(position, generation, num_shards)
    }
    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        match self.write_err() {
            Some(e) => Err(e),
            None => self.inner.ingest_extracted(items),
        }
    }
    fn insert_extracted_with_tags(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        match self.write_err() {
            Some(e) => Err(e),
            None => self
                .inner
                .insert_extracted_with_tags(ex, logical, version, text, tags),
        }
    }
    fn insert_extracted_with_placement(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<Option<u32>, ShardError> {
        if let Some(p) = self.position {
            if placement.mode() == crate::ownership::PlacementMode::Selective
                && placement.positions().binary_search(&p).is_err()
            {
                return Err(crate::ownership::OwnershipError::LocalPositionMissing(p).into());
            }
        }
        match self.write_err() {
            Some(e) => Err(e),
            None => self
                .inner
                .insert_extracted_with_placement(ex, logical, version, text, tags, placement),
        }
    }
    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        match self.write_err() {
            Some(e) => Err(e),
            None => self.inner.delete_by_logical_id(logical),
        }
    }
    fn flush(&self) -> Result<(), ShardError> {
        self.inner.flush()
    }
    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        self.inner.seal_for_checkpoint()
    }
    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        self.inner.segment_filenames()
    }
    fn next_seg_id(&self) -> Result<u64, ShardError> {
        self.inner.next_seg_id()
    }
    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        self.inner.translog_tail(from)
    }
}

#[derive(Default)]
struct RecordingExhaustiveSink {
    chunks: Vec<MatchChunk>,
}

impl ChunkSink for RecordingExhaustiveSink {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        self.chunks.push(chunk.clone());
        Ok(())
    }
}

mod basic;
mod directory;
mod exhaustive;
mod repair;
mod upsert;
