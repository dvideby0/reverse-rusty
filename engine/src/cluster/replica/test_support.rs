//! Shared unit-test fixtures for the `ReplicatedShard` tests: corpus compilation, a scratch
//! dir, the seeding helper, and a fault-injecting `FailingShard`. Split out so the test fns
//! live in a focused `tests.rs` under the ~650-line module budget.

use std::sync::atomic::AtomicU8;

use crate::cluster::clog::ClusterMutation;
use crate::compile::Extracted;
use crate::exact::TagPredicate;
use crate::segment::{IngestReport, MatchStats, PlacedQuery};
use crate::tagdict::TagDict;

use super::*;

/// (shared normalizer, frozen dict, frozen empty tag dict, per-query `(id, Extracted, dsl)`) —
/// what [`compile_corpus`] returns. The tag dict mirrors the coordinator's frozen, shared
/// `Arc<TagDict>` (ADR-055): these untagged unit tests carry an empty, finalized one.
pub(super) type CompiledCorpus = (
    Arc<Normalizer>,
    Arc<Dict>,
    Arc<TagDict>,
    Vec<(u64, Extracted, String)>,
);

/// Compile a list of `(id, DSL)` into a shared frozen dict + a frozen empty tag dict + the
/// per-query `Extracted`, mirroring `ClusterEngine::build`'s pass A (extract into the dict, then
/// finalize the hot mask + tag space). Lets a test seed a `LocalShard` at the same low level the
/// coordinator uses.
pub(super) fn compile_corpus(dsls: &[(u64, &str)]) -> CompiledCorpus {
    let norm = Arc::new(Normalizer::default_vocab().expect("built-in vocab"));
    let mut dict = Dict::new();
    let mut lc = String::new();
    let mut out = Vec::new();
    for (id, dsl) in dsls {
        let ast = crate::dsl::parse(dsl).expect("test dsl parses");
        let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
        out.push((*id, ex, (*dsl).to_string()));
    }
    dict.finalize_mask();
    let mut tag_dict = TagDict::new();
    tag_dict.mark_finalized();
    (norm, Arc::new(dict), Arc::new(tag_dict), out)
}

pub(super) fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rr_replica_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A fault-injecting `Shard` for the failover/ack tests: reads return a configured error
/// (or an empty result), writes optionally error.
pub(super) struct FailingShard {
    /// 0 = ok, 1 = `Remote`, 2 = `DictMismatch` — applied to every read.
    read_mode: AtomicU8,
    fail_writes: AtomicBool,
}

impl FailingShard {
    pub(super) fn reads_remote() -> Self {
        FailingShard {
            read_mode: AtomicU8::new(1),
            fail_writes: AtomicBool::new(false),
        }
    }
    pub(super) fn reads_dict_mismatch() -> Self {
        FailingShard {
            read_mode: AtomicU8::new(2),
            fail_writes: AtomicBool::new(false),
        }
    }
    pub(super) fn writes_fail() -> Self {
        FailingShard {
            read_mode: AtomicU8::new(0),
            fail_writes: AtomicBool::new(false),
        }
        .with_failing_writes()
    }
    fn with_failing_writes(self) -> Self {
        self.fail_writes.store(true, Ordering::Release);
        self
    }
    fn read_err(&self) -> Option<ShardError> {
        match self.read_mode.load(Ordering::Acquire) {
            1 => Some(ShardError::Remote("injected".into())),
            2 => Some(ShardError::DictMismatch {
                expected: 1,
                actual: 2,
            }),
            _ => None,
        }
    }
    fn write_err(&self) -> Result<(), ShardError> {
        if self.fail_writes.load(Ordering::Acquire) {
            Err(ShardError::Remote("injected write".into()))
        } else {
            Ok(())
        }
    }
}

impl Shard for FailingShard {
    fn percolate_filtered(
        &self,
        _t: &str,
        _b: bool,
        _pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        match self.read_err() {
            Some(e) => Err(e),
            None => Ok((Vec::new(), MatchStats::default())),
        }
    }
    fn percolate_filtered_ranked(
        &self,
        _t: &str,
        _b: bool,
        _pred: &TagPredicate,
        _spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        match self.read_err() {
            Some(e) => Err(e),
            None => Ok((Vec::new(), MatchStats::default())),
        }
    }
    fn num_queries(&self) -> Result<usize, ShardError> {
        self.read_err().map_or(Ok(0), Err)
    }
    fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        self.read_err().map_or(Ok([0; 4]), Err)
    }
    fn ingest_extracted(&self, _i: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        self.write_err().map(|()| IngestReport::default())
    }
    fn insert_extracted_with_tags(
        &self,
        _e: &Extracted,
        _l: u64,
        _v: u32,
        _t: &str,
        _tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        self.write_err().map(|()| Some(0))
    }
    fn delete_by_logical_id(&self, _l: u64) -> Result<usize, ShardError> {
        self.write_err().map(|()| 0)
    }
    fn flush(&self) -> Result<(), ShardError> {
        self.write_err()
    }
    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        Ok(LogPos(0))
    }
    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        Ok(Vec::new())
    }
    fn next_seg_id(&self) -> Result<u64, ShardError> {
        Ok(0)
    }
    fn translog_tail(&self, _from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        Ok(Vec::new())
    }
}

pub(super) fn seed(shard: &dyn Shard, corpus: &[(u64, Extracted, String)]) {
    for (id, ex, dsl) in corpus {
        shard
            .insert_extracted_with_tags(ex, *id, 1, dsl, &[])
            .expect("seed insert");
    }
}
