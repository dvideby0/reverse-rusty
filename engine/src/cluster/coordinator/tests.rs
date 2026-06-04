//! Unit tests for the coordinator that need private-state access (e.g. the durable
//! `log` field), kept in-module rather than in the integration oracles.

use super::*;

use std::sync::atomic::{AtomicBool, Ordering};

use crate::cluster::clog::LogPos;
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

/// WAL-first fail-closed: when the durable log append fails, the add is rejected with
/// `ShardError::Log` AND no shard is mutated (the query never becomes matchable). Needs
/// private `log` access, so it lives here rather than in the integration oracle.
#[test]
fn add_query_is_fail_closed_when_log_append_fails() {
    let dir = scratch_dir("failclosed");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    // Build over a seed corpus so the frozen dict knows these tokens.
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");
    let before = cluster.num_queries().expect("count");

    // Break the durable log, then attempt an add of an in-vocabulary query.
    cluster.log.break_writes_for_test();
    let res = cluster.add_query(2, "1995 fleer");
    assert!(
        matches!(res, Err(ShardError::Log(_))),
        "expected Log error, got {res:?}"
    );

    // No shard was mutated: count unchanged and id 2 is not matchable.
    assert_eq!(cluster.num_queries().expect("count"), before);
    let hits = cluster.percolate("1995 fleer").expect("percolate");
    assert!(
        !hits.contains(&2),
        "rejected add must not be matchable: {hits:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// On-disk fingerprint guard: a manifest whose stored `dict_fingerprint` disagrees with
/// the dict it carries must fail `open` loud with `ShardError::DictMismatch` (ADR-030
/// parity for persisted state), never silently opening a divergent feature space. The
/// manifest is rewritten through `write_cluster_manifest` so its trailing CRC stays valid,
/// which exercises the fingerprint check itself — not the CRC check the integration
/// oracle's `corrupt_manifest_*` test already covers.
#[test]
fn open_rejects_manifest_with_divergent_dict_fingerprint() {
    let dir = scratch_dir("fpmismatch");
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    // Flip only the stored fingerprint, then rewrite with a fresh (valid) CRC. The dict
    // bytes are untouched, so on open the dict's recomputed fingerprint won't match.
    let mpath = dir.join(CLUSTER_MANIFEST_FILE);
    let mut manifest = crate::storage::read_cluster_manifest(&mpath).expect("read manifest");
    manifest.dict_fingerprint ^= 0xDEAD_BEEF_DEAD_BEEF;
    crate::storage::write_cluster_manifest(&manifest, &mpath).expect("rewrite manifest");

    // ClusterEngine isn't Debug, so match explicitly rather than `{:?}`-printing the Ok arm.
    match ClusterEngine::open(dir.clone(), vocab(), None) {
        Err(ShardError::DictMismatch { .. }) => {}
        Err(other) => panic!("expected DictMismatch, got {other:?}"),
        Ok(_) => panic!("expected DictMismatch, but open() succeeded"),
    }

    let _ = std::fs::remove_dir_all(&dir);
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
}

impl ToggleFailShard {
    fn new(inner: LocalShard, fail_writes: Arc<AtomicBool>) -> Self {
        ToggleFailShard { inner, fail_writes }
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
    fn num_queries(&self) -> Result<usize, ShardError> {
        self.inner.num_queries()
    }
    fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        self.inner.class_counts()
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

/// Partial-apply detection + `resync` repair (ADR-047): a selective add whose target shard's
/// write fails returns `PartiallyApplied` (not a swallowed error), emits a `ClusterPartialApply`
/// event, and queues the shard for repair — leaving a transient false-negative window. Once the
/// shard recovers, `resync` re-drives ONLY the failed shard and the query becomes matchable again
/// (zero false negatives restored). Deterministic via a `from_parts` cluster over fault-injecting
/// shards; the gRPC oracle proves the same DETECTION over a real wire.
#[test]
fn partial_apply_is_detected_then_resync_converges() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    // A throwaway build gives a frozen norm + dict that already know the query's tokens.
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);

    // A from_parts cluster over fault-injectable shards sharing that frozen feature space.
    let fail = Arc::new(AtomicBool::new(false));
    let shards: Vec<Box<dyn Shard>> = (0..cfg.num_shards)
        .map(|_| {
            let ls = LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            );
            Box::new(ToggleFailShard::new(ls, Arc::clone(&fail))) as Box<dyn Shard>
        })
        .collect();
    let ring = HashRing::new(cfg.num_shards, cfg.vnodes).expect("ring");
    let durable = ClusterDurable::in_memory(cfg.num_shards as u32, cfg.vnodes, dict.fingerprint());
    let cluster = ClusterEngine::from_parts(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        ring,
        shards,
        cfg.include_broad,
        1,
        cfg.per_shard.clone(),
        durable,
    )
    .expect("from_parts cluster");

    // Capture emitted events so we can assert the partial-apply event fires.
    let events: Arc<Mutex<Vec<EngineEvent>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = Arc::clone(&events);
        cluster.set_observer(Arc::new(move |ev: &EngineEvent| {
            sink.lock().unwrap().push(ev.clone());
        }));
    }

    // `"zznovelaterm"` is a single out-of-dict required term ⇒ a synthetic (freq-0, never-hot)
    // feature ⇒ class A ⇒ selective placement (one shard). Confirm on a HEALTHY add + that it is
    // matchable, establishing the baseline. (An in-dict term in this tiny corpus would be hot ⇒
    // the replicated lane, never selective — so a synthetic anchor is what forces class A here.)
    let dsl = "zznovelaterm";
    let placed = cluster.add_query(1, dsl).expect("healthy add");
    assert!(
        matches!(placed, AddOutcome::Placed { ref shards } if shards.len() == 1),
        "expected single-shard selective placement, got {placed:?}"
    );
    assert!(
        cluster
            .percolate("zznovelaterm")
            .expect("percolate")
            .contains(&1),
        "healthy selective add must be matchable"
    );

    // Now fail every shard's writes and add a second query with the SAME (selective) placement.
    fail.store(true, Ordering::Release);
    match cluster.add_query(2, dsl) {
        Err(ShardError::PartiallyApplied {
            logical,
            applied,
            failed,
            ..
        }) => {
            assert_eq!(logical, 2);
            assert!(
                applied.is_empty(),
                "the only target shard failed, got applied={applied:?}"
            );
            assert_eq!(
                failed.len(),
                1,
                "exactly the one selective target failed: {failed:?}"
            );
        }
        other => panic!("expected PartiallyApplied, got {other:?}"),
    }
    assert_eq!(
        cluster.pending_repairs(),
        1,
        "the failed mutation must be queued for repair"
    );
    assert!(
        events.lock().unwrap().iter().any(|e| matches!(
            e,
            EngineEvent::DurabilityFailure {
                op: DurabilityOp::ClusterPartialApply,
                ..
            }
        )),
        "a ClusterPartialApply durability event must be emitted"
    );
    // Divergence: query 2 is not yet matchable (the transient false-negative window).
    assert!(
        !cluster
            .percolate("zznovelaterm")
            .expect("percolate")
            .contains(&2),
        "a partially-applied add must not be matchable until repaired"
    );

    // The shard recovers; resync re-drives only the failed shard and converges.
    fail.store(false, Ordering::Release);
    let report = cluster.resync();
    assert_eq!(report.repaired, 1, "the queued mutation must converge");
    assert_eq!(report.still_pending, 0);
    assert_eq!(cluster.pending_repairs(), 0, "the queue must drain");

    // Zero false negatives restored: both queries are matchable again.
    let hits = cluster.percolate("zznovelaterm").expect("percolate");
    assert!(
        hits.contains(&1) && hits.contains(&2),
        "both queries must match after resync: {hits:?}"
    );
}

/// `resync` keeps a mutation queued when its shard is STILL failing (ADR-047): the repair pass
/// is idempotent and only converges what it can, never silently dropping an unrepaired mutation.
#[test]
fn resync_requeues_when_shard_still_failing() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);

    let fail = Arc::new(AtomicBool::new(false));
    let shards: Vec<Box<dyn Shard>> = (0..cfg.num_shards)
        .map(|_| {
            let ls = LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            );
            Box::new(ToggleFailShard::new(ls, Arc::clone(&fail))) as Box<dyn Shard>
        })
        .collect();
    let ring = HashRing::new(cfg.num_shards, cfg.vnodes).expect("ring");
    let durable = ClusterDurable::in_memory(cfg.num_shards as u32, cfg.vnodes, dict.fingerprint());
    let cluster = ClusterEngine::from_parts(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        ring,
        shards,
        cfg.include_broad,
        1,
        cfg.per_shard.clone(),
        durable,
    )
    .expect("from_parts cluster");

    // Fail the add, then resync while STILL failing — the mutation must stay queued.
    fail.store(true, Ordering::Release);
    assert!(matches!(
        cluster.add_query(7, "zznovelaterm"),
        Err(ShardError::PartiallyApplied { .. })
    ));
    let report = cluster.resync();
    assert_eq!(
        report.repaired, 0,
        "nothing converges while the shard fails"
    );
    assert_eq!(report.still_pending, 1, "the mutation must remain queued");
    assert_eq!(
        cluster.pending_repairs(),
        1,
        "still queued after a failed resync"
    );

    // Recover and resync again — now it converges and the queue drains.
    fail.store(false, Ordering::Release);
    assert_eq!(cluster.resync().repaired, 1);
    assert_eq!(cluster.pending_repairs(), 0);
    assert!(cluster
        .percolate("zznovelaterm")
        .expect("percolate")
        .contains(&7));
}
