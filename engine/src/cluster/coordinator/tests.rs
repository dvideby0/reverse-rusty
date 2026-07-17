//! Unit tests for the coordinator that need private-state access (e.g. the durable
//! `log` field), kept in-module rather than in the integration oracles.

use super::*;

use std::sync::atomic::{AtomicBool, Ordering};

use crate::cluster::clog::{ClusterMutation, LogPos};
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

    // A failed initial bulk load reserves its ids before the first shard write.
    // Even though this injected failure landed no row, the coordinator cannot
    // generally know whether a remote multi-shard load was partial; fail closed
    // against an incremental semantic duplicate.
    fail.store(true, Ordering::Release);
    let failed_bulk = vec![(88u64, "zznovelaterm".to_string())];
    assert!(matches!(
        cluster.ingest(&failed_bulk),
        Err(ShardError::Remote(_))
    ));
    assert!(matches!(
        cluster.add_query(88, "zznovelaterm"),
        Err(ShardError::DuplicateLogicalId(88))
    ));

    // Fail a regular add, then resync while STILL failing — the mutation must stay queued.
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

/// Cluster upsert (ADR-070): a fresh id creates (`removed == 0`), a re-upsert replaces —
/// the OLD version stops matching, the NEW one matches, and exactly one live physical
/// copy remains (no additive duplicate, the pre-ADR-067 hazard at the cluster).
#[test]
fn upsert_creates_then_replaces_by_logical_id() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![
        (1u64, "1994 topps".to_string()),
        (2u64, "1995 fleer".to_string()),
    ];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster builds");

    // Create: a fresh id reports zero prior copies removed.
    let (removed, outcome) = cluster.upsert_query(3, "1996 skybox", 1).expect("upsert");
    assert_eq!(removed, 0, "fresh id ⇒ created");
    assert!(matches!(
        outcome,
        AddOutcome::Placed { .. } | AddOutcome::Replicated
    ));
    assert!(cluster.percolate("1996 skybox").expect("p").contains(&3));

    // Replace: the new version matches, the old does not — old-stops-matching IS the
    // no-additive-duplicate proof (the pre-ADR-067 hazard was both versions live at
    // once). Entry counts grow by design (tombstone + insert), so they are not asserted.
    let (removed, _) = cluster
        .upsert_query(3, "1997 metal universe", 1)
        .expect("upsert");
    assert!(removed > 0, "prior copy tombstoned ⇒ updated");
    assert!(
        !cluster.percolate("1996 skybox").expect("p").contains(&3),
        "old version must stop matching after replace"
    );
    assert!(
        cluster
            .percolate("1997 metal universe")
            .expect("p")
            .contains(&3),
        "new version must match after replace"
    );

    // Replace back: repeated upserts keep converging (no stale copy resurfaces).
    let (removed, _) = cluster.upsert_query(3, "1996 skybox", 1).expect("upsert");
    assert!(removed > 0);
    assert!(cluster.percolate("1996 skybox").expect("p").contains(&3));
    assert!(
        !cluster
            .percolate("1997 metal universe")
            .expect("p")
            .contains(&3),
        "replaced-away version must not resurface"
    );
}

/// A rejected NEW version never deletes the prior one (ADR-067 parity at the cluster):
/// a class-D (negation-only) upsert and a parse-error upsert both leave the stored
/// version live and matchable.
#[test]
fn upsert_rejection_keeps_prior_version_live() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster builds");
    assert!(cluster.percolate("1994 topps").expect("p").contains(&1));

    // Class D: negation-only — rejected at placement, stored nowhere, deletes nothing.
    let (removed, outcome) = cluster.upsert_query(1, "-junk", 1).expect("upsert");
    assert_eq!(removed, 0, "a failed replace never deletes");
    assert!(matches!(outcome, AddOutcome::RejectedClassD));
    assert!(
        cluster.percolate("1994 topps").expect("p").contains(&1),
        "prior version stays matchable after a class-D upsert"
    );

    // Parse error: rejected before logging, deletes nothing.
    let (removed, outcome) = cluster.upsert_query(1, "(((", 1).expect("upsert");
    assert_eq!(removed, 0);
    assert!(matches!(outcome, AddOutcome::RejectedParse(_)));
    assert!(
        cluster.percolate("1994 topps").expect("p").contains(&1),
        "prior version stays matchable after a parse-error upsert"
    );
}

/// WAL-first fail-closed for upsert, mirroring `add_query_is_fail_closed_when_log_append_fails`:
/// when the durable log append fails the upsert is rejected whole — the prior version
/// remains live and matchable (the replace never half-applies).
#[test]
fn upsert_is_fail_closed_when_log_append_fails() {
    let dir = scratch_dir("upsert_failclosed");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let seed = vec![(7u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    cluster.log.break_writes_for_test();
    let res = cluster.upsert_query(7, "1995 fleer", 1);
    assert!(
        matches!(res, Err(ShardError::Log(_))),
        "expected Log error, got {res:?}"
    );
    assert!(
        cluster.percolate("1994 topps").expect("p").contains(&7),
        "prior version must remain matchable after a rejected upsert"
    );
    assert!(
        !cluster.percolate("1995 fleer").expect("p").contains(&7),
        "the rejected new version must not be matchable"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// B2: a cluster `PUT /_doc/{id} {"version":N}` must STORE version N, not the
/// hardcoded 1 — matching single-node `try_upsert_live_with_tags`. The version
/// rides the `ClusterMutation::Upsert` log frame (the durable, replayed-on-reopen
/// source of truth), so asserting the logged frame's version is the faithful
/// round-trip check. Needs private `log` access, so it lives in-module.
#[test]
fn upsert_threads_request_version_into_the_log_frame() {
    let dir = scratch_dir("upsert_version");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    // Upsert id 5 at a non-default version.
    let (_removed, outcome) = cluster
        .upsert_query(5, "1995 fleer", 42)
        .expect("versioned upsert");
    assert!(
        matches!(outcome, AddOutcome::Placed { .. } | AddOutcome::Replicated),
        "in-vocabulary upsert is accepted, got {outcome:?}"
    );

    // The logged Upsert frame must carry version 42 (NOT the old hardcoded 1).
    let replay = cluster.log.replay(LogPos(0)).expect("replay clog");
    let logged_version = replay.entries.iter().find_map(|(_, m)| match m {
        ClusterMutation::Upsert {
            logical: 5,
            version,
            ..
        } => Some(*version),
        _ => None,
    });
    assert_eq!(
        logged_version,
        Some(42),
        "cluster upsert must log the request version, not the hardcoded 1"
    );

    // And the default still logs version 1 (the byte-identical RF=1 path) for a fresh id.
    cluster
        .upsert_query(6, "1994 topps", 1)
        .expect("default-version upsert");
    let replay = cluster.log.replay(LogPos(0)).expect("replay clog");
    let default_version = replay.entries.iter().find_map(|(_, m)| match m {
        ClusterMutation::Upsert {
            logical: 6,
            version,
            ..
        } => Some(*version),
        _ => None,
    });
    assert_eq!(default_version, Some(1), "default upsert version stays 1");

    let _ = std::fs::remove_dir_all(&dir);
}

/// B2 follow-up (codex review): a blue/green rebuild (`set_vocab` / resize) must PRESERVE
/// each query's stored version rather than reset it to 1. Before the fix the rebuild gather
/// dropped the version and `rebuild_from_live` recreated every `PlacedQuery` with
/// `version: 1`, so a `PUT {"version":42}` was silently rewritten to 1 (and the checkpoint
/// truncated the original log frame — durable divergence from single-node). Asserts the
/// gather carries the stored version across the rebuild.
#[test]
fn rebuild_preserves_stored_query_version() {
    let dir = scratch_dir("rebuild_version");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let seed = vec![(1u64, "1994 topps".to_string())];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    // Upsert id 5 at a non-default version, then confirm the gather sees version 42.
    cluster
        .upsert_query(5, "1995 fleer", 42)
        .expect("versioned upsert");
    let before = cluster.live_corpus_tagged().expect("gather");
    let pre = before
        .iter()
        .find(|(l, ..)| *l == 5)
        .map(|(_, _, v, _, _, _)| *v);
    assert_eq!(
        pre,
        Some(42),
        "gather must see the stored version before rebuild"
    );

    // A vocabulary change forces a blue/green rebuild of every shard.
    let mut new_vocab = crate::vocab::Vocab::new();
    new_vocab.add_synonym("rc", "term:rookie", crate::dict::FeatureKind::Category);
    cluster.set_vocab(new_vocab).expect("set_vocab rebuild");

    // After the rebuild id 5 must STILL carry version 42 (not reset to 1) and still match.
    let after = cluster.live_corpus_tagged().expect("gather after rebuild");
    let post = after
        .iter()
        .find(|(l, ..)| *l == 5)
        .map(|(_, _, v, _, _, _)| *v);
    assert_eq!(
        post,
        Some(42),
        "rebuild must preserve the stored version, not reset it to 1"
    );
    assert!(
        cluster
            .percolate("1995 fleer")
            .expect("percolate")
            .contains(&5),
        "the re-placed query must still match after the rebuild"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Tier-D: a degenerate same-node handoff (`from == to`) is a silent no-op — it must NOT
/// fence the source then flip routing onto itself. The `from == to` guard sits before the
/// handle resolve, so the self-handoff returns immediately, emitting no event and never
/// touching a shard. Asserted via the observer (no event) + percolate-unchanged. Gated:
/// `drive_autoscaled_handoff` only exists under `distributed`.
#[cfg(feature = "distributed")]
#[test]
fn self_handoff_is_skipped_without_fencing() {
    use crate::cluster::autoscale::LoadSnapshot;
    use crate::cluster::control::{NodeDescriptor, NodeId, NodeRole, ShardAssignment};

    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![
        (1u64, "1994 topps".to_string()),
        (2u64, "1995 fleer".to_string()),
    ];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster builds");

    // Record any emitted event — a real handoff (or its abort) emits a DurabilityFailure.
    let events: Arc<Mutex<Vec<EngineEvent>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = Arc::clone(&events);
        cluster.set_observer(Arc::new(move |ev: &EngineEvent| {
            sink.lock().unwrap().push(ev.clone());
        }));
    }

    let before = cluster.percolate("1994 topps").expect("percolate");

    // A snapshot where node 7 owns position 0 — and a Handoff that moves it from node 7 to
    // node 7 (the same node, same endpoint). The guard must short-circuit this.
    let node = NodeDescriptor {
        id: NodeId(7),
        addr: Some("http://127.0.0.1:65530".to_string()),
        role: NodeRole::Data,
    };
    let snapshot = LoadSnapshot {
        nodes: vec![node],
        assignments: vec![ShardAssignment {
            position: 0,
            primary: NodeId(7),
            replicas: Vec::new(),
        }],
        shard_corpus: vec![1, 1, 0],
        replicated_corpus: 0,
        num_shards: 3,
        replication_factor: 1,
    };

    cluster.drive_autoscaled_handoff(&snapshot, 0, NodeId(7), NodeId(7));

    assert!(
        events.lock().unwrap().is_empty(),
        "a self-handoff must emit no event (no fence, no abort): {:?}",
        events.lock().unwrap()
    );
    assert_eq!(
        cluster.percolate("1994 topps").expect("percolate"),
        before,
        "matching must be byte-identical across a skipped self-handoff"
    );
}

/// A blue/green rebuild (resize / set_vocab) re-ingests ALREADY-STORED queries through
/// `ingest_extracted`, carrying their tags as pre-resolved `TagId`s. Tightening `max_tags`
/// after those queries were accepted must NOT drop them on the rebuild — the rebuild swaps
/// in the new shards and ignores the ingest report, so a skipped query is permanently lost
/// (a false negative on acknowledged data). The `max_tags` cap applies only to FRESH raw-tag
/// ingestion, never to stored carry-through (codex review).
#[test]
fn rebuild_preserves_stored_tags_under_tightened_max_tags() {
    let mut per_shard = EngineConfig {
        max_tags: 5,
        ..EngineConfig::default()
    };
    per_shard.data_dir = None;
    let cfg = ClusterConfig {
        num_shards: 3,
        per_shard,
        ..Default::default()
    };
    // Seed so the dict knows the tokens, then add a query carrying 4 tags (≤ 5).
    let seed = vec![(1u64, "1994 topps baseball".to_string())];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster builds");
    let four_tags: Vec<(String, String)> = (0..4).map(|i| ("k".into(), format!("v{i}"))).collect();
    cluster
        .add_query_with_tags(2, "1995 fleer baseball", &four_tags)
        .expect("tagged add");
    // The tagged query is matchable and filterable by one of its tags before the rebuild.
    let filter = vec![("k".to_string(), vec!["v3".to_string()])];
    assert!(cluster
        .percolate_filtered("1995 fleer baseball", &filter)
        .expect("filtered")
        .contains(&2));

    // Tighten the per-shard tag ceiling BELOW the stored query's 4 tags, then rebuild
    // (a resize triggers `rebuild_from_live` → `ingest_extracted` with the carry-through).
    cluster.per_shard.max_tags = 2;
    let rebuilt = cluster.resize(5).expect("resize rebuilds");
    assert_eq!(rebuilt, 2, "both stored queries are re-ingested");

    // The 4-tag query SURVIVES the rebuild: still matchable AND still filterable by its tag.
    assert!(
        cluster
            .percolate("1995 fleer baseball")
            .expect("p")
            .contains(&2),
        "stored over-limit-tagged query must survive the rebuild (no silent drop)"
    );
    assert!(
        cluster
            .percolate_filtered("1995 fleer baseball", &filter)
            .expect("filtered")
            .contains(&2),
        "the stored tags must survive carry-through — filter still matches"
    );

    // A FRESH add still respects the now-tightened cap: 3 raw tags > max_tags(2) is rejected.
    let three_tags: Vec<(String, String)> = (0..3).map(|i| ("k".into(), format!("w{i}"))).collect();
    let outcome = cluster
        .add_query_with_tags(9, "1996 skybox baseball", &three_tags)
        .expect("add returns");
    assert!(
        matches!(outcome, AddOutcome::RejectedParse(ref e) if e.kind == crate::error::ParseErrorKind::TooManyTags),
        "a fresh over-limit raw-tag add must still be rejected, got {outcome:?}"
    );
}

/// Distributed local-K/global-K assumes one semantic row per logical id. Query
/// placement is content-derived, so two different rows under one id can have no
/// common routed owner. Reject the invalid state at every cluster load boundary;
/// callers use the existing atomic upsert API to replace an id.
#[test]
fn cluster_load_boundaries_reject_duplicate_logical_ids() {
    let cfg = ClusterConfig {
        num_shards: 5,
        ..Default::default()
    };
    let duplicates = vec![
        (42u64, "1994 topps".to_string()),
        (42u64, "1995 fleer".to_string()),
    ];
    assert!(matches!(
        ClusterEngine::build(vocab(), &cfg, &duplicates),
        Err(ShardError::DuplicateLogicalId(42))
    ));

    let empty = ClusterEngine::build(vocab(), &cfg, &[]).expect("empty cluster");
    assert!(matches!(
        empty.ingest(&duplicates),
        Err(ShardError::DuplicateLogicalId(42))
    ));
    assert_eq!(empty.num_queries().expect("count"), 0);
}

#[test]
fn incremental_add_is_insert_only_and_remove_allows_reuse() {
    let cfg = ClusterConfig {
        num_shards: 8,
        ..Default::default()
    };
    let seed = vec![(42u64, "1994 topps rareplayer0".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster");

    assert!(matches!(
        cluster.add_query(42, "1995 fleer rareplayer1000"),
        Err(ShardError::DuplicateLogicalId(42))
    ));
    assert_eq!(
        cluster.percolate("1994 topps rareplayer0").expect("old"),
        vec![42]
    );
    assert!(cluster
        .percolate("1995 fleer rareplayer1000")
        .expect("rejected new")
        .is_empty());

    cluster.remove_query(42).expect("remove");
    cluster
        .add_query(42, "1995 fleer rareplayer1000")
        .expect("id can be reused after delete");
    assert!(cluster
        .percolate("1994 topps rareplayer0")
        .expect("old removed")
        .is_empty());
    assert_eq!(
        cluster.percolate("1995 fleer rareplayer1000").expect("new"),
        vec![42]
    );
}

#[test]
fn concurrent_same_id_adds_admit_exactly_one_row() {
    use std::sync::{Arc, Barrier};

    let cfg = ClusterConfig {
        num_shards: 8,
        ..Default::default()
    };
    let seed = vec![
        (1u64, "1994 topps rareplayer0".to_string()),
        (2u64, "1995 fleer rareplayer1000".to_string()),
    ];
    let cluster = Arc::new(ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster"));
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for dsl in ["1994 topps rareplayer0", "1995 fleer rareplayer1000"] {
        let cluster = Arc::clone(&cluster);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            cluster.add_query(99, dsl)
        }));
    }
    barrier.wait();
    let outcomes: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().expect("worker"))
        .collect();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(ShardError::DuplicateLogicalId(99))))
            .count(),
        1
    );

    let matches = [
        cluster
            .percolate("1994 topps rareplayer0")
            .expect("first")
            .contains(&99),
        cluster
            .percolate("1995 fleer rareplayer1000")
            .expect("second")
            .contains(&99),
    ];
    assert_eq!(matches.into_iter().filter(|matched| *matched).count(), 1);
}

#[test]
fn reopened_cluster_rebuilds_unique_id_directory() {
    let dir = scratch_dir("logical_ids");
    let cfg = ClusterConfig {
        num_shards: 4,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let seed = vec![(77u64, "1994 topps rareplayer0".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");
    cluster.checkpoint().expect("checkpoint");
    drop(cluster);

    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("open");
    assert!(matches!(
        reopened.add_query(77, "1995 fleer rareplayer1000"),
        Err(ShardError::DuplicateLogicalId(77))
    ));
    reopened.remove_query(77).expect("remove");
    reopened
        .add_query(77, "1995 fleer rareplayer1000")
        .expect("reuse after remove");
    assert_eq!(
        reopened
            .percolate("1995 fleer rareplayer1000")
            .expect("match"),
        vec![77]
    );
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

/// The three `_owned` read paths share one routed-membership guard
/// (`OwnershipContext::require_routed`): a request naming a position outside the
/// routed set (or the shard space) must fail loud with `LocalPositionMissing`,
/// never silently emit zero rows. The ranked path originally omitted the guard —
/// an unrouted position can never equal `owner()`, so it returned an empty scored
/// set where its siblings error (review finding).
#[test]
fn owned_read_paths_fail_loud_on_unrouted_position() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let shard = LocalShard::new(
        Arc::clone(&real.norm),
        Arc::clone(&real.dict),
        Arc::clone(&real.tag_dict),
        cfg.per_shard.clone(),
    );
    let context = crate::ownership::OwnershipContext::new(
        crate::ownership::PlacementGeneration::INITIAL,
        4,
        vec![0, 2],
        None,
    )
    .expect("context");
    let pred = TagPredicate::empty();
    let spec = crate::rank::CompiledRankSpec::default();
    let program = crate::rank::CompiledRankProgram::default();
    // Position 1 is in-range but unrouted; position 7 is out of the shard space.
    for position in [1u32, 7] {
        let unrouted = |r: Result<(), ShardError>| {
            assert!(
                matches!(
                    r,
                    Err(ShardError::OwnershipMismatch(
                        crate::ownership::OwnershipError::LocalPositionMissing(p)
                    )) if p == position
                ),
                "position {position} must fail loud"
            );
        };
        unrouted(
            shard
                .percolate_filtered_owned("1994 topps baseball", true, &pred, &context, position)
                .map(|_| ()),
        );
        unrouted(
            shard
                .percolate_filtered_ranked_owned(
                    "1994 topps baseball",
                    true,
                    &pred,
                    &spec,
                    &context,
                    position,
                )
                .map(|_| ()),
        );
        unrouted(
            shard
                .percolate_top_k_owned(
                    "1994 topps baseball",
                    true,
                    &pred,
                    &program,
                    crate::result::TopKOptions::default(),
                    &context,
                    position,
                    None,
                )
                .map(|_| ()),
        );
    }
}

/// A coordinator attached to an already-populated cluster it could not enumerate
/// (the gRPC connect shape — `RemoteShard` has no live-id enumeration RPC) must
/// not run insert-only admission against an empty directory: `add_query` fails
/// closed with a `Config` error directing to `upsert_query`, which stays fully
/// usable because it re-drives replace-by-id on every shard (review finding).
#[test]
fn add_query_fails_closed_when_directory_is_unseeded() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");
    assert!(cluster.logical_ids_authoritative());

    // Simulate the connect-to-populated-cluster shape: corpus present, directory
    // unseeded.
    cluster.unseed_logical_ids_for_test();
    assert!(!cluster.logical_ids_authoritative());
    let err = cluster.add_query(2, "1994 topps").unwrap_err();
    assert!(
        matches!(err, ShardError::Config(ref m) if m.contains("upsert_query")),
        "expected the fail-closed Config error, got {err:?}"
    );

    // The replacement path is directory-independent and stays available.
    cluster
        .upsert_query(2, "1994 topps", 1)
        .expect("upsert works unseeded");
    assert!(cluster
        .percolate("1994 topps")
        .expect("percolate")
        .contains(&2));
}

/// A partially-applied remove retains its logical-id reservation (fail-closed),
/// and `resync` — the documented repair path — must RELEASE it once the delete
/// converges everywhere. Without the release the id answered 409
/// `DuplicateLogicalId` to every future `add_query` until a coordinator reopen
/// (review finding).
#[test]
fn resync_releases_reservation_after_repairing_a_remove() {
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

    let dsl = "zznovelaterm";
    cluster.add_query(5, dsl).expect("healthy add");

    // Fail the delete: the remove is durably logged, partially applied, and the
    // id stays reserved (fail-closed) — a re-add must refuse.
    fail.store(true, Ordering::Release);
    assert!(
        matches!(
            cluster.remove_query(5),
            Err(ShardError::PartiallyApplied { .. })
        ),
        "the remove must partially apply while the shard is failing"
    );
    assert!(
        matches!(
            cluster.add_query(5, dsl),
            Err(ShardError::DuplicateLogicalId(5))
        ),
        "a partially-removed id must stay reserved"
    );

    // The shard recovers; resync converges the delete and must free the id.
    fail.store(false, Ordering::Release);
    let report = cluster.resync();
    assert_eq!(report.repaired, 1, "the queued remove must converge");
    assert_eq!(cluster.pending_repairs(), 0);
    cluster
        .add_query(5, dsl)
        .expect("a repaired remove must release the id for re-add");
    assert!(
        cluster.percolate(dsl).expect("percolate").contains(&5),
        "the re-added query must be matchable"
    );
}

/// `rebuild_from_live` (resize / set_vocab) must rebuild the logical-id directory
/// from the corpus it actually re-placed, mirroring reopen's live-enumeration
/// seeding. Before the fix the directory survived the rebuild untouched, so a
/// reservation with no surviving row (the leak family: a query dropped by
/// re-placement, or a stale reservation) 409'd every re-add on the LIVE
/// coordinator while a REOPENED one accepted it (review finding).
#[test]
fn rebuild_from_live_reseeds_the_logical_id_directory() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![
        (1u64, "1994 topps baseball".to_string()),
        (2u64, "1995 fleer baseball".to_string()),
    ];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");

    // Plant a stale reservation with no live row (the leak shape).
    assert!(cluster.insert_logical_id(99));
    assert!(matches!(
        cluster.add_query(99, "1994 topps baseball"),
        Err(ShardError::DuplicateLogicalId(99))
    ));

    // The rebuild reseeds the directory from the re-placed corpus: live ids stay
    // reserved, the stale one is healed away.
    let rebuilt = cluster.resize(5).expect("resize rebuilds");
    assert_eq!(rebuilt, 2);
    assert!(matches!(
        cluster.add_query(1, "1994 topps baseball"),
        Err(ShardError::DuplicateLogicalId(1))
    ));
    cluster
        .add_query(99, "1994 topps baseball")
        .expect("stale reservation must be healed by the rebuild");
    assert!(cluster
        .percolate("1994 topps baseball")
        .expect("p")
        .contains(&99));
}

/// The multi-machine-harness wedge: an upsert's DELETE half fans to every
/// shard, so a kill window can queue a repair at a position the placement does
/// not store. Re-driving the full upsert there is refused by ADR-109's
/// shard-side placement validation (`LocalPositionMissing`), so `resync` could
/// never converge that mutation. The repair now re-drives only the delete half
/// on uncovered positions.
#[test]
fn resync_converges_an_upsert_queued_at_a_delete_only_shard() {
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
        .map(|position| {
            let ls = LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            );
            // Position-aware: reproduce the gRPC server's coverage validation,
            // which is what wedged the harness (LocalShard alone cannot).
            Box::new(ToggleFailShard::with_position(
                ls,
                Arc::clone(&fail),
                position as u32,
            )) as Box<dyn Shard>
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

    // A selective (single-shard) query: its upsert's delete half still fans to
    // ALL THREE shards, so two positions are delete-only.
    let dsl = "zznovelaterm";
    cluster.add_query(5, dsl).expect("healthy add");

    // Fail everything mid-upsert: delete-only positions land in the repair
    // queue holding the FULL upsert mutation.
    fail.store(true, Ordering::Release);
    assert!(
        cluster.upsert_query(5, dsl, 2).is_err(),
        "the upsert must partially apply while shards are failing"
    );
    assert!(cluster.pending_repairs() > 0, "repairs must be queued");

    // Shards recover: resync must converge EVERY queued mutation, including the
    // delete-only positions (previously wedged on LocalPositionMissing).
    fail.store(false, Ordering::Release);
    let report = cluster.resync();
    assert_eq!(
        report.still_pending, 0,
        "no repair may stay wedged: {report:?}"
    );
    assert_eq!(cluster.pending_repairs(), 0);
    assert!(
        cluster.percolate(dsl).expect("percolate").contains(&5),
        "the upserted query must be matchable after convergence"
    );
}
