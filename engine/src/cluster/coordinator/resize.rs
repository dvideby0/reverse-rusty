//! `impl ClusterEngine` — runtime shard-count change (ADR-078, ADR-065 criterion 7).
//!
//! A resize swaps the consistent-hash [`HashRing`] for a fresh one over `K′` shards and
//! rebuilds the cluster from its live source set — every query re-extracted, **re-placed**
//! under the new ring (a different `num_shards` moves ~1/N of anchors to a different shard),
//! and re-ingested. This is the SAME blue/green rebuild [`ClusterEngine::set_vocab`] performs
//! (ADR-046), with the RING swapped instead of the NORMALIZER: the shared
//! [`rebuild_from_live`] core does the work, and re-placing every query under a fresh ring
//! makes correctness trivial — placement and routing both read the new ring, exactly the
//! invariant a fresh [`ClusterEngine::build`] relies on (the module-level cover proof in
//! [`crate::cluster::coordinator`]). The hard "online ring re-keying under live handoff"
//! problem is sidestepped entirely.
//!
//! **In-process only (v1).** Like [`set_vocab`](ClusterEngine::set_vocab), a resize refuses a
//! non-local / handoff-wrapped cluster: a remote shard would not be rebuilt under the new
//! ring, so its placement and the coordinator's routing would disagree — a silent
//! cross-process false negative. A cross-process resize (shipping the re-keyed data to remote
//! shards over the live-handoff machinery) is a documented follow-on.
//!
//! **Durable for free.** The only durable change is `num_shards` growing/shrinking + a
//! correspondingly longer/shorter per-shard segment registry — both already expressible in
//! the existing `ClusterManifest` (no format bump). [`checkpoint`](ClusterEngine::checkpoint)
//! writes `num_shards = self.ring.num_shards()` and [`open`](ClusterEngine::open) re-derives
//! `HashRing::new(num_shards, vnodes)`, so a resized cluster reopens byte-identically.
//!
//! **Dict + vocab + tags preserved.** A resize does not touch the feature space: the normalizer
//! is reused as-is and — because the normalizer is unchanged — the frozen dict is REUSED verbatim
//! (same dense ids, same hot-mask, same fingerprint). A resize is a ring change, not a model
//! change, so the manifest's dict fingerprint and the control-plane's `dict_fingerprint` stay
//! valid; an installed alias's equivalences ride the reused dict (`extract_readonly` auto-expands
//! them), and per-query tags carry through as stored `TagId`s exactly as
//! [`set_vocab`](ClusterEngine::set_vocab) does (ADR-074).

use std::path::Path;
use std::sync::{Arc, PoisonError};

use crate::cluster::autoscale::{AutoscaleConfig, LoadSnapshot};
use crate::cluster::control::ClusterStateChange;
use crate::cluster::ring::HashRing;
use crate::cluster::shard::{LocalShard, Shard, ShardError};
use crate::compile::{extract, extract_readonly, Extracted};
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::segment::PlacedQuery;
use crate::tagdict::TagId;
use crate::vocab::Vocab;

use super::{into_shard, placement_of, replica_dir, shard_dir, ClusterEngine, Target};

impl ClusterEngine {
    /// Resize the cluster to `new_num_shards` positions (ADR-078) — a blue/green rebuild of
    /// the cluster under a fresh `HashRing::new(new_num_shards, vnodes)`: re-place every live
    /// query, build fresh shards, atomically swap the ring + shards under `&mut self`, and
    /// (for a durable cluster) commit the result via [`checkpoint`](Self::checkpoint). The
    /// vocabulary and dict are UNCHANGED (the normalizer is reused; the dict is re-minted
    /// identically; declared aliases + per-query tags carry through). Returns the number of
    /// live queries rebuilt.
    ///
    /// Refuses (errors) if `new_num_shards == 0`, or any shard is non-local / handoff-wrapped
    /// (the in-process-only boundary [`set_vocab`](Self::set_vocab) enforces). A no-op
    /// (`Ok(0)`) when `new_num_shards` already equals the current count.
    pub fn resize(&mut self, new_num_shards: usize) -> Result<usize, ShardError> {
        if new_num_shards == 0 {
            return Err(ShardError::Config(
                "resize: new_num_shards must be ≥ 1".into(),
            ));
        }
        // In-process only (same correctness boundary as set_vocab): a remote shard would keep
        // its old placement while the coordinator routes under the new ring — a silent
        // cross-process false negative. Checked BEFORE the no-op short-circuit so the boundary is
        // consistent even for a same-K call. Always compiled, so a future non-local shard can't
        // slip past it on a non-distributed build (where this never fires).
        if self.shards.iter().any(|s| !s.is_local()) {
            return Err(ShardError::Config(
                "resize is in-process only: a cross-process (remote) shard is not rebuilt under \
                 the new ring in v1 (it would be a silent false negative)"
                    .into(),
            ));
        }
        #[cfg(feature = "distributed")]
        if !self.handoffs.is_empty() {
            return Err(ShardError::Config(
                "resize is in-process only: a handoff-wrapped (movable) shard position is not \
                 supported by a resize in v1"
                    .into(),
            ));
        }
        if new_num_shards == self.ring.num_shards() {
            // The LIVE ring already has this many shards — a full rebuild would change nothing.
            // But a PRIOR resize may have swapped the ring in RAM and then FAILED to checkpoint,
            // leaving the durable manifest at the old count; a bare `Ok(0)` here would falsely
            // acknowledge that un-committed resize, and a restart would roll it back. So for a
            // durable cluster, re-ensure the commit (checkpoint is idempotent — a clean one is
            // cheap) + re-assert the on-disk dir set, so a retry HEALS rather than masks.
            if self.data_dir.is_some() {
                self.checkpoint()?;
                self.remove_shard_dirs_at_or_above(new_num_shards);
            }
            return Ok(0);
        }

        let new_ring = HashRing::new(new_num_shards, self.vnodes)?;

        // Partial-apply repairs (ADR-047) index the OLD shard space; after a resize those
        // indices are meaningless (and out of range on shrink). The rebuild below gathers the
        // live corpus (every applied mutation folded in) and the durable backstop is the
        // coordinator log, so drop the queue rather than carry stale shard indices. Empty on
        // the in-process / RF=1 default path, so this is a no-op there.
        self.pending_repair
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();

        // Rebuild under the new ring, reusing the current normalizer + vocab (None ⇒ preserve
        // self.vocab and re-resolve ITS equivalences onto the re-minted dict).
        let new_norm = Arc::clone(&self.norm);
        let rebuilt = self.rebuild_from_live(new_norm, new_ring, None)?;

        // Keep the cluster-state document consistent with the new shard count so `collect_load`
        // / `assignment_for` (introspection + the autoscaler) see K′ positions, not a stale K.
        // Durability rides the manifest (which `open` re-seeds the control plane from), so this
        // only needs to be live-correct.
        self.control.propose(ClusterStateChange::SetShardCount {
            num_shards: new_num_shards as u32,
        })?;

        // Commit the rebuild durably, THEN drop now-orphaned shard dirs (shrink only). Order
        // matters: the orphan dirs are still referenced by the OLD manifest until `checkpoint`
        // commits the new one, so deleting them earlier would break crash-recovery to the old K.
        if self.data_dir.is_some() {
            self.checkpoint()?;
            self.remove_shard_dirs_at_or_above(new_num_shards);
        }
        Ok(rebuilt)
    }

    /// Resize to the autoscaler's [`recommended_shard_count`] for the current load, if any
    /// shard crossed the split threshold. Operator-/test-facing convenience: collects the
    /// load snapshot, computes the recommendation, and applies it via [`resize`](Self::resize).
    /// Returns the new shard count if a resize happened, else `None` (no recommendation, or it
    /// already equals the current count). Refuses a non-local cluster (the gather boundary).
    pub fn resize_to_recommended(
        &mut self,
        config: &AutoscaleConfig,
    ) -> Result<Option<usize>, ShardError> {
        let snapshot = self.collect_load(config)?;
        match recommended_shard_count(&snapshot, config) {
            Some(k) if k != self.ring.num_shards() => self.resize(k).map(|_| Some(k)),
            _ => Ok(None),
        }
    }

    /// The shared blue/green rebuild core (ADR-046/078): gather the deduped live corpus, obtain a
    /// dict (REUSE the frozen one when `new_norm` is the current normalizer — a resize — else
    /// re-mint it — a `set_vocab`), re-place every query under `new_ring`, build fresh shards, and
    /// atomically swap `norm`/`dict`/`ring`/`shards`. Does NOT checkpoint — the caller owns the
    /// durable commit (so it can interleave control-plane + orphan-cleanup steps first). Returns
    /// the number of live queries rebuilt.
    ///
    /// `new_vocab`: `Some(v)` (a [`set_vocab`](Self::set_vocab) call) installs `v` and uses its
    /// equivalence groups; `None` (a [`resize`](Self::resize) call) PRESERVES the existing
    /// `self.vocab` (its equivalences are already installed on the reused dict).
    pub(super) fn rebuild_from_live(
        &mut self,
        new_norm: Arc<Normalizer>,
        new_ring: HashRing,
        new_vocab: Option<Vocab>,
    ) -> Result<usize, ShardError> {
        // Gather the deduped live `(logical, dsl, tag_ids)` set across shards. A selective /
        // any-of query lives on several shards but has ONE dsl (and one tag set — every
        // fanned-out copy carries the same tags) — dedup by logical id. Tags ride as stored
        // `TagId`s, NOT raw strings: the tag space is orthogonal to vocabulary AND to the ring,
        // preserved unchanged through the rebuild, so a stored id — interned dense or post-freeze
        // synthetic (which has no recoverable string) — stays valid and is carried verbatim to
        // the query's new shard (ADR-074). Untagged ⇒ every tag vec is empty ⇒ byte-identical to
        // the pre-tag rebuild.
        let live = self.live_corpus_tagged()?;

        // Pass A — produce the (dict, extracted) the rebuild re-places. Two paths, keyed off
        // whether the NORMALIZER changed (an `Arc::ptr_eq` against the current one):
        //
        //  - **Normalizer unchanged (a resize):** the feature space cannot have changed, so REUSE
        //    the frozen dict verbatim — same dense ids, same hot-mask, same fingerprint. A resize
        //    is a ring change, NOT a model change: reusing the dict keeps the manifest's dict
        //    fingerprint invariant and the control-plane's `dict_fingerprint` valid (re-minting
        //    would renumber ids if the live corpus order differed from the original build, or if
        //    post-freeze terms were added — a spurious fingerprint change desyncing cluster
        //    state). `extract_readonly` resolves each query against it, auto-expanding installed
        //    equivalences (ADR-054) and resolving post-freeze terms to their stable synthetic ids
        //    (ADR-046) — so placement is exactly the live cluster's, just re-distributed.
        //  - **Normalizer changed (a `set_vocab`):** re-mint the dict over the live corpus under
        //    `new_norm` (interning + frequencies + hot-mask), exactly as `build`, then resolve +
        //    expand the new vocab's equivalence groups onto it.
        let mut lc = String::new();
        let mut extracted: Vec<(u64, Extracted, String, u32, Vec<TagId>)> =
            Vec::with_capacity(live.len());
        let new_dict = if Arc::ptr_eq(&new_norm, &self.norm) {
            let dict = Arc::clone(&self.dict);
            for (logical, text, version, tag_ids) in live {
                if let Ok(ast) = crate::dsl::parse(&text) {
                    let ex = extract_readonly(&ast, &new_norm, &dict, &mut lc);
                    extracted.push((logical, ex, text, version, tag_ids));
                }
            }
            dict
        } else {
            let mut dict = Dict::new();
            for (logical, text, version, tag_ids) in live {
                if let Ok(ast) = crate::dsl::parse(&text) {
                    let ex = extract(&ast, &new_norm, &mut dict, &mut lc);
                    extracted.push((logical, ex, text, version, tag_ids));
                }
            }
            dict.finalize_mask();
            // Resolve declared/learned equivalence groups (ADR-054) against the freshly-minted
            // dict and apply them via expansion: widen the already-extracted queries (so THIS
            // rebuild's re-placement + ingest use the FN-safe widened form — a query whose anchor
            // is now an any-of fans to every member's shard), then install the map on the dict so
            // future incremental adds expand through `extract`. The groups come from `new_vocab`
            // (set_vocab) or, when preserving, the EXISTING `self.vocab`. No groups ⇒ no-op.
            let equiv = new_vocab
                .as_ref()
                .or(self.vocab.as_deref())
                .map(|v| v.resolve_equivalences(&new_norm, &dict));
            if let Some(equiv) = equiv {
                for (_, ex, _, _, _) in &mut extracted {
                    ex.expand_equivalences(&equiv);
                }
                dict.set_equivalences(equiv);
            }
            Arc::new(dict)
        };
        let rebuilt = extracted.len();

        // Pass B — re-place each query under the NEW dict + NEW ring and bucket per shard. Tags
        // travel with the query (`tag_ids`, the ADR-074 carry-through): a different shard count
        // moves a query's anchor — hence its shard — and the filtered-read contract requires its
        // tags on whichever shard now holds it.
        let num_shards = new_ring.num_shards();
        let mut buckets: Vec<Vec<PlacedQuery>> = (0..num_shards).map(|_| Vec::new()).collect();
        for (logical, ex, text, version, tag_ids) in extracted {
            // Re-placing ALREADY-STORED queries: a stored class-D was accepted when it was
            // added, so a rebuild (resize / set_vocab) must never drop it via the current knob
            // (mirrors the single-node ADR-068 vocab recompile, which passes accept=true
            // unconditionally). The empty-forbidden guard in `placement_of` still rejects the
            // never-stored empty query, so passing `true` cannot resurrect one.
            match placement_of(&new_dict, &new_ring, &ex, true) {
                Target::Reject => {}
                Target::Replicated => {
                    // The broad lane is replicated to every shard (ADR-080). Carry the stored
                    // version through the rebuild so a re-placed query keeps version N rather
                    // than being reset to 1 (the version-preserving rebuild, ADR-074).
                    for bucket in &mut buckets {
                        bucket.push(PlacedQuery {
                            logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version,
                            tags: Vec::new(),
                            tag_ids: tag_ids.clone(),
                        });
                    }
                }
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push(PlacedQuery {
                            logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version,
                            tags: Vec::new(),
                            tag_ids: tag_ids.clone(),
                        });
                    }
                }
            }
        }

        // Construct fresh shards sharing the new norm + re-minted dict + unchanged tag space,
        // `replication_factor` copies per position, ingesting each bucket into EVERY copy
        // (identical op stream ⇒ copies set-equal, as in `build`). Two cases by position:
        //
        //  - EXISTING position (`s < old_num_shards`): rebuild in the SAME shard dir, numbering
        //    green segments ABOVE the old ones (the set_vocab coexist path), so the new `.seg`
        //    coexist with the still-committed old ones until the manifest commit — a crash before
        //    the commit leaves the old manifest + old segments authoritative.
        //  - NEW position (`s ≥ old_num_shards`, grow only): no old shard to coexist with.
        //    FORCE-CLEAN the dir first so a stale orphan from a PRIOR shrink can't resurrect data
        //    (its checkpoint sidecar would self-restart `new_durable` into an old corpus, or its
        //    `sources.dat` would shadow the green ingest), then build a fresh durable shard —
        //    exactly `build`'s path. The post-commit `remove_orphan_shard_dirs` keeps the
        //    invariant "a resize commit leaves exactly shard_000..shard_{K′-1} on disk".
        let old_num_shards = self.shards.len();
        let rf = self.replication_factor.max(1);
        let data_dir = self.data_dir.clone();
        // The rebuild re-places ALREADY-STORED queries, so stored class-D must survive regardless
        // of the current front-door knob: `placement_of(.., true)` above buckets it, and the shards
        // are coordinator-gated storage that always accept (forced in `LocalShard`), so the fresh
        // shards re-ingest it. NEW class-D adds stay gated at the coordinator by the unchanged
        // `self.per_shard.accept_class_d`.
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(num_shards);
        for (s, bucket) in buckets.into_iter().enumerate() {
            let mut copies = Vec::with_capacity(rf);
            for r in 0..rf {
                let copy = match &data_dir {
                    Some(dir) => {
                        let mut sc = self.per_shard.clone();
                        let cdir = if r == 0 {
                            shard_dir(dir, s)
                        } else {
                            replica_dir(dir, s, r)
                        };
                        sc.data_dir = Some(cdir.clone());
                        if s < old_num_shards {
                            // Existing position: coexist green segments above the old ones.
                            let next_seg = self.shards[s].next_seg_id()?;
                            LocalShard::open_segments(
                                Arc::clone(&new_norm),
                                Arc::clone(&new_dict),
                                Arc::clone(&self.tag_dict),
                                sc,
                                &[],
                                next_seg,
                            )?
                        } else {
                            // New position (grow): clean any stale dir, then build fresh.
                            clean_shard_dir(&cdir)?;
                            LocalShard::new_durable(
                                Arc::clone(&new_norm),
                                Arc::clone(&new_dict),
                                Arc::clone(&self.tag_dict),
                                sc,
                            )?
                        }
                    }
                    None => LocalShard::new(
                        Arc::clone(&new_norm),
                        Arc::clone(&new_dict),
                        Arc::clone(&self.tag_dict),
                        self.per_shard.clone(),
                    ),
                };
                if !bucket.is_empty() {
                    copy.ingest_local(&bucket);
                }
                copies.push(copy);
            }
            shards.push(into_shard(copies)?);
        }

        // Atomic swap (under `&mut self`, so no read observes a half-state). The normalizer is
        // `new_norm` (the SAME instance on a resize); `self.vocab` is replaced only when a new
        // vocab was supplied (set_vocab) — a resize passes `None` and preserves it.
        self.norm = new_norm;
        self.dict = new_dict;
        self.ring = new_ring;
        self.shards = shards;
        if let Some(vocab) = new_vocab {
            self.vocab = Some(Arc::new(vocab));
        }
        Ok(rebuilt)
    }

    /// Best-effort removal of every top-level `shard_NNN` directory whose index is `≥
    /// num_shards`, called AFTER the manifest commit so the committed manifest no longer
    /// references them. This asserts the invariant "a committed cluster's on-disk dir set is
    /// exactly `shard_000..shard_{num_shards-1}`": a SHRINK's orphan dirs are removed (so a later
    /// GROW back through these positions cannot self-restart `new_durable` from a stale sidecar),
    /// and the heal path (a same-K retry after a failed checkpoint) re-asserts it too. Scans
    /// rather than taking an old count, so it is correct without knowing the prior shape. An
    /// orphan left behind is benign for correctness (`open` reads only `0..num_shards`).
    fn remove_shard_dirs_at_or_above(&self, num_shards: usize) {
        let Some(dir) = &self.data_dir else {
            return;
        };
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(idx) = name
                .to_str()
                .and_then(|n| n.strip_prefix("shard_"))
                .and_then(|s| s.parse::<usize>().ok())
            else {
                continue; // not a shard dir (the manifest, the log, a replica is nested, …)
            };
            if idx < num_shards {
                continue;
            }
            let sd = entry.path();
            match std::fs::remove_dir_all(&sd) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => self.emit(EngineEvent::DurabilityFailure {
                    op: DurabilityOp::WalReset,
                    detail: format!(
                        "removing orphaned shard dir {} failed (benign: it is not in the committed \
                         manifest, so `open` ignores it)",
                        sd.display()
                    ),
                    error: e.to_string(),
                }),
            }
        }
    }
}

/// Remove a shard directory and all its contents, treating "not found" as success. Used to
/// guarantee a NEW position (grow) builds over a verified-clean dir — never self-restarting
/// `LocalShard::new_durable` from a leftover checkpoint sidecar / `sources.dat`.
fn clean_shard_dir(dir: &Path) -> Result<(), ShardError> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(ShardError::Log(format!(
            "cleaning new shard dir {} before a grow: {e}",
            dir.display()
        ))),
    }
}

/// Recommend a new `num_shards` from the load snapshot (ADR-078): `None` when split detection
/// is disabled (`split_corpus_threshold == 0`) or no shard is over threshold; otherwise
/// `current_num_shards + count(over-threshold shards)` — one fresh bucket per hot shard. Pure and
/// deterministic (no clock, no randomness), and monotone within a single snapshot: it never
/// recommends shrinking, so an operator/driver applying it cannot thrash a cluster smaller.
pub fn recommended_shard_count(snapshot: &LoadSnapshot, config: &AutoscaleConfig) -> Option<usize> {
    if config.split_corpus_threshold == 0 {
        return None;
    }
    let over = snapshot
        .shard_corpus
        .iter()
        // Discount the replicated broad lane (on every shard regardless of K, ADR-080): only the
        // SELECTIVE load is reduced by splitting, so only it counts as split pressure. Else every
        // shard looks hot and a driver applying `resize_to_recommended` grows without bound
        // (codex review).
        .filter(|&&c| c.saturating_sub(snapshot.replicated_corpus) > config.split_corpus_threshold)
        .count();
    if over == 0 {
        None
    } else {
        Some(snapshot.num_shards as usize + over)
    }
}
