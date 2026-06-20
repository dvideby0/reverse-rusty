//! `impl ClusterEngine` — the write path: bulk `ingest`, incremental `add_query` /
//! `remove_query`, the shared `apply` / `replay_apply` funnel, placement bucketing, and `flush`.

use crate::cluster::clog::ClusterMutation;
use crate::cluster::shard::ShardError;
use crate::compile::{extract_readonly, Extracted};
use crate::events::{DurabilityOp, EngineEvent};
use crate::segment::PlacedQuery;

use super::{placement_of, AddOutcome, ClusterEngine, PendingRepair, ResyncReport, Target};

/// One bulk-load entry: `(logical, version, dsl, raw tags)` (ADR-055) — the input to
/// [`ClusterEngine::bucket_and_ingest`], before placement turns it into a [`PlacedQuery`] per shard.
type TaggedEntry = (u64, u32, String, Vec<(String, String)>);

impl ClusterEngine {
    /// Bulk-load queries into an already-built (frozen-dict) cluster — the load path
    /// for a cluster assembled via [`Self::from_parts`] (e.g. a remote cluster), and
    /// the distributed analog of `build`'s pass B. Buckets each query by placement
    /// (compiling read-only against the shared frozen dict) and ingests each bucket
    /// into its shard through the seam. Parse failures and class-D queries are skipped
    /// (mirroring `build`); a shard write error propagates. Requires a freshly assembled
    /// (empty) cluster: it errors with [`ShardError::Config`] if the cluster already holds
    /// queries, rather than silently re-indexing them as duplicates (use
    /// [`Self::add_query`] for incremental adds).
    pub fn ingest(&self, queries: &[(u64, String)]) -> Result<(), ShardError> {
        self.ingest_with_tags(queries, &[])
    }

    /// [`ingest`](Self::ingest) carrying per-query metadata tags (ADR-049/055) — the bulk-load
    /// counterpart to [`build_with_tags`](Self::build_with_tags), for a freshly assembled (e.g.
    /// remote) cluster. `tags` is parallel to `queries`; an empty slice means no query is tagged
    /// (byte-identical to `ingest`). Each shard resolves the raw tags read-only against the shared
    /// frozen tag space, so a later filtered percolate agrees on the `TagId`s.
    pub fn ingest_with_tags(
        &self,
        queries: &[(u64, String)],
        tags: &[Vec<(String, String)>],
    ) -> Result<(), ShardError> {
        // ingest re-indexes from scratch; on a populated cluster it would create duplicate
        // entries. Refuse loudly instead (the doc contract: a freshly assembled cluster).
        if self.num_queries()? > 0 {
            return Err(ShardError::Config(
                "ingest() requires an empty cluster; it re-indexes from scratch — use \
                 add_query for incremental adds"
                    .into(),
            ));
        }
        if tags.iter().any(|t| !t.is_empty()) {
            self.tags_present
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let entries: Vec<TaggedEntry> = queries
            .iter()
            .enumerate()
            .map(|(i, (l, t))| (*l, 1, t.clone(), tags.get(i).cloned().unwrap_or_default()))
            .collect();
        self.bucket_and_ingest(&entries)?;
        // These bulk adds bypassed the log (they go straight to base segments), so on a
        // durable cluster a checkpoint commits them into the coordinator manifest's
        // per-shard segment registry to survive reopen.
        if self.data_dir.is_some() {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Bucket a set of `(logical, version, dsl, tags)` queries by placement and bulk-ingest one
    /// base segment per shard — the load path for [`Self::ingest_with_tags`] (a freshly assembled,
    /// e.g. remote, cluster). Compiles read-only against the frozen dict, so placement is
    /// byte-identical to the original build. (Recovery no longer re-ingests; [`Self::open`]
    /// attaches each shard's committed segments instead — ADR-032.)
    fn bucket_and_ingest(&self, entries: &[TaggedEntry]) -> Result<(), ShardError> {
        let mut buckets: Vec<Vec<PlacedQuery>> =
            (0..self.ring.num_shards()).map(|_| Vec::new()).collect();
        let mut lc = String::new();
        for (logical, version, text, qtags) in entries {
            let Ok(ast) = crate::dsl::parse(text) else {
                continue;
            };
            let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
            match self.placement(&ex) {
                Target::Reject => {}
                Target::Replicated => {
                    // The broad lane is replicated to every shard (ADR-080).
                    for bucket in &mut buckets {
                        bucket.push(PlacedQuery {
                            logical: *logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version: *version,
                            tags: qtags.clone(),
                            tag_ids: Vec::new(),
                        });
                    }
                }
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push(PlacedQuery {
                            logical: *logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version: *version,
                            tags: qtags.clone(),
                            tag_ids: Vec::new(),
                        });
                    }
                }
            }
        }
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.shards[s].ingest_extracted(&bucket)?;
            }
        }
        Ok(())
    }

    /// The placement decision for one compiled query — see the module-level table.
    /// Delegates to the free [`placement_of`] so `build` can bucket the corpus before
    /// the cluster value exists.
    fn placement(&self, ex: &Extracted) -> Target {
        placement_of(&self.dict, &self.ring, ex, self.per_shard.accept_class_d)
    }

    /// True if the cluster holds (or has ever held) any tagged query (ADR-055): the `tags_present`
    /// latch (any tagged write, incl. post-freeze *synthetic* tags never interned into `tag_dict`)
    /// OR a non-empty `tag_dict` (build-time interned tags). Operator introspection only
    /// ([`Self::has_tagged_queries`]) — the vocab rebuild carries tags by stored `TagId` (ADR-074)
    /// and no longer consults this.
    pub(in crate::cluster::coordinator) fn has_tags(&self) -> bool {
        self.tags_present.load(std::sync::atomic::Ordering::Relaxed) || !self.tag_dict.is_empty()
    }

    /// Latch [`tags_present`](ClusterEngine::tags_present) when a non-empty tagged write happens.
    /// Cheap + idempotent; no-op for an untagged write (the byte-identical path).
    pub(in crate::cluster::coordinator) fn note_tags(&self, tags: &[(String, String)]) {
        if !tags.is_empty() {
            self.tags_present
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Add one query incrementally (lands in the target shard's memtable). Uses a
    /// read-only compile against the frozen shared dict: vocabulary not seen at
    /// [`Self::build`] time is **absorbed** into the reserved synthetic-ID range (a
    /// deterministic hash, ADR-046), not dropped — so a required term new to the dict
    /// still anchors its query (a hash collision is a bounded over-match the exact
    /// matcher rejects, never a dropped required term).
    ///
    /// WAL-first: an ACCEPTED mutation is durably logged BEFORE it is applied to any shard, so a
    /// crash can never leave an acknowledged add that [`Self::open`] would lose. A log append
    /// failure rejects the add (shards untouched) and surfaces a
    /// [`DurabilityFailure`](EngineEvent::DurabilityFailure) — the cluster analogue of the
    /// engine's WAL-first write path (ADR-013). A REJECTED write (class D with the lane off, an
    /// empty query, or a parse error) is classified out BEFORE the log, so the log holds only
    /// accepted mutations and replay is configuration-independent (codex review).
    pub fn add_query(&self, id: u64, dsl: &str) -> Result<AddOutcome, ShardError> {
        self.add_query_with_tags(id, dsl, &[])
    }

    /// [`add_query`](Self::add_query) carrying per-query metadata tags (ADR-049/055). The raw tags
    /// ride the cluster log alongside the DSL (logged BEFORE apply, like the DSL), and are resolved
    /// read-only against the shared frozen tag space on each target shard, so a tagged add and a
    /// later filtered percolate agree on the tag's `TagId`. Empty tags ⇒ byte-identical to
    /// [`add_query`](Self::add_query).
    pub fn add_query_with_tags(
        &self,
        id: u64,
        dsl: &str,
        tags: &[(String, String)],
    ) -> Result<AddOutcome, ShardError> {
        // Reject malformed DSL up front: it carries no replayable mutation, so it must
        // never reach the log (a logged record must parse on replay).
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok(AddOutcome::RejectedParse(e)),
        };
        // Classify BEFORE logging (against the CURRENT knob): a REJECTED write — class D with the
        // lane off, or an effectively-empty query — carries no replayable mutation and must NEVER
        // reach the log. Else, replaying it under a since-flipped knob would resurrect a query the
        // caller was told was rejected (codex review). This is the cluster analogue of the
        // single-node "the WAL records only accepted mutations" (ADR-068); the apply/replay funnel
        // then forces accept=true, so replay reproduces the writer's decision regardless of config.
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        if matches!(self.placement(&ex), Target::Reject) {
            return Ok(AddOutcome::RejectedClassD);
        }
        let m = ClusterMutation::Add {
            logical: id,
            version: 1,
            dsl: dsl.to_string(),
            tags: tags.to_vec(),
        };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster add_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_add(id, 1, dsl, tags)
    }

    /// Atomically replace a query by logical id — ES `index` semantics at the cluster
    /// (ADR-070, the coordinator analogue of the engine's ADR-067 upsert): every prior
    /// live copy is tombstoned and the new version inserted under ONE log frame
    /// ([`ClusterMutation::Upsert`]), so a crash replays the whole replacement or none
    /// of it — never a remove that lost its re-add. Returns the number of prior entries
    /// removed (0 ⇒ created, >0 ⇒ updated) plus where the new version landed. A
    /// rejected new version (parse / class D) **never deletes** — the prior version
    /// stays live and matchable.
    pub fn upsert_query(&self, id: u64, dsl: &str) -> Result<(usize, AddOutcome), ShardError> {
        self.upsert_query_with_tags(id, dsl, &[])
    }

    /// [`upsert_query`](Self::upsert_query) carrying per-query metadata tags for the NEW
    /// version (ADR-055 semantics: raw tags ride the log frame and resolve read-only
    /// against the shared frozen tag space on each target shard).
    pub fn upsert_query_with_tags(
        &self,
        id: u64,
        dsl: &str,
        tags: &[(String, String)],
    ) -> Result<(usize, AddOutcome), ShardError> {
        // Reject malformed DSL up front: it carries no replayable mutation, so it must
        // never reach the log (a logged record must parse on replay) — and a failed
        // replace never deletes.
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok((0, AddOutcome::RejectedParse(e))),
        };
        // Classify BEFORE logging (current knob): a rejected new version carries no replayable
        // mutation AND must not delete the prior version, so it never reaches the log or the
        // tombstone pass. Same config-independent-replay discipline as add (codex review): the
        // log holds only accepted mutations, and apply/replay forces accept=true.
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        if matches!(self.placement(&ex), Target::Reject) {
            return Ok((0, AddOutcome::RejectedClassD));
        }
        let m = ClusterMutation::Upsert {
            logical: id,
            version: 1,
            dsl: dsl.to_string(),
            tags: tags.to_vec(),
        };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster upsert_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_upsert(id, 1, dsl, tags)
    }

    /// Apply an UPSERT to the shards — the state-machine `apply` for replace-by-id,
    /// shared by the live write path (after logging) and log replay, so live and
    /// replayed application are byte-identical. Placement is decided FIRST: a class-D /
    /// parse rejection returns before any tombstone (a failed replace never deletes,
    /// ADR-067 parity). Then pass 1 tombstones the id on every shard (a re-placed query
    /// may live anywhere) and pass 2 inserts the new version on its placement shards —
    /// the two-pass order guarantees delete-before-insert on every shard that keeps the
    /// query. Partial failures ride the ADR-047 machinery with the `Upsert` itself as
    /// the queued repair mutation (re-driving it per shard is an idempotent
    /// delete + insert).
    fn apply_upsert(
        &self,
        id: u64,
        version: u32,
        dsl: &str,
        tags: &[(String, String)],
    ) -> Result<(usize, AddOutcome), ShardError> {
        self.note_tags(tags);
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok((0, AddOutcome::RejectedParse(e))),
        };
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        // Force accept=true: apply is reached ONLY for already-accepted writes (live upsert
        // classified + accepted before logging; replay sees only logged=accepted frames), so this
        // placement is configuration-independent — a knob flip on reopen neither drops nor
        // resurrects (codex review). The empty-class-D guard in `placement_of` still rejects a
        // never-stored empty query defensively.
        let (insert_shards, outcome) = match placement_of(&self.dict, &self.ring, &ex, true) {
            Target::Reject => return Ok((0, AddOutcome::RejectedClassD)),
            // The broad lane is replicated to every shard (ADR-080); pass 1 already tombstones
            // every shard, so pass 2 re-inserts the new version on every shard.
            Target::Replicated => ((0..self.shards.len()).collect(), AddOutcome::Replicated),
            Target::Selective(shards) => (
                shards.clone(),
                AddOutcome::Placed {
                    shards: shards.clone(),
                },
            ),
        };
        // Pass 1 — tombstone every prior copy, everywhere (idempotent on non-holders).
        let mut removed = 0usize;
        let mut failed: Vec<usize> = Vec::new();
        let mut first_err: Option<ShardError> = None;
        for (s, shard) in self.shards.iter().enumerate() {
            match shard.delete_by_logical_id(id) {
                Ok(n) => removed += n,
                Err(e) => {
                    failed.push(s);
                    first_err.get_or_insert(e);
                }
            }
        }
        // Pass 2 — insert the new version on its placement shards. A shard whose delete
        // failed is skipped (its repair re-drives the WHOLE upsert, preserving the
        // per-shard delete-before-insert order).
        let mut inserted: Vec<usize> = Vec::with_capacity(insert_shards.len());
        for &s in &insert_shards {
            if failed.contains(&s) {
                continue;
            }
            match self.shards[s].insert_extracted_with_tags(&ex, id, version, dsl, tags) {
                Ok(_) => inserted.push(s),
                Err(e) => {
                    failed.push(s);
                    first_err.get_or_insert(e);
                }
            }
        }
        if !failed.is_empty() {
            failed.sort_unstable();
            failed.dedup();
            // `applied` reports the shards that now HOLD the new version (the insert
            // pass succeeded there) — not every shard that merely completed its
            // tombstone half, which would overstate where the replacement lives
            // (review finding). Repair targets only `failed`, so this is diagnostic.
            return Err(self.note_partial(
                ClusterMutation::Upsert {
                    logical: id,
                    version,
                    dsl: dsl.to_string(),
                    tags: tags.to_vec(),
                },
                id,
                inserted,
                failed,
                first_err,
            ));
        }
        self.clear_pending(id);
        Ok((removed, outcome))
    }

    /// Remove a query by logical id. Fans the (idempotent) delete out to every
    /// shard and sums the count — sidestepping any placement journal (a replicated
    /// or any-of query may live on several shards; a re-add may have moved it).
    /// WAL-first, like [`Self::add_query`].
    pub fn remove_query(&self, id: u64) -> Result<usize, ShardError> {
        let m = ClusterMutation::Remove { logical: id };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster remove_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_remove(id)
    }

    /// Insert a compiled query on a set of target shards, collecting partial-apply failures
    /// (ADR-047): try EVERY shard rather than bailing on the first error, so a mid-fan-out
    /// remote failure is queued for repair (keyed by logical id, an idempotent re-insert on the
    /// failed shards) instead of leaving a silent partial mutation. In-process inserts are
    /// infallible ⇒ `failed` stays empty ⇒ byte-identical to a plain loop. On any failure it
    /// queues the repair, emits, and returns the honest error; otherwise it returns `success`.
    /// Shared by the `Selective` (its placement shards) and `Replicated` (every shard, ADR-080)
    /// arms of [`Self::apply_add`].
    #[allow(clippy::too_many_arguments)]
    fn insert_on_shards(
        &self,
        shards: &[usize],
        ex: &Extracted,
        id: u64,
        version: u32,
        dsl: &str,
        tags: &[(String, String)],
        success: AddOutcome,
    ) -> Result<AddOutcome, ShardError> {
        let mut applied = Vec::with_capacity(shards.len());
        let mut failed = Vec::new();
        let mut first_err: Option<ShardError> = None;
        for &s in shards {
            match self.shards[s].insert_extracted_with_tags(ex, id, version, dsl, tags) {
                Ok(_) => applied.push(s),
                Err(e) => {
                    failed.push(s);
                    first_err.get_or_insert(e);
                }
            }
        }
        if !failed.is_empty() {
            return Err(self.note_partial(
                ClusterMutation::Add {
                    logical: id,
                    version,
                    dsl: dsl.to_string(),
                    tags: tags.to_vec(),
                },
                id,
                applied,
                failed,
                first_err,
            ));
        }
        Ok(success)
    }

    /// Apply an ADD to the shards — the state-machine `apply` for adds, shared by the live
    /// write path ([`Self::add_query`], after logging) and log replay ([`Self::open`]).
    /// Re-deriving placement here from the frozen dict makes live and replayed application
    /// byte-identical.
    fn apply_add(
        &self,
        id: u64,
        version: u32,
        dsl: &str,
        tags: &[(String, String)],
    ) -> Result<AddOutcome, ShardError> {
        // Latch tags_present (ADR-055, `/_stats` introspection) — covers both the live add
        // (`add_query_with_tags`) and a tagged log-tail entry replayed on `open`.
        self.note_tags(tags);
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok(AddOutcome::RejectedParse(e)),
        };
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        // Force accept=true (same only-accepted-writes invariant as apply_upsert): apply/replay
        // reproduces the writer's decision regardless of the current knob, so a knob flip on
        // reopen cannot drop or resurrect a class-D write (codex review). Rejected writes never
        // reach the log (classified out in add_query), so the Reject arm is defensive.
        let outcome = match placement_of(&self.dict, &self.ring, &ex, true) {
            // Defensive: an effectively-empty query is rejected before logging, so a logged
            // mutation never lands here; a replayed no-op (stored nowhere) is still safe.
            Target::Reject => return Ok(AddOutcome::RejectedClassD),
            // The broad lane (class C / B arity-2 / accepted D): replicated to EVERY shard
            // (ADR-080). Same fail-collect fan-out as Selective, so a mid-fan-out remote failure
            // is queued for repair rather than a silent partial. In-process inserts are infallible
            // ⇒ the outcome is byte-identical save that the entry now lands on every shard.
            Target::Replicated => {
                let all: Vec<usize> = (0..self.shards.len()).collect();
                self.insert_on_shards(&all, &ex, id, version, dsl, tags, AddOutcome::Replicated)?
            }
            Target::Selective(shards) => self.insert_on_shards(
                &shards,
                &ex,
                id,
                version,
                dsl,
                tags,
                AddOutcome::Placed {
                    shards: shards.clone(),
                },
            )?,
        };
        // A successful full apply supersedes any stale partial-apply queued for this id, so
        // `resync` never re-drives an outdated mutation. Cheap no-op on the default path.
        self.clear_pending(id);
        Ok(outcome)
    }

    /// Apply a REMOVE to the shards — the state-machine `apply` for removes. The shard
    /// memtable/segment liveness is the authority; there is no separate coordinator live
    /// set to keep in sync (the durable base is the per-shard segments — ADR-032).
    fn apply_remove(&self, id: u64) -> Result<usize, ShardError> {
        // Remove fans the idempotent delete out to EVERY shard. Try them all (don't bail on the
        // first error) and collect failures, so a partial remove is repairable rather than a
        // silent half-delete (ADR-047). In-process deletes are infallible ⇒ `failed` stays empty
        // ⇒ byte-identical to the old `.sum()`.
        let mut removed = 0usize;
        let mut failed = Vec::new();
        let mut first_err: Option<ShardError> = None;
        for (s, shard) in self.shards.iter().enumerate() {
            match shard.delete_by_logical_id(id) {
                Ok(n) => removed += n,
                Err(e) => {
                    failed.push(s);
                    first_err.get_or_insert(e);
                }
            }
        }
        if !failed.is_empty() {
            let applied: Vec<usize> = (0..self.shards.len())
                .filter(|s| !failed.contains(s))
                .collect();
            return Err(self.note_partial(
                ClusterMutation::Remove { logical: id },
                id,
                applied,
                failed,
                first_err,
            ));
        }
        // A successful full delete supersedes any queued partial Add/Remove for this id.
        self.clear_pending(id);
        Ok(removed)
    }

    /// Record a partial multi-shard apply (ADR-047): queue the failed shards for repair (keyed by
    /// logical id, so the latest mutation for an id wins), emit a `ClusterPartialApply` durability
    /// event, and build the honest [`ShardError::PartiallyApplied`] the caller returns. The
    /// mutation is already durably logged, so this is a liveness gap (a transient false-negative
    /// window on `failed`), not a lost write — [`Self::resync`] or reopen converges it.
    fn note_partial(
        &self,
        mutation: ClusterMutation,
        logical: u64,
        applied: Vec<usize>,
        failed: Vec<usize>,
        first_err: Option<ShardError>,
    ) -> ShardError {
        let detail = first_err.map_or_else(|| "unknown shard error".to_string(), |e| e.to_string());
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                logical,
                PendingRepair {
                    mutation,
                    failed_shards: failed.clone(),
                },
            );
        self.emit(EngineEvent::DurabilityFailure {
            op: DurabilityOp::ClusterPartialApply,
            detail: format!("logical {logical}: applied on {applied:?}, failed on {failed:?}"),
            error: detail.clone(),
        });
        ShardError::PartiallyApplied {
            logical,
            applied,
            failed,
            detail,
        }
    }

    /// Drop any queued partial-apply entry for `logical` — a later full apply (or delete)
    /// supersedes it, so `resync` must not re-drive a stale mutation (e.g. resurrect a removed
    /// query). Cheap (an uncontended lock + a `BTreeMap` miss) on the default path, where the
    /// queue is always empty.
    fn clear_pending(&self, logical: u64) {
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&logical);
    }

    /// Re-drive every queued partial-apply mutation (ADR-047) against its still-failed shards,
    /// converging a cluster left divergent by a mid-fan-out remote write failure WITHOUT a full
    /// reopen. Re-driving touches ONLY the failed shards — re-applying an Add there is a clean
    /// first insert (they never received it) and a Remove is idempotent — so already-converged
    /// shards are untouched. Idempotent and safe to call repeatedly: a still-unreachable shard
    /// stays queued. A no-op (empty report) on the in-process / RF=1 path, which never queues
    /// anything. The durable cluster log stays authoritative — a reopen replays it in order, so
    /// `resync` is a liveness optimization, not the correctness backstop.
    pub fn resync(&self) -> ResyncReport {
        // Drain the queue, then re-drive OUTSIDE the lock (re-driving issues shard RPCs; holding
        // the lock across them would stall concurrent writes' note_partial/clear_pending).
        let pending: Vec<(u64, PendingRepair)> = {
            let mut guard = self
                .pending_repair
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *guard).into_iter().collect()
        };
        let mut repaired = 0usize;
        let mut still_pending = 0usize;
        for (logical, pr) in pending {
            let mut still_failed = Vec::new();
            let mut first_err: Option<ShardError> = None;
            for &s in &pr.failed_shards {
                match crate::cluster::shard::apply_mutation(
                    self.shards[s].as_ref(),
                    &self.norm,
                    &self.dict,
                    &pr.mutation,
                ) {
                    Ok(()) => {}
                    Err(e) => {
                        still_failed.push(s);
                        first_err.get_or_insert(e);
                    }
                }
            }
            if still_failed.is_empty() {
                repaired += 1;
                continue;
            }
            still_pending += 1;
            let detail =
                first_err.map_or_else(|| "unknown shard error".to_string(), |e| e.to_string());
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ClusterPartialApply,
                detail: format!("resync: logical {logical} still failing on {still_failed:?}"),
                error: detail,
            });
            // Re-queue only the still-failed shards — but `or_insert`, so a fresher mutation a
            // concurrent write queued for this id during the drain is not clobbered.
            self.pending_repair
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(logical)
                .or_insert(PendingRepair {
                    mutation: pr.mutation,
                    failed_shards: still_failed,
                });
        }
        ResyncReport {
            repaired,
            still_pending,
        }
    }

    /// Number of mutations currently queued for partial-apply repair (ADR-047): 0 on a healthy
    /// cluster, and always 0 on the in-process / RF=1 path (whose writes never fail). A nonzero
    /// value means at least one shard is lagging — call [`Self::resync`] (or wait for the next
    /// autoscaler `tick`) to converge it. Introspection for operators + tests.
    #[must_use]
    pub fn pending_repairs(&self) -> usize {
        self.pending_repair
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Replay one recovered mutation through the same `apply` funnel as live writes.
    pub(in crate::cluster::coordinator) fn replay_apply(
        &self,
        m: ClusterMutation,
    ) -> Result<(), ShardError> {
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
                tags,
            } => {
                self.apply_add(logical, version, &dsl, &tags)?;
            }
            ClusterMutation::Remove { logical } => {
                self.apply_remove(logical)?;
            }
            ClusterMutation::Upsert {
                logical,
                version,
                dsl,
                tags,
            } => {
                self.apply_upsert(logical, version, &dsl, &tags)?;
            }
        }
        Ok(())
    }

    /// Seal every shard's memtable into an immutable base segment.
    pub fn flush(&self) -> Result<(), ShardError> {
        for s in &self.shards {
            s.flush()?;
        }
        Ok(())
    }
}
