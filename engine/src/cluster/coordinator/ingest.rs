//! `impl ClusterEngine` — the write path: bulk `ingest`, incremental `add_query` /
//! `remove_query`, the shared `apply` / `replay_apply` funnel, placement bucketing, and `flush`.

use crate::cluster::clog::ClusterMutation;
use crate::cluster::shard::ShardError;
use crate::compile::{extract_readonly, Extracted};
use crate::error::{ParseError, ParseErrorKind};
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
        // ADR-113: bulk load is a mutation like any other for the PIT-open
        // barrier — a pin fan interleaving mid-load would freeze half a corpus.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Initial bulk load is one exclusive logical-id admission boundary. A
        // concurrent incremental mutation cannot slip between the empty check,
        // directory install, and shard writes.
        let _logical_guards = self.logical_bulk_write_guards();
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
        let mut accepted_ids = Vec::with_capacity(entries.len());
        for (logical, version, text, qtags) in entries {
            let Ok(ast) = crate::dsl::parse(text) else {
                continue;
            };
            let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
            let target = self.placement(&ex);
            let placement =
                target.placement(self.placement_generation(), self.shards.len() as u32)?;
            if !matches!(&target, Target::Reject) {
                accepted_ids.push(*logical);
            }
            match target {
                Target::Reject => {}
                Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                    // The broad lane is replicated to every shard (ADR-080).
                    for bucket in &mut buckets {
                        bucket.push(PlacedQuery {
                            logical: *logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version: *version,
                            tags: qtags.clone(),
                            tag_ids: Vec::new(),
                            rank: crate::rank::RankValues::default(),
                            placement: placement.clone(),
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
                            rank: crate::rank::RankValues::default(),
                            placement: placement.clone(),
                        });
                    }
                }
            }
        }
        super::logical_ids::sort_and_check_unique(&mut accepted_ids)?;
        // Reserve the complete semantic corpus BEFORE the first shard mutation.
        // If a remote bulk write fails part-way, retaining these reservations is
        // fail-closed: an incremental Add cannot coexist with a physical row that
        // may already have landed. Retrying ingest on the still-empty cluster may
        // replace this directory with the same corpus and continue.
        self.replace_logical_ids(accepted_ids)?;
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                if let Err(error) = self.shards[s].ingest_extracted(&bucket) {
                    // Unlike incremental writes, this initial base-segment fan-out
                    // has no per-logical repair record. Earlier shards may already
                    // hold their buckets, and a transport failure cannot prove the
                    // current shard applied nothing. Keep all id reservations but
                    // revoke the convergence attestation so exact exhaustive
                    // delivery cannot certify the ambiguous corpus (review finding).
                    self.mark_logical_ids_unconverged();
                    self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::ClusterPartialApply,
                        detail: format!(
                            "bulk ingest failed at shard {s}; cluster convergence is unattested"
                        ),
                        error: error.to_string(),
                    });
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// The placement decision for one compiled query — see the module-level table.
    /// Delegates to the free [`placement_of`] so `build` can bucket the corpus before
    /// the cluster value exists.
    fn placement(&self, ex: &Extracted) -> Target {
        placement_of(
            &self.dict,
            &self.ring,
            ex,
            self.per_shard.accept_class_d,
            self.per_shard.hot_anchor_threshold,
        )
    }

    /// True if the cluster holds (or has ever held) any tagged query (ADR-055): the `tags_present`
    /// latch (any tagged write, incl. post-freeze *synthetic* tags never interned into `tag_dict`)
    /// OR a non-empty `tag_dict` (build-time interned tags). Operator introspection only
    /// ([`Self::has_tagged_queries`]) — the vocab rebuild carries tags by stored `TagId` (ADR-074)
    /// and no longer consults this.
    pub(in crate::cluster::coordinator) fn has_tags(&self) -> bool {
        self.tags_present.load(std::sync::atomic::Ordering::Relaxed) || !self.tag_dict.is_empty()
    }

    /// Reject a tag set larger than the per-shard `max_tags` ceiling (ADR-049) at the
    /// cluster front door, BEFORE the mutation reaches the log — so an over-large set
    /// never truncates the shards' u16 tag column (which would silently drop a real tag
    /// and mis-filter). Mirrors the single-node `Engine::check_tag_limit`; conservative
    /// (raw `(key,value)` count, `>=` the post-dedup column count). Replay does not call
    /// this (an acknowledged write must never be dropped on recovery).
    pub(in crate::cluster::coordinator) fn check_tag_limit(
        &self,
        tags: &[(String, String)],
    ) -> Result<(), ParseError> {
        if tags.len() > self.per_shard.max_tags {
            return Err(ParseError::new(ParseErrorKind::TooManyTags, 0));
        }
        Ok(())
    }

    /// Reject a COMPILED query whose required / forbidden / any-of column would overflow
    /// the shards' SoA exact-store `u16` count encoding, BEFORE the mutation reaches the
    /// log — so the truncating cast in `ExactStore::push` is never reached on apply (a
    /// truncated store is a silent false negative). Cluster analogue of the single-node
    /// `Engine::check_column_limit`; runs on the read-only-compiled `Extracted` (after
    /// equivalence expansion). See [`Extracted::column_overflow`](crate::compile::Extracted::column_overflow).
    pub(in crate::cluster::coordinator) fn check_column_limit(
        ex: &crate::compile::Extracted,
    ) -> Result<(), ParseError> {
        if ex.column_overflow().is_some() {
            return Err(ParseError::new(ParseErrorKind::CompiledColumnTooLarge, 0));
        }
        Ok(())
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
        // Reject an over-large tag set BEFORE the log too: it would truncate the u16 tag
        // column on apply and silently drop a real tag. Like a parse error, it carries no
        // replayable mutation (cluster analogue of the single-node front-door gate).
        if let Err(e) = self.check_tag_limit(tags) {
            return Ok(AddOutcome::RejectedParse(e));
        }
        // Classify BEFORE logging (against the CURRENT knob): a REJECTED write — class D with the
        // lane off, or an effectively-empty query — carries no replayable mutation and must NEVER
        // reach the log. Else, replaying it under a since-flipped knob would resurrect a query the
        // caller was told was rejected (codex review). This is the cluster analogue of the
        // single-node "the WAL records only accepted mutations" (ADR-068); the apply/replay funnel
        // then forces accept=true, so replay reproduces the writer's decision regardless of config.
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        // Reject a column-overflowing compiled query before the log too: it would
        // truncate the shards' u16 exact-store counts on apply (a false negative).
        if let Err(e) = Self::check_column_limit(&ex) {
            return Ok(AddOutcome::RejectedParse(e));
        }
        let target = self.placement(&ex);
        if matches!(target, Target::Reject) {
            return Ok(AddOutcome::RejectedClassD);
        }
        let placement = target.placement(self.placement_generation(), self.shards.len() as u32)?;
        // Global lock order is PIT/mutation barrier -> logical stripe. Resync
        // uses the same order; taking the stripe first can deadlock behind a
        // queued exhaustive writer on writer-preferring RwLock implementations.
        // Hold the barrier through the durable append and complete shard fan-out.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // ADR-110's bounded merge requires one live distributed row per logical id.
        // Content-derived placement cannot guarantee a common owner for two different
        // rows sharing an id, so cluster adds are insert-only; replacements use upsert.
        // The stripe closes the same-id check/reservation race without serializing
        // unrelated writes.
        let _logical_guard = self.logical_write_guard(id);
        // A coordinator attached to an already-populated cluster it could not
        // enumerate (the gRPC connect shape) has an unauthoritative directory, so
        // the duplicate check below would be vacuous — fail closed instead of
        // silently admitting a second physical row for a live id (review finding).
        // `upsert_query` stays available: it re-drives replace-by-id on every
        // shard and does not depend on the directory.
        if !self.logical_ids_authoritative() {
            return Err(ShardError::Config(
                "insert-only add_query requires the logical-id directory, which this \
                 coordinator could not seed from its (already-populated) remote shards; \
                 use upsert_query"
                    .to_string(),
            ));
        }
        if self.contains_logical_id(id) {
            return Err(ShardError::DuplicateLogicalId(id));
        }
        let inserted = self.insert_logical_id(id);
        debug_assert!(inserted);
        let m = ClusterMutation::Add {
            logical: id,
            version: 1,
            dsl: dsl.to_string(),
            tags: tags.to_vec(),
            placement: placement.clone(),
        };
        if let Err(e) = self.log.append(&m) {
            self.remove_logical_id(id);
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster add_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_add(id, 1, dsl, tags, &placement)
    }

    /// Atomically replace a query by logical id — ES `index` semantics at the cluster
    /// (ADR-070, the coordinator analogue of the engine's ADR-067 upsert): every prior
    /// live copy is tombstoned and the new version inserted under ONE log frame
    /// ([`ClusterMutation::Upsert`]), so a crash replays the whole replacement or none
    /// of it — never a remove that lost its re-add. Returns the number of prior entries
    /// removed (0 ⇒ created, >0 ⇒ updated) plus where the new version landed. A
    /// rejected new version (parse / class D) **never deletes** — the prior version
    /// stays live and matchable. `version` is the caller-supplied per-logical version
    /// (default 1 from the REST layer); it rides the log frame so replay reproduces the
    /// stored version — passing 1 keeps the in-process / RF=1 path byte-identical.
    pub fn upsert_query(
        &self,
        id: u64,
        dsl: &str,
        version: u32,
    ) -> Result<(usize, AddOutcome), ShardError> {
        self.upsert_query_with_tags(id, dsl, version, &[])
    }

    /// [`upsert_query`](Self::upsert_query) carrying per-query metadata tags for the NEW
    /// version (ADR-055 semantics: raw tags ride the log frame and resolve read-only
    /// against the shared frozen tag space on each target shard). `version` is threaded
    /// into [`ClusterMutation::Upsert`] so a `PUT /_doc/{id} {"version":N}` stores version
    /// N and reopens to N (matching single-node `try_upsert_live_with_tags`).
    pub fn upsert_query_with_tags(
        &self,
        id: u64,
        dsl: &str,
        version: u32,
        tags: &[(String, String)],
    ) -> Result<(usize, AddOutcome), ShardError> {
        // Reject malformed DSL up front: it carries no replayable mutation, so it must
        // never reach the log (a logged record must parse on replay) — and a failed
        // replace never deletes.
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok((0, AddOutcome::RejectedParse(e))),
        };
        // Reject an over-large tag set BEFORE the log (and before any tombstone): it would
        // truncate the u16 tag column on apply. A failed replace never deletes, so this
        // returns 0 replaced — the prior version stays live.
        if let Err(e) = self.check_tag_limit(tags) {
            return Ok((0, AddOutcome::RejectedParse(e)));
        }
        // Classify BEFORE logging (current knob): a rejected new version carries no replayable
        // mutation AND must not delete the prior version, so it never reaches the log or the
        // tombstone pass. Same config-independent-replay discipline as add (codex review): the
        // log holds only accepted mutations, and apply/replay forces accept=true.
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        // Reject a column-overflowing compiled query before the log (and before any
        // tombstone): it would truncate the shards' u16 exact-store counts on apply.
        // A failed replace never deletes, so the prior version stays live (0 replaced).
        if let Err(e) = Self::check_column_limit(&ex) {
            return Ok((0, AddOutcome::RejectedParse(e)));
        }
        let target = self.placement(&ex);
        if matches!(target, Target::Reject) {
            return Ok((0, AddOutcome::RejectedClassD));
        }
        let placement = target.placement(self.placement_generation(), self.shards.len() as u32)?;
        // Keep the same barrier -> logical-stripe order as add/remove/resync.
        // The barrier spans the log append and both delete/insert fan-out passes.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Serialize against an insert-only add/remove for the same id. An upsert
        // keeps the id present; a fresh upsert reserves it before the log append so
        // a concurrent add cannot create a second physical row.
        let _logical_guard = self.logical_write_guard(id);
        let fresh_id = self.insert_logical_id(id);
        let m = ClusterMutation::Upsert {
            logical: id,
            version,
            dsl: dsl.to_string(),
            tags: tags.to_vec(),
            placement: placement.clone(),
        };
        if let Err(e) = self.log.append(&m) {
            if fresh_id {
                self.remove_logical_id(id);
            }
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster upsert_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        self.apply_upsert(id, version, dsl, tags, &placement)
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
        placement: &crate::ownership::QueryPlacement,
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
        let target = placement_of(
            &self.dict,
            &self.ring,
            &ex,
            true,
            self.per_shard.hot_anchor_threshold,
        );
        let expected = target.placement(self.placement_generation(), self.shards.len() as u32)?;
        if &expected != placement {
            return Err(crate::ownership::OwnershipError::PlacementDecisionMismatch.into());
        }
        let (insert_shards, outcome) = match target {
            Target::Reject => return Ok((0, AddOutcome::RejectedClassD)),
            // The broad lane is replicated to every shard (ADR-080); pass 1 already tombstones
            // every shard, so pass 2 re-inserts the new version on every shard.
            Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                ((0..self.shards.len()).collect(), AddOutcome::Replicated)
            }
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
            match self.shards[s]
                .insert_extracted_with_placement(&ex, id, version, dsl, tags, placement)
            {
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
                    placement: placement.clone(),
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
        // Canonical barrier -> logical-stripe order; see add/upsert. Keeping
        // this guard through append + fan-out excludes torn exhaustive/PIT views.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _logical_guard = self.logical_write_guard(id);
        let m = ClusterMutation::Remove { logical: id };
        if let Err(e) = self.log.append(&m) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::WalAppend,
                detail: format!("cluster remove_query(id={id}) not durably logged; rejected"),
                error: e.to_string(),
            });
            return Err(e);
        }
        let removed = self.apply_remove(id);
        // A partially-applied remove keeps the id reserved: allowing a fresh Add
        // before repair could coexist with an old row on the failed shard. Upsert
        // remains available because it re-drives delete+insert on every shard.
        if removed.is_ok() {
            self.remove_logical_id(id);
        }
        removed
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
        placement: &crate::ownership::QueryPlacement,
        success: AddOutcome,
    ) -> Result<AddOutcome, ShardError> {
        let mut applied = Vec::with_capacity(shards.len());
        let mut failed = Vec::new();
        let mut first_err: Option<ShardError> = None;
        for &s in shards {
            match self.shards[s]
                .insert_extracted_with_placement(ex, id, version, dsl, tags, placement)
            {
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
                    placement: placement.clone(),
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
        placement: &crate::ownership::QueryPlacement,
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
        let target = placement_of(
            &self.dict,
            &self.ring,
            &ex,
            true,
            self.per_shard.hot_anchor_threshold,
        );
        let expected = target.placement(self.placement_generation(), self.shards.len() as u32)?;
        if &expected != placement {
            return Err(crate::ownership::OwnershipError::PlacementDecisionMismatch.into());
        }
        let outcome = match target {
            // Defensive: an effectively-empty query is rejected before logging, so a logged
            // mutation never lands here; a replayed no-op (stored nowhere) is still safe.
            Target::Reject => return Ok(AddOutcome::RejectedClassD),
            // The broad lane (class C / B arity-2 / accepted D): replicated to EVERY shard
            // (ADR-080). Same fail-collect fan-out as Selective, so a mid-fan-out remote failure
            // is queued for repair rather than a silent partial. In-process inserts are infallible
            // ⇒ the outcome is byte-identical save that the entry now lands on every shard.
            Target::ReplicatedAlwaysVisible | Target::ReplicatedBroad => {
                let all: Vec<usize> = (0..self.shards.len()).collect();
                self.insert_on_shards(
                    &all,
                    &ex,
                    id,
                    version,
                    dsl,
                    tags,
                    placement,
                    AddOutcome::Replicated,
                )?
            }
            Target::Selective(shards) => self.insert_on_shards(
                &shards,
                &ex,
                id,
                version,
                dsl,
                tags,
                placement,
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
        // Exhaustive cross-shard reads take the exclusive side of the same
        // barrier. A repair re-drive mutates shard visibility just like a live
        // add/upsert/remove and must not slip between sequential shard reads.
        let _pit_barrier = self
            .pit_open_barrier
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            // Serialize the whole per-id re-drive against same-id writers (the
            // same stripe scope the live paths hold), and skip our drained copy
            // when a concurrent writer queued fresher work for this id during
            // the drain — `note_partial` overwrites, so a live map entry is
            // strictly fresher than what we hold.
            let _logical_guard = self.logical_write_guard(logical);
            {
                let guard = self
                    .pending_repair
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if guard.contains_key(&logical) {
                    still_pending += 1;
                    continue;
                }
            }
            let mut still_failed = Vec::new();
            let mut first_err: Option<ShardError> = None;
            for &s in &pr.failed_shards {
                match crate::cluster::shard::apply_mutation(
                    self.shards[s].as_ref(),
                    &self.norm,
                    &self.dict,
                    &pr.mutation,
                    Some(s as u32),
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
                // A converged Remove has now deleted the row everywhere, so the
                // fail-closed reservation retained at the partial-apply point is
                // releasable — without this, the id would 409 every future
                // add_query until a coordinator reopen (review finding).
                if matches!(pr.mutation, ClusterMutation::Remove { .. }) {
                    self.remove_logical_id(logical);
                }
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
                placement,
            } => {
                if !self.insert_logical_id(logical) {
                    return Err(ShardError::DuplicateLogicalId(logical));
                }
                self.apply_add(logical, version, &dsl, &tags, &placement)?;
            }
            ClusterMutation::Remove { logical } => {
                self.apply_remove(logical)?;
                self.remove_logical_id(logical);
            }
            ClusterMutation::Upsert {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => {
                self.insert_logical_id(logical);
                self.apply_upsert(logical, version, &dsl, &tags, &placement)?;
            }
        }
        Ok(())
    }

    /// Seal every shard's memtable into an immutable base segment.
    pub fn flush(&self) -> Result<(), ShardError> {
        for s in &self.shards {
            s.flush()?;
        }
        self.compact_logical_ids();
        Ok(())
    }
}
