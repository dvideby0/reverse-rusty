//! `impl Engine` — runtime vocabulary: querying/recording a [`Vocab`](crate::vocab::Vocab),
//! the stale-epoch bookkeeping, the live-source / live-tag readers, and the
//! recompile-on-vocabulary-change pass ([`recompile_stale_segments`](Engine::recompile_stale_segments))
//! plus the corpus learn-and-apply drivers (ADR-046/053/054).

use crate::segment::{AliasApplyReport, AliasDiscoveryReport, Engine, Segment};
use crate::vocab::AliasSummary;
use std::sync::Arc;

impl Engine {
    /// The vocabulary used to build this engine's normalizer, if one was set.
    pub fn vocab(&self) -> Option<&crate::vocab::Vocab> {
        self.vocab.as_deref()
    }

    /// The governed alias registry (ADR-060), if a vocabulary is installed.
    pub fn aliases(&self) -> Option<&crate::vocab::AliasRegistry> {
        self.vocab.as_deref().map(crate::vocab::Vocab::aliases)
    }

    /// Alias status counts (active / candidate / rejected) for metrics / review (ADR-060 item 9).
    /// `AliasSummary::default()` (all zero) when no vocabulary is installed.
    pub fn alias_summary(&self) -> AliasSummary {
        self.vocab
            .as_deref()
            .map(crate::vocab::Vocab::alias_summary)
            .unwrap_or_default()
    }

    /// Import a Solr/Lucene synonym file into the registry and apply it live (ADR-060): safe
    /// single-token groups auto-activate (FN-safe expansion), multi-word groups are recorded
    /// as candidates. Classifies against the engine's CURRENT normalizer + dict, then reuses
    /// the [`set_vocab`](Self::set_vocab) + [`recompile_stale_segments`](Self::recompile_stale_segments)
    /// apply path — no restart, no full rebuild. The registry is merged into the engine's
    /// existing vocabulary (synonyms / phrases / equivalences / punctuation preserved).
    pub fn import_alias_synonyms(
        &mut self,
        solr_text: &str,
    ) -> Result<AliasApplyReport, crate::error::NormalizerError> {
        let mut vocab = self.vocab.as_deref().cloned().unwrap_or_default();
        let activated = vocab.import_solr_aliases(solr_text, &self.norm, &self.dict);
        self.set_vocab(vocab)?;
        let recompiled = self.recompile_stale_segments();
        Ok(AliasApplyReport {
            activated,
            recompiled,
            summary: self.alias_summary(),
        })
    }

    /// Learn alias candidates from the engine's OWN stored queries (any-of co-occurrence) into
    /// the registry and apply (ADR-060 item 2). Conservative: only clear single-token variants
    /// auto-activate; multi-word, multi-form category alternatives, and mixed-kind groups land
    /// as review candidates. Returns the apply report.
    pub fn learn_aliases_and_apply(
        &mut self,
        min_count: usize,
    ) -> Result<AliasApplyReport, crate::error::NormalizerError> {
        let corpus = self.live_sources();
        let mut vocab = self.vocab.as_deref().cloned().unwrap_or_default();
        let activated =
            vocab.learn_aliases_from_queries(&corpus, min_count, &self.norm, &self.dict);
        self.set_vocab(vocab)?;
        let recompiled = self.recompile_stale_segments();
        Ok(AliasApplyReport {
            activated,
            recompiled,
            summary: self.alias_summary(),
        })
    }

    /// Discover distributional alias candidates over the engine's OWN stored queries
    /// (ADR-102) — compute-only: nothing is recorded or changed. See
    /// [`crate::vocab::discover_pairs`] for the signal + noise model.
    pub fn discover_aliases(
        &self,
        cfg: &crate::vocab::DistributionalConfig,
    ) -> Vec<crate::vocab::DiscoveredPair> {
        crate::vocab::discover_pairs(&self.live_sources(), cfg)
    }

    /// [`discover_aliases`](Self::discover_aliases), then record every proposal into the
    /// registry as a review `Candidate` (`LearnedDistributional` provenance — NEVER
    /// auto-active, ADR-102) and install the updated vocabulary through the metadata-only
    /// seam: candidates change no matching-relevant state, so there is no epoch bump and no
    /// recompile — match results are byte-identical before/after. Like every single-node
    /// runtime vocab mutation, durability is the operator's vocab file (`GET /_vocab` → save;
    /// a cluster checkpoint embeds the vocab in its manifest).
    pub fn discover_aliases_and_record(
        &mut self,
        cfg: &crate::vocab::DistributionalConfig,
    ) -> Result<AliasDiscoveryReport, crate::error::NormalizerError> {
        let pairs = self.discover_aliases(cfg);
        let mut vocab = self.vocab.as_deref().cloned().unwrap_or_default();
        let (new_candidates, rediscovered, rejected_sticky) =
            vocab.record_distributional_candidates(&pairs, &self.norm, &self.dict);
        self.install_vocab_metadata_only(vocab)?;
        Ok(AliasDiscoveryReport {
            proposed: pairs.len(),
            new_candidates,
            rediscovered,
            rejected_sticky,
            summary: self.alias_summary(),
        })
    }

    /// Install a vocabulary whose **matching-relevant projections are unchanged** — the
    /// metadata-only seam (ADR-102). The shipped apply path ([`set_vocab`](Self::set_vocab))
    /// unconditionally bumps `vocab_epoch` and recompiles the corpus; for a change that only
    /// adds/edits registry *candidates* (which `effective_equivalence_groups` +
    /// `active_alias_forms` cannot see) that is an O(corpus) no-op. This seam structurally
    /// verifies the invariant instead of trusting the caller: equal projections ⇒ swap the Arc
    /// (no epoch bump, no normalizer rebuild, no recompile); unequal (unreachable from the
    /// candidate-only paths — belt-and-braces) ⇒ fall back to the full `set_vocab` +
    /// `recompile_stale_segments`, so the fast path can never cause a false negative. Returns
    /// `true` when the fast path was taken.
    pub fn install_vocab_metadata_only(
        &mut self,
        vocab: crate::vocab::Vocab,
    ) -> Result<bool, crate::error::NormalizerError> {
        let current = self.vocab.as_deref().cloned().unwrap_or_default();
        if vocab.effective_equivalence_groups() == current.effective_equivalence_groups()
            && vocab.aliases().active_alias_forms() == current.aliases().active_alias_forms()
        {
            self.vocab = Some(Arc::new(vocab));
            Ok(true)
        } else {
            self.set_vocab(vocab)?;
            self.recompile_stale_segments();
            Ok(false)
        }
    }

    /// Replace the engine's vocabulary and normalizer. Existing compiled
    /// queries become stale — the caller must reingest for consistent matching.
    /// Returns the number of stale segments that need reingestion.
    pub fn set_vocab(
        &mut self,
        mut vocab: crate::vocab::Vocab,
    ) -> Result<usize, crate::error::NormalizerError> {
        let mut norm = Arc::new(vocab.to_normalizer()?);
        // Resolve any declared/learned equivalence groups against the dict under the new
        // normalizer and install them, so the subsequent recompile (and future inserts)
        // expand queries through them (ADR-054). First intern every active equivalence form
        // into the (mutable) dict so a later insert can't mint a different dense id for a form
        // that would otherwise resolve to a synthetic id — the alias-ID-stability fix
        // (ADR-060). No groups ⇒ both are no-ops (the dict clone is dwarfed by the recompile
        // this set_vocab triggers).
        let dict = Arc::make_mut(&mut self.dict);
        // Self-heal first (codex R13): a vocabulary mutation (punct refold, new grader, …) can
        // make an Active alias form unexpressible under the NEW normalizer; demote those back
        // to review candidates rather than leaving an alias that reports active and silently
        // never matches. Demotion can shrink the registered phrase set, so rebuild on change.
        if vocab.aliases_mut().demote_unexpressible(&norm, dict) > 0 {
            norm = Arc::new(vocab.to_normalizer()?);
        }
        vocab.intern_equivalence_forms(&norm, dict);
        let equiv = vocab.resolve_equivalences(&norm, dict);
        dict.set_equivalences(equiv);
        self.norm = norm;
        self.vocab = Some(Arc::new(vocab));
        self.vocab_epoch += 1;
        Ok(self.stale_segment_count())
    }

    /// Number of base segments compiled against an older vocab epoch.
    pub fn stale_segment_count(&self) -> usize {
        let current = self.vocab_epoch;
        self.segments
            .iter()
            .filter(|s| s.vocab_epoch() < current)
            .count()
            + usize::from(self.memtable.vocab_epoch < current && !self.memtable.is_empty())
    }

    /// True if any segment was compiled with a different normalizer than the
    /// current one. Matching still works (no panic) but may produce incorrect
    /// results until stale queries are reingested.
    pub fn has_stale_segments(&self) -> bool {
        self.stale_segment_count() > 0
    }

    /// The current vocab epoch. Segments compiled at this epoch are up-to-date.
    pub fn vocab_epoch(&self) -> u64 {
        self.vocab_epoch
    }

    /// Record a vocabulary on an engine that is ALREADY consistent with it,
    /// WITHOUT recompiling or bumping the epoch. Used at startup after
    /// [`open`](Self::open): the engine was opened with this vocab's normalizer,
    /// so its segments already align with it and only the [`Vocab`](crate::vocab::Vocab)
    /// object needs installing (so `GET /_vocab` can serve it). Unlike
    /// [`set_vocab`](Self::set_vocab) — which signals a normalizer *change* by
    /// bumping the epoch and marking segments stale — this is a pure metadata
    /// record. Use [`set_vocab`] + [`recompile_stale_segments`](Self::recompile_stale_segments)
    /// to actually *change* the vocabulary at runtime.
    pub fn adopt_vocab(
        &mut self,
        mut vocab: crate::vocab::Vocab,
    ) -> Result<(), crate::error::NormalizerError> {
        // WAL-tail hazard (codex R13): `Engine::open` replays the WAL tail BEFORE any vocab is
        // installed, and the `EquivMap` is transient (never persisted in the dict) — so a
        // recovered memtable was recompiled WITHOUT this vocab's equivalence expansion, and a
        // pure metadata adopt would leave those queries unexpanded (a recovery false negative:
        // a replayed `new york mets` query no longer reaches a `ny mets` title). When both the
        // hazard ingredients are present, escalate to the genuine-change path — `set_vocab` +
        // `recompile_stale_segments` re-extracts every live query under the installed
        // equivalences. Prefer [`open_with_vocab`](Self::open_with_vocab), which installs the
        // equivalences BEFORE replay and keeps this adopt a pure metadata record.
        if !self.memtable.is_empty() && !vocab.effective_equivalence_groups().is_empty() {
            self.set_vocab(vocab)?;
            self.recompile_stale_segments();
            return Ok(());
        }
        let mut norm = Arc::new(vocab.to_normalizer()?);
        // Re-install equivalence groups (ADR-054/060) so inserts after this point expand through
        // them. The ID-stability question turns on whether any query is already compiled:
        //
        //   * **Fresh engine** (no segments, empty memtable — e.g. a persistent server started on a
        //     new/empty data dir with a vocab file): there is nothing to desync, so intern the
        //     active forms FIRST, pinning each to a dense id so the first live `PUT /_doc` (mutating
        //     extract) resolves the SAME id the `EquivMap` is keyed by. Without this the map is
        //     synthetic-keyed and the alias dies on the first dense insert (ADR-060).
        //   * **Recovered engine** (segments/memtable present): the already-compiled queries baked
        //     their ids against the persisted dict, so resolve AS-IS and do NOT intern — a form they
        //     resolved synthetic must keep resolving synthetic, or the title side would resolve it
        //     dense and miss those queries (an upgrade FN). A new-code index already has its active
        //     forms interned dense in the persisted dict, so they resolve dense and stay consistent.
        //     A genuine runtime vocabulary *change* (intern + recompile) goes through `set_vocab` +
        //     `recompile_stale_segments`, not this adopt path.
        let fresh = self.segments.is_empty() && self.memtable.is_empty();
        let dict = Arc::make_mut(&mut self.dict);
        // Self-heal stale-active aliases against the live normalizer (codex R13, see set_vocab).
        if vocab.aliases_mut().demote_unexpressible(&norm, dict) > 0 {
            norm = Arc::new(vocab.to_normalizer()?);
        }
        if fresh {
            vocab.intern_equivalence_forms(&norm, dict);
        }
        let equiv = vocab.resolve_equivalences(&norm, dict);
        dict.set_equivalences(equiv);
        self.norm = norm;
        self.vocab = Some(Arc::new(vocab));
        Ok(())
    }

    /// The current live `(logical_id, query_text)` set — the source corpus the
    /// index is a materialized view of, sorted by logical id for deterministic
    /// rebuilds. Backed by the query store (kept in sync with the index by the
    /// insert/delete paths) and **cross-checked against index liveness**: a store
    /// entry with no live copy in this engine is stale residue (e.g. a query a
    /// pre-fix green rebuild moved to another shard — codex retro-review, ADR-074)
    /// and is skipped, so a polluted `sources.dat` self-heals at the next gather
    /// rather than resurrecting moved or deleted queries.
    /// Used by [`recompile_stale_segments`](Self::recompile_stale_segments).
    pub fn live_sources(&self) -> Vec<(u64, String)> {
        let mut out: Vec<(u64, String)> = Vec::with_capacity(self.query_store.len());
        self.query_store.for_each_live(|logical, text| {
            if self.live_tag_ids_for(logical).is_some() {
                out.push((logical, text.to_string()));
            }
        });
        out.sort_unstable_by_key(|&(l, _)| l);
        out
    }

    /// [`live_sources`](Self::live_sources) plus each live query's current `TagId`s — the
    /// gather behind the CLUSTER blue/green rebuild (`ClusterEngine::set_vocab`, ADR-074),
    /// which re-places every query and must carry its tags to the new shard. Ids — interned
    /// dense or post-freeze synthetic — are carried verbatim: the tag space is preserved
    /// across a vocabulary change, so they stay valid (the same ADR-049 carry-through
    /// [`recompile_stale_segments`](Self::recompile_stale_segments) uses in-place).
    pub fn live_sources_tagged(&self) -> Vec<(u64, String, u32, Vec<crate::tagdict::TagId>)> {
        let mut out: Vec<(u64, String, u32, Vec<crate::tagdict::TagId>)> =
            Vec::with_capacity(self.query_store.len());
        // One liveness scan per entry: `None` = no live copy in this engine (stale store
        // residue — skipped, see `live_sources`), `Some((version, tags))` = live, possibly
        // untagged. The version is the live copy's stored version, carried through the rebuild
        // so a `set_vocab`/resize re-places at version N rather than resetting to 1 (ADR-074).
        self.query_store.for_each_live(|logical, text| {
            if let Some((version, tags)) = self.live_tag_ids_for(logical) {
                out.push((logical, text.to_string(), version, tags));
            }
        });
        out.sort_unstable_by_key(|&(l, ..)| l);
        out
    }

    /// The current `TagId`s of the live entry for `logical` (ADR-049), read from the
    /// memtable or a base segment. Used by [`recompile_stale_segments`] to carry a
    /// query's tags through a vocabulary change unchanged (same tag space ⇒ the ids stay
    /// valid), and by the gathers above as the index-liveness check. `None` when the
    /// query has NO live copy in this engine (distinct from `Some(vec![])` — live but
    /// untagged): conflating the two is exactly what let a stale store entry shadow a
    /// moved query's tagged copy (codex retro-review, ADR-074).
    fn live_tag_ids_for(&self, logical: u64) -> Option<(u32, Vec<crate::tagdict::TagId>)> {
        for &local in self.memtable.locals_for_logical(logical) {
            if self.memtable.is_alive(local) {
                return Some((
                    self.memtable.version_of(local),
                    self.memtable.tags_of(local).to_vec(),
                ));
            }
        }
        for seg in &self.segments {
            for &local in seg.locals_for_logical(logical) {
                if seg.is_alive(local) {
                    return Some((seg.version_of(local), seg.tags_of(local).to_vec()));
                }
            }
        }
        None
    }

    /// Recompile every live query under the CURRENT normalizer, replacing all
    /// base segments (and the memtable) with one freshly-compiled segment at the
    /// current vocab epoch. This is the recompile pass that makes a normalizer
    /// change ([`set_vocab`](Self::set_vocab)) actually take effect on
    /// already-ingested queries: without it, segments compiled under the old
    /// normalizer carry stale feature ids, and a title normalized with the new
    /// normalizer can miss them — a **false negative**.
    ///
    /// Queries are recompiled READ-ONLY against the existing (frozen) dict via
    /// [`extract_readonly`](crate::compile::extract_readonly): a declared alias
    /// collapses both surface forms to one feature (so both now match), and a new
    /// alias canonical that isn't interned resolves to a stable synthetic id
    /// (mechanism 1). The dict's feature space is unchanged.
    ///
    /// A no-op (returns 0) when nothing is stale; after it, `has_stale_segments()`
    /// is false. Returns the number of queries recompiled.
    ///
    /// Atomicity: a caller that publishes snapshots (e.g. the server) must call
    /// this **before** publishing the next snapshot, so readers never observe the
    /// new normalizer against not-yet-recompiled segments.
    pub fn recompile_stale_segments(&mut self) -> usize {
        if !self.has_stale_segments() {
            return 0;
        }
        // Recompile the live source set read-only against the frozen dict under
        // the current normalizer into one fresh segment.
        let live = self.live_sources();
        let mut seg = Segment::new();
        seg.vocab_epoch = self.vocab_epoch;
        let mut lc = String::new();
        let mut recompiled = 0usize;
        for (logical, text) in &live {
            if let Ok(ast) = crate::dsl::parse(text) {
                let ex = crate::compile::extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
                // Carry the query's existing tags AND stored version forward unchanged —
                // both are orthogonal to the normalizer, so a vocabulary change must not
                // drop the tags (ADR-049) nor reset the version to 1 (the version-preserving
                // rebuild, ADR-074). `live_sources` only returns entries with a live copy, so
                // the lookup cannot be `None` here; the default is unreachable belt-and-braces.
                let (version, tags) = self.live_tag_ids_for(*logical).unwrap_or((1, Vec::new()));
                // `accept_class_d = true` unconditionally (ADR-068): a STORED query
                // must survive a vocabulary change. A query whose positives vanish
                // under the new vocab (re-classifying to D) is kept as an
                // always-candidate when it still has forbidden features — zero FN,
                // bounded FP — instead of being silently dropped from the rebuilt
                // index (the pre-existing hazard); only the all-empty case drops.
                if seg
                    .add_compiled(&ex, &tags, &self.dict, *logical, version, true)
                    .is_some()
                {
                    recompiled += 1;
                }
            }
        }
        seg.build_filter();

        // Atomic swap: drop every (stale) base segment + the memtable and install
        // the one freshly-compiled segment, so no live query is left at an old
        // epoch. Old segment files are GC'd after the manifest commit.
        let old_files = self.collect_mmap_paths();
        self.segments.clear();
        let mut fresh_mem = Segment::new();
        fresh_mem.vocab_epoch = self.vocab_epoch;
        self.memtable = Arc::new(fresh_mem);
        let persisted = self.seal_and_push(seg);

        // Persist like a flush, but FAIL CLOSED (ADR-051): only retire the old
        // segment files and advance the WAL (checkpoint marks the live queries
        // materialized, reset truncates them) once the freshly-compiled segment is
        // durably on disk AND the manifest — the commit point referencing it — has
        // been written. We just cleared the old segments from the vec, so if the
        // recompiled segment did NOT persist, deleting the old files or resetting
        // the WAL would erase the only durable copy of the whole corpus. Leaving
        // both intact lets a restart recover the pre-recompile state and re-apply
        // the vocab change. The recompiled segment is still served from memory
        // meanwhile; `persistence_healthy` is false to signal the degraded state.
        if persisted && self.save_manifest_if_persistent() {
            self.checkpoint_wal();
            self.reset_wal_if_safe();
            self.cleanup_segment_files(&old_files);
        }
        recompiled
    }

    /// Learn alias/synonym rules from this engine's live corpus (ADR-015 any-of learning)
    /// and apply them (ADR-046 mechanism 2): a synonym appearing in at least `min_count`
    /// any-of groups (e.g. `(rookie,rc)` ⇒ `rc → rookie`) is merged UNDER the current
    /// vocabulary (a previously set alias wins) and the index is recompiled so the change
    /// takes effect immediately. Returns the number of queries recompiled.
    ///
    /// A thin wrapper over [`learn_and_apply_with`](Self::learn_and_apply_with) with NPMI
    /// corpus phrase induction disabled — behaviorally unchanged.
    pub fn learn_and_apply(
        &mut self,
        min_count: usize,
    ) -> Result<usize, crate::error::NormalizerError> {
        self.learn_and_apply_with(&crate::vocab::CorpusLearnConfig {
            anyof_min_count: min_count,
            ..Default::default()
        })
    }

    /// Like [`learn_and_apply`](Self::learn_and_apply) but also runs opt-in **NPMI corpus
    /// phrase induction** when `cfg.corpus_phrases` is set (ADR-053): multi-token entities
    /// induced from the live query text (e.g. `upper deck`) are merged UNDER the current
    /// vocabulary (a declared alias/phrase wins on a token collision) and the index is
    /// recompiled. With `corpus_phrases = false` this is identical to
    /// `learn_and_apply(cfg.anyof_min_count)`. Phrases only — never aliases — so the
    /// same-normalizer gluing is lossless-cover safe (zero false negatives). Returns the
    /// number of queries recompiled.
    pub fn learn_and_apply_with(
        &mut self,
        cfg: &crate::vocab::CorpusLearnConfig,
    ) -> Result<usize, crate::error::NormalizerError> {
        let corpus = self.live_sources();
        let learned = crate::vocab::learn_vocab_from_corpus(&corpus, cfg);
        let mut merged = crate::vocab::Vocab::new();
        if let Some(v) = &self.vocab {
            merged.merge(v);
        }
        merged.merge(&learned);
        self.set_vocab(merged)?; // bumps the epoch / marks segments stale
        Ok(self.recompile_stale_segments())
    }
}
