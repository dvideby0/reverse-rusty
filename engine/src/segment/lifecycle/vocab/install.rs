use super::{Arc, Engine};

impl Engine {
    /// Install a vocabulary whose **matching-relevant state is provably unchanged** — the
    /// metadata-only seam (ADR-102). The shipped apply path ([`set_vocab`](Self::set_vocab))
    /// unconditionally bumps `vocab_epoch` and recompiles the corpus; for a change that only
    /// adds/edits registry *candidates* that is an O(corpus) no-op. The fast path requires BOTH
    /// (structurally verified — never trusted from the caller):
    ///
    /// 1. **Everything outside the alias registry is byte-identical** — compared over the
    ///    serialized vocab documents with the registries blanked, so synonyms, phrases,
    ///    punctuation, number-context, declared equivalences, AND any future `Vocab`
    ///    field automatically participate (a field-list comparison would silently rot; codex
    ///    review). A vocab differing anywhere there affects the normalizer and must go through
    ///    the genuine-change path.
    /// 2. **The registry's matching-relevant projections are equal** —
    ///    `effective_equivalence_groups` + `active_alias_forms` (candidate/rejected entries are
    ///    invisible to both).
    ///
    /// Equal ⇒ swap the Arc (no epoch bump, no normalizer rebuild, no recompile). Anything else
    /// ⇒ fall back to the full `set_vocab` + `recompile_stale_segments`, so the fast path can
    /// never leave the advertised vocab out of sync with the live normalizer. `pub(crate)`
    /// deliberately: callers outside the crate go through `set_vocab` (the general contract) or
    /// the discovery/feedback recording paths that funnel here. Returns `true` when the fast
    /// path was taken.
    pub(crate) fn install_vocab_metadata_only(
        &mut self,
        vocab: crate::vocab::Vocab,
    ) -> Result<bool, crate::error::NormalizerError> {
        let current = self.vocab.as_deref().cloned().unwrap_or_default();
        let outside_registry_identical = {
            let mut a = vocab.clone();
            let mut b = current.clone();
            *a.aliases_mut() = crate::vocab::AliasRegistry::default();
            *b.aliases_mut() = crate::vocab::AliasRegistry::default();
            // Serialization of an in-memory Vocab cannot fail; if it ever did, `None != None`
            // is avoided by mapping to unequal sentinels — the conservative direction (take
            // the full-recompile path, never the skip).
            match (serde_json::to_string(&a), serde_json::to_string(&b)) {
                (Ok(ja), Ok(jb)) => ja == jb,
                _ => false,
            }
        };
        if outside_registry_identical
            && vocab.effective_equivalence_groups() == current.effective_equivalence_groups()
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
        // A prior durability failure means this process may already be serving
        // state that cannot be made the next durable base. Do not install
        // another title normalizer over it: callers publish the engine snapshot
        // after this method returns, and stale exact plans under a new
        // normalizer are a false-negative risk.
        if self.config.data_dir.is_some() && !self.persistence_healthy {
            return Err(crate::error::NormalizerError::new(
                "cannot change vocabulary while persistence is unhealthy; repair or restart \
                 from the last committed state first",
            ));
        }
        // A vocabulary change must rebuild from canonical source. Validate the
        // source/exact pairing BEFORE mutating the normalizer, dict, or epoch;
        // otherwise a stale sidecar could either be recompiled as truth or make
        // the later gather abort after the live normalizer had already changed.
        let live = self.live_source_documents_tagged().map_err(|logical| {
            crate::error::NormalizerError::new(format!(
                "source metadata for logical id {logical} does not match its live exact row"
            ))
        })?;
        let mut norm = Arc::new(vocab.to_normalizer()?);
        // Resolve any declared/learned equivalence groups against the dict under the new
        // normalizer and install them, so the subsequent recompile (and future inserts)
        // expand queries through them (ADR-054). First intern every active equivalence form
        // into the (mutable) dict so a later insert can't mint a different dense id for a form
        // that would otherwise resolve to a synthetic id — the alias-ID-stability fix
        // (ADR-060). No groups ⇒ both are no-ops (the dict clone is dwarfed by the recompile
        // this set_vocab triggers). Build the candidate dict off to the side so a rejected
        // vocabulary cannot partially mutate the live feature space.
        let mut proposed_dict = self.dict.as_ref().clone();
        // Self-heal first (codex R13): a vocabulary mutation such as a punctuation refold can
        // make an Active alias form unexpressible under the NEW normalizer; demote those back
        // to review candidates rather than leaving an alias that reports active and silently
        // never matches. Demotion can shrink the registered phrase set, so rebuild on change.
        if vocab
            .aliases_mut()
            .demote_unexpressible(&norm, &proposed_dict)
            > 0
        {
            norm = Arc::new(vocab.to_normalizer()?);
        }
        vocab.intern_equivalence_forms(&norm, &mut proposed_dict);
        let equiv = vocab.resolve_equivalences(&norm, &proposed_dict);
        proposed_dict.set_equivalences(equiv);

        // `set_vocab` and `recompile_stale_segments` are intentionally separate public
        // operations, but installing a normalizer that cannot represent every acknowledged
        // source would make that split unsafe: the later recompile would abort with the old
        // segments still installed, while callers such as PUT /_vocab would publish the new
        // title normalizer and create false negatives. Preflight the exact rejection
        // conditions against the proposed state before committing any of it.
        let mut lc = String::new();
        for (logical, text, ..) in &live {
            let ast = crate::dsl::parse_for_recovery(text).map_err(|error| {
                crate::error::NormalizerError::new(format!(
                    "stored query for logical id {logical} cannot be rebuilt: {error}"
                ))
            })?;
            let ex = crate::compile::extract_readonly(&ast, &norm, &proposed_dict, &mut lc);
            if let Some(width) = ex.column_overflow() {
                return Err(crate::error::NormalizerError::new(format!(
                    "vocabulary change expands stored query for logical id {logical} \
                     beyond the exact-store column limit ({width} features)"
                )));
            }
            let class =
                crate::compile::anchor_plan(&ex, &proposed_dict, self.config.hot_anchor_threshold)
                    .class;
            if super::super::super::seg::rejects_class_d(class, &ex, true) {
                return Err(crate::error::NormalizerError::new(format!(
                    "vocabulary change makes stored query for logical id {logical} \
                     effectively empty"
                )));
            }
        }

        self.norm = norm;
        self.vocab = Some(Arc::new(vocab));
        self.dict = Arc::new(proposed_dict);
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

    /// Record a vocabulary on an engine opened with its normalizer. For an empty
    /// engine or a vocabulary without equivalence groups this is metadata-only.
    /// For a non-empty recovered engine with equivalences it conservatively
    /// recompiles: the equivalence map is transient, and a prior compiler
    /// migration may have durably rebuilt the rows before the vocabulary was
    /// adopted. Prefer [`open_with_vocab`](Self::open_with_vocab), which installs
    /// equivalences before WAL replay/migration and avoids this extra rebuild.
    pub fn adopt_vocab(
        &mut self,
        mut vocab: crate::vocab::Vocab,
    ) -> Result<(), crate::error::NormalizerError> {
        // Recovery hazard (codex R13 + ADR-118): `Engine::open` replays the WAL and may migrate
        // legacy segments BEFORE any vocab is installed. The `EquivMap` is transient, so either
        // materialization can omit required-to-any-of expansion. There is deliberately no
        // process-local shortcut here: the process may stop after committing the migration but
        // before adoption. Conservatively rebuild every non-empty recovered corpus when the
        // adopted vocabulary has equivalences. `open_with_vocab` avoids the extra pass.
        let equivalences_present = !vocab.effective_equivalence_groups().is_empty();
        if equivalences_present && self.num_live_queries() != 0 {
            let expected = self
                .live_source_documents_tagged()
                .map_err(|logical| {
                    crate::error::NormalizerError::new(format!(
                        "source metadata for logical id {logical} does not match its live exact row"
                    ))
                })?
                .len();
            self.set_vocab(vocab)?;
            let rebuilt = self.recompile_stale_segments();
            if rebuilt != expected
                || self.has_stale_segments()
                || (self.config.data_dir.is_some() && !self.persistence_healthy)
            {
                return Err(crate::error::NormalizerError::new(format!(
                    "equivalence-aware vocabulary adoption did not commit completely \
                     (expected {expected} live queries, rebuilt {rebuilt}, \
                     persistence_healthy={})",
                    self.persistence_healthy
                )));
            }
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
}
