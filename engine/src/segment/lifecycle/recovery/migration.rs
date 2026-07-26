use super::{Arc, Engine};

impl Engine {
    /// Whether any live row uses an older AST→compiled-query lowering.
    pub(crate) fn has_legacy_compiler_segments(&self) -> bool {
        let current = crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION;
        self.segments.iter().any(|segment| {
            segment.alive_count() != 0 && segment.compiler_semantics_version() < current
        }) || (!self.memtable.is_empty() && self.memtable.compiler_semantics_version() < current)
    }

    /// Whether serving this engine requires the ADR-118/119/120/#123 source-driven
    /// compiler migration. Every live row below the current stamp is suspect;
    /// only recompilation from the retained DSL can recover clause boundaries,
    /// any-of member boundaries, quoted adjacency, and complete forbidden terms.
    pub(crate) fn needs_compiler_semantics_migration(&self) -> bool {
        self.has_legacy_compiler_segments()
    }

    /// Standalone upgrade path for ADR-118/119/120/#123. The normalizer and dict do not
    /// change, but every live source must be re-lowered so clause and any-of
    /// member boundaries, quoted adjacency, and complete forbidden terms are reflected in exact
    /// predicates, signatures, and placement.
    pub(crate) fn migrate_legacy_compiler_semantics(&mut self) -> std::io::Result<()> {
        if !self.needs_compiler_semantics_migration() {
            return Ok(());
        }
        if !self.owns_manifest {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot migrate legacy compiler semantics inside one cluster shard: query \
                 placement must be rebuilt and committed by the coordinator",
            ));
        }
        if !self.persistence_healthy || self.skipped_segments != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cannot migrate legacy compiler semantics from a degraded recovery \
                     (persistence_healthy={}, skipped_segments={}): repair the committed \
                     segment set first",
                    self.persistence_healthy, self.skipped_segments
                ),
            ));
        }
        if let Some(logical) = self.duplicate_live_logical_id() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cannot migrate legacy compiler semantics: live query {logical} has multiple \
                     physical predicates but only one canonical source document"
                ),
            ));
        }

        let live = self.live_source_documents_tagged().map_err(|logical| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cannot migrate legacy compiler semantics: live query {logical} does not \
                     have exactly one matching retained source document"
                ),
            )
        })?;

        // A legacy joint stream may have consumed component features that the
        // persisted standalone dict never interned. Recompiling read-only would
        // freeze those newly exposed names as synthetic IDs; a later ordinary
        // standalone insert would intern the same name densely, making titles
        // resolve dense while the migrated row still required synthetic. Build
        // an append-only candidate dict off to the side by running the current
        // mutable extractor over the complete live corpus. Existing IDs and
        // frozen mask bits stay fixed. Existing frequencies are restored after
        // the discovery pass; newly exposed features retain their corpus counts.
        let mut proposed_dict = self.dict.as_ref().clone();
        let old_len = proposed_dict.len();
        let old_freqs: Vec<u32> = (0..old_len)
            .map(|id| proposed_dict.freq(id as crate::dict::FeatureId))
            .collect();
        let old_masks: Vec<u8> = (0..old_len)
            .map(|id| proposed_dict.mask_bit(id as crate::dict::FeatureId))
            .collect();
        let mut lc = String::new();
        for (logical, text, ..) in &live {
            let ast = crate::dsl::parse_for_recovery(text).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "cannot migrate legacy compiler semantics: stored query {logical} \
                         no longer parses: {error}"
                    ),
                )
            })?;
            let ex = crate::compile::extract(&ast, &self.norm, &mut proposed_dict, &mut lc);
            if let Some(width) = ex.column_overflow() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "cannot migrate legacy compiler semantics: stored query {logical} \
                         exceeds the exact-store column limit ({width} features)"
                    ),
                ));
            }
        }
        for id in 0..old_len {
            proposed_dict.set_freq_and_mask(
                id as crate::dict::FeatureId,
                old_freqs[id],
                old_masks[id],
            );
        }
        // Newly interned equivalence members must be keyed by their new dense
        // IDs before the read-only materialization pass below.
        if let Some(vocab) = self.vocab.as_deref() {
            let equiv = vocab.resolve_equivalences(&self.norm, &proposed_dict);
            proposed_dict.set_equivalences(equiv);
        }
        self.dict = Arc::new(proposed_dict);

        let previous_epoch = self.vocab_epoch;
        self.vocab_epoch = self.vocab_epoch.checked_add(1).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot migrate legacy compiler semantics: vocab epoch exhausted",
            )
        })?;
        let rebuilt = self.recompile_stale_segments();

        if rebuilt != live.len() || self.has_legacy_compiler_segments() || !self.persistence_healthy
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "legacy compiler-semantics migration did not commit completely \
                     (expected {} live queries, rebuilt {rebuilt}, persistence_healthy={})",
                    live.len(),
                    self.persistence_healthy
                ),
            ));
        }

        // The epoch was only a process-local trigger for a same-normalizer
        // compiler migration. Restore it so introspection does not report a
        // vocabulary change; the durable idempotency marker is the segment's
        // compiler-semantics header word.
        self.vocab_epoch = previous_epoch;
        Arc::make_mut(&mut self.memtable).vocab_epoch = previous_epoch;
        for segment in &mut self.segments {
            Arc::make_mut(segment).set_vocab_epoch(previous_epoch);
        }
        Ok(())
    }
}
