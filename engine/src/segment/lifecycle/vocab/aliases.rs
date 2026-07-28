use super::{AliasApplyReport, AliasDiscoveryReport, AliasSummary, Engine};

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

    /// Import a Solr/Lucene synonym file into the registry and apply it live (ADR-060/061):
    /// expressible declared single-token and multi-word groups auto-activate through FN-safe
    /// expansion; mixed/unexpressible groups remain candidates. Classifies against the engine's
    /// CURRENT normalizer + dict, then reuses
    /// the [`set_vocab`](Self::set_vocab) + [`recompile_stale_segments`](Self::recompile_stale_segments)
    /// apply path — no restart, no full rebuild. The registry is merged into the engine's
    /// existing vocabulary (synonyms / phrases / equivalences / punctuation preserved).
    pub fn import_alias_synonyms(
        &mut self,
        solr_text: &str,
    ) -> Result<AliasApplyReport, crate::error::NormalizerError> {
        let mut vocab = self.vocab.as_deref().cloned().unwrap_or_default();
        let before = vocab.aliases().clone();
        let activated = vocab
            .import_solr_aliases(solr_text, &self.norm, &self.dict)
            .map_err(|error| crate::error::NormalizerError::new(error.to_string()))?;
        let changed = vocab.aliases() != &before;
        if !changed {
            return Ok(AliasApplyReport {
                applied: false,
                activated,
                recompiled: 0,
                summary: self.alias_summary(),
            });
        }
        self.set_vocab(vocab)?;
        let recompiled = self.recompile_stale_segments();
        Ok(AliasApplyReport {
            applied: true,
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
            applied: true,
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

    /// Apply match-feedback validation outcomes to the registry (ADR-103): stamp
    /// [`FeedbackEvidence`](crate::vocab::FeedbackEvidence) (confidence reconciles by max)
    /// onto each validated pair, and — only with `activate` — promote them via the
    /// reject-refusing [`activate_validated`](crate::vocab::AliasRegistry::activate_validated)
    /// (an automated pass must never resurrect an operator rejection). Evidence stamping alone
    /// is metadata (the ADR-102 fast path: no epoch bump, no recompile); any actual activation
    /// changes the active groups, so it takes the genuine `set_vocab` + recompile path.
    pub fn apply_alias_feedback(
        &mut self,
        validated: &[(Vec<String>, crate::vocab::FeedbackEvidence)],
        activate: bool,
    ) -> Result<crate::segment::AliasFeedbackApplyReport, crate::error::NormalizerError> {
        let mut vocab = self.vocab.as_deref().cloned().unwrap_or_default();
        let (mut stamped, mut activated_n) = (0usize, 0usize);
        for (forms, evidence) in validated {
            if vocab.aliases_mut().record_feedback(forms, *evidence) {
                stamped += 1;
            }
            if activate && vocab.aliases_mut().activate_validated(forms) {
                activated_n += 1;
            }
        }
        let recompiled = if activated_n > 0 {
            self.set_vocab(vocab)?;
            self.recompile_stale_segments()
        } else {
            self.install_vocab_metadata_only(vocab)?;
            0
        };
        Ok(crate::segment::AliasFeedbackApplyReport {
            stamped,
            activated: activated_n,
            recompiled,
            summary: self.alias_summary(),
        })
    }
}
