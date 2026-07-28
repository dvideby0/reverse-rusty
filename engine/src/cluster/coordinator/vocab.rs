//! `impl ClusterEngine` — runtime vocabulary change (ADR-046 mechanism 2).
//!
//! A vocabulary change (e.g. a declared alias `ns ≡ northstar`) swaps the ONE
//! shared normalizer and rebuilds the cluster from its live source set: every
//! query is re-extracted under the new normalizer, **re-placed** (an alias can
//! change a query's anchor → hence its shard), and re-ingested. This is a
//! "blue/green rebuild from the log" (ADR-004): the dict is re-minted over the
//! live corpus so feature frequencies/hotness reflect the post-change
//! distribution, exactly as [`ClusterEngine::build`] does.
//!
//! The swap is atomic under `&mut self` (no reader observes a half-state — reads
//! take `&self`), so both surface forms of an alias resolve to one feature with
//! **zero false negatives**.
//!
//! **In-process only.** An alias is a normalizer operation and is NOT shipped to a
//! `RemoteShard` in v1, so [`ClusterEngine::set_vocab`] refuses a non-local cluster
//! (a remote shard would keep normalizing under the stale normalizer — a silent
//! cross-process false negative the dict-fingerprint handshake cannot catch, since
//! the alias does not change the interned-name set).
//!
//! **Per-query tags survive the rebuild (ADR-074).** The tag space is orthogonal to
//! vocabulary and preserved unchanged, so each query's stored `TagId`s — interned
//! dense or post-freeze *synthetic* (which have no recoverable string) — are gathered
//! alongside its DSL and carried verbatim to wherever re-placement puts it: the
//! cluster analogue of the single-node ADR-049 carry-through in
//! `Engine::recompile_stale_segments`.

use std::collections::BTreeMap;
use std::sync::{atomic::Ordering, Arc};

use crate::vocab::{CorpusLearnConfig, Vocab};

use super::{ClusterEngine, CLUSTER_MANIFEST_FILE};
use crate::cluster::control::ClusterStateChange;
use crate::cluster::shard::ShardError;

type LiveTaggedMetadata = (
    String,
    u32,
    u64,
    Vec<(String, String)>,
    Vec<crate::tagdict::TagId>,
    crate::rank::RankValues,
    crate::ownership::QueryPlacement,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AliasImportManifestState {
    Committed,
    PublishedCurrent(u64),
    ImmediatePredecessor,
}

impl ClusterEngine {
    /// Change the cluster's vocabulary (ADR-046 mechanism 2) — e.g. declare an
    /// alias so two surface forms match. Rebuilds the cluster from its live source
    /// set under the new normalizer: re-mints the shared dict, re-places every
    /// query (an alias can move a query's anchor, hence its shard), and re-ingests —
    /// carrying each query's stored tags with it (ADR-074; the tag space is
    /// preserved unchanged). Atomic under `&mut self`; a durable cluster commits the
    /// rebuild via [`checkpoint`](Self::checkpoint). Returns the number of live
    /// queries rebuilt.
    ///
    /// Refuses (errors) if any shard is non-local or handoff-wrapped. A vocabulary
    /// that activates a multi-word alias is supported (ADR-076: P(T)-aware routing).
    pub fn set_vocab(&mut self, vocab: Vocab) -> Result<usize, ShardError> {
        // 1. Correctness boundary: in-process only (see module doc). On a
        //    non-distributed build every shard is local, so this never fires — but
        //    it is always compiled, so a future non-local shard can't slip past it.
        if self.shards.iter().any(|s| !s.is_local()) {
            return Err(ShardError::Config(
                "set_vocab is in-process only: a cross-process (remote) shard is not shipped \
                 the new normalizer in v1 (it would be a silent false negative)"
                    .into(),
            ));
        }
        #[cfg(feature = "distributed")]
        if !self.handoffs.is_empty() {
            return Err(ShardError::Config(
                "set_vocab is in-process only: a handoff-wrapped (movable) shard position is not \
                 supported by a vocabulary change in v1"
                    .into(),
            ));
        }
        // 2. Build the new normalizer up front (a parse/build error aborts before any swap).
        let new_norm = Arc::new(
            vocab
                .to_normalizer()
                .map_err(|e| ShardError::Config(format!("building normalizer from vocab: {e}")))?,
        );
        // 2b. Self-heal stale-active aliases FIRST (codex R13/R14): a punctuation change in
        //     this vocab can make an Active alias form unexpressible;
        //     demote those to review candidates rather than install an alias that reports
        //     active and silently never matches. Demotion can only shrink the registered phrase
        //     set, so rebuild the normalizer when it fires, so every later consumer (the
        //     rebuild + the installed normalizer) judges the HEALED vocabulary (codex R13/R14;
        //     the multi-word refusal this once guarded is retired by ADR-076, the heal stays).
        let mut vocab = vocab;
        let new_norm =
            if vocab
                .aliases_mut()
                .demote_unexpressible(&new_norm, &self.dict)
                > 0
            {
                Arc::new(vocab.to_normalizer().map_err(|e| {
                    ShardError::Config(format!("building normalizer from vocab: {e}"))
                })?)
            } else {
                new_norm
            };

        // A vocab that activates a multi-word alias is cluster-supported since ADR-076:
        // `route` is P(T)-aware when multi-word aliases are active, so a nested alias
        // entity that lives only in the positive superset still probes the shard holding
        // a query anchored on it. The ADR-061 refusal that used to guard this swap is
        // retired; the rebuild below re-places every query under the new normalizer, so
        // routing and placement stay derived from the same vocabulary.

        // 3. Rebuild the cluster from its live source set under the new normalizer, KEEPING the
        //    ring (same shard count). The shared blue/green core (ADR-046/078) re-mints the dict,
        //    re-places every query, builds fresh shards, and atomically swaps under `&mut self`.
        //    `Some(vocab)` installs the new vocabulary and uses ITS equivalence groups; per-query
        //    tags carry through as stored `TagId`s (ADR-074). The resize path (ADR-078) calls the
        //    SAME core with a fresh ring instead of a new vocab.
        let next_generation = self
            .placement_generation()
            .next()
            .ok_or_else(|| ShardError::Config("placement generation exhausted".into()))?;
        let rebuilt =
            self.rebuild_from_live(new_norm, self.ring.clone(), Some(vocab), next_generation)?;

        self.control.propose(ClusterStateChange::BumpModelVersion {
            dict_fingerprint: self.dict.fingerprint(),
        })?;

        // 4. Commit a durable cluster's rebuild via `checkpoint`: seal the green shards, write the
        //    new manifest (re-minted dict + serialized vocab + green segment registry — the atomic
        //    commit point), truncate the log, and GC the superseded old segment files.
        if self.data_dir.is_some() {
            self.checkpoint()?;
        }
        Ok(rebuilt)
    }

    /// Learn alias/synonym rules from the cluster's OWN live corpus (ADR-015 any-of
    /// learning) and apply them (ADR-046 mechanism 2). A synonym appearing in at least
    /// `min_count` any-of groups (e.g. `(new,pkg)` ⇒ `pkg → new`) is merged UNDER
    /// the current vocabulary — a previously *declared* alias wins over a learned one —
    /// and the cluster is rebuilt via [`Self::set_vocab`]. Returns the number of queries
    /// rebuilt. Refuses a non-local cluster (the gather can't enumerate a remote shard).
    ///
    /// On-demand: a future step can drive this from compaction's "improve" phase (the
    /// LSM-shaped background re-materialize); this is the explicit trigger.
    ///
    /// A thin wrapper over [`learn_and_apply_with`](Self::learn_and_apply_with) with NPMI
    /// corpus phrase induction disabled — behaviorally unchanged.
    pub fn learn_and_apply(&mut self, min_count: usize) -> Result<usize, ShardError> {
        self.learn_and_apply_with(&CorpusLearnConfig {
            anyof_min_count: min_count,
            ..Default::default()
        })
    }

    /// The cluster's deduped live `(logical, dsl)` corpus, gathered across shards — the
    /// source set the index is a materialized view of. Errors on a non-local shard
    /// (the same boundary [`Self::set_vocab`] enforces).
    fn live_corpus(&self) -> Result<Vec<(u64, String)>, ShardError> {
        let mut live: BTreeMap<u64, String> = BTreeMap::new();
        for s in &self.shards {
            for (logical, dsl) in s.live_sources()? {
                live.entry(logical).or_insert(dsl);
            }
        }
        Ok(live.into_iter().collect())
    }

    /// [`live_corpus`](Self::live_corpus) plus each query's stored `version` and `TagId`s —
    /// the gather behind the tagged + version-preserving rebuild (ADR-074). A query fanned out
    /// to several shards carries the same version + tags on every copy (one `PlacedQuery` per
    /// copy, identical op streams), so dedup-by-logical keeps the first copy seen. Same
    /// non-local error boundary.
    /// `pub(super)` so the shared rebuild core in `coordinator::resize` can gather the corpus
    /// for both a vocabulary change ([`set_vocab`](Self::set_vocab)) and a resize.
    pub(super) fn live_corpus_tagged(
        &self,
    ) -> Result<Vec<crate::cluster::shard::LiveTaggedQuery>, ShardError> {
        let mut live: BTreeMap<u64, LiveTaggedMetadata> = BTreeMap::new();
        for s in &self.shards {
            for (logical, dsl, version, source_generation, raw_tags, tag_ids, rank, placement) in
                s.live_sources_tagged()?
            {
                live.entry(logical).or_insert((
                    dsl,
                    version,
                    source_generation,
                    raw_tags,
                    tag_ids,
                    rank,
                    placement,
                ));
            }
        }
        Ok(live
            .into_iter()
            .map(
                |(
                    logical,
                    (dsl, version, source_generation, raw_tags, tag_ids, rank, placement),
                )| {
                    (
                        logical,
                        dsl,
                        version,
                        source_generation,
                        raw_tags,
                        tag_ids,
                        rank,
                        placement,
                    )
                },
            )
            .collect())
    }

    /// Learn vocabulary rules from the cluster's own live corpus WITHOUT applying them —
    /// the dry-run behind the coordinator-mode server's `POST /_vocab/learn` (ADR-070):
    /// the caller reviews the learned [`Vocab`] and decides whether to `PUT /_vocab` it.
    /// Compute-only (`&self`); refuses a non-local cluster (the gather boundary).
    pub fn learn_vocab(&self, cfg: &CorpusLearnConfig) -> Result<Vocab, ShardError> {
        let corpus = self.live_corpus()?;
        Ok(crate::vocab::learn_vocab_from_corpus(&corpus, cfg))
    }

    /// Import a Solr/Lucene synonym file into the governed alias registry and apply it
    /// (ADR-060 at the cluster, ADR-070): classifies against the cluster's CURRENT
    /// normalizer + frozen dict, then rebuilds via [`Self::set_vocab`] — whose non-local
    /// refusal holds unchanged (tags carry through per ADR-074; multi-word activation is
    /// supported per ADR-076). Returns the engine-shaped apply report (`recompiled` =
    /// queries rebuilt).
    pub fn import_alias_synonyms(
        &mut self,
        solr_text: &str,
    ) -> Result<crate::segment::AliasApplyReport, ShardError> {
        let mut vocab = self.vocab.as_deref().cloned().unwrap_or_default();
        let before = vocab.aliases().clone();
        let activated = vocab
            .import_solr_aliases(solr_text, &self.norm, &self.dict)
            .map_err(|error| ShardError::Config(error.to_string()))?;
        let changed = vocab.aliases() != &before;
        if !changed {
            self.finish_pending_alias_import_commit()?;
            return Ok(crate::segment::AliasApplyReport {
                applied: false,
                activated,
                recompiled: 0,
                summary: self
                    .vocab
                    .as_deref()
                    .map(Vocab::alias_summary)
                    .unwrap_or_default(),
            });
        }
        let predecessor = self.capture_alias_import_predecessor()?;
        self.pending_alias_import_predecessor = predecessor;
        *self
            .pending_alias_import_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let rebuilt = self.set_vocab(vocab)?;
        self.clear_pending_alias_import_identity();
        Ok(crate::segment::AliasApplyReport {
            applied: true,
            activated,
            recompiled: rebuilt,
            summary: self
                .vocab
                .as_deref()
                .map(Vocab::alias_summary)
                .unwrap_or_default(),
        })
    }

    /// Complete the post-swap commits a prior `set_vocab` attempt may have
    /// failed before publishing. A fully committed import remains a read-only
    /// no-op; incompatibility and attestation failures stay fail-loud.
    fn finish_pending_alias_import_commit(&mut self) -> Result<(), ShardError> {
        let generation = self.placement_generation();
        let dict_fingerprint = self.dict.fingerprint();
        let manifest_state = self.alias_import_manifest_state(generation)?;
        let state = self.control.cluster_state()?;
        let live_shards = u32::try_from(self.ring.num_shards()).map_err(|_| {
            ShardError::ControlPlane(
                "live shard count exceeds the control-plane representation".into(),
            )
        })?;
        let assignments_match = state.assignments.len() == self.ring.num_shards()
            && state
                .assignments
                .iter()
                .enumerate()
                .all(|(position, assignment)| assignment.position as usize == position);
        if state.num_shards != live_shards || state.vnodes != self.vnodes || !assignments_match {
            return Err(ShardError::ControlPlane(format!(
                "alias-import retry found control topology with {} shards, {} vnodes, and {} \
                 assignment(s); live topology has {} shards and {} vnodes",
                state.num_shards,
                state.vnodes,
                state.assignments.len(),
                self.ring.num_shards(),
                self.vnodes
            )));
        }
        if state.placement_generation != generation.0 || state.dict_fingerprint != dict_fingerprint
        {
            let prior_generation = generation.0.checked_sub(1).ok_or_else(|| {
                ShardError::ControlPlane(
                    "cannot repair alias-import model state at placement generation zero".into(),
                )
            })?;
            if state.placement_generation != prior_generation {
                return Err(ShardError::ControlPlane(format!(
                    "alias-import retry found control placement generation {}, expected {} or {}",
                    state.placement_generation, prior_generation, generation.0
                )));
            }
            self.control
                .propose(ClusterStateChange::BumpModelVersion { dict_fingerprint })?;
            let repaired = self.control.cluster_state()?;
            if repaired.placement_generation != generation.0
                || repaired.dict_fingerprint != dict_fingerprint
            {
                return Err(ShardError::ControlPlane(
                    "alias-import model-state repair was not committed".into(),
                ));
            }
        }

        match manifest_state {
            AliasImportManifestState::Committed => {}
            AliasImportManifestState::PublishedCurrent(epoch) => {
                let dir = self.data_dir.as_ref().ok_or_else(|| {
                    ShardError::Log(
                        "alias-import retry lost its durable directory before sync".into(),
                    )
                })?;
                std::fs::File::open(dir)
                    .and_then(|file| file.sync_all())
                    .map_err(|error| {
                        ShardError::Log(format!(
                            "syncing the published alias-import manifest directory: {error}"
                        ))
                    })?;
                self.epoch.store(epoch, Ordering::Relaxed);
            }
            AliasImportManifestState::ImmediatePredecessor => self.checkpoint()?,
        }
        self.clear_pending_alias_import_identity();
        Ok(())
    }

    fn clear_pending_alias_import_identity(&mut self) {
        self.pending_alias_import_predecessor = None;
        *self
            .pending_alias_import_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// Capture and attest the exact durable state an alias import is allowed to
    /// supersede. Retaining the parsed document makes a later retry compare every
    /// commit-identity field, including vocabulary and segment registry.
    fn capture_alias_import_predecessor(
        &self,
    ) -> Result<Option<crate::storage::ClusterManifest>, ShardError> {
        let Some(dir) = &self.data_dir else {
            return Ok(None);
        };
        let manifest = crate::storage::read_cluster_manifest(&dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|error| {
                ShardError::Log(format!(
                    "reading cluster manifest before alias import: {error}"
                ))
            })?;
        self.attest_alias_import_manifest_common(&manifest)?;
        let committed_matches = self
            .committed_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            == Some(&manifest);
        let vocab_data = self.alias_import_vocab_data()?;
        if !committed_matches
            || manifest.epoch != self.epoch()
            || manifest.placement_generation != self.placement_generation()
            || manifest.dict_fingerprint != self.dict.fingerprint()
            || manifest.dict_data != crate::storage::serialize_dict(&self.dict)
            || manifest.vocab_data != vocab_data
        {
            return Err(ShardError::Log(
                "cluster manifest diverged before alias import".into(),
            ));
        }
        Ok(Some(manifest))
    }

    /// Classify the durable commit point before an identical alias-import retry
    /// mutates the control plane or checkpoints. Only the live commit itself or
    /// its exact epoch/generation predecessor is admissible; every other readable
    /// manifest is divergent and must remain untouched.
    fn alias_import_manifest_state(
        &self,
        generation: crate::ownership::PlacementGeneration,
    ) -> Result<AliasImportManifestState, ShardError> {
        let Some(dir) = &self.data_dir else {
            return Ok(AliasImportManifestState::Committed);
        };
        let manifest = crate::storage::read_cluster_manifest(&dir.join(CLUSTER_MANIFEST_FILE))
            .map_err(|error| {
                ShardError::Log(format!(
                    "reading cluster manifest before alias-import retry: {error}"
                ))
            })?;
        self.attest_alias_import_manifest_common(&manifest)?;
        let committed_manifest = self
            .committed_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let pending_manifest = self
            .pending_alias_import_manifest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();

        if manifest.placement_generation == generation {
            if self.pending_alias_import_predecessor.is_some()
                && self
                    .epoch()
                    .checked_add(1)
                    .is_some_and(|next| manifest.epoch == next)
                && (pending_manifest.as_ref() == Some(&manifest)
                    || committed_manifest.as_ref() == Some(&manifest))
            {
                return Ok(AliasImportManifestState::PublishedCurrent(manifest.epoch));
            }
            if manifest.epoch == self.epoch() && committed_manifest.as_ref() == Some(&manifest) {
                return Ok(AliasImportManifestState::Committed);
            }
            return Err(ShardError::Log(format!(
                "current alias-import manifest epoch {} or recovery identity diverges from live \
                 epoch {}",
                manifest.epoch,
                self.epoch()
            )));
        }

        let prior_generation = generation.0.checked_sub(1).ok_or_else(|| {
            ShardError::Log(
                "cannot attest an alias-import predecessor at placement generation zero".into(),
            )
        })?;
        if manifest.placement_generation.0 != prior_generation
            || manifest.epoch != self.epoch()
            || self.pending_alias_import_predecessor.as_ref() != Some(&manifest)
            || committed_manifest.as_ref() != Some(&manifest)
        {
            return Err(ShardError::Log(format!(
                "cluster manifest placement generation {} is not the alias-import predecessor {}",
                manifest.placement_generation.0, prior_generation
            )));
        }
        Ok(AliasImportManifestState::ImmediatePredecessor)
    }

    fn alias_import_vocab_data(&self) -> Result<Vec<u8>, ShardError> {
        match &self.vocab {
            Some(vocab) => vocab.to_json().map(String::into_bytes).map_err(|error| {
                ShardError::Log(format!("serializing cluster vocab for retry: {error}"))
            }),
            None => Ok(Vec::new()),
        }
    }

    fn attest_alias_import_manifest_common(
        &self,
        manifest: &crate::storage::ClusterManifest,
    ) -> Result<(), ShardError> {
        let topology_matches = manifest.num_shards as usize == self.ring.num_shards()
            && manifest.vnodes == self.vnodes
            && manifest.include_broad == self.include_broad
            && manifest.broad_replicate_all
            && manifest.segment_registry.len() == self.ring.num_shards()
            && manifest.next_seg_ids.len() == self.ring.num_shards()
            && manifest.source_files.len() == self.ring.num_shards();
        if !topology_matches
            || manifest.compiler_semantics_version
                != crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION
            || manifest.tag_dict_data != crate::storage::serialize_tagdict(&self.tag_dict)
        {
            return Err(ShardError::Log(format!(
                "cluster manifest diverged before alias-import retry: epoch {}, placement \
                 generation {}, {} shards, {} vnodes",
                manifest.epoch,
                manifest.placement_generation.0,
                manifest.num_shards,
                manifest.vnodes
            )));
        }

        let persisted_dict =
            crate::storage::deserialize_dict(&manifest.dict_data).map_err(|error| {
                ShardError::Log(format!(
                    "validating persisted cluster dict before alias-import retry: {error}"
                ))
            })?;
        if persisted_dict.fingerprint() != manifest.dict_fingerprint {
            return Err(ShardError::Log(format!(
                "persisted cluster dict fingerprint diverged before alias-import retry: manifest \
                 {:#018x}, actual {:#018x}",
                manifest.dict_fingerprint,
                persisted_dict.fingerprint()
            )));
        }

        if !manifest.vocab_data.is_empty() {
            let persisted = std::str::from_utf8(&manifest.vocab_data).map_err(|error| {
                ShardError::Log(format!(
                    "validating persisted cluster vocab before alias-import retry: {error}"
                ))
            })?;
            let persisted_vocab = Vocab::from_json(persisted).map_err(|error| {
                ShardError::Log(format!(
                    "validating persisted cluster vocab before alias-import retry: {error}"
                ))
            })?;
            persisted_vocab.to_normalizer().map_err(|error| {
                ShardError::Log(format!(
                    "validating persisted cluster vocab before alias-import retry: {error}"
                ))
            })?;
        }
        Ok(())
    }

    /// Learn alias candidates from the cluster's OWN stored queries (any-of
    /// co-occurrence, ADR-060 item 2) into the registry and apply. Conservative: only
    /// clear single-token variants auto-activate; everything else stays a review
    /// candidate. Rebuilds via [`Self::set_vocab`] (all refusals hold).
    pub fn learn_aliases_and_apply(
        &mut self,
        min_count: usize,
    ) -> Result<crate::segment::AliasApplyReport, ShardError> {
        let corpus = self.live_corpus()?;
        let mut vocab = self.vocab.as_deref().cloned().unwrap_or_default();
        let activated =
            vocab.learn_aliases_from_queries(&corpus, min_count, &self.norm, &self.dict);
        let rebuilt = self.set_vocab(vocab)?;
        Ok(crate::segment::AliasApplyReport {
            applied: true,
            activated,
            recompiled: rebuilt,
            summary: self
                .vocab
                .as_deref()
                .map(Vocab::alias_summary)
                .unwrap_or_default(),
        })
    }

    /// Like [`learn_and_apply`](Self::learn_and_apply) but also runs opt-in **NPMI corpus
    /// phrase induction** when `cfg.corpus_phrases` is set (ADR-053): multi-token entities
    /// induced from the cluster's live query text are merged UNDER the current vocabulary
    /// (a declared alias/phrase wins on a token collision) and the cluster is rebuilt via
    /// [`Self::set_vocab`] (which re-places every query — a phrase can move a query's anchor,
    /// hence its shard). With `corpus_phrases = false` this is identical to
    /// `learn_and_apply(cfg.anyof_min_count)`. Phrases only — never aliases — so the
    /// same-normalizer gluing is lossless-cover safe. Refuses a non-local cluster.
    pub fn learn_and_apply_with(&mut self, cfg: &CorpusLearnConfig) -> Result<usize, ShardError> {
        let corpus = self.live_corpus()?;
        let learned = crate::vocab::learn_vocab_from_corpus(&corpus, cfg);
        // Merge learned rules UNDER the current vocab (declared aliases win), then rebuild.
        let mut merged = Vocab::new();
        if let Some(v) = &self.vocab {
            merged.merge(v);
        }
        merged.merge(&learned);
        self.set_vocab(merged)
    }

    /// The vocabulary behind the current normalizer, if one was installed via
    /// [`Self::set_vocab`]/[`Self::learn_and_apply`] (`None` when built directly from
    /// a `Normalizer`).
    pub fn vocab(&self) -> Option<&Vocab> {
        self.vocab.as_deref()
    }
}
