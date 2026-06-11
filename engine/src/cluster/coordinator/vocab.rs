//! `impl ClusterEngine` — runtime vocabulary change (ADR-046 mechanism 2).
//!
//! A vocabulary change (e.g. a declared alias `ud ≡ upperdeck`) swaps the ONE
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
use std::sync::Arc;

use crate::compile::{extract, Extracted};
use crate::dict::Dict;
use crate::segment::PlacedQuery;
use crate::vocab::{CorpusLearnConfig, Vocab};

use super::{into_shard, placement_of, replica_dir, shard_dir, ClusterEngine, Target};
use crate::cluster::shard::{LocalShard, Shard, ShardError};

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
        // 2b. Self-heal stale-active aliases FIRST (codex R13/R14): a punctuation/grader change
        //     in this vocab can make an Active alias form unexpressible (e.g. a fused grader);
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

        // 3. Gather the deduped live `(logical, dsl, tag_ids)` set across shards. A
        //    selective / any-of query lives on several shards but has ONE dsl (and one
        //    tag set — every fanned-out copy carries the same tags) — dedup by logical
        //    id. Tags ride as stored `TagId`s, NOT raw strings: the tag space is
        //    orthogonal to vocabulary and preserved unchanged through the rebuild, so a
        //    stored id — interned dense or post-freeze synthetic (which has no
        //    recoverable string) — stays valid and is carried verbatim to the query's
        //    new shard (ADR-074, the cluster analogue of the single-node ADR-049
        //    carry-through in `recompile_stale_segments`). Untagged ⇒ every tag vec is
        //    empty ⇒ byte-identical to the pre-tag rebuild.
        let live = self.live_corpus_tagged()?;

        // 4. Pass A — re-mint the dict over the live corpus under the new normalizer
        //    (interning + frequencies + hot-mask), exactly as `build`.
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut extracted: Vec<(u64, Extracted, String, Vec<crate::tagdict::TagId>)> =
            Vec::with_capacity(live.len());
        for (logical, text, tag_ids) in live {
            if let Ok(ast) = crate::dsl::parse(&text) {
                let ex = extract(&ast, &new_norm, &mut dict, &mut lc);
                extracted.push((logical, ex, text, tag_ids));
            }
        }
        dict.finalize_mask();
        // Resolve declared/learned equivalence groups (ADR-054) against the freshly-minted
        // dict and apply them via expansion: widen the already-extracted queries (so THIS
        // rebuild's re-placement + ingest use the FN-safe widened form — a query whose anchor
        // is now an any-of fans to every member's shard), then install the map on the dict so
        // future incremental adds expand through `extract`. No groups ⇒ empty ⇒ byte-identical.
        let equiv = vocab.resolve_equivalences(&new_norm, &dict);
        for (_, ex, _, _) in &mut extracted {
            ex.expand_equivalences(&equiv);
        }
        dict.set_equivalences(equiv);
        let new_dict = Arc::new(dict);
        let rebuilt = extracted.len();

        // 5. Pass B — re-place each query under the NEW dict and bucket per shard. Tags
        //    travel with the query (`tag_ids`, the ADR-074 carry-through): an alias can
        //    move a query's anchor — hence its shard — and the filtered-read contract
        //    requires its tags on whichever shard now holds it.
        let num_shards = self.ring.num_shards();
        let mut buckets: Vec<Vec<PlacedQuery>> = (0..num_shards).map(|_| Vec::new()).collect();
        for (logical, ex, text, tag_ids) in extracted {
            match placement_of(&new_dict, &self.ring, &ex) {
                Target::Reject => {}
                Target::Replicated => buckets[0].push(PlacedQuery {
                    logical,
                    ex,
                    dsl: text,
                    version: 1,
                    tags: Vec::new(),
                    tag_ids,
                }),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push(PlacedQuery {
                            logical,
                            ex: ex.clone(),
                            dsl: text.clone(),
                            version: 1,
                            tags: Vec::new(),
                            tag_ids: tag_ids.clone(),
                        });
                    }
                }
            }
        }

        // 6. Construct fresh shards sharing the new norm + re-minted dict, `replication_factor`
        //    copies per position, and ingest each bucket into EVERY copy (identical op stream ⇒
        //    copies set-equal, as in `build`). A durable cluster rebuilds each shard as a
        //    segments-only engine in the SAME shard dir, numbered ABOVE the old segments so the
        //    new `.seg` files coexist with the still-committed old ones until the manifest commit
        //    (step 8) — crash-safe: a crash before the commit leaves the old manifest + old
        //    segments authoritative. An in-memory cluster builds in-RAM copies.
        let rf = self.replication_factor.max(1);
        let data_dir = self.data_dir.clone();
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(num_shards);
        for (s, bucket) in buckets.into_iter().enumerate() {
            let mut copies = Vec::with_capacity(rf);
            for r in 0..rf {
                let copy = match &data_dir {
                    Some(dir) => {
                        let mut sc = self.per_shard.clone();
                        sc.data_dir = Some(if r == 0 {
                            shard_dir(dir, s)
                        } else {
                            replica_dir(dir, s, r)
                        });
                        // Seed green segment numbering above the old shard's (primary and every
                        // replica share the counter, kept equal by identical op streams), so the
                        // freshly written `.seg` never collide with the old ones.
                        let next_seg = self.shards[s].next_seg_id()?;
                        LocalShard::open_segments(
                            Arc::clone(&new_norm),
                            Arc::clone(&new_dict),
                            // The tag space is orthogonal to vocabulary — preserve it
                            // unchanged, so the carried `tag_ids` stay valid (ADR-074).
                            Arc::clone(&self.tag_dict),
                            sc,
                            &[],
                            next_seg,
                        )?
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

        // 7. Atomic swap (under `&mut self`, so no read observes a half-state). For a durable
        //    cluster, `checkpoint` then seals the green shards, writes the new manifest (the
        //    re-minted dict + serialized vocab + green segment registry — the atomic commit
        //    point), truncates the log, and GCs the superseded old segment files.
        self.norm = new_norm;
        self.dict = new_dict;
        self.shards = shards;
        self.vocab = Some(Arc::new(vocab));
        if data_dir.is_some() {
            self.checkpoint()?;
        }
        Ok(rebuilt)
    }

    /// Learn alias/synonym rules from the cluster's OWN live corpus (ADR-015 any-of
    /// learning) and apply them (ADR-046 mechanism 2). A synonym appearing in at least
    /// `min_count` any-of groups (e.g. `(rookie,rc)` ⇒ `rc → rookie`) is merged UNDER
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

    /// [`live_corpus`](Self::live_corpus) plus each query's stored `TagId`s — the gather
    /// behind the tagged rebuild (ADR-074). A query fanned out to several shards carries
    /// the same tags on every copy (one `PlacedQuery` per copy, identical op streams), so
    /// dedup-by-logical keeps the first copy seen. Same non-local error boundary.
    fn live_corpus_tagged(
        &self,
    ) -> Result<Vec<(u64, String, Vec<crate::tagdict::TagId>)>, ShardError> {
        let mut live: BTreeMap<u64, (String, Vec<crate::tagdict::TagId>)> = BTreeMap::new();
        for s in &self.shards {
            for (logical, dsl, tag_ids) in s.live_sources_tagged()? {
                live.entry(logical).or_insert((dsl, tag_ids));
            }
        }
        Ok(live
            .into_iter()
            .map(|(logical, (dsl, tag_ids))| (logical, dsl, tag_ids))
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
        let activated = vocab.import_solr_aliases(solr_text, &self.norm, &self.dict);
        let rebuilt = self.set_vocab(vocab)?;
        Ok(crate::segment::AliasApplyReport {
            activated,
            recompiled: rebuilt,
            summary: self
                .vocab
                .as_deref()
                .map(Vocab::alias_summary)
                .unwrap_or_default(),
        })
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
