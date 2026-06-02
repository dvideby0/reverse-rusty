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
//! the alias does not change the interned-name set). Durable clusters are handled
//! in a follow-on (the manifest must persist the new dict + vocab).

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::compile::{extract, Extracted};
use crate::dict::Dict;
use crate::vocab::Vocab;

use super::{into_shard, placement_of, ClusterEngine, Target};
use crate::cluster::shard::{LocalShard, Shard, ShardError};

impl ClusterEngine {
    /// Change the cluster's vocabulary (ADR-046 mechanism 2) — e.g. declare an
    /// alias so two surface forms match. Rebuilds the cluster from its live source
    /// set under the new normalizer: re-mints the shared dict, re-places every
    /// query (an alias can move a query's anchor, hence its shard), and re-ingests.
    /// Atomic under `&mut self`. Returns the number of live queries rebuilt.
    ///
    /// Refuses (errors) if any shard is non-local, or — for now — if the cluster is
    /// durable (the durable rebuild, which must persist the re-minted dict + vocab
    /// into the manifest, is a follow-on).
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

        // 3. Durable rebuild (persist the re-minted dict + vocab into the manifest)
        //    lands in a follow-on; in-process clusters are fully supported here.
        if self.data_dir.is_some() {
            return Err(ShardError::Config(
                "set_vocab on a durable cluster is not yet supported (in-process clusters only)"
                    .into(),
            ));
        }

        // 4. Gather the deduped live `(logical, dsl)` set across shards. A selective /
        //    any-of query lives on several shards but has ONE dsl — dedup by logical id.
        let mut live: BTreeMap<u64, String> = BTreeMap::new();
        for s in &self.shards {
            for (logical, dsl) in s.live_sources()? {
                live.entry(logical).or_insert(dsl);
            }
        }

        // 5. Pass A — re-mint the dict over the live corpus under the new normalizer
        //    (interning + frequencies + hot-mask), exactly as `build`.
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut extracted: Vec<(u64, Extracted, String)> = Vec::with_capacity(live.len());
        for (logical, text) in live {
            if let Ok(ast) = crate::dsl::parse(&text) {
                let ex = extract(&ast, &new_norm, &mut dict, &mut lc);
                extracted.push((logical, ex, text));
            }
        }
        dict.finalize_mask();
        let new_dict = Arc::new(dict);
        let rebuilt = extracted.len();

        // 6. Pass B — re-place each query under the NEW dict and bucket per shard.
        let num_shards = self.ring.num_shards();
        let mut buckets: Vec<Vec<(u64, Extracted, String, u32)>> =
            (0..num_shards).map(|_| Vec::new()).collect();
        for (logical, ex, text) in extracted {
            match placement_of(&new_dict, &self.ring, &ex) {
                Target::Reject => {}
                Target::Replicated => buckets[0].push((logical, ex, text, 1)),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push((logical, ex.clone(), text.clone(), 1));
                    }
                }
            }
        }

        // 7. Construct fresh in-memory shards sharing the new norm + re-minted dict,
        //    `replication_factor` copies per position, and ingest each bucket into
        //    EVERY copy (identical op stream ⇒ copies set-equal, as in `build`).
        let rf = self.replication_factor.max(1);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(num_shards);
        for bucket in buckets {
            let mut copies = Vec::with_capacity(rf);
            for _ in 0..rf {
                let copy = LocalShard::new(
                    Arc::clone(&new_norm),
                    Arc::clone(&new_dict),
                    self.per_shard.clone(),
                );
                if !bucket.is_empty() {
                    copy.ingest_local(&bucket);
                }
                copies.push(copy);
            }
            shards.push(into_shard(copies)?);
        }

        // 8. Atomic swap (under `&mut self`, so no read observes a half-state).
        self.norm = new_norm;
        self.dict = new_dict;
        self.shards = shards;
        self.vocab = Some(Arc::new(vocab));
        Ok(rebuilt)
    }

    /// The vocabulary behind the current normalizer, if one was installed via
    /// [`Self::set_vocab`] (`None` when built directly from a `Normalizer`).
    pub fn vocab(&self) -> Option<&Vocab> {
        self.vocab.as_deref()
    }
}
