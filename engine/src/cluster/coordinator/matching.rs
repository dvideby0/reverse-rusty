//! `impl ClusterEngine` — the read path: routing, `percolate` (+ stats / explicit-broad
//! variants), the cross-shard merge, and count / fan-out introspection.

use crate::cluster::shard::ShardError;
use crate::compile::is_hot;
use crate::dict::FeatureId;
use crate::exact::TagPredicate;
use crate::segment::MatchStats;

use super::ClusterEngine;

impl ClusterEngine {
    /// The shards a title is routed to: shard 0 (the replicated-lane evaluator)
    /// plus the shard owning each anchor-eligible (non-hot) title feature. Reuses
    /// the same `match_features` primitives the match path uses, so routing and
    /// matching cannot drift.
    ///
    /// **P(T)-aware under multi-word aliases (ADR-076):** with an active multi-word
    /// alias, routing derives targets from the **maximal positive view** `P(T)` —
    /// the same superset the shard-local verifier reads required/any-of against
    /// (ADR-061) — instead of the canonical leftmost-longest `N(T)`. The cover
    /// argument: a query's anchor is one of its extracted positive features, and
    /// `P(T)` contains every feature ANY parse of the title emits (the parse-union
    /// property the ADR-061 oracle pins), so a title that could satisfy a query
    /// always routes to the query's anchor shard — zero false negatives. `P(T) ⊇
    /// N(T)` means fan-out only ever widens, and only on alias-bearing titles; with
    /// no active multi-word alias `P(T) == N(T)` and this takes the single-view
    /// path, byte-identical to the pre-ADR-076 routing.
    fn route(&self, title: &str) -> (Vec<usize>, usize) {
        let mut lc = String::new();
        let mut sc = crate::normalize::NormScratch::new();
        let mut feats: Vec<FeatureId> = Vec::new();
        if self.norm.has_multiword_aliases() {
            let mut neg: Vec<FeatureId> = Vec::new();
            self.norm
                .match_features_dual(title, &self.dict, &mut lc, &mut sc, &mut neg, &mut feats);
        } else {
            self.norm
                .match_features(title, &self.dict, &mut lc, &mut sc, &mut feats);
        }
        // Selective targets: the shard owning each anchor-eligible (non-hot) feature.
        let mut targets: Vec<usize> = Vec::with_capacity(feats.len() + 1);
        for &f in &feats {
            if !is_hot(&self.dict, f) {
                targets.push(self.ring.lookup(f));
            }
        }
        targets.sort_unstable();
        targets.dedup();
        // Broad-eval shard: the ONE shard that evaluates the replicated-to-all broad lane for
        // this title (ADR-080), picked by a stable title hash so no shard is a broad hotspot.
        // Free-ride an already-probed selective target when there is one (zero extra fan-out);
        // otherwise probe a single hashed shard (a title with no selective anchor — all-hot or
        // empty). Either way `broad_eval_shard ∈ targets`, so every title evaluates broad on a
        // shard it probes, and that shard holds the complete (replicated) broad lane.
        let h = crate::util::fnv1a64(title.as_bytes());
        let broad_eval_shard = if targets.is_empty() {
            let s = (h % self.ring.num_shards() as u64) as usize;
            targets.push(s);
            s
        } else {
            targets[(h % targets.len() as u64) as usize]
        };
        (targets, broad_eval_shard)
    }

    /// Match one title against the cluster, using the cluster's default broad-lane
    /// setting. Returns matched logical ids (sorted, deduped).
    pub fn percolate(&self, title: &str) -> Result<Vec<u64>, ShardError> {
        Ok(self
            .percolate_inner(title, self.include_broad, &TagPredicate::empty())?
            .0)
    }

    /// [`Self::percolate`] plus merged [`MatchStats`] across the probed shards.
    pub fn percolate_with_stats(&self, title: &str) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.percolate_inner(title, self.include_broad, &TagPredicate::empty())
    }

    /// Match one title with an explicit broad-lane toggle (overriding the cluster
    /// default) — used by the oracle to sweep broad on/off on one cluster.
    pub fn percolate_with_broad(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<Vec<u64>, ShardError> {
        Ok(self
            .percolate_inner(title, include_broad, &TagPredicate::empty())?
            .0)
    }

    /// Match one title narrowed by a tag filter (ADR-049/055): a conjunction of `(key, [values])`
    /// groups, compiled ONCE against the shared frozen tag space and fanned to every probed shard.
    /// Returns the matched logical ids that also satisfy the filter (sorted, deduped). An empty
    /// filter is byte-identical to [`Self::percolate`]. Mirrors the single-node
    /// `compile_tag_predicate` + `match_title_filtered` so cluster ≡ single-node under a filter.
    pub fn percolate_filtered(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
    ) -> Result<Vec<u64>, ShardError> {
        let pred = self.compile_tag_predicate(filter);
        Ok(self.percolate_inner(title, self.include_broad, &pred)?.0)
    }

    /// [`Self::percolate_filtered`] with an explicit broad-lane toggle — used by the oracle to sweep
    /// broad on/off under a filter on one cluster.
    pub fn percolate_filtered_with_broad(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        include_broad: bool,
    ) -> Result<Vec<u64>, ShardError> {
        let pred = self.compile_tag_predicate(filter);
        Ok(self.percolate_inner(title, include_broad, &pred)?.0)
    }

    /// Compile a request filter — a conjunction of `(key, [values])` groups — into a
    /// [`TagPredicate`] against the coordinator's frozen tag space (ADR-049/055). Each value resolves
    /// via [`get_or_synthetic`](crate::tagdict::TagDict::get_or_synthetic), so a value never seen at
    /// ingest yields a `TagId` no stored query carries (matches nothing — the safe `terms`
    /// semantics), never an over-match. The same frozen tag space the shards resolved their stored
    /// tags against, so the integer groups are directly comparable across the cluster.
    pub fn compile_tag_predicate(&self, filter: &[(String, Vec<String>)]) -> TagPredicate {
        let groups = filter
            .iter()
            .map(|(key, values)| {
                values
                    .iter()
                    .map(|v| self.tag_dict.get_or_synthetic(key, v))
                    .collect()
            })
            .collect();
        TagPredicate::new(groups)
    }

    fn percolate_inner(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let (targets, broad_eval_shard) = self.route(title);
        // The broad lane is replicated to every shard (ADR-080) but evaluated on exactly ONE
        // shard per title — its broad-eval shard — so a broad query is counted once; the other
        // probed shards run with broad off (they would re-scan the same replicated lane). A
        // failed shard probe propagates rather than being dropped: a silently missing shard
        // would shrink the union into a FALSE NEGATIVE.
        let parts: Vec<(Vec<u64>, MatchStats)> = if targets.len() <= 1 {
            targets
                .iter()
                .map(|&s| {
                    self.shards[s].percolate_filtered(
                        title,
                        include_broad && s == broad_eval_shard,
                        pred,
                    )
                })
                .collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(|&s| {
                    self.shards[s].percolate_filtered(
                        title,
                        include_broad && s == broad_eval_shard,
                        pred,
                    )
                })
                .collect::<Result<_, _>>()?
        };

        let mut out = Vec::new();
        let mut stats = MatchStats::default();
        for (ids, st) in parts {
            out.extend_from_slice(&ids);
            stats.merge(st);
        }
        out.sort_unstable();
        out.dedup();
        stats.matches = out.len() as u32;
        Ok((out, stats))
    }

    /// Compile a request `rank` block against the coordinator's frozen tag space
    /// (ADR-059/075) — the ranking analogue of [`Self::compile_tag_predicate`], with
    /// the same `get_or_synthetic` resolution the single-node
    /// `EngineSnapshot::compile_rank_spec` uses: a boost value never seen at ingest
    /// yields a `TagId` no stored query carries and simply never fires. The shards
    /// resolved their stored tags against this SAME shared dict, so the integer
    /// boost ids are directly comparable cluster-wide.
    pub fn compile_rank_spec(&self, spec: &crate::rank::RankSpec) -> crate::rank::CompiledRankSpec {
        let boosts = spec
            .boosts
            .iter()
            .map(|(key, value, weight)| (self.tag_dict.get_or_synthetic(key, value), *weight))
            .collect();
        crate::rank::CompiledRankSpec::new(spec.priority_key.clone(), boosts)
    }

    /// [`Self::percolate_filtered_with_stats`] plus a per-id ranking score (the
    /// cluster `rank` path, ADR-059/075). The spec is compiled ONCE here and fanned
    /// to every probed shard, which scores its own matched ids against its stored
    /// tag columns; the merge dedups by id — copies of one logical are
    /// version-identical across shards (identical op streams), so every shard
    /// reports the same score and dedup cannot lose information. Returns the scored
    /// set sorted by id (the same order the unranked merge returns); the caller owns
    /// the `(score desc, _id asc)` presentation order + `from`/`size`, exactly as
    /// with the single-node `EngineSnapshot::rank`. Ranking only reorders — the id
    /// set is identical to the unranked percolate (zero-FN trivially preserved).
    pub fn percolate_filtered_ranked(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        include_broad: bool,
        rank: &crate::rank::RankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        let pred = self.compile_tag_predicate(filter);
        let spec = self.compile_rank_spec(rank);
        let (targets, broad_eval_shard) = self.route(title);
        // Same fan-out + fail-loud shape as `percolate_inner` (a dropped shard probe
        // would shrink the union into a false negative); broad on the one broad-eval shard.
        let parts: Vec<(Vec<(u64, i64)>, MatchStats)> = if targets.len() <= 1 {
            targets
                .iter()
                .map(|&s| {
                    self.shards[s].percolate_filtered_ranked(
                        title,
                        include_broad && s == broad_eval_shard,
                        &pred,
                        &spec,
                    )
                })
                .collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(|&s| {
                    self.shards[s].percolate_filtered_ranked(
                        title,
                        include_broad && s == broad_eval_shard,
                        &pred,
                        &spec,
                    )
                })
                .collect::<Result<_, _>>()?
        };

        let mut out: Vec<(u64, i64)> = Vec::new();
        let mut stats = MatchStats::default();
        for (scored, st) in parts {
            out.extend_from_slice(&scored);
            stats.merge(st);
        }
        out.sort_unstable_by_key(|&(id, _)| id);
        out.dedup_by_key(|&mut (id, _)| id);
        stats.matches = out.len() as u32;
        Ok((out, stats))
    }

    /// Introspection: the shards a title would be routed to (its fan-out) — the selective
    /// targets plus the one broad-eval shard (ADR-080).
    pub fn shard_fanout(&self, title: &str) -> Vec<usize> {
        self.route(title).0
    }

    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        self.ring.num_shards()
    }

    /// Total physical query count across shards (a replicated/any-of query is
    /// counted once per shard holding it — physical, not distinct-logical).
    pub fn num_queries(&self) -> Result<usize, ShardError> {
        self.shards.iter().map(|s| s.num_queries()).sum()
    }

    /// Per-shard physical query counts (introspection / tests).
    pub fn shard_query_counts(&self) -> Result<Vec<usize>, ShardError> {
        self.shards.iter().map(|s| s.num_queries()).collect()
    }

    /// Cluster-wide per-class entry tally `[A, B, C, D]`, summed across shards
    /// (replicated/any-of queries counted per holding shard). Used by the oracle
    /// to assert each placement branch is actually exercised.
    pub fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        let mut total = [0u64; 4];
        for s in &self.shards {
            let c = s.class_counts()?;
            for i in 0..4 {
                total[i] += c[i];
            }
        }
        Ok(total)
    }

    /// [`Self::percolate_filtered_with_broad`] also returning the merged [`MatchStats`]
    /// across the probed shards — the coordinator-mode server's `/_search` profile path
    /// (ADR-070). An empty filter + the cluster default broad toggle is byte-identical
    /// to [`Self::percolate_with_stats`].
    pub fn percolate_filtered_with_stats(
        &self,
        title: &str,
        filter: &[(String, Vec<String>)],
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let pred = self.compile_tag_predicate(filter);
        self.percolate_inner(title, include_broad, &pred)
    }

    /// The live source DSL stored for `logical`, probing each shard's source store
    /// (first live copy wins — every copy of one logical id is identical). `Ok(None)`
    /// only when EVERY shard answered "not held"; a shard that cannot answer (a
    /// `RemoteShard` in v1) fails the lookup loud rather than letting the coordinator
    /// report a false "not found" (ADR-070).
    pub fn get_source(&self, logical: u64) -> Result<Option<String>, ShardError> {
        let mut first_err: Option<ShardError> = None;
        for s in &self.shards {
            match s.source_of(logical) {
                Ok(Some(dsl)) => return Ok(Some(dsl)),
                Ok(None) => {}
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }

    /// The cluster's default broad-lane toggle (what [`Self::percolate`] uses).
    pub fn include_broad(&self) -> bool {
        self.include_broad
    }

    /// Replication factor (copies per shard position).
    pub fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    /// Whether this cluster persists durable artifacts (built/opened with a `data_dir`).
    pub fn is_durable(&self) -> bool {
        self.data_dir.is_some()
    }

    /// The per-shard engine configuration the cluster was assembled with.
    pub fn per_shard_config(&self) -> &crate::config::EngineConfig {
        &self.per_shard
    }

    /// True if the cluster holds (or has ever held) any tagged query (ADR-055).
    /// Introspection for operators (cluster-mode `/_stats`, ADR-070); best-effort
    /// across reopen (a checkpointed synthetic-only cluster restores it `false`).
    /// No longer gates anything: a vocabulary change carries tags through the
    /// rebuild by stored `TagId` (ADR-074).
    pub fn has_tagged_queries(&self) -> bool {
        self.has_tags()
    }
}
