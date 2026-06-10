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
    /// the same `match_features` primitive the match path uses, so routing and
    /// matching cannot drift.
    fn route(&self, title: &str) -> Vec<usize> {
        let mut lc = String::new();
        let mut feats: Vec<FeatureId> = Vec::new();
        self.norm
            .match_features(title, &self.dict, &mut lc, &mut feats);
        let mut targets: Vec<usize> = Vec::with_capacity(feats.len() + 1);
        targets.push(0);
        for &f in &feats {
            if !is_hot(&self.dict, f) {
                targets.push(self.ring.lookup(f));
            }
        }
        targets.sort_unstable();
        targets.dedup();
        targets
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
        let targets = self.route(title);
        // Broad is evaluated ONLY on shard 0 (the replicated lane); selective
        // shards hold only main-index queries, so probing their (empty) broad
        // index would be pure waste — and double-counting a broadcast query.
        // A failed shard probe propagates rather than being dropped: a silently
        // missing shard would shrink the union into a FALSE NEGATIVE.
        let parts: Vec<(Vec<u64>, MatchStats)> = if targets.len() <= 1 {
            targets
                .iter()
                .map(|&s| self.shards[s].percolate_filtered(title, include_broad && s == 0, pred))
                .collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(|&s| self.shards[s].percolate_filtered(title, include_broad && s == 0, pred))
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

    /// Introspection: the shards a title would be routed to (its fan-out).
    pub fn shard_fanout(&self, title: &str) -> Vec<usize> {
        self.route(title)
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

    /// True if the cluster holds (or has ever held) any tagged query (ADR-055) — the
    /// condition under which a vocabulary change is refused. Introspection for
    /// operators (cluster-mode `/_stats`, ADR-070).
    pub fn has_tagged_queries(&self) -> bool {
        self.has_tags()
    }
}
