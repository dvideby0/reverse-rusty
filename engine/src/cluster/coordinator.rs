//! `ClusterEngine` — the in-process coordinator: placement (writes), content
//! routing (reads), and cross-shard merge.
//!
//! Design: docs/design/clustering-and-scaling.md §3 (placement + routing), §7
//! (broad lane). Owns the ONE authoritative, frozen [`Dict`]/[`Normalizer`] shared
//! into every shard, the [`HashRing`] over `FeatureId`, and `K` [`Shard`]s.
//!
//! ## Placement (by cost class, derived from [`anchor_plan`], never re-derived)
//! - **A** (one rare required anchor `r1`): one shard = `ring.lookup(r1)`.
//! - **B any-of** (members all rare): one shard per any-of member, deduped.
//! - **B arity-2** (rarest required is hot ⇒ all required hot ⇒ no rare anchor):
//!   the replicated lane → shard 0.
//! - **C** (broad, hot-only anchor): the replicated lane → shard 0.
//! - **D** (no anchorable feature): rejected, stored nowhere.
//!
//! Shard 0 is the in-process stand-in for "replicate the broad lane to every node"
//! (§7): it holds the complete class-C + class-B-arity-2 set and is the only shard
//! that evaluates it, so there is no double-counting.
//!
//! ## Routing (reads)
//! A title is probed on shard 0 (always, for the replicated lane) plus the shard
//! owning each of the title's *anchor-eligible* (non-hot) features — a ~2–5 shard
//! fan-out, never all N. Shard 0 runs with `include_broad`; the selective shards
//! run without it (they hold only main-index queries). Results are unioned and
//! deduped.
//!
//! ## Why this is lossless
//! For any query `Q` a title `T` truly matches: if `Q` is class A / B-any-of, its
//! anchor (resp. some matched member) is a *required* feature, hence present in
//! `T` and non-hot, so `T` routes to `ring.lookup(anchor) =` `Q`'s shard; if `Q`
//! is class B-arity-2 / C it lives on shard 0, which `T` always probes. Each shard
//! is a verbatim single-node engine, so its lossless cover + exact verify finish
//! the job. No shard boundary can drop a match.

use std::sync::Arc;

use crate::compile::{anchor_plan, extract, extract_readonly, is_hot, CostClass, Extracted};
use crate::config::EngineConfig;
use crate::dict::{Dict, FeatureId};
use crate::error::ParseError;
use crate::normalize::Normalizer;
use crate::segment::MatchStats;

use super::ring::{HashRing, DEFAULT_VNODES};
use super::shard::{LocalShard, Shard};

/// Configuration for a [`ClusterEngine`].
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// Number of shards (K). Must be ≥ 1; K = 1 reduces to a single-node engine.
    pub num_shards: usize,
    /// Virtual nodes per shard on the consistent-hash ring.
    pub vnodes: u32,
    /// Per-shard engine configuration (forwarded to each shard's `Engine`).
    /// In-process shards are non-durable; leave `data_dir` unset.
    pub per_shard: EngineConfig,
    /// Default broad-lane toggle for [`ClusterEngine::percolate`].
    pub include_broad: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            num_shards: 8,
            vnodes: DEFAULT_VNODES,
            per_shard: EngineConfig::default(),
            include_broad: true,
        }
    }
}

/// Where a freshly added query landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddOutcome {
    /// Selective query (class A / B any-of): placed on these shard(s).
    Placed { shards: Vec<usize> },
    /// Replicated-lane query (class C / B arity-2): placed on the designated shard.
    Replicated,
    /// Compiled but rejected as cost-class D — no anchorable feature, stored nowhere.
    RejectedClassD,
    /// The DSL failed to parse.
    RejectedParse(ParseError),
}

/// Internal placement decision for one compiled query.
enum Target {
    Reject,
    /// The replicated lane (class C / B arity-2) → shard 0.
    Replicated,
    /// Selective shards (class A / B any-of), sorted + deduped, non-empty.
    Selective(Vec<usize>),
}

/// An in-process multi-shard reverse query matcher.
pub struct ClusterEngine {
    /// The one shared feature space (frozen after [`Self::build`]).
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    ring: HashRing,
    shards: Vec<Box<dyn Shard>>,
    include_broad: bool,
}

impl ClusterEngine {
    /// Build a cluster from an initial corpus. This is the primary constructor:
    /// it builds the ONE authoritative dict over the whole corpus (pass A), freezes
    /// it, creates `K` shards sharing it, then distributes each query to its
    /// placement shard(s) (pass B). One immutable base segment per shard.
    ///
    /// After this the dict is frozen: [`Self::add_query`] can only use vocabulary
    /// already present (it compiles read-only against the shared dict), which is
    /// the in-process limitation noted in the design (new-vocabulary adds need the
    /// deferred feature-model-epoch machinery).
    pub fn build(norm: Normalizer, config: &ClusterConfig, queries: &[(u64, String)]) -> Self {
        assert!(config.num_shards > 0, "cluster needs at least one shard");
        let norm = Arc::new(norm);

        // Pass A — build the authoritative dict over the WHOLE corpus, then freeze.
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut extracted: Vec<(u64, Extracted, String)> = Vec::with_capacity(queries.len());
        for (logical, text) in queries {
            if let Ok(ast) = crate::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                extracted.push((*logical, ex, text.clone()));
            }
        }
        dict.finalize_mask();
        let dict = Arc::new(dict);

        let ring = HashRing::new(config.num_shards, config.vnodes);
        let shards: Vec<Box<dyn Shard>> = (0..config.num_shards)
            .map(|_| {
                Box::new(LocalShard::new(
                    Arc::clone(&norm),
                    Arc::clone(&dict),
                    config.per_shard.clone(),
                )) as Box<dyn Shard>
            })
            .collect();

        let cluster = Self::from_parts(norm, dict, ring, shards, config.include_broad);

        // Pass B — bucket by placement, then ingest one base segment per shard.
        let mut buckets: Vec<Vec<(u64, Extracted, String, u32)>> =
            (0..config.num_shards).map(|_| Vec::new()).collect();
        for (logical, ex, text) in extracted {
            match cluster.placement(&ex) {
                Target::Reject => {}
                Target::Replicated => buckets[0].push((logical, ex, text, 1)),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push((logical, ex.clone(), text.clone(), 1));
                    }
                }
            }
        }
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                cluster.shards[s].ingest_extracted(&bucket);
            }
        }
        cluster
    }

    /// Assemble a cluster from pre-built parts — the construction seam shared by
    /// [`Self::build`] (which supplies `LocalShard`s) and the distributed builder /
    /// gRPC integration test (which supply boxed `RemoteShard`s). `shards.len()` must
    /// equal `ring.num_shards()`.
    pub(crate) fn from_parts(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        ring: HashRing,
        shards: Vec<Box<dyn Shard>>,
        include_broad: bool,
    ) -> Self {
        assert_eq!(
            shards.len(),
            ring.num_shards(),
            "shard count must match the ring's shard count"
        );
        ClusterEngine {
            norm,
            dict,
            ring,
            shards,
            include_broad,
        }
    }

    /// The placement decision for one compiled query — see the module-level table.
    fn placement(&self, ex: &Extracted) -> Target {
        let ap = anchor_plan(ex, &self.dict);
        match ap.class {
            CostClass::D => Target::Reject,
            CostClass::C => Target::Replicated,
            CostClass::A | CostClass::B => {
                // A class-B-arity-2 query's only main anchor is an all-hot PAIR
                // (a len-2 group): it has no rare feature to hash on, so it joins
                // the replicated lane. Class A and class-B any-of have only arity-1
                // non-hot anchors, which the ring distributes selectively.
                if ap.main_anchors.iter().any(|g| g.len() != 1) {
                    return Target::Replicated;
                }
                let mut shards: Vec<usize> = ap
                    .main_anchors
                    .iter()
                    .filter_map(|g| g.first().copied())
                    .map(|f| self.ring.lookup(f))
                    .collect();
                shards.sort_unstable();
                shards.dedup();
                if shards.is_empty() {
                    Target::Reject
                } else {
                    Target::Selective(shards)
                }
            }
        }
    }

    /// Add one query incrementally (lands in the target shard's memtable). Uses a
    /// read-only compile against the frozen shared dict, so vocabulary not seen at
    /// [`Self::build`] time is dropped (the deferred new-vocabulary limitation).
    pub fn add_query(&self, id: u64, dsl: &str) -> AddOutcome {
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return AddOutcome::RejectedParse(e),
        };
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        match self.placement(&ex) {
            Target::Reject => AddOutcome::RejectedClassD,
            Target::Replicated => {
                self.shards[0].insert_extracted(&ex, id, 1, dsl);
                AddOutcome::Replicated
            }
            Target::Selective(shards) => {
                for &s in &shards {
                    self.shards[s].insert_extracted(&ex, id, 1, dsl);
                }
                AddOutcome::Placed { shards }
            }
        }
    }

    /// Remove a query by logical id. Fans the (idempotent) delete out to every
    /// shard and sums the count — sidestepping any placement journal (a replicated
    /// or any-of query may live on several shards; a re-add may have moved it).
    pub fn remove_query(&self, id: u64) -> usize {
        self.shards.iter().map(|s| s.delete_by_logical_id(id)).sum()
    }

    /// Seal every shard's memtable into an immutable base segment.
    pub fn flush(&self) {
        for s in &self.shards {
            s.flush();
        }
    }

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
    pub fn percolate(&self, title: &str) -> Vec<u64> {
        self.percolate_inner(title, self.include_broad).0
    }

    /// [`Self::percolate`] plus merged [`MatchStats`] across the probed shards.
    pub fn percolate_with_stats(&self, title: &str) -> (Vec<u64>, MatchStats) {
        self.percolate_inner(title, self.include_broad)
    }

    /// Match one title with an explicit broad-lane toggle (overriding the cluster
    /// default) — used by the oracle to sweep broad on/off on one cluster.
    pub fn percolate_with_broad(&self, title: &str, include_broad: bool) -> Vec<u64> {
        self.percolate_inner(title, include_broad).0
    }

    fn percolate_inner(&self, title: &str, include_broad: bool) -> (Vec<u64>, MatchStats) {
        let targets = self.route(title);
        // Broad is evaluated ONLY on shard 0 (the replicated lane); selective
        // shards hold only main-index queries, so probing their (empty) broad
        // index would be pure waste — and double-counting a broadcast query.
        let parts: Vec<(Vec<u64>, MatchStats)> = if targets.len() <= 1 {
            targets
                .iter()
                .map(|&s| self.shards[s].percolate(title, include_broad && s == 0))
                .collect()
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(|&s| self.shards[s].percolate(title, include_broad && s == 0))
                .collect()
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
        (out, stats)
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
    pub fn num_queries(&self) -> usize {
        self.shards.iter().map(|s| s.num_queries()).sum()
    }

    /// Per-shard physical query counts (introspection / tests).
    pub fn shard_query_counts(&self) -> Vec<usize> {
        self.shards.iter().map(|s| s.num_queries()).collect()
    }

    /// Cluster-wide per-class entry tally `[A, B, C, D]`, summed across shards
    /// (replicated/any-of queries counted per holding shard). Used by the oracle
    /// to assert each placement branch is actually exercised.
    pub fn class_counts(&self) -> [u64; 4] {
        let mut total = [0u64; 4];
        for s in &self.shards {
            let c = s.class_counts();
            for i in 0..4 {
                total[i] += c[i];
            }
        }
        total
    }
}
