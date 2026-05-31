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
use super::shard::{LocalShard, Shard, ShardError};

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

        // Construct concrete local shards so pass-B ingest can use the infallible
        // inherent path — `build` only ever makes `LocalShard`s (remote shards arrive
        // via `from_parts`), so it stays infallible while the trait is Result-typed.
        let locals: Vec<LocalShard> = (0..config.num_shards)
            .map(|_| {
                LocalShard::new(
                    Arc::clone(&norm),
                    Arc::clone(&dict),
                    config.per_shard.clone(),
                )
            })
            .collect();

        // Pass B — bucket by placement, then ingest one base segment per shard.
        let mut buckets: Vec<Vec<(u64, Extracted, String, u32)>> =
            (0..config.num_shards).map(|_| Vec::new()).collect();
        for (logical, ex, text) in extracted {
            match placement_of(&dict, &ring, &ex) {
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
                locals[s].ingest_local(&bucket);
            }
        }

        let shards: Vec<Box<dyn Shard>> = locals
            .into_iter()
            .map(|s| Box::new(s) as Box<dyn Shard>)
            .collect();
        Self::from_parts(norm, dict, ring, shards, config.include_broad)
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

    /// Bulk-load queries into an already-built (frozen-dict) cluster — the load path
    /// for a cluster assembled via [`Self::from_parts`] (e.g. a remote cluster), and
    /// the distributed analog of `build`'s pass B. Buckets each query by placement
    /// (compiling read-only against the shared frozen dict) and ingests each bucket
    /// into its shard through the seam. Parse failures and class-D queries are skipped
    /// (mirroring `build`); a shard write error propagates. Intended for a freshly
    /// assembled (empty) cluster — calling it on an already-populated one re-indexes
    /// those queries (duplicate entries).
    pub fn ingest(&self, queries: &[(u64, String)]) -> Result<(), ShardError> {
        let mut buckets: Vec<Vec<(u64, Extracted, String, u32)>> =
            (0..self.ring.num_shards()).map(|_| Vec::new()).collect();
        let mut lc = String::new();
        for (logical, text) in queries {
            let Ok(ast) = crate::dsl::parse(text) else {
                continue;
            };
            let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
            match self.placement(&ex) {
                Target::Reject => {}
                Target::Replicated => buckets[0].push((*logical, ex, text.clone(), 1)),
                Target::Selective(shs) => {
                    for &s in &shs {
                        buckets[s].push((*logical, ex.clone(), text.clone(), 1));
                    }
                }
            }
        }
        for (s, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.shards[s].ingest_extracted(&bucket)?;
            }
        }
        Ok(())
    }

    /// The placement decision for one compiled query — see the module-level table.
    /// Delegates to the free [`placement_of`] so `build` can bucket the corpus before
    /// the cluster value exists.
    fn placement(&self, ex: &Extracted) -> Target {
        placement_of(&self.dict, &self.ring, ex)
    }

    /// Add one query incrementally (lands in the target shard's memtable). Uses a
    /// read-only compile against the frozen shared dict, so vocabulary not seen at
    /// [`Self::build`] time is dropped (the deferred new-vocabulary limitation).
    pub fn add_query(&self, id: u64, dsl: &str) -> Result<AddOutcome, ShardError> {
        let ast = match crate::dsl::parse(dsl) {
            Ok(a) => a,
            Err(e) => return Ok(AddOutcome::RejectedParse(e)),
        };
        let mut lc = String::new();
        let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
        match self.placement(&ex) {
            Target::Reject => Ok(AddOutcome::RejectedClassD),
            Target::Replicated => {
                self.shards[0].insert_extracted(&ex, id, 1, dsl)?;
                Ok(AddOutcome::Replicated)
            }
            Target::Selective(shards) => {
                for &s in &shards {
                    self.shards[s].insert_extracted(&ex, id, 1, dsl)?;
                }
                Ok(AddOutcome::Placed { shards })
            }
        }
    }

    /// Remove a query by logical id. Fans the (idempotent) delete out to every
    /// shard and sums the count — sidestepping any placement journal (a replicated
    /// or any-of query may live on several shards; a re-add may have moved it).
    pub fn remove_query(&self, id: u64) -> Result<usize, ShardError> {
        self.shards.iter().map(|s| s.delete_by_logical_id(id)).sum()
    }

    /// Seal every shard's memtable into an immutable base segment.
    pub fn flush(&self) -> Result<(), ShardError> {
        for s in &self.shards {
            s.flush()?;
        }
        Ok(())
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
    pub fn percolate(&self, title: &str) -> Result<Vec<u64>, ShardError> {
        Ok(self.percolate_inner(title, self.include_broad)?.0)
    }

    /// [`Self::percolate`] plus merged [`MatchStats`] across the probed shards.
    pub fn percolate_with_stats(&self, title: &str) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.percolate_inner(title, self.include_broad)
    }

    /// Match one title with an explicit broad-lane toggle (overriding the cluster
    /// default) — used by the oracle to sweep broad on/off on one cluster.
    pub fn percolate_with_broad(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<Vec<u64>, ShardError> {
        Ok(self.percolate_inner(title, include_broad)?.0)
    }

    fn percolate_inner(
        &self,
        title: &str,
        include_broad: bool,
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
                .map(|&s| self.shards[s].percolate(title, include_broad && s == 0))
                .collect::<Result<_, _>>()?
        } else {
            use rayon::prelude::*;
            targets
                .par_iter()
                .map(|&s| self.shards[s].percolate(title, include_broad && s == 0))
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
}

/// gRPC remote-cluster construction (behind the `distributed` feature).
#[cfg(feature = "distributed")]
impl ClusterEngine {
    /// Assemble a cluster whose K shards are REMOTE (gRPC) — one per `endpoints[i]`,
    /// connected on the given tokio `handle`. `norm`/`dict` MUST be the same frozen
    /// feature space the servers were built over: placement + routing run here on the
    /// coordinator, while each server re-compiles DSL read-only against its copy of
    /// that dict, so the ids line up only when the dicts match (the shared-dict
    /// invariant extended across the wire). `endpoints.len()` must equal
    /// `config.num_shards`; endpoint `i` serves shard `i`. Load the corpus afterwards
    /// with [`Self::ingest`].
    ///
    /// CAVEAT — TODO(ADR-029): the dict match is currently **unverified**. A coordinator
    /// pointed at servers whose frozen dict diverged drops matches *silently* (the one
    /// false-negative path the fallible seam cannot catch). Until a connect-time
    /// dict-fingerprint handshake lands, only the shared-`Arc<Dict>` configuration
    /// (in-process, or the localhost oracle) is guaranteed correct.
    pub fn connect_remote(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        assert_eq!(
            endpoints.len(),
            config.num_shards,
            "connect_remote needs exactly one endpoint per shard"
        );
        let ring = HashRing::new(config.num_shards, config.vnodes);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(endpoints.len());
        for ep in endpoints {
            let remote = super::remote::RemoteShard::connect(ep.clone(), handle.clone())?;
            shards.push(Box::new(remote) as Box<dyn Shard>);
        }
        Ok(Self::from_parts(
            norm,
            dict,
            ring,
            shards,
            config.include_broad,
        ))
    }
}

/// The placement decision for one compiled query — see the module-level table. A free
/// fn over (`dict`, `ring`) so [`ClusterEngine::build`] can bucket the corpus before
/// the cluster value exists, and [`ClusterEngine::placement`] can delegate. Forbidden
/// features can't leak in: `anchor_plan` reads only `required`/`anyof`, never
/// `forbidden` (ADR-006 holds structurally).
fn placement_of(dict: &Dict, ring: &HashRing, ex: &Extracted) -> Target {
    let ap = anchor_plan(ex, dict);
    match ap.class {
        CostClass::D => Target::Reject,
        CostClass::C => Target::Replicated,
        CostClass::A | CostClass::B => {
            // A class-B-arity-2 query's only main anchor is an all-hot PAIR (a len-2
            // group): no rare feature to hash on, so it joins the replicated lane.
            // Class A and class-B any-of have only arity-1 non-hot anchors, which the
            // ring distributes selectively.
            if ap.main_anchors.iter().any(|g| g.len() != 1) {
                return Target::Replicated;
            }
            let mut shards: Vec<usize> = ap
                .main_anchors
                .iter()
                .filter_map(|g| g.first().copied())
                .map(|f| ring.lookup(f))
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
