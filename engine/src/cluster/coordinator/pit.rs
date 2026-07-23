//! ADR-113 coordinator PIT lifecycle + pit-scoped bounded ranking.
//!
//! A cluster PIT is index-wide and title-independent (the ES shape): `open`
//! pins EVERY position's current snapshot under one coordinator-allocated id,
//! so any later title routes to already-pinned shards. The registry entry
//! records the placement identity the pins were taken under; a page whose
//! current placement differs (a vocab/resize rebuild happened) fails closed as
//! stale BEFORE any shard call — the rebuild dropped the old `LocalShard`s and
//! their pins with them, and serving the new generation into an old cursor
//! would be exactly the generation mixing ADR-113 forbids.

use std::sync::PoisonError;
use std::time::{Duration, Instant};

use crate::pit::{PitConfig, PitError, PitId};
use crate::rank::CompiledRankProgram;
use crate::result::TopKOptions;

use super::ranked::{ClusterRankedError, ClusterRankedMatch};
use super::ClusterEngine;

/// Placement identity a PIT's per-shard pins were taken under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClusterPitMeta {
    pub(crate) generation: crate::ownership::PlacementGeneration,
    pub(crate) num_shards: u32,
}

/// Typed failures from the PIT lifecycle (open). Reads fail through
/// [`ClusterRankedError::StalePit`] instead.
#[derive(Clone, Debug)]
pub enum ClusterPitError {
    /// A shard in this assembly cannot pin snapshots (a remote/wire-backed
    /// shard) — carries the refusal with its alternative.
    Unsupported(String),
    /// Registry admission (cap / keep-alive ceiling).
    Admission(PitError),
}

impl std::fmt::Display for ClusterPitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(detail) => write!(f, "{detail}"),
            Self::Admission(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ClusterPitError {}

impl ClusterEngine {
    /// The shared frozen normalizer — the serving layer computes cursor
    /// fingerprints against it (under a valid PIT the current normalizer IS
    /// the pinned one: any vocab change bumps the placement generation, which
    /// stales the PIT first).
    pub fn normalizer(&self) -> &crate::normalize::Normalizer {
        &self.norm
    }

    /// The shared frozen dict — the fingerprint's feature-id space (same
    /// pinned-≡-current argument as [`Self::normalizer`]).
    pub fn dict(&self) -> &crate::dict::Dict {
        &self.dict
    }

    /// Open an index-wide PIT: reap expired entries (releasing their shard
    /// pins), admit under `cfg`, then pin every position's current snapshot.
    /// Fails closed: any shard refusal releases the already-placed pins and
    /// the registry entry.
    pub fn open_pit(
        &self,
        keep_alive: Option<Duration>,
        cfg: &PitConfig,
        now: Instant,
    ) -> Result<PitId, ClusterPitError> {
        self.reap_pits(now);
        // ADR-113 mutation barrier (WRITE side): every live mutation entry
        // point holds the read side from before its coordinator-log append
        // through the complete shard fan-out, so the pin fan below observes
        // either all of a mutation or none of it on every shard — a PIT is
        // always a view that actually existed. Taken AFTER the reap (which
        // fans shard closes) and held through registry admission + the pin
        // fan; no shard engine lock is taken under it, so no cycle with the
        // writers (codex review).
        let _mutations_excluded = self
            .pit_open_barrier
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let meta = ClusterPitMeta {
            generation: self.placement_generation(),
            num_shards: self.shards.len() as u32,
        };
        let pit = self
            .lock_pits()
            .open(meta, keep_alive, cfg, now)
            .map_err(ClusterPitError::Admission)?;
        for (position, shard) in self.shards.iter().enumerate() {
            if let Err(error) = shard.open_pit(pit.0) {
                for pinned in &self.shards[..position] {
                    pinned.close_pit(pit.0).ok();
                }
                self.lock_pits().close(pit);
                return Err(ClusterPitError::Unsupported(error.to_string()));
            }
        }
        Ok(pit)
    }

    /// Close a PIT, releasing every shard pin. Reaps expired entries first
    /// (releasing THEIR pins too), so an expired target honestly reports
    /// `false` and a DELETE-first client still frees the cap (codex review).
    /// `false` = already gone (expired/closed/rebuilt) — the caller's goal
    /// state either way.
    pub fn close_pit(&self, pit: PitId, now: Instant) -> bool {
        self.reap_pits(now);
        let existed = self.lock_pits().close(pit).is_some();
        if existed {
            for shard in &self.shards {
                shard.close_pit(pit.0).ok();
            }
        }
        existed
    }

    /// Open PITs currently registered (introspection/metrics).
    pub fn open_pit_count(&self) -> usize {
        self.lock_pits().len()
    }

    /// Resolve + renew a PIT and gate it on the recorded placement identity —
    /// the serving layer's pre-check, so a stale PIT is classified 409 BEFORE
    /// any fingerprint comparison against the (possibly rebuilt) normalizer
    /// could mis-classify it as a client mismatch. The kernel re-gates.
    pub fn check_pit(&self, pit: PitId, now: Instant) -> Result<(), ClusterRankedError> {
        self.reap_pits(now);
        let meta = match self.lock_pits().touch(pit, now) {
            Some(meta) => *meta,
            None => return Err(ClusterRankedError::StalePit),
        };
        if meta.generation != self.placement_generation()
            || meta.num_shards != self.shards.len() as u32
        {
            self.lock_pits().close(pit);
            return Err(ClusterRankedError::StalePit);
        }
        Ok(())
    }

    /// Pit-scoped exact distributed top K (ADR-113): resolve + renew the PIT,
    /// gate on the recorded placement identity, then run the ONE bounded fan
    /// with every shard reading its pinned snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn try_percolate_filtered_top_k_pit(
        &self,
        pit: PitId,
        title: &str,
        filter: &[(String, Vec<String>)],
        options: TopKOptions,
        program: &CompiledRankProgram,
        deadline: Option<Instant>,
        now: Instant,
    ) -> Result<ClusterRankedMatch, ClusterRankedError> {
        self.check_pit(pit, now)?;
        self.top_k_core(Some(pit.0), title, filter, options, program, deadline)
    }

    pub(super) fn lock_pits(
        &self,
    ) -> std::sync::MutexGuard<'_, crate::pit::PitRegistry<ClusterPitMeta>> {
        self.pits.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Reap expired registry entries and release their shard pins. Lazy — run
    /// at every PIT-API touch (the RetentionLeases pattern, no background
    /// thread); the shard fan happens after the registry lock is dropped.
    fn reap_pits(&self, now: Instant) {
        let reaped = self.lock_pits().reap_expired(now);
        for (pit, _) in reaped {
            for shard in &self.shards {
                shard.close_pit(pit.0).ok();
            }
        }
    }

    /// Release every registry entry (preserving id uniqueness) — called when a
    /// blue/green rebuild replaces the shard set: the old shards (and their
    /// pins) are gone, so the entries can only ever 409; dropping them frees
    /// cap slots immediately. Ids are never reused, so a stale cursor cannot
    /// alias a post-rebuild PIT.
    pub(super) fn clear_pits(&self) {
        drop(self.lock_pits().clear());
    }
}
