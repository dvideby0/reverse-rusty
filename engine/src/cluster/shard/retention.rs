//! Translog retention-lease bookkeeping for [`LocalShard`](super::local::LocalShard)
//! (ADR-040/048).
//!
//! A peer recovery pins the source's un-sealed translog tail with a [`RetentionLeases`]
//! entry so a concurrent `seal_for_checkpoint` can't trim it out from under the
//! in-flight copy. The TTL reap ([`RetentionLeases::reap_expired`], ADR-048) drops a
//! lease a crashed/stalled recovery left behind. [`resolve_lease_ttl`] centralizes the
//! one config→`Duration` mapping every `LocalShard` constructor shares.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::config::EngineConfig;

/// One held retention lease: the pinned translog position plus the wall-clock of its last
/// acquire/renew. `last_renewed` is the heartbeat a TTL reap (ADR-048) measures against.
struct Lease {
    /// Ops `> pos` are retained for the recovery holding this lease.
    pos: u64,
    /// When this lease was last acquired or renewed — refreshed on every renew so an
    /// actively-progressing recovery never looks stale.
    last_renewed: Instant,
}

/// Translog retention leases (ADR-040): a set of `lease_id → `[`Lease`]. A recovery source
/// keeps every translog op strictly after `min(retained_pos)`, so an in-flight peer recovery's
/// tail is never trimmed out from under it by a concurrent seal. With no leases the floor is
/// absent and a seal trims to its checkpoint `P` — byte-identical to ADR-039.
///
/// A lease has no intrinsic expiry; a crashed recovering node would otherwise pin the source's
/// tail forever. The TTL reap ([`Self::reap_expired`], ADR-048) drops a lease that has not
/// heartbeated (acquire/renew) within the TTL, so a presumed-dead recovery can no longer hold
/// the floor. `renew` is the heartbeat, so a live recovery (which renews every catch-up pass) is
/// never reaped.
#[derive(Default)]
pub(super) struct RetentionLeases {
    next_id: u64,
    held: BTreeMap<u64, Lease>,
}

impl RetentionLeases {
    /// Register a lease pinning ops `> at`; returns its id. Stamps the heartbeat to `now`.
    pub(super) fn acquire(&mut self, at: u64, now: Instant) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.held.insert(
            id,
            Lease {
                pos: at,
                last_renewed: now,
            },
        );
        id
    }
    /// Advance a lease forward (monotonic — a lease never moves a consumer's cursor back) and
    /// refresh its heartbeat (this is what keeps a live recovery from being reaped).
    pub(super) fn renew(&mut self, id: u64, to: u64, now: Instant) {
        if let Some(l) = self.held.get_mut(&id) {
            l.pos = l.pos.max(to);
            l.last_renewed = now;
        }
    }
    pub(super) fn release(&mut self, id: u64) {
        self.held.remove(&id);
    }
    /// Drop every lease whose last heartbeat is older than `ttl` as of `now`; returns how many
    /// were reaped. `now` is a parameter (not `Instant::now()`) so the seal path injects the
    /// real clock while tests inject a synthetic one — fully deterministic, no sleeps.
    pub(super) fn reap_expired(&mut self, now: Instant, ttl: Duration) -> usize {
        let before = self.held.len();
        self.held
            .retain(|_, l| now.duration_since(l.last_renewed) < ttl);
        before - self.held.len()
    }
    /// The lowest pinned position across all leases (`None` ⇒ no lease ⇒ trim freely to `P`).
    pub(super) fn floor(&self) -> Option<u64> {
        self.held.values().map(|l| l.pos).min()
    }
}

/// Resolve the retention-lease TTL (ADR-048) from config: `0` ⇒ disabled (`None`, no expiry),
/// else `Some(Duration)`. Centralized so every `LocalShard` constructor derives it identically.
pub(super) fn resolve_lease_ttl(config: &EngineConfig) -> Option<Duration> {
    match config.retention_lease_ttl_secs {
        0 => None,
        secs => Some(Duration::from_secs(secs)),
    }
}
