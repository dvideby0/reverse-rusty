//! Point-in-time (PIT) registry primitives (ADR-113).
//!
//! A PIT pins an immutable payload — locally an `Arc<EngineSnapshot>`, at the
//! cluster coordinator the placement metadata for a set of per-shard pins —
//! for a bounded keep-alive so cursor pagination can page over one frozen view.
//! The registry is deliberately dumb serving-layer state: ids are process-local
//! (client-facing opacity/integrity is the HTTP layer's HMAC token, not this
//! type), expiry uses an injected clock (the `RetentionLeases` pattern, no
//! background thread — callers reap lazily on every registry touch), and the
//! whole registry dying with the process is the designed restart semantics:
//! a reopened server cannot serve any prior generation, so every old cursor
//! fails closed as stale.

use std::time::{Duration, Instant};

use crate::util::FastMap;

/// Process-local identifier of one open PIT.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PitId(pub u64);

/// Admission bounds for one PIT registry.
#[derive(Clone, Copy, Debug)]
pub struct PitConfig {
    /// Keep-alive applied when the caller does not request one.
    pub default_keep_alive: Duration,
    /// Hard ceiling on the per-PIT keep-alive; larger requests are rejected.
    pub max_keep_alive: Duration,
    /// Hard ceiling on concurrently open PITs; breaches are rejected, never
    /// evicted (an evicted PIT would silently break someone else's cursor).
    pub max_open: usize,
}

impl Default for PitConfig {
    fn default() -> Self {
        Self {
            default_keep_alive: Duration::from_mins(1),
            max_keep_alive: Duration::from_mins(10),
            max_open: 64,
        }
    }
}

/// Typed PIT admission failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PitError {
    /// The registry is at `max_open` live entries.
    LimitExceeded { max: usize },
    /// The requested keep-alive exceeds `max_keep_alive`.
    KeepAliveTooLarge { requested_s: u64, max_s: u64 },
}

impl std::fmt::Display for PitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LimitExceeded { max } => {
                write!(f, "too many open point-in-time snapshots (max {max})")
            }
            Self::KeepAliveTooLarge { requested_s, max_s } => {
                write!(f, "keep_alive {requested_s}s exceeds maximum {max_s}s")
            }
        }
    }
}

impl std::error::Error for PitError {}

struct PitEntry<T> {
    payload: T,
    keep_alive: Duration,
    deadline: Instant,
}

/// TTL'd map of open PITs. Not internally synchronized — callers wrap it in
/// their own lock (server `Mutex`, coordinator `Mutex`).
pub struct PitRegistry<T> {
    next: u64,
    entries: FastMap<u64, PitEntry<T>>,
}

impl<T> Default for PitRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> PitRegistry<T> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: 0,
            entries: crate::util::fast_map(),
        }
    }

    /// Open a PIT pinning `payload` under the keep-alive and open-count
    /// bounds. Callers must [`Self::reap_expired`] first (and release the
    /// reaped payloads' resources) so expired entries never occupy cap slots;
    /// `open` itself does not reap because the caller — not this registry —
    /// owns whatever cleanup a reaped payload requires.
    pub fn open(
        &mut self,
        payload: T,
        keep_alive: Option<Duration>,
        cfg: &PitConfig,
        now: Instant,
    ) -> Result<PitId, PitError> {
        let keep_alive = keep_alive.unwrap_or(cfg.default_keep_alive);
        let deadline = entry_deadline(now, keep_alive, cfg)?;
        if self.entries.len() >= cfg.max_open {
            return Err(PitError::LimitExceeded { max: cfg.max_open });
        }
        let id = self.next;
        self.next = self.next.wrapping_add(1);
        self.entries.insert(
            id,
            PitEntry {
                payload,
                keep_alive,
                deadline,
            },
        );
        Ok(PitId(id))
    }

    /// Resolve a live PIT, renewing its deadline to `now + keep_alive`
    /// (renew-on-use). An unknown or expired id returns `None` — the caller's
    /// stale-cursor signal.
    pub fn touch(&mut self, id: PitId, now: Instant) -> Option<&T> {
        let entry = self.entries.get_mut(&id.0)?;
        if entry.deadline < now {
            self.entries.remove(&id.0);
            return None;
        }
        entry.deadline = now.checked_add(entry.keep_alive)?;
        Some(&self.entries.get(&id.0)?.payload)
    }

    /// Close a PIT, returning its payload so the caller can release any
    /// per-shard pins. Closing an unknown/expired id is a `None` no-op.
    pub fn close(&mut self, id: PitId) -> Option<T> {
        self.entries.remove(&id.0).map(|entry| entry.payload)
    }

    /// Remove every expired entry, returning the payloads for cleanup.
    pub fn reap_expired(&mut self, now: Instant) -> Vec<(PitId, T)> {
        let expired: Vec<u64> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.deadline < now)
            .map(|(&id, _)| id)
            .collect();
        expired
            .into_iter()
            .filter_map(|id| {
                self.entries
                    .remove(&id)
                    .map(|entry| (PitId(id), entry.payload))
            })
            .collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Validate the keep-alive bound and compute the entry deadline. `checked_add`
/// keeps the no-panic contract even for absurd durations.
fn entry_deadline(
    now: Instant,
    keep_alive: Duration,
    cfg: &PitConfig,
) -> Result<Instant, PitError> {
    let too_large = PitError::KeepAliveTooLarge {
        requested_s: keep_alive.as_secs(),
        max_s: cfg.max_keep_alive.as_secs(),
    };
    if keep_alive > cfg.max_keep_alive {
        return Err(too_large);
    }
    now.checked_add(keep_alive).ok_or(too_large)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> PitConfig {
        PitConfig {
            default_keep_alive: Duration::from_secs(10),
            max_keep_alive: Duration::from_secs(100),
            max_open: 2,
        }
    }

    #[test]
    fn open_touch_close_round_trip() {
        let mut reg: PitRegistry<&'static str> = PitRegistry::new();
        let now = Instant::now();
        let id = reg.open("a", None, &cfg(), now).unwrap();
        assert_eq!(reg.touch(id, now), Some(&"a"));
        assert_eq!(reg.close(id), Some("a"));
        assert_eq!(reg.touch(id, now), None);
        assert_eq!(reg.close(id), None);
        assert!(reg.is_empty());
    }

    #[test]
    fn expiry_is_exact_and_touch_renews() {
        let mut reg: PitRegistry<u32> = PitRegistry::new();
        let now = Instant::now();
        let id = reg
            .open(7, Some(Duration::from_secs(10)), &cfg(), now)
            .unwrap();
        // Alive exactly at the deadline, gone strictly after it.
        assert!(reg.touch(id, now + Duration::from_secs(10)).is_some());
        // The touch above renewed to t=20; expired at t=21.
        assert_eq!(reg.touch(id, now + Duration::from_secs(21)), None);
        assert!(reg.is_empty());

        // Without the renewing touch, the original deadline applies.
        let id2 = reg
            .open(8, Some(Duration::from_secs(10)), &cfg(), now)
            .unwrap();
        assert_eq!(reg.touch(id2, now + Duration::from_secs(11)), None);
    }

    #[test]
    fn cap_rejects_without_evicting_and_reap_frees_slots() {
        let mut reg: PitRegistry<u32> = PitRegistry::new();
        let now = Instant::now();
        let a = reg
            .open(1, Some(Duration::from_secs(5)), &cfg(), now)
            .unwrap();
        let _b = reg
            .open(2, Some(Duration::from_secs(50)), &cfg(), now)
            .unwrap();
        assert_eq!(
            reg.open(3, None, &cfg(), now),
            Err(PitError::LimitExceeded { max: 2 })
        );
        // Entry `a` alive: still full. After `a` expires, the caller's
        // reap-then-open sequence frees its slot.
        let later = now + Duration::from_secs(6);
        let reaped = reg.reap_expired(later);
        assert_eq!(reaped, vec![(a, 1)]);
        let c = reg.open(3, None, &cfg(), later).unwrap();
        assert_ne!(c, a);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn keep_alive_ceiling_is_enforced() {
        let mut reg: PitRegistry<u32> = PitRegistry::new();
        let now = Instant::now();
        let err = reg
            .open(1, Some(Duration::from_secs(101)), &cfg(), now)
            .unwrap_err();
        assert_eq!(
            err,
            PitError::KeepAliveTooLarge {
                requested_s: 101,
                max_s: 100
            }
        );
        assert!(reg.is_empty());
    }

    #[test]
    fn reap_returns_only_expired_payloads() {
        let mut reg: PitRegistry<u32> = PitRegistry::new();
        let now = Instant::now();
        let a = reg
            .open(1, Some(Duration::from_secs(5)), &cfg(), now)
            .unwrap();
        let b = reg
            .open(2, Some(Duration::from_secs(50)), &cfg(), now)
            .unwrap();
        let reaped = reg.reap_expired(now + Duration::from_secs(10));
        assert_eq!(reaped, vec![(a, 1)]);
        assert_eq!(reg.len(), 1);
        assert!(reg.touch(b, now + Duration::from_secs(10)).is_some());
    }

    #[test]
    fn ids_are_never_reused() {
        let mut reg: PitRegistry<u32> = PitRegistry::new();
        let now = Instant::now();
        let a = reg.open(1, None, &cfg(), now).unwrap();
        reg.close(a);
        let b = reg.open(2, None, &cfg(), now).unwrap();
        assert_ne!(a, b);
    }
}
