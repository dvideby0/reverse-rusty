//! Unit tests for the translog retention-lease bookkeeping (ADR-040/048).

#[cfg(test)]
mod retention_lease_tests {
    use crate::cluster::shard::retention::RetentionLeases;
    use std::time::{Duration, Instant};

    // ADR-048: the TTL reap drops a lease that has not heartbeated within the window (a
    // crashed/stalled recovery) while keeping one that renewed recently (a live recovery).
    #[test]
    fn reap_expired_drops_stale_keeps_renewed() {
        // Synthetic instants are built by ADDING to `t0` (never subtracting) and the offsets are
        // not whole-minute multiples, so the clock math is panic-free and unit-clean.
        let ttl = Duration::from_secs(100);
        let t0 = Instant::now();
        let mut leases = RetentionLeases::default();

        // Two recoveries each pin a tail position; the floor is the min.
        let stale = leases.acquire(10, t0);
        let live = leases.acquire(20, t0);
        assert_eq!(
            leases.floor(),
            Some(10),
            "floor is the lowest pinned position"
        );

        // The live recovery heartbeats (renew) well inside the window (80s < ttl); the stale one
        // never does (last heartbeat stays t0).
        leases.renew(live, 25, t0 + Duration::from_secs(80));

        // Reap as of t0+150s: the stale lease (idle 150s > ttl) is expired; the live lease (idle
        // 150-80 = 70s < ttl) survives.
        let now = t0 + Duration::from_secs(150);
        let reaped = leases.reap_expired(now, ttl);
        assert_eq!(reaped, 1, "only the un-renewed lease is reaped");
        assert_eq!(
            leases.floor(),
            Some(25),
            "the renewed lease survives and still pins its (advanced) tail"
        );

        // Releasing the survivor clears the floor entirely; the reaped one is already gone.
        leases.release(live);
        assert_eq!(leases.floor(), None);
        let _ = stale;
    }

    // A reap with nothing past the TTL is a no-op (disabled-equivalent behavior within the window).
    #[test]
    fn reap_expired_keeps_everything_within_the_window() {
        let ttl = Duration::from_secs(100);
        let t0 = Instant::now();
        let mut leases = RetentionLeases::default();
        leases.acquire(5, t0);
        let reaped = leases.reap_expired(t0 + Duration::from_secs(50), ttl);
        assert_eq!(reaped, 0);
        assert_eq!(leases.floor(), Some(5));
    }
}
