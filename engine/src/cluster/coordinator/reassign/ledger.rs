//! The busy-endpoint move ledger (ADR-095, `distributed` feature): the concurrency guard for every
//! data-moving operation, replacing the ADR-090 whole-coordinator `reassign_serial: Mutex<()>` with
//! per-NODE granularity so moves touching disjoint node sets may run in parallel while every
//! conflicting pair still serializes.
//!
//! ## What must serialize (the conflict classes, ADR-090/092/094)
//! Two concurrent moves conflict exactly when they touch a common PHYSICAL node:
//! - **Chained reshuffle** — position `p`: F→T while position `q`: T→U makes T a handoff target and
//!   a fenced source at once; the drain-to-convergence proof assumes a quiescent fenced source.
//! - **Shared source** — two positions flipping off one node interleave their fence windows and
//!   repair queues.
//! - **Shared destination** — two moves onto one pending node race the dict-adopt/`AddShard`
//!   handshake.
//! - **Same position** — flip-vs-commit interleaving (the hazard `reassign_serial` was built for):
//!   any two moves of one position both name its current committed primary, so both reserve that
//!   node — serialized by construction.
//! - **Replicated composite installs** — a group move reserves every desired member, so no second
//!   move touches a member mid-assembly (ADR-094).
//!
//! ## Why endpoints, not `NodeId`s
//! The conflicts above are per *server process* (the fence, the slot map, the adopt handshake), and
//! two distinct `NodeId`s may resolve to one endpoint (`reassign_and_move` tolerates that as a
//! no-op) — a `NodeId`-keyed guard would let such aliases race. Every move already resolves its
//! members to endpoints fail-closed before reserving, so the ledger keys on normalized resolved
//! strings (scheme/host case and trailing slashes cannot bypass conflict identity).
//!
//! ## Liveness
//! [`MoveLedger::reserve`] is blocking and ALL-OR-NOTHING: a caller waits until its *whole* set is
//! free and never holds a partial reservation while waiting. Each move holds at most ONE ticket for
//! its lifetime (nested acquisition is structurally excluded — `execute_handoff_inner` exists so a
//! ticket-holding caller never re-reserves), so there is no hold-and-wait and therefore no deadlock.
//! Overlapping operator calls block exactly as they did under `reassign_serial`; disjoint calls now
//! proceed — the deliberate relaxation this ADR ships. The ledger is touched only on the rare
//! admin/autoscaler move path, never on the percolate/ingest hot path.

use std::collections::HashSet;
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Instant;

/// The set of endpoints currently participating in an in-flight data move (as source, target, or
/// group member), plus a condvar to wake reservers when a move completes. One per `ClusterEngine`.
pub(in crate::cluster::coordinator) struct MoveLedger {
    busy: Mutex<HashSet<String>>,
    freed: Condvar,
}

impl MoveLedger {
    pub(in crate::cluster::coordinator) fn new() -> Self {
        MoveLedger {
            busy: Mutex::new(HashSet::new()),
            freed: Condvar::new(),
        }
    }

    /// Reserve every endpoint in `eps` (deduped), blocking until the WHOLE set is free — never
    /// holding a partial set while waiting (no hold-and-wait ⇒ deadlock-free given each caller
    /// holds at most one ticket). Returns an RAII [`MoveTicket`] that releases the set on drop —
    /// including during unwind, so a panicking move never wedges its nodes.
    ///
    /// An empty `eps` returns an empty ticket without blocking (nothing to conflict with); callers
    /// always reserve their resolved endpoints, so this arises only in tests.
    pub(in crate::cluster::coordinator) fn reserve<S: AsRef<str>>(
        &self,
        eps: &[S],
    ) -> MoveTicket<'_> {
        let mut want: Vec<String> = eps.iter().map(|e| normalized(e.as_ref())).collect();
        want.sort_unstable();
        want.dedup();
        let mut busy = self.busy.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if want.iter().all(|e| !busy.contains(e)) {
                for e in &want {
                    busy.insert(e.clone());
                }
                return MoveTicket {
                    ledger: self,
                    eps: want,
                };
            }
            busy = self
                .freed
                .wait(busy)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Reserve every endpoint before `deadline`, or return `None` without
    /// reserving anything. A deadline that has already elapsed is still one
    /// immediate, non-blocking attempt; this is the manager-timeout `0`
    /// behavior used by strict operator APIs.
    pub(in crate::cluster::coordinator) fn reserve_until<S: AsRef<str>>(
        &self,
        eps: &[S],
        deadline: Instant,
    ) -> Option<MoveTicket<'_>> {
        let mut want: Vec<String> = eps.iter().map(|e| normalized(e.as_ref())).collect();
        want.sort_unstable();
        want.dedup();
        let mut busy = self.busy.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if want.iter().all(|e| !busy.contains(e)) {
                for e in &want {
                    busy.insert(e.clone());
                }
                return Some(MoveTicket {
                    ledger: self,
                    eps: want,
                });
            }
            let remaining = deadline.checked_duration_since(Instant::now())?;
            let waited = self.freed.wait_timeout(busy, remaining);
            let (next, timeout) = match waited {
                Ok(pair) => pair,
                Err(poisoned) => poisoned.into_inner(),
            };
            busy = next;
            if timeout.timed_out() && want.iter().any(|e| busy.contains(e)) {
                return None;
            }
        }
    }
}

/// Endpoint scheme/host case and one or more trailing slashes are formatting,
/// not distinct physical servers. Every reservation uses the normalized key so
/// spelling variants cannot bypass conflict serialization.
fn normalized(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_ascii_lowercase()
}

/// An RAII reservation of a move's endpoint footprint in the [`MoveLedger`]. Dropping it (on any
/// exit path, including unwind) releases the endpoints and wakes every waiting reserver.
pub(in crate::cluster::coordinator) struct MoveTicket<'a> {
    ledger: &'a MoveLedger,
    eps: Vec<String>,
}

impl Drop for MoveTicket<'_> {
    fn drop(&mut self) {
        let mut busy = self
            .ledger
            .busy
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for e in &self.eps {
            busy.remove(e);
        }
        drop(busy);
        self.ledger.freed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::Duration;

    /// Two reservations over disjoint endpoint sets are both granted without blocking — the
    /// parallelism the ledger exists to allow.
    #[test]
    fn reserve_disjoint_sets_do_not_block() {
        let ledger = MoveLedger::new();
        let a = ledger.reserve(&["http://a:1", "http://b:1"]);
        let b = ledger.reserve(&["http://c:1", "http://d:1"]);
        drop(a);
        drop(b);
    }

    /// A reservation overlapping a held one blocks until the holder's ticket drops — the
    /// serialization `reassign_serial` provided, now scoped to the conflicting nodes only.
    #[test]
    fn reserve_overlapping_blocks_until_release() {
        let ledger = Arc::new(MoveLedger::new());
        let first = ledger.reserve(&["http://a:1", "http://b:1"]);
        let (granted_tx, granted_rx) = mpsc::channel();
        let waiter = {
            let ledger = Arc::clone(&ledger);
            std::thread::spawn(move || {
                let t = ledger.reserve(&["http://b:1", "http://c:1"]);
                granted_tx.send(()).expect("report grant");
                drop(t);
            })
        };
        // The waiter must NOT be granted while `first` holds the shared endpoint.
        assert!(
            granted_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "an overlapping reserve was granted while the conflicting ticket was held"
        );
        drop(first);
        granted_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the waiter is granted once the conflicting ticket drops");
        waiter.join().expect("waiter thread");
    }

    /// Dropping a ticket releases its endpoints: a subsequent overlapping reserve succeeds
    /// immediately.
    #[test]
    fn ticket_drop_releases_endpoints() {
        let ledger = MoveLedger::new();
        drop(ledger.reserve(&["http://a:1"]));
        // Would block forever if the drop had not released the endpoint.
        drop(ledger.reserve(&["http://a:1"]));
    }

    /// Deadline admission never leaves a partial reservation behind, and a
    /// later caller can acquire the previously-conflicting endpoint normally.
    #[test]
    fn reserve_until_times_out_without_reserving() {
        let ledger = MoveLedger::new();
        let held = ledger.reserve(&["http://a:1"]);
        assert!(
            ledger
                .reserve_until(
                    &["http://a:1", "http://b:1"],
                    Instant::now() + Duration::from_millis(20),
                )
                .is_none(),
            "the overlapping reservation must respect its deadline"
        );
        let b = ledger
            .reserve_until(&["http://b:1"], Instant::now())
            .expect("the timed-out waiter never reserved its disjoint endpoint");
        drop(b);
        drop(held);
    }

    /// An elapsed deadline is a try-once operation rather than an automatic
    /// refusal, matching `cluster_manager_timeout=0` when the ledger is idle.
    #[test]
    fn reserve_until_elapsed_deadline_still_tries_once() {
        let ledger = MoveLedger::new();
        let ticket = ledger
            .reserve_until(&["http://a:1"], Instant::now())
            .expect("an idle endpoint is admitted immediately");
        drop(ticket);
    }

    /// Formatting variants of one physical endpoint still conflict; otherwise
    /// a caller-controlled spelling could bypass the fence-window ledger.
    #[test]
    fn endpoint_identity_is_normalized() {
        let ledger = MoveLedger::new();
        let held = ledger.reserve(&["HTTP://NODE:50051/"]);
        assert!(
            ledger
                .reserve_until(&["http://node:50051"], Instant::now())
                .is_none(),
            "case and trailing slash must not create a disjoint reservation"
        );
        drop(held);
    }

    /// A ticket held by a PANICKING thread is released during unwind (RAII), so a crashed move
    /// never wedges its nodes for every later move.
    #[test]
    fn ticket_releases_on_holder_panic() {
        let ledger = Arc::new(MoveLedger::new());
        let holder = {
            let ledger = Arc::clone(&ledger);
            std::thread::spawn(move || {
                let _t = ledger.reserve(&["http://a:1"]);
                panic!("simulated move panic");
            })
        };
        assert!(holder.join().is_err(), "the holder panicked");
        // Granted immediately: the unwind released the reservation (and any poison is recovered).
        drop(ledger.reserve(&["http://a:1"]));
    }

    /// All-or-nothing: a waiter blocked on {A, B} (A held) must not RESERVE B while waiting — a
    /// third caller wanting only B is granted, proving the waiter holds nothing.
    #[test]
    fn reserve_is_all_or_nothing() {
        let ledger = Arc::new(MoveLedger::new());
        let a = ledger.reserve(&["http://a:1"]);
        let (granted_tx, granted_rx) = mpsc::channel();
        let waiter = {
            let ledger = Arc::clone(&ledger);
            std::thread::spawn(move || {
                let t = ledger.reserve(&["http://a:1", "http://b:1"]);
                granted_tx.send(()).expect("report grant");
                drop(t);
            })
        };
        // Give the waiter time to block on {A, B}.
        std::thread::sleep(Duration::from_millis(50));
        // B must still be free: the blocked waiter holds NO partial reservation.
        drop(ledger.reserve(&["http://b:1"]));
        drop(a);
        granted_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the waiter is granted once A frees");
        waiter.join().expect("waiter thread");
    }
}
