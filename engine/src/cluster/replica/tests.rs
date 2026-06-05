//! `ReplicatedShard` unit tests (read failover, write fan-out/ack, peer recovery, durability).
//! Shared fixtures live in [`super::test_support`].
//!
//! This is the module ROOT: it only wires up the focused submodules, each holding a cohesive
//! group of `#[test]` fns under the ~650-line module budget. Each submodule reaches the parent
//! (`super::super`, i.e. the `replica` module) and the shared [`super::super::test_support`]
//! fixtures directly:
//!   - [`failover`]  — read failover + write fan-out/ack/aggregation (the in-memory composite)
//!   - [`recovery`]  — peer recovery, no-quiesce tail catch-up, durable self-restart, finalize
//!   - [`retention`] — retention leases across a concurrent seal + the stuck-lease TTL reap

mod failover;
mod recovery;
mod retention;
