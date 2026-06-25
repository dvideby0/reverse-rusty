//! Real-process SIGKILL crash-injection harness (ADR-088, Phase 0 item 3).
//!
//! Spawns the `crashwriter` bin, SIGKILLs it mid durable-operation, reopens the
//! data dir in-process, and diffs the recovered engine against the front-end-
//! INDEPENDENT reference matcher (`reverse-rusty-ref-matcher`, ADR-087). The
//! contract proved: every ACKnowledged write survives the kill + reopen (ZERO false
//! negatives — the cardinal guarantee), and recovery introduces no corruption / no
//! false positives. This closes the documented gap — every prior durability test is
//! a *simulation* (permission chmod, appended garbage, CRC bitflips, torn-tail);
//! nobody actually SIGKILLs the OS process mid-syscall.
//!
//! Scenarios, each a `--workload` of the crashwriter steering the kill into one
//! durable window:
//! - `wal_append` — a WAL frame's write + sync
//! - `flush` — a segment write + manifest commit
//! - `compaction` — a merge + manifest swap + WAL reset
//! - `backup` — staging + atomic rename (the source must stay intact)
//! - `churn` — insert + interleaved delete (the `DeleteByLogical` recovery path; a
//!   resurrected delete is a false positive)
//! - `upsert` — atomic replace-by-id (ADR-067); a crash recovers both halves or
//!   neither, never a half-state (vanished id / stale old version)
//! - `watermark` — the multi-reopen `ensure_seq_after` hazard (ADR-066): a delete
//!   appended after a checkpoint-and-reopen must survive a SECOND reopen
//!
//! These tests are `#[ignore]`d (they spawn + SIGKILL real processes and do real
//! fsyncs). Run them via the `check.sh` crash lane or directly:
//! ```text
//!   cargo test --release --test crash_injection -- --ignored --test-threads=1
//! ```
//! `RR_CRASH_ITERS` (default 3) scales the kill/reopen cycles per scenario.
//!
//! ## Verifying this harness actually BITES (mutation testing — run by hand)
//! A crash test that always passes is worthless. Each mutation below must turn the
//! suite RED; if any stays green the corresponding check is vacuous. These were run
//! during development and confirmed to fail:
//!
//! 1. **Recovery drops writes** (zero-FN check) — make `replay_wal_tail`
//!    (`segment/lifecycle/recovery.rs`) skip some recovered `WalEntry::Insert`s
//!    (e.g. `if logical % 3 != 0`) → FALSE NEGATIVES fire across every scenario.
//! 2. **Delete replay neutered** (resurrection / FP check) — skip
//!    `engine.apply_delete_by_logical(logical)` in `replay_wal_tail` → the churn
//!    scenario's durably-deleted ids resurrect and fire as FALSE POSITIVES.
//! 3. **No real kill** (the kill itself) — make `spawn_and_kill` `wait()` without
//!    `kill()` (or force `fire = false`) → the `res.killed` assert fires, proving
//!    the suite exercises a real SIGKILL, not a graceful round-trip (the exact
//!    weakness this item removes).
//! 4. **Upsert insert-half neutered** (vanish / cardinal-sin check) — in
//!    `replay_upsert` (`segment/ingest.rs`) tombstone the prior copies but skip the
//!    insert (`apply_delete_by_logical(logical)` instead of `apply_upsert`) → the
//!    `upsert` scenario fires "id VANISHED" on the both-title. Skipping the whole
//!    replay instead → "acked id's NEW version missing" (the version check).
//! 5. **Watermark re-pin neutered** (multi-reopen resurrection) — make
//!    `Wal::ensure_seq_after` (`wal.rs`) a no-op → the `watermark` scenario's canary
//!    delete is skipped on the SECOND reopen and resurrects (FP), while the
//!    single-reopen `churn` scenario stays GREEN under the same mutation — proving
//!    the watermark scenario covers a hazard churn structurally cannot.
//!
//! (An "ACK before the durable write" mutation is NOT a reliable bite for this
//! design: the writer's loop is sequential — `write_all` of the WAL frame is the
//! statement right after the ack — so the un-durable window is sub-microsecond. The
//! sync-before-ack ordering is asserted structurally by the protocol instead.)

mod backup;
mod churn;
mod compaction;
mod flush;
mod harness;
mod upsert;
mod wal_append;
mod watermark;
