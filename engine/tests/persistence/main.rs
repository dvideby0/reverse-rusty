//! Persistence tests — segment round-trip, mmap matching, and WAL recovery.
//!
//! These tests verify:
//! 1. A segment serialized to disk and mmap'd back produces identical match results
//! 2. WAL recovery after simulated crash restores the memtable
//! 3. Compaction works correctly with mmap'd segments
//! 4. The full lifecycle: build → persist → close → reopen → match

mod harness;

mod backup;
mod compaction;
mod durability;
mod round_trip;
mod sources;
mod tombstone_durability;
mod upsert;
mod wal;
