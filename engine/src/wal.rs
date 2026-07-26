//! Write-ahead log — durable mutation log for crash recovery.
//!
//! Design: docs/DECISIONS.md (ADR-013)
//! Invariant: Every mutation that reaches the memtable MUST be in the WAL first;
//!   on recovery, replaying the WAL from the last checkpoint reproduces the
//!   memtable state exactly.
//!
//! ## Entry format
//!
//! Each entry is framed:
//! ```text
//!   total_len: u32        (bytes of header + payload, excluding this u32 and the CRC)
//!   crc32:     u32        (of everything after: seq + op + payload)
//!   seq:       u64        (monotonic sequence number)
//!   op:        u8         (0=Insert, 1=Tombstone, 2=FlushCheckpoint, 3=DeleteByLogical,
//!                          4=Upsert)
//!   payload:   [u8; ...]  (op-specific, variable length)
//! ```
//!
//! Insert payload: `logical: u64, version: u32, text_len: u32, text: [u8; text_len]`,
//!   then (WAL v2, ADR-049) an optional tag section: `tag_count: u16`, then per tag
//!   `key_len: u16, key, val_len: u16, value`. A v1 entry has no tag section (the payload
//!   ends after `text`); the parser detects this by the absence of trailing bytes, so v1
//!   and v2 entries coexist in one file (e.g. across a binary upgrade) and v1 entries
//!   read back untagged. Tags are not recoverable from `text`, so they must be logged.
//! Tombstone payload: `seg_idx: u32, local_id: u32`
//! FlushCheckpoint payload: `segment_file_len: u32, segment_file: [u8; ...]`
//! DeleteByLogical payload (WAL v3, ADR-066): `logical: u64` — the address-FREE delete.
//!   Replay re-derives the affected copies from the recovered state ("tombstone every
//!   live copy of `logical`"), so the frame stays correct across compaction's
//!   `(seg_idx, local)` renumbering, where a positional Tombstone frame would misfire.
//! Upsert payload (WAL v4, ADR-067): byte-identical to Insert — the atomic
//!   replace-by-id. ONE frame captures "tombstone every prior live copy of `logical`,
//!   then insert this version", so a crash can never recover the delete half without
//!   the insert half (the no-match window the DELETE-then-PUT recipe had).
//! WAL v6 (ADR-108): Insert/Upsert payloads may append one signed `priority: i64`
//!   after tags. WAL v7 (ADR-116) adds a marked source-generation extension:
//!   `SGEN, source_generation: u64, priority_present: u8, [priority: i64]`.
//!   New engine writes always carry the non-zero generation assigned before the
//!   append so recovery can preserve mutation order across WAL and bulk segments.
//!   The header version remains informational: old readers stop after the tag
//!   section (and ignore an extension they do not recognize).
//!
//! On recovery, we scan forward from the beginning, skipping entries with bad CRC
//! (torn writes from a crash). Entries before the last FlushCheckpoint are skipped
//! (those mutations are already in sealed segments).
//!
//! ## Durability policy
//!
//! Appends are `write(2)`-en immediately (reaching the OS page cache). Whether
//! they are also `fsync`'d per-append is controlled by `fsync_each_write` (see
//! [`EngineConfig::wal_sync_on_write`](crate::config::EngineConfig::wal_sync_on_write)):
//! off (default) fsyncs only at flush checkpoints, so an acknowledged write
//! survives a process crash but not a power loss until the next checkpoint; on
//! fsyncs every append, so it survives power loss at the cost of one device
//! flush per mutation. Either way, a failed append is returned to the caller
//! (never swallowed), so the engine rejects the mutation rather than
//! acknowledging a write it could not durably log.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::storage::crc32;

const WAL_MAGIC: [u8; 4] = *b"PWAL";
// v1: original layout. v2 (ADR-049): the Insert payload gains an optional trailing tag
// section. The version is informational — the parser detects per entry whether tags are
// present (by trailing bytes), so v1 and v2 entries coexist. v3 (ADR-066): adds the
// DeleteByLogical op; older entries are unchanged (an old binary reading a v3 tail stops
// at the first op-3 frame and reports it as skipped bytes, like a torn tail). v4
// (ADR-067): adds the Upsert op (atomic replace-by-id), same coexistence story. v5
// (ADR-068): adds the InsertClassD/UpsertClassD ops — payload-identical to
// Insert/Upsert, the op code itself marking "accepted under the class-D lane". The
// marker is load-bearing for UPGRADE correctness: binaries before v5 logged a frame
// BEFORE classifying, so an old file can hold op-0/op-4 frames whose write was
// acknowledged as RejectedClassD — replay applies the legacy ops under the old reject
// gate (reproducing the writer's decision) and only the op-5/6 frames as accepted.
// Same rollback story as v3/v4: an old binary stops at the first op-5/6 frame and
// reports skipped bytes. v6 (ADR-108) adds no opcode: an optional trailing i64
// extends insert-shaped payloads, so old readers safely ignore it after tags.
// v7 (ADR-116) replaces that unmarked tail on new writes with a marked source
// generation + optional priority extension. The marker and exact tail length
// distinguish v7 from v6 while older readers continue to ignore the extra bytes.
const WAL_VERSION: u32 = 7;
const WAL_HEADER_SIZE: usize = 8; // magic + version
const SOURCE_GENERATION_MAGIC: [u8; 4] = *b"SGEN";

const OP_INSERT: u8 = 0;
const OP_TOMBSTONE: u8 = 1;
const OP_FLUSH_CHECKPOINT: u8 = 2;
const OP_DELETE_LOGICAL: u8 = 3;
const OP_UPSERT: u8 = 4;
const OP_INSERT_CLASS_D: u8 = 5;
const OP_UPSERT_CLASS_D: u8 = 6;

/// A single WAL entry, decoded.
#[derive(Debug, Clone)]
pub enum WalEntry {
    Insert {
        seq: u64,
        logical: u64,
        version: u32,
        text: String,
        /// Per-query metadata tags (ADR-049), `(key, value)` pairs. Empty for a v1 entry
        /// or an untagged insert. Not derivable from `text`, so logged explicitly.
        tags: Vec<(String, String)>,
        /// Optional fixed typed priority appended by WAL v6. Legacy frames leave
        /// this absent and replay derives the compatibility value from tags.
        priority: Option<i64>,
        /// Engine-owned source generation appended by WAL v7. `None` denotes a
        /// legacy frame; replay gives those rows generation zero so pre-v8
        /// storage keeps its historical newest-first tie behavior.
        source_generation: Option<u64>,
        /// `true` ⇔ the frame's op is `OP_INSERT_CLASS_D` (WAL v5, ADR-068): the write
        /// was accepted under the class-D lane, so replay stores it unconditionally.
        /// A legacy op-0 frame (`false`) replays under the old reject gate — binaries
        /// before v5 logged BEFORE classifying, so an old file can hold frames whose
        /// write was acknowledged as `RejectedClassD`; accepting those on replay would
        /// resurrect a query the caller was told does not exist.
        class_d_accepted: bool,
    },
    Tombstone {
        seq: u64,
        seg_idx: u32,
        local_id: u32,
    },
    FlushCheckpoint {
        seq: u64,
        segment_file: String,
    },
    /// Address-free delete (WAL v3, ADR-066): tombstone every live copy of `logical`.
    /// The production delete path logs ONE of these instead of N positional Tombstones,
    /// so a compaction that renumbers `(seg_idx, local)` can never make the replay
    /// misfire into a different query (a silent false negative).
    DeleteByLogical {
        seq: u64,
        logical: u64,
    },
    /// Atomic replace-by-id (WAL v4, ADR-067): tombstone every prior live copy of
    /// `logical`, then insert this version — ONE frame, so recovery applies both
    /// halves or neither. Payload is byte-identical to [`Insert`](WalEntry::Insert).
    Upsert {
        seq: u64,
        logical: u64,
        version: u32,
        text: String,
        /// Per-query metadata tags (ADR-049), `(key, value)` pairs.
        tags: Vec<(String, String)>,
        /// Optional fixed typed priority appended by WAL v6.
        priority: Option<i64>,
        /// Engine-owned source generation appended by WAL v7. `None` denotes a
        /// legacy generation-zero frame.
        source_generation: Option<u64>,
        /// `true` ⇔ op `OP_UPSERT_CLASS_D` (WAL v5, ADR-068) — see
        /// [`Insert::class_d_accepted`](WalEntry::Insert). Doubly load-bearing here:
        /// replaying a legacy logged-but-rejected upsert as accepted would not just
        /// resurrect the new version, it would TOMBSTONE the acknowledged-live prior
        /// one — a false negative.
        class_d_accepted: bool,
    },
}

impl WalEntry {
    pub fn seq(&self) -> u64 {
        match self {
            WalEntry::Insert { seq, .. }
            | WalEntry::Tombstone { seq, .. }
            | WalEntry::FlushCheckpoint { seq, .. }
            | WalEntry::DeleteByLogical { seq, .. }
            | WalEntry::Upsert { seq, .. } => *seq,
        }
    }
}

/// Result of WAL recovery — entries to replay plus diagnostic info.
#[derive(Debug)]
pub struct WalRecovery {
    pub entries: Vec<WalEntry>,
    /// Bytes at the tail that could not be parsed (torn writes / corruption).
    pub skipped_bytes: usize,
}

/// Append-only write-ahead log.
pub struct Wal {
    file: std::fs::File,
    path: PathBuf,
    next_seq: u64,
    /// When true, every append `fsync`s before returning (durable across power
    /// loss). When false, appends only reach the OS page cache until the next
    /// checkpoint (durable across process crash only). See
    /// [`EngineConfig::wal_sync_on_write`](crate::config::EngineConfig::wal_sync_on_write).
    fsync_each_write: bool,
    /// Running on-disk size in bytes (header + all framed entries), maintained
    /// incrementally so it can be read without a `stat(2)`.
    size_bytes: u64,
    /// Count of data entries (Insert/Tombstone) appended since the last flush
    /// checkpoint or reset — mutations not yet materialized into a sealed
    /// segment. Mirrors the set replayed by [`Wal::recover`].
    pending_entries: u64,
}

mod recovery;
mod write;

#[cfg(test)]
mod tests;
