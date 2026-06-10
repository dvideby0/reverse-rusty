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

use std::io::{self, Write};
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
// reports skipped bytes.
const WAL_VERSION: u32 = 5;
const WAL_HEADER_SIZE: usize = 8; // magic + version

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

impl Wal {
    /// Open or create a WAL file. If the file exists, scans it to find the next
    /// sequence number. If it doesn't exist, creates it with a header.
    ///
    /// `fsync_each_write` selects the per-append durability policy (see
    /// [`Wal::fsync_each_write`]).
    pub fn open(path: &Path, fsync_each_write: bool) -> io::Result<Self> {
        if path.exists() {
            // Open existing, find the max sequence number and current pending count.
            let (entries, _skipped) = Self::read_entries(path)?;
            let next_seq = entries.iter().map(WalEntry::seq).max().unwrap_or(0) + 1;
            // Pending = entries after the last checkpoint (same set recover() replays).
            let pending_entries = match entries
                .iter()
                .rposition(|e| matches!(e, WalEntry::FlushCheckpoint { .. }))
            {
                Some(idx) => (entries.len() - idx - 1) as u64,
                None => entries.len() as u64,
            };
            let size_bytes = std::fs::metadata(path)?.len();
            let file = std::fs::OpenOptions::new().append(true).open(path)?;
            Ok(Wal {
                file,
                path: path.to_path_buf(),
                next_seq,
                fsync_each_write,
                size_bytes,
                pending_entries,
            })
        } else {
            // Create new
            let mut file = std::fs::File::create(path)?;
            file.write_all(&WAL_MAGIC)?;
            file.write_all(&WAL_VERSION.to_le_bytes())?;
            file.sync_all()?;
            Ok(Wal {
                file,
                path: path.to_path_buf(),
                next_seq: 1,
                fsync_each_write,
                size_bytes: WAL_HEADER_SIZE as u64,
                pending_entries: 0,
            })
        }
    }

    /// Flush an append to its configured durability level: an `fsync` (durable
    /// across power loss) when `fsync_each_write` is set, otherwise a userspace
    /// flush that leaves the bytes in the OS page cache until the next
    /// checkpoint (durable across process crash only).
    #[inline]
    fn sync_after_append(&mut self) -> io::Result<()> {
        if self.fsync_each_write {
            self.file.sync_all()
        } else {
            self.file.flush()
        }
    }

    /// Append an Insert entry. Returns the sequence number assigned. `tags` are the
    /// query's `(key, value)` metadata pairs (ADR-049); pass `&[]` for an untagged insert.
    pub fn append_insert(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_INSERT, logical, version, text, tags)
    }

    /// Append an Insert accepted under the class-D lane (WAL v5, ADR-068). Same
    /// payload as [`append_insert`](Self::append_insert); the op code is the
    /// per-frame accept marker, so replay can store it unconditionally while legacy
    /// op-0 frames (logged before classification by pre-v5 binaries) still replay
    /// under the old reject gate.
    pub fn append_insert_class_d(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_INSERT_CLASS_D, logical, version, text, tags)
    }

    /// Append an Upsert entry (WAL v4, ADR-067) — the atomic replace-by-id. Same
    /// payload as Insert; the op code is what tells recovery to tombstone the prior
    /// live copies of `logical` before inserting this version.
    pub fn append_upsert(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_UPSERT, logical, version, text, tags)
    }

    /// Append an Upsert accepted under the class-D lane (WAL v5, ADR-068) — see
    /// [`append_insert_class_d`](Self::append_insert_class_d).
    pub fn append_upsert_class_d(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_UPSERT_CLASS_D, logical, version, text, tags)
    }

    /// Shared encoder for the two insert-shaped ops (Insert / Upsert): identical
    /// payload layout, different op byte.
    fn append_insert_like(
        &mut self,
        op: u8,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let text_bytes = text.as_bytes();
        // tag section: tag_count(2) + per tag key_len(2)+key + val_len(2)+value
        let mut tag_bytes = Vec::new();
        tag_bytes.extend_from_slice(&(tags.len() as u16).to_le_bytes());
        for (k, v) in tags {
            let kb = k.as_bytes();
            let vb = v.as_bytes();
            tag_bytes.extend_from_slice(&(kb.len() as u16).to_le_bytes());
            tag_bytes.extend_from_slice(kb);
            tag_bytes.extend_from_slice(&(vb.len() as u16).to_le_bytes());
            tag_bytes.extend_from_slice(vb);
        }
        // payload: logical(8) + version(4) + text_len(4) + text + tag section
        let payload_len = 8 + 4 + 4 + text_bytes.len() + tag_bytes.len();
        // entry body: seq(8) + op(1) + payload
        let body_len = 8 + 1 + payload_len;

        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(op);
        body.extend_from_slice(&logical.to_le_bytes());
        body.extend_from_slice(&version.to_le_bytes());
        body.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(text_bytes);
        body.extend_from_slice(&tag_bytes);

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.sync_after_append()?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a Tombstone entry.
    pub fn append_tombstone(&mut self, seg_idx: u32, local_id: u32) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let mut body = Vec::with_capacity(8 + 1 + 8);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_TOMBSTONE);
        body.extend_from_slice(&seg_idx.to_le_bytes());
        body.extend_from_slice(&local_id.to_le_bytes());

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.sync_after_append()?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a DeleteByLogical entry (WAL v3, ADR-066): the address-free
    /// "tombstone every live copy of `logical`" mutation logged by
    /// [`Engine::delete_by_logical_id`](crate::segment::Engine::delete_by_logical_id).
    /// One frame per delete, regardless of how many physical copies it removes.
    pub fn append_delete_logical(&mut self, logical: u64) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let mut body = Vec::with_capacity(8 + 1 + 8);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_DELETE_LOGICAL);
        body.extend_from_slice(&logical.to_le_bytes());

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.sync_after_append()?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a FlushCheckpoint entry. Indicates that all prior WAL entries
    /// have been materialized into sealed segments.
    pub fn append_flush_checkpoint(&mut self, segment_file: &str) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let name_bytes = segment_file.as_bytes();
        let mut body = Vec::with_capacity(8 + 1 + 4 + name_bytes.len());
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_FLUSH_CHECKPOINT);
        body.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(name_bytes);

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.sync_all()?; // fsync on checkpoint
        self.size_bytes += 8 + body.len() as u64; // length prefix + CRC + body
        self.pending_entries = 0; // checkpoint materializes all prior mutations
        Ok(seq)
    }

    /// Sync the WAL to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Current on-disk WAL size in bytes (header + framed entries).
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Number of un-checkpointed entries (mutations not yet in a sealed segment).
    pub fn pending_entries(&self) -> u64 {
        self.pending_entries
    }

    /// The sequence number of the last appended entry (0 if none yet). Sequence
    /// numbers stay monotonic across [`reset`](Self::reset), so this is a valid
    /// high-water mark for the manifest's `wal_seq_watermark` (ADR-066).
    pub fn last_seq(&self) -> u64 {
        self.next_seq - 1
    }

    /// Pin the next sequence number past `watermark` (ADR-066). `reset` keeps the
    /// sequence monotonic only in memory: reopening a reset (header-only) WAL file
    /// rescans it and restarts at 1, while the manifest keeps its old watermark —
    /// so without this, frames appended after the reopen would sort at or below
    /// the watermark and be wrongly skipped by the next recovery (a resurrected
    /// delete). [`Engine::open`](crate::segment::Engine::open) calls this with the
    /// recovered manifest's watermark.
    pub fn ensure_seq_after(&mut self, watermark: u64) {
        if self.next_seq <= watermark {
            self.next_seq = watermark + 1;
        }
    }

    /// Read all valid entries from a WAL file. Returns entries and the byte
    /// count of any trailing data that could not be parsed.
    fn read_entries(path: &Path) -> io::Result<(Vec<WalEntry>, usize)> {
        let data = std::fs::read(path)?;
        if data.len() < WAL_HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "WAL too small"));
        }
        if data[0..4] != WAL_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad WAL magic"));
        }
        Self::parse_entries(&data[WAL_HEADER_SIZE..])
    }

    fn parse_entries(data: &[u8]) -> io::Result<(Vec<WalEntry>, usize)> {
        fn get_u16(buf: &[u8], off: usize) -> Option<u16> {
            buf.get(off..off + 2)
                .and_then(|s| s.try_into().ok())
                .map(u16::from_le_bytes)
        }
        fn get_u32(buf: &[u8], off: usize) -> Option<u32> {
            buf.get(off..off + 4)
                .and_then(|s| s.try_into().ok())
                .map(u32::from_le_bytes)
        }
        fn get_u64(buf: &[u8], off: usize) -> Option<u64> {
            buf.get(off..off + 8)
                .and_then(|s| s.try_into().ok())
                .map(u64::from_le_bytes)
        }

        let mut entries = Vec::new();
        let mut cursor = 0usize;
        // End of the last FULLY-validated frame. `cursor` advances past a frame's
        // len+CRC header before the body is validated, so on a corrupt frame it sits 8
        // bytes into unparseable data — reporting `skipped_bytes` from `cursor` would
        // silently under-count the corrupt frame's own header.
        let mut consumed = 0usize;

        while cursor + 8 <= data.len() {
            let total_len = match get_u32(data, cursor) {
                Some(v) => v as usize,
                None => break,
            };
            let Some(stored_crc) = get_u32(data, cursor + 4) else {
                break;
            };
            cursor += 8;

            if cursor + total_len > data.len() {
                break;
            }

            let body = &data[cursor..cursor + total_len];
            if crc32(body) != stored_crc {
                break;
            }

            if total_len < 9 {
                break;
            }

            let Some(seq) = get_u64(body, 0) else {
                break;
            };
            let op = body[8];
            let payload = &body[9..];

            match op {
                // Insert/Upsert (WAL v4, ADR-067) and their class-D-accepted twins
                // (WAL v5, ADR-068) share one payload layout; the op byte selects the
                // decoded variant + the accept marker.
                OP_INSERT | OP_UPSERT | OP_INSERT_CLASS_D | OP_UPSERT_CLASS_D => {
                    if payload.len() < 16 {
                        break;
                    }
                    let Some(logical) = get_u64(payload, 0) else {
                        break;
                    };
                    let Some(version) = get_u32(payload, 8) else {
                        break;
                    };
                    let text_len = match get_u32(payload, 12) {
                        Some(v) => v as usize,
                        None => break,
                    };
                    if payload.len() < 16 + text_len {
                        break;
                    }
                    let text = std::str::from_utf8(&payload[16..16 + text_len])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                        .to_string();
                    // Optional tag section (WAL v2). A v1 entry ends after `text` (no
                    // trailing bytes), so its tags read back empty. The entry CRC has
                    // already passed, so the section is intact; the bounds checks are
                    // belt-and-suspenders.
                    let mut tags: Vec<(String, String)> = Vec::new();
                    let mut p = 16 + text_len;
                    if let Some(tag_count) = get_u16(payload, p) {
                        p += 2;
                        for _ in 0..tag_count {
                            let Some(kl) = get_u16(payload, p).map(usize::from) else {
                                break;
                            };
                            p += 2;
                            let Some(kb) = payload.get(p..p + kl) else {
                                break;
                            };
                            p += kl;
                            let Some(vl) = get_u16(payload, p).map(usize::from) else {
                                break;
                            };
                            p += 2;
                            let Some(vb) = payload.get(p..p + vl) else {
                                break;
                            };
                            p += vl;
                            let key = std::str::from_utf8(kb)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                                .to_string();
                            let value = std::str::from_utf8(vb)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                                .to_string();
                            tags.push((key, value));
                        }
                    }
                    entries.push(if op == OP_INSERT || op == OP_INSERT_CLASS_D {
                        WalEntry::Insert {
                            seq,
                            logical,
                            version,
                            text,
                            tags,
                            class_d_accepted: op == OP_INSERT_CLASS_D,
                        }
                    } else {
                        WalEntry::Upsert {
                            seq,
                            logical,
                            version,
                            text,
                            tags,
                            class_d_accepted: op == OP_UPSERT_CLASS_D,
                        }
                    });
                }
                OP_TOMBSTONE => {
                    if payload.len() < 8 {
                        break;
                    }
                    let Some(seg_idx) = get_u32(payload, 0) else {
                        break;
                    };
                    let Some(local_id) = get_u32(payload, 4) else {
                        break;
                    };
                    entries.push(WalEntry::Tombstone {
                        seq,
                        seg_idx,
                        local_id,
                    });
                }
                OP_DELETE_LOGICAL => {
                    if payload.len() < 8 {
                        break;
                    }
                    let Some(logical) = get_u64(payload, 0) else {
                        break;
                    };
                    entries.push(WalEntry::DeleteByLogical { seq, logical });
                }
                OP_FLUSH_CHECKPOINT => {
                    if payload.len() < 4 {
                        break;
                    }
                    let name_len = match get_u32(payload, 0) {
                        Some(v) => v as usize,
                        None => break,
                    };
                    if payload.len() < 4 + name_len {
                        break;
                    }
                    let segment_file = std::str::from_utf8(&payload[4..4 + name_len])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                        .to_string();
                    entries.push(WalEntry::FlushCheckpoint { seq, segment_file });
                }
                _ => break,
            }

            cursor += total_len;
            consumed = cursor;
        }

        let skipped_bytes = data.len() - consumed;
        Ok((entries, skipped_bytes))
    }

    /// Recover: read all entries, then return only those AFTER the last
    /// FlushCheckpoint (those are the un-materialized mutations).
    /// Returns a `WalRecovery` with entries to replay and skipped-bytes count.
    pub fn recover(path: &Path) -> io::Result<WalRecovery> {
        let (all, skipped_bytes) = Self::read_entries(path)?;
        let last_checkpoint_idx = all
            .iter()
            .rposition(|e| matches!(e, WalEntry::FlushCheckpoint { .. }));
        let entries = match last_checkpoint_idx {
            Some(idx) => all[idx + 1..].to_vec(),
            None => all,
        };
        Ok(WalRecovery {
            entries,
            skipped_bytes,
        })
    }

    /// Reset the WAL: truncate to just the header. Called after a successful
    /// compaction + manifest write when all data is in sealed segments.
    pub fn reset(&mut self) -> io::Result<()> {
        self.file = std::fs::File::create(&self.path)?;
        self.file.write_all(&WAL_MAGIC)?;
        self.file.write_all(&WAL_VERSION.to_le_bytes())?;
        self.file.sync_all()?;
        self.size_bytes = WAL_HEADER_SIZE as u64;
        self.pending_entries = 0;
        // Don't reset next_seq — keep it monotonic across resets
        Ok(())
    }

    /// Test-only: swap the underlying file for a read-only handle so subsequent
    /// appends fail with an `io::Error`, simulating a disk-full / EIO / revoked
    /// permission fault on a live WAL (an open fd is not affected by `chmod`, so
    /// this is the deterministic way to inject a write fault).
    #[cfg(test)]
    pub(crate) fn break_writes_for_test(&mut self) {
        self.file = std::fs::OpenOptions::new()
            .read(true)
            .open(&self.path)
            .expect("reopen WAL read-only");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_path(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "reverse_rusty_wal_{}_{}.log",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn append_surfaces_write_errors_instead_of_swallowing() {
        let path = scratch_path("append_err");
        let mut wal = Wal::open(&path, false).unwrap();
        // A healthy append succeeds.
        assert!(wal.append_insert(1, 1, "michael jordan", &[]).is_ok());
        // Once the file can no longer be written, the error is returned (not swallowed).
        wal.break_writes_for_test();
        assert!(wal.append_insert(2, 1, "scottie pippen", &[]).is_err());
        assert!(wal.append_tombstone(u32::MAX, 0).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fsync_each_write_round_trips_through_recovery() {
        let path = scratch_path("fsync_roundtrip");
        {
            let mut wal = Wal::open(&path, true).unwrap();
            wal.append_insert(7, 2, "wander franco", &[]).unwrap();
            wal.append_tombstone(0, 3).unwrap();
        }
        let recovered = Wal::recover(&path).unwrap();
        assert_eq!(recovered.entries.len(), 2);
        assert_eq!(recovered.skipped_bytes, 0);
        match &recovered.entries[0] {
            WalEntry::Insert {
                logical,
                version,
                text,
                ..
            } => {
                assert_eq!(*logical, 7);
                assert_eq!(*version, 2);
                assert_eq!(text, "wander franco");
            }
            other => panic!("expected Insert, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn insert_tags_round_trip_through_recovery_and_untagged_reads_empty() {
        let path = scratch_path("tags_roundtrip");
        {
            let mut wal = Wal::open(&path, true).unwrap();
            // A tagged insert (the ADR-049 case) and an untagged one.
            wal.append_insert(
                7,
                1,
                "1994 upper deck",
                &[
                    ("category".to_string(), "cards".to_string()),
                    ("status".to_string(), "active".to_string()),
                ],
            )
            .unwrap();
            wal.append_insert(8, 1, "no tags here", &[]).unwrap();
        }
        let recovered = Wal::recover(&path).unwrap();
        assert_eq!(recovered.entries.len(), 2);
        match &recovered.entries[0] {
            WalEntry::Insert { logical, tags, .. } => {
                assert_eq!(*logical, 7);
                assert_eq!(
                    tags,
                    &vec![
                        ("category".to_string(), "cards".to_string()),
                        ("status".to_string(), "active".to_string()),
                    ]
                );
            }
            other => panic!("expected Insert, got {other:?}"),
        }
        match &recovered.entries[1] {
            WalEntry::Insert { logical, tags, .. } => {
                assert_eq!(*logical, 8);
                assert!(tags.is_empty(), "an untagged insert recovers empty tags");
            }
            other => panic!("expected Insert, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_by_logical_round_trips_through_recovery() {
        let path = scratch_path("delete_logical_roundtrip");
        {
            let mut wal = Wal::open(&path, true).unwrap();
            wal.append_insert(7, 1, "wander franco", &[]).unwrap();
            wal.append_delete_logical(7).unwrap();
            // Old positional frames still coexist in the same file.
            wal.append_tombstone(u32::MAX, 3).unwrap();
        }
        let recovered = Wal::recover(&path).unwrap();
        assert_eq!(recovered.entries.len(), 3);
        assert_eq!(recovered.skipped_bytes, 0);
        match &recovered.entries[1] {
            WalEntry::DeleteByLogical { logical, .. } => assert_eq!(*logical, 7),
            other => panic!("expected DeleteByLogical, got {other:?}"),
        }
        match &recovered.entries[2] {
            WalEntry::Tombstone {
                seg_idx, local_id, ..
            } => {
                assert_eq!(*seg_idx, u32::MAX);
                assert_eq!(*local_id, 3);
            }
            other => panic!("expected Tombstone, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn upsert_round_trips_with_tags_and_coexists_with_insert() {
        let path = scratch_path("upsert_roundtrip");
        {
            let mut wal = Wal::open(&path, true).unwrap();
            wal.append_insert(7, 1, "wander franco", &[]).unwrap();
            wal.append_upsert(
                7,
                2,
                "wander franco psa 10",
                &[("category".to_string(), "cards".to_string())],
            )
            .unwrap();
        }
        let recovered = Wal::recover(&path).unwrap();
        assert_eq!(recovered.entries.len(), 2);
        assert_eq!(recovered.skipped_bytes, 0);
        match &recovered.entries[1] {
            WalEntry::Upsert {
                logical,
                version,
                text,
                tags,
                ..
            } => {
                assert_eq!(*logical, 7);
                assert_eq!(*version, 2);
                assert_eq!(text, "wander franco psa 10");
                assert_eq!(tags, &vec![("category".to_string(), "cards".to_string())]);
            }
            other => panic!("expected Upsert, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn last_seq_is_monotonic_across_reset() {
        let path = scratch_path("last_seq_monotonic");
        let mut wal = Wal::open(&path, false).unwrap();
        assert_eq!(wal.last_seq(), 0, "no entries yet");
        wal.append_insert(1, 1, "michael jordan", &[]).unwrap();
        wal.append_delete_logical(1).unwrap();
        assert_eq!(wal.last_seq(), 2);
        wal.reset().unwrap();
        assert_eq!(wal.last_seq(), 2, "reset must not rewind the watermark");
        wal.append_insert(2, 1, "scottie pippen", &[]).unwrap();
        assert_eq!(wal.last_seq(), 3);
        let _ = std::fs::remove_file(&path);
    }

    /// Micro-benchmark: per-write fsync vs. checkpoint-only. Ignored by default
    /// (it does real device flushes). Run with:
    ///   cargo test --release -p reverse-rusty --lib wal::tests::bench_fsync_cost -- --ignored --nocapture
    #[test]
    #[ignore = "benchmark: does real device flushes; run with --ignored"]
    fn bench_fsync_cost() {
        use std::time::Instant;
        const N: u64 = 5_000;
        for &(label, fsync) in &[
            ("checkpoint-only (fsync=false)", false),
            ("per-write fsync=true", true),
        ] {
            let path = scratch_path(&format!("bench_{fsync}"));
            let mut wal = Wal::open(&path, fsync).unwrap();
            let t = Instant::now();
            for i in 0..N {
                wal.append_insert(i, 1, "1994 upper deck michael jordan sp psa 10", &[])
                    .unwrap();
            }
            let per = t.elapsed().as_secs_f64() / N as f64;
            println!(
                "{label:35}: {:.1} us/append   ({:.0} appends/sec)",
                per * 1e6,
                1.0 / per
            );
            let _ = std::fs::remove_file(&path);
        }
    }
}
