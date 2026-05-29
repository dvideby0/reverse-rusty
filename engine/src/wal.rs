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
//!   op:        u8         (0=Insert, 1=Tombstone, 2=FlushCheckpoint)
//!   payload:   [u8; ...]  (op-specific, variable length)
//! ```
//!
//! Insert payload: `logical: u64, version: u32, text_len: u32, text: [u8; text_len]`
//! Tombstone payload: `seg_idx: u32, local_id: u32`
//! FlushCheckpoint payload: `segment_file_len: u32, segment_file: [u8; ...]`
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
const WAL_VERSION: u32 = 1;
const WAL_HEADER_SIZE: usize = 8; // magic + version

const OP_INSERT: u8 = 0;
const OP_TOMBSTONE: u8 = 1;
const OP_FLUSH_CHECKPOINT: u8 = 2;

/// A single WAL entry, decoded.
#[derive(Debug, Clone)]
pub enum WalEntry {
    Insert {
        seq: u64,
        logical: u64,
        version: u32,
        text: String,
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
}

impl WalEntry {
    pub fn seq(&self) -> u64 {
        match self {
            WalEntry::Insert { seq, .. }
            | WalEntry::Tombstone { seq, .. }
            | WalEntry::FlushCheckpoint { seq, .. } => *seq,
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
}

impl Wal {
    /// Open or create a WAL file. If the file exists, scans it to find the next
    /// sequence number. If it doesn't exist, creates it with a header.
    ///
    /// `fsync_each_write` selects the per-append durability policy (see
    /// [`Wal::fsync_each_write`]).
    pub fn open(path: &Path, fsync_each_write: bool) -> io::Result<Self> {
        if path.exists() {
            // Open existing, find the max sequence number
            let (entries, _skipped) = Self::read_entries(path)?;
            let next_seq = entries.iter().map(WalEntry::seq).max().unwrap_or(0) + 1;
            let file = std::fs::OpenOptions::new().append(true).open(path)?;
            Ok(Wal {
                file,
                path: path.to_path_buf(),
                next_seq,
                fsync_each_write,
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

    /// Append an Insert entry. Returns the sequence number assigned.
    pub fn append_insert(&mut self, logical: u64, version: u32, text: &str) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let text_bytes = text.as_bytes();
        // payload: logical(8) + version(4) + text_len(4) + text
        let payload_len = 8 + 4 + 4 + text_bytes.len();
        // entry body: seq(8) + op(1) + payload
        let body_len = 8 + 1 + payload_len;

        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_INSERT);
        body.extend_from_slice(&logical.to_le_bytes());
        body.extend_from_slice(&version.to_le_bytes());
        body.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(text_bytes);

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.sync_after_append()?;
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
        Ok(seq)
    }

    /// Sync the WAL to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
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
                OP_INSERT => {
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
                    entries.push(WalEntry::Insert {
                        seq,
                        logical,
                        version,
                        text,
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
        }

        let skipped_bytes = data.len() - cursor;
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
            "percolator_wal_{}_{}.log",
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
        assert!(wal.append_insert(1, 1, "michael jordan").is_ok());
        // Once the file can no longer be written, the error is returned (not swallowed).
        wal.break_writes_for_test();
        assert!(wal.append_insert(2, 1, "scottie pippen").is_err());
        assert!(wal.append_tombstone(u32::MAX, 0).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fsync_each_write_round_trips_through_recovery() {
        let path = scratch_path("fsync_roundtrip");
        {
            let mut wal = Wal::open(&path, true).unwrap();
            wal.append_insert(7, 2, "wander franco").unwrap();
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

    /// Micro-benchmark: per-write fsync vs. checkpoint-only. Ignored by default
    /// (it does real device flushes). Run with:
    ///   cargo test --release -p percolator --lib wal::tests::bench_fsync_cost -- --ignored --nocapture
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
                wal.append_insert(i, 1, "1994 upper deck michael jordan sp psa 10")
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
