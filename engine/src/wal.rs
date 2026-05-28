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
            WalEntry::Insert { seq, .. } => *seq,
            WalEntry::Tombstone { seq, .. } => *seq,
            WalEntry::FlushCheckpoint { seq, .. } => *seq,
        }
    }
}

/// Append-only write-ahead log.
pub struct Wal {
    file: std::fs::File,
    path: PathBuf,
    next_seq: u64,
}

impl Wal {
    /// Open or create a WAL file. If the file exists, scans it to find the next
    /// sequence number. If it doesn't exist, creates it with a header.
    pub fn open(path: &Path) -> io::Result<Self> {
        if path.exists() {
            // Open existing, find the max sequence number
            let entries = Self::read_entries(path)?;
            let next_seq = entries.iter().map(|e| e.seq()).max().unwrap_or(0) + 1;
            let file = std::fs::OpenOptions::new().append(true).open(path)?;
            Ok(Wal {
                file,
                path: path.to_path_buf(),
                next_seq,
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
            })
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
        self.file.flush()?;
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
        self.file.flush()?;
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

    /// Read all valid entries from a WAL file. Entries with bad CRC (torn writes)
    /// are silently skipped.
    fn read_entries(path: &Path) -> io::Result<Vec<WalEntry>> {
        let data = std::fs::read(path)?;
        if data.len() < WAL_HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "WAL too small"));
        }
        if &data[0..4] != &WAL_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad WAL magic"));
        }
        Self::parse_entries(&data[WAL_HEADER_SIZE..])
    }

    fn parse_entries(data: &[u8]) -> io::Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut cursor = 0usize;

        while cursor + 8 <= data.len() {
            let total_len = u32::from_le_bytes(
                data[cursor..cursor + 4].try_into().unwrap()
            ) as usize;
            let stored_crc = u32::from_le_bytes(
                data[cursor + 4..cursor + 8].try_into().unwrap()
            );
            cursor += 8;

            if cursor + total_len > data.len() {
                break; // truncated entry (torn write)
            }

            let body = &data[cursor..cursor + total_len];
            if crc32(body) != stored_crc {
                break; // corrupt entry (torn write)
            }

            if total_len < 9 {
                break; // too short for seq + op
            }

            let seq = u64::from_le_bytes(body[0..8].try_into().unwrap());
            let op = body[8];
            let payload = &body[9..];

            match op {
                OP_INSERT => {
                    if payload.len() < 16 {
                        break;
                    }
                    let logical = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                    let version = u32::from_le_bytes(payload[8..12].try_into().unwrap());
                    let text_len = u32::from_le_bytes(payload[12..16].try_into().unwrap()) as usize;
                    if payload.len() < 16 + text_len {
                        break;
                    }
                    let text = std::str::from_utf8(&payload[16..16 + text_len])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                        .to_string();
                    entries.push(WalEntry::Insert { seq, logical, version, text });
                }
                OP_TOMBSTONE => {
                    if payload.len() < 8 {
                        break;
                    }
                    let seg_idx = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                    let local_id = u32::from_le_bytes(payload[4..8].try_into().unwrap());
                    entries.push(WalEntry::Tombstone { seq, seg_idx, local_id });
                }
                OP_FLUSH_CHECKPOINT => {
                    if payload.len() < 4 {
                        break;
                    }
                    let name_len = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
                    if payload.len() < 4 + name_len {
                        break;
                    }
                    let segment_file = std::str::from_utf8(&payload[4..4 + name_len])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                        .to_string();
                    entries.push(WalEntry::FlushCheckpoint { seq, segment_file });
                }
                _ => break, // unknown op
            }

            cursor += total_len;
        }

        Ok(entries)
    }

    /// Recover: read all entries, then return only those AFTER the last
    /// FlushCheckpoint (those are the un-materialized mutations).
    pub fn recover(path: &Path) -> io::Result<Vec<WalEntry>> {
        let all = Self::read_entries(path)?;
        // Find the last FlushCheckpoint
        let last_checkpoint_idx = all.iter().rposition(|e| {
            matches!(e, WalEntry::FlushCheckpoint { .. })
        });
        match last_checkpoint_idx {
            Some(idx) => Ok(all[idx + 1..].to_vec()),
            None => Ok(all), // no checkpoint — replay everything
        }
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
}
