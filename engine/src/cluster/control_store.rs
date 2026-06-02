//! Durable on-disk persistence for the openraft control-plane backend
//! (`control_raft.rs`), clustering build-path step 5e / ADR-041.
//!
//! Design: docs/design/clustering-and-scaling.md §4.3 (control plane), §10 step 5e.
//!
//! ADR-038 shipped the openraft backend with an **in-memory** log/state store — enough to
//! prove consensus convergence, but a manager node lost everything on restart. This module is
//! the byte-level durable substrate that lets [`RaftControlPlane`](super::control_raft) survive a
//! restart and rejoin the quorum. Two shapes:
//!
//! - a **CRC-framed append-only record log** ([`append_record`] / [`read_records`] /
//!   [`rewrite_records`]) — the Raft log entries, reusing the same forward-scan / torn-tail
//!   recovery shape as [`clog`](super::clog) / `wal.rs` (a crash mid-append drops the last partial
//!   frame, never corrupts an acknowledged prefix); and
//! - **atomic single-value files** ([`write_value`] / [`read_value`]) — the Raft hard state that
//!   must survive a crash whole: the **vote** (election safety), the **committed** log id (so a
//!   restart re-applies committed-but-un-snapshotted entries), the **last-purged** log id, and the
//!   **state-machine snapshot** (so the log can be compacted + the SM rebuilt). Written tmp +
//!   fsync + rename + parent-fsync, so a reader never sees a torn value.
//!
//! What openraft requires durable (0.9.24, from its storage FAQ) and where it lives:
//!
//! - `save_vote` MUST be durable before returning → [`RaftPaths::vote`] (fsync each write).
//! - `append` MUST be durable before the flush callback → the record log (fsync if asked).
//! - `save_committed` makes a restart re-apply `(snapshot.last, committed]` → [`RaftPaths::committed`].
//! - a snapshot lets `purge` compact the log + rebuilds the SM on restart → [`RaftPaths::snapshot`].
//!
//! The state machine itself is NOT persisted per-apply — it is rebuilt from the snapshot + the
//! durable log replayed up to `committed`, exactly as openraft prescribes.
//!
//! All of this is `distributed`-gated (openraft only); serialization is `serde_json` (the same
//! codec the gRPC `RaftNetwork` already uses — the control plane is low-rate, so JSON overhead
//! is irrelevant and the files stay debuggable). CRC via the core [`crate::storage::crc32`].

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::storage::crc32;

/// Header of the record log: magic + format version. A fresh/missing file has no header until
/// the first [`ensure_log`].
const LOG_MAGIC: [u8; 4] = *b"RRRL"; // Reverse-Rusty Raft Log
const LOG_VERSION: u32 = 1;
const LOG_HEADER: usize = 8;

/// The set of files the durable control-plane store keeps under one manager node's raft dir.
pub(super) struct RaftPaths {
    dir: PathBuf,
}

impl RaftPaths {
    pub(super) fn new(dir: PathBuf) -> Self {
        RaftPaths { dir }
    }
    /// The CRC-framed Raft log (entries).
    pub(super) fn log(&self) -> PathBuf {
        self.dir.join("raft-log.bin")
    }
    /// The persisted hard-state vote (election safety).
    pub(super) fn vote(&self) -> PathBuf {
        self.dir.join("raft-vote.json")
    }
    /// The persisted committed log id (so a restart re-applies committed entries).
    pub(super) fn committed(&self) -> PathBuf {
        self.dir.join("raft-committed.json")
    }
    /// The persisted last-purged log id (the log's lower bound after compaction).
    pub(super) fn purged(&self) -> PathBuf {
        self.dir.join("raft-purged.json")
    }
    /// The persisted state-machine snapshot (meta + serialized document).
    pub(super) fn snapshot(&self) -> PathBuf {
        self.dir.join("raft-snapshot.json")
    }
}

/// Ensure the log file exists with a valid header, and return an **append** handle. Creating
/// the dir + header is idempotent; a real I/O failure surfaces.
pub(super) fn ensure_log(path: &Path) -> io::Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if !path.exists() {
        let mut f = std::fs::File::create(path)?;
        f.write_all(&LOG_MAGIC)?;
        f.write_all(&LOG_VERSION.to_le_bytes())?;
        f.sync_all()?;
    }
    std::fs::OpenOptions::new().append(true).open(path)
}

/// Append one serde record to an open append handle: `len u32 | crc u32 | json(body)`. fsync
/// (durable before return) when `fsync` is set, else flush to the OS page cache. The framing +
/// torn-tail recovery mirror [`clog`](super::clog) / `wal.rs`.
pub(super) fn append_record<T: Serialize>(
    file: &mut std::fs::File,
    value: &T,
    fsync: bool,
) -> io::Result<()> {
    let body =
        serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let crc = crc32(&body);
    file.write_all(&(body.len() as u32).to_le_bytes())?;
    file.write_all(&crc.to_le_bytes())?;
    file.write_all(&body)?;
    if fsync {
        file.sync_all()
    } else {
        file.flush()
    }
}

/// Read every valid record from a log file, oldest-first (forward scan, stopping at the first
/// bad-CRC / truncated frame — a torn tail from a crash, which was never acknowledged durable so
/// dropping it is safe). A missing file reads as empty (a fresh node).
pub(super) fn read_records<T: DeserializeOwned>(path: &Path) -> io::Result<Vec<T>> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    if data.len() < LOG_HEADER || data[0..4] != LOG_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raft log: bad magic or too small",
        ));
    }
    let get_u32 = |off: usize| -> Option<u32> {
        data.get(off..off + 4)
            .and_then(|s| s.try_into().ok())
            .map(u32::from_le_bytes)
    };
    let mut out = Vec::new();
    let mut cursor = LOG_HEADER;
    while cursor + 8 <= data.len() {
        let Some(len) = get_u32(cursor).map(|v| v as usize) else {
            break;
        };
        let Some(stored_crc) = get_u32(cursor + 4) else {
            break;
        };
        cursor += 8;
        if cursor + len > data.len() {
            break; // truncated body (torn tail)
        }
        let body = &data[cursor..cursor + len];
        if crc32(body) != stored_crc {
            break; // bad CRC (torn tail)
        }
        match serde_json::from_slice::<T>(body) {
            Ok(v) => out.push(v),
            Err(_) => break, // unparseable record — treat as torn tail, drop it + everything after
        }
        cursor += len;
    }
    Ok(out)
}

/// Atomically rewrite the log to exactly `records` (header + framed bodies) — the durable form of
/// `truncate` / `purge`, which drop a suffix / prefix. tmp + fsync + rename + parent-fsync, so a
/// crash mid-rewrite leaves the old (consistent) file in place.
pub(super) fn rewrite_records<T: Serialize>(
    path: &Path,
    records: &[T],
    fsync: bool,
) -> io::Result<()> {
    let tmp = path.with_extension("bin.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&LOG_MAGIC)?;
    f.write_all(&LOG_VERSION.to_le_bytes())?;
    for value in records {
        let body =
            serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let crc = crc32(&body);
        f.write_all(&(body.len() as u32).to_le_bytes())?;
        f.write_all(&crc.to_le_bytes())?;
        f.write_all(&body)?;
    }
    if fsync {
        f.sync_all()?;
    }
    drop(f);
    std::fs::rename(&tmp, path)?;
    if fsync {
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    Ok(())
}

/// Atomically write one serde value (vote / committed / purged / snapshot) — `json` to a tmp
/// file, fsync, rename over the target, fsync the parent dir, so a reader never sees a torn value.
pub(super) fn write_value<T: Serialize>(path: &Path, value: &T, fsync: bool) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body =
        serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let tmp = path.with_extension("json.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&body)?;
    if fsync {
        f.sync_all()?;
    }
    drop(f);
    std::fs::rename(&tmp, path)?;
    if fsync {
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    Ok(())
}

/// Read one serde value back; `Ok(None)` if the file is absent (a fresh node). A present but
/// unparseable value is a fail-loud error (never silently treated as absent, which would drop
/// hard state).
pub(super) fn read_value<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let v = serde_json::from_slice::<T>(&data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("rr_ctrlstore_{}_{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn log_round_trips_and_appends_are_monotonic() {
        let dir = scratch("log");
        let path = dir.join("raft-log.bin");
        {
            let mut f = ensure_log(&path).unwrap();
            append_record(&mut f, &(1u64, "a".to_string()), true).unwrap();
            append_record(&mut f, &(2u64, "b".to_string()), true).unwrap();
        }
        let recs: Vec<(u64, String)> = read_records(&path).unwrap();
        assert_eq!(recs, vec![(1, "a".into()), (2, "b".into())]);
        // Reopen + append keeps prior records.
        {
            let mut f = ensure_log(&path).unwrap();
            append_record(&mut f, &(3u64, "c".to_string()), false).unwrap();
        }
        let recs: Vec<(u64, String)> = read_records(&path).unwrap();
        assert_eq!(recs.len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_drops_a_torn_tail() {
        let dir = scratch("torn");
        let path = dir.join("raft-log.bin");
        {
            let mut f = ensure_log(&path).unwrap();
            append_record(&mut f, &(1u64, "alpha".to_string()), true).unwrap();
            append_record(&mut f, &(2u64, "beta".to_string()), true).unwrap();
        }
        // Corrupt the tail with bytes that can't frame a valid record.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[0x10, 0, 0, 0, 0xAA, 0xBB, 0xCC]).unwrap();
        }
        let recs: Vec<(u64, String)> = read_records(&path).unwrap();
        assert_eq!(recs.len(), 2, "the two whole records survive a torn tail");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rewrite_drops_prefix_and_suffix() {
        let dir = scratch("rewrite");
        let path = dir.join("raft-log.bin");
        {
            let mut f = ensure_log(&path).unwrap();
            for i in 1..=5u64 {
                append_record(&mut f, &(i, "x".to_string()), false).unwrap();
            }
        }
        // Keep only records 2..=4 (a purge of 1 + a truncate of 5).
        let kept: Vec<(u64, String)> = vec![(2, "x".into()), (3, "x".into()), (4, "x".into())];
        rewrite_records(&path, &kept, true).unwrap();
        let recs: Vec<(u64, String)> = read_records(&path).unwrap();
        assert_eq!(recs, kept);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn value_round_trips_and_absent_is_none() {
        let dir = scratch("value");
        let path = dir.join("raft-vote.json");
        assert!(read_value::<(u64, bool)>(&path).unwrap().is_none());
        write_value(&path, &(7u64, true), true).unwrap();
        assert_eq!(read_value::<(u64, bool)>(&path).unwrap(), Some((7, true)));
        // Overwrite is atomic + last-writer-wins.
        write_value(&path, &(9u64, false), true).unwrap();
        assert_eq!(read_value::<(u64, bool)>(&path).unwrap(), Some((9, false)));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
