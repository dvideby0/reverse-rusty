//! `ClusterLog` — the coordinator's durable, ordered mutation log (the externalized
//! "log is the database" source of truth, clustering build-path step 3a / ADR-031).
//!
//! Design: docs/design/clustering-and-scaling.md §4.1 (durable log), §10 step 3.
//!
//! [`ClusterLog`] is to durability what [`Shard`](super::shard::Shard) is to a shard:
//! a sync, fallible, `Send + Sync` seam that abstracts the OPERATION, so the
//! single-node file backend ([`FileClusterLog`]) shipped here can later be swapped for a
//! Raft-backed one *without touching the coordinator* — `append` becomes a
//! quorum-commit, `replay` a committed-prefix read, `checkpoint` a snapshot install,
//! `epoch` the Raft term. A second backend ([`NullClusterLog`]) is the in-memory /
//! no-`data_dir` path and the fast test backend; running a churn script through both and
//! asserting identical results is the differential proof that coordinator behavior is
//! log-impl-independent.
//!
//! ## Why a separate file format (not the engine [`Wal`](crate::wal::Wal))
//! The engine WAL's tombstone is a *per-shard physical* `(seg_idx, local_id)`; the
//! coordinator mutates by *logical id*. The engine WAL's parser also treats an unknown
//! op code as a torn tail, so widening it for cluster ops is subtly wrong. We instead
//! copy its proven CRC-framing / forward-scan / torn-tail recovery pattern into an
//! independent file with logical-level ops, so a cluster log and an engine WAL can never
//! be confused.
//!
//! ## On-disk frame (mirrors `wal.rs`)
//! ```text
//!   header (once): magic "CMLG" (4) + format_version u32 (4)
//!   per record:    total_len u32 | crc32 u32 | seq u64 | op u8 | payload
//!     op ADD    (0): logical u64 | version u32 | dsl_len u32 | dsl [u8]
//!     op REMOVE (1): logical u64
//! ```
//! On recovery we scan forward, stopping at the first bad-CRC / truncated frame (a torn
//! tail from a crash); the skipped byte count is surfaced as a diagnostic. The
//! checkpoint *cursor* (which records are already captured by a base snapshot) and the
//! *epoch* live in the coordinator manifest — the atomic commit point — not in this
//! file, so [`ClusterLog::replay`] takes the cursor as an argument and a checkpoint
//! simply truncates already-captured records.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use super::shard::ShardError;
use crate::storage::crc32;

const CLOG_MAGIC: [u8; 4] = *b"CMLG";
const CLOG_VERSION: u32 = 1;
const CLOG_HEADER_SIZE: usize = 8; // magic + version

const OP_ADD: u8 = 0;
const OP_REMOVE: u8 = 1;

/// Opaque, ordered position in the log — the Raft log index later. New-typed so callers
/// can't do arithmetic on it; `LogPos(0)` is "before the first record".
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub(crate) struct LogPos(pub u64);

/// One coordinator-visible cluster mutation. Logical-id + raw DSL is node-independent
/// and re-compilable against the manifest's frozen dict (the ADR-029 DSL-on-wire
/// invariant), so replaying it reproduces byte-identical placement → zero false
/// negatives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ClusterMutation {
    /// Add (or replace, by logical id) a query. `version` mirrors the engine's
    /// per-logical version carried on the write path.
    Add {
        logical: u64,
        version: u32,
        dsl: String,
    },
    /// Remove every live entry for a logical id (idempotent).
    Remove { logical: u64 },
}

/// Result of replaying a log from a cursor — mirrors [`WalRecovery`](crate::wal::WalRecovery):
/// the ordered mutations to apply plus a torn-tail byte count.
pub(crate) struct ClusterReplay {
    pub entries: Vec<(LogPos, ClusterMutation)>,
    /// Trailing bytes that could not be parsed (a torn write from a crash). Never
    /// acknowledged as durable, so dropping them is safe.
    pub skipped_bytes: usize,
}

/// The durable, ordered source of truth the coordinator applies to its shards.
///
/// Sync + fallible (`Result<_, ShardError>`) + `Send + Sync`, exactly like
/// [`Shard`](super::shard::Shard). A `NullClusterLog` is infallible; a `FileClusterLog`
/// errors on I/O — surfacing that (rather than swallowing it) is load-bearing for the
/// rebuild-from-log contract.
pub(crate) trait ClusterLog: Send + Sync {
    /// Durably append one mutation and return its assigned position. MUST be durable
    /// before returning `Ok` (WAL-first: the coordinator applies to shards only after
    /// this succeeds). Raft: returns once the entry commits on a quorum.
    fn append(&self, m: &ClusterMutation) -> Result<LogPos, ShardError>;

    /// Replay every committed record strictly after `from`, oldest-first. Used by
    /// `ClusterEngine::open` (with the manifest's snapshot cursor) and, later, a
    /// follower catching up.
    fn replay(&self, from: LogPos) -> Result<ClusterReplay, ShardError>;

    /// The highest position appended so far (`LogPos(0)` if none).
    fn last_pos(&self) -> Result<LogPos, ShardError>;

    /// Drop every record at or before `up_to` (now captured by a base snapshot). The
    /// caller (coordinator) MUST have durably written the snapshot + manifest first —
    /// the manifest is the atomic commit point, so a crash before this truncation just
    /// replays an already-captured (idempotent) tail. The epoch/checkpoint generation
    /// lives in the coordinator manifest (the future Raft cluster-state document), not
    /// in the log, so this byte-log stays a pure ordered store.
    fn checkpoint(&self, up_to: LogPos) -> Result<(), ShardError>;

    /// Test-only fault injection: make subsequent `append`s fail. Default no-op (e.g.
    /// `NullClusterLog`); `FileClusterLog` revokes its write handle. Exposed on the trait
    /// so a coordinator test can break the log through a `Box<dyn ClusterLog>` and prove
    /// the WAL-first fail-closed contract.
    #[cfg(test)]
    fn break_writes_for_test(&self) {}
}

// ---- NullClusterLog: in-memory (no data_dir) + the fast test backend ----

/// A non-durable log: assigns monotonic positions in memory, but persists nothing and
/// replays empty. This is the behavior of an in-process cluster built without a
/// `data_dir` (byte-identical to the pre-ADR-031 cluster) and the fast backend the
/// durability oracle diffs the file backend against.
pub(crate) struct NullClusterLog {
    next_seq: AtomicU64,
}

impl NullClusterLog {
    pub(crate) fn new() -> Self {
        NullClusterLog {
            next_seq: AtomicU64::new(1),
        }
    }
}

impl ClusterLog for NullClusterLog {
    fn append(&self, _m: &ClusterMutation) -> Result<LogPos, ShardError> {
        Ok(LogPos(self.next_seq.fetch_add(1, Ordering::Relaxed)))
    }

    fn replay(&self, _from: LogPos) -> Result<ClusterReplay, ShardError> {
        Ok(ClusterReplay {
            entries: Vec::new(),
            skipped_bytes: 0,
        })
    }

    fn last_pos(&self) -> Result<LogPos, ShardError> {
        Ok(LogPos(
            self.next_seq.load(Ordering::Relaxed).saturating_sub(1),
        ))
    }

    fn checkpoint(&self, _up_to: LogPos) -> Result<(), ShardError> {
        Ok(())
    }
}

// ---- FileClusterLog: the durable single-node backend ----

/// Mutable file state behind the `&self` trait (the coordinator holds the log shared).
/// A `std::sync::Mutex` both gives interior mutability and enforces the single-writer
/// total order a Raft leader will also want.
struct FileState {
    file: std::fs::File,
    path: PathBuf,
    /// Next position to assign — kept monotonic across checkpoints/reopens (seeded from
    /// the manifest's snapshot cursor as a floor, so a truncated-then-reopened log never
    /// reissues a position).
    next_seq: u64,
}

/// A durable, CRC-framed, append-only cluster log (the file backend of [`ClusterLog`]).
pub(crate) struct FileClusterLog {
    state: Mutex<FileState>,
    /// When true, every append `fsync`s before returning (survives power loss); when
    /// false, appends only reach the OS page cache (survives process crash). Mirrors
    /// the engine WAL's `fsync_each_write` policy.
    fsync_each_write: bool,
}

impl FileClusterLog {
    /// Open or create the log at `path`. `floor_pos` (the manifest's snapshot cursor)
    /// seeds the position counter so it stays monotonic even after a checkpoint
    /// truncated the file.
    pub(crate) fn open(path: &Path, fsync_each_write: bool, floor_pos: LogPos) -> io::Result<Self> {
        let (file, next_seq) = if path.exists() {
            let (entries, _skipped) = Self::read_entries(path)?;
            let max_seq = entries.iter().map(|(p, _)| p.0).max().unwrap_or(0);
            let next_seq = max_seq.max(floor_pos.0) + 1;
            let file = std::fs::OpenOptions::new().append(true).open(path)?;
            (file, next_seq)
        } else {
            let mut file = std::fs::File::create(path)?;
            file.write_all(&CLOG_MAGIC)?;
            file.write_all(&CLOG_VERSION.to_le_bytes())?;
            file.sync_all()?;
            (file, floor_pos.0 + 1)
        };
        Ok(FileClusterLog {
            state: Mutex::new(FileState {
                file,
                path: path.to_path_buf(),
                next_seq,
            }),
            fsync_each_write,
        })
    }

    /// Lock the file state, recovering a poisoned guard rather than panicking (a prior
    /// writer panic must not take down the cluster; the file is append-framed, so the
    /// on-disk state is still consistent up to the last whole record).
    fn lock(&self) -> std::sync::MutexGuard<'_, FileState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Encode one mutation's body: `seq | op | payload` (the CRC'd, length-framed part).
    fn encode_body(seq: u64, m: &ClusterMutation) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&seq.to_le_bytes());
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
            } => {
                let dsl_bytes = dsl.as_bytes();
                body.push(OP_ADD);
                body.extend_from_slice(&logical.to_le_bytes());
                body.extend_from_slice(&version.to_le_bytes());
                body.extend_from_slice(&(dsl_bytes.len() as u32).to_le_bytes());
                body.extend_from_slice(dsl_bytes);
            }
            ClusterMutation::Remove { logical } => {
                body.push(OP_REMOVE);
                body.extend_from_slice(&logical.to_le_bytes());
            }
        }
        body
    }

    /// Read every valid record from a log file. Returns positioned mutations plus the
    /// byte count of any trailing data that could not be parsed (torn tail).
    fn read_entries(path: &Path) -> io::Result<(Vec<(LogPos, ClusterMutation)>, usize)> {
        let data = std::fs::read(path)?;
        if data.len() < CLOG_HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "clog too small"));
        }
        if data[0..4] != CLOG_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad clog magic"));
        }
        Ok(Self::parse_entries(&data[CLOG_HEADER_SIZE..]))
    }

    fn parse_entries(data: &[u8]) -> (Vec<(LogPos, ClusterMutation)>, usize) {
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
            let Some(total_len) = get_u32(data, cursor).map(|v| v as usize) else {
                break;
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
            // body = seq(8) + op(1) + payload
            if total_len < 9 {
                break;
            }
            let Some(seq) = get_u64(body, 0) else { break };
            let op = body[8];
            let payload = &body[9..];

            let mutation = match op {
                OP_ADD => {
                    if payload.len() < 16 {
                        break;
                    }
                    let Some(logical) = get_u64(payload, 0) else {
                        break;
                    };
                    let Some(version) = get_u32(payload, 8) else {
                        break;
                    };
                    let Some(dsl_len) = get_u32(payload, 12).map(|v| v as usize) else {
                        break;
                    };
                    if payload.len() < 16 + dsl_len {
                        break;
                    }
                    let Ok(dsl) = std::str::from_utf8(&payload[16..16 + dsl_len]) else {
                        break;
                    };
                    ClusterMutation::Add {
                        logical,
                        version,
                        dsl: dsl.to_string(),
                    }
                }
                OP_REMOVE => {
                    if payload.len() < 8 {
                        break;
                    }
                    let Some(logical) = get_u64(payload, 0) else {
                        break;
                    };
                    ClusterMutation::Remove { logical }
                }
                _ => break,
            };
            entries.push((LogPos(seq), mutation));
            cursor += total_len;
        }

        let skipped_bytes = data.len() - cursor;
        (entries, skipped_bytes)
    }
}

impl ClusterLog for FileClusterLog {
    fn append(&self, m: &ClusterMutation) -> Result<LogPos, ShardError> {
        let mut st = self.lock();
        let seq = st.next_seq;
        let body = Self::encode_body(seq, m);
        let crc = crc32(&body);

        let write = (|| -> io::Result<()> {
            st.file.write_all(&(body.len() as u32).to_le_bytes())?;
            st.file.write_all(&crc.to_le_bytes())?;
            st.file.write_all(&body)?;
            if self.fsync_each_write {
                st.file.sync_all()
            } else {
                st.file.flush()
            }
        })();
        write.map_err(|e| ShardError::Log(format!("append: {e}")))?;
        st.next_seq += 1;
        Ok(LogPos(seq))
    }

    fn replay(&self, from: LogPos) -> Result<ClusterReplay, ShardError> {
        let path = { self.lock().path.clone() };
        let (all, skipped_bytes) =
            Self::read_entries(&path).map_err(|e| ShardError::Log(format!("replay: {e}")))?;
        let entries = all.into_iter().filter(|(p, _)| *p > from).collect();
        Ok(ClusterReplay {
            entries,
            skipped_bytes,
        })
    }

    fn last_pos(&self) -> Result<LogPos, ShardError> {
        Ok(LogPos(self.lock().next_seq.saturating_sub(1)))
    }

    fn checkpoint(&self, up_to: LogPos) -> Result<(), ShardError> {
        let mut st = self.lock();
        // Rewrite the file keeping only records strictly after `up_to` (those not yet
        // captured by the base snapshot). Atomic via tmp + rename so a crash mid-rewrite
        // leaves the old (already-consistent) file in place.
        let (all, _skipped) = Self::read_entries(&st.path)
            .map_err(|e| ShardError::Log(format!("checkpoint read: {e}")))?;
        let kept: Vec<(LogPos, ClusterMutation)> =
            all.into_iter().filter(|(p, _)| *p > up_to).collect();

        let rewrite = (|| -> io::Result<()> {
            let tmp = st.path.with_extension("clog.tmp");
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&CLOG_MAGIC)?;
            f.write_all(&CLOG_VERSION.to_le_bytes())?;
            for (pos, m) in &kept {
                let body = Self::encode_body(pos.0, m);
                let crc = crc32(&body);
                f.write_all(&(body.len() as u32).to_le_bytes())?;
                f.write_all(&crc.to_le_bytes())?;
                f.write_all(&body)?;
            }
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, &st.path)?;
            if let Some(parent) = st.path.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        })();
        rewrite.map_err(|e| ShardError::Log(format!("checkpoint rewrite: {e}")))?;

        // Re-open the appending handle on the rewritten file.
        st.file = std::fs::OpenOptions::new()
            .append(true)
            .open(&st.path)
            .map_err(|e| ShardError::Log(format!("checkpoint reopen: {e}")))?;
        Ok(())
    }

    #[cfg(test)]
    fn break_writes_for_test(&self) {
        FileClusterLog::break_writes_for_test(self);
    }
}

#[cfg(test)]
impl FileClusterLog {
    /// Test-only: swap the file handle for a read-only one so subsequent appends fail —
    /// the deterministic write-fault injection used by the durability oracle (mirrors
    /// `Wal::break_writes_for_test`).
    pub(crate) fn break_writes_for_test(&self) {
        let mut st = self.lock();
        st.file = std::fs::OpenOptions::new()
            .read(true)
            .open(&st.path)
            .expect("reopen clog read-only");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_path(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "reverse_rusty_clog_{}_{}.log",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn add(logical: u64, dsl: &str) -> ClusterMutation {
        ClusterMutation::Add {
            logical,
            version: 1,
            dsl: dsl.to_string(),
        }
    }

    #[test]
    fn append_then_replay_round_trips() {
        let path = scratch_path("roundtrip");
        {
            let log = FileClusterLog::open(&path, true, LogPos(0)).unwrap();
            assert_eq!(log.append(&add(1, "1994 upper deck")).unwrap(), LogPos(1));
            assert_eq!(
                log.append(&ClusterMutation::Remove { logical: 1 }).unwrap(),
                LogPos(2)
            );
            assert_eq!(log.append(&add(2, "topps chrome")).unwrap(), LogPos(3));
            assert_eq!(log.last_pos().unwrap(), LogPos(3));
        }
        // Reopen and replay from the start.
        let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
        let replay = log.replay(LogPos(0)).unwrap();
        assert_eq!(replay.skipped_bytes, 0);
        assert_eq!(replay.entries.len(), 3);
        assert_eq!(replay.entries[0], (LogPos(1), add(1, "1994 upper deck")));
        assert_eq!(
            replay.entries[1],
            (LogPos(2), ClusterMutation::Remove { logical: 1 })
        );
        // next_seq stays monotonic across reopen.
        assert_eq!(log.append(&add(3, "x")).unwrap(), LogPos(4));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn replay_from_cursor_skips_captured_prefix() {
        let path = scratch_path("cursor");
        let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
        for i in 1..=5 {
            log.append(&add(i, "q")).unwrap();
        }
        let replay = log.replay(LogPos(3)).unwrap();
        let positions: Vec<u64> = replay.entries.iter().map(|(p, _)| p.0).collect();
        assert_eq!(positions, vec![4, 5]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn torn_tail_is_dropped_not_fatal() {
        let path = scratch_path("torn");
        {
            let log = FileClusterLog::open(&path, true, LogPos(0)).unwrap();
            log.append(&add(1, "alpha")).unwrap();
            log.append(&add(2, "beta")).unwrap();
        }
        // Corrupt the tail by appending junk that can't frame a valid record.
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[0xFF, 0xFF, 0xFF, 0x7F, 0xAA, 0xBB]).unwrap();
        }
        let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
        let replay = log.replay(LogPos(0)).unwrap();
        assert_eq!(replay.entries.len(), 2, "the two whole records survive");
        assert!(replay.skipped_bytes > 0, "torn tail counted");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn checkpoint_truncates_captured_records() {
        let path = scratch_path("checkpoint");
        let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
        for i in 1..=5 {
            log.append(&add(i, "q")).unwrap();
        }
        let size_before = std::fs::metadata(&path).unwrap().len();
        log.checkpoint(LogPos(3)).unwrap();
        let size_after = std::fs::metadata(&path).unwrap().len();
        assert!(size_after < size_before, "captured prefix dropped");
        // Only records after the cursor remain; new appends stay monotonic.
        let replay = log.replay(LogPos(0)).unwrap();
        let positions: Vec<u64> = replay.entries.iter().map(|(p, _)| p.0).collect();
        assert_eq!(positions, vec![4, 5]);
        assert_eq!(log.append(&add(6, "q")).unwrap(), LogPos(6));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn append_surfaces_write_errors() {
        let path = scratch_path("writefault");
        let log = FileClusterLog::open(&path, false, LogPos(0)).unwrap();
        assert!(log.append(&add(1, "ok")).is_ok());
        log.break_writes_for_test();
        assert!(matches!(log.append(&add(2, "no")), Err(ShardError::Log(_))));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn null_log_assigns_positions_and_replays_empty() {
        let log = NullClusterLog::new();
        assert_eq!(log.append(&add(1, "q")).unwrap(), LogPos(1));
        assert_eq!(log.append(&add(2, "q")).unwrap(), LogPos(2));
        assert_eq!(log.last_pos().unwrap(), LogPos(2));
        assert_eq!(log.replay(LogPos(0)).unwrap().entries.len(), 0);
        log.checkpoint(LogPos(2)).unwrap();
    }
}
