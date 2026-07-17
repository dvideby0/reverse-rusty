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
//!                    tag_count u32 | (klen u32|k|vlen u32|v)*
//!                    placement_generation u64 | num_shards u32 | mode u8
//!                    position_count u32 | positions [u32]           (v4, ADR-109)
//!     op REMOVE (1): logical u64
//!     op UPSERT (2): same payload as ADD
//! ```
//! v4 requires explicit placement identity for ADD/UPSERT and therefore rejects v1–v3 logs with
//! an actionable rebuild error; re-deriving ownership under a newer ring/generation would be unsafe.
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
// v2 (ADR-055): optional trailing tags. v3 (ADR-070): atomic UPSERT. v4 (ADR-109):
// mandatory write-time placement metadata for ADD/UPSERT. v1-v3 are now a migration fence:
// without their original placement identity a new binary cannot choose one emission owner safely.
const CLOG_VERSION: u32 = 4;
const CLOG_HEADER_SIZE: usize = 8; // magic + version

const OP_ADD: u8 = 0;
const OP_REMOVE: u8 = 1;
const OP_UPSERT: u8 = 2;

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
        /// Raw `(key, value)` metadata tags (ADR-055), re-resolved to `TagId`s against the
        /// frozen shared tag space on apply/replay (the tags-on-wire analogue of raw DSL).
        /// Empty for an untagged query — the byte-identical pre-tag path.
        tags: Vec<(String, String)>,
        /// ADR-109 write-time placement identity. Persisted so coordinator and
        /// per-shard translog replay cannot re-materialize stale ownership.
        placement: crate::ownership::QueryPlacement,
    },
    /// Remove every live entry for a logical id (idempotent).
    Remove { logical: u64 },
    /// Atomically replace a query by logical id (ADR-070): tombstone every prior live
    /// copy AND insert the new version under ONE frame, so replay reproduces the whole
    /// replacement or none of it — never a remove without its re-add (the cluster
    /// analogue of the engine WAL's `Upsert`, ADR-067). Payload layout is identical
    /// to [`Add`](Self::Add).
    Upsert {
        logical: u64,
        version: u32,
        dsl: String,
        /// Raw `(key, value)` metadata tags for the NEW version (ADR-055 semantics).
        tags: Vec<(String, String)>,
        placement: crate::ownership::QueryPlacement,
    },
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
        // The shared ADD/UPSERT payload: `logical | version | dsl_len | dsl | [tag block]`.
        // ADR-055: the tag block is appended ONLY when non-empty, so an untagged frame is
        // byte-identical to a v1 record (and the durability oracle's two-backend diff stays
        // exact). Each tag is a length-prefixed key + value.
        fn encode_add_like(
            body: &mut Vec<u8>,
            op: u8,
            logical: u64,
            version: u32,
            dsl: &str,
            tags: &[(String, String)],
            placement: &crate::ownership::QueryPlacement,
        ) {
            let dsl_bytes = dsl.as_bytes();
            body.push(op);
            body.extend_from_slice(&logical.to_le_bytes());
            body.extend_from_slice(&version.to_le_bytes());
            body.extend_from_slice(&(dsl_bytes.len() as u32).to_le_bytes());
            body.extend_from_slice(dsl_bytes);
            body.extend_from_slice(&(tags.len() as u32).to_le_bytes());
            for (k, v) in tags {
                let kb = k.as_bytes();
                let vb = v.as_bytes();
                body.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                body.extend_from_slice(kb);
                body.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                body.extend_from_slice(vb);
            }
            body.extend_from_slice(&placement.generation().0.to_le_bytes());
            body.extend_from_slice(&placement.num_shards().to_le_bytes());
            body.push(placement.mode() as u8);
            body.extend_from_slice(&(placement.positions().len() as u32).to_le_bytes());
            for position in placement.positions() {
                body.extend_from_slice(&position.to_le_bytes());
            }
        }
        let mut body = Vec::new();
        body.extend_from_slice(&seq.to_le_bytes());
        match m {
            ClusterMutation::Add {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => encode_add_like(&mut body, OP_ADD, *logical, *version, dsl, tags, placement),
            ClusterMutation::Upsert {
                logical,
                version,
                dsl,
                tags,
                placement,
            } => encode_add_like(
                &mut body, OP_UPSERT, *logical, *version, dsl, tags, placement,
            ),
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
        let version = u32::from_le_bytes(data[4..8].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid cluster-log version")
        })?);
        if version != CLOG_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                if version < CLOG_VERSION {
                    format!(
                        "cluster log format v{version} predates ADR-109 ownership metadata; rebuild the cluster with this binary"
                    )
                } else {
                    format!("unsupported cluster log format v{version}")
                },
            ));
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
        // ADR-055: decode one length-prefixed `(klen u32|k|vlen u32|v)` tag at `off`, returning the
        // pair and the next offset, or `None` on any bounds / UTF-8 error (a torn tail).
        fn parse_tag(buf: &[u8], off: usize) -> Option<(String, String, usize)> {
            let klen = (buf
                .get(off..off + 4)?
                .try_into()
                .ok()
                .map(u32::from_le_bytes)?) as usize;
            let ks = off + 4;
            let k = std::str::from_utf8(buf.get(ks..ks + klen)?).ok()?;
            let vlen_off = ks + klen;
            let vlen = (buf
                .get(vlen_off..vlen_off + 4)?
                .try_into()
                .ok()
                .map(u32::from_le_bytes)?) as usize;
            let vs = vlen_off + 4;
            let v = std::str::from_utf8(buf.get(vs..vs + vlen)?).ok()?;
            Some((k.to_string(), v.to_string(), vs + vlen))
        }
        // Decode the shared ADD/UPSERT payload (`logical | version | dsl_len | dsl |
        // [tag block]`); `None` on any malformed byte (treated as a torn tail by the
        // caller, mirroring the rest of the parse).
        #[allow(clippy::type_complexity)]
        fn parse_add_like(
            payload: &[u8],
        ) -> Option<(
            u64,
            u32,
            String,
            Vec<(String, String)>,
            crate::ownership::QueryPlacement,
        )> {
            if payload.len() < 16 {
                return None;
            }
            let logical = get_u64(payload, 0)?;
            let version = get_u32(payload, 8)?;
            let dsl_len = get_u32(payload, 12)? as usize;
            let dsl = std::str::from_utf8(payload.get(16..16 + dsl_len)?).ok()?;
            // ADR-055: optional trailing tag block. An untagged record ends exactly at
            // the DSL, so `toff == payload.len()` ⇒ empty tags.
            let mut toff = 16 + dsl_len;
            let mut tags: Vec<(String, String)> = Vec::new();
            let tag_count = get_u32(payload, toff)? as usize;
            toff += 4;
            for _ in 0..tag_count {
                let (k, v, next) = parse_tag(payload, toff)?;
                tags.push((k, v));
                toff = next;
            }
            let generation = get_u64(payload, toff)?;
            toff += 8;
            let num_shards = get_u32(payload, toff)?;
            toff += 4;
            let mode = *payload.get(toff)?;
            toff += 1;
            let count = get_u32(payload, toff)? as usize;
            toff += 4;
            let mut positions = Vec::with_capacity(count);
            for _ in 0..count {
                positions.push(get_u32(payload, toff)?);
                toff += 4;
            }
            if toff != payload.len() {
                return None;
            }
            let placement = crate::ownership::QueryPlacement::from_raw(
                crate::ownership::PlacementGeneration(generation),
                num_shards,
                mode,
                positions,
            )
            .ok()?;
            Some((logical, version, dsl.to_string(), tags, placement))
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
                OP_ADD => match parse_add_like(payload) {
                    Some((logical, version, dsl, tags, placement)) => ClusterMutation::Add {
                        logical,
                        version,
                        dsl,
                        tags,
                        placement,
                    },
                    None => break,
                },
                OP_UPSERT => match parse_add_like(payload) {
                    Some((logical, version, dsl, tags, placement)) => ClusterMutation::Upsert {
                        logical,
                        version,
                        dsl,
                        tags,
                        placement,
                    },
                    None => break,
                },
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
mod tests;
