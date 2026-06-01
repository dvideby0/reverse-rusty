//! `Translog` — the per-shard durable query log (the Elasticsearch "translog"),
//! clustering build-path step 5c / ADR-039.
//!
//! Design: docs/design/clustering-and-scaling.md §4.2 (per-shard query log), §10 step 5c.
//!
//! Structurally this is the coordinator's [`ClusterLog`](super::clog::ClusterLog)
//! *re-homed onto a single durable shard*: the SAME logical-id + raw-DSL op
//! ([`ClusterMutation`](super::clog::ClusterMutation)), the SAME opaque
//! [`LogPos`](super::clog::LogPos), and the SAME CRC-framed file backend
//! ([`FileClusterLog`](super::clog::FileClusterLog)) reused verbatim — so a shard's
//! translog and the coordinator log can never be confused and the proven
//! torn-tail / forward-scan / atomic-checkpoint machinery is shared.
//!
//! ## Why a per-shard log exists at all (it is NOT the coordinator log)
//! The coordinator [`ClusterLog`](super::clog::ClusterLog) is WHOLE-CLUSTER and exists only
//! for an *in-process* cluster (a remote/gRPC cluster uses
//! [`NullClusterLog`](super::clog::NullClusterLog) — there is no durable coordinator tail
//! across processes). That is exactly why ADR-036's gRPC peer recovery had to **quiesce
//! writes** for the segment-copy window. The per-shard translog lives on EACH durable shard
//! (in-process replica *or* gRPC data node), so a recovering replica can stream a peer's
//! sealed segments at position `P` and then **replay the translog tail (ops > P)** — no
//! quiesce. It is also distinct from the control-plane document (ADR-037/038), which holds
//! cluster *state*, never query mutations.
//!
//! ## Position semantics (the zero-false-negative lynchpin)
//! A durable shard keeps its OWN dense, monotonic positions. `seal_for_checkpoint`
//! (flush memtable → reseal base tombstones) captures `P = last_pos` under the write lock
//! and trims the log to `P`, so the on-disk segments hold exactly the ops `≤ P` and the
//! translog holds exactly the un-sealed tail `> P`. Recovery streams segments (`≤ P`) then
//! replays the tail (`> P`): no overlap, no double-apply — the same property
//! `ClusterEngine::open` relies on, pushed down to the shard.

use std::io::Write;
use std::path::Path;

use super::clog::{ClusterLog, FileClusterLog, LogPos, NullClusterLog};
use super::shard::ShardError;
use crate::storage::crc32;

/// The per-shard translog file, rooted under the durable shard's `data_dir` (alongside
/// `segments/`).
pub(crate) const TRANSLOG_FILE: &str = "translog.clog";

/// A per-shard durable query log — the same seam as the coordinator
/// [`ClusterLog`](super::clog::ClusterLog), scoped to one durable shard. The alias documents
/// intent at the call sites (a shard owns a `Box<dyn ShardLog>`, the coordinator owns a
/// `Box<dyn ClusterLog>`); the trait is identical so the file backend is shared.
pub(crate) type ShardLog = dyn ClusterLog;

/// Open a **fresh** per-shard translog under `dir` (removing any stale file first), starting
/// at `LogPos(0)`. This is the construction-time / attach-time translog for the CORE recovery
/// path: the durable base is the attached/loaded segments, and the translog accumulates only
/// this shard instance's un-sealed writes. For the in-process durable cluster the coordinator
/// [`ClusterLog`](super::clog::ClusterLog) remains the authoritative crash-rebuild source, so a
/// stale on-disk tail from before a crash must NOT linger here (it would double-apply against
/// the coordinator-log replay) — hence the reset. (Data-node self-restart-from-translog, which
/// instead *keeps* and replays this file, is a separate open path — ADR-039 §6.)
pub(crate) fn open_fresh(dir: &Path, fsync_each_write: bool) -> Result<Box<ShardLog>, ShardError> {
    // The durable ctor opens the translog before the engine creates the data dir, so ensure it
    // exists (idempotent; a real failure surfaces).
    std::fs::create_dir_all(dir)
        .map_err(|e| ShardError::Log(format!("creating shard dir {}: {e}", dir.display())))?;
    let path = dir.join(TRANSLOG_FILE);
    // Reset: the segments are the durable base; the translog starts empty. A real removal
    // failure surfaces (a lingering tail would corrupt recovery); a missing file is benign.
    if let Err(e) = std::fs::remove_file(&path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(ShardError::Log(format!(
                "resetting shard translog {}: {e}",
                path.display()
            )));
        }
    }
    let log = FileClusterLog::open(&path, fsync_each_write, LogPos(0))
        .map_err(|e| ShardError::Log(format!("opening shard translog {}: {e}", path.display())))?;
    Ok(Box::new(log))
}

/// A non-durable translog for an in-memory shard: assigns monotonic positions, persists
/// nothing, replays empty — byte-identical to pre-ADR-039 (an in-memory shard has no `.seg`
/// files to recover from, so it is never a peer-recovery source/target). The
/// [`NullClusterLog`](super::clog::NullClusterLog) sibling, one level down.
pub(crate) fn null() -> Box<ShardLog> {
    Box::new(NullClusterLog::new())
}

/// Open an **existing** durable translog under `dir` WITHOUT resetting it (ADR-039 §6) — the
/// data-node self-restart path: the on-disk tail is the authority, replayed over the attached
/// segments. `floor` (the sidecar's local checkpoint) seeds the position counter so new appends
/// stay monotonic across the restart.
pub(crate) fn open_existing(
    dir: &Path,
    fsync_each_write: bool,
    floor: LogPos,
) -> Result<Box<ShardLog>, ShardError> {
    let path = dir.join(TRANSLOG_FILE);
    let log = FileClusterLog::open(&path, fsync_each_write, floor).map_err(|e| {
        ShardError::Log(format!(
            "opening existing shard translog {}: {e}",
            path.display()
        ))
    })?;
    Ok(Box::new(log))
}

// ---- per-shard checkpoint sidecar (ADR-039 §6: data-node self-restart) ----

/// The data node's own durable commit point — what a gRPC `shardserver --data-dir` needs to
/// self-recover after a crash, since it has no coordinator manifest (a remote cluster's
/// coordinator is non-durable). Records which segments are committed, the seg-id cursor, and the
/// translog position `P` the segments capture through; on restart the node attaches these
/// segments and replays the translog tail (ops > `P`). Written atomically (tmp + rename + fsync)
/// at each seal, AFTER the segments are durable and BEFORE the translog is trimmed — so a crash
/// in between just replays an already-captured (idempotent, position-filtered) prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ShardCheckpoint {
    pub next_seg_id: u64,
    /// The translog position the committed segments capture through (replay starts strictly after).
    pub local_checkpoint: u64,
    /// The frozen-dict fingerprint the segments were compiled against — checked on self-recovery
    /// so a node never silently attaches segments built for a divergent feature space.
    pub dict_fingerprint: u64,
    pub segment_files: Vec<String>,
}

const CKPT_FILE: &str = "shard.ckpt";
const CKPT_TMP: &str = "shard.ckpt.tmp";
const CKPT_MAGIC: [u8; 4] = *b"RSCK";
const CKPT_VERSION: u32 = 1;

fn encode_ckpt_body(c: &ShardCheckpoint) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&c.next_seg_id.to_le_bytes());
    b.extend_from_slice(&c.local_checkpoint.to_le_bytes());
    b.extend_from_slice(&c.dict_fingerprint.to_le_bytes());
    b.extend_from_slice(&(c.segment_files.len() as u32).to_le_bytes());
    for name in &c.segment_files {
        let nb = name.as_bytes();
        b.extend_from_slice(&(nb.len() as u32).to_le_bytes());
        b.extend_from_slice(nb);
    }
    b
}

/// Atomically write the per-shard checkpoint sidecar to `dir` (tmp + CRC + rename + parent fsync).
pub(crate) fn write_sidecar(dir: &Path, c: &ShardCheckpoint) -> Result<(), ShardError> {
    let body = encode_ckpt_body(c);
    let crc = crc32(&body);
    let path = dir.join(CKPT_FILE);
    let tmp = dir.join(CKPT_TMP);
    let write = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&CKPT_MAGIC)?;
        f.write_all(&CKPT_VERSION.to_le_bytes())?;
        f.write_all(&crc.to_le_bytes())?;
        f.write_all(&body)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, &path)?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    write.map_err(|e| ShardError::Log(format!("writing shard checkpoint {}: {e}", path.display())))
}

/// Read the per-shard checkpoint sidecar from `dir`. `Ok(None)` if absent (a fresh node). A
/// present-but-corrupt sidecar is a fail-loud error (never silently treated as a fresh node,
/// which would drop the committed segments).
pub(crate) fn read_sidecar(dir: &Path) -> Result<Option<ShardCheckpoint>, ShardError> {
    let path = dir.join(CKPT_FILE);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(ShardError::Log(format!("reading shard checkpoint: {e}"))),
    };
    // Panic-free little-endian reads (no `unwrap`/`expect` in library code).
    let g32 = |buf: &[u8], o: usize| -> Option<u32> {
        buf.get(o..o + 4)
            .and_then(|s| s.try_into().ok())
            .map(u32::from_le_bytes)
    };
    let g64 = |buf: &[u8], o: usize| -> Option<u64> {
        buf.get(o..o + 8)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
    };
    let bad = |m: &str| ShardError::Log(format!("shard checkpoint: {m}"));
    if data.len() < 12 || data[0..4] != CKPT_MAGIC {
        return Err(bad("bad magic or too small"));
    }
    let version = g32(&data, 4).ok_or_else(|| bad("truncated header"))?;
    if version != CKPT_VERSION {
        return Err(bad(&format!("unsupported version {version}")));
    }
    let stored_crc = g32(&data, 8).ok_or_else(|| bad("truncated header"))?;
    let body = &data[12..];
    if crc32(body) != stored_crc {
        return Err(bad("CRC mismatch"));
    }
    let get_u64 = |o: usize| g64(body, o);
    let get_u32 = |o: usize| g32(body, o);
    let trunc = || bad("truncated body");
    let next_seg_id = get_u64(0).ok_or_else(trunc)?;
    let local_checkpoint = get_u64(8).ok_or_else(trunc)?;
    let dict_fingerprint = get_u64(16).ok_or_else(trunc)?;
    let n = get_u32(24).ok_or_else(trunc)? as usize;
    let mut off = 28;
    let mut segment_files = Vec::with_capacity(n);
    for _ in 0..n {
        let len = get_u32(off).ok_or_else(trunc)? as usize;
        off += 4;
        let name = body.get(off..off + len).ok_or_else(trunc)?;
        off += len;
        let name = std::str::from_utf8(name)
            .map_err(|_| ShardError::Log("shard checkpoint: non-utf8 segment name".into()))?;
        segment_files.push(name.to_string());
    }
    Ok(Some(ShardCheckpoint {
        next_seg_id,
        local_checkpoint,
        dict_fingerprint,
        segment_files,
    }))
}

#[cfg(test)]
mod tests {
    use super::super::clog::ClusterMutation;
    use super::*;

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rr_translog_{}_{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }
    fn add(logical: u64, dsl: &str) -> ClusterMutation {
        ClusterMutation::Add {
            logical,
            version: 1,
            dsl: dsl.to_string(),
        }
    }

    #[test]
    fn open_fresh_round_trips() {
        let dir = scratch_dir("rt");
        let log = open_fresh(&dir, false).expect("open");
        assert_eq!(log.append(&add(1, "a")).unwrap(), LogPos(1));
        assert_eq!(
            log.append(&ClusterMutation::Remove { logical: 1 }).unwrap(),
            LogPos(2)
        );
        let replay = log.replay(LogPos(0)).unwrap();
        assert_eq!(replay.entries.len(), 2);
        assert_eq!(replay.entries[0].1, add(1, "a"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_fresh_resets_a_stale_translog() {
        let dir = scratch_dir("reset");
        {
            let log = open_fresh(&dir, true).expect("open");
            log.append(&add(1, "a")).unwrap();
            log.append(&add(2, "b")).unwrap();
        }
        // A second open_fresh WIPES the prior file: for the in-process cluster the coordinator
        // log is the crash-rebuild authority, so a stale on-disk tail must not linger (it would
        // double-apply against the coordinator-log replay).
        let log = open_fresh(&dir, false).expect("reopen");
        assert_eq!(
            log.replay(LogPos(0)).unwrap().entries.len(),
            0,
            "stale tail must be reset"
        );
        assert_eq!(
            log.append(&add(3, "c")).unwrap(),
            LogPos(1),
            "a fresh log restarts positions at 1"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn null_translog_persists_nothing() {
        let log = null();
        assert_eq!(log.append(&add(1, "a")).unwrap(), LogPos(1));
        assert_eq!(log.last_pos().unwrap(), LogPos(1));
        assert_eq!(log.replay(LogPos(0)).unwrap().entries.len(), 0);
    }

    #[test]
    fn sidecar_round_trips_and_absent_is_none() {
        let dir = scratch_dir("ckpt");
        std::fs::create_dir_all(&dir).unwrap();
        let c = ShardCheckpoint {
            next_seg_id: 7,
            local_checkpoint: 42,
            dict_fingerprint: 0xDEAD_BEEF_CAFE_F00D,
            segment_files: vec!["seg_0000001.seg".into(), "seg_0000003.seg".into()],
        };
        write_sidecar(&dir, &c).expect("write");
        assert_eq!(read_sidecar(&dir).expect("read").expect("present"), c);
        // A dir with no sidecar reads as None (a fresh node), not an error.
        let empty = scratch_dir("ckpt_empty");
        std::fs::create_dir_all(&empty).unwrap();
        assert!(read_sidecar(&empty).expect("read empty").is_none());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&empty);
    }
}
