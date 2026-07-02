//! The durable disk layer of a multi-shard node (ADR-072/093/096): the persisted adopted
//! feature-space blob (`feature_space.bin`), the `shard_<NNN>/` slot-dir naming + restart scan,
//! and the dropped-slot trash-rename reclaim. Split from `server.rs` (the <650-line budget);
//! everything here is `pub(super)` — the server + its service handlers are the only consumers.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use crate::cluster::coordinator::shard_dir;
use crate::cluster::shard::{LocalShard, ShardError};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::tagdict::TagDict;

use super::{ServerState, ShardSlot};

/// The adopted feature space (dict + tag dict), persisted by a durable `AdoptDict`
/// so a restarted node self-restores without a coordinator (ADR-072). The dict and
/// tag dict are written as ONE length-framed blob under one atomic rename, so the
/// pair can never desync — a crash leaves the whole prior file or the whole new one,
/// never a new dict beside a stale/absent tag space (which would silently mis-filter
/// tagged reads after restart; review finding).
pub(super) const ADOPTED_SPACE_FILE: &str = "feature_space.bin";

/// Persist the adopted (already fingerprint-verified) dict + tag-space bytes under
/// `dir` as one atomically-renamed blob: `dict_len u64 | dict | tag_dict`.
pub(super) fn persist_adopted_space(
    dir: &Path,
    dict_bytes: &[u8],
    tag_bytes: &[u8],
) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut blob = Vec::with_capacity(8 + dict_bytes.len() + tag_bytes.len());
    blob.extend_from_slice(&(dict_bytes.len() as u64).to_le_bytes());
    blob.extend_from_slice(dict_bytes);
    blob.extend_from_slice(tag_bytes);
    let tmp = dir.join(format!("{ADOPTED_SPACE_FILE}.tmp"));
    std::fs::write(&tmp, &blob)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, dir.join(ADOPTED_SPACE_FILE))?;
    Ok(())
}

/// The persisted dict + tag-space bytes, as read back from the feature-space blob.
pub(super) type AdoptedSpaceBytes = (Vec<u8>, Vec<u8>);

/// Read back the persisted feature-space blob: `Some((dict_bytes, tag_bytes))` when
/// present + well-framed, `None` when absent (a never-adopted durable node), an error
/// on a corrupt/torn frame (fail loud rather than misparse).
pub(super) fn read_adopted_space(dir: &Path) -> Result<Option<AdoptedSpaceBytes>, ShardError> {
    let path = dir.join(ADOPTED_SPACE_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let blob = std::fs::read(&path)
        .map_err(|e| ShardError::Log(format!("reading {}: {e}", path.display())))?;
    let dict_len = blob
        .get(0..8)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_le_bytes)
        .map(|n| n as usize)
        .filter(|&n| 8 + n <= blob.len())
        .ok_or_else(|| ShardError::Log(format!("corrupt feature-space file {}", path.display())))?;
    let dict_bytes = blob[8..8 + dict_len].to_vec();
    let tag_bytes = blob[8 + dict_len..].to_vec();
    Ok(Some((dict_bytes, tag_bytes)))
}

/// Parse a `shard_<NNN>` directory name back to its `shard_id`, mirroring [`shard_dir`]'s `{:03}`
/// zero-padded scheme. Any other name (e.g. `feature_space.bin`, a legacy root `segments/`) ⇒ `None`.
pub(super) fn parse_shard_subdir(name: &OsStr) -> Option<u32> {
    name.to_str()?.strip_prefix("shard_")?.parse().ok()
}

/// A trash-renamed slot dir left by a [`DropShard`] whose final delete was interrupted (ADR-096):
/// `shard_<NNN>.dropped.<nanos>`. Structurally invisible to [`parse_shard_subdir`] (the suffix
/// does not parse as a `u32`), so a restart never re-attaches it; the boot sweep reclaims it.
pub(super) fn is_dropped_trash(name: &OsStr) -> bool {
    name.to_str()
        .is_some_and(|s| s.starts_with("shard_") && s.contains(".dropped."))
}

/// Best-effort boot sweep of trash-renamed dropped-slot dirs (ADR-096). Never fails boot: a
/// missing/unreadable dir or a failed delete just leaves the trash for the next boot (it is
/// structurally invisible to the slot scan either way).
pub(super) fn sweep_dropped_trash(data_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(data_dir) else {
        return;
    };
    for entry in entries.flatten() {
        if is_dropped_trash(&entry.file_name()) {
            // A failed delete leaves the trash for the next boot — never fails the sweep.
            std::fs::remove_dir_all(entry.path()).ok();
        }
    }
}

/// Reclaim a dropped slot's `shard_<id>/` dir (ADR-096): rename it to the trash name FIRST — one
/// atomic step that makes it invisible to a restart's [`restore_durable_slots`] scan (an in-place
/// `remove_dir_all` interrupted mid-delete would leave a live-named dir whose checkpoint sidecar
/// lists already-deleted segments, and the restart's reopen fails LOUD — bricking the node) —
/// then best-effort delete the trash. Returns `true` when the dir is fully gone; `false` when the
/// rename succeeded but the delete did not (the boot sweep finishes the job). A missing dir (an
/// in-memory slot, or an already-reclaimed re-run) is vacuously `true`.
pub(super) fn reclaim_slot_dir(root: &Path, shard_id: u32) -> bool {
    let live = shard_dir(root, shard_id as usize);
    if !live.exists() {
        return true;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let trash = root.join(format!("shard_{shard_id:03}.dropped.{nanos}"));
    if std::fs::rename(&live, &trash).is_err() {
        // Rename failed: the live-named dir remains and a restart re-attaches it (the slot
        // resurrects as an orphan and a later sweep re-drops it) — safe, just not reclaimed.
        return false;
    }
    // Make the rename durable against power loss (fsync the parent dir); best-effort — a torn
    // rename after a crash leaves either name, both safe (live ⇒ re-attach + re-drop; trash ⇒
    // swept at boot).
    if let Ok(dir) = std::fs::File::open(root) {
        dir.sync_all().ok();
    }
    std::fs::remove_dir_all(&trash).is_ok()
}

/// Restore every durable slot a node previously hosted (ADR-093): scan `data_dir` for `shard_<NNN>/`
/// subdirs and reopen each `LocalShard` over the node-shared dict/tag space (self-restoring from that
/// subdir's checkpoint sidecar + translog tail). A fresh dir (no subdirs) ⇒ an empty map (the node
/// adopts on connect). Fails LOUD on a corrupt subdir dict (fingerprint mismatch) — never serves a slot
/// compiled against a divergent feature space. No in-place migration of a legacy root `segments/`
/// layout (the distributed shard store holds no production data yet — ADR-093 §Backward-compat).
pub(super) fn restore_durable_slots(
    data_dir: &Path,
    norm: &Arc<Normalizer>,
    dict: &Arc<Dict>,
    tag_dict: &Arc<TagDict>,
    config: &EngineConfig,
) -> Result<HashMap<u32, Arc<ShardSlot>>, ShardError> {
    let mut slots = HashMap::new();
    let entries = match std::fs::read_dir(data_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(slots),
        Err(e) => {
            return Err(ShardError::Log(format!(
                "scanning {}: {e}",
                data_dir.display()
            )))
        }
    };
    for entry in entries {
        let entry =
            entry.map_err(|e| ShardError::Log(format!("reading {}: {e}", data_dir.display())))?;
        let Some(shard_id) = parse_shard_subdir(&entry.file_name()) else {
            // feature_space.bin, a legacy root dir, or a trash-renamed dropped slot (ADR-096 —
            // the `.dropped.` suffix does not parse, so an interrupted delete never re-attaches;
            // `open_durable`'s boot sweep reclaims it).
            continue;
        };
        let subdir = shard_dir(data_dir, shard_id as usize);
        let mut sc = config.clone();
        sc.data_dir = Some(subdir);
        let shard =
            LocalShard::new_durable(Arc::clone(norm), Arc::clone(dict), Arc::clone(tag_dict), sc)?;
        slots.insert(
            shard_id,
            ShardSlot::loaded(ServerState {
                dict: Arc::clone(dict),
                tag_dict: Arc::clone(tag_dict),
                shard,
            }),
        );
    }
    Ok(slots)
}
