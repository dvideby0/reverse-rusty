use super::{
    crc32, Path, Wal, WalEntry, WalRecovery, OP_DELETE_LOGICAL, OP_FLUSH_CHECKPOINT, OP_INSERT,
    OP_INSERT_CLASS_D, OP_TOMBSTONE, OP_UPSERT, OP_UPSERT_CLASS_D, SOURCE_GENERATION_MAGIC,
    WAL_HEADER_SIZE, WAL_MAGIC,
};
use std::io;

impl Wal {
    /// Read all valid entries from a WAL file. Returns entries and the byte
    /// count of any trailing data that could not be parsed.
    pub(super) fn read_entries(path: &Path) -> io::Result<(Vec<WalEntry>, usize)> {
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
        fn get_i64(buf: &[u8], off: usize) -> Option<i64> {
            buf.get(off..off + 8)
                .and_then(|s| s.try_into().ok())
                .map(i64::from_le_bytes)
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
                    // WAL v6 appends one optional i64. WAL v7 engine writes use
                    // the marked generation tail and carry an explicit
                    // priority-present byte. Unknown tails remain ignored for
                    // the same forward-compatible discipline as v6.
                    let remaining = payload.get(p..).unwrap_or_default();
                    let (source_generation, priority) = if remaining.len() == 8 {
                        (None, get_i64(remaining, 0))
                    } else if remaining.len() >= 13
                        && remaining.get(..4) == Some(SOURCE_GENERATION_MAGIC.as_slice())
                    {
                        let generation = get_u64(remaining, 4);
                        let priority_present = remaining.get(12).copied();
                        let priority = match (priority_present, remaining.len()) {
                            (Some(1), 21) => get_i64(remaining, 13),
                            _ => None,
                        };
                        let valid_shape = matches!(
                            (priority_present, remaining.len()),
                            (Some(0), 13) | (Some(1), 21)
                        );
                        (
                            valid_shape
                                .then_some(generation)
                                .flatten()
                                .filter(|&g| g != 0),
                            valid_shape.then_some(priority).flatten(),
                        )
                    } else {
                        (None, None)
                    };
                    entries.push(if op == OP_INSERT || op == OP_INSERT_CLASS_D {
                        WalEntry::Insert {
                            seq,
                            logical,
                            version,
                            text,
                            tags,
                            priority,
                            source_generation,
                            class_d_accepted: op == OP_INSERT_CLASS_D,
                        }
                    } else {
                        WalEntry::Upsert {
                            seq,
                            logical,
                            version,
                            text,
                            tags,
                            priority,
                            source_generation,
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
}
