//! Tag-dictionary binary (de)serialization — the frozen tag space stored inside the
//! engine + cluster manifests (ADR-049). Mirrors [`super::dict`] (the feature dict).

use std::io;

use crate::tagdict::{TagDict, TagId};

use super::{read_u16_at, read_u32_at};

/// Serialize the tag dictionary to a binary format.
/// Layout: `[num_tags: u32, then per tag: key_len: u16, key, val_len: u16, value]`
/// followed by `[finalized: u8]`. Tags are written in dense-id order, so deserialization
/// re-interns them into the same ids.
pub fn serialize_tagdict(td: &TagDict) -> Vec<u8> {
    let mut buf = Vec::new();
    let n = td.len();
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    for i in 0..n {
        let (key, value) = td.key_value(i as TagId).unwrap_or(("", ""));
        let kb = key.as_bytes();
        let vb = value.as_bytes();
        buf.extend_from_slice(&(kb.len() as u16).to_le_bytes());
        buf.extend_from_slice(kb);
        buf.extend_from_slice(&(vb.len() as u16).to_le_bytes());
        buf.extend_from_slice(vb);
    }
    buf.push(u8::from(td.is_finalized()));
    buf
}

/// Deserialize a `TagDict` from bytes produced by [`serialize_tagdict`]. An empty slice
/// (an older manifest that predates the tag space) yields an empty dict — the
/// backward-compatible "no tags" reading.
pub fn deserialize_tagdict(data: &[u8]) -> io::Result<TagDict> {
    let mut td = TagDict::new();
    if data.is_empty() {
        return Ok(td);
    }
    let mut cursor = 0usize;
    let n = read_u32_at(data, cursor)? as usize;
    cursor += 4;
    for _ in 0..n {
        let klen = read_u16_at(data, cursor)? as usize;
        cursor += 2;
        let key = std::str::from_utf8(
            data.get(cursor..cursor + klen)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated tag key"))?,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        cursor += klen;
        let vlen = read_u16_at(data, cursor)? as usize;
        cursor += 2;
        let value =
            std::str::from_utf8(data.get(cursor..cursor + vlen).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated tag value")
            })?)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        cursor += vlen;
        td.intern(key, value);
    }
    if cursor < data.len() && data[cursor] == 1 {
        td.mark_finalized();
    }
    Ok(td)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagdict_round_trips_and_empty_blob_reads_empty() {
        let mut td = TagDict::new();
        let a = td.intern("category", "trading-cards");
        let b = td.intern("status", "active");
        let c = td.intern("category", "coins");
        td.mark_finalized();

        let bytes = serialize_tagdict(&td);
        let got = deserialize_tagdict(&bytes).expect("deserialize");

        // ids are preserved (dense, in-order) and resolve identically.
        assert_eq!(got.len(), 3);
        assert_eq!(got.get("category", "trading-cards"), Some(a));
        assert_eq!(got.get("status", "active"), Some(b));
        assert_eq!(got.get("category", "coins"), Some(c));
        assert!(got.is_finalized());
        assert_eq!(got.fingerprint(), td.fingerprint());

        // an empty blob (older manifest, no tag space) reads back empty, not an error.
        let empty = deserialize_tagdict(&[]).expect("empty");
        assert!(empty.is_empty());
        assert!(!empty.is_finalized());
    }
}
