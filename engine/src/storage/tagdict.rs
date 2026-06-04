//! Tag-dictionary binary (de)serialization — the frozen tag space stored inside the
//! engine + cluster manifests (ADR-049), and shipped to data nodes over gRPC
//! (`AdoptDict`, ADR-055). Mirrors [`super::dict`] (the feature dict), including its
//! `magic + version` header and fully-fallible parse (ADR-057). An empty slice (an older
//! manifest that predates the tag space) still reads back as an empty dict — the
//! backward-compatible "no tags" reading — and a pre-ADR-057 (header-less) non-empty blob
//! is still read, so existing on-disk tag spaces open unchanged.

use std::io;

use crate::tagdict::{TagDict, TagId};

use super::{read_u16_at, read_u32_at};

// ---- TagDict serialization (for the manifest + gRPC tag-dict shipping) ----

/// Self-describing header tag for a serialized tag dict (ADR-057). Its little-endian u32
/// value (~1.1 × 10⁹) dwarfs any real tag count, so a legacy (header-less) blob — which
/// opens with `num_tags: u32` — can never be mistaken for a versioned one.
const TAGDICT_MAGIC: [u8; 4] = *b"RTGD";
/// Current tag-dict serialization version. Bump on any *layout* change.
const TAGDICT_VERSION: u32 = 1;
/// magic(4) + version(4).
const TAGDICT_HEADER: usize = 8;

#[inline]
fn invalid<E: Into<Box<dyn std::error::Error + Send + Sync>>>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// Serialize the tag dictionary to a binary format.
///
/// Layout: `[magic: "RTGD"][version: u32][num_tags: u32]`, then per tag
/// `[key_len: u16][key][val_len: u16][value]`, then `[finalized: u8]`. Tags are written in
/// dense-id order, so deserialization re-interns them into the same ids.
pub fn serialize_tagdict(td: &TagDict) -> Vec<u8> {
    let n = td.len();
    let mut buf = Vec::with_capacity(TAGDICT_HEADER + 4 + n * 12 + 1);
    buf.extend_from_slice(&TAGDICT_MAGIC);
    buf.extend_from_slice(&TAGDICT_VERSION.to_le_bytes());
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

/// Deserialize a `TagDict` from bytes produced by [`serialize_tagdict`] (any supported
/// version) or by a pre-ADR-057 build (the header-less "v0" layout). An empty slice (an
/// older manifest that predates the tag space) yields an empty dict.
pub fn deserialize_tagdict(data: &[u8]) -> io::Result<TagDict> {
    if data.is_empty() {
        return Ok(TagDict::new());
    }
    if data.len() >= 4 && data[0..4] == TAGDICT_MAGIC {
        let version = read_u32_at(data, 4)?;
        if version == 0 || version > TAGDICT_VERSION {
            return Err(invalid(format!(
                "unsupported tag-dict format version {version} (this build reads 1..={TAGDICT_VERSION}); \
                 file written by a newer Reverse Rusty"
            )));
        }
        parse_tagdict_body(data, TAGDICT_HEADER)
    } else {
        // Legacy v0 (pre-ADR-057): no header, body starts at byte 0.
        parse_tagdict_body(data, 0)
    }
}

/// Parse the tag-dict field stream at `cursor`. One parser serves both v0 and v1 (identical
/// body layout). Fully fallible: a truncated/corrupt blob errors, never panics.
fn parse_tagdict_body(data: &[u8], mut cursor: usize) -> io::Result<TagDict> {
    let mut td = TagDict::new();
    let n = read_u32_at(data, cursor)? as usize;
    cursor += 4;
    for _ in 0..n {
        let klen = read_u16_at(data, cursor)? as usize;
        cursor += 2;
        let key = std::str::from_utf8(
            data.get(cursor..cursor + klen)
                .ok_or_else(|| invalid("truncated tag key"))?,
        )
        .map_err(invalid)?;
        cursor += klen;
        let vlen = read_u16_at(data, cursor)? as usize;
        cursor += 2;
        let value = std::str::from_utf8(
            data.get(cursor..cursor + vlen)
                .ok_or_else(|| invalid("truncated tag value"))?,
        )
        .map_err(invalid)?;
        cursor += vlen;
        td.intern(key, value);
    }
    if data.get(cursor) == Some(&1) {
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
        // Self-describing header up front.
        assert_eq!(&bytes[0..4], &TAGDICT_MAGIC);
        assert_eq!(read_u32_at(&bytes, 4).unwrap(), TAGDICT_VERSION);
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

    #[test]
    fn legacy_v0_tagdict_without_header_still_reads() {
        // The v1 body is byte-identical to the pre-ADR-057 v0 layout, so a header-stripped
        // blob is a genuine legacy tag space and must still deserialize equally.
        let mut td = TagDict::new();
        td.intern("category", "cards");
        td.intern("status", "active");
        td.mark_finalized();
        let v1 = serialize_tagdict(&td);
        let v0 = &v1[TAGDICT_HEADER..];
        assert_ne!(&v0[0..4], &TAGDICT_MAGIC);
        let got = deserialize_tagdict(v0).expect("legacy v0 deserialize");
        assert_eq!(got.len(), 2);
        assert_eq!(got.get("category", "cards"), Some(0));
        assert_eq!(got.get("status", "active"), Some(1));
        assert!(got.is_finalized());
    }

    #[test]
    fn rejects_a_newer_tagdict_version_loud() {
        let mut td = TagDict::new();
        td.intern("k", "v");
        let mut bytes = serialize_tagdict(&td);
        bytes[4..8].copy_from_slice(&7u32.to_le_bytes());
        let err = deserialize_tagdict(&bytes).expect_err("must reject a newer version");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("version 7"), "got: {err}");
    }

    #[test]
    fn truncation_errors_without_panicking() {
        let mut td = TagDict::new();
        td.intern("category", "trading-cards");
        td.intern("status", "active");
        let full = serialize_tagdict(&td);
        for cut in 0..full.len() {
            let _ = deserialize_tagdict(&full[..cut]); // must not panic
        }
    }
}
