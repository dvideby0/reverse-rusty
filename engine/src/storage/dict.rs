//! Feature-dictionary binary (de)serialization — the frozen feature space stored
//! inside the engine + cluster manifests, and shipped to data nodes over gRPC
//! (`AdoptDict`, ADR-034).
//!
//! **Versioned (ADR-057).** The blob carries a `magic + version` header so a layout
//! change (or a newer build that added a `FeatureKind`) fails loud instead of silently
//! misparsing — the one binary frozen-space format that previously had no version. A
//! pre-ADR-057 (header-less, "v0") blob is still read, so existing on-disk manifests open
//! unchanged. The kind↔byte mapping is the canonical [`crate::dict::kind_tag`] /
//! [`crate::dict::kind_from_tag`] pair (shared with the cross-process fingerprint), and the
//! body parse is fully fallible — a truncated/corrupt blob yields an `InvalidData` error,
//! never a panic (the "no panics in library code" invariant).

use std::io;

use super::{read_u16_at, read_u32_at};

// ---- Dict serialization (for the manifest + gRPC dict shipping) ----

/// Self-describing header tag for a serialized feature dict (ADR-057). Its little-endian
/// u32 value (~1.4 × 10⁹) is far larger than any real feature count, so a legacy (header-
/// less) blob — which opens with `num_features: u32` — can never be mistaken for a
/// versioned one.
const DICT_MAGIC: [u8; 4] = *b"RDCT";
/// Current dict serialization version. Bump on any *layout* change (a new per-feature
/// field, a reordering); a new `FeatureKind` variant does not need a bump (the layout is
/// unchanged) but does require updating [`crate::dict::kind_from_tag`], and an old build
/// then fails loud on the unknown tag rather than reading it as `Generic`.
const DICT_VERSION: u32 = 1;
/// magic(4) + version(4).
const DICT_HEADER: usize = 8;

#[inline]
fn invalid<E: Into<Box<dyn std::error::Error + Send + Sync>>>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// Serialize the feature dictionary to a binary format.
///
/// Layout: `[magic: "RDCT"][version: u32][num_features: u32]`, then per feature
/// `[name_len: u16][name][kind: u8][freq: u32][mask_bit: u8]`, then `[finalized: u8]`.
pub fn serialize_dict(dict: &crate::dict::Dict) -> Vec<u8> {
    use crate::dict::{kind_tag, FeatureId};
    let n = dict.len();
    // header + ~10 bytes/feature (over the name) + finalized
    let mut buf = Vec::with_capacity(DICT_HEADER + 4 + n * 10 + 1);
    buf.extend_from_slice(&DICT_MAGIC);
    buf.extend_from_slice(&DICT_VERSION.to_le_bytes());
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    for i in 0..n {
        let id = i as FeatureId;
        let name_bytes = dict.name(id).as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.push(kind_tag(dict.kind(id)));
        buf.extend_from_slice(&dict.freq(id).to_le_bytes());
        buf.push(dict.mask_bit(id));
    }
    buf.push(u8::from(dict.is_finalized()));
    buf
}

/// Deserialize a `Dict` from bytes produced by [`serialize_dict`] (any supported version)
/// or by a pre-ADR-057 build (the header-less "v0" layout).
pub fn deserialize_dict(data: &[u8]) -> io::Result<crate::dict::Dict> {
    if data.len() >= 4 && data[0..4] == DICT_MAGIC {
        let version = read_u32_at(data, 4)?;
        if version == 0 || version > DICT_VERSION {
            return Err(invalid(format!(
                "unsupported dict format version {version} (this build reads 1..={DICT_VERSION}); \
                 file written by a newer Reverse Rusty"
            )));
        }
        // v1 body is byte-identical to v0; a future layout change branches on `version` here.
        parse_dict_body(data, DICT_HEADER)
    } else {
        // Legacy v0 (pre-ADR-057): no header, body starts at byte 0.
        parse_dict_body(data, 0)
    }
}

/// Parse the dict field stream starting at `cursor`. One parser serves both v0 (header-less)
/// and v1 because their body layouts are identical — the version guard in [`deserialize_dict`]
/// is what makes a *future* divergent layout fail loud. Fully fallible: a truncated or corrupt
/// blob errors (`InvalidData`), never panics.
fn parse_dict_body(data: &[u8], mut cursor: usize) -> io::Result<crate::dict::Dict> {
    use crate::dict::{kind_from_tag, Dict, FeatureId};
    let n = read_u32_at(data, cursor)? as usize;
    cursor += 4;
    let mut dict = Dict::new();
    for _ in 0..n {
        let name_len = read_u16_at(data, cursor)? as usize;
        cursor += 2;
        let name = std::str::from_utf8(
            data.get(cursor..cursor + name_len)
                .ok_or_else(|| invalid("truncated dict name"))?,
        )
        .map_err(invalid)?;
        cursor += name_len;
        let tag = *data
            .get(cursor)
            .ok_or_else(|| invalid("truncated dict feature-kind"))?;
        let kind = kind_from_tag(tag)
            .ok_or_else(|| invalid(format!("unknown dict feature-kind tag {tag}")))?;
        cursor += 1;
        let freq = read_u32_at(data, cursor)?;
        cursor += 4;
        let mask_bit = *data
            .get(cursor)
            .ok_or_else(|| invalid("truncated dict mask_bit"))?;
        cursor += 1;
        dict.intern(name, kind);
        dict.set_freq_and_mask(dict.len() as FeatureId - 1, freq, mask_bit);
    }
    // Optional trailing finalized flag (absent ⇒ not finalized).
    if data.get(cursor) == Some(&1) {
        dict.mark_finalized();
    }
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict::{Dict, FeatureId, FeatureKind};

    /// Build a dict exercising every `FeatureKind`, plus freq + mask + finalized state.
    fn sample_dict() -> Dict {
        let mut d = Dict::new();
        let kinds = [
            ("year:1986", FeatureKind::Year),
            ("brand:topps", FeatureKind::Brand),
            ("player:jordan", FeatureKind::Player),
            ("cat:rookie", FeatureKind::Category),
            ("grader:psa", FeatureKind::Grader),
            ("grade:10", FeatureKind::Grade),
            ("gg:psa10", FeatureKind::GraderGrade),
            ("flag:auto", FeatureKind::Flag),
            ("term:misc", FeatureKind::Generic),
        ];
        for (i, (name, kind)) in kinds.iter().enumerate() {
            let id = d.intern(name, *kind);
            d.set_freq_and_mask(id, (i as u32 + 1) * 7, i as u8);
        }
        d.mark_finalized();
        d
    }

    fn assert_dict_eq(a: &Dict, b: &Dict) {
        assert_eq!(a.len(), b.len(), "len");
        for i in 0..a.len() {
            let id = i as FeatureId;
            assert_eq!(a.name(id), b.name(id), "name[{i}]");
            assert_eq!(a.kind(id), b.kind(id), "kind[{i}]");
            assert_eq!(a.freq(id), b.freq(id), "freq[{i}]");
            assert_eq!(a.mask_bit(id), b.mask_bit(id), "mask_bit[{i}]");
        }
        assert_eq!(a.is_finalized(), b.is_finalized(), "finalized");
    }

    #[test]
    fn round_trips_with_header_and_all_kinds() {
        let d = sample_dict();
        let bytes = serialize_dict(&d);
        // The blob is self-describing: magic + version up front.
        assert_eq!(&bytes[0..4], &DICT_MAGIC);
        assert_eq!(read_u32_at(&bytes, 4).unwrap(), DICT_VERSION);
        let got = deserialize_dict(&bytes).expect("deserialize");
        assert_dict_eq(&d, &got);
        // And the fingerprint (the cross-process identity) is preserved through the round trip.
        assert_eq!(d.fingerprint(), got.fingerprint());
    }

    #[test]
    fn legacy_v0_blob_without_header_still_reads() {
        // The v1 body is byte-identical to the pre-ADR-057 v0 layout, so stripping the
        // 8-byte header yields a genuine legacy blob — exactly what an existing on-disk
        // manifest holds. It must still deserialize to the same dict.
        let d = sample_dict();
        let v1 = serialize_dict(&d);
        let v0 = &v1[DICT_HEADER..];
        assert_ne!(
            &v0[0..4],
            &DICT_MAGIC,
            "stripped blob must not look versioned"
        );
        let got = deserialize_dict(v0).expect("legacy v0 deserialize");
        assert_dict_eq(&d, &got);

        // Also a hand-built minimal v0 blob (one Generic feature, freq 3, mask 5, finalized),
        // independent of the current serializer, proves the legacy reader directly.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes()); // num_features
        buf.extend_from_slice(&5u16.to_le_bytes()); // name_len
        buf.extend_from_slice(b"alpha");
        buf.push(8); // kind tag: Generic
        buf.extend_from_slice(&3u32.to_le_bytes()); // freq
        buf.push(5); // mask_bit
        buf.push(1); // finalized
        let hand = deserialize_dict(&buf).expect("hand-built v0");
        assert_eq!(hand.len(), 1);
        assert_eq!(hand.name(0), "alpha");
        assert_eq!(hand.kind(0), FeatureKind::Generic);
        assert_eq!(hand.freq(0), 3);
        assert_eq!(hand.mask_bit(0), 5);
        assert!(hand.is_finalized());
    }

    #[test]
    fn rejects_a_newer_format_version_loud() {
        let mut bytes = serialize_dict(&sample_dict());
        bytes[4..8].copy_from_slice(&999u32.to_le_bytes()); // bump the version field
        let err = deserialize_dict(&bytes).expect_err("must reject a newer version");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("version 999"), "got: {err}");
    }

    #[test]
    fn rejects_an_unknown_kind_tag_instead_of_silently_downgrading() {
        // A single Generic feature, then overwrite its kind byte with an unmapped tag.
        let mut d = Dict::new();
        d.intern("only", FeatureKind::Generic);
        let mut bytes = serialize_dict(&d);
        // kind byte = header(8) + num_features(4) + name_len(2) + name(4) = offset 18.
        let kind_off = DICT_HEADER + 4 + 2 + "only".len();
        bytes[kind_off] = 200; // not a known FeatureKind tag
        let err = deserialize_dict(&bytes).expect_err("must reject an unknown kind tag");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("200"), "got: {err}");
    }

    #[test]
    fn truncation_errors_without_panicking() {
        let full = serialize_dict(&sample_dict());
        // Truncating at every length must yield an error (or, for a complete prefix that
        // happens to parse, an Ok) — but never a panic. The header-present truncations and
        // mid-record truncations all exercise the fallible reads.
        for cut in 0..full.len() {
            let _ = deserialize_dict(&full[..cut]); // must not panic
        }
    }
}
