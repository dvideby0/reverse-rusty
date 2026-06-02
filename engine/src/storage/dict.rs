//! Feature-dictionary binary (de)serialization — the frozen feature space stored
//! inside the engine + cluster manifests.

use std::io;

use crate::dict::FeatureId;

use super::read_u32_at;

// ---- Dict serialization (for manifest) ----

/// Serialize the feature dictionary to a binary format.
/// Layout: [num_features: u32, then for each: name_len: u16, name: [u8], kind: u8, freq: u32, mask_bit: u8]
/// Followed by: [finalized: u8]
pub fn serialize_dict(dict: &crate::dict::Dict) -> Vec<u8> {
    let mut buf = Vec::new();
    let n = dict.len();
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    for i in 0..n {
        let id = i as FeatureId;
        let name = dict.name(id);
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.push(kind_to_u8(dict.kind(id)));
        buf.extend_from_slice(&dict.freq(id).to_le_bytes());
        buf.push(dict.mask_bit(id));
    }
    buf.push(u8::from(dict.is_finalized()));
    buf
}

/// Deserialize a Dict from bytes produced by `serialize_dict`.
pub fn deserialize_dict(data: &[u8]) -> io::Result<crate::dict::Dict> {
    use crate::dict::Dict;
    let mut cursor = 0usize;
    if data.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "dict too short"));
    }
    let n = read_u32_at(data, cursor)? as usize;
    cursor += 4;
    let mut dict = Dict::new();
    for _ in 0..n {
        let name_len = data
            .get(cursor..cursor + 2)
            .and_then(|s| <[u8; 2]>::try_from(s).ok())
            .map(u16::from_le_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated dict name_len"))?
            as usize;
        cursor += 2;
        let name =
            std::str::from_utf8(data.get(cursor..cursor + name_len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated dict name")
            })?)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        cursor += name_len;
        let kind = u8_to_kind(data[cursor]);
        cursor += 1;
        let freq = read_u32_at(data, cursor)?;
        cursor += 4;
        let mask_bit = data[cursor];
        cursor += 1;
        dict.intern(name, kind);
        dict.set_freq_and_mask(dict.len() as FeatureId - 1, freq, mask_bit);
    }
    if cursor < data.len() && data[cursor] == 1 {
        dict.mark_finalized();
    }
    Ok(dict)
}

fn kind_to_u8(k: crate::dict::FeatureKind) -> u8 {
    use crate::dict::FeatureKind::{
        Brand, Category, Flag, Generic, Grade, Grader, GraderGrade, Player, Year,
    };
    match k {
        Year => 0,
        Brand => 1,
        Player => 2,
        Category => 3,
        Grader => 4,
        Grade => 5,
        GraderGrade => 6,
        Flag => 7,
        Generic => 8,
    }
}

fn u8_to_kind(b: u8) -> crate::dict::FeatureKind {
    use crate::dict::FeatureKind::{
        Brand, Category, Flag, Generic, Grade, Grader, GraderGrade, Player, Year,
    };
    match b {
        0 => Year,
        1 => Brand,
        2 => Player,
        3 => Category,
        4 => Grader,
        5 => Grade,
        6 => GraderGrade,
        7 => Flag,
        _ => Generic,
    }
}
