use super::{
    bad_sources, crc32, durable_rename, read_u32_at, read_u64_at, Arc, File, LazyBase,
    MetadataLayout, Path, SourceEntryRef, StoredSource, TagsRef, Write, METADATA_KNOWN,
    META_FOOTER, META_HEADER_MARKER, META_IDX_REC, META_IDX_REC_V1, META_MAGIC, META_VERSION,
    META_VERSION_V1, SOURCES_MAGIC, SOURCES_VERSION, SOURCES_VERSION_V1, SRC_HEADER, SRC_IDX_REC,
    TAGS_KNOWN,
};
use std::io;

/// Peek the version field of a sources file (magic-checked).
pub(super) fn peek_sources_version(path: &Path) -> io::Result<u32> {
    use std::io::Read;
    let mut f = File::open(path)?;
    let mut head = [0u8; 8];
    f.read_exact(&mut head)?;
    if head[0..4] != SOURCES_MAGIC {
        return Err(bad_sources());
    }
    Ok(u32::from_le_bytes([head[4], head[5], head[6], head[7]]))
}

pub(super) fn encode_tags(tags: &[(String, String)], out: &mut Vec<u8>) -> io::Result<()> {
    let count = u32::try_from(tags.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many source tags"))?;
    out.extend_from_slice(&count.to_le_bytes());
    for (key, value) in tags {
        let key_len = u32::try_from(key.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source tag key too long"))?;
        let value_len = u32::try_from(value.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "source tag value too long")
        })?;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&value_len.to_le_bytes());
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    Ok(())
}

fn encoded_tag_count(data: &[u8]) -> io::Result<usize> {
    if data.len() < 4 {
        return Err(bad_sources());
    }
    let count = read_u32_at(data, 0)? as usize;
    if count > data.len().saturating_sub(4) / 8 {
        return Err(bad_sources());
    }
    Ok(count)
}

/// Validate and visit the encoded tag slice without allocating owned strings.
/// Lazy open uses the no-op visitor so corruption still fails loud without
/// cloning an entire tagged corpus merely to discard it.
fn visit_encoded_tags(data: &[u8], mut visit: impl FnMut(&str, &str)) -> io::Result<()> {
    let count = encoded_tag_count(data)?;
    let mut cursor = 4usize;
    for _ in 0..count {
        let key_len = read_u32_at(data, cursor)? as usize;
        cursor = cursor.checked_add(4).ok_or_else(bad_sources)?;
        let value_len = read_u32_at(data, cursor)? as usize;
        cursor = cursor.checked_add(4).ok_or_else(bad_sources)?;
        let key_end = cursor.checked_add(key_len).ok_or_else(bad_sources)?;
        let key = std::str::from_utf8(data.get(cursor..key_end).ok_or_else(bad_sources)?)
            .map_err(|_| bad_sources())?;
        cursor = key_end;
        let value_end = cursor.checked_add(value_len).ok_or_else(bad_sources)?;
        let value = std::str::from_utf8(data.get(cursor..value_end).ok_or_else(bad_sources)?)
            .map_err(|_| bad_sources())?;
        cursor = value_end;
        visit(key, value);
    }
    if cursor != data.len() {
        return Err(bad_sources());
    }
    Ok(())
}

fn validate_encoded_tags(data: &[u8]) -> io::Result<()> {
    visit_encoded_tags(data, |_, _| {})
}

pub(super) fn decode_tags(data: &[u8]) -> io::Result<Vec<(String, String)>> {
    let mut tags = Vec::with_capacity(encoded_tag_count(data)?);
    visit_encoded_tags(data, |key, value| {
        tags.push((key.to_owned(), value.to_owned()));
    })?;
    Ok(tags)
}

/// Write a caller-sorted set of source documents as an extended v2 file.
///
/// The original v2 header/index/query blob stays byte-readable by pre-ADR-116
/// binaries. A metadata directory/blob and fixed footer are appended before the
/// existing CRC. Old readers ignore the tail and keep source text on rollback;
/// new readers discover it from `SMET`.
pub(super) fn write_sources_v2(entries: &[SourceEntryRef<'_>], path: &Path) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(SRC_HEADER + entries.len() * SRC_IDX_REC + 64);
    buf.extend_from_slice(&SOURCES_MAGIC);
    buf.extend_from_slice(&SOURCES_VERSION.to_le_bytes());
    let entry_count = u32::try_from(entries.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many query sources"))?;
    buf.extend_from_slice(&entry_count.to_le_bytes());
    buf.extend_from_slice(&META_HEADER_MARKER.to_le_bytes());
    let mut query_blob: Vec<u8> = Vec::new();
    let mut metadata_blob: Vec<u8> = Vec::new();
    let mut query_records: Vec<(u64, u64, u32)> = Vec::with_capacity(entries.len());
    let mut metadata_records: Vec<(u32, u32, u64, u64, u32)> = Vec::with_capacity(entries.len());
    let mut prev: Option<u64> = None;
    for entry in entries {
        debug_assert!(
            prev.is_none_or(|p| p <= entry.logical),
            "write_sources_v2 requires entries sorted by logical id"
        );
        prev = Some(entry.logical);
        let query_off = query_blob.len() as u64;
        let query_len = u32::try_from(entry.query.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "query source too long"))?;
        query_blob.extend_from_slice(entry.query.as_bytes());
        query_records.push((entry.logical, query_off, query_len));

        let metadata_off = metadata_blob.len() as u64;
        match entry.tags {
            TagsRef::Decoded(tags) => encode_tags(tags, &mut metadata_blob)?,
            TagsRef::Encoded(encoded) => metadata_blob.extend_from_slice(encoded),
        }
        let metadata_len = u32::try_from(metadata_blob.len() as u64 - metadata_off)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source metadata too long"))?;
        metadata_records.push((
            (u32::from(entry.tags_known) * TAGS_KNOWN)
                | (u32::from(entry.metadata_known) * METADATA_KNOWN),
            entry.version,
            entry.source_generation,
            metadata_off,
            metadata_len,
        ));
    }
    for (logical, query_off, query_len) in query_records {
        buf.extend_from_slice(&logical.to_le_bytes());
        buf.extend_from_slice(&query_off.to_le_bytes());
        buf.extend_from_slice(&query_len.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    buf.extend_from_slice(&query_blob);

    let metadata_directory_off = buf.len() as u64;
    for (flags, version, source_generation, metadata_off, metadata_len) in metadata_records {
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&version.to_le_bytes());
        buf.extend_from_slice(&source_generation.to_le_bytes());
        buf.extend_from_slice(&metadata_off.to_le_bytes());
        buf.extend_from_slice(&metadata_len.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    buf.extend_from_slice(&metadata_blob);
    buf.extend_from_slice(&META_MAGIC);
    buf.extend_from_slice(&META_VERSION.to_le_bytes());
    buf.extend_from_slice(&metadata_directory_off.to_le_bytes());
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let tmp = path.with_extension("sources.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);
    durable_rename(&tmp, path)?;
    Ok(())
}

fn metadata_layout(
    data: &[u8],
    count: usize,
    query_blob_off: usize,
) -> io::Result<Option<MetadataLayout>> {
    if read_u32_at(data, 12)? != META_HEADER_MARKER {
        return Ok(None);
    }
    let footer_off = data
        .len()
        .checked_sub(4 + META_FOOTER)
        .ok_or_else(bad_sources)?;
    if data.get(footer_off..footer_off + 4) != Some(META_MAGIC.as_slice()) {
        return Err(bad_sources());
    }
    let version = read_u32_at(data, footer_off + 4)?;
    let record_size = match version {
        META_VERSION_V1 => META_IDX_REC_V1,
        META_VERSION => META_IDX_REC,
        _ => return Err(bad_sources()),
    };
    let directory_off = read_u64_at(data, footer_off + 8)? as usize;
    let blob_off = directory_off
        .checked_add(count.checked_mul(record_size).ok_or_else(bad_sources)?)
        .ok_or_else(bad_sources)?;
    if directory_off < query_blob_off || blob_off > footer_off {
        return Err(bad_sources());
    }
    for i in 0..count {
        let record = directory_off + i * record_size;
        let (tags_off, tags_len) = if version == META_VERSION_V1 {
            (
                read_u64_at(data, record + 8)? as usize,
                read_u32_at(data, record + 16)? as usize,
            )
        } else {
            (
                read_u64_at(data, record + 16)? as usize,
                read_u32_at(data, record + 24)? as usize,
            )
        };
        let tags_start = blob_off.checked_add(tags_off).ok_or_else(bad_sources)?;
        let tags_end = tags_start.checked_add(tags_len).ok_or_else(bad_sources)?;
        if tags_end > footer_off {
            return Err(bad_sources());
        }
        validate_encoded_tags(data.get(tags_start..tags_end).ok_or_else(bad_sources)?)?;
    }
    Ok(Some(MetadataLayout {
        version,
        record_size,
        directory_off,
        blob_off,
    }))
}

/// mmap a v2 sources file as a `LazyBase` (validates magic/version/CRC/bounds).
pub(super) fn open_lazy_base(path: &Path) -> io::Result<LazyBase> {
    let file = File::open(path)?;
    // SAFETY: `path` is an immutable, atomically-renamed sources file written by
    // this single-writer engine and never mutated in place (a rewrite goes to a
    // tmp file + rename, leaving this inode untouched). The mapping is read-only,
    // accessed only via safe `&[u8]` slicing, and the `Arc<Mmap>` keeps it alive
    // for as long as any `LazyBase` (or clone) references it — mirroring the
    // `MmapSegment` mmap-open invariant.
    let mmap = Arc::new(unsafe { memmap2::Mmap::map(&file)? });
    let (count, index_off, blob_off, metadata) = {
        let data: &[u8] = &mmap;
        if data.len() < SRC_HEADER + 4 || data[0..4] != SOURCES_MAGIC {
            return Err(bad_sources());
        }
        let version = read_u32_at(data, 4)?;
        if version != SOURCES_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected sources v{SOURCES_VERSION}, got v{version}"),
            ));
        }
        let count = read_u32_at(data, 8)? as usize;
        let index_off = SRC_HEADER;
        let blob_off = index_off
            .checked_add(count.checked_mul(SRC_IDX_REC).ok_or_else(bad_sources)?)
            .ok_or_else(bad_sources)?;
        if blob_off + 4 > data.len() {
            return Err(bad_sources());
        }
        // CRC over everything but the trailing 4-byte checksum.
        let want = read_u32_at(data, data.len() - 4)?;
        if crc32(&data[..data.len() - 4]) != want {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sources CRC mismatch",
            ));
        }
        let blob_limit = data.len() - 4;
        let metadata = metadata_layout(data, count, blob_off)?;
        let query_limit = metadata.map_or(blob_limit, |layout| layout.directory_off);
        let mut previous: Option<u64> = None;
        for i in 0..count {
            let rec = index_off + i * SRC_IDX_REC;
            let logical = read_u64_at(data, rec)?;
            if previous.is_some_and(|prior| prior >= logical) {
                return Err(bad_sources());
            }
            previous = Some(logical);
            let query_off = read_u64_at(data, rec + 8)? as usize;
            let query_len = read_u32_at(data, rec + 16)? as usize;
            let query_start = blob_off.checked_add(query_off).ok_or_else(bad_sources)?;
            let query_end = query_start.checked_add(query_len).ok_or_else(bad_sources)?;
            let query_bytes = data.get(query_start..query_end).ok_or_else(bad_sources)?;
            if query_end > query_limit || std::str::from_utf8(query_bytes).is_err() {
                return Err(bad_sources());
            }
        }
        (count, index_off, blob_off, metadata)
    };
    Ok(LazyBase {
        mmap,
        index_off,
        count,
        blob_off,
        metadata,
    })
}

/// Read any supported `sources.dat` fully into canonical stored documents.
pub(super) fn load_stored_sources(
    path: &Path,
) -> io::Result<crate::util::FastMap<u64, StoredSource>> {
    if !path.exists() {
        return Ok(crate::util::fast_map());
    }
    let data = std::fs::read(path)?;
    if data.len() < 12 || data[0..4] != SOURCES_MAGIC {
        return Err(bad_sources());
    }
    let version = read_u32_at(&data, 4)?;
    let count = read_u32_at(&data, 8)? as usize;
    let mut store = crate::util::FastMap::with_capacity_and_hasher(
        count,
        std::hash::BuildHasherDefault::default(),
    );
    match version {
        SOURCES_VERSION_V1 => {
            let mut cursor = 12;
            for _ in 0..count {
                if cursor + 12 > data.len() {
                    break;
                }
                let logical_id = read_u64_at(&data, cursor)?;
                cursor += 8;
                let text_len = read_u32_at(&data, cursor)? as usize;
                cursor += 4;
                if cursor + text_len > data.len() {
                    break;
                }
                let text = std::str::from_utf8(&data[cursor..cursor + text_len])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .to_string();
                cursor += text_len;
                store.insert(logical_id, StoredSource::legacy(text));
            }
        }
        SOURCES_VERSION => {
            let index_off = SRC_HEADER;
            let blob_off = index_off
                .checked_add(count.checked_mul(SRC_IDX_REC).ok_or_else(bad_sources)?)
                .ok_or_else(bad_sources)?;
            if blob_off + 4 > data.len() {
                return Err(bad_sources());
            }
            let want = read_u32_at(&data, data.len() - 4)?;
            if crc32(&data[..data.len() - 4]) != want {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sources CRC mismatch",
                ));
            }
            let blob_limit = data.len() - 4;
            let metadata = metadata_layout(&data, count, blob_off)?;
            let query_limit = metadata.map_or(blob_limit, |layout| layout.directory_off);
            let mut previous: Option<u64> = None;
            for i in 0..count {
                let rec = index_off + i * SRC_IDX_REC;
                let logical_id = read_u64_at(&data, rec)?;
                if previous.is_some_and(|prior| prior >= logical_id) {
                    return Err(bad_sources());
                }
                previous = Some(logical_id);
                let boff = read_u64_at(&data, rec + 8)? as usize;
                let len = read_u32_at(&data, rec + 16)? as usize;
                let start = blob_off.checked_add(boff).ok_or_else(bad_sources)?;
                let end = start.checked_add(len).ok_or_else(bad_sources)?;
                if end > query_limit {
                    return Err(bad_sources());
                }
                let query = std::str::from_utf8(&data[start..end])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .to_string();
                let source = if let Some(metadata) = metadata {
                    let metadata_rec = metadata.directory_off + i * metadata.record_size;
                    let flags = read_u32_at(&data, metadata_rec)?;
                    let stored_version = read_u32_at(&data, metadata_rec + 4)?;
                    let (source_generation, tags_off, tags_len, metadata_known) =
                        if metadata.version == META_VERSION_V1 {
                            (
                                0,
                                read_u64_at(&data, metadata_rec + 8)? as usize,
                                read_u32_at(&data, metadata_rec + 16)? as usize,
                                true,
                            )
                        } else {
                            (
                                read_u64_at(&data, metadata_rec + 8)?,
                                read_u64_at(&data, metadata_rec + 16)? as usize,
                                read_u32_at(&data, metadata_rec + 24)? as usize,
                                flags & METADATA_KNOWN != 0,
                            )
                        };
                    let tags_start = metadata
                        .blob_off
                        .checked_add(tags_off)
                        .ok_or_else(bad_sources)?;
                    let tags_end = tags_start.checked_add(tags_len).ok_or_else(bad_sources)?;
                    let tags =
                        decode_tags(data.get(tags_start..tags_end).ok_or_else(bad_sources)?)?;
                    StoredSource {
                        query,
                        version: stored_version,
                        source_generation,
                        tags,
                        tags_known: flags & TAGS_KNOWN != 0,
                        metadata_known,
                    }
                } else {
                    StoredSource::legacy(query)
                };
                store.insert(logical_id, source);
            }
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported sources version {other}"),
            ));
        }
    }
    Ok(store)
}

/// Read a source file into the historical query-text-only map used by backup
/// verification and compatibility callers. Metadata is validated and then
/// deliberately projected away.
#[allow(clippy::implicit_hasher)]
pub fn load_query_sources(path: &Path) -> io::Result<crate::util::FastMap<u64, String>> {
    Ok(load_stored_sources(path)?
        .into_iter()
        .map(|(logical, source)| (logical, source.query))
        .collect())
}
