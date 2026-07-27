use super::{
    decode_tags, read_u32_at, read_u64_at, LazyBase, SourceRecord, StoredSource, METADATA_KNOWN,
    META_VERSION_V1, SRC_IDX_REC, TAGS_KNOWN,
};

impl LazyBase {
    #[inline]
    pub(super) fn logical_at(&self, i: usize) -> Option<u64> {
        read_u64_at(&self.mmap, self.index_off + i * SRC_IDX_REC).ok()
    }

    fn query_record(&self, i: usize) -> Option<(u64, &str)> {
        let data: &[u8] = &self.mmap;
        let rec = self.index_off + i * SRC_IDX_REC;
        let logical = read_u64_at(data, rec).ok()?;
        let query_off = read_u64_at(data, rec + 8).ok()? as usize;
        let query_len = read_u32_at(data, rec + 16).ok()? as usize;

        let query_start = self.blob_off.checked_add(query_off)?;
        let query_end = query_start.checked_add(query_len)?;
        let query = std::str::from_utf8(data.get(query_start..query_end)?).ok()?;
        Some((logical, query))
    }

    pub(super) fn record(&self, i: usize) -> Option<SourceRecord<'_>> {
        let data: &[u8] = &self.mmap;
        let (logical, query) = self.query_record(i)?;
        let (version, source_generation, tags_known, metadata_known, encoded_tags) =
            match self.metadata {
                Some(metadata) => {
                    let metadata_rec = metadata.directory_off + i * metadata.record_size;
                    let flags = read_u32_at(data, metadata_rec).ok()?;
                    let version = read_u32_at(data, metadata_rec + 4).ok()?;
                    let (source_generation, tags_off, tags_len, metadata_known) =
                        if metadata.version == META_VERSION_V1 {
                            (
                                0,
                                read_u64_at(data, metadata_rec + 8).ok()? as usize,
                                read_u32_at(data, metadata_rec + 16).ok()? as usize,
                                true,
                            )
                        } else {
                            (
                                read_u64_at(data, metadata_rec + 8).ok()?,
                                read_u64_at(data, metadata_rec + 16).ok()? as usize,
                                read_u32_at(data, metadata_rec + 24).ok()? as usize,
                                flags & METADATA_KNOWN != 0,
                            )
                        };
                    let tags_start = metadata.blob_off.checked_add(tags_off)?;
                    let tags_end = tags_start.checked_add(tags_len)?;
                    (
                        version,
                        source_generation,
                        flags & TAGS_KNOWN != 0,
                        metadata_known,
                        Some(data.get(tags_start..tags_end)?),
                    )
                }
                None => (1, 0, false, false, None),
            };
        Some(SourceRecord {
            logical,
            query,
            version,
            source_generation,
            tags_known,
            metadata_known,
            encoded_tags,
        })
    }

    fn index_of(&self, logical: u64) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let found = self.logical_at(mid)?;
            match found.cmp(&logical) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    pub(super) fn find(&self, logical: u64) -> Option<SourceRecord<'_>> {
        self.record(self.index_of(logical)?)
    }

    /// Read one source only when it fits the caller's remaining byte credit.
    /// The query index/blob is deliberately read without touching the optional
    /// metadata directory/blob; winner enrichment remains the pre-ADR-116
    /// query-only mmap path. The query length is checked before `to_owned`, so an
    /// over-budget source is rejected without allocating its text.
    pub(super) fn get_bounded(
        &self,
        logical: u64,
        max_bytes: usize,
    ) -> Result<Option<String>, usize> {
        let Some(i) = self.index_of(logical) else {
            return Ok(None);
        };
        let Some((_, query)) = self.query_record(i) else {
            return Ok(None);
        };
        if query.len() > max_bytes {
            return Err(query.len());
        }
        Ok(Some(query.to_owned()))
    }

    /// Read one source only when its internal generation matches the exact row
    /// held by the caller's snapshot. This touches the fixed-size metadata
    /// directory but never decodes tags.
    pub(super) fn get_bounded_at_generation(
        &self,
        logical: u64,
        expected_generation: u64,
        max_bytes: usize,
    ) -> Result<Option<String>, usize> {
        let Some(record) = self.find(logical) else {
            return Ok(None);
        };
        if record.source_generation != expected_generation {
            return Ok(None);
        }
        if record.query.len() > max_bytes {
            return Err(record.query.len());
        }
        Ok(Some(record.query.to_owned()))
    }

    pub(super) fn get_document(&self, logical: u64) -> Option<StoredSource> {
        let record = self.find(logical)?;
        let tags = match record.encoded_tags {
            Some(encoded) => decode_tags(encoded).ok()?,
            None => Vec::new(),
        };
        Some(StoredSource {
            query: record.query.to_owned(),
            version: record.version,
            source_generation: record.source_generation,
            tags,
            tags_known: record.tags_known,
            metadata_known: record.metadata_known,
        })
    }
}
