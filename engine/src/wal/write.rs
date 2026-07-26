use super::{
    crc32, Path, Wal, WalEntry, Write, OP_DELETE_LOGICAL, OP_FLUSH_CHECKPOINT, OP_INSERT,
    OP_INSERT_CLASS_D, OP_TOMBSTONE, OP_UPSERT, OP_UPSERT_CLASS_D, SOURCE_GENERATION_MAGIC,
    WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION,
};
use std::io;

impl Wal {
    /// Open or create a WAL file. If the file exists, scans it to find the next
    /// sequence number. If it doesn't exist, creates it with a header.
    ///
    /// `fsync_each_write` selects the per-append durability policy (see
    /// [`Wal::fsync_each_write`]).
    pub fn open(path: &Path, fsync_each_write: bool) -> io::Result<Self> {
        if path.exists() {
            // Open existing, find the max sequence number and current pending count.
            let (entries, _skipped) = Self::read_entries(path)?;
            let next_seq = entries.iter().map(WalEntry::seq).max().unwrap_or(0) + 1;
            // Pending = entries after the last checkpoint (same set recover() replays).
            let pending_entries = match entries
                .iter()
                .rposition(|e| matches!(e, WalEntry::FlushCheckpoint { .. }))
            {
                Some(idx) => (entries.len() - idx - 1) as u64,
                None => entries.len() as u64,
            };
            let size_bytes = std::fs::metadata(path)?.len();
            let file = std::fs::OpenOptions::new().append(true).open(path)?;
            Ok(Wal {
                file,
                path: path.to_path_buf(),
                next_seq,
                fsync_each_write,
                size_bytes,
                pending_entries,
            })
        } else {
            // Create new
            let mut file = std::fs::File::create(path)?;
            file.write_all(&WAL_MAGIC)?;
            file.write_all(&WAL_VERSION.to_le_bytes())?;
            file.sync_all()?;
            Ok(Wal {
                file,
                path: path.to_path_buf(),
                next_seq: 1,
                fsync_each_write,
                size_bytes: WAL_HEADER_SIZE as u64,
                pending_entries: 0,
            })
        }
    }

    /// Flush an append to its configured durability level: an `fsync` (durable
    /// across power loss) when `fsync_each_write` is set, otherwise a userspace
    /// flush that leaves the bytes in the OS page cache until the next
    /// checkpoint (durable across process crash only).
    #[inline]
    fn sync_after_append(&mut self) -> io::Result<()> {
        if self.fsync_each_write {
            self.file.sync_all()
        } else {
            self.file.flush()
        }
    }

    /// Append an Insert entry. Returns the sequence number assigned. `tags` are the
    /// query's `(key, value)` metadata pairs (ADR-049); pass `&[]` for an untagged insert.
    pub fn append_insert(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_INSERT, logical, version, text, tags, None, None)
    }

    pub fn append_insert_ranked(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: i64,
    ) -> io::Result<u64> {
        self.append_insert_like(
            OP_INSERT,
            logical,
            version,
            text,
            tags,
            Some(priority),
            None,
        )
    }

    /// Append an Insert accepted under the class-D lane (WAL v5, ADR-068). Same
    /// payload as [`append_insert`](Self::append_insert); the op code is the
    /// per-frame accept marker, so replay can store it unconditionally while legacy
    /// op-0 frames (logged before classification by pre-v5 binaries) still replay
    /// under the old reject gate.
    pub fn append_insert_class_d(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_INSERT_CLASS_D, logical, version, text, tags, None, None)
    }

    pub fn append_insert_class_d_ranked(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: i64,
    ) -> io::Result<u64> {
        self.append_insert_like(
            OP_INSERT_CLASS_D,
            logical,
            version,
            text,
            tags,
            Some(priority),
            None,
        )
    }

    /// Append an Upsert entry (WAL v4, ADR-067) — the atomic replace-by-id. Same
    /// payload as Insert; the op code is what tells recovery to tombstone the prior
    /// live copies of `logical` before inserting this version.
    pub fn append_upsert(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_UPSERT, logical, version, text, tags, None, None)
    }

    pub fn append_upsert_ranked(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: i64,
    ) -> io::Result<u64> {
        self.append_insert_like(
            OP_UPSERT,
            logical,
            version,
            text,
            tags,
            Some(priority),
            None,
        )
    }

    /// Append an Upsert accepted under the class-D lane (WAL v5, ADR-068) — see
    /// [`append_insert_class_d`](Self::append_insert_class_d).
    pub fn append_upsert_class_d(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> io::Result<u64> {
        self.append_insert_like(OP_UPSERT_CLASS_D, logical, version, text, tags, None, None)
    }

    pub fn append_upsert_class_d_ranked(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: i64,
    ) -> io::Result<u64> {
        self.append_insert_like(
            OP_UPSERT_CLASS_D,
            logical,
            version,
            text,
            tags,
            Some(priority),
            None,
        )
    }

    /// Append an engine-owned Insert carrying its source generation (WAL v7).
    /// This is the live Engine path; the compatibility helpers above deliberately
    /// retain their historical generation-less wire shape for direct callers.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_insert_with_source_generation(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: Option<i64>,
        source_generation: u64,
        class_d_accepted: bool,
    ) -> io::Result<u64> {
        let op = if class_d_accepted {
            OP_INSERT_CLASS_D
        } else {
            OP_INSERT
        };
        self.append_insert_like(
            op,
            logical,
            version,
            text,
            tags,
            priority,
            Some(source_generation),
        )
    }

    /// Append an engine-owned Upsert carrying its source generation (WAL v7).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_upsert_with_source_generation(
        &mut self,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: Option<i64>,
        source_generation: u64,
        class_d_accepted: bool,
    ) -> io::Result<u64> {
        let op = if class_d_accepted {
            OP_UPSERT_CLASS_D
        } else {
            OP_UPSERT
        };
        self.append_insert_like(
            op,
            logical,
            version,
            text,
            tags,
            priority,
            Some(source_generation),
        )
    }

    /// Shared encoder for the insert-shaped ops. Generation-less compatibility
    /// calls retain the v6 optional-priority tail; engine-owned writes use the
    /// marked v7 generation tail.
    // Keep the payload fields explicit here so every compatibility helper chooses
    // its priority/source-generation wire shape at the call site.
    #[allow(clippy::too_many_arguments)]
    fn append_insert_like(
        &mut self,
        op: u8,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        priority: Option<i64>,
        source_generation: Option<u64>,
    ) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let text_bytes = text.as_bytes();
        // tag section: tag_count(2) + per tag key_len(2)+key + val_len(2)+value
        let mut tag_bytes = Vec::new();
        tag_bytes.extend_from_slice(&(tags.len() as u16).to_le_bytes());
        for (k, v) in tags {
            let kb = k.as_bytes();
            let vb = v.as_bytes();
            tag_bytes.extend_from_slice(&(kb.len() as u16).to_le_bytes());
            tag_bytes.extend_from_slice(kb);
            tag_bytes.extend_from_slice(&(vb.len() as u16).to_le_bytes());
            tag_bytes.extend_from_slice(vb);
        }
        // payload: logical(8) + version(4) + text_len(4) + text + tag section
        let extension_len = match source_generation {
            Some(_) => 4 + 8 + 1 + priority.map_or(0, |_| 8),
            None => priority.map_or(0, |_| 8),
        };
        let payload_len = 8 + 4 + 4 + text_bytes.len() + tag_bytes.len() + extension_len;
        // entry body: seq(8) + op(1) + payload
        let body_len = 8 + 1 + payload_len;

        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(op);
        body.extend_from_slice(&logical.to_le_bytes());
        body.extend_from_slice(&version.to_le_bytes());
        body.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(text_bytes);
        body.extend_from_slice(&tag_bytes);
        if let Some(source_generation) = source_generation {
            debug_assert_ne!(
                source_generation, 0,
                "engine-owned WAL generations are non-zero"
            );
            body.extend_from_slice(&SOURCE_GENERATION_MAGIC);
            body.extend_from_slice(&source_generation.to_le_bytes());
            body.push(u8::from(priority.is_some()));
            if let Some(value) = priority {
                body.extend_from_slice(&value.to_le_bytes());
            }
        } else if let Some(value) = priority {
            body.extend_from_slice(&value.to_le_bytes());
        }

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.sync_after_append()?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a Tombstone entry.
    pub fn append_tombstone(&mut self, seg_idx: u32, local_id: u32) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let mut body = Vec::with_capacity(8 + 1 + 8);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_TOMBSTONE);
        body.extend_from_slice(&seg_idx.to_le_bytes());
        body.extend_from_slice(&local_id.to_le_bytes());

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.sync_after_append()?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a DeleteByLogical entry (WAL v3, ADR-066): the address-free
    /// "tombstone every live copy of `logical`" mutation logged by
    /// [`Engine::delete_by_logical_id`](crate::segment::Engine::delete_by_logical_id).
    /// One frame per delete, regardless of how many physical copies it removes.
    pub fn append_delete_logical(&mut self, logical: u64) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let mut body = Vec::with_capacity(8 + 1 + 8);
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_DELETE_LOGICAL);
        body.extend_from_slice(&logical.to_le_bytes());

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.sync_after_append()?;
        // Framed on disk as a 4-byte length prefix + 4-byte CRC + body.
        self.size_bytes += 8 + body.len() as u64;
        self.pending_entries += 1;
        Ok(seq)
    }

    /// Append a FlushCheckpoint entry. Indicates that all prior WAL entries
    /// have been materialized into sealed segments.
    pub fn append_flush_checkpoint(&mut self, segment_file: &str) -> io::Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;

        let name_bytes = segment_file.as_bytes();
        let mut body = Vec::with_capacity(8 + 1 + 4 + name_bytes.len());
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(OP_FLUSH_CHECKPOINT);
        body.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(name_bytes);

        let crc = crc32(&body);
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&body)?;
        self.file.sync_all()?; // fsync on checkpoint
        self.size_bytes += 8 + body.len() as u64; // length prefix + CRC + body
        self.pending_entries = 0; // checkpoint materializes all prior mutations
        Ok(seq)
    }

    /// Sync the WAL to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Current on-disk WAL size in bytes (header + framed entries).
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Number of un-checkpointed entries (mutations not yet in a sealed segment).
    pub fn pending_entries(&self) -> u64 {
        self.pending_entries
    }

    /// The sequence number of the last appended entry (0 if none yet). Sequence
    /// numbers stay monotonic across [`reset`](Self::reset), so this is a valid
    /// high-water mark for the manifest's `wal_seq_watermark` (ADR-066).
    pub fn last_seq(&self) -> u64 {
        self.next_seq - 1
    }

    /// Pin the next sequence number past `watermark` (ADR-066). `reset` keeps the
    /// sequence monotonic only in memory: reopening a reset (header-only) WAL file
    /// rescans it and restarts at 1, while the manifest keeps its old watermark —
    /// so without this, frames appended after the reopen would sort at or below
    /// the watermark and be wrongly skipped by the next recovery (a resurrected
    /// delete). [`Engine::open`](crate::segment::Engine::open) calls this with the
    /// recovered manifest's watermark.
    pub fn ensure_seq_after(&mut self, watermark: u64) {
        if self.next_seq <= watermark {
            self.next_seq = watermark + 1;
        }
    }

    /// Reset the WAL: truncate to just the header. Called after a successful
    /// compaction + manifest write when all data is in sealed segments.
    pub fn reset(&mut self) -> io::Result<()> {
        self.file = std::fs::File::create(&self.path)?;
        self.file.write_all(&WAL_MAGIC)?;
        self.file.write_all(&WAL_VERSION.to_le_bytes())?;
        self.file.sync_all()?;
        self.size_bytes = WAL_HEADER_SIZE as u64;
        self.pending_entries = 0;
        // Don't reset next_seq — keep it monotonic across resets
        Ok(())
    }

    /// Test-only: swap the underlying file for a read-only handle so subsequent
    /// appends fail with an `io::Error`, simulating a disk-full / EIO / revoked
    /// permission fault on a live WAL (an open fd is not affected by `chmod`, so
    /// this is the deterministic way to inject a write fault).
    #[cfg(test)]
    pub(crate) fn break_writes_for_test(&mut self) {
        self.file = std::fs::OpenOptions::new()
            .read(true)
            .open(&self.path)
            .expect("reopen WAL read-only");
    }
}
