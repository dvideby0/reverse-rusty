use super::{
    crc32, parse_frozen_index, read_u16_slice, read_u32_at, read_u32_slice, read_u64_at,
    read_u64_slice, read_u8_slice, slice_ptr, validate_columns, Arc, FrozenSlot, MmapLogicalIndex,
    MmapSegment, Path, FORMAT_VERSION_COMPOUND_PREDICATE, FORMAT_VERSION_HOT,
    FORMAT_VERSION_OWNERSHIP, FORMAT_VERSION_PHRASE_PREDICATE, FORMAT_VERSION_RANK,
    FORMAT_VERSION_SOURCE_GENERATION, HEADER_SIZE, MAGIC,
};
use std::io;

impl MmapSegment {
    /// AST→compiled-query lowering semantics baked into this file. Legacy files
    /// read as zero because this header word was previously reserved.
    pub fn compiler_semantics_version(&self) -> u32 {
        self.compiler_semantics_version
    }

    /// Whether this segment's file carries the class-D rollback fence (format v4,
    /// ADR-068) — i.e. it holds at least one always-candidate. The manifest commit
    /// ORs this across registered segments to pick its own version word.
    pub fn carries_class_d_fence(&self) -> bool {
        let mut counts = [0u64; 5];
        self.class_counts(&mut counts);
        counts[3] != 0
    }

    /// Whether this segment's file carries the hot-tier fence (format v5,
    /// ADR-105) — i.e. it holds class-H entries a pre-ADR-105 binary would
    /// silently never probe. Propagated to the engine manifest's version word.
    pub fn carries_hot_fence(&self) -> bool {
        self.hot_cap != 0
    }

    /// Whether this file carries v8 source/exact generations. Propagated to the
    /// standalone manifest so older recovery code refuses the corpus loudly
    /// instead of skipping the unreadable segment.
    pub fn carries_source_generation_fence(&self) -> bool {
        self.format_version >= FORMAT_VERSION_SOURCE_GENERATION
    }

    /// Load a segment from a file, memory-mapping it.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        // SAFETY: memory-mapping is unsafe because the mapping aliases the file's
        // bytes and the borrow checker cannot prove the file is not mutated
        // underneath us. Reverse Rusty segment files are immutable once written
        // (segments are append-only and never edited in place; compaction writes
        // a new file and atomically swaps it), so the mapped region is effectively
        // read-only for the lifetime of this `Arc<Mmap>`.
        let mmap = Arc::new(unsafe { memmap2::Mmap::map(&file)? });

        if mmap.len() < HEADER_SIZE + 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "file too small"));
        }
        // Verify trailing CRC32
        {
            let content = &mmap[..mmap.len() - 4];
            let stored_crc = read_u32_at(&mmap, mmap.len() - 4)?;
            if crc32(content) != stored_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("segment file CRC mismatch: {}", path.display()),
                ));
            }
        }
        // We need to parse the mmap contents to extract offsets and lengths,
        // then store raw pointers into the mmap. To satisfy the borrow checker
        // (we move `mmap` into the struct but store pointers derived from it),
        // we use a two-phase approach: parse with a temporary borrow to get
        // offsets/lengths, then construct pointers from the base after move.

        // Phase 1: validate and parse offsets/lengths from a temporary borrow
        let (format_version, compiler_semantics_version) = {
            let data = &mmap[..];
            if data[0..4] != MAGIC {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
            }
            let version = read_u32_at(data, 4)?;
            // v1–v10 are supported (v1 reconstructs the reverse index; v1/v2 read
            // back with an empty tag column; v4 is the class-D fence; v5 adds
            // the hot index; v6 priority, v7 ownership, and v8 source generation
            // append cumulative exact-row columns; v9 adds compound predicates;
            // v10 admits quoted token-graph programs).
            if version == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid format version 0",
                ));
            }
            if version > FORMAT_VERSION_PHRASE_PREDICATE {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("unsupported format version {version}"),
                ));
            }
            let compiler_semantics_version = read_u32_at(data, 12)?;
            if compiler_semantics_version > super::super::CURRENT_COMPILER_SEMANTICS_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("unsupported compiler semantics version {compiler_semantics_version}"),
                ));
            }
            (version, compiler_semantics_version)
        };

        // Phase 2: extract section layout using raw pointer arithmetic.
        // All pointers are derived from `base` which points into `mmap`.
        // After we move `mmap` into the struct, the backing memory doesn't move
        // (it's OS-mapped), so the pointers remain valid for the struct's lifetime.
        let base = mmap.as_ptr();
        let mmap_len = mmap.len();
        // SAFETY: `base`/`mmap_len` come straight from the live `mmap` (still owned
        // on the stack here), so the pointer is valid and aligned for `mmap_len`
        // bytes of `u8`. This borrow is read-only and dropped before `mmap` moves
        // into the struct.
        let data_for_parse = unsafe { std::slice::from_raw_parts(base, mmap_len) };

        let num_queries = read_u32_at(data_for_parse, 8)?;
        let exact_off = read_u64_at(data_for_parse, 16)? as usize;
        let main_off = read_u64_at(data_for_parse, 24)? as usize;
        let broad_off = read_u64_at(data_for_parse, 32)? as usize;
        let filter_off = read_u64_at(data_for_parse, 40)? as usize;
        let meta_off = read_u64_at(data_for_parse, 48)? as usize;

        // ---- Parse exact section ----
        let mut cursor = exact_off;
        let (req_mask_s, next) = read_u64_slice(data_for_parse, cursor)?;
        cursor = next;
        let (forb_mask_s, next) = read_u64_slice(data_for_parse, cursor)?;
        cursor = next;
        let (req_off_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (req_len_s, next) = read_u16_slice(data_for_parse, cursor)?;
        cursor = next;
        let (req_blob_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (forb_off_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (forb_len_s, next) = read_u16_slice(data_for_parse, cursor)?;
        cursor = next;
        let (forb_blob_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (q_group_start_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (q_group_count_s, next) = read_u16_slice(data_for_parse, cursor)?;
        cursor = next;
        let (group_off_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (group_len_s, next) = read_u16_slice(data_for_parse, cursor)?;
        cursor = next;
        let (anyof_blob_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (version_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (logical_s, after_logical) = read_u64_slice(data_for_parse, cursor)?;
        let (priority_s, priority_count, after_priority) = if format_version >= FORMAT_VERSION_RANK
        {
            let (raw, next) = read_u64_slice(data_for_parse, after_logical)?;
            if raw.len() != num_queries as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment priority column length mismatch",
                ));
            }
            // SAFETY: i64/u64 have identical size and alignment; every bit pattern
            // is valid for both, and the immutable slice remains mmap-borrowed.
            let signed =
                unsafe { std::slice::from_raw_parts(raw.as_ptr().cast::<i64>(), raw.len()) };
            (signed, signed.len(), next)
        } else {
            (&[][..], 0usize, after_logical)
        };
        let priority_ptr = if priority_count == 0 {
            std::ptr::NonNull::<i64>::dangling().as_ptr().cast_const()
        } else {
            priority_s.as_ptr()
        };

        let (
            placement_generation_s,
            placement_num_shards_s,
            placement_mode_s,
            placement_off_s,
            placement_len_s,
            placement_blob_s,
            after_placement,
        ) = if format_version >= FORMAT_VERSION_OWNERSHIP {
            let (generation, next) = read_u64_slice(data_for_parse, after_priority)?;
            let (num_shards, next) = read_u32_slice(data_for_parse, next)?;
            let (mode, next) = read_u8_slice(data_for_parse, next)?;
            let (off, next) = read_u32_slice(data_for_parse, next)?;
            let (len, next) = read_u32_slice(data_for_parse, next)?;
            let (blob, next) = read_u32_slice(data_for_parse, next)?;
            let n = num_queries as usize;
            if generation.len() != n
                || num_shards.len() != n
                || mode.len() != n
                || off.len() != n
                || len.len() != n
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment ownership column length mismatch",
                ));
            }
            for i in 0..n {
                let start = off[i] as usize;
                let count = len[i] as usize;
                let positions = blob.get(start..start + count).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "segment ownership positions overrun placement blob",
                    )
                })?;
                crate::ownership::QueryPlacement::from_raw(
                    crate::ownership::PlacementGeneration(generation[i]),
                    num_shards[i],
                    mode[i],
                    positions.to_vec(),
                )
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            }
            (generation, num_shards, mode, off, len, blob, next)
        } else {
            (
                &[][..],
                &[][..],
                &[][..],
                &[][..],
                &[][..],
                &[][..],
                after_priority,
            )
        };
        let placement_count = placement_generation_s.len();
        let (source_generation_s, after_source_generation) =
            if format_version >= FORMAT_VERSION_SOURCE_GENERATION {
                let (generation, next) = read_u64_slice(data_for_parse, after_placement)?;
                if generation.len() != num_queries as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "segment source-generation column length mismatch",
                    ));
                }
                (generation, next)
            } else {
                (&[][..], after_placement)
            };
        let source_generation_count = source_generation_s.len();
        let (predicate_off_s, predicate_len_s, predicate_blob_s) =
            if format_version >= FORMAT_VERSION_COMPOUND_PREDICATE {
                let (off, next) = read_u32_slice(data_for_parse, after_source_generation)?;
                let (len, next) = read_u32_slice(data_for_parse, next)?;
                let (blob, _) = read_u32_slice(data_for_parse, next)?;
                (off, len, blob)
            } else {
                (&[][..], &[][..], &[][..])
            };
        let predicate_count = predicate_off_s.len();

        // ---- Parse main index ----
        let (main_slots_s, main_blob_s, main_cap) = parse_frozen_index(data_for_parse, main_off)?;

        // ---- Parse broad index ----
        let (broad_slots_s, broad_blob_s, broad_cap) =
            parse_frozen_index(data_for_parse, broad_off)?;

        // ---- Parse hot-tier index (v5, ADR-105) ----
        // Pre-v5 files (and, defensively, a v5 header with a zero offset) have no
        // section: cap 0 + dangling pointers, the tag-column soundness pattern.
        let (hot_slots_s, hot_blob_s, hot_cap) = if format_version >= FORMAT_VERSION_HOT {
            let hoff = read_u64_at(data_for_parse, 72)? as usize;
            if hoff != 0 {
                parse_frozen_index(data_for_parse, hoff)?
            } else {
                (&[][..], &[][..], 0usize)
            }
        } else {
            (&[][..], &[][..], 0usize)
        };
        let hot_slots_ptr = if hot_cap != 0 {
            hot_slots_s.as_ptr()
        } else {
            std::ptr::NonNull::<FrozenSlot>::dangling()
                .as_ptr()
                .cast_const()
        };
        let hot_blob_ptr = if hot_blob_s.is_empty() {
            std::ptr::NonNull::<u32>::dangling().as_ptr().cast_const()
        } else {
            hot_blob_s.as_ptr()
        };

        // ---- Parse filter ----
        let filter_num_blocks = read_u32_at(data_for_parse, filter_off)? as usize;
        let filter_mask_val = read_u64_at(data_for_parse, filter_off + 8)?;
        let filter_data_off = filter_off + 16;
        let filter_data_ptr = if filter_num_blocks > 0 {
            // SAFETY: `filter_data_off` is an offset within the CRC-verified mmap
            // (derived from `filter_off`, itself read from the validated header),
            // so `base.add(filter_data_off)` stays in bounds of the mapping. The
            // result is only read back through `filter_data()`, which bounds it to
            // `filter_num_blocks * 8` u64s laid down by the writer.
            unsafe { base.add(filter_data_off).cast::<u64>() }
        } else {
            std::ptr::null()
        };

        // ---- Parse meta ----
        cursor = meta_off;
        let (class_s, next) = read_u8_slice(data_for_parse, cursor)?;
        cursor = next;
        let (alive_s, _) = read_u8_slice(data_for_parse, cursor)?;

        // Validate the class bytes against the version's ceiling ONCE at open
        // (class 4 = H exists only in v5 files; anything higher came from a
        // future build). A corrupt/foreign byte would otherwise be silently
        // mis-bucketed by `class_counts`/`to_memory_segment` — fail loud instead.
        let class_ceiling: u8 = if format_version >= FORMAT_VERSION_HOT {
            4
        } else {
            3
        };
        if let Some(bad) = class_s.iter().find(|&&b| b > class_ceiling) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cost-class byte {bad} exceeds format v{format_version}'s ceiling {class_ceiling}: {}",
                    path.display()
                ),
            ));
        }

        // Build alive overlay from on-disk data; seed the dead set from the same
        // flags so it stays ≡ the overlay's dead entries from the start (ADR-066).
        let alive_overlay: Vec<bool> = alive_s.iter().map(|&b| b != 0).collect();
        let alive_counter = alive_overlay.iter().filter(|&&a| a).count();
        let dead_overlay: roaring::RoaringBitmap = alive_overlay
            .iter()
            .enumerate()
            .filter(|(_, &a)| !a)
            .map(|(i, _)| i as u32)
            .collect();

        // Reverse index (ADR-020 Item 2): v2 borrows the sorted columns straight
        // from the mmap (zero resident heap); v1 reconstructs them in RAM from
        // `logical_arr` (one logical id per local).
        let logical_index = if format_version >= 2 {
            let loff = read_u64_at(data_for_parse, 56)? as usize;
            let (li_logical_s, after) = read_u64_slice(data_for_parse, loff)?;
            let (li_local_s, _) = read_u32_slice(data_for_parse, after)?;
            if li_logical_s.len() != li_local_s.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "logical index column length mismatch",
                ));
            }
            MmapLogicalIndex::Mapped {
                logical: li_logical_s.as_ptr(),
                local: li_local_s.as_ptr(),
                count: li_logical_s.len(),
            }
        } else {
            let mut pairs: Vec<(u64, u32)> = logical_s
                .iter()
                .take(num_queries as usize)
                .enumerate()
                .map(|(i, &lid)| (lid, i as u32))
                .collect();
            pairs.sort_unstable();
            let logical = pairs.iter().map(|&(l, _)| l).collect();
            let local = pairs.iter().map(|&(_, c)| c).collect();
            MmapLogicalIndex::Owned { logical, local }
        };

        // Tag section (ADR-049): v3 borrows the SoA tag columns straight from the mmap;
        // v1/v2 have no section, so the columns read back empty (every query untagged).
        // A non-null dangling pointer keeps the empty-slice accessors sound.
        let (tag_off_s, tag_len_s, tag_blob_ptr, tag_blob_len, tag_count) = if format_version >= 3 {
            let toff = read_u64_at(data_for_parse, 64)? as usize;
            let (tag_off_s, after) = read_u32_slice(data_for_parse, toff)?;
            let (tag_len_s, after2) = read_u16_slice(data_for_parse, after)?;
            let (tag_blob_s, _) = read_u32_slice(data_for_parse, after2)?;
            if tag_off_s.len() != tag_len_s.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "tag column length mismatch",
                ));
            }
            (
                tag_off_s,
                tag_len_s,
                tag_blob_s.as_ptr(),
                tag_blob_s.len(),
                tag_off_s.len(),
            )
        } else {
            (
                &[][..],
                &[][..],
                std::ptr::NonNull::<u32>::dangling().as_ptr().cast_const(),
                0usize,
                0usize,
            )
        };
        let tag_off_ptr = if tag_count != 0 {
            tag_off_s.as_ptr()
        } else {
            std::ptr::NonNull::<u32>::dangling().as_ptr().cast_const()
        };
        let tag_len_ptr = if tag_count != 0 {
            tag_len_s.as_ptr()
        } else {
            std::ptr::NonNull::<u16>::dangling().as_ptr().cast_const()
        };

        // Cross-validate the per-query columns against their blobs once, here at open,
        // so the hot path stays branch-free. Turns an intra-section inconsistency (a
        // CRC-valid offset/length column overrunning its blob) into a fail-loud
        // `InvalidData` error instead of an out-of-bounds slice panic downstream.
        validate_columns(
            format_version,
            num_queries as usize,
            req_off_s,
            req_len_s,
            req_blob_s.len(),
            forb_off_s,
            forb_len_s,
            forb_blob_s.len(),
            q_group_start_s,
            q_group_count_s,
            group_off_s,
            group_len_s,
            anyof_blob_s.len(),
            tag_off_s,
            tag_len_s,
            tag_blob_len,
            predicate_off_s,
            predicate_len_s,
            predicate_blob_s,
        )?;
        let live_phrase_predicates = predicate_off_s
            .iter()
            .zip(predicate_len_s)
            .enumerate()
            .filter(|&(i, (&off, &len))| {
                alive_overlay.get(i).copied().unwrap_or(false) && {
                    let start = off as usize;
                    let end = start + len as usize;
                    crate::exact::predicate_has_phrases(&predicate_blob_s[start..end])
                }
            })
            .count();

        Ok(MmapSegment {
            format_version,
            compiler_semantics_version,
            mmap,
            num_queries,
            req_mask: req_mask_s.as_ptr(),
            forb_mask: forb_mask_s.as_ptr(),
            req_off: req_off_s.as_ptr(),
            req_len: req_len_s.as_ptr(),
            req_blob: req_blob_s.as_ptr(),
            req_blob_len: req_blob_s.len(),
            forb_off: forb_off_s.as_ptr(),
            forb_len: forb_len_s.as_ptr(),
            forb_blob: forb_blob_s.as_ptr(),
            forb_blob_len: forb_blob_s.len(),
            q_group_start: q_group_start_s.as_ptr(),
            q_group_count: q_group_count_s.as_ptr(),
            group_off: group_off_s.as_ptr(),
            group_off_len: group_off_s.len(),
            group_len: group_len_s.as_ptr(),
            anyof_blob: anyof_blob_s.as_ptr(),
            anyof_blob_len: anyof_blob_s.len(),
            tag_off: tag_off_ptr,
            tag_len: tag_len_ptr,
            tag_blob: tag_blob_ptr,
            tag_blob_len,
            tag_count,
            version_arr: version_s.as_ptr(),
            logical_arr: logical_s.as_ptr(),
            priority_arr: priority_ptr,
            priority_count,
            placement_generation: slice_ptr(placement_generation_s),
            placement_num_shards: slice_ptr(placement_num_shards_s),
            placement_mode: slice_ptr(placement_mode_s),
            placement_off: slice_ptr(placement_off_s),
            placement_len: slice_ptr(placement_len_s),
            placement_blob: slice_ptr(placement_blob_s),
            placement_blob_len: placement_blob_s.len(),
            placement_count,
            source_generation: slice_ptr(source_generation_s),
            source_generation_count,
            predicate_off: slice_ptr(predicate_off_s),
            predicate_len: slice_ptr(predicate_len_s),
            predicate_blob: slice_ptr(predicate_blob_s),
            predicate_blob_len: predicate_blob_s.len(),
            predicate_count,
            live_phrase_predicates,
            main_slots: main_slots_s.as_ptr(),
            main_cap,
            main_mask: if main_cap > 0 {
                (main_cap - 1) as u64
            } else {
                0
            },
            main_blob: main_blob_s.as_ptr(),
            main_blob_len: main_blob_s.len(),
            broad_slots: broad_slots_s.as_ptr(),
            broad_cap,
            broad_mask: if broad_cap > 0 {
                (broad_cap - 1) as u64
            } else {
                0
            },
            broad_blob: broad_blob_s.as_ptr(),
            broad_blob_len: broad_blob_s.len(),
            hot_slots: hot_slots_ptr,
            hot_cap,
            hot_mask: if hot_cap > 0 { (hot_cap - 1) as u64 } else { 0 },
            hot_blob: hot_blob_ptr,
            hot_blob_len: hot_blob_s.len(),
            filter_data: filter_data_ptr,
            filter_num_blocks,
            filter_mask: filter_mask_val,
            class_arr: class_s.as_ptr(),
            alive_overlay,
            alive_counter,
            dead_overlay,
            path: path.to_path_buf(),
            vocab_epoch: 0,
            logical_index,
        })
    }
}
