# ADR-012: mmap'd segment file format with frozen hash tables

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Sealed segments are immutable — perfect for memory-mapped I/O. The in-memory
  representation uses `HashMap<u64, Posting>` for the candidate index, which can't be mmap'd
  (pointers, allocator metadata). The ExactStore is already struct-of-arrays (flat `Vec`s),
  so it maps naturally to flat byte regions. The key design question is what replaces HashMap
  on disk.
- **Decision:** Custom binary segment file format (`.seg`) with five sections:
  (1) **ExactStore** — each SoA array written sequentially with a count prefix and 8-byte
      alignment padding; directly castable to typed slices via mmap.
  (2) **CandidateIndex** (main + broad) — **frozen open-addressing hash table** with linear
      probing. Each slot is `(key: u64, offset: u32, len: u32)` = 16 bytes. Postings are
      flattened into a contiguous `[u32]` blob; slots point into it. Capacity is next-power-of-2
      above 2× entry count (~50% load factor). Probe uses the sig_key directly (already
      well-mixed via FNV-1a + avalanche).
  (3) **SegmentFilter** — raw u64 array + metadata (num_blocks, mask).
  (4) **Metadata** — cost classes as `[u8]`, alive flags as `[u8]`.
  File header (80 bytes) carries magic, version, query count, and byte offsets to each section.
  Written atomically via tmp-file + rename. No new dependencies (uses `memmap2` only for
  reading).
- **Consequence:** Zero-copy reads — `MmapSegment::open()` mmaps the file and casts pointers
  directly to typed slices. The frozen hash table trades ~2 probes/lookup (at 50% load) for
  zero allocation and OS-managed paging. Cold segments stay on disk; hot segments live in the
  page cache. The per-segment anchor filter (ADR-011) now delivers its full ROI: a bloom check
  in L1 cache skips a frozen-table probe that might page-fault. Backward compatible: engines
  without `data_dir` remain fully in-memory; no existing test changes.
- **Alternatives considered:** (A) Minimal perfect hashing (CHD/MPHF) — optimal space but
  requires an external crate or complex implementation; construction can fail. (B) Sorted array
  with binary search — O(log n) per probe vs O(1) average for open addressing; too slow for
  the hot path. (C) Serializing the Rust HashMap directly — non-portable, not mmap-safe.

