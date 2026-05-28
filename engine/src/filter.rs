//! Per-segment anchor filter — a compact probabilistic membership filter over
//! signature keys, used to skip segment probes that would definitely miss.
//!
//! Design: docs/design/ingestion-and-updates.md §6 ("our Bloom filters")
//! Invariant: NO false negatives — if a key was inserted, `may_contain` MUST
//!   return true. False positives are allowed (they just cause a wasted probe).
//! Hot path: yes — `may_contain` is called per title-signature per segment
//!
//! Implementation: a **cache-line blocked bloom filter** following the approach
//! proven by RocksDB's Full Filter (since 2014) and backed by Putze et al. 2007.
//!
//! Each block is 512 bits (64 bytes = one cache line = 8 × u64 words). A check
//! touches exactly ONE block — one cache-line fetch, zero additional memory
//! accesses. The key's upper bits select the block; the lower bits derive
//! multiple independent hash probes placed within that single block.
//!
//! This is the right design for Percolator's hot path because the comparison
//! point (hash-map miss with an identity hasher) costs ~1 memory access. A
//! classic bloom with scattered probes is *slower* than no filter at all (we
//! confirmed this empirically). Cache-line blocking matches the 1-access budget.
//!
//! Sizing: ~10 bits/key → ~1.25 bytes/key. A segment with 100k distinct
//! signature keys gets a ~125 KB filter. FPR is ~1% at this density.
//!
//! Decision rationale: see docs/DECISIONS.md (cache-line blocked bloom vs
//! binary fuse vs u64-blocked bloom).

/// Number of u64 words per cache-line block (512 bits / 64 bits = 8).
const WORDS_PER_BLOCK: usize = 8;

/// Number of independent hash probes set per key within a block.
/// At ~10 bits/key with 512-bit blocks, 6 probes gives ~1% FPR
/// (Putze et al. 2007, §3.1).
const NUM_PROBES: u32 = 6;

/// Compact probabilistic membership filter for signature keys.
///
/// Built once when a segment is sealed (immutable); checked on every probe
/// during matching. Answers "is this signature key *possibly* in this segment?"
/// with no false negatives.
///
/// **Cache-line blocked design:** each check is one indexed load of a 64-byte
/// block + NUM_PROBES bit tests within that block. One cache-line access total.
/// This makes the filter competitive with a hash-map miss (also ~1 memory
/// access with the identity hasher), while skipping the miss entirely when the
/// key is absent.
#[derive(Clone)]
pub struct SegmentFilter {
    /// Flat array of u64 words. Blocks of WORDS_PER_BLOCK consecutive words
    /// form 512-bit cache-line-aligned logical blocks. Total length is
    /// `num_blocks * WORDS_PER_BLOCK`.
    data: Vec<u64>,
    /// Number of 512-bit blocks. Always a power of 2.
    num_blocks: usize,
    /// `num_blocks - 1` — bit mask for fast modulo (power of 2).
    mask: u64,
}

impl std::fmt::Debug for SegmentFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentFilter")
            .field("num_blocks", &self.num_blocks)
            .field("heap_bytes", &(self.data.len() * 8))
            .finish()
    }
}

impl SegmentFilter {
    /// Build a filter from a set of u64 signature keys.
    ///
    /// `keys` need not be deduplicated — duplicates are harmless (just redundant
    /// bit-sets). An empty key set produces a filter that rejects everything.
    ///
    /// Sizing targets ~10 bits per key. Each 512-bit block holds ~51 keys at
    /// this density (512 / 10 ≈ 51). We round up to a power of 2 for masking.
    pub fn build(keys: &[u64]) -> Self {
        if keys.is_empty() {
            return SegmentFilter {
                data: Vec::new(),
                num_blocks: 0,
                mask: 0,
            };
        }

        // Target ~10 bits/key → ~51 keys per 512-bit block.
        // Round up to power of 2 for fast masking.
        let target_blocks = (keys.len() / 51).max(1);
        let num_blocks = target_blocks.next_power_of_two();
        let mask = (num_blocks - 1) as u64;
        let mut data = vec![0u64; num_blocks * WORDS_PER_BLOCK];

        for &key in keys {
            let block_start = block_offset(key, mask);
            set_probes(&mut data, block_start, key);
        }

        SegmentFilter { data, num_blocks, mask }
    }

    /// Check if a key is *possibly* present. Returns `false` only when the key
    /// is **definitely** not in the set (no false negatives).
    ///
    /// Cost: one cache-line load (64 bytes) + NUM_PROBES bit tests within that
    /// block. No cross-block access, no loop over the full array.
    #[inline]
    pub fn may_contain(&self, key: u64) -> bool {
        if self.data.is_empty() {
            return false;
        }
        let block_start = block_offset(key, self.mask);
        check_probes(&self.data, block_start, key)
    }

    /// Heap bytes used by the data array.
    pub fn heap_bytes(&self) -> usize {
        self.data.len() * std::mem::size_of::<u64>()
    }

    // ---- accessors for serialization (storage.rs) ----
    pub fn num_blocks_raw(&self) -> usize { self.num_blocks }
    pub fn mask_raw(&self) -> u64 { self.mask }
    pub fn data_raw(&self) -> &[u64] { &self.data }
}

/// Check bloom filter membership on raw slices (used by MmapSegment).
/// `data` is the flat u64 array, `mask` is `num_blocks - 1`.
#[inline]
pub fn bloom_check(key: u64, data: &[u64], mask: u64) -> bool {
    if data.is_empty() {
        return false;
    }
    let block_start = block_offset(key, mask);
    check_probes(data, block_start, key)
}

/// Compute the starting index in `data` for the block this key maps to.
/// Uses the upper 32 bits of the key (the lower bits are used for probe
/// positions), ensuring independence between block selection and probes.
#[inline]
fn block_offset(key: u64, mask: u64) -> usize {
    let block_idx = ((key >> 32) & mask) as usize;
    block_idx * WORDS_PER_BLOCK
}

/// Derive NUM_PROBES independent bit positions within a 512-bit block and
/// set them. Each probe selects one of 512 bit positions (9 bits needed).
///
/// We use a simple double-hashing scheme (Kirsch & Mitzenmacher 2006):
///   h_i = (h1 + i * h2) mod 512
/// where h1 and h2 are derived from non-overlapping portions of the key's
/// lower 32 bits. This gives us as many independent probes as we want from
/// just two base hashes, with provably the same FPR as fully independent
/// hashes.
#[inline]
fn set_probes(data: &mut [u64], block_start: usize, key: u64) {
    let lo = key as u32;
    let h1 = lo & 0x1FF;           // bits 0..8  → 9 bits → range [0, 511]
    let h2 = (lo >> 9) | 1;        // bits 9..   → odd number (ensures full period mod 512)

    for i in 0..NUM_PROBES {
        let bit_pos = (h1.wrapping_add(i.wrapping_mul(h2))) & 0x1FF;
        let word_idx = (bit_pos >> 6) as usize;    // which of the 8 words
        let bit_idx = bit_pos & 63;                 // which bit in that word
        data[block_start + word_idx] |= 1u64 << bit_idx;
    }
}

/// Check whether all NUM_PROBES bit positions for this key are set in the
/// block. Returns false (definite miss) if any probe bit is unset.
#[inline]
fn check_probes(data: &[u64], block_start: usize, key: u64) -> bool {
    let lo = key as u32;
    let h1 = lo & 0x1FF;
    let h2 = (lo >> 9) | 1;

    for i in 0..NUM_PROBES {
        let bit_pos = (h1.wrapping_add(i.wrapping_mul(h2))) & 0x1FF;
        let word_idx = (bit_pos >> 6) as usize;
        let bit_idx = bit_pos & 63;
        if data[block_start + word_idx] & (1u64 << bit_idx) == 0 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter_rejects_everything() {
        let f = SegmentFilter::build(&[]);
        assert!(!f.may_contain(0));
        assert!(!f.may_contain(42));
        assert!(!f.may_contain(u64::MAX));
    }

    #[test]
    fn no_false_negatives() {
        let keys: Vec<u64> = (0..10_000).map(|i| crate::util::sig_key(&[i])).collect();
        let f = SegmentFilter::build(&keys);
        for &k in &keys {
            assert!(f.may_contain(k), "false negative for key {}", k);
        }
    }

    #[test]
    fn no_false_negatives_large() {
        // Larger test to stress the blocked structure.
        let keys: Vec<u64> = (0..200_000).map(|i| crate::util::sig_key(&[i])).collect();
        let f = SegmentFilter::build(&keys);
        for &k in &keys {
            assert!(f.may_contain(k), "false negative for key {}", k);
        }
    }

    #[test]
    fn false_positive_rate_below_5_percent() {
        // Cache-line blocked bloom with ~10 bits/key should achieve ~1% FPR.
        // We bound at 5% to allow margin; print actual for visibility.
        let keys: Vec<u64> = (0..50_000).map(|i| crate::util::sig_key(&[i])).collect();
        let f = SegmentFilter::build(&keys);

        let mut false_positives = 0u64;
        let test_count = 100_000u64;
        for i in 50_000..(50_000 + test_count as u32) {
            let probe = crate::util::sig_key(&[i]);
            if f.may_contain(probe) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / test_count as f64;
        assert!(
            fpr < 0.05,
            "FPR too high: {:.4} ({} / {})",
            fpr,
            false_positives,
            test_count
        );
        // Also print for visibility
        eprintln!(
            "cache-line blocked bloom FPR: {:.4}% ({}/{})",
            fpr * 100.0, false_positives, test_count
        );
    }

    #[test]
    fn memory_is_compact() {
        // 100k keys at ~10 bits/key → ~125 KB.
        let keys: Vec<u64> = (0..100_000).map(|i| i as u64 * 7 + 13).collect();
        let f = SegmentFilter::build(&keys);
        let bytes = f.heap_bytes();
        // At ~1.25 bytes/key: 100k → ~125 KB. Power-of-2 rounding may double.
        assert!(bytes <= 512_000, "filter too large: {} bytes", bytes);
        assert!(bytes >= 64_000, "filter too small: {} bytes", bytes);
        eprintln!(
            "cache-line blocked bloom: {} keys → {} bytes ({:.1} bytes/key)",
            100_000, bytes, bytes as f64 / 100_000.0
        );
    }

    #[test]
    fn power_of_two_block_count() {
        for &n in &[1, 7, 100, 1000, 50_000] {
            let keys: Vec<u64> = (0..n).map(|i| i as u64).collect();
            let f = SegmentFilter::build(&keys);
            assert!(
                f.num_blocks.is_power_of_two(),
                "block count {} is not power of 2 for {} keys",
                f.num_blocks,
                n
            );
        }
    }

    #[test]
    fn block_size_is_cache_line() {
        // Verify each block is exactly 64 bytes (512 bits).
        assert_eq!(WORDS_PER_BLOCK * 8, 64, "block size must be 64 bytes (one cache line)");
    }

    #[test]
    fn probes_are_within_block() {
        // Verify that all probe positions stay within the 512-bit block.
        for key in 0u64..10_000 {
            let lo = key as u32;
            let h1 = lo & 0x1FF;
            let h2 = (lo >> 9) | 1;
            for i in 0..NUM_PROBES {
                let bit_pos = (h1.wrapping_add(i.wrapping_mul(h2))) & 0x1FF;
                assert!(bit_pos < 512, "probe bit_pos {} out of range for key {}", bit_pos, key);
            }
        }
    }
}
