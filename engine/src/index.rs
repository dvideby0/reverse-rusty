//! Candidate index — maps signature keys to posting lists of query IDs.
//!
//! Design: docs/design/matching.md §2
//! Invariant: Postings are append-only within a segment; local IDs are sorted
//!   by construction (no per-insert sort/dedup needed)
//! Hot path: yes — probe() is called per title-signature
//!
//! Postings are adaptive by cardinality:
//!   0..=INLINE_CAP ids  -> inline, no heap allocation
//!   INLINE_CAP+1..=ROARING_THRESHOLD -> sorted Vec<u32>
//!   > ROARING_THRESHOLD -> RoaringBitmap (compressed sorted set)

use crate::util::{fast_map, FastMap};
use roaring::RoaringBitmap;

const INLINE_CAP: usize = 8;
/// Postings above this cardinality are promoted to a roaring bitmap.
const ROARING_THRESHOLD: usize = 256;

#[derive(Clone)]
pub enum Posting {
    Inline { ids: [u32; INLINE_CAP], len: u8 },
    Heap(Vec<u32>),
    Roaring(RoaringBitmap),
}

impl std::fmt::Debug for Posting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Posting::Inline { len, .. } => write!(f, "Posting::Inline({len})"),
            Posting::Heap(v) => write!(f, "Posting::Heap({})", v.len()),
            Posting::Roaring(bm) => write!(f, "Posting::Roaring({})", bm.len()),
        }
    }
}

impl Posting {
    #[inline]
    fn new(first: u32) -> Self {
        let mut ids = [0u32; INLINE_CAP];
        ids[0] = first;
        Posting::Inline { ids, len: 1 }
    }

    #[inline]
    fn push(&mut self, id: u32) {
        match self {
            Posting::Inline { ids, len } => {
                if (*len as usize) < INLINE_CAP {
                    ids[*len as usize] = id;
                    *len += 1;
                } else {
                    let mut v = Vec::with_capacity(INLINE_CAP * 2);
                    v.extend_from_slice(&ids[..]);
                    v.push(id);
                    *self = Posting::Heap(v);
                }
            }
            Posting::Heap(v) => {
                v.push(id);
                if v.len() > ROARING_THRESHOLD {
                    let mut bm = RoaringBitmap::new();
                    for &val in v.iter() {
                        bm.insert(val);
                    }
                    *self = Posting::Roaring(bm);
                }
            }
            Posting::Roaring(bm) => {
                bm.insert(id);
            }
        }
    }

    /// Iterate all local IDs in the posting, calling `f` for each.
    /// This is the primary hot-path access method — it handles all three
    /// variants without allocating or materializing a temporary buffer.
    #[inline]
    pub fn for_each<F: FnMut(u32)>(&self, mut f: F) {
        self.for_each_while(|id| {
            f(id);
            true
        });
    }

    /// Iterate until `f` returns false. Exhaustive delivery uses this to stop a
    /// large posting immediately after its bounded sink fails or is cancelled;
    /// ordinary matchers still compile through [`Self::for_each`].
    #[inline]
    pub fn for_each_while<F: FnMut(u32) -> bool>(&self, mut f: F) {
        match self {
            Posting::Inline { ids, len } => {
                for &id in &ids[..*len as usize] {
                    if !f(id) {
                        break;
                    }
                }
            }
            Posting::Heap(v) => {
                for &id in v {
                    if !f(id) {
                        break;
                    }
                }
            }
            Posting::Roaring(bm) => {
                for id in bm {
                    if !f(id) {
                        break;
                    }
                }
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Posting::Inline { len, .. } => *len as usize,
            Posting::Heap(v) => v.len(),
            Posting::Roaring(bm) => bm.len() as usize,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes of heap storage used by this posting (inline postings use none).
    pub fn heap_bytes(&self) -> usize {
        match self {
            Posting::Inline { .. } => 0,
            Posting::Heap(v) => v.capacity() * std::mem::size_of::<u32>(),
            Posting::Roaring(bm) => bm.serialized_size(),
        }
    }
}

#[derive(Clone)]
pub struct CandidateIndex {
    map: FastMap<u64, Posting>,
}

impl Default for CandidateIndex {
    fn default() -> Self {
        CandidateIndex { map: fast_map() }
    }
}

impl std::fmt::Debug for CandidateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CandidateIndex")
            .field("signatures", &self.map.len())
            .field("total_postings", &self.total_postings())
            .finish()
    }
}

impl CandidateIndex {
    pub fn new() -> Self {
        CandidateIndex { map: fast_map() }
    }

    /// Append a local query id under a signature key (ids appended in order).
    pub fn insert(&mut self, sig: u64, local_id: u32) {
        self.map
            .entry(sig)
            .and_modify(|p| p.push(local_id))
            .or_insert_with(|| Posting::new(local_id));
    }

    #[inline]
    pub fn get(&self, sig: u64) -> Option<&Posting> {
        self.map.get(&sig)
    }

    pub fn num_signatures(&self) -> usize {
        self.map.len()
    }

    pub fn total_postings(&self) -> usize {
        self.map.values().map(Posting::len).sum()
    }

    pub fn heap_bytes(&self) -> usize {
        self.map.values().map(Posting::heap_bytes).sum()
    }

    /// Iterate all (sig_key, posting) pairs. Used by compaction to remap and
    /// rebuild the index into a fresh segment.
    pub fn for_each_posting<F: FnMut(u64, &Posting)>(&self, mut f: F) {
        for (&key, posting) in &self.map {
            f(key, posting);
        }
    }

    /// Collect all signature keys in this index. Used to build per-segment
    /// anchor filters (bloom filters over signature keys).
    pub fn keys(&self) -> Vec<u64> {
        self.map.keys().copied().collect()
    }

    /// Posting-length distribution for the perf report (max, and count over a threshold).
    pub fn max_posting_len(&self) -> usize {
        self.map.values().map(Posting::len).max().unwrap_or(0)
    }
    pub fn count_over(&self, threshold: usize) -> usize {
        self.map.values().filter(|p| p.len() > threshold).count()
    }

    /// Append every posting's length to `into` — the raw material for the
    /// `/_stats` per-lane posting-length percentiles (Broad-Query Cost Program
    /// observe-first telemetry). Off the hot path; called on demand per snapshot.
    pub fn collect_posting_lens(&self, into: &mut Vec<u32>) {
        into.extend(self.map.values().map(|p| p.len() as u32));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids_of(p: &Posting) -> Vec<u32> {
        let mut v = Vec::new();
        p.for_each(|id| v.push(id));
        v
    }

    /// The tier ladder at its exact promotion boundaries: 8 ids stay Inline, the 9th
    /// promotes to Heap, 256 stay Heap, the 257th promotes to Roaring — and the id
    /// sequence survives every promotion byte-for-byte (order preserved through Heap;
    /// sorted-set semantics in Roaring, which coincide because ids are appended in
    /// issue order).
    #[test]
    fn tier_promotions_happen_at_exact_boundaries_and_preserve_ids() {
        let mut idx = CandidateIndex::new();
        let sig = 0x0051_6709_u64;

        for id in 0..8u32 {
            idx.insert(sig, id);
        }
        let p = idx.get(sig).expect("posting exists");
        assert!(
            matches!(p, Posting::Inline { len: 8, .. }),
            "8 ids must still be Inline, got {p:?}"
        );
        assert_eq!(ids_of(p), (0..8).collect::<Vec<_>>());

        idx.insert(sig, 8);
        let p = idx.get(sig).expect("posting exists");
        assert!(
            matches!(p, Posting::Heap(_)),
            "the 9th id must promote Inline→Heap, got {p:?}"
        );
        assert_eq!(
            ids_of(p),
            (0..9).collect::<Vec<_>>(),
            "ids lost in promotion"
        );

        for id in 9..=255u32 {
            idx.insert(sig, id);
        }
        let p = idx.get(sig).expect("posting exists");
        assert!(
            matches!(p, Posting::Heap(_)),
            "256 ids must still be Heap (threshold is exclusive), got {p:?}"
        );
        assert_eq!(p.len(), 256, "len mismatch at the Heap ceiling");
        assert_eq!(ids_of(p), (0..=255).collect::<Vec<_>>());

        idx.insert(sig, 256);
        let p = idx.get(sig).expect("posting exists");
        assert!(
            matches!(p, Posting::Roaring(_)),
            "the 257th id (len 257 > threshold 256) must promote Heap→Roaring, got {p:?}"
        );
        assert_eq!(
            ids_of(p),
            (0..=256).collect::<Vec<_>>(),
            "ids lost in Heap→Roaring promotion"
        );
        assert_eq!(p.len(), 257);
    }

    /// Non-contiguous, large-valued ids survive both promotions (Roaring containers
    /// span multiple 64k chunks; nothing about promotion may assume density).
    #[test]
    fn sparse_large_ids_survive_promotions() {
        let mut idx = CandidateIndex::new();
        let sig = 42u64;
        let ids: Vec<u32> = (0..300u32).map(|i| i * 70_001).collect(); // sorted, sparse
        for &id in &ids {
            idx.insert(sig, id);
        }
        let p = idx.get(sig).expect("posting exists");
        assert!(matches!(p, Posting::Roaring(_)));
        assert_eq!(
            ids_of(p),
            ids,
            "sparse ids corrupted across the tier ladder"
        );
        assert!(p.heap_bytes() > 0, "roaring postings report heap use");
    }
}
