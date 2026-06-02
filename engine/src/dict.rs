//! Feature dictionary — interns feature strings to dense `u32` IDs.
//!
//! Design: docs/design/normalization.md §5
//! Invariant: Strings die here — everything downstream is integers only
//! Hot path: ID lookup is on the hot path; frequency tracking is compile-time
//!
//! Tracks the kind of each feature and its query-document frequency, and assigns
//! the 64 "common mask" bits to the most frequent features (used by the exact
//! matcher for near-free reject — see `exact.rs`).

use crate::util::{fast_map, FastMap};

pub type FeatureId = u32;

pub const NO_MASK_BIT: u8 = 64; // sentinel: this feature is not in the common mask

/// Base of the reserved synthetic-ID range (the top bit of the `u32` ID space). A term
/// absent from the frozen dict is assigned a deterministic *synthetic* `FeatureId` at or
/// above this value (dynamic vocabulary, ADR-046), disjoint from the densely-interned IDs
/// below it. The interned range cannot reach this size in practice — a `debug_assert` in
/// [`Dict::intern`] guards the disjointness.
pub const SYNTHETIC_BASE: FeatureId = 0x8000_0000;

/// True if `id` is a synthetic (hash-assigned) feature ID rather than an interned one.
#[inline]
pub fn is_synthetic(id: FeatureId) -> bool {
    id >= SYNTHETIC_BASE
}

/// Deterministically hash a term `name` into the reserved synthetic-ID range. Every node
/// and the coordinator compute the *same* ID for the same name with **no coordination**
/// (ADR-046) because [`crate::util::fnv1a64`] is stable across runs and processes — exactly
/// the cross-shard agreement that content routing needs. A collision is a bounded over-match
/// (a false positive the exact matcher accepts), *never* a missed match.
#[inline]
pub fn synthetic_id(name: &str) -> FeatureId {
    let h = crate::util::fnv1a64(name.as_bytes());
    let folded = (h ^ (h >> 32)) as FeatureId;
    SYNTHETIC_BASE | (folded & (SYNTHETIC_BASE - 1))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeatureKind {
    Year,
    Brand,
    Player,
    /// Domain-specific category term (e.g. "rookie", "refractor" in trading cards,
    /// "retro" or "limited" in sneakers). The catch-all for vocabulary terms that
    /// aren't brands, players, grades, or flags.
    Category,
    Grader,
    Grade,
    GraderGrade,
    Flag,
    Generic,
}

/// Stable byte tag for a [`FeatureKind`], used by [`Dict::fingerprint`]. Explicit (not
/// `as u8`) so reordering the enum variants can't silently change a fingerprint — this
/// mapping is part of the cross-process dict-identity contract.
fn kind_tag(kind: FeatureKind) -> u8 {
    match kind {
        FeatureKind::Year => 0,
        FeatureKind::Brand => 1,
        FeatureKind::Player => 2,
        FeatureKind::Category => 3,
        FeatureKind::Grader => 4,
        FeatureKind::Grade => 5,
        FeatureKind::GraderGrade => 6,
        FeatureKind::Flag => 7,
        FeatureKind::Generic => 8,
    }
}

#[derive(Clone)]
pub struct Dict {
    map: FastMap<String, FeatureId>,
    names: Vec<String>,
    kinds: Vec<FeatureKind>,
    /// query-document frequency: how many compiled queries reference this feature
    freq: Vec<u32>,
    /// feature_id -> common-mask bit index (0..64), or NO_MASK_BIT
    mask_bit: Vec<u8>,
    finalized: bool,
}

impl Dict {
    pub fn new() -> Self {
        Dict {
            map: fast_map(),
            names: Vec::new(),
            kinds: Vec::new(),
            freq: Vec::new(),
            mask_bit: Vec::new(),
            finalized: false,
        }
    }

    /// Intern a feature, creating it if new. `kind` is recorded on first sight.
    pub fn intern(&mut self, name: &str, kind: FeatureKind) -> FeatureId {
        if let Some(&id) = self.map.get(name) {
            return id;
        }
        debug_assert!(
            self.names.len() < SYNTHETIC_BASE as usize,
            "interned dict reached the reserved synthetic-ID range"
        );
        let id = self.names.len() as FeatureId;
        self.map.insert(name.to_string(), id);
        self.names.push(name.to_string());
        self.kinds.push(kind);
        self.freq.push(0);
        self.mask_bit.push(NO_MASK_BIT);
        id
    }

    /// Look up an existing feature without creating it (hot path / titles).
    #[inline]
    pub fn get(&self, name: &str) -> Option<FeatureId> {
        self.map.get(name).copied()
    }

    /// Resolve a feature name to its interned ID, or a deterministic *synthetic* ID if the
    /// term is absent from the (frozen) dict — dynamic vocabulary (ADR-046). The read-only
    /// compile + match paths use this so a term that first appears after the dict is frozen is
    /// *absorbed* (a consistent ID every node agrees on) instead of dropped — dropping would
    /// broaden a query or drop a match. Interned terms keep their dense ID; only true misses
    /// are hashed (see [`synthetic_id`]).
    #[inline]
    pub fn get_or_synthetic(&self, name: &str) -> FeatureId {
        match self.map.get(name) {
            Some(&id) => id,
            None => synthetic_id(name),
        }
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Feature name for an interned ID; `"<oov>"` for a synthetic/out-of-range ID (a hashed
    /// term has no stored name, so explain shows the placeholder).
    #[inline]
    pub fn name(&self, id: FeatureId) -> &str {
        self.names.get(id as usize).map_or("<oov>", String::as_str)
    }
    #[inline]
    pub fn kind(&self, id: FeatureId) -> FeatureKind {
        self.kinds
            .get(id as usize)
            .copied()
            .unwrap_or(FeatureKind::Generic)
    }
    /// Query-document frequency; `0` for a synthetic/out-of-range ID (a hashed term is rare by
    /// construction, so it sorts as the rarest — a good selective anchor).
    #[inline]
    pub fn freq(&self, id: FeatureId) -> u32 {
        self.freq.get(id as usize).copied().unwrap_or(0)
    }
    /// Common-mask bit; `NO_MASK_BIT` for a synthetic/out-of-range ID (hashed terms are never
    /// in the 64-hot mask, so they always land in the exact verifier's non-mask tail).
    #[inline]
    pub fn mask_bit(&self, id: FeatureId) -> u8 {
        self.mask_bit
            .get(id as usize)
            .copied()
            .unwrap_or(NO_MASK_BIT)
    }

    /// Record that a compiled query referenced this feature (drives frequency).
    #[inline]
    pub fn bump_freq(&mut self, id: FeatureId) {
        self.freq[id as usize] = self.freq[id as usize].saturating_add(1);
    }

    /// After all queries are compiled, assign mask bits to the 64 highest-freq
    /// features so the exact matcher can reject most candidates with two u64 ops.
    pub fn finalize_mask(&mut self) {
        let mut idx: Vec<FeatureId> = (0..self.names.len() as FeatureId).collect();
        idx.sort_unstable_by_key(|&id| std::cmp::Reverse(self.freq[id as usize]));
        for b in &mut self.mask_bit {
            *b = NO_MASK_BIT;
        }
        for (bit, &id) in idx.iter().take(64).enumerate() {
            self.mask_bit[id as usize] = bit as u8;
        }
        self.finalized = true;
    }

    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Set frequency and mask bit for a feature directly. Used by Dict
    /// deserialization to restore persisted state without re-computing.
    pub fn set_freq_and_mask(&mut self, id: FeatureId, freq: u32, mask_bit: u8) {
        let i = id as usize;
        if i < self.freq.len() {
            self.freq[i] = freq;
        }
        if i < self.mask_bit.len() {
            self.mask_bit[i] = mask_bit;
        }
    }

    /// Mark the dictionary as finalized without recomputing the mask. Used by
    /// deserialization when the mask bits are already set from persisted data.
    pub fn mark_finalized(&mut self) {
        self.finalized = true;
    }

    /// A stable 64-bit fingerprint of the dict's *correctness-relevant* content: the
    /// `name -> id` mapping (names in id order), each feature's `kind`, and its
    /// common-mask bit, plus the `finalized` flag. Two dicts with equal fingerprints
    /// produce identical matching for any title; a differing fingerprint means their ids
    /// or masks disagree, so matching one side's queries against the other would drop
    /// results.
    ///
    /// Used by the gRPC connect handshake to reject a coordinator/shard pair whose frozen
    /// dicts diverged — the one cross-process false-negative path the fallible seam cannot
    /// otherwise catch (ADR-029).
    ///
    /// Hashes with [`crate::util::fnv1a64`], stable across runs and processes (std hashers
    /// are randomized and unusable for a cross-process identity check). `freq` is
    /// deliberately EXCLUDED: it is build-time-only metadata whose sole match-relevant
    /// effect (which features receive a mask bit) is already captured by `mask_bit`, so
    /// including it would flag false mismatches between dicts that agree where it matters.
    pub fn fingerprint(&self) -> u64 {
        let mut buf: Vec<u8> = Vec::with_capacity(self.names.len() * 16 + 8);
        buf.extend_from_slice(&(self.names.len() as u32).to_le_bytes());
        for ((name, &kind), &mask) in self
            .names
            .iter()
            .zip(self.kinds.iter())
            .zip(self.mask_bit.iter())
        {
            buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.push(kind_tag(kind));
            buf.push(mask);
        }
        buf.push(u8::from(self.finalized));
        crate::util::fnv1a64(&buf)
    }

    /// Resident heap bytes used by the dictionary. The existing per-segment
    /// accounting (exact/index/filter) ignores the dict entirely, yet the dict is
    /// held resident (`Arc<Dict>`) and stores every feature name *twice* — once as
    /// a `map` key and once in `names` — so it is a real, uncounted resident cost.
    /// Counts both string copies plus the parallel metadata vectors.
    pub fn heap_bytes(&self) -> usize {
        use std::mem::size_of;
        let names_chars: usize = self.names.iter().map(String::capacity).sum();
        let map_key_chars: usize = self.map.keys().map(String::capacity).sum();
        names_chars
            + self.names.capacity() * size_of::<String>()
            + map_key_chars
            + self.map.capacity() * size_of::<(String, FeatureId)>()
            + self.kinds.capacity() * size_of::<FeatureKind>()
            + self.freq.capacity() * size_of::<u32>()
            + self.mask_bit.capacity() * size_of::<u8>()
    }
}

impl std::fmt::Debug for Dict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dict")
            .field("features", &self.names.len())
            .field("finalized", &self.finalized)
            .finish()
    }
}

impl Default for Dict {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_ids_are_stable_in_range_and_disjoint_from_interned() {
        // Deterministic: the same name hashes to the same id every call (the basis for
        // every node agreeing with no coordination).
        assert_eq!(synthetic_id("term:vapormax"), synthetic_id("term:vapormax"));
        // Always in the reserved range.
        assert!(is_synthetic(synthetic_id("term:vapormax")));
        assert!(synthetic_id("term:anything") >= SYNTHETIC_BASE);
        // Distinct unknown names get distinct ids (no trivial collision).
        assert_ne!(synthetic_id("term:aaa"), synthetic_id("term:bbb"));

        // Interned ids are dense and never land in the synthetic range.
        let mut d = Dict::new();
        let topps = d.intern("term:topps", FeatureKind::Generic);
        let rookie = d.intern("term:rookie", FeatureKind::Category);
        assert!(!is_synthetic(topps));
        assert!(!is_synthetic(rookie));

        // get_or_synthetic: a hit keeps its dense id; a miss hashes (and matches the
        // free function, so the compile path and the match path agree).
        assert_eq!(d.get_or_synthetic("term:topps"), topps);
        let miss = d.get_or_synthetic("term:never-seen");
        assert!(is_synthetic(miss));
        assert_eq!(miss, synthetic_id("term:never-seen"));
    }

    #[test]
    fn by_id_accessors_are_safe_for_synthetic_ids() {
        // A synthetic id is out of the interned Vecs' range; the accessors must return
        // safe defaults (not panic), so a hashed term flows through compile/exact as a
        // rare, non-mask, unknown-name feature.
        let d = Dict::new();
        let s = synthetic_id("term:oov");
        assert_eq!(d.mask_bit(s), NO_MASK_BIT);
        assert_eq!(d.freq(s), 0);
        assert_eq!(d.kind(s), FeatureKind::Generic);
        assert_eq!(d.name(s), "<oov>");
    }
}
