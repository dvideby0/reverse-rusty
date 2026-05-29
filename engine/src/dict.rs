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

    pub fn len(&self) -> usize {
        self.names.len()
    }
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    #[inline]
    pub fn name(&self, id: FeatureId) -> &str {
        &self.names[id as usize]
    }
    #[inline]
    pub fn kind(&self, id: FeatureId) -> FeatureKind {
        self.kinds[id as usize]
    }
    #[inline]
    pub fn freq(&self, id: FeatureId) -> u32 {
        self.freq[id as usize]
    }
    #[inline]
    pub fn mask_bit(&self, id: FeatureId) -> u8 {
        self.mask_bit[id as usize]
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
