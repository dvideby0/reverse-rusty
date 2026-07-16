//! Tag dictionary — interns per-query metadata `(key, value)` tags to dense `u32`
//! `TagId`s, a space DISJOINT from the feature `FeatureId`s (a separate interner).
//!
//! Design: docs/design/matching.md §5.1; docs/DECISIONS.md ADR-049.
//! Invariant: tag strings die here — everything downstream (the SoA tag column in
//!   `exact.rs`, the filter predicate) is integers only, off the hot path.
//!
//! Mirrors [`crate::dict`] (the feature dictionary), including the dynamic-vocabulary
//! escape hatch: a tag absent from a *frozen* `TagDict` resolves to a deterministic
//! *synthetic* `TagId` (the ADR-046 pattern) so every cluster node agrees on the same
//! id with no coordination. A synthetic collision is a bounded over-match (a query may
//! pass a tag filter it should not — a false-positive *candidate* the caller asked to
//! narrow), never a missed match: tags only ever remove queries, so they cannot cause
//! a false negative. The interned (single-node) path never hashes — it interns exactly.

use crate::util::FastMap;

pub type TagId = u32;

/// Separator between a tag's key and value in the canonical interned string. A control
/// byte (`0x01`) that does not occur in JSON tag keys/values in practice, so
/// `category=a` and `status=a` intern to distinct ids, and the pair can be recovered by
/// splitting on it.
const TAG_SEP: char = '\u{1}';

/// Base of the reserved synthetic-`TagId` range (the top bit of the `u32` space). A tag
/// absent from a frozen dict is assigned a deterministic synthetic id at or above this
/// (dynamic vocabulary, ADR-046), disjoint from the densely-interned ids below it.
pub const SYNTHETIC_TAG_BASE: TagId = 0x8000_0000;

/// True if `id` is a synthetic (hash-assigned) tag id rather than an interned one.
#[inline]
pub fn is_synthetic_tag(id: TagId) -> bool {
    id >= SYNTHETIC_TAG_BASE
}

/// Build the canonical `"key\u{1}value"` string a tag interns under.
fn canonical(key: &str, value: &str) -> String {
    let mut s = String::with_capacity(key.len() + 1 + value.len());
    s.push_str(key);
    s.push(TAG_SEP);
    s.push_str(value);
    s
}

/// Deterministically hash a `(key, value)` tag into the reserved synthetic range. Every
/// node and the coordinator compute the *same* id for the same tag with no coordination
/// (ADR-046) because [`crate::util::fnv1a64`] is stable across runs and processes —
/// exactly the cross-shard agreement filtered percolation needs. Hashes the canonical
/// `key\u{1}value`, so two keys sharing a value never collide.
#[inline]
pub fn synthetic_tag_id(key: &str, value: &str) -> TagId {
    let h = crate::util::fnv1a64(canonical(key, value).as_bytes());
    let folded = (h ^ (h >> 32)) as TagId;
    SYNTHETIC_TAG_BASE | (folded & (SYNTHETIC_TAG_BASE - 1))
}

/// Interns metadata tags to dense `TagId`s. The engine holds one behind `Arc` (copy-on-
/// write), shared into every snapshot like the feature [`crate::dict::Dict`].
#[derive(Clone, Default)]
pub struct TagDict {
    /// canonical `"key\u{1}value"` -> `TagId`
    map: FastMap<String, TagId>,
    /// `TagId` (dense) -> canonical `"key\u{1}value"`
    keys: Vec<String>,
    /// Dense-only cache for the compatibility `priority` tag. `None` means the
    /// tag has another key; `Some(0)` deliberately covers malformed legacy
    /// priority values, preserving their historical score-zero semantics.
    priority_values: Vec<Option<i64>>,
    finalized: bool,
}

impl TagDict {
    pub fn new() -> Self {
        Self::default()
    }

    /// Intern a `(key, value)` tag, creating it if new. The exact (write) path — used
    /// single-node where the dict is mutable; assigns a dense id.
    pub fn intern(&mut self, key: &str, value: &str) -> TagId {
        let c = canonical(key, value);
        if let Some(&id) = self.map.get(&c) {
            return id;
        }
        debug_assert!(
            self.keys.len() < SYNTHETIC_TAG_BASE as usize,
            "interned tag dict reached the reserved synthetic-id range"
        );
        let id = self.keys.len() as TagId;
        self.map.insert(c.clone(), id);
        self.keys.push(c);
        self.priority_values
            .push((key == "priority").then(|| value.parse::<i64>().unwrap_or(0)));
        id
    }

    /// Look up an existing tag without creating it; `None` if absent.
    #[inline]
    pub fn get(&self, key: &str, value: &str) -> Option<TagId> {
        self.map.get(&canonical(key, value)).copied()
    }

    /// Resolve a tag to its interned id, or a deterministic synthetic id if it is absent
    /// from this (frozen) dict — dynamic vocabulary for tags (ADR-046). Used by the
    /// read / filter-compile path and the cluster apply path so a tag first seen after
    /// the dict froze still resolves to a consistent id every node agrees on. An interned
    /// tag keeps its dense id; only true misses are hashed (see [`synthetic_tag_id`]).
    #[inline]
    pub fn get_or_synthetic(&self, key: &str, value: &str) -> TagId {
        match self.map.get(&canonical(key, value)) {
            Some(&id) => id,
            None => synthetic_tag_id(key, value),
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The `(key, value)` for an interned id, or `None` for a synthetic / out-of-range id
    /// (a hashed tag has no stored string). For explain / serialization only — never the
    /// hot path.
    #[inline]
    pub fn key_value(&self, id: TagId) -> Option<(&str, &str)> {
        self.keys
            .get(id as usize)
            .and_then(|c| c.split_once(TAG_SEP))
    }

    /// Integer-only compatibility lookup for the canonical legacy `priority`
    /// tag. Synthetic ids intentionally return `None`: their source string is
    /// not retained, so their documented compatibility score remains zero.
    #[inline]
    pub fn legacy_priority(&self, id: TagId) -> Option<i64> {
        self.priority_values.get(id as usize).copied().flatten()
    }

    /// Resolve the first canonical priority tag in an already sorted/deduped
    /// dense tag slice. This exactly mirrors compatibility ranking's first-tag
    /// behavior without string lookup or parsing on the match path.
    #[inline]
    pub fn legacy_priority_for_tags(&self, tags: &[TagId]) -> i64 {
        tags.iter()
            .find_map(|&id| self.legacy_priority(id))
            .unwrap_or(0)
    }

    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Mark the dict frozen (no further interning expected). Used after a cluster build
    /// so the tag space matches the frozen feature dict's lifecycle; part of the
    /// fingerprint so a frozen vs unfrozen dict are distinguishable.
    pub fn mark_finalized(&mut self) {
        self.finalized = true;
    }

    /// A stable 64-bit fingerprint of the tag space (canonical strings in id order + the
    /// `finalized` flag). Used by the gRPC connect handshake to reject a coordinator /
    /// shard pair whose tag spaces diverged — they would resolve the same tag to
    /// different ids, mis-filtering results. Mirrors [`crate::dict::Dict::fingerprint`];
    /// hashes with [`crate::util::fnv1a64`] (stable across processes — std hashers are
    /// randomized and unusable for a cross-process identity check).
    pub fn fingerprint(&self) -> u64 {
        let mut buf: Vec<u8> = Vec::with_capacity(self.keys.len() * 12 + 5);
        buf.extend_from_slice(&(self.keys.len() as u32).to_le_bytes());
        for c in &self.keys {
            buf.extend_from_slice(&(c.len() as u32).to_le_bytes());
            buf.extend_from_slice(c.as_bytes());
        }
        buf.push(u8::from(self.finalized));
        crate::util::fnv1a64(&buf)
    }

    /// Resident heap bytes used by the tag dictionary (each canonical string is held
    /// twice — once as a `map` key, once in `keys` — like the feature dict).
    pub fn heap_bytes(&self) -> usize {
        use std::mem::size_of;
        let key_chars: usize = self.keys.iter().map(String::capacity).sum();
        let map_key_chars: usize = self.map.keys().map(String::capacity).sum();
        key_chars
            + self.keys.capacity() * size_of::<String>()
            + map_key_chars
            + self.map.capacity() * size_of::<(String, TagId)>()
            + self.priority_values.capacity() * size_of::<Option<i64>>()
    }
}

impl std::fmt::Debug for TagDict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TagDict")
            .field("tags", &self.keys.len())
            .field("finalized", &self.finalized)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_keys_with_same_value_never_collide() {
        let mut td = TagDict::new();
        let cat_a = td.intern("category", "a");
        let status_a = td.intern("status", "a");
        let cat_b = td.intern("category", "b");
        // category=a, status=a, category=b are three distinct ids.
        assert_ne!(cat_a, status_a);
        assert_ne!(cat_a, cat_b);
        // re-interning is idempotent.
        assert_eq!(td.intern("category", "a"), cat_a);
        // interned ids are dense and never land in the synthetic range.
        assert!(!is_synthetic_tag(cat_a));
        assert!(!is_synthetic_tag(status_a));
        assert_eq!(td.len(), 3);
    }

    #[test]
    fn get_or_synthetic_hits_interned_and_hashes_misses() {
        let mut td = TagDict::new();
        let cat_a = td.intern("category", "a");
        // a hit keeps its dense id.
        assert_eq!(td.get_or_synthetic("category", "a"), cat_a);
        assert_eq!(td.get("category", "a"), Some(cat_a));
        // a miss hashes deterministically (matches the free function, so the storage
        // path and the filter-compile path agree), into the synthetic range.
        let miss = td.get_or_synthetic("category", "never-seen");
        assert!(is_synthetic_tag(miss));
        assert_eq!(miss, synthetic_tag_id("category", "never-seen"));
        assert_eq!(td.get("category", "never-seen"), None);
        // synthetic ids are stable across calls — the basis for cross-node agreement.
        assert_eq!(
            synthetic_tag_id("category", "x"),
            synthetic_tag_id("category", "x")
        );
        // a different (key,value) hashes elsewhere (no trivial collision).
        assert_ne!(
            synthetic_tag_id("category", "x"),
            synthetic_tag_id("status", "x")
        );
    }

    #[test]
    fn key_value_round_trips_for_interned_and_is_none_for_synthetic() {
        let mut td = TagDict::new();
        let id = td.intern("category", "trading-cards");
        assert_eq!(td.key_value(id), Some(("category", "trading-cards")));
        // a synthetic id has no stored string.
        assert_eq!(td.key_value(synthetic_tag_id("category", "oov")), None);
    }

    #[test]
    fn fingerprint_reflects_tag_space_and_finalized_flag() {
        let mut a = TagDict::new();
        a.intern("category", "a");
        let mut b = TagDict::new();
        b.intern("category", "a");
        assert_eq!(
            a.fingerprint(),
            b.fingerprint(),
            "same tags ⇒ same fingerprint"
        );
        b.intern("status", "active");
        assert_ne!(a.fingerprint(), b.fingerprint(), "differing tags ⇒ differ");
        // finalized flag participates (mirrors Dict).
        let mut c = TagDict::new();
        c.intern("category", "a");
        c.mark_finalized();
        assert_ne!(a.fingerprint(), c.fingerprint());
    }
}
