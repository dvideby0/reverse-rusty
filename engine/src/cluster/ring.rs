//! Consistent-hash ring over feature IDs.
//!
//! Design: docs/design/clustering-and-scaling.md §3 (entity-anchor consistent
//! hashing). Maps a query's anchor feature — or a title's candidate-anchor
//! feature — to the shard that owns it.
//!
//! The ring is keyed on `FeatureId`, which is safe to do across shards ONLY
//! because every shard shares the coordinator's one frozen `Dict` (see
//! [`crate::cluster::coordinator`]): the same term has the same id everywhere, so
//! the *placement* decision (coordinator side) and the *routing* decision (title
//! side) hit the same ring slot by construction.
//!
//! Virtual nodes spread each shard across the ring so adding/removing a shard
//! moves only ~1/N of the feature ranges — the standard elastic-rebalance
//! property (§3 "Why consistent hashing").

use crate::dict::FeatureId;
use crate::util::fnv1a64;
use std::collections::BTreeMap;

use super::shard::ShardError;

/// Default virtual nodes per shard. 128 keeps per-shard load within a few percent
/// of even for the shard counts we target (a handful to low hundreds).
pub const DEFAULT_VNODES: u32 = 128;

/// A consistent-hash ring mapping `FeatureId -> shard index`.
#[derive(Debug, Clone)]
pub struct HashRing {
    /// ring position -> shard index; `BTreeMap` gives O(log V) clockwise lookup.
    ring: BTreeMap<u64, usize>,
    num_shards: usize,
}

impl HashRing {
    /// Build a ring over `num_shards` shards with `vnodes` virtual nodes each.
    /// Deterministic: positions are `fnv1a64` over (shard, vnode), so two rings
    /// with the same parameters are byte-identical. Errors with [`ShardError::Config`]
    /// if `num_shards` is zero.
    pub fn new(num_shards: usize, vnodes: u32) -> Result<Self, ShardError> {
        if num_shards == 0 {
            return Err(ShardError::Config(
                "HashRing needs at least one shard".into(),
            ));
        }
        let vnodes = vnodes.max(1);
        let mut ring = BTreeMap::new();
        for shard in 0..num_shards {
            for v in 0..vnodes {
                // On the astronomically unlikely position collision, last writer
                // wins; correctness is unaffected (lookup stays deterministic and
                // any owning shard is a valid placement target).
                ring.insert(vnode_pos(shard, v), shard);
            }
        }
        Ok(HashRing { ring, num_shards })
    }

    /// Number of shards this ring distributes over.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// The shard that owns `feature`: the first virtual node clockwise of the
    /// feature's hash, wrapping past the end of the ring.
    #[inline]
    pub fn lookup(&self, feature: FeatureId) -> usize {
        let h = ring_hash(&feature.to_le_bytes());
        self.ring
            .range(h..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map_or(0, |(_, &shard)| shard)
    }
}

/// Hash bytes to a well-distributed ring position. FNV-1a alone clusters badly
/// for sequential small integers (feature ids 0,1,2,… share most bytes), which
/// skews shard load; the murmur3 64-bit finalizer (two avalanche multiplies)
/// spreads them uniformly. Deterministic across runs (no external state).
#[inline]
fn ring_hash(bytes: &[u8]) -> u64 {
    let mut h = fnv1a64(bytes);
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// Deterministic ring position for one virtual node of a shard.
#[inline]
fn vnode_pos(shard: usize, vnode: u32) -> u64 {
    let mut bytes = [0u8; 12];
    bytes[..8].copy_from_slice(&(shard as u64).to_le_bytes());
    bytes[8..].copy_from_slice(&vnode.to_le_bytes());
    ring_hash(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_is_deterministic_and_in_range() {
        let ring = HashRing::new(8, DEFAULT_VNODES).unwrap();
        for f in 0u32..5_000 {
            let s = ring.lookup(f);
            assert!(s < 8, "shard {s} out of range for 8 shards");
            assert_eq!(s, ring.lookup(f), "lookup must be deterministic");
        }
    }

    #[test]
    fn single_shard_ring_routes_everything_to_zero() {
        let ring = HashRing::new(1, DEFAULT_VNODES).unwrap();
        for f in 0u32..1_000 {
            assert_eq!(ring.lookup(f), 0);
        }
    }

    #[test]
    fn zero_shards_is_an_error() {
        assert!(matches!(
            HashRing::new(0, DEFAULT_VNODES),
            Err(ShardError::Config(_))
        ));
    }

    #[test]
    fn distribution_is_roughly_balanced() {
        let k = 8usize;
        let ring = HashRing::new(k, DEFAULT_VNODES).unwrap();
        let n = 80_000u32;
        let mut counts = vec![0usize; k];
        for f in 0..n {
            counts[ring.lookup(f)] += 1;
        }
        let expected = f64::from(n) / k as f64;
        for (s, &c) in counts.iter().enumerate() {
            let ratio = c as f64 / expected;
            assert!(
                ratio > 0.5 && ratio < 1.5,
                "shard {s} got {c} keys (ratio {ratio:.2}) — ring too unbalanced"
            );
        }
    }

    #[test]
    fn adding_a_shard_moves_about_one_over_n_of_keys() {
        let k = 8usize;
        let n = 80_000u32;
        let before = HashRing::new(k, DEFAULT_VNODES).unwrap();
        let after = HashRing::new(k + 1, DEFAULT_VNODES).unwrap();
        let moved = (0..n)
            .filter(|&f| before.lookup(f) != after.lookup(f))
            .count();
        let frac = moved as f64 / f64::from(n);
        let ideal = 1.0 / (k as f64 + 1.0); // ~0.111
                                            // Consistent hashing should move ~1/(k+1) of keys; allow generous slack
                                            // for vnode quantization but reject a full reshuffle.
        assert!(
            frac < ideal * 2.0,
            "adding a shard moved {frac:.3} of keys (ideal ~{ideal:.3}) — not consistent"
        );
    }
}
