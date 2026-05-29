//! Small shared utilities — stable hash and fast HashMap alias.
//!
//! Design: — (infrastructure, no dedicated design doc)
//! Invariant: FNV-1a hash must be deterministic across runs (signature keys
//!   must be stable so segments built at different times agree)
//! Hot path: sig_key() is called per title-signature on the match path

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// FNV-1a 64-bit. Deterministic across runs (important: signature keys must be
/// stable so segments built at different times agree). Not used for security.
#[inline]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Mix a sequence of u32 feature IDs into a stable signature key.
#[inline]
pub fn sig_key(features: &[u32]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &f in features {
        // process 4 bytes of the feature id
        h ^= u64::from(f);
        h = h.wrapping_mul(0x0100_0000_01b3);
        h ^= u64::from(f) >> 13;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    // final avalanche
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h
}

/// A trivial fast hasher for u64 keys (signature keys are already well-mixed,
/// so the map hasher can be the identity-with-avalanche). Avoids SipHash cost.
#[derive(Default)]
pub struct IdentityU64Hasher(u64);

impl Hasher for IdentityU64Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Fallback for non-u64 writes (e.g. string keys in the dictionary).
        let mut h = self.0;
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        self.0 = h;
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        // already-mixed key; one more avalanche step for good measure
        let mut x = i;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
        self.0 = x;
    }
}

pub type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<IdentityU64Hasher>>;

pub fn fast_map<K, V>() -> FastMap<K, V> {
    FastMap::default()
}
