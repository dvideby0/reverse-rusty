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

/// Final avalanche + zero reservation for a signature key. Extracted from
/// [`sig_key`] so the zero-reservation invariant is unit-testable.
#[inline]
fn finalize_sig(mut h: u64) -> u64 {
    // final avalanche
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    // Reserve 0: the frozen on-disk hash tables (`storage::segment`) use a slot key of
    // 0 as the empty-slot sentinel, so a signature key that hashed to 0 would be
    // INVISIBLE in the frozen table — its posting list never retrieved, a real match
    // silently dropped (a zero-false-negative contract violation, ADR-052). Fold the
    // single 0 value to 1. NOT `h | 1`: that would remap half the keyspace and so
    // silently change the on-disk `.seg` key set (a format break needing a reindex).
    // The avalanche fixes 0 (`avalanche(0) == 0`), so this fold only ever perturbs the
    // ~2^-64 input that accumulates to 0 — every existing segment's keys are unchanged.
    if h == 0 {
        1
    } else {
        h
    }
}

/// Mix a sequence of u32 feature IDs into a stable signature key. Never returns 0
/// (reserved as the frozen-table empty sentinel — see [`finalize_sig`]).
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
    finalize_sig(h)
}

/// The **universal signature** — `sig_key` of the empty feature group, the
/// lossless cover of an empty positive set (ADR-068). Every title generates it
/// implicitly: the match path probes it once per segment (broad lane), making a
/// query stored under it an *always-candidate*. A stable non-zero constant (the
/// FNV basis avalanched); a real feature group colliding with it is FP-only —
/// both postings sit in one list the universal probe always retrieves.
#[inline]
pub fn universal_sig() -> u64 {
    sig_key(&[])
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

#[cfg(test)]
mod tests {
    use super::{finalize_sig, sig_key};

    #[test]
    fn sig_key_reserves_zero() {
        // The frozen-table empty-slot sentinel is key==0, so sig_key must never produce
        // 0. The avalanche fixes 0 (avalanche(0)==0), so a feature vector accumulating to
        // 0 would hash to 0 without the fold; finalize_sig catches exactly that case.
        assert_eq!(finalize_sig(0), 1, "a zero key must be folded to 1");
        // Non-zero keys are NOT perturbed by the fold (only 0 is remapped), which is what
        // keeps existing .seg key sets byte-stable.
        for h in [1u64, 42, 0xdead_beef, 0x8000_0000_0000_0000, u64::MAX] {
            assert_eq!(finalize_sig(h), {
                let mut x = h;
                x ^= x >> 33;
                x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
                x ^= x >> 33;
                x
            });
            assert_ne!(finalize_sig(h), 0);
        }
    }

    #[test]
    fn sig_key_never_zero_over_sample() {
        // No realistic feature vector (including the empty set = the FNV offset basis)
        // produces the reserved 0 key.
        assert_ne!(sig_key(&[]), 0);
        for n in 0u32..4000 {
            assert_ne!(sig_key(&[n]), 0);
            assert_ne!(sig_key(&[n, n.wrapping_mul(7), n ^ 0x5555]), 0);
        }
    }
}
