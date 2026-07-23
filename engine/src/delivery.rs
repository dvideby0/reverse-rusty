//! Bounded exhaustive-result delivery primitives (ADR-114).
//!
//! These types describe the lean engine/sink boundary only. HTTP, gRPC, broker
//! retries, job retention, and admission live in the server/distributed layers.

use serde::{Deserialize, Serialize};

use crate::result::QueryScope;
use crate::segment::MatchStats;

pub const DEFAULT_MATCH_CHUNK_SIZE: usize = 512;
pub const MAX_MATCH_CHUNK_SIZE: usize = 16_384;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExhaustiveOptions {
    pub query_scope: QueryScope,
    pub chunk_size: usize,
}

impl Default for ExhaustiveOptions {
    fn default() -> Self {
        Self {
            query_scope: QueryScope::Standard,
            chunk_size: DEFAULT_MATCH_CHUNK_SIZE,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExhaustiveMatch {
    pub logical_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MatchChunk {
    pub sequence: u64,
    pub matches: Vec<ExhaustiveMatch>,
}

/// An order-independent set checksum. `xor` and `sum` use different mixes of
/// the same member payload; both compose across ownership-disjoint shards.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryChecksum {
    pub xor: u64,
    pub sum: u64,
}

impl DeliveryChecksum {
    pub fn observe(&mut self, member: ExhaustiveMatch) {
        // Presence is domain-separated independently in BOTH accumulators.
        // Encoding `None` as one sentinel in the score word let a valid
        // `Some(value)` XOR into that same word, making absence and that score
        // indistinguishable in the completion checksum.
        let (score_first, score_second) = match member.score {
            None => (mix64(0x9e37_79b9_7f4a_7c15), mix64(0xa076_1d64_78bd_642f)),
            Some(value) => (
                mix64((value as u64) ^ 0xd6e8_feb8_6659_fd93),
                mix64((value as u64) ^ 0xe703_7ed1_a0b4_28db),
            ),
        };
        let first = mix64(member.logical_id ^ score_first.rotate_left(23));
        let second =
            mix64(score_second ^ member.logical_id.rotate_right(17) ^ 0x8ebc_6af0_9c88_c6e3);
        self.xor ^= first;
        self.sum = self.sum.wrapping_add(second);
    }

    pub fn merge(&mut self, other: Self) {
        self.xor ^= other.xor;
        self.sum = self.sum.wrapping_add(other.sum);
    }
}

#[inline]
fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExhaustiveSummary {
    pub exact_total: u64,
    pub chunk_count: u64,
    pub checksum: DeliveryChecksum,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExhaustiveMatchResult {
    pub summary: ExhaustiveSummary,
    pub stats: MatchStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkSinkError {
    detail: String,
}

impl ChunkSinkError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for ChunkSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for ChunkSinkError {}

/// Synchronous on purpose: a bounded implementation blocks/yields at the
/// chunk boundary and therefore propagates downstream backpressure to matching.
pub trait ChunkSink: Send {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError>;

    /// Poll an out-of-band cancellation or transport failure even when matching
    /// has not produced a full chunk. The default keeps existing sinks
    /// infallible; job and gRPC sinks override it so a zero-result or
    /// below-chunk-size match can still stop promptly.
    fn check_cancelled(&mut self) -> Result<(), ChunkSinkError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExhaustiveMatchError {
    InvalidChunkSize { requested: usize, max: usize },
    Cancelled,
    Sink(ChunkSinkError),
}

impl std::fmt::Display for ExhaustiveMatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidChunkSize { requested, max } => {
                write!(f, "exhaustive chunk size {requested} is outside 1..={max}")
            }
            Self::Cancelled => f.write_str("exhaustive matching deadline exceeded"),
            Self::Sink(error) => write!(f, "exhaustive delivery sink failed: {error}"),
        }
    }
}

impl std::error::Error for ExhaustiveMatchError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_is_order_independent_and_score_sensitive() {
        let a = ExhaustiveMatch {
            logical_id: 7,
            score: Some(3),
        };
        let b = ExhaustiveMatch {
            logical_id: 9,
            score: None,
        };
        let mut left = DeliveryChecksum::default();
        left.observe(a);
        left.observe(b);
        let mut right = DeliveryChecksum::default();
        right.observe(b);
        right.observe(a);
        assert_eq!(left, right);

        let mut changed = DeliveryChecksum::default();
        changed.observe(ExhaustiveMatch {
            logical_id: 7,
            score: Some(4),
        });
        changed.observe(b);
        assert_ne!(left, changed);
    }

    #[test]
    fn checksum_domain_separates_absent_and_every_regression_score() {
        let logical_id = 17;
        let mut absent = DeliveryChecksum::default();
        absent.observe(ExhaustiveMatch {
            logical_id,
            score: None,
        });

        // Under the old sentinel/XOR encoding this exact valid score encoded
        // to the `None` sentinel in both checksum inputs.
        let mut former_collision = DeliveryChecksum::default();
        former_collision.observe(ExhaustiveMatch {
            logical_id,
            score: Some(0x48df_8701_1913_8186),
        });
        assert_ne!(absent, former_collision);

        let mut zero = DeliveryChecksum::default();
        zero.observe(ExhaustiveMatch {
            logical_id,
            score: Some(0),
        });
        assert_ne!(absent, zero);
    }
}
