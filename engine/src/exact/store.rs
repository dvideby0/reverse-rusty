//! The [`ExactStore`] — the struct-of-arrays exact-verification store indexed by
//! SegmentLocalQueryId. Holds the common-mask words, the required/forbidden tails,
//! the any-of groups, the per-query tag column (ADR-049), and identity; plus the
//! scalar [`verify`](ExactStore::verify), the columnar
//! [`eval_batch`](ExactStore::eval_batch) (delegating to
//! [`eval_batch_slices`](super::eval_batch_slices)), the pure-anchor derivation,
//! the compaction copy/re-anchor helpers, and the serialization slice accessors.

use super::{
    encode_predicate, eval_batch_slices, predicate_has_phrases, query_passes_tags,
    verify_predicate, BatchEvalError, TagPredicate, TitleView,
};
use crate::compile::Extracted;
use crate::dict::{Dict, FeatureId, NO_MASK_BIT};
use crate::ownership::{PlacementGeneration, PlacementMode, QueryPlacement, QueryPlacementRef};
use crate::rank::RankValues;
use crate::tagdict::TagId;

#[derive(Clone, Default)]
pub struct ExactStore {
    // common-mask words (the 64 hottest features)
    req_mask: Vec<u64>,
    forb_mask: Vec<u64>,
    // required tail (non-mask features), sorted, sliced from req_blob
    req_off: Vec<u32>,
    req_len: Vec<u16>,
    req_blob: Vec<u32>,
    // forbidden tail
    forb_off: Vec<u32>,
    forb_len: Vec<u16>,
    forb_blob: Vec<u32>,
    // any-of groups: per query a run of groups in the groups table
    q_group_start: Vec<u32>,
    q_group_count: Vec<u16>,
    group_off: Vec<u32>,
    group_len: Vec<u16>,
    anyof_blob: Vec<u32>,
    // Optional compound any-of / forbidden-member program. Ordinary
    // single-token queries store a zero length and never touch the blob.
    predicate_off: Vec<u32>,
    predicate_len: Vec<u32>,
    predicate_blob: Vec<u32>,
    /// O(1) layout bit: any appended row carries a v2 quoted graph. This is
    /// intentionally historical for persistence-format selection; `Segment`
    /// separately counts live phrase rows for snapshot matching mode.
    has_phrase_predicates: bool,
    // per-query metadata tags (ADR-049): sorted TagIds sliced from tag_blob, exactly
    // parallel to the required tail. Verify-stage only — never gates retrieval (§5.3).
    tag_off: Vec<u32>,
    tag_len: Vec<u16>,
    tag_blob: Vec<TagId>,
    // Fixed signed typed rank column (ADR-108), parallel to logical/version.
    priority: Vec<i64>,
    // Distributed emission ownership (ADR-109). The fixed-width columns are
    // parallel to identity; selective positions are sliced from placement_blob.
    placement_generation: Vec<u64>,
    placement_num_shards: Vec<u32>,
    placement_mode: Vec<u8>,
    placement_off: Vec<u32>,
    placement_len: Vec<u32>,
    placement_blob: Vec<u32>,
    // Source/exact coupling (ADR-116 hardening). This internal generation is
    // independent of the caller-visible version and changes on every accepted
    // write, so two writes that both use `_version = 1` cannot be mistaken for
    // the same stored document. Zero means a pre-generation legacy row.
    source_generation: Vec<u64>,
    // identity, resolved only on a confirmed match
    version: Vec<u32>,
    logical: Vec<u64>,
}

impl std::fmt::Debug for ExactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExactStore")
            .field("queries", &self.req_mask.len())
            .field("req_blob_len", &self.req_blob.len())
            .field("forb_blob_len", &self.forb_blob.len())
            .field("anyof_blob_len", &self.anyof_blob.len())
            .field("predicate_blob_len", &self.predicate_blob.len())
            .finish()
    }
}

mod columns;
mod matching;
mod semantics;
mod write;
