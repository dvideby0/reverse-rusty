//! Engine-side glue for the generated gRPC types (behind `distributed`).
//!
//! Re-exports the generated proto crate so the rest of the cluster module refers to
//! the messages + client + server as `proto::*`, and holds the field-by-field
//! `MatchStats` ⇄ proto map — the ONE place the 11-field wire layout is converted.
//! Keep in sync with `grpc/proto/shard.proto` and [`crate::segment::MatchStats`].

pub(crate) use reverse_rusty_shard_proto::*;

use crate::segment::MatchStats as EngineStats;

/// Proto wire `MatchStats` → engine [`MatchStats`]. Field order pinned to `segment.rs`.
pub(crate) fn stats_to_engine(p: MatchStats) -> EngineStats {
    EngineStats {
        unique_candidates: p.unique_candidates,
        postings_scanned: p.postings_scanned,
        broad_postings_scanned: p.broad_postings_scanned,
        main_candidates: p.main_candidates,
        broad_candidates: p.broad_candidates,
        matches: p.matches,
        probes_attempted: p.probes_attempted,
        probes_skipped: p.probes_skipped,
        broad_queries_evaluated: p.broad_queries_evaluated,
        broad_anchors_scanned: p.broad_anchors_scanned,
        broad_batches: p.broad_batches,
    }
}

/// Engine [`MatchStats`] → proto wire `MatchStats`.
pub(crate) fn stats_from_engine(s: EngineStats) -> MatchStats {
    MatchStats {
        unique_candidates: s.unique_candidates,
        postings_scanned: s.postings_scanned,
        broad_postings_scanned: s.broad_postings_scanned,
        main_candidates: s.main_candidates,
        broad_candidates: s.broad_candidates,
        matches: s.matches,
        probes_attempted: s.probes_attempted,
        probes_skipped: s.probes_skipped,
        broad_queries_evaluated: s.broad_queries_evaluated,
        broad_anchors_scanned: s.broad_anchors_scanned,
        broad_batches: s.broad_batches,
    }
}
