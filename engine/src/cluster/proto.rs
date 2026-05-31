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

#[cfg(test)]
mod tests {
    use super::{stats_from_engine, stats_to_engine, EngineStats, MatchStats};

    // 11 DISTINCT values, so any field swap in either mapper changes the result — a pure
    // round-trip alone would miss a *symmetric* transposition present in both directions,
    // which the per-field, by-name assertions below catch.
    const VALS: [u32; 11] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

    fn engine_sample() -> EngineStats {
        EngineStats {
            unique_candidates: VALS[0],
            postings_scanned: VALS[1],
            broad_postings_scanned: VALS[2],
            main_candidates: VALS[3],
            broad_candidates: VALS[4],
            matches: VALS[5],
            probes_attempted: VALS[6],
            probes_skipped: VALS[7],
            broad_queries_evaluated: VALS[8],
            broad_anchors_scanned: VALS[9],
            broad_batches: VALS[10],
        }
    }

    #[test]
    fn engine_to_proto_maps_each_field_by_name() {
        let p = stats_from_engine(engine_sample());
        assert_eq!(p.unique_candidates, VALS[0]);
        assert_eq!(p.postings_scanned, VALS[1]);
        assert_eq!(p.broad_postings_scanned, VALS[2]);
        assert_eq!(p.main_candidates, VALS[3]);
        assert_eq!(p.broad_candidates, VALS[4]);
        assert_eq!(p.matches, VALS[5]);
        assert_eq!(p.probes_attempted, VALS[6]);
        assert_eq!(p.probes_skipped, VALS[7]);
        assert_eq!(p.broad_queries_evaluated, VALS[8]);
        assert_eq!(p.broad_anchors_scanned, VALS[9]);
        assert_eq!(p.broad_batches, VALS[10]);
    }

    #[test]
    fn proto_to_engine_maps_each_field_by_name() {
        let p = MatchStats {
            unique_candidates: VALS[0],
            postings_scanned: VALS[1],
            broad_postings_scanned: VALS[2],
            main_candidates: VALS[3],
            broad_candidates: VALS[4],
            matches: VALS[5],
            probes_attempted: VALS[6],
            probes_skipped: VALS[7],
            broad_queries_evaluated: VALS[8],
            broad_anchors_scanned: VALS[9],
            broad_batches: VALS[10],
        };
        let e = stats_to_engine(p);
        assert_eq!(e.unique_candidates, VALS[0]);
        assert_eq!(e.postings_scanned, VALS[1]);
        assert_eq!(e.broad_postings_scanned, VALS[2]);
        assert_eq!(e.main_candidates, VALS[3]);
        assert_eq!(e.broad_candidates, VALS[4]);
        assert_eq!(e.matches, VALS[5]);
        assert_eq!(e.probes_attempted, VALS[6]);
        assert_eq!(e.probes_skipped, VALS[7]);
        assert_eq!(e.broad_queries_evaluated, VALS[8]);
        assert_eq!(e.broad_anchors_scanned, VALS[9]);
        assert_eq!(e.broad_batches, VALS[10]);
    }

    #[test]
    fn round_trip_is_identity() {
        let e = engine_sample();
        assert_eq!(stats_to_engine(stats_from_engine(e)), e);
    }
}
