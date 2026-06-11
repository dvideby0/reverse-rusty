//! Engine-side glue for the generated gRPC types (behind `distributed`).
//!
//! Re-exports the generated proto crate so the rest of the cluster module refers to
//! the messages + client + server as `proto::*`, and holds the field-by-field
//! `MatchStats` ⇄ proto map — the ONE place the 11-field wire layout is converted.
//! Keep in sync with `grpc/proto/shard.proto` and [`crate::segment::MatchStats`].

pub(crate) use reverse_rusty_shard_proto::*;

use super::clog::{ClusterMutation, LogPos};
use crate::exact::TagPredicate;
use crate::segment::MatchStats as EngineStats;
use crate::tagdict::TagId;

/// Raw `(key, value)` tags → proto `TagKv`s (ADR-055): the tags-on-wire form, re-resolved
/// read-only on the server. Empty ⇒ empty (untagged, byte-identical wire).
pub(crate) fn tags_to_proto(tags: &[(String, String)]) -> Vec<TagKv> {
    tags.iter()
        .map(|(k, v)| TagKv {
            key: k.clone(),
            value: v.clone(),
        })
        .collect()
}

/// Proto `TagKv`s → raw `(key, value)` tags.
pub(crate) fn tags_from_proto(tags: Vec<TagKv>) -> Vec<(String, String)> {
    tags.into_iter().map(|t| (t.key, t.value)).collect()
}

/// Resolved [`TagPredicate`] → proto `TagGroup`s (ADR-055): the already-resolved `TagId` groups.
/// They are globally consistent (frozen tag dict + synthetic hash), so the server rebuilds the
/// predicate from the raw ids without re-resolving strings. Empty ⇒ unfiltered.
pub(crate) fn tag_predicate_to_proto(pred: &TagPredicate) -> Vec<TagGroup> {
    pred.groups()
        .iter()
        .map(|g| TagGroup { ids: g.clone() })
        .collect()
}

/// Proto `TagGroup`s → a [`TagPredicate`] (`TagPredicate::new` re-sorts/dedups each group, so a
/// malformed/unsorted wire group is still a correct conjunction). Empty ⇒ the empty predicate.
pub(crate) fn tag_predicate_from_proto(groups: Vec<TagGroup>) -> TagPredicate {
    let groups: Vec<Vec<TagId>> = groups.into_iter().map(|g| g.ids).collect();
    TagPredicate::new(groups)
}

/// Compiled engine rank spec → the proto `RankSpec` (ADR-075): already-resolved `TagId`
/// boosts + the priority key, mirroring how the tag filter ships resolved ids — the
/// shard never re-resolves strings. The wire's empty `priority_key` encodes `None`.
pub(crate) fn rank_spec_to_proto(spec: &crate::rank::CompiledRankSpec) -> RankSpec {
    RankSpec {
        priority_key: spec.priority_key().unwrap_or_default().to_string(),
        boosts: spec
            .boosts()
            .map(|(tag_id, weight)| RankBoost { tag_id, weight })
            .collect(),
    }
}

/// Proto `RankSpec` → the compiled engine spec (ADR-075). An empty wire
/// `priority_key` decodes to `None` (no priority term).
pub(crate) fn rank_spec_from_proto(p: RankSpec) -> crate::rank::CompiledRankSpec {
    let boosts = p.boosts.into_iter().map(|b| (b.tag_id, b.weight)).collect();
    let priority_key = if p.priority_key.is_empty() {
        None
    } else {
        Some(p.priority_key)
    };
    crate::rank::CompiledRankSpec::new(priority_key, boosts)
}

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

/// Proto `TranslogEntry` → engine `(LogPos, ClusterMutation)` (ADR-039). `None` if the oneof
/// is unset (a malformed frame). The Add arm reuses `AddItem {logical_id, dsl, version}`, so
/// the wire stays DSL-bearing/dict-agnostic — the receiver re-compiles read-only on replay.
pub(crate) fn translog_entry_to_mutation(e: TranslogEntry) -> Option<(LogPos, ClusterMutation)> {
    let m = match e.op? {
        translog_entry::Op::Add(item) => ClusterMutation::Add {
            logical: item.logical_id,
            version: item.version.max(1),
            dsl: item.dsl,
            // Tags ride the translog entry (ADR-055), so a peer-recovered replica keeps them.
            tags: tags_from_proto(item.tags),
        },
        translog_entry::Op::RemoveLogical(logical) => ClusterMutation::Remove { logical },
    };
    Some((LogPos(e.seqno), m))
}

/// Engine `(LogPos, &ClusterMutation)` → proto `TranslogEntry` — the source side of
/// `FetchTranslog` (ADR-039). `None` for a frame the wire cannot represent: a
/// per-shard translog never holds a whole `Upsert` (the coordinator decomposes a
/// cluster upsert into per-shard delete + insert seam calls, each re-logged as its own
/// Remove/Add record — ADR-070), so shipping one would mean silently dropping half its
/// semantics; the caller fails the recovery stream loud instead.
pub(crate) fn translog_entry_from_mutation(
    pos: LogPos,
    m: &ClusterMutation,
) -> Option<TranslogEntry> {
    let op = match m {
        ClusterMutation::Add {
            logical,
            version,
            dsl,
            tags,
        } => translog_entry::Op::Add(AddItem {
            logical_id: *logical,
            dsl: dsl.clone(),
            version: *version,
            tags: tags_to_proto(tags),
        }),
        ClusterMutation::Remove { logical } => translog_entry::Op::RemoveLogical(*logical),
        ClusterMutation::Upsert { .. } => return None,
    };
    Some(TranslogEntry {
        seqno: pos.0,
        op: Some(op),
    })
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
