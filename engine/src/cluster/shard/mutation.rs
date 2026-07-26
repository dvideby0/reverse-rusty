use super::{extract_readonly, ClusterMutation, Dict, Normalizer, Shard, ShardError};

/// Apply one logged mutation to a shard through its normal write path — so the op is itself
/// re-logged into that shard's translog (a recovered replica's tail stays consistent) and
/// applied to its engine. Re-derives features from the raw DSL against the frozen `dict`
/// (the ADR-029 DSL-on-wire invariant), so a replayed op is byte-identical to the original
/// live write → the recovered shard converges to the same logical set (zero false negatives).
/// Used by both in-process peer recovery ([`super::replica::peer_recover`]) and the
/// coordinator's gRPC tail-replay.
pub(crate) fn apply_mutation(
    shard: &dyn Shard,
    norm: &Normalizer,
    dict: &Dict,
    m: &ClusterMutation,
    // The target's shard position when the CALLER knows it (the resync repair
    // loop); `None` when coverage holds by construction (a replica catch-up
    // replays its own position's translog). Used to gate an Upsert's insert
    // half to covered positions only — see the Upsert arm.
    position: Option<u32>,
) -> Result<(), ShardError> {
    match m {
        ClusterMutation::Add {
            logical,
            version,
            dsl,
            tags,
            placement,
        } => {
            // The source already acknowledged this logged mutation. Fail loud
            // on structural corruption, but never re-apply today's policy
            // limits and silently skip it.
            let ast = crate::dsl::parse_for_recovery(dsl).map_err(|error| {
                ShardError::Log(format!(
                    "parsing acknowledged shard add during recovery: {error}"
                ))
            })?;
            let mut lc = String::new();
            let ex = extract_readonly(&ast, norm, dict, &mut lc);
            shard.insert_extracted_with_placement(&ex, *logical, *version, dsl, tags, placement)?;
        }
        ClusterMutation::Remove { logical } => {
            shard.delete_by_logical_id(*logical)?;
        }
        ClusterMutation::Upsert {
            logical,
            version,
            dsl,
            tags,
            placement,
        } => {
            let ast = crate::dsl::parse_for_recovery(dsl).map_err(|error| {
                ShardError::Log(format!(
                    "parsing acknowledged shard upsert during recovery: {error}"
                ))
            })?;
            // Replace-by-id ON THIS SHARD: tombstone any prior copy, then insert the new
            // version — but only where the placement actually STORES the row. An upsert's
            // delete half fans to every shard, so a repair can legitimately target a
            // delete-only position; ADR-109 made shard-side inserts validate placement
            // coverage, so re-driving the insert there is refused (`LocalPositionMissing`)
            // and would wedge `resync` on that mutation forever (multi-machine harness
            // catch). Replicated modes cover every position; only Selective restricts.
            shard.delete_by_logical_id(*logical)?;
            let covered = position.is_none_or(|p| {
                placement.mode() != crate::ownership::PlacementMode::Selective
                    || placement.positions().binary_search(&p).is_ok()
            });
            if covered {
                let mut lc = String::new();
                let ex = extract_readonly(&ast, norm, dict, &mut lc);
                shard.insert_extracted_with_placement(
                    &ex, *logical, *version, dsl, tags, placement,
                )?;
            }
        }
    }
    Ok(())
}
