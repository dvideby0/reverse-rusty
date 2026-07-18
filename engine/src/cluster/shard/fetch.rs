//! The one winner-fetch credit step shared by the in-process and gRPC
//! delivery paths (ADR-110).
//!
//! Both `LocalShard::fetch_matches` and the streamed `FetchMatches` RPC body
//! drain a winner group against ONE cumulative byte credit; keeping the
//! check order (deadline → bounded lookup → decrement) in a single function
//! is what stops the two loops from drifting on failure classification.

use std::time::Instant;

use crate::segment::EngineSnapshot;

use super::ShardError;

/// Fetch one winner's source under the group's running byte credit.
///
/// `remaining` is decremented on success; `limit` is the group's full credit,
/// reported when the bounded lookup refuses (the caller-facing
/// [`ShardError::EnrichmentLimit`] names the whole credit, not the residue).
pub(crate) fn fetch_source_step(
    snap: &EngineSnapshot,
    logical_id: u64,
    remaining: &mut usize,
    limit: usize,
    deadline: Option<Instant>,
) -> Result<String, ShardError> {
    if deadline.is_some_and(|at| Instant::now() >= at) {
        return Err(ShardError::DeadlineExceeded);
    }
    let source = snap
        .get_query_source_bounded(logical_id, *remaining)
        .map_err(|_| ShardError::EnrichmentLimit { limit })?
        .ok_or(ShardError::SourceUnavailable(logical_id))?;
    *remaining -= source.len();
    Ok(source)
}
