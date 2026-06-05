//! Percolate ranking + pagination (ADR-059): the request-side `rank` block, its
//! lowering to an engine [`RankSpec`], and `order_and_page` — the post-match reorder +
//! `from`/`size` slice shared by `/_search` and `/_mpercolate`. Ranking runs AFTER
//! matching, on the final id set; it only reorders + paginates, never changes which
//! queries match.

use serde::Deserialize;

use reverse_rusty::{CompiledRankSpec, EngineSnapshot, RankSpec};

/// The optional `rank` block (ADR-059) on a percolate request. Maps to
/// [`reverse_rusty::RankSpec`]: order matched queries by a numeric priority tag
/// and/or additive request boosts. Ranking runs AFTER matching, on the final id
/// set — it only reorders + paginates, never changes which queries match.
#[derive(Deserialize)]
pub(super) struct RankBody {
    /// Tag key whose numeric value is a query's base priority (e.g. `"priority"`).
    priority_key: Option<String>,
    /// Additive boosts applied when a query carries the given `(key, value)` tag.
    #[serde(default)]
    boosts: Vec<BoostBody>,
}

#[derive(Deserialize)]
struct BoostBody {
    key: String,
    value: String,
    boost: i64,
}

/// Lower a request `rank` block to an engine [`RankSpec`]. `None` ⇒ no ranking.
pub(super) fn to_rank_spec(rank: Option<RankBody>) -> Option<RankSpec> {
    rank.map(|r| RankSpec {
        priority_key: r.priority_key,
        boosts: r
            .boosts
            .into_iter()
            .map(|b| (b.key, b.value, b.boost))
            .collect(),
    })
}

/// Order + paginate one matched-id list for a hit array (ADR-059). With a ranking
/// spec, score via the snapshot and sort by `(score desc, _id asc)` — a total order,
/// so pagination is byte-stable — then apply `from`/`size`. Without one, keep the
/// engine's (ascending) order and slice: the pre-ranking path, byte-identical.
/// Returns `(id, Option<score>)`; `_score` is `Some` only when ranked.
pub(super) fn order_and_page(
    snap: &EngineSnapshot,
    ids: &[u64],
    rank: Option<&CompiledRankSpec>,
    from: usize,
    size: usize,
) -> Vec<(u64, Option<i64>)> {
    match rank {
        Some(spec) => {
            let mut scored = snap.rank(ids, spec);
            scored.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            scored
                .into_iter()
                .skip(from)
                .take(size)
                .map(|(id, s)| (id, Some(s)))
                .collect()
        }
        None => ids
            .iter()
            .copied()
            .skip(from)
            .take(size)
            .map(|id| (id, None))
            .collect(),
    }
}
