//! Post-match RANKING — an optional, out-of-core layer over the boolean-correct
//! result set (ADR-049 §5.4 / ADR-059). Matching stays boolean and complete;
//! ranking only reorders + paginates the already-final `Vec<u64>` of matched
//! logical ids. It touches NEITHER the candidate index NOR the verifier, so it
//! cannot add or drop a match — the zero-false-negative contract holds trivially.
//!
//! Design: docs/design/matching.md §5.4; docs/DECISIONS.md ADR-049 / ADR-059.
//! Invariant: ranking is opt-in. With no [`RankSpec`] (or a no-op one) the read
//!   path is byte-identical to the pre-ranking engine. Tags never gate here, just
//!   as in [`crate::exact::TagPredicate`] — ranking is presentation, not matching.
//!
//! Score model (additive): `score = Σ(weight for each (key,value) boost the query's
//! tags match) + (numeric value of the query's `priority_key` tag)`. The caller
//! tie-breaks equal scores by ascending `_id` for a total, byte-stable order. An
//! additive single score (vs strict `(boost, priority)` lexicographic) is the
//! simpler ES-`function_score`-"sum"-style realization and fits this workload,
//! where operator-supplied boosts are meant to be commensurate with priority;
//! strict dominance is achievable by choosing boost magnitudes above the priority
//! range — a request-shaping choice, not a code branch.

use crate::tagdict::{TagDict, TagId};
use crate::util::FastMap;

/// Fixed typed rank columns stored beside the exact-verification rows.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RankValues {
    pub priority: i64,
}

/// Raw bounded-ranking program. Increment 2 supports one fixed typed field;
/// boosts retain the existing integer tag-id scoring model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RankProgramSpec {
    pub priority_field: Option<String>,
    pub boosts: Vec<(String, String, i64)>,
}

impl Default for RankProgramSpec {
    fn default() -> Self {
        Self {
            priority_field: Some("priority".to_string()),
            boosts: Vec::new(),
        }
    }
}

/// Integer-only compiled bounded-ranking program.
#[derive(Clone, Debug, Default)]
pub struct CompiledRankProgram {
    use_priority: bool,
    boosts: FastMap<TagId, i64>,
}

impl CompiledRankProgram {
    pub(crate) fn new(use_priority: bool, boosts: FastMap<TagId, i64>) -> Self {
        Self {
            use_priority,
            boosts,
        }
    }

    #[must_use]
    pub fn uses_priority(&self) -> bool {
        self.use_priority
    }

    pub fn boosts(&self) -> impl Iterator<Item = (TagId, i64)> + '_ {
        self.boosts.iter().map(|(&tag, &weight)| (tag, weight))
    }
}

/// Compile-time rejection for rank fields not implemented by this increment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RankProgramError {
    UnsupportedField(String),
}

impl std::fmt::Display for RankProgramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedField(field) => write!(f, "unsupported rank field `{field}`"),
        }
    }
}

impl std::error::Error for RankProgramError {}

/// Bounded-ranking collection telemetry, separate from Boolean match stats.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RankStats {
    pub evaluations: u64,
    pub heap_replacements: u64,
}

/// One winner under the deterministic `(score desc, logical_id asc)` order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RankedHit {
    pub logical_id: u64,
    pub score: i64,
}

/// Complete local bounded-ranked result. Boolean matching statistics remain
/// separate from collector/scoring telemetry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RankedMatch {
    pub hits: Vec<RankedHit>,
    pub total_hits: crate::result::TotalHits,
    pub stats: crate::segment::MatchStats,
    pub rank_stats: RankStats,
}

/// Failures from local bounded ranked matching.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RankedMatchError {
    Admission(crate::result::TopKAdmissionError),
    Cancelled(crate::segment::MatchCancelled),
}

impl std::fmt::Display for RankedMatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(error) => error.fmt(f),
            Self::Cancelled(_) => f.write_str("ranked match deadline exceeded"),
        }
    }
}

impl std::error::Error for RankedMatchError {}

/// Score newest-live typed metadata and tag boosts with saturating addition.
#[must_use]
pub(crate) fn score_program(values: RankValues, tags: &[TagId], spec: &CompiledRankProgram) -> i64 {
    let mut score = if spec.use_priority {
        values.priority
    } else {
        0
    };
    for tag in tags {
        if let Some(weight) = spec.boosts.get(tag) {
            score = score.saturating_add(*weight);
        }
    }
    score
}

/// A ranking request in raw, pre-resolution form (ADR-049 §5.4). Built from the
/// REST `rank` block and compiled against a snapshot's tag space by
/// [`EngineSnapshot::compile_rank_spec`](crate::EngineSnapshot::compile_rank_spec).
#[derive(Clone, Debug, Default)]
pub struct RankSpec {
    /// The tag key whose numeric value is a query's base priority (e.g. `"priority"`).
    /// `None` ⇒ priority contributes nothing to the score.
    pub priority_key: Option<String>,
    /// Additive boosts: a query scores `+weight` for each `(key, value)` tag it carries.
    pub boosts: Vec<(String, String, i64)>,
}

/// A [`RankSpec`] resolved against a tag dictionary: boost `(key,value)`s mapped to
/// interned `TagId`s for integer-only lookup at score time. `priority_key` stays a
/// string because priority *values* are open-ended (resolved per matched tag).
#[derive(Clone, Debug, Default)]
pub struct CompiledRankSpec {
    priority_key: Option<String>,
    boosts: FastMap<TagId, i64>,
}

impl CompiledRankSpec {
    /// Build a compiled spec from already-resolved parts. Prefer
    /// [`EngineSnapshot::compile_rank_spec`](crate::EngineSnapshot::compile_rank_spec),
    /// which resolves boost `(key,value)`s against the live tag dict.
    ///
    /// An EMPTY `priority_key` normalizes to `None`: the gRPC wire encodes the absent
    /// key as `""` (proto3 strings have no presence), so accepting `Some("")` here
    /// would score empty-key tags in-process but never on a remote shard — a silent
    /// local/remote ranking divergence (codex retro-review, ADR-075). Every
    /// construction path (single-node compile, cluster compile, wire decode) funnels
    /// through this constructor, so the two sides agree by construction.
    #[must_use]
    pub fn new(priority_key: Option<String>, boosts: FastMap<TagId, i64>) -> Self {
        Self {
            priority_key: priority_key.filter(|k| !k.is_empty()),
            boosts,
        }
    }

    /// True when ranking would be a no-op (no priority key and no boosts). The
    /// caller treats this exactly like "no ranking requested", so an empty `rank`
    /// block leaves the response byte-identical to the unranked path.
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.priority_key.is_none() && self.boosts.is_empty()
    }

    /// The priority tag key, if one was requested. Read access for the gRPC wire
    /// mapper (ADR-075): a compiled spec crosses the cluster wire as resolved ids.
    #[must_use]
    pub fn priority_key(&self) -> Option<&str> {
        self.priority_key.as_deref()
    }

    /// The resolved `(TagId, weight)` boost pairs (arbitrary order). Read access for
    /// the gRPC wire mapper (ADR-075).
    pub fn boosts(&self) -> impl Iterator<Item = (TagId, i64)> + '_ {
        self.boosts.iter().map(|(&t, &w)| (t, w))
    }
}

/// Score one query's tag set under a compiled spec. Off the match hot path:
/// integer boost lookups, plus — only when a `priority_key` is set — one string
/// compare + parse per tag until the priority tag is found. Pure and total: a
/// non-numeric or absent priority value contributes 0 and never panics (honoring
/// the "no `unwrap()` in library code" invariant). `saturating_add` keeps a
/// pathological boost/priority sum from overflowing.
#[must_use]
pub fn score(tags: &[TagId], tag_dict: &TagDict, spec: &CompiledRankSpec) -> i64 {
    let mut s: i64 = 0;
    if !spec.boosts.is_empty() {
        for t in tags {
            if let Some(w) = spec.boosts.get(t) {
                s = s.saturating_add(*w);
            }
        }
    }
    if let Some(pkey) = spec.priority_key.as_deref() {
        for &t in tags {
            if let Some((k, v)) = tag_dict.key_value(t) {
                if k == pkey {
                    // tags are sorted+deduped per query, so the first match is THE
                    // priority tag; a non-numeric value falls back to 0.
                    s = s.saturating_add(v.parse::<i64>().unwrap_or(0));
                    break;
                }
            }
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::{score, CompiledRankSpec, RankSpec};
    use crate::tagdict::{TagDict, TagId};
    use crate::util::FastMap;

    /// Build a `CompiledRankSpec` from resolved `(TagId, weight)` boosts + a priority key.
    fn compiled(priority_key: Option<&str>, boosts: &[(TagId, i64)]) -> CompiledRankSpec {
        let mut map: FastMap<TagId, i64> = FastMap::default();
        for &(id, w) in boosts {
            map.insert(id, w);
        }
        CompiledRankSpec::new(priority_key.map(str::to_string), map)
    }

    #[test]
    fn empty_spec_is_noop_and_scores_zero() {
        let dict = TagDict::new();
        let spec = CompiledRankSpec::default();
        assert!(spec.is_noop());
        assert_eq!(score(&[1, 2, 3], &dict, &spec), 0);
    }

    #[test]
    fn empty_priority_key_normalizes_to_none() {
        // The gRPC wire encodes the absent priority key as "" (proto3 — no string
        // presence), so `Some("")` must mean "no priority term" EVERYWHERE or
        // in-process and remote shards rank differently (codex retro-review, ADR-075).
        let spec = compiled(Some(""), &[]);
        assert_eq!(spec.priority_key(), None);
        assert!(spec.is_noop(), "an empty key alone requests no ranking");
        // With boosts the spec stays live — only the priority term is absent.
        let mut dict = TagDict::new();
        let gold = dict.intern("tier", "gold");
        let empty_key = dict.intern("", "5");
        let spec = compiled(Some(""), &[(gold, 100)]);
        assert_eq!(spec.priority_key(), None);
        assert!(!spec.is_noop());
        // The empty-key tag contributes NO priority — same as a remote shard, where
        // the wire cannot even express the empty key.
        assert_eq!(score(&[gold, empty_key], &dict, &spec), 100);
    }

    #[test]
    fn boosts_are_additive_and_misses_contribute_zero() {
        let mut dict = TagDict::new();
        let gold = dict.intern("tier", "gold");
        let promo = dict.intern("campaign", "spring");
        let other = dict.intern("color", "red");
        let spec = compiled(None, &[(gold, 100), (promo, 25)]);
        assert!(!spec.is_noop());
        // carries gold + promo + an unboosted tag → 100 + 25, the miss adds nothing.
        assert_eq!(score(&[gold, promo, other], &dict, &spec), 125);
        // carries only the unboosted tag → 0.
        assert_eq!(score(&[other], &dict, &spec), 0);
    }

    #[test]
    fn priority_is_parsed_from_the_tag_value() {
        let mut dict = TagDict::new();
        let p = dict.intern("priority", "500");
        let spec = compiled(Some("priority"), &[]);
        assert_eq!(score(&[p], &dict, &spec), 500);
        // negative priorities are honored (i64).
        let neg = dict.intern("priority", "-3");
        assert_eq!(score(&[neg], &dict, &spec), -3);
    }

    #[test]
    fn non_numeric_or_absent_priority_contributes_zero() {
        let mut dict = TagDict::new();
        let bad = dict.intern("priority", "high"); // not a number
        let unrelated = dict.intern("color", "blue");
        let spec = compiled(Some("priority"), &[]);
        assert_eq!(score(&[bad], &dict, &spec), 0);
        assert_eq!(score(&[unrelated], &dict, &spec), 0);
        assert_eq!(score(&[], &dict, &spec), 0);
    }

    #[test]
    fn priority_and_boost_sum() {
        let mut dict = TagDict::new();
        let p = dict.intern("priority", "10");
        let gold = dict.intern("tier", "gold");
        let spec = compiled(Some("priority"), &[(gold, 100)]);
        assert_eq!(score(&[p, gold], &dict, &spec), 110);
    }

    #[test]
    fn raw_spec_default_is_empty() {
        let raw = RankSpec::default();
        assert!(raw.priority_key.is_none());
        assert!(raw.boosts.is_empty());
    }
}
