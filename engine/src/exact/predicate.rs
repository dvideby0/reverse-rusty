//! Integer-only exact predicates for compound any-of members and quoted token
//! graphs.
//!
//! The common single-token case remains in the original SoA columns. Only a
//! positive OR group containing a multi-feature member, or a multi-feature
//! member of a negated OR group, emits this compact u32 program:
//!
//! ```text
//! version
//! positive_group_count
//!   member_count
//!     requirement_count
//!       alternative_count feature_id...
//! negative_conjunction_count
//!   feature_count feature_id...
//! # version 2 only:
//! required_phrase_count
//!   positions arc_count
//!     start end alternative_count feature_id...
//! forbidden_phrase_count
//!   positions arc_count
//!     start end alternative_count feature_id...
//! ```
//!
//! Positive groups are ANDed with the query; within one group members are ORed,
//! within one member requirements are ANDed, and within one requirement learned
//! equivalents are ORed. Every negative conjunction is forbidden as a whole.

use crate::compile::Extracted;
use crate::dict::FeatureId;
use crate::exact::{PositionGraph, TitleView};
use crate::normalize::PhraseGraph;

const FEATURE_PROGRAM_VERSION: u32 = 1;
const PHRASE_PROGRAM_VERSION: u32 = 2;

/// Append `ex`'s optional compound predicate and return its `(offset, length)`.
pub(crate) fn encode_predicate(ex: &Extracted, blob: &mut Vec<u32>) -> (u32, u32) {
    if ex.anyof_predicates.is_empty()
        && ex.forbidden_conjunctions.is_empty()
        && ex.required_phrases.is_empty()
        && ex.forbidden_phrases.is_empty()
    {
        return (blob.len() as u32, 0);
    }

    let offset = blob.len() as u32;
    let version = if ex.required_phrases.is_empty() && ex.forbidden_phrases.is_empty() {
        FEATURE_PROGRAM_VERSION
    } else {
        PHRASE_PROGRAM_VERSION
    };
    blob.push(version);
    blob.push(ex.anyof_predicates.len() as u32);
    for predicate in &ex.anyof_predicates {
        blob.push(predicate.members.len() as u32);
        for member in &predicate.members {
            blob.push(member.requirements.len() as u32);
            for alternatives in &member.requirements {
                blob.push(alternatives.len() as u32);
                blob.extend_from_slice(alternatives);
            }
        }
    }
    blob.push(ex.forbidden_conjunctions.len() as u32);
    for conjunction in &ex.forbidden_conjunctions {
        blob.push(conjunction.len() as u32);
        blob.extend_from_slice(conjunction);
    }
    if version == PHRASE_PROGRAM_VERSION {
        encode_phrases(&ex.required_phrases, blob);
        encode_phrases(&ex.forbidden_phrases, blob);
    }
    (offset, blob.len() as u32 - offset)
}

fn encode_phrases(phrases: &[PhraseGraph], blob: &mut Vec<u32>) {
    blob.push(phrases.len() as u32);
    for phrase in phrases {
        blob.push(phrase.positions);
        blob.push(phrase.arcs.len() as u32);
        for arc in &phrase.arcs {
            blob.push(arc.start);
            blob.push(arc.end);
            blob.push(arc.alternatives.len() as u32);
            blob.extend_from_slice(&arc.alternatives);
        }
    }
}

#[inline]
pub(crate) fn predicate_has_phrases(words: &[u32]) -> bool {
    words.first().copied() == Some(PHRASE_PROGRAM_VERSION)
}

/// Validate one persisted predicate program before the mmap hot path can see it.
pub(crate) fn validate_predicate(words: &[u32]) -> Result<(), &'static str> {
    if words.is_empty() {
        return Ok(());
    }
    let mut at = 0usize;
    let version = read_word(words, &mut at)?;
    if !matches!(version, FEATURE_PROGRAM_VERSION | PHRASE_PROGRAM_VERSION) {
        return Err("unsupported compound predicate version");
    }
    let positive_count = read_word(words, &mut at)?;
    for _ in 0..positive_count {
        let member_count = read_word(words, &mut at)?;
        if member_count == 0 {
            return Err("compound positive group has no members");
        }
        for _ in 0..member_count {
            let requirement_count = read_word(words, &mut at)?;
            if requirement_count == 0 {
                return Err("compound member has no requirements");
            }
            for _ in 0..requirement_count {
                let alternative_count = read_word(words, &mut at)?;
                if alternative_count == 0 {
                    return Err("compound requirement has no alternatives");
                }
                take_words(words, &mut at, alternative_count)?;
            }
        }
    }
    let negative_count = read_word(words, &mut at)?;
    for _ in 0..negative_count {
        let feature_count = read_word(words, &mut at)?;
        if feature_count < 2 {
            return Err("compound negative member must contain multiple features");
        }
        take_words(words, &mut at, feature_count)?;
    }
    if version == PHRASE_PROGRAM_VERSION {
        let required = validate_phrases(words, &mut at)?;
        let forbidden = validate_phrases(words, &mut at)?;
        if required == 0 && forbidden == 0 {
            return Err("phrase predicate program has no quoted graphs");
        }
    }
    if at != words.len() {
        return Err("compound predicate has trailing words");
    }
    Ok(())
}

fn read_word(words: &[u32], at: &mut usize) -> Result<u32, &'static str> {
    let value = words
        .get(*at)
        .copied()
        .ok_or("compound predicate truncated")?;
    *at += 1;
    Ok(value)
}

fn take_words(words: &[u32], at: &mut usize, count: u32) -> Result<(), &'static str> {
    let end = at
        .checked_add(count as usize)
        .ok_or("compound predicate length overflow")?;
    if end > words.len() {
        return Err("compound predicate feature run truncated");
    }
    *at = end;
    Ok(())
}

fn validate_phrases(words: &[u32], at: &mut usize) -> Result<u32, &'static str> {
    let phrase_count = read_word(words, at)?;
    for _ in 0..phrase_count {
        let positions = read_word(words, at)?;
        let arc_count = read_word(words, at)?;
        if positions == 0 || arc_count == 0 {
            return Err("quoted predicate has an empty token graph");
        }
        let mut previous: Option<(u32, u32)> = None;
        let mut reachable = std::collections::BTreeSet::new();
        reachable.insert(0u32);
        for _ in 0..arc_count {
            let start = read_word(words, at)?;
            let end = read_word(words, at)?;
            let alternative_count = read_word(words, at)?;
            if start >= end || end > positions {
                return Err("quoted predicate arc is outside its token graph");
            }
            if previous.is_some_and(|old| old >= (start, end)) {
                return Err("quoted predicate arcs are not canonical");
            }
            previous = Some((start, end));
            if alternative_count == 0 {
                return Err("quoted predicate arc has no labels");
            }
            let alternatives_start = *at;
            take_words(words, at, alternative_count)?;
            if words[alternatives_start..*at]
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            {
                return Err("quoted predicate labels are not canonical");
            }
            if reachable.contains(&start) {
                reachable.insert(end);
            }
        }
        if !reachable.contains(&positions) {
            return Err("quoted predicate has no complete analyzer path");
        }
    }
    Ok(phrase_count)
}

/// Scalar allocation-free evaluation of a validated compound predicate.
#[inline]
pub(crate) fn verify_predicate(words: &[u32], view: &TitleView<'_>) -> bool {
    if words.is_empty() {
        return true;
    }

    let version = words[0];
    let mut at = 1usize; // version was validated when persisted/built
    let positive_count = words[at] as usize;
    at += 1;
    for _ in 0..positive_count {
        let member_count = words[at] as usize;
        at += 1;
        let mut group_hit = false;
        for _ in 0..member_count {
            let requirement_count = words[at] as usize;
            at += 1;
            let mut member_hit = true;
            for _ in 0..requirement_count {
                let alternative_count = words[at] as usize;
                at += 1;
                let alternatives = &words[at..at + alternative_count];
                at += alternative_count;
                if member_hit
                    && !alternatives
                        .iter()
                        .any(|feature| view.pos.binary_search(feature).is_ok())
                {
                    member_hit = false;
                }
            }
            group_hit |= member_hit;
        }
        if !group_hit {
            return false;
        }
    }

    let negative_count = words[at] as usize;
    at += 1;
    for _ in 0..negative_count {
        let feature_count = words[at] as usize;
        at += 1;
        let features = &words[at..at + feature_count];
        at += feature_count;
        if features
            .iter()
            .all(|feature| view.neg.binary_search(feature).is_ok())
        {
            return false;
        }
    }
    if version == PHRASE_PROGRAM_VERSION {
        let required_count = words[at] as usize;
        at += 1;
        let required_start = at;
        for _ in 0..required_count {
            skip_graph(words, &mut at);
        }
        let forbidden_count = words[at] as usize;
        at += 1;
        let forbidden_start = at;

        // A positioned view is always supplied by the engine while phrase rows
        // are live. If a low-level compatibility caller omits it (or re-enters
        // the scratch cell), fail open: required phrases do not reject and
        // forbidden phrases do not trip. That direction can only over-match,
        // never violate the zero-false-negative contract.
        let (Some(pos_graph), Some(neg_graph), Some(scratch_cell)) =
            (view.pos_graph, view.neg_graph, view.phrase_scratch)
        else {
            return true;
        };
        let Ok(mut scratch) = scratch_cell.try_borrow_mut() else {
            return true;
        };

        at = required_start;
        for _ in 0..required_count {
            let graph = take_graph(words, &mut at);
            if graph_matches(graph, pos_graph, &mut scratch) == Some(false) {
                return false;
            }
        }
        at = forbidden_start;
        for _ in 0..forbidden_count {
            let graph = take_graph(words, &mut at);
            if graph_matches(graph, neg_graph, &mut scratch) == Some(true) {
                return false;
            }
        }
    }
    true
}

fn skip_graph(words: &[u32], at: &mut usize) {
    *at += 1; // positions
    let arc_count = words[*at] as usize;
    *at += 1;
    for _ in 0..arc_count {
        *at += 2; // start, end
        let alternatives = words[*at] as usize;
        *at += 1 + alternatives;
    }
}

fn take_graph<'a>(words: &'a [u32], at: &mut usize) -> &'a [u32] {
    let start = *at;
    skip_graph(words, at);
    &words[start..*at]
}

/// Bounded graph-language intersection. `None` is the explicit complexity
/// fail-open signal; the caller interprets it by predicate polarity.
fn graph_matches(
    query: &[u32],
    title: PositionGraph<'_>,
    scratch: &mut crate::exact::PhraseMatchScratch,
) -> Option<bool> {
    const MAX_STATES: usize = 65_536;

    if !title.complete {
        return None;
    }
    let query_positions = query[0];
    scratch.stack.clear();
    scratch.seen.clear();
    if title.positions as usize > MAX_STATES {
        return None;
    }
    for title_start in 0..title.positions {
        let state = (0, title_start);
        scratch.stack.push(state);
        scratch.seen.insert(state);
    }

    while let Some((query_node, title_node)) = scratch.stack.pop() {
        if query_node == query_positions {
            return Some(true);
        }

        let mut query_at = 2usize;
        let query_arc_count = query[1] as usize;
        for _ in 0..query_arc_count {
            let start = query[query_at];
            let end = query[query_at + 1];
            let alternative_count = query[query_at + 2] as usize;
            let alternatives = &query[query_at + 3..query_at + 3 + alternative_count];
            query_at += 3 + alternative_count;
            if start != query_node {
                continue;
            }
            for title_arc in title
                .arcs
                .iter()
                .skip_while(|arc| arc.start < title_node)
                .take_while(|arc| arc.start == title_node)
            {
                if alternatives.binary_search(&title_arc.feature).is_err() {
                    continue;
                }
                let next = (end, title_arc.end);
                if scratch.seen.contains(&next) {
                    continue;
                }
                if scratch.seen.len() >= MAX_STATES {
                    return None;
                }
                scratch.seen.insert(next);
                scratch.stack.push(next);
            }
        }
    }
    Some(false)
}

/// Columnar bitmap transpose of [`verify_predicate`].
///
/// `acc` already contains the result of the ordinary SoA clauses. `group`,
/// `member`, and `choice` are reusable scratch bitmaps of the same length.
#[allow(clippy::too_many_arguments)]
#[inline]
pub(crate) fn eval_predicate_batch<'a>(
    words: &[u32],
    lookup: &impl Fn(FeatureId) -> Option<&'a [u64]>,
    acc: &mut [u64],
    group: &mut [u64],
    member: &mut [u64],
    choice: &mut [u64],
) {
    if words.is_empty() {
        return;
    }

    let mut at = 1usize;
    let positive_count = words[at] as usize;
    at += 1;
    for _ in 0..positive_count {
        group.fill(0);
        let member_count = words[at] as usize;
        at += 1;
        for _ in 0..member_count {
            member.fill(u64::MAX);
            let requirement_count = words[at] as usize;
            at += 1;
            for _ in 0..requirement_count {
                choice.fill(0);
                let alternative_count = words[at] as usize;
                at += 1;
                for &feature in &words[at..at + alternative_count] {
                    if let Some(bitmap) = lookup(feature) {
                        for (dst, src) in choice.iter_mut().zip(bitmap) {
                            *dst |= *src;
                        }
                    }
                }
                at += alternative_count;
                for (dst, present) in member.iter_mut().zip(choice.iter()) {
                    *dst &= *present;
                }
            }
            for (dst, matched) in group.iter_mut().zip(member.iter()) {
                *dst |= *matched;
            }
        }
        let mut nonzero = 0u64;
        for (dst, matched) in acc.iter_mut().zip(group.iter()) {
            *dst &= *matched;
            nonzero |= *dst;
        }
        if nonzero == 0 {
            return;
        }
    }

    let negative_count = words[at] as usize;
    at += 1;
    for _ in 0..negative_count {
        member.fill(u64::MAX);
        let feature_count = words[at] as usize;
        at += 1;
        let features = &words[at..at + feature_count];
        at += feature_count;
        for &feature in features {
            if let Some(bitmap) = lookup(feature) {
                for (dst, present) in member.iter_mut().zip(bitmap) {
                    *dst &= *present;
                }
            } else {
                member.fill(0);
                break;
            }
        }
        let mut nonzero = 0u64;
        for (dst, forbidden_match) in acc.iter_mut().zip(member.iter()) {
            *dst &= !*forbidden_match;
            nonzero |= *dst;
        }
        if nonzero == 0 {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{AnyOfMember, AnyOfPredicate};
    use crate::normalize::{PhraseArc, PhraseGraph, PositionArc};

    fn extracted() -> Extracted {
        Extracted {
            required: vec![],
            forbidden: vec![],
            anyof: vec![vec![1, 3]],
            anyof_predicates: vec![AnyOfPredicate {
                members: vec![
                    AnyOfMember {
                        requirements: vec![vec![1], vec![2]],
                    },
                    AnyOfMember {
                        requirements: vec![vec![3]],
                    },
                ],
            }],
            forbidden_conjunctions: vec![vec![4, 5]],
            ..Extracted::default()
        }
    }

    #[test]
    fn program_validates_and_preserves_member_boundaries() {
        let mut blob = Vec::new();
        let (off, len) = encode_predicate(&extracted(), &mut blob);
        let words = &blob[off as usize..off as usize + len as usize];
        validate_predicate(words).expect("valid program");

        let matches =
            |pos: &[u32], neg: &[u32]| verify_predicate(words, &TitleView::dual(0, pos, 0, neg));
        assert!(matches(&[1, 2], &[]));
        assert!(matches(&[3], &[]));
        assert!(!matches(&[1], &[]));
        assert!(matches(&[3], &[4]));
        assert!(!matches(&[3], &[4, 5]));
    }

    #[test]
    fn malformed_program_fails_validation() {
        assert!(validate_predicate(&[99, 0, 0]).is_err());
        assert!(validate_predicate(&[FEATURE_PROGRAM_VERSION, 1, 1, 1, 2, 7]).is_err());
        assert!(validate_predicate(&[FEATURE_PROGRAM_VERSION, 0, 1, 1, 7]).is_err());
    }

    #[test]
    fn phrase_program_preserves_required_and_forbidden_adjacency() {
        let ex = Extracted {
            required_phrases: vec![PhraseGraph {
                positions: 2,
                arcs: vec![
                    PhraseArc {
                        start: 0,
                        end: 1,
                        alternatives: vec![1, 10],
                    },
                    PhraseArc {
                        start: 1,
                        end: 2,
                        alternatives: vec![2],
                    },
                ],
            }],
            forbidden_phrases: vec![PhraseGraph {
                positions: 2,
                arcs: vec![
                    PhraseArc {
                        start: 0,
                        end: 1,
                        alternatives: vec![4],
                    },
                    PhraseArc {
                        start: 1,
                        end: 2,
                        alternatives: vec![5],
                    },
                ],
            }],
            ..Extracted::default()
        };
        let mut blob = Vec::new();
        let (off, len) = encode_predicate(&ex, &mut blob);
        let words = &blob[off as usize..off as usize + len as usize];
        validate_predicate(words).expect("valid phrase program");
        assert!(predicate_has_phrases(words));

        let verify = |positions: u32, arcs: &[PositionArc]| {
            let scratch = std::cell::RefCell::new(crate::exact::PhraseMatchScratch::default());
            verify_predicate(
                words,
                &TitleView::dual_positioned(
                    &[],
                    0,
                    &[],
                    positions,
                    arcs,
                    true,
                    0,
                    &[],
                    positions,
                    arcs,
                    &scratch,
                ),
            )
        };
        assert!(verify(
            2,
            &[
                PositionArc {
                    feature: 1,
                    start: 0,
                    end: 1,
                },
                PositionArc {
                    feature: 2,
                    start: 1,
                    end: 2,
                },
            ],
        ));
        assert!(!verify(
            3,
            &[
                PositionArc {
                    feature: 1,
                    start: 0,
                    end: 1,
                },
                PositionArc {
                    feature: 9,
                    start: 1,
                    end: 2,
                },
                PositionArc {
                    feature: 2,
                    start: 2,
                    end: 3,
                },
            ],
        ));
        assert!(!verify(
            4,
            &[
                PositionArc {
                    feature: 10,
                    start: 0,
                    end: 1,
                },
                PositionArc {
                    feature: 2,
                    start: 1,
                    end: 2,
                },
                PositionArc {
                    feature: 4,
                    start: 2,
                    end: 3,
                },
                PositionArc {
                    feature: 5,
                    start: 3,
                    end: 4,
                },
            ],
        ));
    }

    #[test]
    fn malformed_phrase_graphs_fail_validation() {
        assert!(
            validate_predicate(&[PHRASE_PROGRAM_VERSION, 0, 0, 0, 0]).is_err(),
            "program v2 must carry at least one quoted graph"
        );
        assert!(validate_predicate(&[PHRASE_PROGRAM_VERSION, 0, 0, 1, 0, 0, 0]).is_err());
        assert!(
            validate_predicate(&[PHRASE_PROGRAM_VERSION, 0, 0, 1, 2, 1, 0, 1, 1, 7, 0,]).is_err(),
            "a graph without a path to its final position is invalid"
        );
        assert!(
            validate_predicate(&[PHRASE_PROGRAM_VERSION, 0, 0, 1, 1, 1, 0, 1, 2, 9, 7, 0,])
                .is_err(),
            "labels must be strictly canonical"
        );
    }
}
