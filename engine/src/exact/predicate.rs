//! Integer-only exact predicates for compound any-of members.
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
//! ```
//!
//! Positive groups are ANDed with the query; within one group members are ORed,
//! within one member requirements are ANDed, and within one requirement learned
//! equivalents are ORed. Every negative conjunction is forbidden as a whole.

use crate::compile::Extracted;
use crate::dict::FeatureId;

const PROGRAM_VERSION: u32 = 1;

/// Append `ex`'s optional compound predicate and return its `(offset, length)`.
pub(crate) fn encode_predicate(ex: &Extracted, blob: &mut Vec<u32>) -> (u32, u32) {
    if ex.anyof_predicates.is_empty() && ex.forbidden_conjunctions.is_empty() {
        return (blob.len() as u32, 0);
    }

    let offset = blob.len() as u32;
    blob.push(PROGRAM_VERSION);
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
    (offset, blob.len() as u32 - offset)
}

/// Validate one persisted predicate program before the mmap hot path can see it.
pub(crate) fn validate_predicate(words: &[u32]) -> Result<(), &'static str> {
    if words.is_empty() {
        return Ok(());
    }
    let mut at = 0usize;
    let read = |at: &mut usize| -> Result<u32, &'static str> {
        let value = words
            .get(*at)
            .copied()
            .ok_or("compound predicate truncated")?;
        *at += 1;
        Ok(value)
    };
    let take = |at: &mut usize, count: u32| -> Result<(), &'static str> {
        let end = at
            .checked_add(count as usize)
            .ok_or("compound predicate length overflow")?;
        if end > words.len() {
            return Err("compound predicate feature run truncated");
        }
        *at = end;
        Ok(())
    };

    if read(&mut at)? != PROGRAM_VERSION {
        return Err("unsupported compound predicate version");
    }
    let positive_count = read(&mut at)?;
    for _ in 0..positive_count {
        let member_count = read(&mut at)?;
        if member_count == 0 {
            return Err("compound positive group has no members");
        }
        for _ in 0..member_count {
            let requirement_count = read(&mut at)?;
            if requirement_count == 0 {
                return Err("compound member has no requirements");
            }
            for _ in 0..requirement_count {
                let alternative_count = read(&mut at)?;
                if alternative_count == 0 {
                    return Err("compound requirement has no alternatives");
                }
                take(&mut at, alternative_count)?;
            }
        }
    }
    let negative_count = read(&mut at)?;
    for _ in 0..negative_count {
        let feature_count = read(&mut at)?;
        if feature_count < 2 {
            return Err("compound negative member must contain multiple features");
        }
        take(&mut at, feature_count)?;
    }
    if at != words.len() {
        return Err("compound predicate has trailing words");
    }
    Ok(())
}

/// Scalar allocation-free evaluation of a validated compound predicate.
#[inline]
pub(crate) fn verify_predicate(
    words: &[u32],
    pos_features: &[FeatureId],
    neg_features: &[FeatureId],
) -> bool {
    if words.is_empty() {
        return true;
    }

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
                        .any(|feature| pos_features.binary_search(feature).is_ok())
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
            .all(|feature| neg_features.binary_search(feature).is_ok())
        {
            return false;
        }
    }
    true
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
            match lookup(feature) {
                Some(bitmap) => {
                    for (dst, present) in member.iter_mut().zip(bitmap) {
                        *dst &= *present;
                    }
                }
                None => {
                    member.fill(0);
                    break;
                }
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
        }
    }

    #[test]
    fn program_validates_and_preserves_member_boundaries() {
        let mut blob = Vec::new();
        let (off, len) = encode_predicate(&extracted(), &mut blob);
        let words = &blob[off as usize..off as usize + len as usize];
        validate_predicate(words).expect("valid program");

        assert!(verify_predicate(words, &[1, 2], &[]));
        assert!(verify_predicate(words, &[3], &[]));
        assert!(!verify_predicate(words, &[1], &[]));
        assert!(verify_predicate(words, &[3], &[4]));
        assert!(!verify_predicate(words, &[3], &[4, 5]));
    }

    #[test]
    fn malformed_program_fails_validation() {
        assert!(validate_predicate(&[99, 0, 0]).is_err());
        assert!(validate_predicate(&[PROGRAM_VERSION, 1, 1, 1, 2, 7]).is_err());
        assert!(validate_predicate(&[PROGRAM_VERSION, 0, 1, 1, 7]).is_err());
    }
}
