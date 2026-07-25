//! The overlapping (`MatchKind::Standard`) phrase automaton: ADR-061 uses it
//! for alias-enabled flat `P(T)`, while ADR-120 uses it for phrase-aware
//! positive graphs regardless of alias activation. Split out of `core.rs` to
//! keep that file within the size budget.

use crate::dict::{Dict, FeatureId, FeatureKind};
use crate::normalize::PositionArc;
use daachorse::DoubleArrayAhoCorasick;

/// The overlapping phrase automaton + its per-pattern entity features. Built
/// over every registered phrase. Flat ADR-061 callers use it only while an
/// alias is active; positioned ADR-120 callers always use it so an ordinary
/// collapse phrase cannot erase a valid component or overlapping-entity path.
pub(in crate::normalize) struct PhraseOverlap {
    pub(in crate::normalize) automaton: DoubleArrayAhoCorasick<usize>,
    /// pattern index -> (entity feature name, kind).
    pub(in crate::normalize) entries: Vec<(String, FeatureKind)>,
    /// pattern index -> index into the normalizer's `phrase_entries`, so the boundary-aware
    /// phase-1 selection (codex R12) can recover each pattern's
    /// [`PhraseEntry`](crate::normalize::PhraseEntry) (its mode).
    pub(in crate::normalize) entry_idx: Vec<usize>,
    /// Pattern index -> number of cleaned token positions spanned.
    pub(in crate::normalize) token_lens: Vec<u32>,
}

impl PhraseOverlap {
    /// Append the entity feature id of every word-boundary-aligned phrase occurrence in the
    /// already-cleaned text `lc` (overlapping matches included) to `out`. Unknown entities hash to
    /// a stable synthetic id (ADR-046), exactly as the leftmost-longest pass resolves.
    ///
    /// The scan **collapses whitespace runs** so a phrase (registered single-spaced) still matches
    /// a title with repeated spaces or adjacent split punctuation (`new  york`, `new---york`) —
    /// codex R8. This is positive-view (`P(T)`) only and only ever ADDS entities (recall-safe); the
    /// canonical `N(T)` and the compile path keep `lc` verbatim, so persisted segments are NOT
    /// desynced by a whitespace-cleaning change. No allocation unless a run is actually present.
    pub(in crate::normalize) fn collect_into(
        &self,
        lc: &str,
        dict: &Dict,
        out: &mut Vec<FeatureId>,
    ) {
        if lc.as_bytes().windows(2).any(|w| w == b"  ") {
            let mut collapsed = String::with_capacity(lc.len());
            let mut prev_space = true; // suppress a leading space
            for c in lc.chars() {
                if c == ' ' {
                    if !prev_space {
                        collapsed.push(' ');
                    }
                    prev_space = true;
                } else {
                    collapsed.push(c);
                    prev_space = false;
                }
            }
            self.scan_overlapping(&collapsed, dict, out);
        } else {
            self.scan_overlapping(lc, dict, out);
        }
    }

    /// Append every boundary-aligned overlapping phrase as a positioned graph
    /// edge (ADR-120). This is the graph analogue of [`collect_into`](Self::collect_into):
    /// it supplies alternate multi-word paths to the positive quoted-phrase
    /// view while the canonical negative graph remains leftmost-longest.
    pub(in crate::normalize) fn collect_positioned_into(
        &self,
        lc: &str,
        dict: &Dict,
        out: &mut Vec<PositionArc>,
    ) {
        if lc.as_bytes().windows(2).any(|w| w == b"  ") {
            let mut collapsed = String::with_capacity(lc.len());
            let mut prev_space = true;
            for c in lc.chars() {
                if c == ' ' {
                    if !prev_space {
                        collapsed.push(' ');
                    }
                    prev_space = true;
                } else {
                    collapsed.push(c);
                    prev_space = false;
                }
            }
            self.scan_positioned(&collapsed, dict, out);
        } else {
            self.scan_positioned(lc, dict, out);
        }
    }

    fn scan_positioned(&self, text: &str, dict: &Dict, out: &mut Vec<PositionArc>) {
        let bytes = text.as_bytes();
        for m in self.automaton.find_overlapping_iter(text) {
            let (s, e) = (m.start(), m.end());
            let ok_start = s == 0 || bytes[s - 1] == b' ';
            let ok_end = e == text.len() || bytes[e] == b' ';
            if !ok_start || !ok_end {
                continue;
            }
            let start = u32::try_from(text[..s].split_whitespace().count()).unwrap_or(u32::MAX);
            out.push(PositionArc {
                feature: dict.get_or_synthetic(&self.entries[m.value()].0),
                start,
                end: start.saturating_add(self.token_lens[m.value()]),
            });
        }
    }

    /// Emit the entity id of every word-boundary-aligned overlapping phrase match in `text`.
    fn scan_overlapping(&self, text: &str, dict: &Dict, out: &mut Vec<FeatureId>) {
        let bytes = text.as_bytes();
        for m in self.automaton.find_overlapping_iter(text) {
            let (s, e) = (m.start(), m.end());
            let ok_start = s == 0 || bytes[s - 1] == b' ';
            let ok_end = e == text.len() || bytes[e] == b' ';
            if ok_start && ok_end {
                out.push(dict.get_or_synthetic(&self.entries[m.value()].0));
            }
        }
    }

    /// Phase-1 phrase selection with **boundary validity participating in selection** (codex R12):
    /// collect every word-boundary-aligned occurrence from the overlapping automaton, then apply
    /// leftmost-longest over the VALID candidates only. The shared leftmost-longest automaton
    /// commits to a match *before* the boundary check — a boundary-invalid occurrence (a pattern
    /// found mid-token, e.g. `a b` inside `xa b`) is selected, consumes its span, suppresses a
    /// valid overlapping pattern (`b c`), and is then dropped by the post-filter, so the valid
    /// phrase is silently lost — on the query side that compiles an alias query to component
    /// terms, an FN. Selecting over valid candidates only is identical to the legacy pass whenever
    /// no mid-token occurrence exists, and strictly recovers suppressed phrases when one does.
    /// Pushes `(byte_start, byte_end, phrase_entries index)` tuples, non-overlapping, in order.
    pub(in crate::normalize) fn select_phrases(
        &self,
        lc: &str,
        out: &mut Vec<(usize, usize, usize)>,
    ) {
        let bytes = lc.as_bytes();
        for m in self.automaton.find_overlapping_iter(lc) {
            let (s, e) = (m.start(), m.end());
            let ok_start = s == 0 || bytes[s - 1] == b' ';
            let ok_end = e == lc.len() || bytes[e] == b' ';
            if ok_start && ok_end {
                out.push((s, e, self.entry_idx[m.value()]));
            }
        }
        // Leftmost-longest over the boundary-valid candidates: smallest start wins, ties prefer
        // the longest; later candidates overlapping an accepted span are dropped. (Two distinct
        // patterns can never share a span — same span ⇒ same string ⇒ same automaton pattern.)
        out.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
        let mut end = 0usize;
        out.retain(|&(s, e, _)| {
            if s >= end {
                end = e;
                true
            } else {
                false
            }
        });
    }
}
