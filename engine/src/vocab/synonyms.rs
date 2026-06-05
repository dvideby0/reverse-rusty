//! Bulk synonym / alias registration from a Solr/Lucene-format synonym file (ADR-060).
//!
//! Real deployments maintain large alias tables (hundreds of abbreviation→canonical and
//! variant-spelling rules) *outside* of code. The de-facto interchange format for that is the
//! Solr/Lucene synonym file (what Elasticsearch/OpenSearch's `synonyms_path` consumes), so RR
//! parses it directly. The format is two line shapes:
//!
//! ```text
//! # comment lines start with '#'; blank lines are ignored
//!
//! # 1. Equivalent set (comma-separated, NO arrow): every form is interchangeable
//! auto, autograph, autographed, signature, signed
//! rc, rookie, rookie card
//!
//! # 2. Mapping (with '=>'): left and right forms are declared interchangeable
//! ud => upper deck
//! ```
//!
//! **Recall-first semantics (the load-bearing design choice).** Every parsed rule is applied
//! through RR's **equivalence expansion** mechanism ([`Vocab::add_equivalence`], ADR-054): a
//! query requiring one form is widened to an any-of over the whole group, so it matches a title
//! bearing *any* form. Expansion is structurally false-negative-safe — a wrong alias can only add
//! a (cheap) false-positive candidate, never drop a real match. RR is therefore expansion-based
//! and the `=>` arrow is accepted only for Solr-file compatibility: both sides are unioned into one
//! equivalence group (direction is immaterial to recall). This deliberately does **not** implement
//! Solr's directional token-collapse, which interacts badly with forbidden terms (a collapsed
//! `c -a` becomes the contradiction `term:c -term:c`) — see ADR-060.
//!
//! **Multi-token forms** (`upper deck`, `i-pod`) cannot be a single feature on their own, so each
//! is registered as a gluing **phrase** (`["upper","deck"] -> "term:upper_deck"`) and the glued
//! feature joins the equivalence group. (A multi-token form therefore tightens to adjacency, the
//! documented ADR-053 residual — but it is the operator's explicit choice to declare it as a unit,
//! and the same normalizer runs over titles, so the lossless cover still holds.)
//!
//! Admin/build-time only — never on the match hot path.

use std::path::Path;

use super::Vocab;
use crate::dict::FeatureKind;

/// Outcome of loading a synonym file: how many equivalence groups and gluing phrases the parse
/// produced (before merge dedup). Returned so a caller (e.g. the REST endpoint) can report what
/// it absorbed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SynonymLoadStats {
    /// Equivalence groups parsed (one per non-comment line).
    pub groups: usize,
    /// Gluing phrases registered for multi-token forms.
    pub phrases: usize,
}

/// A synonym-file parse failure, with the 1-based line number so the operator can fix the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynonymParseError {
    /// 1-based line number of the offending rule.
    pub line: usize,
    /// Human-readable description of the problem.
    pub message: String,
}

impl std::fmt::Display for SynonymParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "synonym file line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for SynonymParseError {}

/// Either I/O (reading the file) or a parse failure — the error of the file-loading variants.
#[derive(Debug)]
pub enum SynonymLoadError {
    Io(std::io::Error),
    Parse(SynonymParseError),
}

impl std::fmt::Display for SynonymLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SynonymLoadError::Io(e) => write!(f, "reading synonym file: {e}"),
            SynonymLoadError::Parse(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SynonymLoadError {}

impl From<std::io::Error> for SynonymLoadError {
    fn from(e: std::io::Error) -> Self {
        SynonymLoadError::Io(e)
    }
}

impl From<SynonymParseError> for SynonymLoadError {
    fn from(e: SynonymParseError) -> Self {
        SynonymLoadError::Parse(e)
    }
}

/// Split a form into normalized tokens the way the default normalizer tokenizes a title: fold
/// diacritics, lowercase, and break on any run of non-alphanumerics (so `i-pod` -> `["i","pod"]`,
/// `Upper Deck` -> `["upper","deck"]`). A form yielding `[]` (all punctuation) cannot be a feature.
fn tokenize_form(form: &str) -> Vec<String> {
    let mut toks = Vec::new();
    let mut cur = String::new();
    for ch in form.chars() {
        let c = crate::normalize::fold_diacritic(ch);
        if c.is_ascii_alphanumeric() {
            cur.push(c.to_ascii_lowercase());
        } else if !cur.is_empty() {
            toks.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        toks.push(cur);
    }
    toks
}

/// Split a comma-separated form list, trimming each and dropping empties.
fn split_forms(s: &str) -> Vec<&str> {
    s.split(',')
        .map(str::trim)
        .filter(|f| !f.is_empty())
        .collect()
}

/// Parse a Solr/Lucene-format synonym file into a fresh [`Vocab`] of equivalence groups (+ gluing
/// phrases for any multi-token forms). See the [module docs](self) for the format and semantics.
/// Fails fast with the 1-based line number on the first malformed rule (a rule needs at least two
/// distinct forms).
pub fn parse_synonyms(text: &str) -> Result<Vocab, SynonymParseError> {
    let mut vocab = Vocab::new();
    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Union both sides of a `=>` mapping into one equivalence group (RR is expansion-based,
        // so the arrow's direction is immaterial — see the module docs).
        let raw_forms: Vec<&str> = if let Some((lhs, rhs)) = line.split_once("=>") {
            let mut forms = split_forms(lhs);
            forms.extend(split_forms(rhs));
            forms
        } else {
            split_forms(line)
        };

        // Resolve each form to a single feature: a single-token form is used directly; a
        // multi-token form is glued by a collapse phrase so it becomes one feature.
        let mut group: Vec<String> = Vec::with_capacity(raw_forms.len());
        for form in raw_forms {
            let toks = tokenize_form(form);
            let member = match toks.len() {
                0 => continue, // punctuation-only form — cannot be a feature
                1 => toks[0].clone(),
                _ => {
                    let canonical = format!("term:{}", toks.join("_"));
                    let refs: Vec<&str> = toks.iter().map(String::as_str).collect();
                    vocab.add_phrase(&refs, &canonical, FeatureKind::Generic);
                    toks.join(" ") // resolved through the phrase to `canonical` at apply time
                }
            };
            if !group.contains(&member) {
                group.push(member);
            }
        }

        if group.len() < 2 {
            return Err(SynonymParseError {
                line: line_no,
                message: format!(
                    "a synonym rule needs at least two distinct forms (got {})",
                    group.len()
                ),
            });
        }
        let refs: Vec<&str> = group.iter().map(String::as_str).collect();
        vocab.add_equivalence(&refs);
    }
    Ok(vocab)
}

impl Vocab {
    /// Register several equivalence groups at once (bulk [`add_equivalence`](Self::add_equivalence)).
    /// Each group is a set of surface forms treated as the same entity, applied via FN-safe
    /// expansion (ADR-054). Groups with fewer than two forms are skipped.
    pub fn add_equivalences(&mut self, groups: &[Vec<&str>]) {
        for g in groups {
            self.add_equivalence(g);
        }
    }

    /// Register several single-token synonyms at once (bulk [`add_synonym`](Self::add_synonym)).
    pub fn add_synonyms(&mut self, entries: &[(&str, &str, FeatureKind)]) {
        for (token, canonical, kind) in entries {
            self.add_synonym(token, canonical, *kind);
        }
    }

    /// Parse a Solr/Lucene-format synonym table (see the [module docs](self)) and merge it into
    /// this vocab. Returns the parse stats; the table's rules become FN-safe equivalence groups
    /// (ADR-060). Errors (with a line number) without mutating `self` if the table is malformed.
    pub fn extend_from_synonyms(
        &mut self,
        text: &str,
    ) -> Result<SynonymLoadStats, SynonymParseError> {
        let parsed = parse_synonyms(text)?;
        let stats = SynonymLoadStats {
            groups: parsed.equivalences().len(),
            phrases: parsed.phrases().len(),
        };
        self.merge(&parsed);
        Ok(stats)
    }

    /// Read a Solr/Lucene-format synonym file from `path` and merge it into this vocab
    /// ([`extend_from_synonyms`](Self::extend_from_synonyms) over the file's contents).
    pub fn extend_from_synonyms_file(
        &mut self,
        path: &Path,
    ) -> Result<SynonymLoadStats, SynonymLoadError> {
        let text = std::fs::read_to_string(path)?;
        Ok(self.extend_from_synonyms(&text)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_equivalence_lines_skipping_comments_and_blanks() {
        let text = "\
# domain aliases
auto, autograph, autographed, signature, signed

rc, rookie
";
        let v = parse_synonyms(text).expect("parse");
        let groups = v.equivalences();
        assert_eq!(groups.len(), 2, "two equivalence groups");
        assert!(groups.iter().any(|g| g.contains(&"auto".to_string())
            && g.contains(&"signature".to_string())
            && g.len() == 5));
        assert!(groups
            .iter()
            .any(|g| g == &vec!["rc".to_string(), "rookie".to_string()]));
        assert!(
            v.phrases().is_empty(),
            "all forms single-token => no phrases"
        );
    }

    #[test]
    fn arrow_mapping_unions_both_sides_into_one_group() {
        let v = parse_synonyms("ud, upperdeck => upper deck").expect("parse");
        let groups = v.equivalences();
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert!(g.contains(&"ud".to_string()));
        assert!(g.contains(&"upperdeck".to_string()));
        assert!(g.contains(&"upper deck".to_string()));
        // The multi-token form `upper deck` is glued by a phrase.
        assert_eq!(v.phrases().len(), 1);
        let p = &v.phrases()[0];
        assert_eq!(p.tokens, vec!["upper".to_string(), "deck".to_string()]);
        assert_eq!(p.canonical, "term:upper_deck");
        assert!(!p.additive, "gluing phrase must collapse (one feature)");
    }

    #[test]
    fn multi_token_form_via_hyphen_is_glued() {
        // `i-pod` splits to two tokens by the default normalizer, so it must be glued like a
        // space-separated phrase for the equivalence to resolve to one feature.
        let v = parse_synonyms("ipod, i-pod").expect("parse");
        assert_eq!(v.phrases().len(), 1);
        assert_eq!(
            v.phrases()[0].tokens,
            vec!["i".to_string(), "pod".to_string()]
        );
        let g = &v.equivalences()[0];
        assert!(g.contains(&"ipod".to_string()));
        assert!(g.contains(&"i pod".to_string())); // normalized, space-joined
    }

    #[test]
    fn duplicate_forms_within_a_line_are_deduped() {
        let v = parse_synonyms("rc, rc, rookie").expect("parse");
        assert_eq!(v.equivalences()[0].len(), 2, "rc deduped");
    }

    #[test]
    fn single_form_line_is_an_error_with_line_number() {
        let err = parse_synonyms("auto, autograph\nlonely").expect_err("must fail");
        assert_eq!(err.line, 2);
        assert!(err.message.contains("at least two"), "{}", err.message);
    }

    #[test]
    fn punctuation_only_form_is_dropped_and_can_trip_the_min_forms_check() {
        let err = parse_synonyms("rc, !!!").expect_err("only one usable form");
        assert_eq!(err.line, 1);
    }

    #[test]
    fn extend_from_synonyms_merges_and_reports_stats() {
        let mut v = Vocab::new();
        v.add_equivalence(&["existing", "preexisting"]);
        let stats = v
            .extend_from_synonyms("ud, upper deck\nrc, rookie")
            .expect("extend");
        assert_eq!(stats.groups, 2);
        assert_eq!(stats.phrases, 1, "`upper deck` glued");
        // Merged on top of the pre-existing group.
        assert_eq!(v.equivalences().len(), 3);
    }

    #[test]
    fn bulk_add_equivalences_and_synonyms() {
        let mut v = Vocab::new();
        v.add_equivalences(&[vec!["a", "b"], vec!["c", "d", "e"]]);
        assert_eq!(v.equivalences().len(), 2);
        v.add_synonyms(&[
            ("ud", "brand:upper_deck", FeatureKind::Brand),
            ("rc", "term:rookie", FeatureKind::Category),
        ]);
        assert_eq!(v.synonyms().len(), 2);
    }

    #[test]
    fn empty_or_all_comment_input_yields_empty_vocab() {
        let v = parse_synonyms("# just a comment\n\n   \n").expect("parse");
        assert!(v.equivalences().is_empty());
        assert!(v.phrases().is_empty());
    }
}
