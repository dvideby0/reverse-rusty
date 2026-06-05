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
//! is registered as an **additive** gluing phrase to a single-token canonical
//! (`["upper","deck"] -> "term:upperdeck"`, ADR-053) and that canonical joins the equivalence group.
//! *Additive* (not collapse) is load-bearing: a title bearing the form still emits its component
//! features, so a pre-existing component-token query (e.g. `deck`) never loses a match — loading a
//! synonym table only ever *grows* recall. The single-token canonical (the tokens joined, so it
//! survives the normalizer's re-tokenization as ONE feature) is what lets the equivalence resolve.
//! (Forms whose glued canonical coincides with a sibling form — `ipod, i-pod` — are already
//! equivalent via the phrase, so the rule adds no equivalence and is a no-op, not an error.)
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

/// Split a form into normalized tokens the way the **default** normalizer tokenizes a title
/// ([`Normalizer::clean_into`](crate::normalize::Normalizer)): fold diacritics, lowercase, **keep
/// `.` inside the token** (`PunctClass::Keep` — so `st.` and `9.5` stay one token), and break on
/// every other non-alphanumeric (so `i-pod` -> `["i","pod"]`, `Upper Deck` -> `["upper","deck"]`).
/// A token with no alphanumeric (e.g. all dots) is dropped — it can't be a feature. (A *custom*
/// punctuation config — folded `-`, or `#`/`/` markers — can diverge; the common default is
/// mirrored, and divergence only makes an alias a no-op, never a false negative.)
fn tokenize_form(form: &str) -> Vec<String> {
    let mut toks: Vec<String> = Vec::new();
    let mut cur = String::new();
    for ch in form.chars() {
        let c = crate::normalize::fold_diacritic(ch);
        if c.is_ascii_alphanumeric() || c == '.' {
            cur.push(c.to_ascii_lowercase());
        } else if !cur.is_empty() {
            toks.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        toks.push(cur);
    }
    toks.retain(|t| t.bytes().any(|b| b.is_ascii_alphanumeric()));
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

        // Resolve each form to a single entity feature. A single-token form is its own feature; a
        // multi-token form is glued by an **alias-entity** phrase (ADR-061, the ES `synonym_graph`
        // equivalent): additive on the title side (the entity feature AND its components, so a
        // component query — e.g. `deck` — still matches), collapsed on the query side (only the
        // entity feature, so a query phrased with the multi-word form requires just the entity,
        // which the equivalence then widens to its synonyms — bidirectional). The member is the
        // RAW multi-word form: `resolve_equivalences` runs the query/compile path, so the alias
        // phrase collapses it to the single entity feature (and, if an existing phrase already
        // covers those tokens, that phrase's canonical is used instead — no broken link).
        let mut group: Vec<String> = Vec::with_capacity(raw_forms.len());
        let mut usable_forms = 0usize;
        for form in raw_forms {
            let toks = tokenize_form(form);
            if toks.is_empty() {
                continue; // punctuation-only form — cannot be a feature
            }
            usable_forms += 1;
            let member = if toks.len() == 1 {
                toks.join("") // the lone token (lowercased, `.` kept) — matches real text 1:1
            } else {
                let canonical = format!("term:{}", toks.join(""));
                let refs: Vec<&str> = toks.iter().map(String::as_str).collect();
                vocab.add_phrase_alias(&refs, &canonical, FeatureKind::Generic);
                toks.join(" ") // raw form: collapses through the alias phrase to one entity feature
            };
            if !group.contains(&member) {
                group.push(member);
            }
        }

        // A genuinely under-specified rule (fewer than two usable forms) is an error; a rule whose
        // forms collapse to the SAME feature (e.g. `ipod, i-pod` — already made equivalent by the
        // gluing phrase) is redundant, not malformed, so it adds no equivalence and is not an error.
        if usable_forms < 2 {
            return Err(SynonymParseError {
                line: line_no,
                message: format!("a synonym rule needs at least two forms (got {usable_forms})"),
            });
        }
        if group.len() >= 2 {
            let refs: Vec<&str> = group.iter().map(String::as_str).collect();
            vocab.add_equivalence(&refs);
        }
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
        let v = parse_synonyms("ud => upper deck").expect("parse");
        let groups = v.equivalences();
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        // The member is the RAW multi-word form; it collapses through the alias phrase at resolve.
        assert!(g.contains(&"ud".to_string()));
        assert!(g.contains(&"upper deck".to_string()));
        // `upper deck` is glued by an ALIAS-ENTITY phrase (ADR-061): collapse-on-query,
        // additive-on-title.
        assert_eq!(v.phrases().len(), 1);
        let p = &v.phrases()[0];
        assert_eq!(p.tokens, vec!["upper".to_string(), "deck".to_string()]);
        assert_eq!(p.canonical, "term:upperdeck");
        assert!(p.alias, "multi-word gluing phrase must be an alias entity");
        assert!(!p.additive, "alias takes precedence; additive stays false");
    }

    #[test]
    fn hyphenated_multi_token_form_is_glued_as_alias_entity() {
        // `new-york` splits to two tokens by the default normalizer, so it is glued by an alias
        // entity phrase to `term:newyork`, joining the group with `nyc` (raw member `new york`).
        let v = parse_synonyms("nyc, new-york").expect("parse");
        assert_eq!(v.phrases().len(), 1);
        let p = &v.phrases()[0];
        assert_eq!(p.tokens, vec!["new".to_string(), "york".to_string()]);
        assert_eq!(p.canonical, "term:newyork");
        assert!(p.alias);
        let g = &v.equivalences()[0];
        assert!(g.contains(&"nyc".to_string()) && g.contains(&"new york".to_string()));
    }

    #[test]
    fn glued_form_equal_to_a_sibling_still_parses() {
        // `ipod` and `i-pod` both resolve to `term:ipod` (the latter via its alias phrase), so the
        // stored equivalence is harmless (it dedups to one feature at resolve time). The rule is
        // valid, not an error; the alias phrase is registered.
        let v = parse_synonyms("ipod, i-pod").expect("parse");
        assert_eq!(v.phrases().len(), 1, "the gluing phrase is registered");
        assert_eq!(v.phrases()[0].canonical, "term:ipod");
        assert!(v.phrases()[0].alias);
    }

    #[test]
    fn dotted_form_keeps_the_dot_to_match_the_normalizer() {
        // The default normalizer keeps `.` inside a token (`st.` -> `st.`), so the equivalence
        // member must too — otherwise the alias would resolve to `st` and never fire on real
        // `st.` text. (P2 from the ADR-060 Codex review.)
        let v = parse_synonyms("st., saint").expect("parse");
        assert!(v.phrases().is_empty(), "`st.` is one token => no phrase");
        let g = &v.equivalences()[0];
        assert!(
            g.contains(&"st.".to_string()) && g.contains(&"saint".to_string()),
            "dotted form kept verbatim: {g:?}"
        );
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
