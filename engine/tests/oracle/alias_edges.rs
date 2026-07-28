//! Multi-word alias (ADR-061) edge-case differential tests, split out of `alias.rs` to keep that
//! file under the size budget. Covers the ID-stability-on-fresh-index FN class for the multi-word
//! entity-interning path, and the end-to-end "Goldilocks parse" stateful-level parse-union (the
//! exhaustive normalizer-level sweep lives in `src/normalize/parse_union_oracle.rs`).

use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::{Normalizer, PunctClass};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::Vocab;
use std::collections::HashSet;

fn matched(eng: &mut Engine, s: &mut MatchScratch, title: &str) -> HashSet<u64> {
    let mut out = Vec::new();
    eng.match_title(title, s, &mut out, true);
    out.iter().copied().collect()
}

/// The ADR-060 ID-stability bug, MULTI-WORD variant: a multi-word alias installed on a FRESH index
/// — before its entity (`term:new_york`) is interned — must survive a LATER live insert that interns
/// that entity as a dense id. The multi-word entity is interned via the alias-phrase collapse (a
/// different path from a single-token form), so it gets its own guard. Without correct interning the
/// equivalence map keys on the entity's synthetic id, the dense insert never matches it, and the
/// alias silently dies (a false negative). Single-token analogue lives in
/// `alias::alias_ids_are_stable_after_future_insert`.
#[test]
fn multiword_alias_survives_future_insert_on_fresh_index() {
    let mut v = Vocab::new();
    let activated = v.import_solr_aliases(
        "ny => new york",
        &Normalizer::default_vocab().expect("vocab"),
        &Dict::new(),
    );
    assert_eq!(activated, 1, "the declared multi-word alias must activate");

    // Fresh engine: empty dict ⇒ `new york`'s entity is NOT yet interned when the alias installs.
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.set_vocab(v).expect("set_vocab on fresh engine");
    // A later insert interns the multi-word entity dense; the alias must still resolve to it.
    eng.try_insert_live("new york inventory", 1, 1)
        .expect("insert");

    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "ny inventory").contains(&1),
        "multi-word alias must survive a future insert on a fresh index (a ny title reaches the \
         new york query)"
    );
}

/// Codex R11 (P2): a whitespace RUN inside a query-side alias occurrence — the DSL passes a
/// quoted phrase's inner text verbatim to `compile_features` — must still collapse to the alias
/// entity. Without the query-side run collapse the query compiles to component terms, equivalence
/// expansion never reaches the group, and `"new  york" catalog` misses a `ny catalog` title (a false
/// negative of the zero-FN contract).
#[test]
fn query_alias_with_whitespace_run_still_reaches_the_group() {
    let mut v = Vocab::new();
    let activated = v.import_solr_aliases(
        "ny => new york",
        &Normalizer::default_vocab().expect("vocab"),
        &Dict::new(),
    );
    assert_eq!(activated, 1, "the declared multi-word alias must activate");

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.set_vocab(v).expect("set_vocab");
    // The quoted phrase carries a DOUBLE space; the DSL hands it to the normalizer verbatim.
    eng.build_from_queries(&[(1, "\"new  york\" catalog".into())]);

    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "ny catalog").contains(&1),
        "a whitespace run inside the quoted alias phrase must not hide the alias"
    );
    assert!(
        matched(&mut eng, &mut s, "new york catalog").contains(&1),
        "the literal form still matches"
    );
}

/// Codex R11: an ACTIVE alias must keep matching across a punctuation-table change that alters
/// its forms' tokenization. `ab => a-b` under `-`:Fold classifies single-token (`a-b` cleans to
/// `ab`); re-classing `-` to Split makes `a-b` clean to `a b`. Phrase registration re-derives
/// multi-wordness from the LIVE punctuation table (not the stored kind snapshot), so the form
/// registers as an alias phrase, still resolves to one feature, and the group survives — instead
/// of being dropped from the equivalence map and silently dying while still reporting Active.
#[test]
fn active_alias_survives_punctuation_reclassification() {
    let mut v = Vocab::new();
    v.fold_punctuation('-');
    let norm = v.to_normalizer().expect("normalizer with - folded");
    assert_eq!(v.import_solr_aliases("ab => a-b", &norm, &Dict::new()), 1);
    v.set_punct_class('-', PunctClass::Split);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.set_vocab(v).expect("set_vocab");
    eng.build_from_queries(&[(1, "a b foo".into())]);

    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "ab foo").contains(&1),
        "an active alias must track a punctuation reclassification of its forms"
    );
}

/// Codex R12 (P1): a boundary-INVALID automaton match must not suppress a valid overlapping
/// alias. With `ab => a b` and `bc => b c`, the query `xa b c` contains `a b` mid-token (inside
/// `xa b`); the legacy leftmost-longest pass selected it, consumed its span (suppressing the
/// valid `b c`), then dropped it at the boundary post-filter — the query compiled to component
/// terms, expansion never reached the `bc` group, and an `xa bc` title was missed (an FN).
#[test]
fn boundary_overlap_alias_query_still_matches() {
    let mut v = Vocab::new();
    let activated = v.import_solr_aliases(
        "ab => a b\nbc => b c",
        &Normalizer::default_vocab().expect("vocab"),
        &Dict::new(),
    );
    assert_eq!(activated, 2, "both declared multi-word aliases activate");

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.set_vocab(v).expect("set_vocab");
    eng.build_from_queries(&[(1, "xa b c".into())]);

    let mut s = MatchScratch::new();
    assert!(
        matched(&mut eng, &mut s, "xa bc").contains(&1),
        "the mid-token `a b` candidate must not suppress the valid `b c` alias"
    );
    assert!(
        matched(&mut eng, &mut s, "xa b c").contains(&1),
        "the literal title still matches"
    );
}
