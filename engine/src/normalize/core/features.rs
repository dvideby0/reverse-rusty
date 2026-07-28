use super::{Dict, FeatureId, FeatureKind, NormScratch, Normalizer, Side};

impl Normalizer {
    // ---- compile-time and match-time entry points ----

    /// Compile path: intern features (creating new ones), returning sorted+deduped IDs.
    ///
    /// Off the hot path (per stored query at compile time, not per title), so it owns a
    /// local [`NormScratch`] and keeps its stable `&mut String` signature — callers across
    /// the compile/vocab paths are unchanged. The per-title reuse lives on the match path.
    pub fn compile_features(&self, text: &str, dict: &mut Dict, lc: &mut String) -> Vec<FeatureId> {
        let mut ids: Vec<FeatureId> = Vec::new();
        let mut names: Vec<(String, FeatureKind)> = Vec::new();
        let mut sc = NormScratch::new();
        self.emit(text, lc, &mut sc, Side::Query, false, &mut |name, kind| {
            names.push((name.to_string(), kind));
        });
        for (name, kind) in names {
            ids.push(dict.intern(&name, kind));
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Read-only compile: resolve features by name without interning new ones. A term
    /// absent from the (frozen) dict is assigned a deterministic *synthetic* ID
    /// (dynamic vocabulary, ADR-046) rather than dropped — so a query added after the
    /// dict is frozen is *absorbed* with its full semantics instead of silently
    /// broadening. Used by the cluster live-write path and by explain.
    pub fn compile_features_readonly(
        &self,
        text: &str,
        dict: &Dict,
        lc: &mut String,
    ) -> Vec<FeatureId> {
        let mut ids: Vec<FeatureId> = Vec::new();
        let mut sc = NormScratch::new();
        self.emit(text, lc, &mut sc, Side::Query, false, &mut |name, _kind| {
            ids.push(dict.get_or_synthetic(name));
        });
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Match path: resolve title features by name. A token absent from the (frozen)
    /// dict is assigned a deterministic *synthetic* ID (dynamic vocabulary, ADR-046)
    /// rather than dropped — so a live-added query that references a new term still
    /// matches a title containing it (the title side must hash too, or that match
    /// would be a false negative). Interned tokens keep their dense ID. Fills `out`
    /// with sorted+deduped IDs.
    pub fn match_features(
        &self,
        text: &str,
        dict: &Dict,
        lc: &mut String,
        sc: &mut NormScratch,
        out: &mut Vec<FeatureId>,
    ) {
        // Push straight into `out` (the caller's reused buffer), then sort + dedup in
        // place — no separate `tmp` allocation. `emit` borrows `sc` for its internal
        // working buffers; the closure writes to `out`, which is disjoint from `sc`.
        out.clear();
        self.emit(text, lc, sc, Side::Title, false, &mut |name, _kind| {
            out.push(dict.get_or_synthetic(name));
        });
        out.sort_unstable();
        out.dedup();
    }

    /// Match path producing the **two title feature views** of ADR-061:
    ///
    /// - `neg` = the canonical leftmost-longest set `N(T)` — the same set
    ///   [`match_features`](Self::match_features) produces. Used **only** for forbidden
    ///   (MUST_NOT) checks, so a forbidden clause stays recall-correct (`foo -"new york"`
    ///   still matches `foo new york city`).
    /// - `pos` = the **maximal positive view** `P(T) ⊇ N(T)`. Computed as the parse-union: a second
    ///   emit with **all phrases forced additive** (nothing consumed ⇒ every token feature plus
    ///   every leftmost-longest entity) ∪ the **overlapping** entity pass. So `P(T)` contains every
    ///   feature any parse could emit — every nested/overlapping alias entity AND the component
    ///   tokens of a phrase displaced from the leftmost-longest parse. It drives flat candidate
    ///   retrieval plus required + any-of; the phrase-aware path extends a separate probe view
    ///   with positioned-only labels. A strict superset of every parse ⇒ FN-safe; it only ever
    ///   adds to the positive view (a wider positive read is a bounded false positive, never a
    ///   negative).
    ///
    /// With no active multi-word alias, `P(T) == N(T)` and the two
    /// outputs are identical — the caller then passes one slice for both views and the
    /// verifier is byte-identical to the single-view path. Both outputs are sorted + deduped.
    pub fn match_features_dual(
        &self,
        text: &str,
        dict: &Dict,
        lc: &mut String,
        sc: &mut NormScratch,
        neg: &mut Vec<FeatureId>,
        pos: &mut Vec<FeatureId>,
    ) {
        neg.clear();
        pos.clear();
        // N(T): the canonical leftmost-longest parse (phrase modes respected). `emit` cleans
        // `text` into `lc` first. We accumulate into `pos` (the caller's reused buffer, disjoint
        // from the `sc` working buffers `emit` borrows), sort + dedup, then copy the canonical
        // set into `neg`. `pos` then stays the running superset accumulator below — so the path
        // allocates no per-call `tmp`.
        self.emit(text, lc, sc, Side::Title, false, &mut |name, _kind| {
            pos.push(dict.get_or_synthetic(name));
        });
        pos.sort_unstable();
        pos.dedup();
        neg.extend_from_slice(pos);

        match (self.has_multiword_aliases, self.phrase_overlap.as_ref()) {
            // No alias phrases: positive view == negative view (single-view fast path elsewhere).
            // `pos` already holds N(T) == P(T); nothing more to add.
            (false, _) => {}
            (true, Some(ov)) => {
                // P(T) = N(T) ∪ force-additive parse-union ∪ raw token features ∪ overlapping
                // entities. `pos` already holds N(T); only ever ADD (never replace), so P(T) is a
                // strict superset of every parse and activating an alias can never drop a feature.
                // The force-additive re-emit recovers components of a displaced additive
                // phrase. The raw-token pass below also retains the lexical reading of every
                // cleaned component, and the second `emit` leaves `lc` holding the text used
                // by the overlap pass and token scan.
                self.emit(text, lc, sc, Side::Title, true, &mut |name, _kind| {
                    pos.push(dict.get_or_synthetic(name));
                });
                // The `"term:<token>"` builder is reused on `sc.name` (the second `emit` has
                // returned, so `sc` is free again). `lc` is borrowed immutably for tokenization,
                // disjoint from the `&mut sc.name` write and the `&mut pos` push.
                let name = &mut sc.name;
                name.clear();
                name.push_str("term:");
                for tok in lc.split_whitespace() {
                    if tok == "#" || tok == "/" {
                        continue; // structural markers, never a term feature
                    }
                    name.truncate(5); // keep the "term:" prefix
                    name.push_str(tok);
                    pos.push(dict.get_or_synthetic(name));
                }
                ov.collect_into(lc, dict, pos);
                pos.sort_unstable();
                pos.dedup();
            }
            // Private builder state guarantees this arm is unreachable. Keep
            // the library fail-safe if that invariant is ever broken.
            (true, None) => debug_assert!(false, "alias phrase missing overlap automaton"),
        }
    }
}
