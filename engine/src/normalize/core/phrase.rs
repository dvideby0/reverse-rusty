use super::{
    position_index, Dict, EmitMode, FeatureId, FeatureKind, NormScratch, Normalizer, PhraseArc,
    PhraseGraph, PositionArc, Side,
};

impl Normalizer {
    /// Compile one quoted DSL clause into its analyzed token graph, interning
    /// query-side feature labels (ADR-120).
    ///
    /// This is deliberately separate from [`compile_features`](Self::compile_features):
    /// unquoted clauses retain their set semantics and compact SoA columns,
    /// while only quoted clauses preserve analyzer positions and alternate
    /// multi-word paths.
    pub fn compile_phrase(&self, text: &str, dict: &mut Dict, lc: &mut String) -> PhraseGraph {
        let mut scratch = NormScratch::new();
        let mut arcs = Vec::new();
        let (positions, _complete) = self.analyze_position_arcs(
            text,
            lc,
            &mut scratch,
            Side::Query,
            false,
            &mut arcs,
            |name, kind| dict.intern(name, kind),
        );
        phrase_graph(positions, arcs)
    }

    /// Read-only twin of [`compile_phrase`](Self::compile_phrase), resolving
    /// out-of-dictionary labels to deterministic synthetic ids.
    pub fn compile_phrase_readonly(&self, text: &str, dict: &Dict, lc: &mut String) -> PhraseGraph {
        let mut scratch = NormScratch::new();
        let mut arcs = Vec::new();
        let (positions, _complete) = self.analyze_position_arcs(
            text,
            lc,
            &mut scratch,
            Side::Query,
            false,
            &mut arcs,
            |name, _kind| dict.get_or_synthetic(name),
        );
        phrase_graph(positions, arcs)
    }

    /// Phrase-aware title normalization (ADR-120).
    ///
    /// The ordinary flat feature views are produced by the existing ADR-061
    /// entry point first, so phrase support cannot silently change bare-term,
    /// any-of, or MUST_NOT behavior. The extra graph pass is activated by the
    /// engine only while at least one live row contains a quoted predicate.
    /// `probe` is the candidate-only union of flat positive and graph labels.
    /// Exact flat semantics remain in `pos`; keeping the two separate prevents
    /// an unrelated quoted row from changing how ordinary queries verify.
    ///
    /// Returns `(positions, positive_graph_complete)`. An incomplete positive
    /// graph hit a bounded analyzer-state guard and must fail open in exact
    /// phrase verification.
    #[allow(clippy::too_many_arguments)]
    pub fn match_phrase_views(
        &self,
        text: &str,
        dict: &Dict,
        lc: &mut String,
        sc: &mut NormScratch,
        neg: &mut Vec<FeatureId>,
        pos: &mut Vec<FeatureId>,
        probe: &mut Vec<FeatureId>,
        neg_arcs: &mut Vec<PositionArc>,
        pos_arcs: &mut Vec<PositionArc>,
    ) -> (u32, bool) {
        self.match_features_dual(text, dict, lc, sc, neg, pos);

        let (positions, _negative_complete) = self.analyze_position_arcs(
            text,
            lc,
            sc,
            Side::Title,
            false,
            neg_arcs,
            |name, _kind| dict.get_or_synthetic(name),
        );

        let (positive_positions, positive_complete) =
            self.analyze_position_arcs(text, lc, sc, Side::Title, true, pos_arcs, |name, _kind| {
                dict.get_or_synthetic(name)
            });
        debug_assert_eq!(positions, positive_positions);

        // Phrase-aware P(T) is a union independently of whether ADR-061 aliases
        // are configured: retain the canonical path, force-additive analyzer
        // paths, normalized raw-token alternatives, and every overlapping
        // declared phrase entity. Gating this union on alias activation lets an
        // ordinary collapse phrase erase a valid quoted component path.
        pos_arcs.extend_from_slice(neg_arcs);
        for (i, &(start, end)) in sc.tokens.iter().enumerate() {
            let tok = &lc[start..end];
            if tok == "#" || tok == "/" {
                continue;
            }
            sc.name.clear();
            sc.name.push_str("term:");
            sc.name.push_str(tok);
            pos_arcs.push(PositionArc {
                feature: dict.get_or_synthetic(&sc.name),
                start: position_index(i),
                end: position_index(i.saturating_add(1)),
            });
        }
        if let Some(overlap) = &self.phrase_overlap {
            overlap.collect_positioned_into(lc, &sc.tokens, dict, pos_arcs);
        }
        pos_arcs.sort_unstable_by_key(|arc| (arc.start, arc.end, arc.feature));
        pos_arcs.dedup();

        // Every exact phrase path has at least one graph edge. Making all
        // positive graph labels CANDIDATE-visible therefore supplies a lossless
        // proxy even for analyzer-only gap labels without widening flat exact
        // semantics for phrase-free rows.
        probe.clear();
        probe.extend_from_slice(pos);
        probe.extend(pos_arcs.iter().map(|arc| arc.feature));
        probe.sort_unstable();
        probe.dedup();
        (positions, positive_complete)
    }

    /// Analyze `text` into a flat token-graph edge list. `out` is caller-owned
    /// reusable storage on the title hot path.
    #[allow(clippy::too_many_arguments)]
    fn analyze_position_arcs<F>(
        &self,
        text: &str,
        lc: &mut String,
        sc: &mut NormScratch,
        side: Side,
        force_additive: bool,
        out: &mut Vec<PositionArc>,
        mut resolve: F,
    ) -> (u32, bool)
    where
        F: FnMut(&str, FeatureKind) -> FeatureId,
    {
        out.clear();
        self.emit_positioned(
            text,
            lc,
            sc,
            EmitMode::positioned(side, force_additive),
            &mut |name, kind, start, end| {
                let arc = PositionArc {
                    feature: resolve(name, kind),
                    start,
                    end,
                };
                out.push(arc);
            },
        );

        let positions = position_index(sc.tokens.len());

        // The semantic analyzer intentionally emits nothing for structural
        // markers and a few context words. Quoted phrases still need those
        // lexical positions to remain contiguous, so fill only graph holes with
        // a normalized raw term edge. Build coverage as a difference array:
        // the old pair of `out.iter().any(...)` scans per token made ordinary
        // one-edge-per-token titles quadratic whenever any quoted row was live.
        // Do NOT restore tokens consumed by a collapse/alias edge: that would
        // defeat ADR-061's canonical negative parse and manufacture an
        // unconfigured alternate path.
        let position_count = positions as usize;
        sc.position_coverage_delta.clear();
        sc.position_coverage_delta
            .resize(position_count.saturating_add(1), 0);
        for arc in out.iter() {
            let start = (arc.start as usize).min(position_count);
            let end = (arc.end as usize).min(position_count);
            if start < end {
                sc.position_coverage_delta[start] += 1;
                sc.position_coverage_delta[end] -= 1;
            }
        }
        let mut coverage = 0i64;
        for i in 0..position_count {
            coverage += sc.position_coverage_delta[i];
            if coverage > 0 {
                continue;
            }
            let (start, end) = sc.tokens[i];
            sc.name.clear();
            sc.name.push_str("term:");
            sc.name.push_str(&lc[start..end]);
            out.push(PositionArc {
                feature: resolve(&sc.name, FeatureKind::Generic),
                start: position_index(i),
                end: position_index(i.saturating_add(1)),
            });
        }

        out.sort_unstable_by_key(|arc| (arc.start, arc.end, arc.feature));
        out.dedup();
        (positions, true)
    }
}

/// Group singleton analyzed edges by span into the query-side alternatives
/// stored in one quoted predicate.
fn phrase_graph(positions: u32, mut arcs: Vec<PositionArc>) -> PhraseGraph {
    arcs.sort_unstable_by_key(|arc| (arc.start, arc.end, arc.feature));
    arcs.dedup();
    let mut grouped: Vec<PhraseArc> = Vec::new();
    for arc in arcs {
        if let Some(last) = grouped.last_mut() {
            if last.start == arc.start && last.end == arc.end {
                last.alternatives.push(arc.feature);
                continue;
            }
        }
        grouped.push(PhraseArc {
            start: arc.start,
            end: arc.end,
            alternatives: vec![arc.feature],
        });
    }
    PhraseGraph {
        positions,
        arcs: grouped,
    }
}
