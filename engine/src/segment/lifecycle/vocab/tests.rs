#[cfg(test)]
mod source_generation_tests {
    use crate::normalize::Normalizer;
    use crate::segment::Engine;

    #[test]
    fn live_document_gather_rejects_same_version_stale_source() {
        let mut engine = Engine::new(Normalizer::default_vocab().expect("default normalizer"));
        engine
            .try_upsert_live_with_tags("1994 acme", 7, 1, &[("status".into(), "old".into())])
            .expect("first write");
        let stale = engine
            .query_store
            .get_document(7)
            .expect("first source document");

        engine
            .try_upsert_live_with_tags("1995 vertex", 7, 1, &[("status".into(), "new".into())])
            .expect("same-version replacement");
        // The source store intentionally refuses generation rollback. Model a
        // divergent sidecar by publishing the stale payload under a distinct,
        // later generation instead; either direction must fail the exact/source
        // equality check.
        let divergent_generation = engine.allocate_source_generation();
        engine.query_store.insert_document_with_generation(
            7,
            stale.query().to_owned(),
            stale.version(),
            divergent_generation,
            stale.tags(),
        );

        assert_eq!(
            engine.live_source_documents_tagged(),
            Err(7),
            "a rebuild/fingerprint gather must fail closed on stale source evidence"
        );
        let epoch = engine.vocab_epoch();
        assert!(
            engine.set_vocab(crate::vocab::Vocab::default()).is_err(),
            "vocabulary mutation must reject before changing the live normalizer"
        );
        assert_eq!(engine.vocab_epoch(), epoch);
    }

    #[test]
    fn additive_live_then_bulk_uses_the_newest_source_generation() {
        let mut engine = Engine::new(Normalizer::default_vocab().expect("default normalizer"));
        engine
            .try_insert_live("1994 acme", 7, 1)
            .expect("live insert");
        let report = engine.bulk_ingest(&[(7, "1995 vertex".to_string())]);
        assert_eq!(report.ingested, 1);

        let document = engine
            .snapshot()
            .get_query_document(7)
            .expect("newer bulk source must pair with its base-segment exact row");
        assert_eq!(document.query(), "1995 vertex");
        engine
            .set_vocab(crate::vocab::Vocab::default())
            .expect("a coherent additive history must remain rebuildable");
        assert_eq!(engine.recompile_stale_segments(), 1);
    }

    #[test]
    fn set_vocab_rejects_an_unrebuildable_corpus_without_mutating_live_state() {
        let mut engine = Engine::new(Normalizer::default_vocab().expect("default normalizer"));
        engine.try_insert_live("...", 1, 1).expect("dot query");
        engine.try_insert_live("a.b", 2, 1).expect("term query");

        let mut scratch = crate::segment::MatchScratch::new();
        let mut before = Vec::new();
        engine.match_title("a.b", &mut scratch, &mut before, true);
        assert!(before.contains(&2));
        let epoch = engine.vocab_epoch();

        let mut vocab = crate::vocab::Vocab::new();
        vocab.set_punct_class('.', crate::normalize::PunctClass::Split);
        let error = engine
            .set_vocab(vocab)
            .expect_err("the dot-only stored query becomes effectively empty");
        assert!(
            error.to_string().contains("logical id 1")
                && error.to_string().contains("effectively empty"),
            "got: {error}"
        );
        assert_eq!(engine.vocab_epoch(), epoch);
        assert!(!engine.has_stale_segments());

        let mut after = Vec::new();
        engine.match_title("a.b", &mut scratch, &mut after, true);
        assert!(
            after.contains(&2),
            "a rejected vocabulary must leave the old query/title feature space live"
        );
    }
}

#[cfg(test)]
mod metadata_only_tests {
    use crate::normalize::Normalizer;
    use crate::segment::Engine;
    use crate::vocab::{AliasProvenance, AliasStatus};

    /// The install_vocab_metadata_only guard (ADR-102, hardened per codex): a candidate-only
    /// registry change takes the fast path (no epoch bump); ANY change outside the registry —
    /// here a synonym, which affects the normalizer while leaving the alias projections equal —
    /// must fall back to the genuine-change path, so the advertised vocab can never desync from
    /// the live normalizer.
    #[test]
    fn fast_path_only_for_registry_candidate_changes() {
        let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
        eng.build_from_queries(&[(1, "vertex product gamma".to_string())]);
        let e0 = eng.vocab_epoch();

        // Candidates-only: fast path — vocab installed, epoch untouched.
        let mut v = eng.vocab().cloned().unwrap_or_default();
        let status = v.aliases_mut().add_classified(
            &["zzns".to_string(), "zznorthstar".to_string()],
            AliasProvenance::LearnedDistributional,
            0.7,
            &eng.norm,
            &eng.dict,
        );
        assert_eq!(status, Some(AliasStatus::Candidate));
        assert!(
            eng.install_vocab_metadata_only(v).expect("install"),
            "a candidate-only change takes the fast path"
        );
        assert_eq!(eng.vocab_epoch(), e0, "no epoch bump on the fast path");
        assert!(
            eng.aliases().expect("vocab").entries().len() == 1,
            "the candidate was installed"
        );

        // A synonym added with IDENTICAL alias projections: the whole-document guard must
        // reject the fast path (epoch bumps, set_vocab ran).
        let mut v = eng.vocab().cloned().unwrap_or_default();
        v.add_synonym("colour", "color", crate::dict::FeatureKind::Generic);
        assert!(
            !eng.install_vocab_metadata_only(v).expect("install"),
            "a normalizer-affecting change must take the full set_vocab path"
        );
        assert!(
            eng.vocab_epoch() > e0,
            "the fallback is the genuine-change path (epoch bumped)"
        );
    }
}

#[cfg(test)]
mod alias_import_tests {
    use crate::normalize::Normalizer;
    use crate::segment::{Engine, MatchScratch};
    use crate::vocab::Vocab;

    #[test]
    fn identical_import_completes_a_pending_split_apply() {
        let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
        engine.build_from_queries(&[(1, "package adapter".to_string())]);

        let mut vocab = Vocab::new();
        vocab
            .import_solr_aliases("package, pkg", &engine.norm, &engine.dict)
            .expect("valid aliases");
        engine
            .set_vocab(vocab)
            .expect("install vocabulary without recompiling");
        assert!(engine.has_stale_segments(), "split apply is pending");

        let report = engine
            .import_alias_synonyms("package, pkg")
            .expect("identical import must finish the rebuild");
        assert!(report.applied, "the pending rebuild changed live state");
        assert_eq!(report.recompiled, 1);
        assert!(!engine.has_stale_segments());

        let mut scratch = MatchScratch::new();
        let mut matches = Vec::new();
        engine.match_title("pkg adapter", &mut scratch, &mut matches, true);
        assert_eq!(matches, vec![1]);
    }
}
