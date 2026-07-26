use super::*;

/// A single-node DURABLE engine with an active MULTI-WORD alias (ADR-061) survives reopen with zero
/// false negatives, INCLUDING a post-reopen live insert. The cluster durability suites can't cover
/// this (the cluster refuses multi-word aliases), and it exercises the trickiest `adopt_vocab`
/// branch: a RECOVERED engine must restore the equivalence map by resolving the multi-word entity
/// (`term:new_york`) AS-IS against the persisted dict (where it is already dense), so both the
/// baked-in existing queries AND a fresh `ny` insert key the same id the title side resolves.
#[test]
fn durable_reopen_preserves_multiword_alias() {
    let dir = test_dir("durable_multiword_alias");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut vocab = Vocab::new();
    vocab.import_solr_aliases(
        "ny => new york",
        &Normalizer::default_vocab().expect("vocab"),
        &reverse_rusty::dict::Dict::new(),
    );

    // Build durable with the alias active, store a `new york` query, flush, close.
    {
        let mut eng = Engine::with_vocab(vocab.clone(), config.clone()).expect("with_vocab");
        eng.build_from_queries(&[(1, "new york yankees".into())]);
        assert!(
            match_ids(&eng, "ny yankees").contains(&1),
            "pre-persist: ny title reaches the new york query"
        );
        eng.flush();
    }

    // Reopen (server path): open with the rebuilt normalizer, then adopt the vocab on the recovered
    // engine to restore the equivalence map.
    let mut eng = Engine::open(vocab.to_normalizer().expect("norm"), config).expect("reopen");
    eng.adopt_vocab(vocab)
        .expect("adopt_vocab on recovered engine");

    // Existing query keeps the alias across reopen.
    assert!(
        match_ids(&eng, "ny yankees").contains(&1),
        "FN: existing multi-word alias query lost its reach after durable reopen"
    );
    // A post-reopen live insert gains the alias too (recovered-engine resolve-as-is keys the map on
    // the same dense id the title side resolves).
    eng.try_insert_live("ny mets", 2, 1)
        .expect("post-reopen insert");
    assert!(
        match_ids(&eng, "new york mets").contains(&2),
        "FN: post-reopen ny insert did not gain the multi-word alias"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
