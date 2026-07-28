use super::*;

/// Codex R13 (P1): a query in the WAL TAIL (inserted after the last flush) must recover with its
/// alias expansion. `Engine::open` replays the tail BEFORE any vocab is installed, and the
/// equivalence map is transient (never persisted in the dict) — so the pre-fix open + adopt order
/// recompiled those queries unexpanded, and a recovered `new york catalog` query no longer reached a
/// `ny catalog` title (a recovery false negative). Covers BOTH healing paths: `open_with_vocab`
/// (equivalences installed before replay — the server's path) and the legacy `open` +
/// `adopt_vocab` (which now detects the hazard and escalates to a full recompile).
#[test]
fn wal_tail_recovers_alias_expansion() {
    for use_open_with_vocab in [true, false] {
        let dir = test_dir(&format!("wal_tail_alias_{use_open_with_vocab}"));
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

        {
            let mut eng = Engine::with_vocab(vocab.clone(), config.clone()).expect("with_vocab");
            eng.build_from_queries(&[(99, "seed".into())]); // flushed base state
                                                            // The WAL-tail query: inserted live, never flushed.
            eng.try_insert_live("new york catalog", 1, 1)
                .expect("insert");
            assert!(
                match_ids(&eng, "ny catalog").contains(&1),
                "pre-crash sanity"
            );
        } // drop without a flush — the query survives only in the WAL

        let eng = if use_open_with_vocab {
            Engine::open_with_vocab(vocab.clone(), config).expect("open_with_vocab")
        } else {
            let mut e = Engine::open(vocab.to_normalizer().expect("norm"), config).expect("open");
            e.adopt_vocab(vocab.clone()).expect("adopt_vocab");
            e
        };
        assert!(
            match_ids(&eng, "ny catalog").contains(&1),
            "FN: the WAL-tail query lost its alias expansion on recovery \
             (open_with_vocab={use_open_with_vocab})"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
