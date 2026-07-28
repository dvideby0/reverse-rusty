use super::*;
use crate::segment::MatchScratch;

fn scratch_dir() -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "reverse_rusty_clause_migration_ids_{}_{}",
        std::process::id(),
        nonce
    ))
}

fn stamp_legacy(path: &std::path::Path) {
    let mut bytes = std::fs::read(path).expect("read segment");
    bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
    let body = bytes.len() - 4;
    let crc = crate::storage::crc32(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(path, bytes).expect("write legacy stamp");
}

fn matches(engine: &Engine, title: &str, logical: u64) -> bool {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.contains(&logical)
}

#[test]
fn migration_interns_features_exposed_by_splitting_the_legacy_stream() {
    let dir = scratch_dir();
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut vocab = crate::vocab::Vocab::new();
    vocab.import_solr_aliases(
        "ny => new york",
        &Normalizer::default_vocab().expect("normalizer"),
        &Dict::new(),
    );

    {
        let mut engine = Engine::with_vocab(vocab.clone(), config.clone()).expect("vocab engine");
        // This exact plan contains only the collapsed alias entity, matching
        // what the legacy cross-clause stream produced.
        engine.build_from_queries(&[(1, "new york".to_string())]);
        assert!(engine.dict().get("term:new").is_none());
        assert!(engine.dict().get("term:york").is_none());

        // Retain the same exact-row metadata but substitute the true source
        // predicate that the legacy compiler mis-lowered.
        let source = engine
            .snapshot()
            .get_query_document(1)
            .expect("source metadata");
        engine.query_store.insert_document_with_generation(
            1,
            "new -used york".to_string(),
            source.version(),
            source.source_generation(),
            source.tags(),
        );
        engine.save_query_sources();
    }

    let manifest = crate::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &manifest.segment_files {
        stamp_legacy(&dir.join("segments").join(name));
    }

    let mut reopened = Engine::open_with_vocab(vocab, config).expect("source-driven migration");
    let new_id = reopened
        .dict()
        .get("term:new")
        .expect("newly exposed term is interned");
    assert!(
        reopened.dict().get("term:york").is_some(),
        "every separated component must be dense before commit"
    );
    assert!(matches(&reopened, "new vintage product york", 1));

    // A later standalone insert uses the same dense ID; it cannot turn the
    // migrated row's synthetic feature into an unreachable split brain.
    reopened
        .try_insert_live("new", 2, 1)
        .expect("post-migration insert");
    assert_eq!(reopened.dict().get("term:new"), Some(new_id));
    assert!(matches(&reopened, "new vintage product york", 1));

    drop(reopened);
    std::fs::remove_dir_all(dir).expect("cleanup");
}
