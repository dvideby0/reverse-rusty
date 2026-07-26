use super::*;

#[test]
fn extracted_ingest_rejects_a_merged_tag_column_over_u16() {
    let mut engine =
        Engine::new(crate::normalize::Normalizer::default_vocab().expect("normalizer"));
    let seed = vec![(1, "1994 upper deck".to_string())];
    assert_eq!(engine.build_from_queries(&seed).ingested, 1);

    let ast = crate::dsl::parse("1994 upper deck").expect("parse");
    let mut lc = String::new();
    let ex = crate::compile::extract_readonly(&ast, &engine.norm, &engine.dict, &mut lc);
    let item = PlacedQuery {
        logical: 2,
        ex,
        dsl: "1994 upper deck".into(),
        version: 1,
        source_generation: None,
        tags: Vec::new(),
        // Nonempty carry-through bypasses the runtime max_tags check, but
        // the exact-store u16 count ceiling remains unconditional.
        tag_ids: (0..=u32::from(u16::MAX)).collect(),
        rank: crate::rank::RankValues::default(),
        placement: crate::ownership::QueryPlacement::standalone(),
    };
    let report = engine.ingest_extracted(&[item]);
    assert_eq!(report.ingested, 0);
    assert_eq!(report.rejected_parse, 1);
    assert!(
        !engine.snapshot().has_live_query(2),
        "a wrapping tag column must never reach the exact store"
    );
}
