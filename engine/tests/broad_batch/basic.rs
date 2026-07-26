use super::*;

#[test]
fn batch_equals_scalar_single_segment() {
    let data = gen(0xB0A7, 20_000, 2_000, 0.05);
    let eng = build_single(&data);
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_equals_scalar_multi_segment_memtable() {
    let data = gen(0x00C0_FFEE, 20_000, 2_000, 0.05);
    let eng = build_multi(&data);
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_equals_scalar_with_tombstones() {
    let data = gen(0xDEAD, 20_000, 2_000, 0.05);
    let mut eng = build_multi(&data);
    // Delete ~30% by logical id — tombstones across base segments + memtable.
    for (id, _) in data.queries.iter().take(data.queries.len() * 3 / 10) {
        let _ = eng.delete_by_logical_id(*id);
    }
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_equals_scalar_high_broad_fraction() {
    // Heavier broad population stresses the broad lane (more reachable broad
    // queries per batch, more pure-anchor + non-pure mix).
    let data = gen(0x5EED, 15_000, 1_500, 0.30);
    let eng = build_multi(&data);
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_equals_scalar_with_class_d_lane() {
    // Class-D always-candidates (ADR-068) ride the broad lane under the
    // universal signature: the batch kernel probes it ONCE per batch, the scalar
    // path once per title — the full matrix (columnar/inline × materialize ×
    // batch sizes × broad on/off) must stay byte-identical with them stored.
    use reverse_rusty::config::EngineConfig;
    use reverse_rusty::gen::gen_class_d_queries;
    let data = gen(0xD1A5, 12_000, 1_200, 0.10);
    let mut eng = Engine::with_config(
        Normalizer::default_vocab().expect("vocab"),
        EngineConfig {
            accept_class_d: true,
            ..EngineConfig::default()
        },
    );
    let n = data.queries.len();
    let c = n / 4;
    eng.build_from_queries(&data.queries[..c]);
    eng.bulk_ingest(&data.queries[c..2 * c]);
    eng.bulk_ingest(&data.queries[2 * c..3 * c]);
    for (id, text) in &data.queries[3 * c..] {
        eng.insert_live(text, *id, 1);
    }
    // Negation-only queries across every layout: sealed base segments AND the
    // live memtable tail.
    for (i, q) in gen_class_d_queries(0xD1A5_D00D, 150).iter().enumerate() {
        eng.insert_live(q, 2_000_000 + i as u64, 1);
    }
    eng.flush();
    for (i, q) in gen_class_d_queries(0xD1A5_BEEF, 150).iter().enumerate() {
        eng.insert_live(q, 3_000_000 + i as u64, 1);
    }
    run_matrix(&eng, &data.titles);
}

#[test]
fn batch_inline_equals_columnar() {
    // Independent of the scalar baseline: the two strategies must agree.
    let data = gen(0xA11CE, 12_000, 1_000, 0.15);
    let eng = build_multi(&data);
    for &bs in &[1usize, 64, 256, 999] {
        let inline = batch_result(
            &eng,
            &data.titles,
            BatchMatchOptions {
                include_broad: true,
                broad_batch_size: bs,
                broad_strategy: BroadStrategy::Inline,
                broad_materialize: true,
                broad_prefilter: true,
            },
        );
        let columnar = batch_result(
            &eng,
            &data.titles,
            BatchMatchOptions {
                include_broad: true,
                broad_batch_size: bs,
                broad_strategy: BroadStrategy::Columnar,
                broad_materialize: true,
                broad_prefilter: true,
            },
        );
        assert_eq!(inline, columnar, "Inline != Columnar at batch_size {bs}");
    }
}

#[test]
fn batch_materialize_on_equals_off() {
    // The pure-anchor materialization fast path is a kill-switch: turning it off
    // forces those queries through full bitmap verification, which must yield
    // byte-identical results (only slower). Independent of the scalar baseline.
    let data = gen(0x11_1A7E, 12_000, 1_000, 0.25);
    let eng = build_multi(&data);
    for &bs in &[1usize, 64, 256, 999] {
        let opts = |materialize| BatchMatchOptions {
            include_broad: true,
            broad_batch_size: bs,
            broad_strategy: BroadStrategy::Columnar,
            broad_materialize: materialize,
            broad_prefilter: true,
        };
        let on = batch_result(&eng, &data.titles, opts(true));
        let off = batch_result(&eng, &data.titles, opts(false));
        assert_eq!(on, off, "materialize on != off at batch_size {bs}");
    }
}

#[test]
fn batch_empty_and_singleton() {
    let data = gen(0xE3, 5_000, 500, 0.1);
    let eng = build_single(&data);

    // Empty batch: no panic, empty result.
    let empty: Vec<String> = Vec::new();
    let r = eng.snapshot().match_titles_batch(
        &empty,
        BatchMatchOptions {
            include_broad: true,
            ..Default::default()
        },
    );
    assert!(r.is_empty());

    // Singleton batch equals scalar for that one title.
    let one = vec![data.titles[0].clone()];
    assert_equiv(&eng, &one, true, 256, BroadStrategy::Columnar, true, true);
    assert_equiv(&eng, &one, true, 1, BroadStrategy::Columnar, true, true);
}

#[test]
fn batch_size_never_changes_results() {
    // The same corpus at wildly different batch sizes must yield identical
    // per-title results (batch size is a performance knob, never a semantic one).
    let data = gen(0x1234, 10_000, 1_000, 0.2);
    let eng = build_multi(&data);
    let reference = batch_result(
        &eng,
        &data.titles,
        BatchMatchOptions {
            include_broad: true,
            broad_batch_size: 256,
            broad_strategy: BroadStrategy::Columnar,
            broad_materialize: true,
            broad_prefilter: true,
        },
    );
    for &bs in &[1usize, 3, 64, 65, 1000, 5000] {
        let other = batch_result(
            &eng,
            &data.titles,
            BatchMatchOptions {
                include_broad: true,
                broad_batch_size: bs,
                broad_strategy: BroadStrategy::Columnar,
                broad_materialize: true,
                broad_prefilter: true,
            },
        );
        assert_eq!(other, reference, "results changed at batch_size {bs}");
    }
}
